package rpc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

// StreamValue is a single value received from a push-based stream.
type StreamValue struct {
	Data  any
	Error error
}

// PullValue is a single value received from a pull-based iterator.
type PullValue struct {
	Value any
	Error error
}

// CallStream makes a push-based streaming RPC call whose channel yields values until the stream ends or ctx is done.
func (c *Client) CallStream(ctx context.Context, subject string, args ...any) (<-chan StreamValue, error) {
	var ch <-chan StreamValue
	err := c.withNoResponderRetry(ctx, nil, methodProbe{}, func() error {
		var callErr error
		ch, callErr = c.callStreamOnce(ctx, subject, args...)
		return callErr
	})
	return ch, err
}

// CallStreamWithOptions is CallStream with per-call no-responder retry overrides, nil keeps the client defaults.
func (c *Client) CallStreamWithOptions(ctx context.Context, subject string, opts *RequestOptions, args ...any) (<-chan StreamValue, error) {
	return c.callStreamWithOptions(ctx, subject, opts, methodProbe{}, args...)
}

func (c *Client) callStreamWithOptions(ctx context.Context, subject string, opts *RequestOptions, probe methodProbe, args ...any) (<-chan StreamValue, error) {
	var override *NoResponderRetryOptions
	if opts != nil {
		override = opts.NoResponderRetry
	}

	var ch <-chan StreamValue
	err := c.withNoResponderRetry(ctx, override, probe, func() error {
		var callErr error
		ch, callErr = c.callStreamOnce(ctx, subject, args...)
		return callErr
	})
	return ch, err
}

func (c *Client) callStreamOnce(ctx context.Context, subject string, args ...any) (<-chan StreamValue, error) {
	if c.needsConnect() {
		if err := c.Connect(ctx); err != nil {
			return nil, err
		}
	}

	c.mu.RLock()
	nc := c.nc
	c.mu.RUnlock()
	if nc == nil {
		return nil, fmt.Errorf("not connected")
	}

	c.ensureMuxSubscription(true)

	id := c.generateID()
	streamSubject := fmt.Sprintf("stream.%s.%s", subject, id)

	ch := make(chan StreamValue, 64)

	statusDone := make(chan struct{})
	var statusOnce sync.Once
	closeStatus := func() { statusOnce.Do(func() { close(statusDone) }) }

	// mu serializes every send on ch with close(ch); end closes done first so a push parked on a full buffer lets go
	var mu sync.Mutex
	ended := false
	done := make(chan struct{})
	var doneOnce sync.Once
	send := func(v StreamValue) {
		select {
		case ch <- v:
		default:
			select {
			case ch <- v:
			case <-done:
			}
		}
	}

	sh := &streamHandler{
		subject: subject,
		push: func(v any) {
			mu.Lock()
			if !ended {
				send(StreamValue{Data: v})
			}
			mu.Unlock()
		},
		end: func() {
			doneOnce.Do(func() { close(done) })
			mu.Lock()
			if !ended {
				ended = true
				close(ch)
			}
			mu.Unlock()
			closeStatus()
		},
		error: func(err error) {
			mu.Lock()
			if !ended {
				ended = true
				send(StreamValue{Error: err})
				close(ch)
			}
			mu.Unlock()
			closeStatus()
		},
	}
	c.streamHandlers.Store(id, sh)

	// a slow consumer backs up into nats.go instead of losing items, the end message must not be dropped either
	unsub, err := c.Subscribe(streamSubject, func(data []byte) {
		var msg StreamMessage
		if err := DecodeMessageInto(data, &msg); err != nil {
			return
		}
		if msg.ID != id {
			return
		}

		v, ok := c.streamHandlers.Load(msg.ID)
		if !ok {
			return
		}
		handler := v.(*streamHandler)

		switch msg.Type {
		case "data":
			handler.push(msg.Data)
		case "end":
			handler.end()
			c.streamHandlers.Delete(msg.ID)
			c.statusHandlers.Delete(msg.ID)
		case "error":
			if msg.Error != nil {
				handler.error(RPCExceptionFromError(msg.Error))
			} else {
				handler.error(NewRPCException(ErrCodeStreamError, "Unknown stream error"))
			}
			c.streamHandlers.Delete(msg.ID)
			c.statusHandlers.Delete(msg.ID)
		}
	}, func(sub *nats.Subscription) { _ = sub.SetPendingLimits(-1, -1) })
	if err != nil {
		c.streamHandlers.Delete(id)
		sh.end()
		return nil, err
	}

	// ends with ctx or with the stream itself, so a finished stream does not keep its subscription
	go func() {
		select {
		case <-ctx.Done():
		case <-statusDone:
		}
		if _, ok := c.streamHandlers.LoadAndDelete(id); ok && ctx.Err() != nil {
			_ = c.Publish(streamSubject+".cancel", map[string]any{"id": id})
		}
		sh.end()
		c.statusHandlers.Delete(id)
		unsub()
	}()

	// the reply subject only ever carries a 503 status, stream responders never answer on it
	noRespCh := make(chan *RPCException, 1)
	c.statusHandlers.Store(id, func(error) {
		select {
		case noRespCh <- noRespondersException(subject, "No responders for "+subject):
		default:
		}
	})

	streamParams := StreamParams{
		Stream:        true,
		StreamSubject: streamSubject,
		Args:          args,
	}
	message := RPCMessage{
		ID:     id,
		Method: "stream",
		Params: streamParams,
	}

	if err := c.publishInternal(subject, message, "rpc.reply."+id); err != nil {
		c.streamHandlers.Delete(id)
		c.statusHandlers.Delete(id)
		sh.end()
		unsub()
		return nil, err
	}

	// the flush lets a synchronous 503 arrive before the check below
	_ = nc.Flush()

	select {
	case noRespErr := <-noRespCh:
		c.streamHandlers.Delete(id)
		c.statusHandlers.Delete(id)
		sh.end()
		unsub()
		return nil, &localNoResponders{err: noRespErr}
	default:
	}

	// a late 503 that raced the flush
	go func() {
		select {
		case noRespErr := <-noRespCh:
			if v, loaded := c.streamHandlers.LoadAndDelete(id); loaded {
				handler := v.(*streamHandler)
				handler.error(noRespErr)
			}
		case <-statusDone:
		}
	}()

	return ch, nil
}

// CallPullIterator makes a pull-based iterator RPC call whose channel yields values on demand until ctx is done.
func (c *Client) CallPullIterator(ctx context.Context, subject string, args ...any) (<-chan PullValue, error) {
	return c.callPullIterator(ctx, subject, nil, args...)
}

func (c *Client) callPullIterator(ctx context.Context, subject string, opts *RequestOptions, args ...any) (<-chan PullValue, error) {
	if c.needsConnect() {
		if err := c.Connect(ctx); err != nil {
			return nil, err
		}
	}

	c.ensureMuxSubscription(true)

	iteratorID := c.generateID()
	requestSubject := fmt.Sprintf("_rpc.iterator.%s.request", iteratorID)
	responseSubject := fmt.Sprintf("_rpc.iterator.%s.response", iteratorID)
	// only the 503 statuses of next requests land here, the responses come on responseSubject
	statusInbox := "rpc.reply." + iteratorID

	initParams := PullIteratorParams{
		PullIterator: true,
		IteratorID:   iteratorID,
		Args:         args,
	}
	if err := c.initPullSession(ctx, subject, "pull iterator", iteratorID, opts, initParams); err != nil {
		return nil, err
	}

	ch := make(chan PullValue, 1)

	// settles a consumer parked in next() when the connection is torn down, or it hangs forever
	disconnected := make(chan struct{})
	var settleOnce sync.Once
	c.pullIteratorSettles.Store(iteratorID, func() {
		settleOnce.Do(func() { close(disconnected) })
	})

	go func() {
		defer close(ch)
		defer c.pullIteratorSettles.Delete(iteratorID)

		respCh := make(chan PullIteratorResponse, 8)
		var ended atomic.Bool

		unsub, err := c.Subscribe(responseSubject, func(data []byte) {
			var resp PullIteratorResponse
			if err := DecodeMessageInto(data, &resp); err != nil {
				return
			}
			if !ended.Load() {
				respCh <- resp
			}
		})
		if err != nil {
			ch <- PullValue{Error: err}
			return
		}
		defer unsub()

		noResponders := &RPCError{Code: "503", Message: "No responders for " + subject}
		c.statusHandlers.Store(iteratorID, func(error) {
			if !ended.Load() {
				respCh <- PullIteratorResponse{ID: iteratorID, Type: "error", Error: noResponders}
			}
		})
		defer c.statusHandlers.Delete(iteratorID)

		sendCancel := func() {
			cancelReq := PullIteratorRequest{ID: iteratorID, Type: "cancel"}
			_ = c.Publish(requestSubject, cancelReq)
		}

		// with nobody waiting, the closed channel ends the consumer's range loop instead
		sendDisconnected := func() {
			ended.Store(true)
			select {
			case ch <- PullValue{Error: NewRPCException(ErrCodeConnectionClosed, "Connection closed")}:
			default:
			}
		}

		sendToConsumer := func(v PullValue) bool {
			select {
			case ch <- v:
				return true
			case <-ctx.Done():
				sendCancel()
				ended.Store(true)
				return false
			case <-disconnected:
				ended.Store(true)
				return false
			}
		}

		for {
			// reply points at the status inbox so a vanished responder shows as a 503 instead of a hang
			nextReq := PullIteratorRequest{
				ID:   iteratorID,
				Type: "next",
			}
			if err := c.publishInternal(requestSubject, nextReq, statusInbox); err != nil {
				sendToConsumer(PullValue{Error: err})
				return
			}

			select {
			case resp := <-respCh:
				switch resp.Type {
				case "value":
					if !sendToConsumer(PullValue{Value: resp.Value}) {
						return
					}
				case "done":
					return
				case "error":
					var pv PullValue
					if resp.Error != nil {
						pv = PullValue{Error: pullError(resp.Error, noResponders, subject)}
					} else {
						pv = PullValue{Error: NewRPCException(ErrCodeStreamError, "Iterator error")}
					}
					sendToConsumer(pv)
					return
				}
			case <-ctx.Done():
				sendCancel()
				ended.Store(true)
				return
			case <-disconnected:
				sendDisconnected()
				return
			}
		}
	}()

	return ch, nil
}

func (c *Client) initPullSession(ctx context.Context, subject, kind, iteratorID string, opts *RequestOptions, params any) error {
	cancelled := func(err error) error {
		return fmt.Errorf("init %s: %w", kind, NewRPCException(ErrCodeTimeout, fmt.Sprintf("RPC call to %q context cancelled: %v", subject, err)))
	}
	if err := ctx.Err(); err != nil {
		return cancelled(err)
	}
	sendCancel := func() {
		_ = c.Publish("_rpc.iterator."+iteratorID+".request", PullIteratorRequest{ID: iteratorID, Type: "cancel"})
	}
	call := func(ctx context.Context) error {
		result, err := c.CallWithOptions(ctx, subject, opts, params)
		if err != nil {
			err = fmt.Errorf("init %s: %w", kind, err)
		} else if m, ok := result.(map[string]any); !ok {
			err = fmt.Errorf("failed to initialize %s: unexpected response type", kind)
		} else if id, ok := m["iteratorId"].(string); !ok || id != iteratorID {
			err = fmt.Errorf("failed to initialize %s", kind)
		}
		if err != nil {
			sendCancel()
		}
		return err
	}
	if ctx.Done() == nil {
		return call(ctx)
	}

	// a cancel that overtakes the init reaches no session yet, so the init runs on to its own timeout and a cancel follows
	detached, stop := context.WithoutCancel(ctx), context.CancelFunc(func() {})
	if deadline, ok := ctx.Deadline(); ok {
		timeout := c.Options.Timeout
		if opts != nil && opts.Timeout > 0 {
			timeout = opts.Timeout
		}
		detached, stop = context.WithTimeout(detached, max(timeout, time.Until(deadline)))
	}
	done := make(chan error, 1)
	go func() {
		defer stop()
		done <- call(detached)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		go func() {
			if <-done == nil {
				sendCancel()
			}
		}()
		return cancelled(ctx.Err())
	}
}

func pullError(e, local *RPCError, subject string) *RPCException {
	ex := RPCExceptionFromError(e)
	// the 503 of a next request is a no-responder this client saw itself
	if e == local {
		ex.upstream = subject
	}
	return ex
}
