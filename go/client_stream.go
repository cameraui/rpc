package rpc

import (
	"context"
	"fmt"

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

// CallStream makes a push-based streaming RPC call.
// Returns a channel that yields StreamValue items until the stream ends.
// Cancel via the context to stop early.
func (c *Client) CallStream(ctx context.Context, subject string, args ...any) (<-chan StreamValue, error) {
	var ch <-chan StreamValue
	err := c.withNoResponderRetry(ctx, nil, func() error {
		var callErr error
		ch, callErr = c.callStreamOnce(ctx, subject, args...)
		return callErr
	})
	return ch, err
}

// CallStreamWithOptions is the same as CallStream but with per-call no-responder
// retry overrides. Pass nil to use client-wide defaults.
func (c *Client) CallStreamWithOptions(ctx context.Context, subject string, opts *RequestOptions, args ...any) (<-chan StreamValue, error) {
	var override *NoResponderRetryOptions
	if opts != nil {
		override = opts.NoResponderRetry
	}

	var ch <-chan StreamValue
	err := c.withNoResponderRetry(ctx, override, func() error {
		var callErr error
		ch, callErr = c.callStreamOnce(ctx, subject, args...)
		return callErr
	})
	return ch, err
}

// callStreamOnce performs a single streaming RPC call attempt.
func (c *Client) callStreamOnce(ctx context.Context, subject string, args ...any) (<-chan StreamValue, error) {
	if !c.IsConnected() && !c.IsClosed() {
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

	id := GenerateID()
	streamSubject := fmt.Sprintf("stream.%s.%s", subject, id)

	ch := make(chan StreamValue, 64)
	ended := false

	sh := &streamHandler{
		push: func(v any) {
			if !ended {
				ch <- StreamValue{Data: v}
			}
		},
		end: func() {
			if !ended {
				ended = true
				close(ch)
			}
		},
		error: func(err error) {
			if !ended {
				ended = true
				ch <- StreamValue{Error: err}
				close(ch)
			}
		},
	}
	c.streamHandlers.Store(id, sh)

	// Subscribe to stream messages
	unsub, err := c.Subscribe(streamSubject, func(data []byte) {
		var msg StreamMessage
		if err := Decode(data, &msg); err != nil {
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
		case "error":
			if msg.Error != nil {
				handler.error(RPCExceptionFromError(msg.Error))
			} else {
				handler.error(NewRPCException(ErrCodeStreamError, "Unknown stream error"))
			}
			c.streamHandlers.Delete(msg.ID)
		}
	})
	if err != nil {
		c.streamHandlers.Delete(id)
		close(ch)
		return nil, err
	}

	// Handle context cancellation
	go func() {
		<-ctx.Done()
		if _, ok := c.streamHandlers.LoadAndDelete(id); ok {
			_ = c.Publish(streamSubject+".cancel", map[string]any{"id": id})
			if !ended {
				ended = true
				close(ch)
			}
		}
		unsub()
	}()

	// Send request — use a channel to detect 503 synchronously before returning
	noRespCh := make(chan error, 1)
	inbox := nc.NewInbox()
	noRespSub, err := nc.Subscribe(inbox, func(msg *nats.Msg) {
		if len(msg.Data) == 0 && msg.Header != nil && msg.Header.Get("Status") == "503" {
			noRespCh <- NewRPCException(ErrCodeNotFound, "No responders for "+subject)
		}
	})
	if err != nil {
		c.streamHandlers.Delete(id)
		unsub()
		close(ch)
		return nil, err
	}
	_ = noRespSub.AutoUnsubscribe(1)

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

	if err := c.publishInternal(subject, message, inbox); err != nil {
		c.streamHandlers.Delete(id)
		unsub()
		_ = noRespSub.Unsubscribe()
		close(ch)
		return nil, err
	}

	// Flush to ensure the 503 response (if any) is received before we return
	_ = nc.Flush()

	// Check for no-responder error
	select {
	case noRespErr := <-noRespCh:
		c.streamHandlers.Delete(id)
		unsub()
		_ = noRespSub.Unsubscribe()
		close(ch)
		return nil, noRespErr
	default:
	}

	// Wire up the 503 handler for late arrivals (edge case)
	go func() {
		if noRespErr, ok := <-noRespCh; ok {
			if v, loaded := c.streamHandlers.LoadAndDelete(id); loaded {
				handler := v.(*streamHandler)
				handler.error(noRespErr)
			}
		}
	}()

	return ch, nil
}

// CallPullIterator makes a pull-based iterator RPC call.
// Returns a channel that yields PullValue items on demand.
// Cancel via the context to stop early.
func (c *Client) CallPullIterator(ctx context.Context, subject string, args ...any) (<-chan PullValue, error) {
	if !c.IsConnected() && !c.IsClosed() {
		if err := c.Connect(ctx); err != nil {
			return nil, err
		}
	}

	iteratorID := GenerateID()
	requestSubject := fmt.Sprintf("_rpc.iterator.%s.request", iteratorID)
	responseSubject := fmt.Sprintf("_rpc.iterator.%s.response", iteratorID)

	// Initialize the pull iterator
	initParams := PullIteratorParams{
		PullIterator: true,
		IteratorID:   iteratorID,
		Args:         args,
	}
	initResult, err := c.Call(ctx, subject, initParams)
	if err != nil {
		return nil, fmt.Errorf("init pull iterator: %w", err)
	}

	// Validate response contains iteratorId
	if m, ok := initResult.(map[string]any); ok {
		if retID, ok := m["iteratorId"].(string); !ok || retID != iteratorID {
			return nil, fmt.Errorf("failed to initialize pull iterator")
		}
	} else {
		return nil, fmt.Errorf("failed to initialize pull iterator: unexpected response type")
	}

	ch := make(chan PullValue, 1)

	go func() {
		defer close(ch)

		// Subscribe to responses
		respCh := make(chan PullIteratorResponse, 8)
		ended := false

		unsub, err := c.Subscribe(responseSubject, func(data []byte) {
			var resp PullIteratorResponse
			if err := Decode(data, &resp); err != nil {
				return
			}
			if !ended {
				respCh <- resp
			}
		})
		if err != nil {
			ch <- PullValue{Error: err}
			return
		}
		defer unsub()

		sendCancel := func() {
			cancelReq := PullIteratorRequest{ID: iteratorID, Type: "cancel"}
			_ = c.Publish(requestSubject, cancelReq)
		}

		// sendToConsumer wraps a channel send in a ctx-aware select so
		// a consumer that has stopped reading doesn't permanently park
		// the goroutine. See client_pull_callback.go for rationale.
		sendToConsumer := func(v PullValue) bool {
			select {
			case ch <- v:
				return true
			case <-ctx.Done():
				sendCancel()
				ended = true
				return false
			}
		}

		for {
			// Send next request
			nextReq := PullIteratorRequest{
				ID:   iteratorID,
				Type: "next",
			}
			if err := c.Publish(requestSubject, nextReq); err != nil {
				sendToConsumer(PullValue{Error: err})
				return
			}

			// Wait for response
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
						pv = PullValue{Error: RPCExceptionFromError(resp.Error)}
					} else {
						pv = PullValue{Error: NewRPCException(ErrCodeStreamError, "Iterator error")}
					}
					sendToConsumer(pv)
					return
				}
			case <-ctx.Done():
				sendCancel()
				ended = true
				return
			}
		}
	}()

	return ch, nil
}
