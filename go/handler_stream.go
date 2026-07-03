package rpc

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
)

// handleStreamRequestGo handles a push-based streaming request.
// It calls the handler, iterates over the returned channel, and publishes
// data/end/error messages to the stream subject.
func handleStreamRequestGo(fn reflect.Value, args []any, streamSubject, requestID string, client *Client) {
	// Call the handler
	result, err := callHandler(fn, args)
	if err != nil {
		errMsg := StreamMessage{
			ID:    requestID,
			Type:  "error",
			Error: FormatErrorObject(err),
		}
		_ = client.Publish(streamSubject, errMsg)
		return
	}

	// Set up cancellation listener. atomic.Bool: written by the NATS
	// subscription goroutine, read by this goroutine's publish loop.
	var cancelled atomic.Bool
	cancelUnsub, err := client.Subscribe(streamSubject+".cancel", func(data []byte) {
		cancelled.Store(true)
	})
	if err != nil {
		errMsg := StreamMessage{
			ID:    requestID,
			Type:  "error",
			Error: FormatErrorObject(fmt.Errorf("failed to setup cancel listener: %w", err)),
		}
		_ = client.Publish(streamSubject, errMsg)
		return
	}
	defer cancelUnsub()

	// Try to iterate over the result
	if err := iterateAndPublish(result, requestID, streamSubject, client, &cancelled); err != nil {
		if !cancelled.Load() && client.IsConnected() {
			errMsg := StreamMessage{
				ID:    requestID,
				Type:  "error",
				Error: FormatErrorObject(err),
			}
			_ = client.Publish(streamSubject, errMsg)
		}
		return
	}

	if !cancelled.Load() && client.IsConnected() {
		endMsg := StreamMessage{
			ID:   requestID,
			Type: "end",
		}
		_ = client.Publish(streamSubject, endMsg)
	}
}

// iterateAndPublish iterates over a channel or slice and publishes each value.
func iterateAndPublish(result any, requestID, streamSubject string, client *Client, cancelled *atomic.Bool) error {
	if result == nil {
		return NewRPCException(ErrCodeInternalError, "Handler must return a channel or slice for stream")
	}

	rv := reflect.ValueOf(result)

	switch rv.Kind() {
	case reflect.Chan:
		for {
			if cancelled.Load() || !client.IsConnected() {
				return nil
			}
			val, ok := rv.Recv()
			if !ok {
				return nil // Channel closed
			}
			dataMsg := StreamMessage{
				ID:   requestID,
				Type: "data",
				Data: val.Interface(),
			}
			if err := client.Publish(streamSubject, dataMsg); err != nil {
				return err
			}
		}

	case reflect.Slice, reflect.Array:
		for i := 0; i < rv.Len(); i++ {
			if cancelled.Load() || !client.IsConnected() {
				return nil
			}
			dataMsg := StreamMessage{
				ID:   requestID,
				Type: "data",
				Data: rv.Index(i).Interface(),
			}
			if err := client.Publish(streamSubject, dataMsg); err != nil {
				return err
			}
		}
		return nil

	default:
		return NewRPCException(ErrCodeInternalError, fmt.Sprintf("Handler must return a channel or slice for stream, got %s", rv.Kind()))
	}
}

// handlePullIteratorRequestGo handles a pull-based iterator request.
// It calls the handler and sets up the request/response protocol for iteration.
// onFinished is invoked once when the iterator ends naturally (done/cancel) so
// the caller can drop its cleanup-map entry for this session.
func handlePullIteratorRequestGo(fn reflect.Value, args []any, iteratorID string, client *Client, onFinished func()) (func(), error) {
	result, err := callHandler(fn, args)
	if err != nil {
		return nil, err
	}

	requestSubject := fmt.Sprintf("_rpc.iterator.%s.request", iteratorID)
	responseSubject := fmt.Sprintf("_rpc.iterator.%s.response", iteratorID)

	rv := reflect.ValueOf(result)
	if rv.Kind() != reflect.Chan {
		return nil, NewRPCException(ErrCodeInternalError, "Handler must return a channel for pull iterator")
	}

	// atomic.Bool: written by finish()/cleanup() (NATS goroutine or an
	// arbitrary caller goroutine), read by the request-subscription handler.
	var active atomic.Bool
	active.Store(true)

	// subUnsub is assigned after Subscribe returns but read from the NATS
	// callback goroutine — guard it against that race.
	var subUnsubMu sync.Mutex
	var subUnsub func()

	// Natural end (done/cancel): drop the request subscription and let the
	// caller remove its cleanup-map entry. Without this, every finished
	// session leaves its `_rpc.iterator.*.request` subscription and cleanup
	// closure behind until the whole client disconnects.
	var finishOnce sync.Once
	finish := func() {
		finishOnce.Do(func() {
			active.Store(false)
			subUnsubMu.Lock()
			u := subUnsub
			subUnsubMu.Unlock()
			if u != nil {
				u()
			}
			if onFinished != nil {
				onFinished()
			}
		})
	}

	unsub, err := client.Subscribe(requestSubject, func(data []byte) {
		if !active.Load() || !client.IsConnected() {
			return
		}

		var req PullIteratorRequest
		if err := DecodeMessageInto(data, &req); err != nil {
			return
		}

		switch req.Type {
		case "cancel":
			finish()
			resp := PullIteratorResponse{
				ID:   iteratorID,
				Type: "done",
			}
			_ = client.Publish(responseSubject, resp)

		case "next":
			val, ok := rv.Recv()
			if !ok {
				finish()
				resp := PullIteratorResponse{
					ID:   iteratorID,
					Type: "done",
				}
				_ = client.Publish(responseSubject, resp)
			} else {
				resp := PullIteratorResponse{
					ID:    iteratorID,
					Type:  "value",
					Value: val.Interface(),
				}
				_ = client.Publish(responseSubject, resp)
			}
		}
	})
	if err != nil {
		return nil, err
	}
	subUnsubMu.Lock()
	subUnsub = unsub
	subUnsubMu.Unlock()

	cleanup := func() {
		finishOnce.Do(func() {
			active.Store(false)
			unsub()
			// No onFinished here — the caller is already sweeping its map.
		})
	}

	return cleanup, nil
}
