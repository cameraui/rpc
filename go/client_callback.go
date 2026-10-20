package rpc

import (
	"context"
	"fmt"
)

// CallWithCallback makes an RPC call with a callback subscription and returns its unsubscribe function.
func CallWithCallback[T any](client *Client, subject string, args []any, callback func(T)) (func(), error) {
	return callWithCallback(client, subject, nil, args, callback)
}

func callWithCallback[T any](client *Client, subject string, opts *RequestOptions, args []any, callback func(T)) (func(), error) {
	id := client.generateID()
	callbackSubject := fmt.Sprintf("rpc.cb.%s", id)

	if client.needsConnect() {
		if err := client.Connect(context.Background()); err != nil {
			return nil, err
		}
	}

	unsub, err := client.Subscribe(callbackSubject, func(data []byte) {
		var msg CallbackMessage
		if err := DecodeMessageInto(data, &msg); err != nil {
			return
		}
		if msg.Type == "data" {
			if val, ok := msg.Data.(T); ok {
				callback(val)
			} else {
				var typed T
				if err := convertPooled(msg.Data, &typed); err == nil {
					callback(typed)
				}
			}
		}
	})
	if err != nil {
		return nil, err
	}

	callbackParams := CallbackParams{
		Callback:        true,
		CallbackSubject: callbackSubject,
		Args:            args,
	}

	resp, err := client.CallWithOptions(context.Background(), subject, opts, callbackParams)
	if err != nil {
		unsub()
		return nil, err
	}
	_ = resp

	unsubscribe := func() {
		_ = client.Publish(callbackSubject+".cancel", map[string]any{"id": id})
		unsub()
	}

	return unsubscribe, nil
}
