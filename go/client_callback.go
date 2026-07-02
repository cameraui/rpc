package rpc

import (
	"context"
	"fmt"
)

// CallWithCallback makes an RPC call with a callback subscription.
// The callback is invoked for each value pushed by the handler.
// Returns an unsubscribe function.
func CallWithCallback[T any](client *Client, subject string, args []any, callback func(T)) (func(), error) {
	id := client.generateID()
	callbackSubject := fmt.Sprintf("rpc.cb.%s", id)

	if !client.IsConnected() && !client.IsClosed() {
		if err := client.Connect(context.Background()); err != nil {
			return nil, err
		}
	}

	// Subscribe to callback messages
	unsub, err := client.Subscribe(callbackSubject, func(data []byte) {
		var msg CallbackMessage
		if err := Decode(data, &msg); err != nil {
			return
		}
		if msg.Type == "data" {
			if val, ok := msg.Data.(T); ok {
				callback(val)
			} else {
				// Try coercion via re-encode/decode
				encoded, err := Encode(msg.Data)
				if err == nil {
					var typed T
					if err := Decode(encoded, &typed); err == nil {
						callback(typed)
					}
				}
			}
		}
	})
	if err != nil {
		return nil, err
	}

	// Send RPC request with callback marker
	callbackParams := CallbackParams{
		Callback:        true,
		CallbackSubject: callbackSubject,
		Args:            args,
	}

	// Use standard call to get initial response
	resp, err := client.Call(context.Background(), subject, callbackParams)
	if err != nil {
		unsub()
		return nil, err
	}
	_ = resp // {ok: true}

	// Return unsubscribe function
	unsubscribe := func() {
		_ = client.Publish(callbackSubject+".cancel", map[string]any{"id": id})
		unsub()
	}

	return unsubscribe, nil
}
