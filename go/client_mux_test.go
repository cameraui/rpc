package rpc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestBrokenReplyFailsTheCallAtOnce(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	for subject, reply := range map[string]func(id string) []byte{
		"rpc.broken.garbage": func(string) []byte { return []byte{0xc1, 0xc1} },
		"rpc.broken.other": func(string) []byte {
			data, _ := EncodeMessage(RPCResponse{ID: "someone.else", Result: "x"})
			return data
		},
	} {
		if _, err := server.Subscribe(subject, func(data []byte) {
			var msg RPCMessage
			if DecodeMessageInto(data, &msg) == nil {
				_ = server.nc.Publish("rpc.reply."+msg.ID, reply(msg.ID))
			}
		}); err != nil {
			t.Fatal(err)
		}
	}
	client := connectFake(t, s, &ClientOptions{Timeout: 2 * time.Second})

	for _, subject := range []string{"rpc.broken.garbage", "rpc.broken.other"} {
		start := time.Now()
		_, err := client.Call(context.Background(), subject)
		var rerr *RPCException
		if !errors.As(err, &rerr) || rerr.Code != ErrCodeInternalError || !strings.Contains(rerr.Msg, `Reply to "`+subject+`" failed`) {
			t.Errorf("%s: err = %v, want INTERNAL_ERROR", subject, err)
		}
		if took := time.Since(start); took > time.Second {
			t.Errorf("%s: error after %v, the call waited for its timeout", subject, took)
		}
	}
}

func TestMuxDropsAReplyNobodyWaitsForWithoutDecoding(t *testing.T) {
	c := NewClient(ClientOptions{})
	reply, err := EncodeMessage(RPCResponse{ID: "other.1", Result: map[string]any{"faces": strings.Repeat("f", 2000), "n": 1}})
	if err != nil {
		t.Fatal(err)
	}
	msg := &nats.Msg{Subject: "rpc.reply.other.1", Data: reply}
	if allocs := testing.AllocsPerRun(100, func() { c.handleMuxMessage(msg) }); allocs > 1 {
		t.Errorf("%v allocations for a reply of another client", allocs)
	}
}
