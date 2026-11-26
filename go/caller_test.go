package rpc

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type callerHandler struct{}

func (*callerHandler) Viewer(ctx context.Context) bool {
	return CallerIsViewer(ctx)
}

func (*callerHandler) Echo(ctx context.Context, _ string) bool {
	return CallerIsViewer(ctx)
}

func (*callerHandler) Stream(ctx context.Context) <-chan bool {
	values := make(chan bool, 1)
	values <- CallerIsViewer(ctx)
	close(values)
	return values
}

func callAs(t *testing.T, nc *nats.Conn, id string, header nats.Header) any {
	t.Helper()
	data, err := Encode(RPCMessage{ID: id, Params: []any{}})
	if err != nil {
		t.Fatal(err)
	}
	var response RPCResponse
	firstReply(t, nc, "rpc.reply."+id, &response, &nats.Msg{Subject: "rpc.who.viewer", Data: data, Header: header})
	return response.Result
}

func firstReply(t *testing.T, nc *nats.Conn, subject string, into any, msgs ...*nats.Msg) {
	t.Helper()
	replies, err := nc.SubscribeSync(subject)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range msgs {
		if err := nc.PublishMsg(m); err != nil {
			t.Fatal(err)
		}
	}
	msg, err := replies.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := DecodeMessageInto(msg.Data, into); err != nil {
		t.Fatal(err)
	}
}

func startCallerServer(t *testing.T) *nats.Conn {
	t.Helper()
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	if _, err := server.RegisterHandler("who", &callerHandler{}); err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(s.url())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func TestAChunkedRequestKeepsItsViewerCaller(t *testing.T) {
	nc := startCallerServer(t)
	data, err := Encode(RPCMessage{ID: "big", Params: []any{strings.Repeat("x", 1000)}})
	if err != nil {
		t.Fatal(err)
	}
	msgs := chunkedMessages(t, "rpc.who.echo", "t1", data)
	msgs[0].Header.Add(CallerRoleHeader, viewerRole)

	var response RPCResponse
	firstReply(t, nc, "rpc.reply.big", &response, msgs...)
	if response.Result != true {
		t.Errorf("chunked call with the header: viewer = %v", response.Result)
	}
}

func TestAStreamHandlerSeesItsViewerCaller(t *testing.T) {
	nc := startCallerServer(t)
	data, err := Encode(RPCMessage{ID: "s1", Params: map[string]any{"__stream": true, "__streamSubject": "stream.s1"}})
	if err != nil {
		t.Fatal(err)
	}

	var first StreamMessage
	firstReply(t, nc, "stream.s1", &first, &nats.Msg{Subject: "rpc.who.stream", Data: data, Header: nats.Header{CallerRoleHeader: {viewerRole}}})
	if first.Type != "data" || first.Data != true {
		t.Errorf("stream with the header: first message = %+v", first)
	}
}

func TestHandlerSeesAViewerCaller(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []HandlerOption
	}{
		{"per method", nil},
		{"namespace", []HandlerOption{WithNamespaceSubscription()}},
		{"lane", []HandlerOption{WithDropExpired("viewer")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := startFakeNATS(t)
			server := connectFake(t, s, nil)
			if _, err := server.RegisterHandler("who", &callerHandler{}, tc.opts...); err != nil {
				t.Fatal(err)
			}
			nc, err := nats.Connect(s.url())
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()

			if got := callAs(t, nc, "plain", nil); got != false {
				t.Errorf("call without the header: viewer = %v", got)
			}
			if got := callAs(t, nc, "forged", nats.Header{CallerRoleHeader: {"admin", viewerRole}}); got != true {
				t.Errorf("call with the header: viewer = %v", got)
			}
		})
	}
}
