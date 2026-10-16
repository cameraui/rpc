package rpc

import (
	"context"
	"testing"
	"time"
)

func TestSuspendAndConnectRestoreIsolatedChildren(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	if _, err := server.RegisterHandler("shared", map[string]any{"ping": func() string { return "shared" }}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.RegisterHandler("own", map[string]any{"ping": func() string { return "own" }}, WithIsolatedConnection()); err != nil {
		t.Fatal(err)
	}
	proxy, _, err := server.CreateIsolatedProxy("shared")
	if err != nil {
		t.Fatal(err)
	}
	closedProxy, closeProxy, err := server.CreateIsolatedProxy("shared")
	if err != nil {
		t.Fatal(err)
	}
	if err := closeProxy(); err != nil {
		t.Fatal(err)
	}

	if err := server.Suspend(); err != nil {
		t.Fatal(err)
	}
	if err := server.Reconfigure(ReconfigureOptions{Servers: []string{s.url()}}); err != nil {
		t.Fatal(err)
	}
	if err := server.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	caller := connectFake(t, s, nil)
	for _, ns := range []string{"shared", "own"} {
		if got, err := caller.CallWithOptions(context.Background(), "rpc."+ns+".ping", noRetry); err != nil || got != ns {
			t.Errorf("%s after Suspend and Connect = %v, %v", ns, got, err)
		}
	}
	if got, err := proxy.Invoke(context.Background(), "ping"); err != nil || got != "shared" {
		t.Errorf("isolated proxy after Suspend and Connect = %v, %v", got, err)
	}
	if closedProxy.client.IsConnected() {
		t.Error("a closed isolated proxy came back with Connect")
	}
	if n := len(server.isolatedChildren()); n != 2 {
		t.Errorf("%d isolated clients tracked, want 2 (the handler's and the open proxy's)", n)
	}
}

func waitConns(t *testing.T, s *fakeNATS, want int) {
	t.Helper()
	for range 200 {
		if s.connCount() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%d connections on the server, want %d", s.connCount(), want)
}

func TestIsolatedClientsAreReleased(t *testing.T) {
	s := startFakeNATS(t)
	parent := connectFake(t, s, &ClientOptions{Name: "me"})
	base := s.connCount()

	ch, err := parent.Channel("iso", true)
	if err != nil {
		t.Fatal(err)
	}
	private, err := parent.PrivateChannelConnect("iso", "peer", true)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(parent.isolatedChildren()); n != 2 {
		t.Fatalf("%d isolated clients after opening two channels", n)
	}
	_ = ch.Close()
	_ = private.Close()
	if n := len(parent.isolatedChildren()); n != 0 {
		t.Errorf("%d isolated clients tracked after closing the channels, want 0", n)
	}

	if _, err := parent.CreateServiceProxy(context.Background(), "nobody", WithIsolatedServiceProxy()); err == nil {
		t.Error("a proxy for a missing service was created")
	}
	if _, err := NewRPCService(parent).RegisterHandler(ServiceConfig{Name: "bad name"}, map[string]any{"x": func() {}}, true); err == nil {
		t.Error("a service with an invalid name was registered")
	}
	if _, err := parent.RegisterHandler("reply", map[string]any{"x": func() {}}, WithIsolatedConnection(), WithNamespaceSubscription()); err == nil {
		t.Error("a namespace subscription on rpc.reply was registered")
	}
	if _, err := parent.RegisterHandler("bad ns", map[string]any{"x": func() {}}, WithIsolatedConnection()); err == nil {
		t.Error("a handler with an invalid subject was registered")
	}

	if n := len(parent.isolatedChildren()); n != 0 {
		t.Errorf("%d isolated clients tracked after the failed setups, want 0", n)
	}
	waitConns(t, s, base)
}
