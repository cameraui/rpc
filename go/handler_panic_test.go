package rpc

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"
)

func internalErrorMessage(t *testing.T, err error) string {
	t.Helper()
	var rerr *RPCException
	if !errors.As(err, &rerr) || rerr.Code != ErrCodeInternalError {
		t.Fatalf("err = %v, want INTERNAL_ERROR", err)
	}
	return rerr.Msg
}

func TestUnencodableResultIsAnswered(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	if _, err := server.RegisterHandler("enc", map[string]any{"chan": func() chan int { return make(chan int) }}); err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, &ClientOptions{Timeout: 3 * time.Second})

	start := time.Now()
	_, err := client.CreateProxy("enc").Invoke(context.Background(), "chan")
	if msg := internalErrorMessage(t, err); !strings.Contains(msg, "encode") {
		t.Errorf("message = %q", msg)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("error after %v, the caller waited for its timeout", elapsed)
	}
}

func TestPanicRecoveryAnswersAndKeepsServing(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	handlers := map[string]any{
		"boom": func(xs []int) int { return xs[3] },
		"echo": func(v string) string { return v },
	}
	if _, err := server.RegisterHandler("pr", handlers, WithPanicRecovery()); err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, &ClientOptions{Timeout: 3 * time.Second})
	proxy := client.CreateProxy("pr")

	_, err := proxy.Invoke(context.Background(), "boom", []int{1})
	if msg := internalErrorMessage(t, err); !strings.Contains(msg, "handler panic: runtime error: index out of range") {
		t.Errorf("message = %q", msg)
	}
	if got, err := proxy.Invoke(context.Background(), "echo", "still here"); err != nil || got != "still here" {
		t.Errorf("follow-up call = %v, %v", got, err)
	}
}

func TestStreamPanicIsReported(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	handlers := map[string]any{"gen": func() (<-chan int, error) { panic("boom") }}
	if _, err := server.RegisterHandler("sp", handlers, WithPanicRecovery()); err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch, err := client.CreateProxy("sp").InvokeStream(ctx, "gen")
	if err != nil {
		t.Fatal(err)
	}
	var last StreamValue
	for v := range ch {
		last = v
	}
	if msg := internalErrorMessage(t, last.Error); msg != "handler panic: boom" {
		t.Errorf("message = %q", msg)
	}
}

func startPanicResponder(t *testing.T, handlers map[string]any) (proxy *Proxy, exited <-chan error) {
	t.Helper()
	// the default re-raises the panic and ends the process, so the responder runs as a child of this test binary
	if url := os.Getenv("RPC_TEST_PANIC_RESPONDER"); url != "" {
		c := NewClient(ClientOptions{Servers: []string{url}})
		if err := c.Connect(context.Background()); err != nil {
			os.Exit(3)
		}
		handlers["ready"] = func() bool { return true }
		_, _ = c.RegisterHandler("crash", handlers)
		select {}
	}

	s := startFakeNATS(t)
	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	cmd.Env = append(os.Environ(), "RPC_TEST_PANIC_RESPONDER="+s.url())
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	client := connectFake(t, s, &ClientOptions{Timeout: 3 * time.Second})
	proxy = client.CreateProxy("crash")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := proxy.Invoke(ctx, "ready"); err != nil {
		t.Fatalf("responder did not come up: %v", err)
	}
	return proxy, done
}

func expectCrash(t *testing.T, exited <-chan error) {
	t.Helper()
	select {
	case err := <-exited:
		if err == nil {
			t.Error("the responder survived the panic")
		}
	case <-time.After(5 * time.Second):
		t.Error("the responder did not exit after the panic")
	}
}

func TestHandlerPanicAnswersThenCrashes(t *testing.T) {
	proxy, exited := startPanicResponder(t, map[string]any{"boom": func() { panic("boom") }})

	start := time.Now()
	_, err := proxy.Invoke(context.Background(), "boom")
	internalErrorMessage(t, err)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("error after %v", elapsed)
	}
	expectCrash(t, exited)
}

func TestStreamPanicAnswersThenCrashes(t *testing.T) {
	proxy, exited := startPanicResponder(t, map[string]any{"gen": func() (<-chan int, error) { panic("boom") }})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch, err := proxy.InvokeStream(ctx, "gen")
	if err != nil {
		t.Fatal(err)
	}
	var last StreamValue
	for v := range ch {
		last = v
	}
	if msg := internalErrorMessage(t, last.Error); msg != "handler panic: boom" {
		t.Errorf("message = %q", msg)
	}
	expectCrash(t, exited)
}

func TestVariadicHandlerWithoutArgs(t *testing.T) {
	fn := reflect.ValueOf(func(xs ...string) int { return len(xs) })
	got, err := callHandler(t.Context(), fn, nil)
	if err != nil || got != 0 {
		t.Errorf("callHandler = %v, %v, want 0", got, err)
	}

	anyFn := reflect.ValueOf(func(key string, def ...any) int { return len(def) })
	if got, _ := callHandler(t.Context(), anyFn, []any{"k"}); got != 0 {
		t.Errorf("...any without args got %v values, want 0", got)
	}
}
