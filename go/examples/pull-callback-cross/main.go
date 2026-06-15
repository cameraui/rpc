package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

type DataService struct{}

func (s *DataService) PullBatches(batchCount, chunksPerBatch int, invoker *rpc.CallbackInvoker) (<-chan struct{}, error) {
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		for b := range batchCount {
			if !invoker.Active() {
				return
			}
			for i := range chunksPerBatch {
				invoker.Invoke("onChunk", map[string]any{"batch": b, "index": i})
			}
			invoker.Invoke("onChunk", nil)
			select {
			case ch <- struct{}{}:
			case <-time.After(10 * time.Second):
				return
			}
		}
	}()
	return ch, nil
}

func toInt(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case uint8:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		return int64(n), true
	case float32:
		return int64(n), true
	case float64:
		return int64(n), true
	}
	return 0, false
}

func runServer(name string) {
	fmt.Printf("server %s starting...\n", name)
	ctx := context.Background()
	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    "pullcb-cross-go-server-" + name,
	})
	if err := client.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Connect: %v\n", err)
		os.Exit(2)
	}

	unsub, err := client.RegisterHandler("pullcb-"+name, &DataService{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Register: %v\n", err)
		os.Exit(2)
	}
	fmt.Printf("server %s registered under namespace pullcb-%s, ready.\n", name, name)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig
	fmt.Printf("server %s shutting down...\n", name)
	_ = unsub()
	_ = client.Disconnect()
}

func testTarget(clientName, target string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
		Name:    fmt.Sprintf("pullcb-cross-go-client-%s-to-%s", clientName, target),
	})
	if err := client.Connect(ctx); err != nil {
		fmt.Printf("  [go -> %s] ERROR: connect: %v\n", target, err)
		return false
	}
	defer func() { _ = client.Disconnect() }()

	proxy := client.CreateProxy("pullcb-" + target)

	received := []map[string]any{}
	batchEnds := 0

	callbacks := rpc.PullCallbackMap{
		"onChunk": func(data any) {
			if data == nil {
				batchEnds++
				return
			}
			if m, ok := data.(map[string]any); ok {
				received = append(received, m)
			}
		},
	}

	const BATCHES = 3
	const CHUNKS = 5

	ch, err := proxy.InvokePullIteratorWithCallback(
		ctx, "pullBatches", callbacks, []string{"onChunk"}, BATCHES, CHUNKS,
	)
	if err != nil {
		fmt.Printf("  [go -> %s] ERROR: %v\n", target, err)
		return false
	}

	batchesConsumed := 0
	for v := range ch {
		if v.Error != nil {
			fmt.Printf("  [go -> %s] ERROR: iterator: %v\n", target, v.Error)
			return false
		}
		batchesConsumed++
	}

	time.Sleep(100 * time.Millisecond)

	orderOk := true
	for i, r := range received {
		expectedBatch := int64(i / CHUNKS)
		expectedIdx := int64(i % CHUNKS)
		gotBatch, _ := toInt(r["batch"])
		gotIdx, _ := toInt(r["index"])
		if gotBatch != expectedBatch || gotIdx != expectedIdx {
			orderOk = false
			break
		}
	}

	ok := batchesConsumed == BATCHES &&
		len(received) == BATCHES*CHUNKS &&
		batchEnds == BATCHES &&
		orderOk

	if ok {
		fmt.Printf("  [go -> %s] PASS (%d chunks, %d EOB)\n", target, len(received), batchEnds)
	} else {
		fmt.Printf(
			"  [go -> %s] FAIL: consumed=%d/%d, chunks=%d/%d, ends=%d/%d, orderOk=%v\n",
			target, batchesConsumed, BATCHES, len(received), BATCHES*CHUNKS, batchEnds, BATCHES, orderOk,
		)
	}
	return ok
}

func runClient(targets []string) {
	fmt.Printf("testing targets: %s\n", strings.Join(targets, ", "))
	results := make(map[string]bool)
	for _, t := range targets {
		results[t] = testTarget("go", t)
	}

	failed := 0
	for _, ok := range results {
		if !ok {
			failed++
		}
	}

	fmt.Println()
	if failed == 0 {
		fmt.Printf("all %d targets passed\n", len(results))
		os.Exit(0)
	} else {
		fmt.Printf("%d/%d targets failed\n", failed, len(results))
		os.Exit(1)
	}
}

func main() {
	roleFlag := flag.String("role", "client", "server | client")
	nameFlag := flag.String("name", "go", "server: namespace suffix (pullcb-<name>)")
	targetsFlag := flag.String("targets", "node,go,python", "client: comma-separated target names")
	flag.Parse()

	switch *roleFlag {
	case "server":
		runServer(*nameFlag)
	case "client":
		targets := strings.Split(*targetsFlag, ",")
		for i, t := range targets {
			targets[i] = strings.TrimSpace(t)
		}
		runClient(targets)
	default:
		fmt.Fprintf(os.Stderr, "unknown role: %s\n", *roleFlag)
		os.Exit(2)
	}
}
