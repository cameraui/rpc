package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	rpc "github.com/cameraui/rpc/go"
)

type GeneratorService struct{}

func (s *GeneratorService) AsyncGeneratorGenerateNumbers(count int) (<-chan int, error) {
	ch := make(chan int)
	go func() {
		defer close(ch)
		for i := range count {
			time.Sleep(100 * time.Millisecond)
			ch <- i
		}
	}()
	return ch, nil
}

func (s *GeneratorService) SyncGeneratorGenerateNumbers(count int) (<-chan int, error) {
	ch := make(chan int)
	go func() {
		defer close(ch)
		for i := range count {
			ch <- i * 2
		}
	}()
	return ch, nil
}

func (s *GeneratorService) AsyncGenerateFuncReturningAsyncGen(count int) (<-chan string, error) {
	ch := make(chan string)
	go func() {
		defer close(ch)
		for i := range count {
			time.Sleep(50 * time.Millisecond)
			ch <- fmt.Sprintf("async-%d", i)
		}
	}()
	return ch, nil
}

func (s *GeneratorService) AsyncGenerateFuncReturningSyncGen(count int) (<-chan string, error) {
	time.Sleep(100 * time.Millisecond)

	ch := make(chan string)
	go func() {
		defer close(ch)
		for i := range count {
			ch <- fmt.Sprintf("sync-from-async-%d", i)
		}
	}()
	return ch, nil
}

func (s *GeneratorService) SyncGenerateFuncReturningSyncGen(count int) (<-chan map[string]any, error) {
	ch := make(chan map[string]any)
	go func() {
		defer close(ch)
		for i := range count {
			ch <- map[string]any{"index": i, "value": i * i}
		}
	}()
	return ch, nil
}

func (s *GeneratorService) MixedGenerateTypeGenerator(count int) (<-chan any, error) {
	ch := make(chan any)
	go func() {
		defer close(ch)
		for i := range count {
			if i%3 == 0 {
				ch <- i
			} else if i%3 == 1 {
				ch <- fmt.Sprintf("string-%d", i)
			} else {
				ch <- map[string]any{"type": "dict", "value": i}
			}
		}
	}()
	return ch, nil
}

func (s *GeneratorService) GetIterableArray(count int) ([]int, error) {
	result := make([]int, count)
	for i := range count {
		result[i] = i * 3
	}
	return result, nil
}

func (s *GeneratorService) GetAsyncIterableArray(count int) ([]string, error) {
	time.Sleep(50 * time.Millisecond)
	result := make([]string, count)
	for i := range count {
		result[i] = fmt.Sprintf("item-%d", i)
	}
	return result, nil
}

func main() {
	ctx := context.Background()

	server := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Name:    "generator-test-server",
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
	})

	client := rpc.NewClient(rpc.ClientOptions{
		Servers: []string{"nats://localhost:4222"},
		Name:    "generator-test-client",
		Auth:    &rpc.AuthOptions{User: "server", Password: "server_password"},
	})

	if err := server.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "server connect: %v\n", err)
		os.Exit(1)
	}
	if err := client.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "client connect: %v\n", err)
		os.Exit(1)
	}

	unsub, err := server.RegisterHandler("generator", &GeneratorService{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "register handler: %v\n", err)
		os.Exit(1)
	}

	proxy := client.CreateProxy("generator")

	fmt.Println("Testing different generator types:")
	fmt.Println()
	totalStart := time.Now()

	fmt.Println("1. Async generator function:")
	start := time.Now()
	count := 0
	streamCh, err := proxy.InvokeStream(ctx, "asyncGeneratorGenerateNumbers", 5)
	if err != nil {
		fatal("asyncGeneratorGenerateNumbers", err)
	}
	for val := range streamCh {
		if val.Error != nil {
			fmt.Fprintf(os.Stderr, "   stream error: %v\n", val.Error)
			break
		}
		count++
		fmt.Printf("   Received: %v\n", val.Data)
	}
	elapsed := ms(time.Since(start))
	fmt.Printf("   Time: %.1fms (5 items, ~%.1fms per item)\n", elapsed, elapsed/5)

	fmt.Println("\n2. Sync generator function:")
	start = time.Now()
	count = 0
	streamCh, err = proxy.InvokeStream(ctx, "syncGeneratorGenerateNumbers", 5)
	if err != nil {
		fatal("syncGeneratorGenerateNumbers", err)
	}
	for val := range streamCh {
		if val.Error != nil {
			fmt.Fprintf(os.Stderr, "   stream error: %v\n", val.Error)
			break
		}
		count++
		fmt.Printf("   Received: %v\n", val.Data)
	}
	elapsed = ms(time.Since(start))
	fmt.Printf("   Time: %.1fms (%d items, ~%.1fms per item)\n", elapsed, count, elapsed/float64(count))

	fmt.Println("\n3. Async function returning async generator:")
	start = time.Now()
	count = 0
	streamCh, err = proxy.InvokeStream(ctx, "asyncGenerateFuncReturningAsyncGen", 5)
	if err != nil {
		fatal("asyncGenerateFuncReturningAsyncGen", err)
	}
	for val := range streamCh {
		if val.Error != nil {
			fmt.Fprintf(os.Stderr, "   stream error: %v\n", val.Error)
			break
		}
		count++
		fmt.Printf("   Received: %v\n", val.Data)
	}
	elapsed = ms(time.Since(start))
	fmt.Printf("   Time: %.1fms (%d items)\n", elapsed, count)

	fmt.Println("\n4. Async function returning sync generator:")
	start = time.Now()
	count = 0
	streamCh, err = proxy.InvokeStream(ctx, "asyncGenerateFuncReturningSyncGen", 5)
	if err != nil {
		fatal("asyncGenerateFuncReturningSyncGen", err)
	}
	for val := range streamCh {
		if val.Error != nil {
			fmt.Fprintf(os.Stderr, "   stream error: %v\n", val.Error)
			break
		}
		count++
		fmt.Printf("   Received: %v\n", val.Data)
	}
	elapsed = ms(time.Since(start))
	fmt.Printf("   Time: %.1fms (%d items)\n", elapsed, count)

	fmt.Println("\n5. Sync function returning sync generator:")
	start = time.Now()
	count = 0
	streamCh, err = proxy.InvokeStream(ctx, "syncGenerateFuncReturningSyncGen", 5)
	if err != nil {
		fatal("syncGenerateFuncReturningSyncGen", err)
	}
	for val := range streamCh {
		if val.Error != nil {
			fmt.Fprintf(os.Stderr, "   stream error: %v\n", val.Error)
			break
		}
		count++
		fmt.Printf("   Received: %s\n", toJSON(val.Data))
	}
	elapsed = ms(time.Since(start))
	fmt.Printf("   Time: %.1fms (%d items)\n", elapsed, count)

	fmt.Println("\n6. Mixed type generator:")
	start = time.Now()
	typeCounts := map[string]int{"number": 0, "string": 0, "object": 0}
	streamCh, err = proxy.InvokeStream(ctx, "mixedGenerateTypeGenerator", 9)
	if err != nil {
		fatal("mixedGenerateTypeGenerator", err)
	}
	for val := range streamCh {
		if val.Error != nil {
			fmt.Fprintf(os.Stderr, "   stream error: %v\n", val.Error)
			break
		}
		typeName := goType(val.Data)
		typeCounts[typeName]++
		fmt.Printf("   Received: %s (type: %s)\n", toJSON(val.Data), typeName)
	}
	elapsed = ms(time.Since(start))
	fmt.Printf("   Time: %.1fms (types: %s)\n", elapsed, toJSON(typeCounts))

	fmt.Println("\n7. Function returning iterable (array):")
	start = time.Now()
	iterableResult, err := proxy.Invoke(ctx, "getIterableArray", 5)
	if err != nil {
		fatal("getIterableArray", err)
	}
	elapsed = ms(time.Since(start))
	items := toSlice(iterableResult)
	for _, v := range items {
		fmt.Printf("   Received: %v\n", v)
	}
	fmt.Printf("   Time: %.1fms (returned %d items)\n", elapsed, len(items))

	fmt.Println("\n8. Async function returning iterable:")
	start = time.Now()
	asyncIterableResult, err := proxy.Invoke(ctx, "getAsyncIterableArray", 5)
	if err != nil {
		fatal("getAsyncIterableArray", err)
	}
	elapsed = ms(time.Since(start))
	asyncItems := toSlice(asyncIterableResult)
	for _, v := range asyncItems {
		fmt.Printf("   Received: %v\n", v)
	}
	fmt.Printf("   Time: %.1fms (returned %d items)\n", elapsed, len(asyncItems))

	fmt.Println("\n9. Early termination test:")
	start = time.Now()
	cancelCtx, cancel := context.WithCancel(ctx)
	streamCh, err = proxy.InvokeStream(cancelCtx, "asyncGeneratorGenerateNumbers", 100)
	if err != nil {
		fatal("asyncGeneratorGenerateNumbers (early)", err)
	}
	received := 0
	for val := range streamCh {
		if val.Error != nil {
			break
		}
		fmt.Printf("   Received: %v\n", val.Data)
		received++
		if received >= 3 {
			fmt.Println("   Breaking early...")
			cancel()
			for range streamCh {
			}
			break
		}
	}
	cancel()
	elapsed = ms(time.Since(start))
	fmt.Printf("   Time: %.1fms (received %d items before breaking)\n", elapsed, received)

	totalElapsed := ms(time.Since(totalStart))
	fmt.Printf("\nAll generator tests passed. Total time: %.1fms\n", totalElapsed)

	unsub()
	client.Disconnect()
	server.Disconnect()
}

func fatal(label string, err error) {
	fmt.Fprintf(os.Stderr, "%s error: %v\n", label, err)
	os.Exit(1)
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func toJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func goType(v any) string {
	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return "number"
	case string:
		return "string"
	default:
		return "object"
	}
}

func toSlice(v any) []any {
	if s, ok := v.([]any); ok {
		return s
	}
	return nil
}
