package rpc

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type chunkResult struct {
	mu        sync.Mutex
	completes [][]byte
	errs      []error
}

func (r *chunkResult) onComplete(data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completes = append(r.completes, bytes.Clone(data))
}

func (r *chunkResult) onError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, err)
}

func (r *chunkResult) counts() (completes, errs int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.completes), len(r.errs)
}

func chunkedMessages(t *testing.T, subject, transferID string, payload []byte) []*nats.Msg {
	t.Helper()
	const chunkSize = 300
	chunks := CreateChunks(payload, transferID, chunkSize)
	hdr, err := Encode(ChunkedTransferHeader{
		Type:        "chunked",
		TransferID:  transferID,
		TotalChunks: len(chunks),
		TotalSize:   len(payload),
		ChunkSize:   chunkSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	msgs := make([]*nats.Msg, 0, 1+len(chunks))
	msgs = append(msgs, &nats.Msg{
		Subject: subject,
		Data:    hdr,
		Header:  nats.Header{"x-chunked-transfer": {"header"}, "x-chunk-id": {transferID}},
	})
	for _, c := range chunks {
		msgs = append(msgs, &nats.Msg{
			Subject: subject,
			Data:    c.Data,
			Header: nats.Header{
				"x-chunked-transfer": {"chunk"},
				"x-chunk-id":         {transferID},
				"x-chunk-index":      {strconv.Itoa(c.ChunkIndex)},
			},
		})
	}
	return msgs
}

func TestStartReceivingRejectsBadHeaders(t *testing.T) {
	tests := []struct {
		name                              string
		totalChunks, totalSize, chunkSize int
	}{
		{"negative size", 1, -5, 300},
		{"zero size", 1, 0, 300},
		{"over the limit", 4, 1001, 300},
		{"zero chunks", 0, 1000, 300},
		{"chunk count too low", 2, 1000, 300},
		{"chunk count too high", 5, 1000, 300},
		{"zero chunk size", 4, 1000, 0},
		{"negative chunk size", 4, 1000, -300},
		{"chunk size over 64 MiB", 1, 1000, 64<<20 + 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr := newChunkingManager(1000, nil)
			var res chunkResult
			mgr.StartReceiving("bad", tt.totalChunks, res.onComplete, res.onError, tt.totalSize, tt.chunkSize)

			if _, errs := res.counts(); errs != 1 {
				t.Errorf("onError ran %d times, want 1", errs)
			}
			if len(mgr.assemblers) != 0 || mgr.timer != nil {
				t.Error("a rejected header must not register a transfer or arm the timer")
			}
		})
	}
}

func TestStartReceivingAcceptsReleasedSenderHeaders(t *testing.T) {
	// released senders cut min(chunkSize, rest), with the header first
	for _, size := range []int{1, 299, 300, 301, 900, 901} {
		mgr := newChunkingManager(1000, nil)
		var res chunkResult
		payload := bytes.Repeat([]byte{7}, size)
		chunks := CreateChunks(payload, "ok", 300)
		mgr.StartReceiving("ok", len(chunks), res.onComplete, res.onError, size, 300)
		for _, c := range chunks {
			mgr.ProcessChunk(c)
		}
		if completes, errs := res.counts(); completes != 1 || errs != 0 || !bytes.Equal(res.completes[0], payload) {
			t.Errorf("size %d: %d completes, %d errors", size, completes, errs)
		}
	}
}

func TestStartReceivingAcceptsBoundaryHeaders(t *testing.T) {
	tests := []struct {
		name                              string
		totalChunks, totalSize, chunkSize int
	}{
		{"one chunk", 1, 10, 300},
		{"chunk size of 64 MiB", 1, 10, 64 << 20},
		{"size at the limit", 4, 1000, 300},
		{"one byte per chunk", 1000, 1000, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr := newChunkingManager(1000, nil)
			defer mgr.Close()
			var res chunkResult
			mgr.StartReceiving("ok", tt.totalChunks, res.onComplete, res.onError, tt.totalSize, tt.chunkSize)
			if _, errs := res.counts(); errs != 0 || mgr.assemblers["ok"] == nil {
				t.Errorf("%d errors, want the header accepted", errs)
			}
		})
	}
}

func TestDecodeChunkHeaderTakesIntegralFloats(t *testing.T) {
	hdrs := nats.Header{"x-chunk-id": {"f"}}
	header := func(totalSize, chunkSize float64) []byte {
		data, err := Encode(map[string]any{
			"type": "chunked", "transferId": "f", "totalChunks": 4.0, "totalSize": totalSize, "chunkSize": chunkSize,
		})
		if err != nil {
			t.Fatal(err)
		}
		return data
	}

	hdr, ok := decodeChunkHeader(hdrs, header(1000, 300))
	if !ok || hdr.TotalChunks != 4 || hdr.TotalSize != 1000 || hdr.ChunkSize != 300 {
		t.Errorf("integral floats: %+v, ok %v", hdr, ok)
	}
	for _, sizes := range [][2]float64{{1000.5, 300}, {1000, 300.7}} {
		if _, ok := decodeChunkHeader(hdrs, header(sizes[0], sizes[1])); ok {
			t.Errorf("non-integral sizes %v accepted", sizes)
		}
	}
}

func TestTransferBudgetCountsTheBitset(t *testing.T) {
	mgr := newChunkingManager(1000, nil)
	defer mgr.Close()
	var res chunkResult
	// 1000 one-byte chunks need a 128-byte bitset on top of the buffer
	mgr.StartReceiving("tiny", 1000, res.onComplete, res.onError, 1000, 1)
	mgr.ProcessChunk(ChunkData{ID: "tiny", ChunkIndex: 0, Data: []byte{1}})

	if _, errs := res.counts(); errs != 1 || len(mgr.assemblers) != 0 {
		t.Errorf("%d errors, %d transfers, want the transfer dropped with its first chunk", errs, len(mgr.assemblers))
	}
	if used := mgr.budget.used.Load(); used != 0 {
		t.Errorf("a dropped transfer keeps %d bytes reserved", used)
	}
}

func TestTransferBudgetIsSharedAndReleased(t *testing.T) {
	budget := newTransferBudget(1000)
	a, b := newChunkingManager(1000, budget), newChunkingManager(1000, budget)
	defer a.Close()
	defer b.Close()
	chunk := func(id string, index int) ChunkData {
		return ChunkData{ID: id, ChunkIndex: index, Data: make([]byte, 300)}
	}
	var resA, resB chunkResult

	a.StartReceiving("x", 2, resA.onComplete, resA.onError, 600, 300)
	a.ProcessChunk(chunk("x", 0))
	b.StartReceiving("y", 2, resB.onComplete, resB.onError, 600, 300)
	if b.assemblers["y"] == nil {
		t.Fatal("a header alone must not need budget")
	}
	b.ProcessChunk(chunk("y", 0))
	if _, errs := resB.counts(); errs != 1 || len(b.assemblers) != 0 {
		t.Fatal("the second manager went over the shared budget")
	}

	a.ProcessChunk(chunk("x", 1))
	if completes, _ := resA.counts(); completes != 1 || budget.used.Load() != 0 {
		t.Fatalf("%d completes, %d bytes still reserved", completes, budget.used.Load())
	}

	b.StartReceiving("y", 2, resB.onComplete, resB.onError, 600, 300)
	b.ProcessChunk(chunk("y", 0))
	if budget.used.Load() == 0 {
		t.Fatal("the transfer was not charged after the release")
	}
	b.StartReceiving("y", 2, resB.onComplete, resB.onError, 600, 300)
	if used := budget.used.Load(); used != 0 {
		t.Errorf("a repeated header keeps %d bytes of the replaced transfer", used)
	}

	b.ProcessChunk(chunk("y", 0))
	b.ProcessChunk(ChunkData{ID: "y", ChunkIndex: 1, Data: make([]byte, 5)})
	if used := budget.used.Load(); used != 0 {
		t.Errorf("a transfer dropped for a bad chunk keeps %d bytes", used)
	}

	a.StartReceiving("z", 2, resA.onComplete, resA.onError, 600, 300)
	a.ProcessChunk(chunk("z", 0))
	a.Close()
	if used := budget.used.Load(); used != 0 {
		t.Errorf("Close keeps %d bytes", used)
	}
}

func TestStartReceivingAllocatesWithTheFirstChunk(t *testing.T) {
	mgr := NewChunkingManager()
	var res chunkResult
	mgr.StartReceiving("big", 256, res.onComplete, res.onError, 1<<30, 4<<20)

	asm := mgr.assemblers["big"]
	if asm == nil {
		t.Fatal("a 1 GiB header within the default limit must be accepted")
	}
	if asm.buffer != nil {
		t.Errorf("header alone allocated %d bytes", len(asm.buffer))
	}
	mgr.Close()
}

func TestProcessChunkRejectsBadChunks(t *testing.T) {
	tests := []struct {
		name  string
		index int
		size  int
	}{
		{"negative index", -1, 300},
		{"index past the last chunk", 4, 100},
		{"index far past the buffer", 99, 300},
		{"short chunk", 0, 299},
		{"long chunk", 1, 301},
		{"long last chunk", 3, 101},
		{"short last chunk", 3, 99},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr := NewChunkingManager()
			var res chunkResult
			mgr.StartReceiving("t", 4, res.onComplete, res.onError, 1000, 300)
			mgr.ProcessChunk(ChunkData{ID: "t", ChunkIndex: tt.index, Data: make([]byte, tt.size)})

			if completes, errs := res.counts(); completes != 0 || errs != 1 {
				t.Errorf("%d completes, %d errors, want 0 and 1", completes, errs)
			}
			if len(mgr.assemblers) != 0 {
				t.Error("an invalid chunk must drop its transfer")
			}

			// the manager lock must be free again
			done := make(chan struct{})
			go func() {
				mgr.StartReceiving("next", 1, nil, nil, 10, 300)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("StartReceiving blocked after a bad chunk")
			}
			mgr.Close()
		})
	}
}

func TestProcessChunkDropsTransferOnDuplicateWithWrongLength(t *testing.T) {
	for _, size := range []int{299, 301} {
		mgr := NewChunkingManager()
		var res chunkResult
		mgr.StartReceiving("t", 4, res.onComplete, res.onError, 1000, 300)
		mgr.ProcessChunk(ChunkData{ID: "t", ChunkIndex: 0, Data: make([]byte, 300)})
		mgr.ProcessChunk(ChunkData{ID: "t", ChunkIndex: 0, Data: make([]byte, size)})
		if completes, errs := res.counts(); completes != 0 || errs != 1 || len(mgr.assemblers) != 0 || mgr.budget.used.Load() != 0 {
			t.Fatalf("%d bytes: %d completes, %d errors, %d transfers, want the transfer dropped", size, completes, errs, len(mgr.assemblers))
		}
		for i, n := range []int{300, 300, 300, 100} {
			mgr.ProcessChunk(ChunkData{ID: "t", ChunkIndex: i, Data: make([]byte, n)})
		}
		if completes, errs := res.counts(); completes != 0 || errs != 1 {
			t.Errorf("%d bytes: the resent chunks gave %d completes and %d errors", size, completes, errs)
		}
		mgr.Close()
	}
}

func TestProcessChunkIgnoresDuplicates(t *testing.T) {
	mgr := NewChunkingManager()
	var res chunkResult
	mgr.StartReceiving("dup", 2, res.onComplete, res.onError, 6, 3)

	mgr.ProcessChunk(ChunkData{ID: "dup", ChunkIndex: 0, Data: []byte("AAA")})
	mgr.ProcessChunk(ChunkData{ID: "dup", ChunkIndex: 0, Data: []byte("BBB")})
	mgr.ProcessChunk(ChunkData{ID: "dup", ChunkIndex: 1, Data: []byte("CCC")})

	if completes, errs := res.counts(); completes != 1 || errs != 0 {
		t.Fatalf("%d completes, %d errors, want 1 and 0", completes, errs)
	}
	if got := string(res.completes[0]); got != "AAACCC" {
		t.Errorf("data = %q, want AAACCC", got)
	}
}

func TestChunkAssemblerRejectsOutOfRangeChunks(t *testing.T) {
	asm := NewChunkAssembler("a", 100, 2, 50)
	for _, index := range []int{-1, 2, 7} {
		if _, err := asm.AddChunk(ChunkData{ID: "a", ChunkIndex: index, Data: make([]byte, 50)}); err == nil {
			t.Errorf("index %d accepted", index)
		}
	}

	neg := NewChunkAssembler("n", -5, 1, 50)
	if _, err := neg.AddChunk(ChunkData{ID: "n", ChunkIndex: 0, Data: make([]byte, 5)}); err == nil {
		t.Error("a chunk for a negative size accepted")
	}
}

func TestChunkingManagerLimitsConcurrentTransfers(t *testing.T) {
	mgr := NewChunkingManager()
	defer mgr.Close()
	var res chunkResult
	for i := range maxConcurrentTransfers {
		mgr.StartReceiving("t"+strconv.Itoa(i), 1, res.onComplete, res.onError, 10, 300)
	}
	if _, errs := res.counts(); errs != 0 {
		t.Fatalf("%d errors within the limit", errs)
	}

	mgr.StartReceiving("over", 1, res.onComplete, res.onError, 10, 300)
	if _, errs := res.counts(); errs != 1 {
		t.Errorf("transfer %d: %d errors, want 1", maxConcurrentTransfers+1, errs)
	}
	if _, ok := mgr.assemblers["over"]; ok {
		t.Error("the transfer over the limit was registered")
	}

	mgr.StartReceiving("t0", 1, res.onComplete, res.onError, 10, 300)
	if _, errs := res.counts(); errs != 1 {
		t.Error("a repeated header of a known transfer must not count against the limit")
	}
}

func TestChunkingManagerSweepsStaleTransfersWithoutTraffic(t *testing.T) {
	mgr := NewChunkingManager()
	mgr.ttl = 50 * time.Millisecond
	var res chunkResult
	mgr.StartReceiving("stale", 2, res.onComplete, res.onError, 600, 300)
	mgr.ProcessChunk(ChunkData{ID: "stale", ChunkIndex: 0, Data: make([]byte, 300)})

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, errs := res.counts(); errs == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the stale transfer was not dropped without further traffic")
		}
		time.Sleep(5 * time.Millisecond)
	}

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if len(mgr.assemblers) != 0 || len(mgr.lastActivity) != 0 || mgr.timer != nil {
		t.Error("the sweep left state behind")
	}
}

func TestChunkingManagerSweepKeepsActiveTransfers(t *testing.T) {
	mgr := NewChunkingManager()
	mgr.ttl = 200 * time.Millisecond
	defer mgr.Close()
	var res chunkResult
	mgr.StartReceiving("old", 2, res.onComplete, res.onError, 600, 300)
	time.Sleep(120 * time.Millisecond)
	mgr.StartReceiving("fresh", 2, res.onComplete, res.onError, 600, 300)
	time.Sleep(140 * time.Millisecond)

	mgr.mu.Lock()
	_, oldLeft := mgr.assemblers["old"]
	_, freshLeft := mgr.assemblers["fresh"]
	mgr.mu.Unlock()
	if oldLeft || !freshLeft {
		t.Fatalf("old kept %v, fresh kept %v, want false and true", oldLeft, freshLeft)
	}

	// no further traffic: the first sweep must have armed the next one
	waitFor(t, "the kept transfer timed out", func() bool {
		_, errs := res.counts()
		return errs == 2
	})
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if len(mgr.assemblers) != 0 || mgr.timer != nil {
		t.Error("the second sweep left a transfer or a timer behind")
	}
}

func TestChunkingManagerCloseStopsTheTimer(t *testing.T) {
	mgr := NewChunkingManager()
	mgr.ttl = 20 * time.Millisecond
	var res chunkResult
	mgr.StartReceiving("t", 2, res.onComplete, res.onError, 600, 300)
	mgr.Close()
	time.Sleep(60 * time.Millisecond)

	if _, errs := res.counts(); errs != 0 {
		t.Errorf("Close must drop transfers silently, got %d errors", errs)
	}
	if len(mgr.assemblers) != 0 || mgr.timer != nil {
		t.Error("Close left a transfer or a timer behind")
	}
}

func TestSubscriptionDropsTransferOnBadChunkIndex(t *testing.T) {
	for _, index := range []string{"first", "", "+1", "-0", "99999999999999999999"} {
		t.Run(strconv.Quote(index), func(t *testing.T) {
			c := NewClient(ClientOptions{})
			var got [][]byte
			entry := &subscriptionEntry{handler: func(data []byte) { got = append(got, data) }, chunks: &chunkScope{}}
			handle := c.entryHandler(entry)

			msgs := chunkedMessages(t, "a.b", "x1", bytes.Repeat([]byte("p"), 1000))
			bad := *msgs[2]
			bad.Header = nats.Header{"x-chunked-transfer": {"chunk"}, "x-chunk-id": {"x1"}, "x-chunk-index": {index}}
			bad.Data = bytes.Repeat([]byte("g"), len(bad.Data))
			handle(msgs[0])
			handle(msgs[1])
			handle(&bad)

			cm := entry.chunks.manager.Load()
			if open := len(cm.assemblers); open != 0 || c.chunkBudget.used.Load() != 0 {
				t.Errorf("%d transfers and %d bytes left after the bad chunk", open, c.chunkBudget.used.Load())
			}
			for _, m := range msgs[1:] {
				handle(m)
			}
			if len(got) != 0 {
				t.Errorf("the transfer was delivered after a bad chunk: %d times", len(got))
			}
		})
	}
}

func TestDecodeChunkIndexIsPlainDigits(t *testing.T) {
	decode := func(raw string) (ChunkData, error) {
		return decodeChunk(nats.Header{"x-chunk-id": {"t"}, "x-chunk-index": {raw}}, nil)
	}
	for _, raw := range []string{"", "x", "+1", "-1", "-0", " 1", "1 ", "1.0", "1e3", "0x1", "1234567890123456", "99999999999999999999", "\u0661"} {
		chunk, err := decode(raw)
		if err == nil {
			t.Errorf("index %q accepted as %d", raw, chunk.ChunkIndex)
		} else if chunk.ID != "t" {
			t.Errorf("index %q lost the transfer id", raw)
		}
	}
	valid := map[string]int{"0": 0, "7": 7, "007": 7}
	if strconv.IntSize == 64 {
		big := int64(123456789012345)
		valid["123456789012345"] = int(big)
	}
	for raw, want := range valid {
		if chunk, err := decode(raw); err != nil || chunk.ChunkIndex != want {
			t.Errorf("index %q = %d, %v, want %d", raw, chunk.ChunkIndex, err, want)
		}
	}
}

func TestOverlappingSubscriptionsBothReassemble(t *testing.T) {
	c := NewClient(ClientOptions{})
	var wildcard, exact [][]byte
	handleWildcard := c.entryHandler(&subscriptionEntry{pattern: "a.*", handler: func(d []byte) { wildcard = append(wildcard, d) }, chunks: &chunkScope{}})
	handleExact := c.entryHandler(&subscriptionEntry{pattern: "a.b", handler: func(d []byte) { exact = append(exact, d) }, chunks: &chunkScope{}})

	payload := bytes.Repeat([]byte("0123456789"), 100)
	msgs := chunkedMessages(t, "a.b", "shared", payload)
	// nats delivers a copy per matching subscription, interleaved
	for _, m := range msgs {
		handleWildcard(m)
		handleExact(m)
	}

	if len(wildcard) != 1 || len(exact) != 1 {
		t.Fatalf("deliveries: a.* %d, a.b %d, want 1 each", len(wildcard), len(exact))
	}
	if !bytes.Equal(wildcard[0], payload) || !bytes.Equal(exact[0], payload) {
		t.Error("reassembled payload mismatch")
	}
}

func TestSuspendClosesSubscriptionChunks(t *testing.T) {
	c := NewClient(ClientOptions{})
	entry := &subscriptionEntry{pattern: "a.b", handler: func([]byte) {}, chunks: &chunkScope{}}
	c.subEntries[1] = entry
	handle := c.entryHandler(entry)
	handle(chunkedMessages(t, "a.b", "t", make([]byte, 600))[0])

	cm := entry.chunks.manager.Load()
	if cm == nil || len(cm.assemblers) != 1 {
		t.Fatal("header did not start a transfer")
	}
	if err := c.Suspend(); err != nil {
		t.Fatal(err)
	}
	if entry.chunks.manager.Load() != nil || len(cm.assemblers) != 0 || cm.timer != nil {
		t.Error("Suspend left the subscription's transfer and timer behind")
	}
}

func TestMuxRoutesChunkedReply(t *testing.T) {
	c := NewClient(ClientOptions{})
	pr := &pendingRequest{done: make(chan struct{}), subject: "rpc.ns.big"}
	c.pendingRequests.Store("p.1", pr)

	reply, err := EncodeMessage(RPCResponse{ID: "p.1", Result: string(bytes.Repeat([]byte("r"), 1000))})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range chunkedMessages(t, "rpc.reply.p.1", "m1", reply) {
		c.handleMuxMessage(m)
	}

	select {
	case <-pr.done:
		if got, err := pr.outcome("p.1"); err != nil || got != string(bytes.Repeat([]byte("r"), 1000)) {
			t.Errorf("result %v, err %v", got, err)
		}
	default:
		t.Fatal("chunked reply did not settle the call")
	}
}

func TestMuxRejectsCallOnBadChunkIndex(t *testing.T) {
	for _, index := range []string{"", "x", "1.5", "+1", "-0", "99999999999999999999"} {
		t.Run(strconv.Quote(index), func(t *testing.T) {
			c := NewClient(ClientOptions{})
			pr := &pendingRequest{done: make(chan struct{}), subject: "rpc.ns.big"}
			c.pendingRequests.Store("p.2", pr)

			reply, err := EncodeMessage(RPCResponse{ID: "p.2", Result: string(bytes.Repeat([]byte("r"), 1000))})
			if err != nil {
				t.Fatal(err)
			}
			msgs := chunkedMessages(t, "rpc.reply.p.2", "m2", reply)
			msgs[1].Header.Set("x-chunk-index", index)
			for _, m := range msgs[:2] {
				c.handleMuxMessage(m)
			}

			expectChunkedReplyError(t, pr, `Chunked reply to "rpc.ns.big" failed: invalid chunk index `+strconv.Quote(index))
			if _, ok := c.pendingRequests.Load("p.2"); ok {
				t.Error("the rejected call stayed pending")
			}
			if n := muxTransfers(c); n != 0 {
				t.Errorf("%d transfers left in the mux manager", n)
			}
		})
	}
}

func TestMuxRejectsUndecodableChunkedReply(t *testing.T) {
	c := NewClient(ClientOptions{})
	pr := &pendingRequest{done: make(chan struct{}), subject: "rpc.ns.big"}
	c.pendingRequests.Store("p.6", pr)

	for _, m := range chunkedMessages(t, "rpc.reply.p.6", "m6", bytes.Repeat([]byte{0xc1}, 1000)) {
		c.handleMuxMessage(m)
	}

	expectReplyError(t, pr, "p.6", `Reply to "rpc.ns.big" failed`)
	if _, ok := c.pendingRequests.Load("p.6"); ok {
		t.Error("the rejected call stayed pending")
	}
	if used := c.chunkBudget.used.Load(); used != 0 {
		t.Errorf("%d bytes still reserved", used)
	}
}

func TestMuxRejectsChunkedReplyOfAnotherShape(t *testing.T) {
	pad := strings.Repeat("x", 1000)
	for name, reply := range map[string]any{
		"id not a string":    map[string]any{"id": 5, "result": pad},
		"error not a map":    map[string]any{"id": "other", "error": pad},
		"timing not numbers": map[string]any{"id": "other", "__timing": map[string]any{"start": pad}},
		"top-level scalar":   pad,
	} {
		t.Run(name, func(t *testing.T) {
			c := NewClient(ClientOptions{})
			pr := &pendingRequest{done: make(chan struct{}), subject: "rpc.ns.big"}
			c.pendingRequests.Store("p.8", pr)
			data, err := Encode(reply)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range chunkedMessages(t, "rpc.reply.p.8", "m8", data) {
				c.handleMuxMessage(m)
			}

			expectReplyError(t, pr, "p.8", `Reply to "rpc.ns.big" failed`)
			if n := muxTransfers(c); n != 0 {
				t.Errorf("%d transfers left", n)
			}
			if used := c.chunkBudget.used.Load(); used != 0 {
				t.Errorf("%d bytes still reserved", used)
			}
		})
	}
}

func TestMuxIgnoresBadChunkOfUnknownTransfer(t *testing.T) {
	c := NewClient(ClientOptions{})
	pr := &pendingRequest{done: make(chan struct{}), subject: "rpc.ns.big"}
	c.pendingRequests.Store("p.5", pr)

	reply, err := EncodeMessage(RPCResponse{ID: "p.5", Result: string(bytes.Repeat([]byte("r"), 1000))})
	if err != nil {
		t.Fatal(err)
	}
	msgs := chunkedMessages(t, "rpc.reply.p.5", "m5", reply)
	stray := chunkedMessages(t, "rpc.reply.p.5", "other", reply)[1]
	stray.Header.Set("x-chunk-index", "x")
	c.handleMuxMessage(stray)
	for _, m := range msgs {
		c.handleMuxMessage(m)
	}

	select {
	case <-pr.done:
		if pr.err != nil {
			t.Errorf("a stray chunk failed the call: %v", pr.err)
		}
	default:
		t.Fatal("the chunked reply did not settle the call")
	}
}

func TestMuxRejectsCallOnBadChunk(t *testing.T) {
	c := NewClient(ClientOptions{})
	pr := &pendingRequest{done: make(chan struct{}), subject: "rpc.ns.big"}
	c.pendingRequests.Store("p.3", pr)

	reply, err := EncodeMessage(RPCResponse{ID: "p.3", Result: string(bytes.Repeat([]byte("r"), 1000))})
	if err != nil {
		t.Fatal(err)
	}
	msgs := chunkedMessages(t, "rpc.reply.p.3", "m3", reply)
	msgs[1].Data = msgs[1].Data[:10]
	for _, m := range msgs[:2] {
		c.handleMuxMessage(m)
	}

	expectChunkedReplyError(t, pr, `Chunked reply to "rpc.ns.big" failed`)
	if _, ok := c.pendingRequests.Load("p.3"); ok {
		t.Error("the rejected call stayed pending")
	}
	if n := muxTransfers(c); n != 0 {
		t.Errorf("%d transfers left in the mux manager", n)
	}
}

func TestMuxRejectsRefusedHeaderAtOnce(t *testing.T) {
	c := NewClient(ClientOptions{MaxTransferSize: 1000})
	pr := &pendingRequest{done: make(chan struct{}), subject: "rpc.ns.big"}
	c.pendingRequests.Store("p.4", pr)

	reply, err := EncodeMessage(RPCResponse{ID: "p.4", Result: string(bytes.Repeat([]byte("r"), 2000))})
	if err != nil {
		t.Fatal(err)
	}
	c.handleMuxMessage(chunkedMessages(t, "rpc.reply.p.4", "m4", reply)[0])

	expectChunkedReplyError(t, pr, "invalid transfer size")
}

func TestMuxAssemblesMoreRepliesThanTheTransferCap(t *testing.T) {
	c := NewClient(ClientOptions{})
	const calls = maxConcurrentTransfers + 8
	prs := make([]*pendingRequest, calls)
	replies := make([][]*nats.Msg, calls)
	for i := range calls {
		id := "p." + strconv.Itoa(i)
		prs[i] = &pendingRequest{done: make(chan struct{}), subject: "rpc.ns.big"}
		c.pendingRequests.Store(id, prs[i])
		reply, err := EncodeMessage(RPCResponse{ID: id, Result: strings.Repeat(strconv.Itoa(i), 500)})
		if err != nil {
			t.Fatal(err)
		}
		replies[i] = chunkedMessages(t, "rpc.reply."+id, "m"+strconv.Itoa(i), reply)
	}

	// every header first, so all transfers are open at once
	for _, msgs := range replies {
		c.handleMuxMessage(msgs[0])
	}
	for _, msgs := range replies {
		for _, m := range msgs[1:] {
			c.handleMuxMessage(m)
		}
	}

	for i, pr := range prs {
		select {
		case <-pr.done:
			if got, err := pr.outcome("p." + strconv.Itoa(i)); err != nil || got != strings.Repeat(strconv.Itoa(i), 500) {
				t.Fatalf("call %d: result %.20v, err %v", i, got, err)
			}
		default:
			t.Fatalf("call %d did not settle", i)
		}
	}
}

func TestMuxIgnoresTransferWithoutPendingCall(t *testing.T) {
	c := NewClient(ClientOptions{})
	reply, err := EncodeMessage(RPCResponse{ID: "nobody.1", Result: string(bytes.Repeat([]byte("r"), 1000))})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range chunkedMessages(t, "rpc.reply.nobody.1", "m1", reply)[:2] {
		c.handleMuxMessage(m)
	}
	if n := muxTransfers(c); n != 0 {
		t.Errorf("%d transfers started without a pending call", n)
	}
}

func TestMuxKeepsOneTransferPerCall(t *testing.T) {
	c := NewClient(ClientOptions{})
	pr := &pendingRequest{done: make(chan struct{}), subject: "rpc.ns.big"}
	c.pendingRequests.Store("p.5", pr)
	encode := func(fill string) []byte {
		reply, err := EncodeMessage(RPCResponse{ID: "p.5", Result: strings.Repeat(fill, 1000)})
		if err != nil {
			t.Fatal(err)
		}
		return reply
	}
	first := chunkedMessages(t, "rpc.reply.p.5", "a", encode("a"))
	second := chunkedMessages(t, "rpc.reply.p.5", "b", encode("b"))

	c.handleMuxMessage(first[0])
	c.handleMuxMessage(first[1])
	c.handleMuxMessage(second[0])
	c.chunkingManager.mu.Lock()
	_, kept := c.chunkingManager.assemblers["p.5|b"]
	open := len(c.chunkingManager.assemblers)
	c.chunkingManager.mu.Unlock()
	if !kept || open != 1 {
		t.Fatalf("%d transfers open, second kept %v; want only the second", open, kept)
	}

	for _, m := range append(first[2:], second[1:]...) {
		c.handleMuxMessage(m)
	}
	<-pr.done
	if got, err := pr.outcome("p.5"); err != nil || got != strings.Repeat("b", 1000) {
		t.Errorf("result %.20v, err %v", got, err)
	}
}

func TestCallTimeoutDropsHalfAssembledReply(t *testing.T) {
	c := connectFake(t, startFakeNATS(t), &ClientOptions{Timeout: 300 * time.Millisecond})
	// a silent responder, so the call waits for its timeout instead of a 503
	if _, err := c.Subscribe("rpc.ns.slow", func([]byte) {}); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := c.Call(context.Background(), "rpc.ns.slow")
		done <- err
	}()
	var id string
	deadline := time.Now().Add(time.Second)
	for id == "" && time.Now().Before(deadline) {
		c.pendingRequests.Range(func(key, _ any) bool {
			id = key.(string)
			return false
		})
		time.Sleep(time.Millisecond)
	}
	if id == "" {
		t.Fatal("the call never became pending")
	}
	reply, err := EncodeMessage(RPCResponse{ID: id, Result: string(bytes.Repeat([]byte("r"), 1000))})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range chunkedMessages(t, "rpc.reply."+id, "m1", reply)[:2] {
		c.handleMuxMessage(m)
	}
	if n := muxTransfers(c); n != 1 {
		t.Fatalf("%d transfers open before the timeout, want 1", n)
	}

	if err := <-done; err == nil {
		t.Fatal("the call did not time out")
	}
	if n := muxTransfers(c); n != 0 {
		t.Errorf("%d transfers left after the timeout", n)
	}
}

func expectChunkedReplyError(t *testing.T, pr *pendingRequest, want string) {
	t.Helper()
	select {
	case <-pr.done:
	default:
		t.Fatal("the call was not rejected at once")
	}
	var rerr *RPCException
	if !errors.As(pr.err, &rerr) || rerr.Code != ErrCodeInternalError || !strings.Contains(rerr.Msg, want) {
		t.Errorf("err = %v, want INTERNAL_ERROR containing %q", pr.err, want)
	}
}

func expectReplyError(t *testing.T, pr *pendingRequest, id, want string) {
	t.Helper()
	select {
	case <-pr.done:
	default:
		t.Fatal("the call was not settled at once")
	}
	_, err := pr.outcome(id)
	var rerr *RPCException
	if !errors.As(err, &rerr) || rerr.Code != ErrCodeInternalError || !strings.Contains(rerr.Msg, want) {
		t.Errorf("err = %v, want INTERNAL_ERROR containing %q", err, want)
	}
}

func muxTransfers(c *Client) int {
	c.chunkingManager.mu.Lock()
	defer c.chunkingManager.mu.Unlock()
	return len(c.chunkingManager.assemblers)
}

func TestQueueMembersShareChunkScope(t *testing.T) {
	c := connectFake(t, startFakeNATS(t), nil)
	var got [][]byte
	member := func(queue string) (*subscriptionEntry, nats.MsgHandler, func()) {
		unsub, err := c.SubscribeQueue("a.b", queue, func(d []byte) { got = append(got, d) })
		if err != nil {
			t.Fatal(err)
		}
		c.subMu.Lock()
		e := c.subEntries[c.subSeq]
		c.subMu.Unlock()
		return e, c.entryHandler(e), unsub
	}
	payload := bytes.Repeat([]byte("0123456789"), 100)

	a, handleA, unsubA := member("q")
	msgs := chunkedMessages(t, "a.b", "q1", payload)
	handleA(msgs[0])
	// a member that joins later must not drop the transfer in flight
	b, handleB, unsubB := member("q")
	other, _, _ := member("r")
	if a.chunks != b.chunks || a.chunks == other.chunks {
		t.Fatal("queue members must share a scope per pattern and queue")
	}
	// the server hands each message of a transfer to one member
	for i, m := range msgs[1:] {
		if i%2 == 0 {
			handleB(m)
		} else {
			handleA(m)
		}
	}
	if len(got) != 1 || !bytes.Equal(got[0], payload) {
		t.Fatalf("%d deliveries, want the payload once", len(got))
	}

	msgs = chunkedMessages(t, "a.b", "q2", payload)
	handleA(msgs[0])
	unsubA()
	for _, m := range msgs[1:] {
		handleB(m)
	}
	if len(got) != 2 {
		t.Fatal("a member leaving dropped the transfer of the other")
	}

	handleB(chunkedMessages(t, "a.b", "q3", payload)[0])
	cm := b.chunks.manager.Load()
	unsubB()
	c.subMu.Lock()
	_, kept := c.queueChunks["a.b\x00q"]
	c.subMu.Unlock()
	if kept || b.chunks.manager.Load() != nil || len(cm.assemblers) != 0 {
		t.Error("the last member left its scope open")
	}
}

func TestClientMaxTransferSizeReachesEveryManager(t *testing.T) {
	c := NewClient(ClientOptions{MaxTransferSize: 1000})
	var res chunkResult
	c.chunkingManager.StartReceiving("m", 4, res.onComplete, res.onError, 1001, 300)
	if _, errs := res.counts(); errs != 1 {
		t.Error("the reply mux accepted a header over MaxTransferSize")
	}

	entry := &subscriptionEntry{pattern: "a.b", handler: func([]byte) {}, chunks: &chunkScope{}}
	c.entryHandler(entry)(chunkedMessages(t, "a.b", "big", make([]byte, 1001))[0])
	cm := entry.chunks.manager.Load()
	if cm == nil {
		t.Fatal("the header did not reach the subscription manager")
	}
	if len(cm.assemblers) != 0 {
		t.Error("a subscription accepted a header over MaxTransferSize")
	}
	if cm.budget != c.chunkBudget || c.chunkingManager.budget != c.chunkBudget || c.chunkBudget.limit != 1000 {
		t.Error("the managers do not share the client budget")
	}
}

func TestSubscriptionChunksCloseOnTeardown(t *testing.T) {
	s := startFakeNATS(t)
	subscribe := func(t *testing.T, c *Client) (*subscriptionEntry, *ChunkingManager, func()) {
		t.Helper()
		unsub, err := c.Subscribe("a.b", func([]byte) {})
		if err != nil {
			t.Fatal(err)
		}
		var entry *subscriptionEntry
		c.subMu.Lock()
		for _, e := range c.subEntries {
			if e.pattern == "a.b" {
				entry = e
			}
		}
		c.subMu.Unlock()
		cm := entry.chunks.load(c.Options.MaxTransferSize, c.chunkBudget)
		cm.StartReceiving("t", 2, nil, nil, 600, 300)
		cm.ProcessChunk(ChunkData{ID: "t", ChunkIndex: 0, Data: make([]byte, 300)})
		return entry, cm, unsub
	}
	closed := func(t *testing.T, c *Client, e *subscriptionEntry, cm *ChunkingManager) {
		t.Helper()
		cm.mu.Lock()
		defer cm.mu.Unlock()
		if e.chunks.manager.Load() != nil || len(cm.assemblers) != 0 || cm.timer != nil {
			t.Error("the subscription kept its transfer or timer")
		}
		if used := c.chunkBudget.used.Load(); used != 0 {
			t.Errorf("%d bytes still reserved", used)
		}
	}

	t.Run("unsubscribe", func(t *testing.T) {
		c := connectFake(t, s, nil)
		e, cm, unsub := subscribe(t, c)
		unsub()
		closed(t, c, e, cm)
	})

	t.Run("disconnect", func(t *testing.T) {
		c := connectFake(t, s, nil)
		e, cm, _ := subscribe(t, c)
		c.chunkingManager.StartReceiving("m", 2, nil, nil, 600, 300)
		c.chunkingManager.ProcessChunk(ChunkData{ID: "m", ChunkIndex: 0, Data: make([]byte, 300)})
		if err := c.Disconnect(); err != nil {
			t.Fatal(err)
		}
		closed(t, c, e, cm)
		if len(c.chunkingManager.assemblers) != 0 {
			t.Error("Disconnect kept the reply mux transfer")
		}
	})

	t.Run("restore", func(t *testing.T) {
		c := connectFake(t, s, nil)
		e, cm, _ := subscribe(t, c)
		c.mu.RLock()
		nc := c.nc
		c.mu.RUnlock()
		nc.Close()
		if err := c.Connect(context.Background()); err != nil {
			t.Fatal(err)
		}
		closed(t, c, e, cm)
		if e.sub == nil || !e.sub.IsValid() {
			t.Error("the subscription was not restored")
		}
	})
}

func TestServiceEndpointDropsTransferOnBadChunk(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	echo := map[string]any{"echo": func(v string) string { return v }}
	if _, err := NewRPCService(server).RegisterHandler(ServiceConfig{Name: "svc", Version: "1.0.0"}, echo); err != nil {
		t.Fatal(err)
	}
	sender := connectFake(t, s, nil)
	replies, err := sender.nc.SubscribeSync("echo.reply.>")
	if err != nil {
		t.Fatal(err)
	}

	call, err := EncodeMessage(RPCMessage{ID: "c1", Method: "call", Params: []any{string(bytes.Repeat([]byte("x"), 1000))}})
	if err != nil {
		t.Fatal(err)
	}
	msgs := chunkedMessages(t, "echo", "s1", call)
	bad := *msgs[2]
	bad.Header = nats.Header{"x-chunked-transfer": {"chunk"}, "x-chunk-id": {"s1"}, "x-chunk-index": {"+1"}}
	for _, m := range append([]*nats.Msg{msgs[0], msgs[1], &bad}, msgs[2:]...) {
		if err := sender.nc.PublishMsg(m); err != nil {
			t.Fatal(err)
		}
	}
	// the endpoint handles its messages in order, so this reply follows the dropped transfer
	if err := sender.Publish("echo", RPCMessage{ID: "p1", Method: "call", Params: []any{"hi"}}); err != nil {
		t.Fatal(err)
	}
	msg, err := replies.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp RPCResponse
	if err := DecodeMessageInto(msg.Data, &resp); err != nil || resp.ID != "p1" {
		t.Fatalf("first reply %+v, %v, want p1 and nothing for the dropped transfer", resp, err)
	}
	if used := server.chunkBudget.used.Load(); used != 0 {
		t.Errorf("the dropped transfer keeps %d bytes", used)
	}
}

func TestServiceEndpointKeepsItsOwnTransfers(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	echo := map[string]any{"echo": func(v string) string { return v }}
	if _, err := NewRPCService(server).RegisterHandler(ServiceConfig{Name: "svc", Version: "1.0.0"}, echo); err != nil {
		t.Fatal(err)
	}
	sender := connectFake(t, s, nil)
	replies, err := sender.nc.SubscribeSync("echo.reply.>")
	if err != nil {
		t.Fatal(err)
	}
	reply := func(t *testing.T, id string) {
		t.Helper()
		msg, err := replies.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var resp RPCResponse
		if err := DecodeMessageInto(msg.Data, &resp); err != nil || resp.ID != id || resp.Error != nil {
			t.Fatalf("reply %+v, %v, want %s", resp, err, id)
		}
	}

	call, err := EncodeMessage(RPCMessage{ID: "c1", Method: "call", Params: []any{string(bytes.Repeat([]byte("x"), 1000))}})
	if err != nil {
		t.Fatal(err)
	}
	msgs := chunkedMessages(t, "echo", "s1", call)
	for _, m := range msgs[:2] {
		if err := sender.nc.PublishMsg(m); err != nil {
			t.Fatal(err)
		}
	}
	// the endpoint handles its messages in order, so this reply follows the first chunk
	if err := sender.Publish("echo", RPCMessage{ID: "p1", Method: "call", Params: []any{"hi"}}); err != nil {
		t.Fatal(err)
	}
	reply(t, "p1")

	server.chunkingManager.mu.Lock()
	inMux := len(server.chunkingManager.assemblers)
	server.chunkingManager.mu.Unlock()
	if inMux != 0 {
		t.Error("the endpoint transfer landed in the reply mux manager")
	}
	if server.chunkBudget.used.Load() == 0 {
		t.Error("the endpoint transfer is not charged to the client budget")
	}

	for _, m := range msgs[2:] {
		if err := sender.nc.PublishMsg(m); err != nil {
			t.Fatal(err)
		}
	}
	reply(t, "c1")
}
