package rpc

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/nats-io/nats.go"
)

// joins a chunked transfer the way receivers do: chunk i at i*chunkSize
type collectPublisher struct {
	single  []byte
	buf     []byte
	size    int
	pending int
	chunks  int
	reply   string
}

func (c *collectPublisher) Publish(_ string, data []byte) error {
	c.single = bytes.Clone(data)
	return nil
}

func (c *collectPublisher) PublishRequest(_, reply string, data []byte) error {
	c.reply = reply
	return c.Publish("", data)
}

func (c *collectPublisher) PublishMsg(msg *nats.Msg) error {
	c.reply = msg.Reply
	switch msg.Header.Get("x-chunked-transfer") {
	case "header":
		var hdr ChunkedTransferHeader
		if err := Decode(msg.Data, &hdr); err != nil {
			return err
		}
		c.buf = make([]byte, hdr.TotalSize)
		c.size = hdr.ChunkSize
		c.pending = hdr.TotalChunks
	case "chunk":
		i, err := strconv.Atoi(msg.Header.Get("x-chunk-index"))
		if err != nil {
			return err
		}
		if want := min(c.size, len(c.buf)-i*c.size); len(msg.Data) != want {
			return fmt.Errorf("chunk %d has %d bytes, want %d", i, len(msg.Data), want)
		}
		copy(c.buf[i*c.size:], msg.Data)
		c.pending--
		c.chunks++
	}
	return nil
}

func (c *collectPublisher) result() []byte {
	if c.single != nil {
		return c.single
	}
	if c.pending != 0 {
		return nil
	}
	return c.buf
}

func scrubFixture(segments, size int) map[string]any {
	frames := make([]any, segments)
	for i := range frames {
		data := make([]byte, size+i)
		for j := range data {
			data[j] = byte(i + j)
		}
		frames[i] = map[string]any{"pts": i * 40, "data": data}
	}
	return map[string]any{"id": "scrub", "frames": frames}
}

func TestPublishFrameMatchesEncodeMessage(t *testing.T) {
	blob := make([]byte, 3<<20)
	for i := range blob {
		blob[i] = byte(i * 7)
	}
	for _, tc := range []struct {
		name       string
		value      any
		maxPayload int
		chunks     int
	}{
		{"plain under the limit", map[string]any{"a": 1}, 1 << 20, 0},
		{"frame under the limit", scrubFixture(4, 20_000), 1 << 20, 0},
		{"plain over the limit", map[string]any{"text": string(blob[:100_000])}, 30_000, 4},
		{"one big segment", map[string]any{"blob": blob}, 1 << 20, 4},
		{"segments across chunks", scrubFixture(60, 40_000), 1 << 20, 3},
		{"envelope larger than a chunk", map[string]any{"names": scrubNames(5000), "blob": blob[:50_000]}, 20_000, 0},
		{"chunk size ends inside a segment", scrubFixture(10, 33_333), 50_000, 7},
	} {
		want, err := EncodeMessage(tc.value)
		if err != nil {
			t.Fatal(err)
		}
		frame, err := encodeFramePooled(tc.value)
		if err != nil {
			t.Fatal(err)
		}
		pub := &collectPublisher{}
		if err := publishFrame(pub, "s", "r", &frame, tc.maxPayload, false); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		frame.release()
		// map keys come out in any order, so compare what the receiver decodes
		got := pub.result()
		gotValue, err := DecodeMessage(got)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		wantValue, _ := DecodeMessage(want)
		if len(got) != len(want) || !reflect.DeepEqual(gotValue, wantValue) {
			t.Errorf("%s: %d bytes on the wire decode differently from EncodeMessage's %d", tc.name, len(got), len(want))
		}
		if tc.chunks > 0 && pub.chunks != tc.chunks {
			t.Errorf("%s: %d chunks, want %d", tc.name, pub.chunks, tc.chunks)
		}
		if pub.reply != "r" {
			t.Errorf("%s: reply %q, want it on every message", tc.name, pub.reply)
		}
	}
}

func scrubNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("camera-%05d", i)
	}
	return names
}

func BenchmarkPublishFrameChunked(b *testing.B) {
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"scrub8", scrubFixture(201, 40_000)},
		{"blob20", map[string]any{"blob": make([]byte, 20<<20)}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				frame, _ := encodeFramePooled(tc.value)
				_ = publishFrame(discardPublisher{}, "s", "", &frame, 4<<20-8192, false)
				frame.release()
			}
		})
	}
}

type discardPublisher struct{}

func (discardPublisher) Publish(string, []byte) error                { return nil }
func (discardPublisher) PublishRequest(string, string, []byte) error { return nil }
func (discardPublisher) PublishMsg(*nats.Msg) error                  { return nil }
