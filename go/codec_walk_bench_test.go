package rpc

import (
	"strconv"
	"testing"
)

type benchFace struct {
	ID     string         `msgpack:"id"`
	Person string         `msgpack:"person,omitempty"`
	Score  float64        `msgpack:"score"`
	Box    []float64      `msgpack:"box"`
	Camera string         `msgpack:"camera"`
	At     int64          `msgpack:"at"`
	Thumb  []byte         `msgpack:"thumb,omitempty"`
	Attrs  map[string]any `msgpack:"attrs,omitempty"`
}

func benchFaces(n int) []benchFace {
	faces := make([]benchFace, n)
	for i := range faces {
		faces[i] = benchFace{
			ID: "face-" + strconv.Itoa(i), Person: "p" + strconv.Itoa(i%7), Score: 0.83,
			Box: []float64{0.1, 0.2, 0.3, 0.4}, Camera: "cam-1", At: 1759500000000 + int64(i),
			Attrs: map[string]any{"mask": false},
		}
	}
	return faces
}

func benchEncodeReply(b *testing.B, result any) {
	reply := RPCResponse{ID: "p.1", Result: result}
	b.ReportAllocs()
	for b.Loop() {
		_, release, err := encodeMessagePooled(reply)
		if err != nil {
			b.Fatal(err)
		}
		release()
	}
}

func BenchmarkEncodeMessageFaces500(b *testing.B) {
	benchEncodeReply(b, benchFaces(500))
}

func BenchmarkEncodeMessageFaces500Pointers(b *testing.B) {
	faces := benchFaces(500)
	ptrs := make([]*benchFace, len(faces))
	for i := range faces {
		ptrs[i] = &faces[i]
	}
	benchEncodeReply(b, ptrs)
}

func BenchmarkEncodeMessageSmallReply(b *testing.B) {
	benchEncodeReply(b, map[string]any{"ok": true, "n": 3})
}

type benchEventThumb struct {
	Path   string `msgpack:"path"`
	Width  int    `msgpack:"width"`
	Height int    `msgpack:"height"`
}

type benchEvent struct {
	ID        string           `msgpack:"id"`
	CameraID  string           `msgpack:"cameraId"`
	Start     int64            `msgpack:"start"`
	End       int64            `msgpack:"end,omitempty"`
	Labels    []string         `msgpack:"labels"`
	Score     float64          `msgpack:"score"`
	Zones     []string         `msgpack:"zones,omitempty"`
	Thumb     *benchEventThumb `msgpack:"thumb,omitempty"`
	Favorite  bool             `msgpack:"favorite,omitempty"`
	Faces     []benchFace      `msgpack:"faces,omitempty"`
	SubLabel  string           `msgpack:"subLabel,omitempty"`
	EpisodeID string           `msgpack:"episodeId,omitempty"`
}

func benchEvents(n int) []benchEvent {
	events := make([]benchEvent, n)
	for i := range events {
		events[i] = benchEvent{
			ID: "evt-" + strconv.Itoa(i), CameraID: "cam-1", Start: 1759500000000 + int64(i)*1000, End: 1759500005000 + int64(i)*1000,
			Labels: []string{"person", "car"}, Score: 0.91, Thumb: &benchEventThumb{Path: "/t/" + strconv.Itoa(i) + ".jpg", Width: 320, Height: 180},
		}
		if i%3 == 0 {
			events[i].Zones = []string{"driveway"}
			events[i].Faces = []benchFace{{ID: "f", Score: 0.7, Box: []float64{1, 2, 3, 4}}}
		}
	}
	return events
}

func BenchmarkEncodeMessageEvents500(b *testing.B) {
	benchEncodeReply(b, map[string]any{"events": benchEvents(500), "total": 500})
}

func BenchmarkEncodeMessageEvents40(b *testing.B) {
	benchEncodeReply(b, benchEvents(40))
}

func BenchmarkEncodeMessageCallEnvelope(b *testing.B) {
	msg := RPCMessage{ID: "abc.def", Method: "call", Params: []any{"cam-1", map[string]any{"limit": 50}}}
	b.ReportAllocs()
	for b.Loop() {
		_, release, err := encodeMessagePooled(msg)
		if err != nil {
			b.Fatal(err)
		}
		release()
	}
}
