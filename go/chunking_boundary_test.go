package rpc

import (
	"bytes"
	"testing"
)

func reassemble(t *testing.T, data []byte, chunkSize int) []byte {
	t.Helper()
	chunks := CreateChunks(data, "b-id", chunkSize)
	asm := NewChunkAssembler("b-id", len(data), len(chunks), chunkSize)
	for _, c := range chunks {
		if _, err := asm.AddChunk(c); err != nil {
			t.Fatal(err)
		}
	}
	out, err := asm.GetData()
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestCreateChunksBoundarySizes(t *testing.T) {
	tests := []struct {
		name      string
		dataLen   int
		chunkSize int
		wantCount int
	}{
		{"empty", 0, 300, 0},
		{"single byte", 1, 300, 1},
		{"exact multiple", 900, 300, 3},
		{"one over multiple", 901, 300, 4},
		{"one under multiple", 899, 300, 3},
		{"smaller than chunk", 100, 300, 1},
		{"equal to chunk", 300, 300, 1},
		{"chunk size 1", 5, 1, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := bytes.Repeat([]byte("x"), tt.dataLen)
			chunks := CreateChunks(data, "id", tt.chunkSize)
			if len(chunks) != tt.wantCount {
				t.Fatalf("chunk count = %d, want %d", len(chunks), tt.wantCount)
			}
			if tt.wantCount > 0 {
				if !chunks[len(chunks)-1].IsLast {
					t.Error("last chunk missing IsLast")
				}
				total := 0
				for i, c := range chunks {
					total += len(c.Data)
					if c.ChunkIndex != i {
						t.Errorf("chunk %d has index %d", i, c.ChunkIndex)
					}
				}
				if total != tt.dataLen {
					t.Errorf("total bytes = %d, want %d", total, tt.dataLen)
				}
			}
		})
	}
}

func TestChunkAssemblerBoundaryReassembly(t *testing.T) {
	tests := []struct {
		name      string
		dataLen   int
		chunkSize int
	}{
		{"single byte", 1, 300},
		{"exact multiple", 900, 300},
		{"one over", 901, 300},
		{"large payload", 1 << 16, 4096},
		{"chunk size one", 32, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, tt.dataLen)
			for i := range data {
				data[i] = byte(i % 251)
			}
			out := reassemble(t, data, tt.chunkSize)
			if !bytes.Equal(out, data) {
				t.Error("reassembled data mismatch")
			}
		})
	}
}

func TestChunkAssemblerProgress(t *testing.T) {
	data := bytes.Repeat([]byte("y"), 1000)
	chunks := CreateChunks(data, "p-id", 300)
	asm := NewChunkAssembler("p-id", len(data), len(chunks), 300)

	got, total := asm.Progress()
	if got != 0 || total != len(chunks) {
		t.Errorf("initial progress = %d/%d, want 0/%d", got, total, len(chunks))
	}

	for i, c := range chunks {
		if _, err := asm.AddChunk(c); err != nil {
			t.Fatal(err)
		}
		got, total := asm.Progress()
		if got != i+1 {
			t.Errorf("progress after %d chunks = %d", i+1, got)
		}
		if total != len(chunks) {
			t.Errorf("total = %d, want %d", total, len(chunks))
		}
	}
}

func TestChunkAssemblerIDMismatch(t *testing.T) {
	asm := NewChunkAssembler("expected", 10, 1, 300)
	_, err := asm.AddChunk(ChunkData{ID: "wrong", ChunkIndex: 0, Data: []byte("data")})
	if err == nil {
		t.Fatal("expected ID mismatch error")
	}
}

func TestChunkAssemblerGetDataBeforeComplete(t *testing.T) {
	data := bytes.Repeat([]byte("z"), 1000)
	chunks := CreateChunks(data, "ic-id", 300)
	asm := NewChunkAssembler("ic-id", len(data), len(chunks), 300)

	if _, err := asm.AddChunk(chunks[0]); err != nil {
		t.Fatal(err)
	}
	if asm.IsComplete() {
		t.Error("should not be complete after one chunk")
	}
	if _, err := asm.GetData(); err == nil {
		t.Error("expected error getting data before complete")
	}
}

func TestChunkingManagerUnknownChunk(t *testing.T) {
	mgr := NewChunkingManager()
	// ProcessChunk for an unregistered transfer must not panic.
	mgr.ProcessChunk(ChunkData{ID: "missing", ChunkIndex: 0, Data: []byte("data")})
}

func TestChunkingManagerIDMismatchTriggersError(t *testing.T) {
	mgr := NewChunkingManager()
	errCh := make(chan error, 1)
	mgr.StartReceiving("real-id", 1,
		func([]byte) { t.Error("should not complete") },
		func(err error) { errCh <- err },
		10, 300,
	)

	// Inject a chunk routed to real-id but carrying a mismatched inner ID.
	mgr.assemblers["real-id"].id = "other"
	mgr.ProcessChunk(ChunkData{ID: "real-id", ChunkIndex: 0, Data: []byte("data")})

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("expected non-nil error")
		}
	default:
		t.Error("expected error callback")
	}
}
