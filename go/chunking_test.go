package rpc

import (
	"bytes"
	"testing"
)

func TestCreateChunks(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 1000)
	chunks := CreateChunks(data, "test-id", 300)

	if len(chunks) != 4 { // ceil(1000/300)
		t.Errorf("expected 4 chunks, got %d", len(chunks))
	}

	// Verify all chunk IDs
	for _, c := range chunks {
		if c.ID != "test-id" {
			t.Errorf("chunk ID = %v, want test-id", c.ID)
		}
	}

	// Verify last chunk
	if !chunks[len(chunks)-1].IsLast {
		t.Error("expected last chunk to have IsLast=true")
	}
	for i := 0; i < len(chunks)-1; i++ {
		if chunks[i].IsLast {
			t.Errorf("chunk %d should not have IsLast=true", i)
		}
	}
}

func TestChunkAssembler(t *testing.T) {
	original := bytes.Repeat([]byte("hello world! "), 100)
	chunkSize := 300
	chunks := CreateChunks(original, "test-id", chunkSize)

	assembler := NewChunkAssembler("test-id", len(original), len(chunks), chunkSize)

	for _, c := range chunks {
		complete, err := assembler.AddChunk(c)
		if err != nil {
			t.Fatal(err)
		}
		if c.IsLast && !complete {
			t.Error("expected complete after last chunk")
		}
	}

	data, err := assembler.GetData()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data, original) {
		t.Error("reassembled data does not match original")
	}
}

func TestChunkAssemblerOutOfOrder(t *testing.T) {
	original := bytes.Repeat([]byte("ABCD"), 250) // 1000 bytes
	chunkSize := 300
	chunks := CreateChunks(original, "ooo-id", chunkSize)

	assembler := NewChunkAssembler("ooo-id", len(original), len(chunks), chunkSize)

	// Add in reverse order
	for i := len(chunks) - 1; i >= 0; i-- {
		_, err := assembler.AddChunk(chunks[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	if !assembler.IsComplete() {
		t.Error("expected complete")
	}

	data, err := assembler.GetData()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, original) {
		t.Error("reassembled data does not match original")
	}
}

func TestChunkingManager(t *testing.T) {
	original := bytes.Repeat([]byte("test"), 500) // 2000 bytes
	chunkSize := 300
	chunks := CreateChunks(original, "mgr-id", chunkSize)

	mgr := NewChunkingManager()

	done := make(chan []byte, 1)
	errCh := make(chan error, 1)

	mgr.StartReceiving("mgr-id", len(chunks),
		func(data []byte) { done <- data },
		func(err error) { errCh <- err },
		len(original), chunkSize,
	)

	for _, c := range chunks {
		mgr.ProcessChunk(c)
	}

	select {
	case data := <-done:
		if !bytes.Equal(data, original) {
			t.Error("reassembled data does not match")
		}
	case err := <-errCh:
		t.Fatal(err)
	}
}

func TestChunkingManagerCancel(t *testing.T) {
	mgr := NewChunkingManager()

	errCh := make(chan error, 1)
	mgr.StartReceiving("cancel-id", 5,
		func(data []byte) { t.Error("should not complete") },
		func(err error) { errCh <- err },
		1500, 300,
	)

	mgr.Cancel("cancel-id")

	err := <-errCh
	if err == nil {
		t.Error("expected error from cancel")
	}
}
