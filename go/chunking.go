package rpc

import (
	"fmt"
	"sync"
)

// CreateChunks splits encoded data into chunks for transmission.
func CreateChunks(encoded []byte, chunkID string, maxChunkSize int) []ChunkData {
	totalSize := len(encoded)
	totalChunks := (totalSize + maxChunkSize - 1) / maxChunkSize
	chunks := make([]ChunkData, 0, totalChunks)

	for i := range totalChunks {
		start := i * maxChunkSize
		end := min(start+maxChunkSize, totalSize)
		chunks = append(chunks, ChunkData{
			ID:         chunkID,
			ChunkIndex: i,
			Data:       encoded[start:end],
			IsLast:     i == totalChunks-1,
		})
	}
	return chunks
}

// ChunkAssembler reassembles chunks back into the original data using a pre-allocated buffer.
type ChunkAssembler struct {
	id             string
	buffer         []byte
	receivedChunks map[int]bool
	totalChunks    int
	chunkSize      int
}

// NewChunkAssembler creates a new assembler with a pre-allocated buffer.
func NewChunkAssembler(id string, totalSize, totalChunks, chunkSize int) *ChunkAssembler {
	return &ChunkAssembler{
		id:             id,
		buffer:         make([]byte, totalSize),
		receivedChunks: make(map[int]bool, totalChunks),
		totalChunks:    totalChunks,
		chunkSize:      chunkSize,
	}
}

// AddChunk writes a chunk directly to the pre-allocated buffer.
// Returns true if all chunks are received.
func (a *ChunkAssembler) AddChunk(chunk ChunkData) (bool, error) {
	if chunk.ID != a.id {
		return false, fmt.Errorf("chunk ID mismatch: expected %s, got %s", a.id, chunk.ID)
	}
	offset := chunk.ChunkIndex * a.chunkSize
	copy(a.buffer[offset:], chunk.Data)
	a.receivedChunks[chunk.ChunkIndex] = true
	return a.IsComplete(), nil
}

// IsComplete returns true if all chunks have been received.
func (a *ChunkAssembler) IsComplete() bool {
	return len(a.receivedChunks) == a.totalChunks
}

// GetData returns the reassembled raw data. It must be decoded by the caller.
func (a *ChunkAssembler) GetData() ([]byte, error) {
	if !a.IsComplete() {
		return nil, fmt.Errorf("not all chunks received")
	}
	return a.buffer, nil
}

// Progress returns the current reassembly progress.
func (a *ChunkAssembler) Progress() (received, total int) {
	return len(a.receivedChunks), a.totalChunks
}

// ChunkingManager manages multiple concurrent chunk transfers.
type ChunkingManager struct {
	mu         sync.Mutex
	assemblers map[string]*ChunkAssembler
	onComplete map[string]func([]byte)
	onError    map[string]func(error)
}

// NewChunkingManager creates a new ChunkingManager.
func NewChunkingManager() *ChunkingManager {
	return &ChunkingManager{
		assemblers: make(map[string]*ChunkAssembler),
		onComplete: make(map[string]func([]byte)),
		onError:    make(map[string]func(error)),
	}
}

// StartReceiving begins receiving chunks for a transfer.
func (m *ChunkingManager) StartReceiving(id string, totalChunks int, onComplete func([]byte), onError func(error), totalSize, chunkSize int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	assembler := NewChunkAssembler(id, totalSize, totalChunks, chunkSize)
	m.assemblers[id] = assembler
	m.onComplete[id] = onComplete
	m.onError[id] = onError
}

// ProcessChunk processes an incoming chunk.
func (m *ChunkingManager) ProcessChunk(chunk ChunkData) {
	m.mu.Lock()
	assembler, ok := m.assemblers[chunk.ID]
	if !ok {
		m.mu.Unlock()
		return
	}

	complete, err := assembler.AddChunk(chunk)
	if err != nil {
		errCb := m.onError[chunk.ID]
		delete(m.assemblers, chunk.ID)
		delete(m.onComplete, chunk.ID)
		delete(m.onError, chunk.ID)
		m.mu.Unlock()
		if errCb != nil {
			errCb(err)
		}
		return
	}

	if complete {
		data, err := assembler.GetData()
		completeCb := m.onComplete[chunk.ID]
		errCb := m.onError[chunk.ID]
		delete(m.assemblers, chunk.ID)
		delete(m.onComplete, chunk.ID)
		delete(m.onError, chunk.ID)
		m.mu.Unlock()

		if err != nil {
			if errCb != nil {
				errCb(err)
			}
			return
		}
		if completeCb != nil {
			completeCb(data)
		}
		return
	}

	m.mu.Unlock()
}

// Cancel cancels a transfer.
func (m *ChunkingManager) Cancel(id string) {
	m.mu.Lock()
	errCb := m.onError[id]
	delete(m.assemblers, id)
	delete(m.onComplete, id)
	delete(m.onError, id)
	m.mu.Unlock()

	if errCb != nil {
		errCb(fmt.Errorf("transfer cancelled"))
	}
}
