package rpc

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	staleTransferTTL       = 30 * time.Second
	defaultMaxTransferSize = 1 << 30
	chunkSizeLimit         = 64 << 20
	maxConcurrentTransfers = 32
	// kept free in every message for the protocol and the chunk headers
	payloadReserve = 8192
	// the smallest payload limit that still leaves chunks of 8 KiB
	minPayloadLimit = 16384
)

type framePublisher interface {
	Publish(subject string, data []byte) error
	PublishRequest(subject, reply string, data []byte) error
	PublishMsg(msg *nats.Msg) error
}

func publishFrame(nc framePublisher, subject, reply string, f *wireFrame, maxPayload int, unchunked bool) error {
	if f.size <= maxPayload {
		if reply != "" {
			return nc.PublishRequest(subject, reply, f.joined())
		}
		return nc.Publish(subject, f.joined())
	}
	if unchunked || maxPayload <= 0 {
		return payloadTooSmall(maxPayload)
	}

	transferID := GenerateID()
	totalChunks := (f.size + maxPayload - 1) / maxPayload
	headerEncoded, err := Encode(ChunkedTransferHeader{
		Type:        "chunked",
		TransferID:  transferID,
		TotalChunks: totalChunks,
		TotalSize:   f.size,
		ChunkSize:   maxPayload,
	})
	if err != nil {
		return fmt.Errorf("encode header: %w", err)
	}
	headerMsg := &nats.Msg{Subject: subject, Reply: reply, Data: headerEncoded, Header: nats.Header{}}
	headerMsg.Header.Set("x-chunked-transfer", "header")
	headerMsg.Header.Set("x-chunk-id", transferID)
	if err := nc.PublishMsg(headerMsg); err != nil {
		return fmt.Errorf("publish header: %w", err)
	}

	index := 0
	send := func(data []byte) error {
		msg := &nats.Msg{Subject: subject, Reply: reply, Data: data, Header: nats.Header{}}
		msg.Header.Set("x-chunked-transfer", "chunk")
		msg.Header.Set("x-chunk-id", transferID)
		msg.Header.Set("x-chunk-index", strconv.Itoa(index))
		if err := nc.PublishMsg(msg); err != nil {
			return fmt.Errorf("publish chunk %d: %w", index, err)
		}
		index++
		return nil
	}

	var scratch *bytes.Buffer
	defer func() {
		if scratch != nil {
			releaseEncodeBuf(scratch)
		}
	}()
	var fill []byte
	pieces := append([][]byte{f.head.Bytes()}, f.segments...)
	for i, p := range pieces {
		last := i == len(pieces)-1
		for len(p) > 0 {
			if len(fill) == 0 && (len(p) >= maxPayload || last) {
				n := min(len(p), maxPayload)
				if err := send(p[:n]); err != nil {
					return err
				}
				p = p[n:]
				continue
			}
			if scratch == nil {
				scratch = encodeBufPool.Get().(*bytes.Buffer)
				scratch.Reset()
				if scratch.Cap() < maxPayload {
					scratch = bytes.NewBuffer(make([]byte, 0, maxPayload))
				}
				fill = scratch.AvailableBuffer()
			}
			n := min(len(p), maxPayload-len(fill))
			fill = append(fill, p[:n]...)
			p = p[n:]
			if len(fill) == maxPayload {
				if err := send(fill); err != nil {
					return err
				}
				// nats.go copied the data before PublishMsg returned
				fill = fill[:0]
			}
		}
	}
	if len(fill) > 0 {
		if err := send(fill); err != nil {
			return err
		}
	}
	if index != totalChunks {
		return fmt.Errorf("chunked transfer sent %d of %d chunks", index, totalChunks)
	}
	return nil
}

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

// ChunkAssembler reassembles chunks back into the original data using a single buffer.
type ChunkAssembler struct {
	id          string
	buffer      []byte
	received    []uint64
	count       int
	totalSize   int
	totalChunks int
	chunkSize   int
	budget      *transferBudget
	reserved    int
}

// NewChunkAssembler creates an assembler for totalSize bytes sent in totalChunks chunks of chunkSize bytes.
func NewChunkAssembler(id string, totalSize, totalChunks, chunkSize int) *ChunkAssembler {
	return &ChunkAssembler{
		id:          id,
		totalSize:   max(totalSize, 0),
		totalChunks: max(totalChunks, 0),
		chunkSize:   chunkSize,
	}
}

// AddChunk writes a chunk into the buffer and reports whether all chunks are received.
func (a *ChunkAssembler) AddChunk(chunk ChunkData) (bool, error) {
	if chunk.ID != a.id {
		return false, fmt.Errorf("chunk ID mismatch: expected %s, got %s", a.id, chunk.ID)
	}
	index := chunk.ChunkIndex
	// the second bound keeps index*chunkSize inside totalSize, so it cannot overflow
	if index < 0 || index >= a.totalChunks || a.chunkSize <= 0 || index > (a.totalSize-1)/a.chunkSize {
		return false, fmt.Errorf("chunk index %d out of range for %d chunks", index, a.totalChunks)
	}
	offset := index * a.chunkSize
	want := min(a.chunkSize, a.totalSize-offset)
	if len(chunk.Data) != want {
		return false, fmt.Errorf("chunk %d has %d bytes, want %d", index, len(chunk.Data), want)
	}

	if a.received == nil {
		words := (a.totalChunks + 63) / 64
		cost := a.totalSize + 8*words
		if !a.budget.reserve(cost) {
			return false, fmt.Errorf("transfers in progress would hold more than %d bytes", a.budget.limit)
		}
		a.reserved = cost
		a.received = make([]uint64, words)
		a.buffer = make([]byte, a.totalSize)
	}
	word, bit := index/64, uint64(1)<<(index%64)
	if a.received[word]&bit != 0 {
		return a.IsComplete(), nil
	}
	copy(a.buffer[offset:], chunk.Data)
	a.received[word] |= bit
	a.count++
	return a.IsComplete(), nil
}

// IsComplete returns true if all chunks have been received.
func (a *ChunkAssembler) IsComplete() bool {
	return a.count == a.totalChunks
}

// GetData returns the reassembled raw data, which the caller still has to decode.
func (a *ChunkAssembler) GetData() ([]byte, error) {
	if !a.IsComplete() {
		return nil, fmt.Errorf("not all chunks received")
	}
	if a.buffer == nil {
		a.buffer = make([]byte, a.totalSize)
	}
	return a.buffer, nil
}

// Progress returns the current reassembly progress.
func (a *ChunkAssembler) Progress() (received, total int) {
	return a.count, a.totalChunks
}

func (a *ChunkAssembler) release() {
	a.budget.release(a.reserved)
	a.reserved = 0
}

// ChunkingManager manages concurrent chunk transfers and drops one that gets no chunk for 30 seconds.
type ChunkingManager struct {
	mu              sync.Mutex
	assemblers      map[string]*ChunkAssembler
	onComplete      map[string]func([]byte)
	onError         map[string]func(error)
	lastActivity    map[string]time.Time
	maxTransferSize int
	maxTransfers    int
	budget          *transferBudget
	ttl             time.Duration
	timer           *time.Timer
	timerGen        int
}

// NewChunkingManager creates a ChunkingManager with 1 GiB per transfer and 1 GiB for all transfers in progress.
func NewChunkingManager() *ChunkingManager {
	return newChunkingManager(0, nil)
}

func newChunkingManager(maxTransferSize int, budget *transferBudget) *ChunkingManager {
	if maxTransferSize <= 0 {
		maxTransferSize = defaultMaxTransferSize
	}
	if budget == nil {
		budget = newTransferBudget(maxTransferSize)
	}
	return &ChunkingManager{
		assemblers:      make(map[string]*ChunkAssembler),
		onComplete:      make(map[string]func([]byte)),
		onError:         make(map[string]func(error)),
		lastActivity:    make(map[string]time.Time),
		maxTransferSize: maxTransferSize,
		maxTransfers:    maxConcurrentTransfers,
		budget:          budget,
		ttl:             staleTransferTTL,
	}
}

// StartReceiving begins receiving chunks for a transfer and drops a header that is inconsistent, too large or over the concurrency limit.
func (m *ChunkingManager) StartReceiving(id string, totalChunks int, onComplete func([]byte), onError func(error), totalSize, chunkSize int) {
	m.mu.Lock()
	err := validateChunkHeader(totalChunks, totalSize, chunkSize, m.maxTransferSize)
	old, known := m.assemblers[id]
	if err == nil && !known && m.maxTransfers > 0 && len(m.assemblers) >= m.maxTransfers {
		err = fmt.Errorf("too many concurrent transfers (%d)", m.maxTransfers)
	}
	if err == nil {
		if known {
			old.release()
		}
		asm := NewChunkAssembler(id, totalSize, totalChunks, chunkSize)
		asm.budget = m.budget
		m.assemblers[id] = asm
		m.onComplete[id] = onComplete
		m.onError[id] = onError
		m.lastActivity[id] = time.Now()
		m.armSweepLocked()
	}
	m.mu.Unlock()

	if err != nil && onError != nil {
		onError(err)
	}
}

// ProcessChunk processes an incoming chunk; an invalid chunk drops its transfer and calls the error callback.
func (m *ChunkingManager) ProcessChunk(chunk ChunkData) {
	m.mu.Lock()
	assembler, ok := m.assemblers[chunk.ID]
	if !ok {
		m.mu.Unlock()
		return
	}
	m.lastActivity[chunk.ID] = time.Now()

	complete, err := assembler.AddChunk(chunk)
	if err != nil {
		errCb := m.dropLocked(chunk.ID)
		m.mu.Unlock()
		if errCb != nil {
			errCb(err)
		}
		return
	}

	if complete {
		data, err := assembler.GetData()
		completeCb := m.onComplete[chunk.ID]
		errCb := m.dropLocked(chunk.ID)
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
	m.fail(id, errors.New("transfer cancelled"))
}

// Close drops every transfer in progress without calling its callbacks; the manager stays usable.
func (m *ChunkingManager) Close() {
	m.mu.Lock()
	if m.timer != nil {
		m.timer.Stop()
		m.timer = nil
	}
	m.timerGen++
	for _, asm := range m.assemblers {
		asm.release()
	}
	clear(m.assemblers)
	clear(m.onComplete)
	clear(m.onError)
	clear(m.lastActivity)
	m.mu.Unlock()
}

func (m *ChunkingManager) fail(id string, err error) {
	m.mu.Lock()
	errCb := m.dropLocked(id)
	m.mu.Unlock()

	if errCb != nil {
		errCb(err)
	}
}

func (m *ChunkingManager) discard(id string) {
	m.mu.Lock()
	m.dropLocked(id)
	m.mu.Unlock()
}

func (m *ChunkingManager) dropLocked(id string) func(error) {
	errCb := m.onError[id]
	if asm := m.assemblers[id]; asm != nil {
		asm.release()
	}
	delete(m.assemblers, id)
	delete(m.onComplete, id)
	delete(m.onError, id)
	delete(m.lastActivity, id)
	return errCb
}

func (m *ChunkingManager) armSweepLocked() {
	if m.timer == nil {
		m.scheduleSweepLocked(m.ttl)
	}
}

func (m *ChunkingManager) scheduleSweepLocked(after time.Duration) {
	m.timerGen++
	gen := m.timerGen
	m.timer = time.AfterFunc(after, func() { m.sweepStale(gen) })
}

func (m *ChunkingManager) sweepStale(gen int) {
	m.mu.Lock()
	// a timer stopped by Close or replaced by a reschedule may still fire once
	if gen != m.timerGen {
		m.mu.Unlock()
		return
	}
	m.timer = nil
	now := time.Now()
	var staleCbs []func(error)
	var oldest time.Time
	for id, activity := range m.lastActivity {
		if now.Sub(activity) >= m.ttl {
			if errCb := m.dropLocked(id); errCb != nil {
				staleCbs = append(staleCbs, errCb)
			}
			continue
		}
		if oldest.IsZero() || activity.Before(oldest) {
			oldest = activity
		}
	}
	if !oldest.IsZero() {
		m.scheduleSweepLocked(oldest.Add(m.ttl).Sub(now))
	}
	m.mu.Unlock()

	for _, cb := range staleCbs {
		cb(fmt.Errorf("transfer timed out"))
	}
}

type transferBudget struct {
	limit int
	used  atomic.Int64
}

func newTransferBudget(limit int) *transferBudget {
	if limit <= 0 {
		limit = defaultMaxTransferSize
	}
	return &transferBudget{limit: limit}
}

func (b *transferBudget) reserve(n int) bool {
	if b == nil {
		return true
	}
	for {
		used := b.used.Load()
		if used+int64(n) > int64(b.limit) {
			return false
		}
		if b.used.CompareAndSwap(used, used+int64(n)) {
			return true
		}
	}
}

func (b *transferBudget) release(n int) {
	if b != nil && n > 0 {
		b.used.Add(-int64(n))
	}
}

func validateChunkHeader(totalChunks, totalSize, chunkSize, maxTransferSize int) error {
	switch {
	case chunkSize <= 0 || chunkSize > chunkSizeLimit:
		return fmt.Errorf("invalid chunk size %d", chunkSize)
	case totalSize <= 0 || totalSize > maxTransferSize:
		return fmt.Errorf("invalid transfer size %d, limit %d", totalSize, maxTransferSize)
	case totalChunks != (totalSize-1)/chunkSize+1:
		return fmt.Errorf("%d chunks do not match %d bytes in chunks of %d", totalChunks, totalSize, chunkSize)
	}
	return nil
}

func decodeChunkHeader(hdrs nats.Header, data []byte) (ChunkedTransferHeader, bool) {
	var hdr ChunkedTransferHeader
	if err := Decode(data, &hdr); err != nil {
		return hdr, false
	}
	id := hdrs.Get("x-chunk-id")
	return hdr, id != "" && hdr.TransferID == id
}

func decodeChunk(hdrs nats.Header, data []byte) (ChunkData, error) {
	id := hdrs.Get("x-chunk-id")
	if id == "" {
		return ChunkData{}, errors.New("chunk without transfer id")
	}
	raw := hdrs.Get("x-chunk-index")
	index, ok := parseChunkIndex(raw)
	if !ok {
		return ChunkData{ID: id}, fmt.Errorf("invalid chunk index %q", raw)
	}
	return ChunkData{ID: id, ChunkIndex: index, Data: data}, nil
}

func parseChunkIndex(raw string) (int, bool) {
	// more than 15 digits is beyond an exact float64, never a real index
	if raw == "" || len(raw) > 15 {
		return 0, false
	}
	for i := range len(raw) {
		if raw[i] < '0' || raw[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(raw, 10, 0)
	return int(n), err == nil
}

func payloadTooSmall(limit int) *RPCException {
	return NewRPCException(ErrCodePayloadTooLarge, fmt.Sprintf("Payload limit %d is too small for chunking", limit))
}
