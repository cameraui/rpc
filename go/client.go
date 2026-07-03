package rpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// subscriptionEntry represents one Subscribe/SubscribeQueue call. Entries are
// stable across Suspend()/Connect() cycles so the unsubscribe closure a caller
// holds keeps working after a restore. Multiple subscribers on the same
// pattern coexist — each gets its own entry and its own NATS subscription.
type subscriptionEntry struct {
	pattern string
	queue   string
	handler func(data []byte)
	// rawHandler switches the entry to raw mode: it receives the undecoded
	// *nats.Msg (with headers and subject) instead of decoded payload data.
	// No chunk reassembly — used by the muxed reply inbox, which routes
	// chunks itself. Mutually exclusive with handler.
	rawHandler func(msg *nats.Msg)
	opts       []func(*nats.Subscription)
	sub        *nats.Subscription
}

// Client is the main RPC client that communicates over NATS using MessagePack.
type Client struct {
	Options ClientOptions

	nc              *nats.Conn
	mu              sync.RWMutex
	closed          bool
	maxPayloadSize  int
	chunkingManager *ChunkingManager

	subSeq      int
	subEntries  map[int]*subscriptionEntry
	requestSubs []*nats.Subscription
	subMu       sync.Mutex

	// replyPrefix is the first dot-separated segment of every id this client
	// generates. Equals Options.ConnID when configured (firewall allowlists
	// `rpc.reply.<connId>.>` for such clients), otherwise a local random
	// prefix. All reply subjects derived from those ids therefore fall under
	// one wildcard: `rpc.reply.<replyPrefix>.>` — the muxed reply inbox.
	replyPrefix string

	// muxEntry is the single persistent reply-mux subscription entry
	// (wildcard `rpc.reply.<replyPrefix>.>`). Lives in subEntries so
	// Suspend()/Connect() restore it like any other subscription.
	// Guarded by subMu.
	muxEntry *subscriptionEntry

	// statusHandlers holds 503/no-responder handlers keyed by reply-subject
	// suffix (the part after `rpc.reply.`). Used by pull-iterator/stream
	// paths whose per-message reply subject only ever carries no-responder
	// statuses. One-shot: the mux dispatcher removes an entry when it fires
	// (mirrors the previous per-iterator `AutoUnsubscribe(1)` inboxes).
	statusHandlers sync.Map // map[string]func(error)

	pendingRequests sync.Map // map[string]*pendingRequest
	streamHandlers  sync.Map // map[string]*streamHandler

	// Settle hooks for client-side pull iterators: a consumer parked in a
	// next() wait must be force-settled when the connection tears down,
	// otherwise it hangs forever (streamHandlers get the same treatment).
	pullIteratorSettles sync.Map // map[string]func()

	isolatedClients   []*Client
	isolatedClientsMu sync.Mutex

	pullIteratorCleanups sync.Map // map[string]func()
	callbackCleanups     sync.Map // map[string]func()
}

type pendingRequest struct {
	done chan struct{}
	// cleanup tears down per-call subscriptions. Only the legacy service
	// path (`<subject>.reply.<id>`) still holds any — mux-path calls leave
	// it nil. Idempotent; invoked from the call's own exit path and from
	// Disconnect()/Suspend() so a suspended in-flight call doesn't restore a
	// dead reply subscription on the next Connect().
	cleanup func()
	// subject is the request subject — used for the no-responder error
	// message when a 503 status arrives on the call's muxed reply subject.
	subject string
	result  any
	err     error
}

type streamHandler struct {
	push  func(any)
	end   func()
	error func(error)
}

// NewClient creates a new RPC client with the given options.
func NewClient(opts ClientOptions) *Client { //nolint:gocritic // opts is copied intentionally
	if opts.Timeout == 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.MaxReconnectAttempts == 0 {
		opts.MaxReconnectAttempts = -1
	}
	if opts.ReconnectWait == 0 {
		opts.ReconnectWait = 2 * time.Second
	}
	// With a ConnID the prefix MUST be exactly the ConnID — a server-side
	// firewall may allowlist `rpc.reply.<connId>.>` for such clients.
	replyPrefix := opts.ConnID
	if replyPrefix == "" {
		replyPrefix = GenerateReplyPrefix()
	}
	return &Client{
		Options:         opts,
		maxPayloadSize:  1024 * 1024, // 1MB default
		chunkingManager: NewChunkingManager(),
		subEntries:      make(map[int]*subscriptionEntry),
		replyPrefix:     replyPrefix,
	}
}

// generateID returns a fresh call/iterator/stream id carrying the client's
// replyPrefix as its first dot-separated segment. The reply subject derived
// from such an id (`rpc.reply.<id>` — pure string concatenation, the wire
// contract with every responder implementation) therefore falls under the
// muxed reply inbox wildcard `rpc.reply.<replyPrefix>.>`.
func (c *Client) generateID() string {
	return c.replyPrefix + "." + GenerateID()
}

// IsConnected returns true if the client is connected to NATS.
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nc != nil && c.nc.IsConnected()
}

// IsClosed returns true if the client has been intentionally closed.
func (c *Client) IsClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

// MaxPayloadSize returns the effective maximum payload size.
func (c *Client) MaxPayloadSize() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.maxPayloadSize
}

// SetMaxPayloadSize overrides the maximum payload size (for testing).
func (c *Client) SetMaxPayloadSize(size int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxPayloadSize = size
}

// Connect establishes a connection to the NATS server.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()

	if c.nc != nil && c.nc.IsConnected() {
		c.mu.Unlock()
		return nil
	}

	natsOpts := []nats.Option{
		nats.Name(c.Options.Name),
		nats.ReconnectWait(c.Options.ReconnectWait),
	}

	if c.Options.Reconnect {
		natsOpts = append(natsOpts, nats.MaxReconnects(c.Options.MaxReconnectAttempts))
	} else {
		natsOpts = append(natsOpts, nats.MaxReconnects(0))
	}

	if c.Options.Auth != nil {
		natsOpts = append(natsOpts, nats.UserInfo(c.Options.Auth.User, c.Options.Auth.Password))
	}

	if c.Options.TLS != nil {
		tlsConfig, err := buildTLSConfig(c.Options.TLS)
		if err != nil {
			c.mu.Unlock()
			return fmt.Errorf("tls config: %w", err)
		}
		natsOpts = append(natsOpts, nats.Secure(tlsConfig))
	}

	// When WaitOnFirstConnect is explicitly false, return immediately and
	// reconnect in the background (allows Disconnect() to abort in-flight connects).
	if c.Options.WaitOnFirstConnect != nil && !*c.Options.WaitOnFirstConnect {
		natsOpts = append(natsOpts, nats.RetryOnFailedConnect(true))
	}

	servers := nats.DefaultURL
	if len(c.Options.Servers) > 0 {
		servers = c.Options.Servers[0]
		for i := 1; i < len(c.Options.Servers); i++ {
			servers += "," + c.Options.Servers[i]
		}
	}

	nc, err := nats.Connect(servers, natsOpts...)
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("nats connect: %w", err)
	}

	c.nc = nc

	// A client that was Disconnect()ed is revivable by an explicit Connect().
	// Without this reset, auto-connect in callOnce and the no-responder retry
	// loop stay permanently disabled.
	c.closed = false

	// Auto-detect max_payload from server
	if mp := nc.MaxPayload(); mp > 0 {
		c.maxPayloadSize = int(mp)
	}
	if c.Options.MaxPayloadSize > 0 {
		c.maxPayloadSize = c.Options.MaxPayloadSize
	}
	// Reserve 8KB for NATS protocol overhead and MsgPack envelope per message
	c.maxPayloadSize -= 8192

	c.mu.Unlock()

	// Register the muxed reply inbox (idempotent). Registered as a normal
	// subscription entry so the restore loop below (re-)subscribes it on
	// first connect and after every suspend cycle alike.
	c.ensureMuxSubscription(false)

	// Restore subscriptions from previous session (e.g. after Suspend+Connect).
	// Entries keep their identity so unsubscribe closures held by callers stay
	// valid across the restore.
	c.subMu.Lock()
	for _, entry := range c.subEntries {
		if entry.sub != nil && entry.sub.IsValid() {
			continue
		}
		entry.sub = nil
		// Ignore errors — partial restore is better than none
		_ = c.natsSubscribe(nc, entry)
	}
	c.subMu.Unlock()

	return nil
}

// Disconnect closes the connection and cleans up all resources.
func (c *Client) Disconnect() error {
	c.mu.Lock()
	c.closed = true
	nc := c.nc
	c.mu.Unlock()

	// Reject pending requests. pr.cleanup drops the service path's per-call
	// subscriptions too — otherwise an in-flight call would leak them.
	c.rejectPendingRequests()
	c.clearStatusHandlers()

	// End stream handlers
	c.streamHandlers.Range(func(key, value any) bool {
		sh := value.(*streamHandler)
		sh.end()
		c.streamHandlers.Delete(key)
		return true
	})

	// Force-settle client-side pull iterators parked in a next() wait
	c.settlePullIterators()

	// Cleanup pull iterators
	c.pullIteratorCleanups.Range(func(key, value any) bool {
		if cleanup, ok := value.(func()); ok {
			cleanup()
		}
		c.pullIteratorCleanups.Delete(key)
		return true
	})

	// Cleanup callbacks
	c.callbackCleanups.Range(func(key, value any) bool {
		if cleanup, ok := value.(func()); ok {
			cleanup()
		}
		c.callbackCleanups.Delete(key)
		return true
	})

	// Unsubscribe all
	c.subMu.Lock()
	for _, entry := range c.subEntries {
		if entry.sub != nil {
			_ = entry.sub.Unsubscribe()
		}
	}
	c.subEntries = make(map[int]*subscriptionEntry)
	// Entries were dropped — a revive-Connect() must re-register the mux.
	c.muxEntry = nil
	for _, sub := range c.requestSubs {
		_ = sub.Unsubscribe()
	}
	c.requestSubs = nil
	c.subMu.Unlock()

	// Reset chunking manager
	c.chunkingManager = NewChunkingManager()

	// Disconnect isolated clients
	c.isolatedClientsMu.Lock()
	for _, ic := range c.isolatedClients {
		_ = ic.Disconnect()
	}
	c.isolatedClients = nil
	c.isolatedClientsMu.Unlock()

	// Close NATS connection with timeout
	if nc != nil {
		timeout := c.Options.DisconnectTimeout
		if timeout <= 0 {
			timeout = 2 * time.Second
		}

		done := make(chan struct{})
		go func() {
			nc.Close()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(timeout):
		}
	}

	c.mu.Lock()
	c.nc = nil
	c.mu.Unlock()

	return nil
}

// rejectPendingRequests settles every in-flight call with a connection-closed
// error and runs its cleanup (reply + inbox subscription teardown). Runs on
// Disconnect()/Suspend(). LoadAndDelete guards against a concurrent settle by
// the reply handler — only one side wins.
func (c *Client) rejectPendingRequests() {
	c.pendingRequests.Range(func(key, _ any) bool {
		if v, ok := c.pendingRequests.LoadAndDelete(key); ok {
			pr := v.(*pendingRequest)
			if pr.cleanup != nil {
				pr.cleanup()
			}
			pr.err = NewRPCException(ErrCodeConnectionClosed, "Connection closed")
			close(pr.done)
		}
		return true
	})
}

// clearStatusHandlers drops every registered 503/no-responder status handler.
// Runs on Disconnect()/Suspend() — a torn-down connection can't deliver
// statuses anymore, and the iterator/stream owners are settled separately.
func (c *Client) clearStatusHandlers() {
	c.statusHandlers.Range(func(key, _ any) bool {
		c.statusHandlers.Delete(key)
		return true
	})
}

// settlePullIterators force-settles every client-side pull iterator parked in
// a next() wait. Runs on Disconnect()/Suspend() so consumers terminate with a
// connection error instead of hanging forever.
func (c *Client) settlePullIterators() {
	c.pullIteratorSettles.Range(func(key, value any) bool {
		if settle, ok := value.(func()); ok {
			settle()
		}
		c.pullIteratorSettles.Delete(key)
		return true
	})
}

// Publish sends a message to a subject with automatic chunking for large payloads.
func (c *Client) Publish(subject string, data any) error {
	return c.publishInternal(subject, data, "")
}

func (c *Client) publishInternal(subject string, data any, reply string) error {
	c.mu.RLock()
	nc := c.nc
	maxPayload := c.maxPayloadSize
	c.mu.RUnlock()

	if nc == nil {
		return fmt.Errorf("not connected")
	}

	encoded, release, err := encodeMessagePooled(data)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	// Safe to release when this function returns: nats.go copies the payload
	// synchronously into its flush buffer inside Publish/PublishRequest/
	// PublishMsg (headers included) and never retains the slice. The chunking
	// path below references sub-slices of `encoded` until its last PublishMsg,
	// which this defer runs after. See encodeBufPool for the full invariant.
	defer release()

	// Small enough to send directly
	if len(encoded) <= maxPayload {
		if reply != "" {
			return nc.PublishRequest(subject, reply, encoded)
		}
		return nc.Publish(subject, encoded)
	}

	// Message is too large — chunk it
	transferID := GenerateID()
	totalChunks := (len(encoded) + maxPayload - 1) / maxPayload

	// Send header
	header := ChunkedTransferHeader{
		Type:        "chunked",
		TransferID:  transferID,
		TotalChunks: totalChunks,
		TotalSize:   len(encoded),
		ChunkSize:   maxPayload,
	}

	headerMsg := nats.NewMsg(subject)
	headerEncoded, err := Encode(header)
	if err != nil {
		return fmt.Errorf("encode header: %w", err)
	}
	headerMsg.Data = headerEncoded
	headerMsg.Header = nats.Header{}
	headerMsg.Header.Set("x-chunked-transfer", "header")
	headerMsg.Header.Set("x-chunk-id", transferID)
	if reply != "" {
		headerMsg.Reply = reply
	}
	if err := nc.PublishMsg(headerMsg); err != nil {
		return fmt.Errorf("publish header: %w", err)
	}

	// Send chunks
	chunks := CreateChunks(encoded, transferID, maxPayload)
	for i, chunk := range chunks {
		chunkMsg := nats.NewMsg(subject)
		chunkMsg.Data = chunk.Data
		chunkMsg.Header = nats.Header{}
		chunkMsg.Header.Set("x-chunked-transfer", "chunk")
		chunkMsg.Header.Set("x-chunk-id", transferID)
		chunkMsg.Header.Set("x-chunk-index", strconv.Itoa(i))
		if reply != "" {
			chunkMsg.Reply = reply
		}
		if err := nc.PublishMsg(chunkMsg); err != nil {
			return fmt.Errorf("publish chunk %d: %w", i, err)
		}
	}

	return nil
}

// Subscribe subscribes to a subject pattern. The handler receives decoded MessagePack data.
// Returns an unsubscribe function.
func (c *Client) Subscribe(pattern string, handler func(data []byte), opts ...func(*nats.Subscription)) (func(), error) {
	return c.subscribeEntry(&subscriptionEntry{pattern: pattern, handler: handler, opts: opts})
}

// SubscribeQueue subscribes to a subject with a queue group.
func (c *Client) SubscribeQueue(pattern, queue string, handler func(data []byte)) (func(), error) {
	return c.subscribeEntry(&subscriptionEntry{pattern: pattern, queue: queue, handler: handler})
}

// subscribeEntry registers a subscription entry and creates its NATS
// subscription. The entry survives Suspend()/Connect() cycles; the returned
// unsubscribe closure removes exactly this entry (and only this entry), even
// after the underlying NATS subscription was replaced by a restore.
func (c *Client) subscribeEntry(entry *subscriptionEntry) (func(), error) {
	c.mu.RLock()
	nc := c.nc
	c.mu.RUnlock()

	if nc == nil {
		return nil, fmt.Errorf("not connected")
	}

	if err := c.natsSubscribe(nc, entry); err != nil {
		return nil, err
	}

	c.subMu.Lock()
	c.subSeq++
	key := c.subSeq
	c.subEntries[key] = entry
	c.subMu.Unlock()

	unsub := func() {
		c.subMu.Lock()
		e, ok := c.subEntries[key]
		delete(c.subEntries, key)
		var sub *nats.Subscription
		if ok {
			sub = e.sub
		}
		c.subMu.Unlock()
		if sub != nil {
			_ = sub.Unsubscribe()
		}
	}

	return unsub, nil
}

// natsSubscribe creates the NATS subscription for an entry. Called from
// subscribeEntry and again from Connect() when restoring entries after a
// Suspend cycle.
func (c *Client) natsSubscribe(nc *nats.Conn, entry *subscriptionEntry) error {
	// Raw mode (muxed reply inbox): hand the undecoded *nats.Msg to the
	// handler — it needs headers (503 status, chunk markers) and the subject,
	// and does its own chunk reassembly and routing.
	if entry.rawHandler != nil {
		sub, err := nc.Subscribe(entry.pattern, entry.rawHandler)
		if err != nil {
			return fmt.Errorf("subscribe: %w", err)
		}
		// Every RPC reply of the client shares this one subscription; a burst
		// of large chunked responses would blow through nats.go's default
		// pending limits (64MB) and get dropped as "slow consumer". Lift the
		// limits — memory stays bounded by the client's in-flight calls
		// (Node's mux subscription is unbounded, too).
		_ = sub.SetPendingLimits(-1, -1)
		entry.sub = sub
		return nil
	}

	handler := entry.handler

	msgHandler := func(msg *nats.Msg) {
		chunkType := ""
		if msg.Header != nil {
			chunkType = msg.Header.Get("x-chunked-transfer")
		}

		switch chunkType {
		case "header":
			var hdr ChunkedTransferHeader
			if err := Decode(msg.Data, &hdr); err != nil {
				return
			}
			chunkID := ""
			if msg.Header != nil {
				chunkID = msg.Header.Get("x-chunk-id")
			}
			if chunkID == "" || hdr.TransferID != chunkID {
				return
			}
			c.chunkingManager.StartReceiving(
				hdr.TransferID, hdr.TotalChunks,
				func(data []byte) { handler(data) },
				func(err error) {},
				hdr.TotalSize, hdr.ChunkSize,
			)

		case "chunk":
			chunkID := ""
			chunkIndex := 0
			if msg.Header != nil {
				chunkID = msg.Header.Get("x-chunk-id")
				chunkIndex, _ = strconv.Atoi(msg.Header.Get("x-chunk-index"))
			}
			if chunkID == "" {
				return
			}
			c.chunkingManager.ProcessChunk(ChunkData{
				ID:         chunkID,
				ChunkIndex: chunkIndex,
				Data:       msg.Data,
			})

		default:
			// Regular message — pass raw bytes to handler
			handler(msg.Data)
		}
	}

	var sub *nats.Subscription
	var err error
	if entry.queue != "" {
		sub, err = nc.QueueSubscribe(entry.pattern, entry.queue, msgHandler)
		if err != nil {
			return fmt.Errorf("queue subscribe: %w", err)
		}
		// Ensure subscription is registered on the server
		_ = nc.Flush()
	} else {
		sub, err = nc.Subscribe(entry.pattern, msgHandler)
		if err != nil {
			return fmt.Errorf("subscribe: %w", err)
		}
	}

	for _, opt := range entry.opts {
		opt(sub)
	}

	entry.sub = sub
	return nil
}

// ensureMuxSubscription registers (and, with subscribeNow, subscribes) the
// single persistent wildcard subscription `rpc.reply.<replyPrefix>.>` that
// receives every RPC reply of this client — real responses, chunked responses
// and 503/no-responder statuses. Idempotent.
//
// Connect() passes subscribeNow=false: it only registers the entry and lets
// its restore loop create the actual subscription alongside all other entries
// (also after suspend cycles).
func (c *Client) ensureMuxSubscription(subscribeNow bool) {
	c.mu.RLock()
	nc := c.nc
	c.mu.RUnlock()

	c.subMu.Lock()
	defer c.subMu.Unlock()

	if c.muxEntry == nil {
		c.muxEntry = &subscriptionEntry{
			pattern:    "rpc.reply." + c.replyPrefix + ".>",
			rawHandler: c.handleMuxMessage,
		}
		c.subSeq++
		c.subEntries[c.subSeq] = c.muxEntry
	}
	if subscribeNow && nc != nil && (c.muxEntry.sub == nil || !c.muxEntry.sub.IsValid()) {
		// Ignore errors — the call path will surface a timeout; consistent
		// with the Connect() restore loop.
		_ = c.natsSubscribe(nc, c.muxEntry)
	}
}

// handleMuxMessage dispatches a message from the muxed reply inbox by kind:
//   - 503/no-responder status (empty payload + Status header 503): the
//     call/iterator is identified by the SUBJECT (`rpc.reply.<suffix>`) — the
//     server echoes the request's reply subject, there is no payload to decode.
//   - chunked transfer header/chunk: reassemble via chunkingManager, then
//     route the assembled response by envelope id.
//   - regular response: decode and route by envelope id.
func (c *Client) handleMuxMessage(msg *nats.Msg) {
	// No-responder status. Wire detail: the reply subject of an RPC call is
	// exactly `rpc.reply.<call id>`, so the suffix IS the call id. For
	// iterator/stream status inboxes the suffix is the registered token.
	if len(msg.Data) == 0 && msg.Header != nil && msg.Header.Get("Status") == "503" {
		suffix := strings.TrimPrefix(msg.Subject, "rpc.reply.")

		// One-shot (mirrors the previous per-iterator AutoUnsubscribe(1) inboxes).
		if v, ok := c.statusHandlers.LoadAndDelete(suffix); ok {
			if fn, ok := v.(func(error)); ok {
				fn(NewRPCException(ErrCodeNotFound, "No responders for "+suffix))
			}
			return
		}

		if v, ok := c.pendingRequests.LoadAndDelete(suffix); ok {
			pr := v.(*pendingRequest)
			subject := pr.subject
			if subject == "" {
				subject = suffix
			}
			pr.err = NewRPCException(ErrCodeNotFound, "No responders for "+subject)
			close(pr.done)
		}
		return
	}

	chunkType := ""
	if msg.Header != nil {
		chunkType = msg.Header.Get("x-chunked-transfer")
	}

	switch chunkType {
	case "header":
		var hdr ChunkedTransferHeader
		if err := Decode(msg.Data, &hdr); err != nil {
			return
		}
		chunkID := msg.Header.Get("x-chunk-id")
		if chunkID == "" || hdr.TransferID != chunkID {
			return
		}
		c.chunkingManager.StartReceiving(
			hdr.TransferID, hdr.TotalChunks,
			func(data []byte) { c.routeMuxResponse(data) },
			func(err error) {},
			hdr.TotalSize, hdr.ChunkSize,
		)
	case "chunk":
		chunkID := msg.Header.Get("x-chunk-id")
		chunkIndex, _ := strconv.Atoi(msg.Header.Get("x-chunk-index"))
		if chunkID == "" {
			return
		}
		c.chunkingManager.ProcessChunk(ChunkData{
			ID:         chunkID,
			ChunkIndex: chunkIndex,
			Data:       msg.Data,
		})
	default:
		c.routeMuxResponse(msg.Data)
	}
}

// routeMuxResponse settles the pending request a (possibly reassembled) RPC
// response belongs to. Unknown ids are dropped silently — late replies after
// a timeout, or traffic of another client sharing the same ConnID prefix.
func (c *Client) routeMuxResponse(data []byte) {
	var resp RPCResponse
	if err := DecodeMessageInto(data, &resp); err != nil {
		return
	}
	if resp.ID == "" {
		return
	}

	v, ok := c.pendingRequests.LoadAndDelete(resp.ID)
	if !ok {
		return
	}
	pr := v.(*pendingRequest)
	if resp.Error != nil {
		pr.err = RPCExceptionFromError(resp.Error)
	} else {
		pr.result = resp.Result
	}
	close(pr.done)
}

// Request sends a native NATS request/reply, with automatic retry on no-responder errors.
func (c *Client) Request(ctx context.Context, subject string, data any, timeout ...time.Duration) ([]byte, error) {
	var result []byte
	err := c.withNoResponderRetry(ctx, nil, func() error {
		var reqErr error
		result, reqErr = c.requestOnce(ctx, subject, data, timeout...)
		return reqErr
	})
	return result, err
}

// RequestWithOptions is the same as Request but with per-call overrides for
// timeout and no-responder retry. Pass nil to use client-wide defaults.
func (c *Client) RequestWithOptions(ctx context.Context, subject string, data any, opts *RequestOptions) ([]byte, error) {
	var timeout []time.Duration
	var override *NoResponderRetryOptions
	if opts != nil {
		if opts.Timeout > 0 {
			timeout = []time.Duration{opts.Timeout}
		}
		override = opts.NoResponderRetry
	}

	var result []byte
	err := c.withNoResponderRetry(ctx, override, func() error {
		var reqErr error
		result, reqErr = c.requestOnce(ctx, subject, data, timeout...)
		return reqErr
	})
	return result, err
}

func (c *Client) requestOnce(ctx context.Context, subject string, data any, timeout ...time.Duration) ([]byte, error) {
	c.mu.RLock()
	nc := c.nc
	c.mu.RUnlock()

	if nc == nil {
		return nil, fmt.Errorf("not connected")
	}

	t := 5 * time.Second
	if len(timeout) > 0 {
		t = timeout[0]
	}

	// nc.Request copies the payload into the connection's flush buffer before
	// returning, so releasing the pooled buffer afterwards is safe.
	encoded, release, err := encodeMessagePooled(data)
	if err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	defer release()

	msg, err := nc.Request(subject, encoded, t)
	if err != nil {
		if err == nats.ErrNoResponders {
			return nil, NewRPCException(ErrCodeNotFound, "No responders available")
		}
		if err == nats.ErrTimeout || ctx.Err() != nil {
			return nil, NewRPCException(ErrCodeTimeout, fmt.Sprintf("Request to %q timed out after %v", subject, t))
		}
		return nil, err
	}

	// Check for service error headers
	if msg.Header != nil && msg.Header.Get("Nats-Service-Error-Code") != "" {
		errCode := msg.Header.Get("Nats-Service-Error-Code")
		errMsg := msg.Header.Get("Nats-Service-Error")
		if errMsg == "" {
			errMsg = "Service error"
		}
		return nil, NewRPCException(errCode, errMsg)
	}

	return msg.Data, nil
}

// OnRequest registers a handler for native NATS request/reply on the given pattern.
func (c *Client) OnRequest(pattern string, handler func(data []byte) (any, error)) (func(), error) {
	c.mu.RLock()
	nc := c.nc
	c.mu.RUnlock()

	if nc == nil {
		return nil, fmt.Errorf("not connected")
	}

	sub, err := nc.Subscribe(pattern, func(msg *nats.Msg) {
		result, err := handler(msg.Data)
		if msg.Reply == "" {
			return
		}
		if err != nil {
			var errResp struct {
				Error string `msgpack:"error"`
				Code  string `msgpack:"code"`
			}
			if rpcErr, ok := err.(*RPCException); ok {
				errResp.Code = rpcErr.Code
				errResp.Error = rpcErr.Msg
			} else {
				errResp.Code = ErrCodeInternalError
				errResp.Error = err.Error()
			}
			respData, _ := Encode(errResp)
			_ = nc.Publish(msg.Reply, respData)
			return
		}
		respData, encErr := EncodeMessage(result)
		if encErr != nil {
			return
		}
		_ = nc.Publish(msg.Reply, respData)
	})
	if err != nil {
		return nil, err
	}
	// Ensure subscription is registered on the server
	_ = nc.Flush()

	c.subMu.Lock()
	c.requestSubs = append(c.requestSubs, sub)
	c.subMu.Unlock()

	return func() {
		_ = sub.Unsubscribe()
		c.subMu.Lock()
		for i, s := range c.requestSubs {
			if s == sub {
				c.requestSubs = append(c.requestSubs[:i], c.requestSubs[i+1:]...)
				break
			}
		}
		c.subMu.Unlock()
	}, nil
}

// Call makes an RPC call and waits for a response, with automatic retry on no-responder errors.
func (c *Client) Call(ctx context.Context, subject string, args ...any) (any, error) {
	var result any
	err := c.withNoResponderRetry(ctx, nil, func() error {
		var callErr error
		result, callErr = c.callOnce(ctx, subject, args...)
		return callErr
	})
	return result, err
}

// CallWithOptions is the same as Call but with per-call overrides for
// no-responder retry. Pass nil to use client-wide defaults. (Timeout on
// RequestOptions is currently honored only by RequestWithOptions; Call uses
// the client-wide timeout.)
func (c *Client) CallWithOptions(ctx context.Context, subject string, opts *RequestOptions, args ...any) (any, error) {
	var override *NoResponderRetryOptions
	if opts != nil {
		override = opts.NoResponderRetry
	}

	var result any
	err := c.withNoResponderRetry(ctx, override, func() error {
		var callErr error
		result, callErr = c.callOnce(ctx, subject, args...)
		return callErr
	})
	return result, err
}

// callOnce performs a single RPC call attempt via the muxed reply inbox: the
// reply subject is `rpc.reply.<id>` and falls under the client's persistent
// wildcard subscription — no per-call subscriptions. Responses (plain or
// chunked) and no-responder statuses all arrive on the mux, which settles the
// pendingRequests entry; cleanup only has to drop the map entry.
func (c *Client) callOnce(ctx context.Context, subject string, args ...any) (any, error) {
	if !c.IsConnected() && !c.IsClosed() {
		if err := c.Connect(ctx); err != nil {
			return nil, err
		}
	}

	c.mu.RLock()
	nc := c.nc
	c.mu.RUnlock()
	if nc == nil {
		return nil, fmt.Errorf("not connected")
	}

	// Service calls (`<subject>.reply.<id>`) keep the legacy per-call
	// subscription flow — separate refactor later.
	if !strings.HasPrefix(subject, "rpc.") {
		return c.callOnceService(ctx, nc, subject, args...)
	}

	// Normally established by Connect(); covers clients whose connection was
	// wired up out-of-band (tests). No-op when already subscribed.
	c.ensureMuxSubscription(true)

	id := c.generateID()
	timeout := c.Options.Timeout
	// The reply subject is derived from the id by pure string concatenation —
	// this is the wire contract with every responder implementation (Node,
	// Go, Python): they publish the response to `rpc.reply.<msg.ID>` and
	// treat the id as opaque. Because the id starts with our replyPrefix,
	// the muxed reply inbox (`rpc.reply.<replyPrefix>.>`) catches it.
	replySubject := "rpc.reply." + id

	pr := &pendingRequest{done: make(chan struct{}), subject: subject}
	c.pendingRequests.Store(id, pr)

	// Send request. `reply` is set to the call's own reply subject so the
	// NATS server delivers a no-responder 503 status to the SAME subject the
	// real response would use — the mux catches both.
	message := RPCMessage{
		ID:     id,
		Method: "call",
		Params: args,
	}
	if err := c.publishInternal(subject, message, replySubject); err != nil {
		c.pendingRequests.Delete(id)
		return nil, err
	}

	// Wait for response or timeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-pr.done:
		return pr.result, pr.err
	case <-timer.C:
		c.pendingRequests.Delete(id)
		return nil, NewRPCException(ErrCodeTimeout, fmt.Sprintf("RPC call to %q timed out after %v", subject, timeout))
	case <-ctx.Done():
		c.pendingRequests.Delete(id)
		return nil, NewRPCException(ErrCodeTimeout, fmt.Sprintf("RPC call to %q context cancelled: %v", subject, ctx.Err()))
	}
}

// callOnceService is the legacy single-attempt call for service subjects
// (reply pattern `<subject>.reply.<id>`): per-call reply subscription plus a
// one-shot no-responder inbox. The rpc.* path is muxed — see callOnce.
func (c *Client) callOnceService(ctx context.Context, nc *nats.Conn, subject string, args ...any) (any, error) {
	id := c.generateID()
	timeout := c.Options.Timeout
	replySubject := subject + ".reply." + id

	// Subscribe to reply
	unsub, err := c.Subscribe(replySubject, func(data []byte) {
		var resp RPCResponse
		if err := DecodeMessageInto(data, &resp); err != nil {
			return
		}
		if resp.ID != id {
			return
		}

		if v, ok := c.pendingRequests.LoadAndDelete(resp.ID); ok {
			pending := v.(*pendingRequest)
			if resp.Error != nil {
				pending.err = RPCExceptionFromError(resp.Error)
			} else {
				pending.result = resp.Result
			}
			close(pending.done)
		}
	})
	if err != nil {
		return nil, err
	}

	inbox := nc.NewInbox()
	noRespSub, err := nc.Subscribe(inbox, func(msg *nats.Msg) {
		if len(msg.Data) == 0 && msg.Header != nil && msg.Header.Get("Status") == "503" {
			if v, ok := c.pendingRequests.LoadAndDelete(id); ok {
				pending := v.(*pendingRequest)
				pending.err = NewRPCException(ErrCodeNotFound, "No responders for "+subject)
				close(pending.done)
			}
		}
	})
	if err != nil {
		unsub()
		return nil, err
	}
	_ = noRespSub.AutoUnsubscribe(1)

	// Tear down the reply + inbox subscriptions exactly once. Runs on every
	// exit path of this call AND from Disconnect()/Suspend() via pr.cleanup —
	// a timed-out or suspended call must not leak its subscriptions (or have
	// a dead rpc.reply.* entry restored on the next Connect()).
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			unsub()
			_ = noRespSub.Unsubscribe()
		})
	}
	defer cleanup()

	pr := &pendingRequest{done: make(chan struct{}), cleanup: cleanup}
	c.pendingRequests.Store(id, pr)

	// Build and send the RPC message
	message := RPCMessage{
		ID:     id,
		Method: "call",
		Params: args,
	}

	if err := c.publishInternal(subject, message, inbox); err != nil {
		c.pendingRequests.Delete(id)
		return nil, err
	}

	// Wait for response or timeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-pr.done:
		return pr.result, pr.err
	case <-timer.C:
		c.pendingRequests.Delete(id)
		return nil, NewRPCException(ErrCodeTimeout, fmt.Sprintf("RPC call to %q timed out after %v", subject, timeout))
	case <-ctx.Done():
		c.pendingRequests.Delete(id)
		return nil, NewRPCException(ErrCodeTimeout, fmt.Sprintf("RPC call to %q context cancelled: %v", subject, ctx.Err()))
	}
}

// withNoResponderRetry retries fn on no-responder errors. The optional
// override (per-call) takes precedence over the client-wide default — useful
// for letting a single Request or Call extend the retry window when targeting
// a known-flaky responder without affecting the rest of the client's traffic.
func (c *Client) withNoResponderRetry(ctx context.Context, override *NoResponderRetryOptions, fn func() error) error {
	maxRetries := 3
	delays := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second}

	src := c.Options.NoResponderRetry
	if override != nil {
		src = override
	}

	if src != nil {
		if src.MaxRetries > 0 {
			maxRetries = src.MaxRetries
		}
		if len(src.Delays) > 0 {
			delays = src.Delays
		}
	}

	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if !isNoResponderError(err) || attempt >= maxRetries || c.IsClosed() {
			return err
		}
		delay := delays[min(attempt, len(delays)-1)]
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

func isNoResponderError(err error) bool {
	if errors.Is(err, nats.ErrNoResponders) {
		return true
	}
	var rpcErr *RPCException
	if errors.As(err, &rpcErr) && rpcErr.Code == ErrCodeNotFound {
		return true
	}
	return false
}

// Suspend closes the connection and cleans up resources but preserves subscription
// metadata so that a subsequent Connect() restores them automatically.
// Unlike Disconnect(), Suspend() does NOT mark the client as closed.
func (c *Client) Suspend() error {
	c.mu.Lock()
	nc := c.nc
	c.mu.Unlock()

	// Reject pending requests. RPC calls are muxed (no per-call
	// subscription); service-path calls still hold per-call subscriptions
	// which pr.cleanup drops — otherwise a suspended in-flight call would be
	// restored as a dead reply subscription on the next Connect().
	c.rejectPendingRequests()
	c.clearStatusHandlers()

	// End stream handlers
	c.streamHandlers.Range(func(key, value any) bool {
		sh := value.(*streamHandler)
		sh.end()
		c.streamHandlers.Delete(key)
		return true
	})

	// Force-settle client-side pull iterators parked in a next() wait
	c.settlePullIterators()

	// Cleanup pull iterators
	c.pullIteratorCleanups.Range(func(key, value any) bool {
		if cleanup, ok := value.(func()); ok {
			cleanup()
		}
		c.pullIteratorCleanups.Delete(key)
		return true
	})

	// Cleanup callbacks
	c.callbackCleanups.Range(func(key, value any) bool {
		if cleanup, ok := value.(func()); ok {
			cleanup()
		}
		c.callbackCleanups.Delete(key)
		return true
	})

	// Unsubscribe all subscriptions but keep the entries — Connect() restores
	// them on the fresh transport.
	c.subMu.Lock()
	for _, entry := range c.subEntries {
		if entry.sub != nil {
			_ = entry.sub.Unsubscribe()
			entry.sub = nil
		}
	}
	for _, sub := range c.requestSubs {
		_ = sub.Unsubscribe()
	}
	c.requestSubs = nil
	c.subMu.Unlock()

	// Reset chunking manager
	c.chunkingManager = NewChunkingManager()

	// Disconnect isolated clients
	c.isolatedClientsMu.Lock()
	for _, ic := range c.isolatedClients {
		_ = ic.Disconnect()
	}
	c.isolatedClients = nil
	c.isolatedClientsMu.Unlock()

	// Close NATS connection with timeout
	if nc != nil {
		timeout := c.Options.DisconnectTimeout
		if timeout <= 0 {
			timeout = 2 * time.Second
		}

		done := make(chan struct{})
		go func() {
			nc.Close()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(timeout):
		}
	}

	c.mu.Lock()
	c.nc = nil
	c.mu.Unlock()

	return nil
}

// ReconfigureOptions selectively overrides connection options for the next Connect().
type ReconfigureOptions struct {
	// Servers, when non-nil, replaces Options.Servers.
	Servers []string
	// Auth, when non-nil, replaces Options.Auth. Pass &AuthOptions{} to clear.
	Auth *AuthOptions
}

// Reconfigure updates connection options between Suspend() and Connect(). Used
// for token rotation / endpoint switching: subscription metadata is preserved
// by Suspend(), Reconfigure() points the next Connect() at the new server, and
// Connect() re-subscribes everything on the fresh transport.
func (c *Client) Reconfigure(overrides ReconfigureOptions) error {
	c.mu.Lock()
	if c.nc != nil && c.nc.IsConnected() {
		c.mu.Unlock()
		return errors.New("cannot reconfigure while connected; call Suspend() first")
	}
	if overrides.Servers != nil {
		c.Options.Servers = overrides.Servers
	}
	if overrides.Auth != nil {
		c.Options.Auth = overrides.Auth
	}
	c.mu.Unlock()

	c.isolatedClientsMu.Lock()
	children := make([]*Client, len(c.isolatedClients))
	copy(children, c.isolatedClients)
	c.isolatedClientsMu.Unlock()

	for _, child := range children {
		if err := child.Reconfigure(overrides); err != nil {
			return err
		}
	}
	return nil
}

// createIsolatedClient creates a new client with the same options but a different name.
func (c *Client) createIsolatedClient(suffix string) *Client {
	opts := c.Options
	opts.Name = c.Options.Name + "-" + suffix
	return NewClient(opts)
}

// removeIsolatedClient removes a client from the tracked isolated clients list.
func (c *Client) removeIsolatedClient(client *Client) {
	c.isolatedClientsMu.Lock()
	defer c.isolatedClientsMu.Unlock()
	for i, ic := range c.isolatedClients {
		if ic == client {
			c.isolatedClients = append(c.isolatedClients[:i], c.isolatedClients[i+1:]...)
			return
		}
	}
}

func buildTLSConfig(opts *TLSOptions) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load cert/key: %w", err)
	}

	caCert, err := os.ReadFile(opts.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read CA: %w", err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}, nil
}
