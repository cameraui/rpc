package rpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

const registerFlushTimeout = 2 * time.Second

var (
	defaultRetryDelays = []time.Duration{
		25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond,
		325 * time.Millisecond, time.Second, 2 * time.Second,
	}
	legacyRetryDelays = []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second}
)

// subscriptionEntry represents one Subscribe/SubscribeQueue call. Entries are
// stable across Suspend()/Connect() cycles so the unsubscribe closure a caller
// holds keeps working after a restore. Multiple subscribers on the same
// pattern coexist — each gets its own entry and its own NATS subscription.
type subscriptionEntry struct {
	pattern string
	queue   string
	handler func(data []byte)
	// routed replaces handler for a wildcard pattern whose handler needs the subject
	routed func(subject string, data []byte)
	// rawHandler switches the entry to raw mode: it receives the undecoded
	// *nats.Msg (with headers and subject) instead of decoded payload data.
	// No chunk reassembly — used by the muxed reply inbox, which routes
	// chunks itself. Mutually exclusive with handler.
	rawHandler func(msg *nats.Msg)
	opts       []func(*nats.Subscription)
	sub        *nats.Subscription
	// two subscriptions matching one publish both see the same transfer id,
	// so each assembles its own copy; queue members of one client share a
	// scope, since the server spreads one transfer across them
	chunks *chunkScope
	// the method a subject of a namespace subscription goes to, "" for one its
	// handler does not serve; each method gets transfers of its own, as with a
	// subscription per method
	chunkLane func(subject string) string
}

type chunkScope struct {
	manager atomic.Pointer[ChunkingManager]
	// queue scopes only, guarded by Client.subMu
	key  string
	refs int

	lanesMu sync.Mutex
	lanes   map[string]*ChunkingManager
}

// Client is the main RPC client that communicates over NATS using MessagePack.
type Client struct {
	Options ClientOptions

	nc              *nats.Conn
	mu              sync.RWMutex
	closed          bool
	maxPayloadSize  int
	chunkingManager *ChunkingManager
	chunkBudget     *transferBudget
	// the payload limit is below minPayloadLimit: maxPayloadSize is that limit
	// and a larger message fails instead of going out in chunks
	unchunked bool
	// the connection Suspend or Disconnect is closing, its closed handler is ignored
	closingConn *nats.Conn
	// nats.go closed the connection for good, the next Connect replaces it and
	// runs the OnReconnect handlers
	connLost bool
	// when the current connection came up, it decides how soon the first reconnect runs
	connectedAt atomic.Pointer[time.Time]

	subSeq         int
	subEntries     map[int]*subscriptionEntry
	queueChunks    map[string]*chunkScope
	requestSubs    []*nats.Subscription
	endpointChunks map[*ChunkingManager]struct{}
	handlerRegs    map[*handlerRegistration]struct{}
	subMu          sync.Mutex

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
	streamCleanups       sync.Map // map[string]func()

	reconnectMu       sync.Mutex
	reconnectSeq      int
	reconnectHandlers map[int]func()
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
	// the mux routes by reply subject and hands the reply over undecoded, the
	// caller decodes it
	raw []byte
	// set on the mux goroutine, read by the call when it gives up
	transferKey atomic.Pointer[string]
}

type streamHandler struct {
	push  func(any)
	end   func()
	error func(error)
	// the request subject of the stream start
	subject string
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
	if opts.ConnectTimeout == 0 {
		opts.ConnectTimeout = 10 * time.Second
	}
	if opts.WriteBufferSize == 0 {
		opts.WriteBufferSize = 1 << 20
	}
	// With a ConnID the prefix MUST be exactly the ConnID — a server-side
	// firewall may allowlist `rpc.reply.<connId>.>` for such clients.
	replyPrefix := opts.ConnID
	if replyPrefix == "" {
		replyPrefix = GenerateReplyPrefix()
	}
	budget := newTransferBudget(opts.MaxTransferSize)
	replies := newChunkingManager(opts.MaxTransferSize, budget)
	// one transfer per pending call bounds the reply mux, not the transfer cap
	replies.maxTransfers = 0
	return &Client{
		Options:         opts,
		maxPayloadSize:  1024 * 1024, // 1MB default
		chunkingManager: replies,
		chunkBudget:     budget,
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

func (c *Client) needsConnect() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return !c.closed && (c.nc == nil || c.nc.IsClosed())
}

// MaxPayloadSize returns the effective maximum payload size: the largest
// message sent whole. A larger one goes out in chunks of this size, or fails
// with PAYLOAD_TOO_LARGE when the server's limit is below 16384.
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
	c.unchunked = false
}

// Connect establishes a connection to the NATS server. It returns at once
// while the current connection is up or nats.go is reconnecting it. It
// refuses a ClientOptions.MaxPayloadSize below 16384.
func (c *Client) Connect(ctx context.Context) error {
	if opt := c.Options.MaxPayloadSize; opt != 0 && opt < minPayloadLimit {
		return fmt.Errorf("MaxPayloadSize %d is below the minimum of %d", opt, minPayloadLimit)
	}

	c.mu.RLock()
	prev := c.nc
	c.mu.RUnlock()
	// nats.go runs the closed handler on its own goroutine, it may still be pending
	if prev != nil && prev.IsClosed() {
		c.onConnClosed(prev)
	}

	c.mu.Lock()

	// a connection that nats.go is still reconnecting keeps its
	// subscriptions and buffers publishes; dialing a second one would split
	// the client and leak the first
	if c.nc != nil && !c.nc.IsClosed() {
		c.mu.Unlock()
		return nil
	}

	natsOpts := []nats.Option{
		nats.Name(c.Options.Name),
		nats.Timeout(c.Options.ConnectTimeout),
		nats.ReconnectWait(c.Options.ReconnectWait),
		nats.CustomReconnectDelay(c.reconnectDelay),
		nats.ConnectHandler(func(*nats.Conn) { c.markConnected() }),
		nats.ReconnectHandler(func(*nats.Conn) {
			c.markConnected()
			c.notifyReconnect()
		}),
		nats.ClosedHandler(c.onConnClosed),
		nats.ErrorHandler(c.asyncError),
		nats.MaxReconnects(c.maxReconnects()),
		nats.WriteBufferSize(c.Options.WriteBufferSize),
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
	if nc.IsConnected() {
		c.markConnected()
	}

	// A client that was Disconnect()ed is revivable by an explicit Connect().
	// Without this reset, auto-connect in callOnce and the no-responder retry
	// loop stay permanently disabled.
	c.closed = false
	replaced := c.connLost
	c.connLost = false

	// the option only lowers the server's limit: a larger message would be
	// refused, a smaller cap keeps messages fit for a leaf link with a lower one
	limit := 1 << 20
	serverMax := int(nc.MaxPayload())
	if serverMax > 0 {
		limit = serverMax
	}
	if opt := c.Options.MaxPayloadSize; opt > 0 && (serverMax <= 0 || opt < serverMax) {
		limit = opt
	}
	c.unchunked = limit < minPayloadLimit
	c.maxPayloadSize = limit
	if !c.unchunked {
		c.maxPayloadSize -= payloadReserve
	}

	c.mu.Unlock()

	// Register the muxed reply inbox (idempotent). Registered as a normal
	// subscription entry so the restore loop below (re-)subscribes it on
	// first connect and after every suspend cycle alike.
	c.ensureMuxSubscription(false)

	// Restore subscriptions from previous session (e.g. after Suspend+Connect).
	// Entries keep their identity so unsubscribe closures held by callers stay
	// valid across the restore.
	restored := 0
	c.subMu.Lock()
	for _, entry := range c.subEntries {
		if entry.sub != nil && entry.sub.IsValid() {
			continue
		}
		entry.sub = nil
		entry.chunks.close()
		// Ignore errors — partial restore is better than none
		_ = c.natsSubscribe(nc, entry)
		if entry != c.muxEntry {
			restored++
		}
	}
	c.subMu.Unlock()

	// once the PONG is back the server holds every restored subscription, so a
	// caller on another connection reaches them as soon as Connect returns
	if restored > 0 {
		c.flushTimeout(registerFlushTimeout)
	}

	// Suspend took the isolated connections down with this one; a failed child
	// stays down, the parent connection is up either way
	for _, child := range c.isolatedChildren() {
		if !child.IsClosed() {
			_ = child.Connect(ctx)
		}
	}

	if replaced {
		c.notifyReconnect()
	}
	return nil
}

// Disconnect closes the connection and cleans up all resources. Calls the
// registered handlers already received get until ClientOptions.DrainTimeout to
// finish first.
func (c *Client) Disconnect() error {
	c.drainHandlers(c.handlerRegistrations())

	c.mu.Lock()
	c.closed = true
	nc := c.nc
	c.closingConn = nc
	c.connLost = false
	c.mu.Unlock()

	c.endSession()

	// Unsubscribe all
	c.subMu.Lock()
	for _, entry := range c.subEntries {
		if entry.sub != nil {
			_ = entry.sub.Unsubscribe()
		}
		entry.chunks.close()
	}
	c.subEntries = make(map[int]*subscriptionEntry)
	c.queueChunks = nil
	for r := range c.handlerRegs {
		r.lanes.close()
	}
	c.handlerRegs = nil
	// Entries were dropped — a revive-Connect() must re-register the mux.
	c.muxEntry = nil
	for _, sub := range c.requestSubs {
		_ = sub.Unsubscribe()
	}
	c.requestSubs = nil
	c.closeEndpointChunksLocked()
	c.subMu.Unlock()

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
	c.closingConn = nil
	c.mu.Unlock()

	return nil
}

// nats.go gave the connection up for good: entries and scopes stay and closed
// stays false, so the next call or Connect dials again and restores them
func (c *Client) onConnClosed(nc *nats.Conn) {
	c.mu.Lock()
	if c.nc != nc || c.closingConn == nc {
		c.mu.Unlock()
		return
	}
	c.nc = nil
	c.connLost = true
	// under mu, so a concurrent Connect cannot restore an entry in between
	c.subMu.Lock()
	for _, entry := range c.subEntries {
		entry.sub = nil
		entry.chunks.close()
	}
	c.requestSubs = nil
	c.closeEndpointChunksLocked()
	c.subMu.Unlock()
	c.mu.Unlock()

	c.endSession()
}

func (c *Client) endSession() {
	c.rejectPendingRequests()
	c.clearStatusHandlers()

	c.streamHandlers.Range(func(key, value any) bool {
		sh := value.(*streamHandler)
		sh.end()
		c.streamHandlers.Delete(key)
		return true
	})

	c.settlePullIterators()
	c.endHandlerSessions()
	c.chunkingManager.Close()
}

// LoadAndDelete, so an unregister at the same time cannot end a session twice
func (c *Client) endHandlerSessions() {
	for _, cleanups := range []*sync.Map{&c.streamCleanups, &c.pullIteratorCleanups, &c.callbackCleanups} {
		cleanups.Range(func(key, _ any) bool {
			if v, ok := cleanups.LoadAndDelete(key); ok {
				if cleanup, ok := v.(func()); ok {
					cleanup()
				}
			}
			return true
		})
	}
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
	unchunked := c.unchunked
	c.mu.RUnlock()

	if nc == nil {
		return fmt.Errorf("not connected")
	}

	frame, err := encodeFramePooled(data)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	// nats.go copies every payload before the publish returns, see encodeBufPool
	defer frame.release()
	return publishFrame(nc, subject, reply, &frame, maxPayload, unchunked)
}

// Subscribe subscribes to a subject pattern. The handler receives raw wire
// bytes (chunked transfers arrive reassembled) — decode them with
// DecodeMessageInto, NOT Decode: publishers extract large binaries into CUIB
// frames, which plain Decode cannot parse. Returns an unsubscribe function.
func (c *Client) Subscribe(pattern string, handler func(data []byte), opts ...func(*nats.Subscription)) (func(), error) {
	return c.subscribeEntry(&subscriptionEntry{pattern: pattern, handler: handler, opts: opts})
}

// SubscribeQueue subscribes to a subject with a queue group. See Subscribe
// for the handler's decode contract (DecodeMessageInto, not Decode). It
// returns once the server holds the subscription.
func (c *Client) SubscribeQueue(pattern, queue string, handler func(data []byte)) (func(), error) {
	unsub, err := c.subscribeEntry(&subscriptionEntry{pattern: pattern, queue: queue, handler: handler})
	if err == nil {
		c.flushTimeout(registerFlushTimeout)
	}
	return unsub, err
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

	c.subMu.Lock()
	c.joinChunkScopeLocked(entry)
	c.subMu.Unlock()

	if err := c.natsSubscribe(nc, entry); err != nil {
		c.subMu.Lock()
		c.leaveChunkScopeLocked(entry)
		c.subMu.Unlock()
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
		if ok {
			c.subMu.Lock()
			c.leaveChunkScopeLocked(e)
			c.subMu.Unlock()
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

	msgHandler := c.entryHandler(entry)

	var sub *nats.Subscription
	var err error
	if entry.queue != "" {
		sub, err = nc.QueueSubscribe(entry.pattern, entry.queue, msgHandler)
		if err != nil {
			return fmt.Errorf("queue subscribe: %w", err)
		}
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

func (c *Client) entryHandler(entry *subscriptionEntry) nats.MsgHandler {
	handler := entry.handler
	routed := entry.routed
	scope := entry.chunks
	laneOf := entry.chunkLane
	lane := func(subject string) string {
		if laneOf == nil {
			return ""
		}
		return laneOf(subject)
	}
	deliver := func(subject string, data []byte) {
		if routed != nil {
			routed(subject, data)
		} else {
			handler(data)
		}
	}
	return func(msg *nats.Msg) {
		chunkType := ""
		if msg.Header != nil {
			chunkType = msg.Header.Get("x-chunked-transfer")
		}

		switch chunkType {
		case "header":
			hdr, ok := decodeChunkHeader(msg.Header, msg.Data)
			if !ok {
				return
			}
			subject := msg.Subject
			scope.loadLane(lane(subject), c.Options.MaxTransferSize, c.chunkBudget).StartReceiving(
				hdr.TransferID, hdr.TotalChunks,
				func(data []byte) { deliver(subject, data) },
				func(err error) {},
				hdr.TotalSize, hdr.ChunkSize,
			)

		case "chunk":
			cm := scope.loaded(lane(msg.Subject))
			if cm == nil {
				return
			}
			chunk, err := decodeChunk(msg.Header, msg.Data)
			if err != nil {
				// a chunk that does not fit drops its transfer, an unknown id is ignored
				cm.fail(chunk.ID, err)
				return
			}
			cm.ProcessChunk(chunk)

		default:
			deliver(msg.Subject, msg.Data)
		}
	}
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
//   - chunked transfer header/chunk: reassemble via chunkingManager while the
//     call is pending, then route the assembled response by envelope id. A
//     reply that cannot be assembled rejects the call at once.
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
			pr.err = newLocalNoResponders(subject, "No responders for "+subject)
			close(pr.done)
		}
		return
	}

	chunkType := ""
	if msg.Header != nil {
		chunkType = msg.Header.Get("x-chunked-transfer")
	}

	switch chunkType {
	case "header", "chunk":
		// the subject suffix is the call id; without a pending call nobody reads the reply
		callID := strings.TrimPrefix(msg.Subject, "rpc.reply.")
		v, ok := c.pendingRequests.Load(callID)
		if !ok {
			return
		}
		pr := v.(*pendingRequest)
		if chunkType == "chunk" {
			chunk, err := decodeChunk(msg.Header, msg.Data)
			chunk.ID = callID + "|" + chunk.ID
			if err != nil {
				c.chunkingManager.fail(chunk.ID, err)
				return
			}
			c.chunkingManager.ProcessChunk(chunk)
			return
		}
		hdr, ok := decodeChunkHeader(msg.Header, msg.Data)
		if !ok {
			return
		}
		key := callID + "|" + hdr.TransferID
		// one transfer per call keeps the uncapped manager bounded
		if prev := pr.transferKey.Swap(&key); prev != nil && *prev != key {
			c.chunkingManager.discard(*prev)
		}
		c.chunkingManager.StartReceiving(
			key, hdr.TotalChunks,
			func(data []byte) { c.settleReply(callID, data) },
			func(err error) { c.failChunkedReply(callID, err) },
			hdr.TotalSize, hdr.ChunkSize,
		)
	default:
		c.settleReply(strings.TrimPrefix(msg.Subject, "rpc.reply."), msg.Data)
	}
}

func (c *Client) failChunkedReply(callID string, err error) {
	v, ok := c.pendingRequests.LoadAndDelete(callID)
	if !ok {
		return
	}
	pr := v.(*pendingRequest)
	if key := pr.transferKey.Load(); key != nil {
		c.chunkingManager.discard(*key)
	}
	subject := pr.subject
	if subject == "" {
		subject = callID
	}
	pr.err = NewRPCException(ErrCodeInternalError, fmt.Sprintf("Chunked reply to %q failed: %v", subject, err))
	close(pr.done)
}

// replies come on rpc.reply.<call id>, so a reply nobody waits for is dropped undecoded
func (c *Client) settleReply(callID string, data []byte) {
	v, ok := c.pendingRequests.LoadAndDelete(callID)
	if !ok {
		return
	}
	pr := v.(*pendingRequest)
	if key := pr.transferKey.Load(); key != nil {
		c.chunkingManager.discard(*key)
	}
	pr.raw = data
	close(pr.done)
}

// Request sends a native NATS request/reply, with automatic retry on no-responder errors.
func (c *Client) Request(ctx context.Context, subject string, data any, timeout ...time.Duration) ([]byte, error) {
	var result []byte
	err := c.withNoResponderRetry(ctx, nil, methodProbe{}, func() error {
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
	err := c.withNoResponderRetry(ctx, override, methodProbe{}, func() error {
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
	} else if deadline, ok := ctx.Deadline(); ok {
		t = time.Until(deadline)
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
			return nil, newLocalNoResponders(subject, "No responders available")
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

func (c *Client) flushTimeout(timeout time.Duration) {
	c.mu.RLock()
	nc := c.nc
	c.mu.RUnlock()
	if nc != nil && nc.IsConnected() {
		_ = nc.FlushTimeout(timeout)
	}
}

// OnReconnect registers fn to run after the connection is re-established.
// Subscriptions survive a reconnect, but messages published while the link was
// down are lost, so a consumer that caches server state uses this to resync.
// It also runs when a call or Connect dials a new connection because nats.go
// gave the previous one up for good (reconnect attempts used up, reconnect
// off), not after Suspend or Disconnect. Returns a function that unregisters
// the handler.
//
// Handlers run on their own goroutine, so a slow one does not stall the NATS
// reconnect path, and a panicking one does not take the process down.
func (c *Client) OnReconnect(fn func()) func() {
	c.reconnectMu.Lock()
	if c.reconnectHandlers == nil {
		c.reconnectHandlers = make(map[int]func())
	}
	c.reconnectSeq++
	id := c.reconnectSeq
	c.reconnectHandlers[id] = fn
	c.reconnectMu.Unlock()

	return func() {
		c.reconnectMu.Lock()
		delete(c.reconnectHandlers, id)
		c.reconnectMu.Unlock()
	}
}

func (c *Client) notifyReconnect() {
	c.reconnectMu.Lock()
	handlers := make([]func(), 0, len(c.reconnectHandlers))
	for _, fn := range c.reconnectHandlers {
		handlers = append(handlers, fn)
	}
	c.reconnectMu.Unlock()

	for _, fn := range handlers {
		go func() {
			defer func() { _ = recover() }()
			fn()
		}()
	}
}

func (c *Client) asyncError(_ *nats.Conn, sub *nats.Subscription, err error) {
	if subject, ok := refusedPublishSubject(err); ok {
		c.failRefusedPublish(subject)
	}
	e := &AsyncError{Err: err}
	if sub != nil {
		e.Subject = sub.Subject
		if errors.Is(err, nats.ErrSlowConsumer) {
			e.Dropped, _ = sub.Dropped()
		}
	}
	c.reportAsyncError(e)
}

// nats-server refused a publish of this connection: the calls and stream
// starts waiting on that subject would only run into their timeout, nothing
// retries a refused subject
func (c *Client) failRefusedPublish(subject string) {
	c.pendingRequests.Range(func(key, v any) bool {
		if pr := v.(*pendingRequest); pr.subject == subject {
			if _, ok := c.pendingRequests.LoadAndDelete(key); ok {
				if transfer := pr.transferKey.Load(); transfer != nil {
					c.chunkingManager.discard(*transfer)
				}
				pr.err = forbiddenException(subject)
				close(pr.done)
			}
		}
		return true
	})
	c.streamHandlers.Range(func(key, v any) bool {
		if sh := v.(*streamHandler); sh.subject == subject {
			if _, ok := c.streamHandlers.LoadAndDelete(key); ok {
				sh.error(forbiddenException(subject))
			}
		}
		return true
	})
}

func (c *Client) reportAsyncError(e *AsyncError) {
	if hook := c.Options.OnAsyncError; hook != nil {
		hook(e)
		return
	}
	fmt.Fprintln(os.Stderr, e.Error())
}

// a lane dropped a request, reported the way nats.go reports a subscription
// over its pending limits
func (c *Client) slowConsumer(subject string, dropped int) {
	c.reportAsyncError(&AsyncError{Err: nats.ErrSlowConsumer, Subject: subject, Dropped: dropped})
}

func (c *Client) maxReconnects() int {
	if c.Options.Reconnect != nil && !*c.Options.Reconnect {
		return 0
	}
	return c.Options.MaxReconnectAttempts
}

func (c *Client) markConnected() {
	now := time.Now()
	c.connectedAt.Store(&now)
}

// replaces ReconnectWait and its jitter in nats.go: a connection that lived at
// least ReconnectWait tries again at once, one that keeps dropping waits, so a
// client kicked as a slow consumer does not loop
func (c *Client) reconnectDelay(attempts int) time.Duration {
	wait := c.Options.ReconnectWait
	if at := c.connectedAt.Load(); attempts <= 1 && at != nil && time.Since(*at) >= wait {
		return rand.N(10 * time.Millisecond)
	}
	jitter := 100 * time.Millisecond
	if c.Options.TLS != nil {
		jitter = time.Second
	}
	return wait + rand.N(jitter)
}

// OnRequest registers a handler for native NATS request/reply on the given
// pattern. The handler receives raw wire bytes — decode them with
// DecodeMessageInto, NOT Decode (see Subscribe).
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
// While nats.go reconnects, the request waits in its reconnect buffer and goes
// out once the connection is back; the timeout still applies.
func (c *Client) Call(ctx context.Context, subject string, args ...any) (any, error) {
	var result any
	err := c.withNoResponderRetry(ctx, nil, methodProbe{}, func() error {
		var callErr error
		result, callErr = c.callOnce(ctx, subject, 0, args...)
		return callErr
	})
	return result, err
}

// CallWithOptions is the same as Call but with per-call overrides for timeout
// and no-responder retry. Pass nil to use client-wide defaults.
func (c *Client) CallWithOptions(ctx context.Context, subject string, opts *RequestOptions, args ...any) (any, error) {
	return c.callWithOptions(ctx, subject, opts, methodProbe{}, args...)
}

// a set probe asks the namespace for its methods after the first no-responder
func (c *Client) callWithOptions(ctx context.Context, subject string, opts *RequestOptions, probe methodProbe, args ...any) (any, error) {
	var override *NoResponderRetryOptions
	var timeout time.Duration
	if opts != nil {
		override = opts.NoResponderRetry
		timeout = opts.Timeout
	}

	var result any
	err := c.withNoResponderRetry(ctx, override, probe, func() error {
		var callErr error
		result, callErr = c.callOnce(ctx, subject, timeout, args...)
		return callErr
	})
	return result, err
}

// callDeadline resolves how long a call may take: an explicit per-call
// timeout, else what is left of the caller's deadline, else the client
// default. A caller that grants more time than the default (a plugin waiting
// for a slow model) must not be cut short by the transport.
func (c *Client) callDeadline(ctx context.Context, timeout time.Duration) time.Duration {
	if timeout > 0 {
		return timeout
	}
	if deadline, ok := ctx.Deadline(); ok {
		return time.Until(deadline)
	}
	return c.Options.Timeout
}

// callOnce performs a single RPC call attempt via the muxed reply inbox: the
// reply subject is `rpc.reply.<id>` and falls under the client's persistent
// wildcard subscription — no per-call subscriptions. Responses (plain or
// chunked) and no-responder statuses all arrive on the mux, which settles the
// pendingRequests entry; cleanup only has to drop the map entry.
func (c *Client) callOnce(ctx context.Context, subject string, timeout time.Duration, args ...any) (any, error) {
	return c.callOnceTo(ctx, subject, "", timeout, args...)
}

// direct is the per-instance subject of a service that takes a chunked request there
func (c *Client) callOnceTo(ctx context.Context, subject, direct string, timeout time.Duration, args ...any) (any, error) {
	if c.needsConnect() {
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
		return c.callOnceService(ctx, nc, subject, direct, timeout, args...)
	}

	// Normally established by Connect(); covers clients whose connection was
	// wired up out-of-band (tests). No-op when already subscribed.
	c.ensureMuxSubscription(true)

	id := c.generateID()
	timeout = c.callDeadline(ctx, timeout)
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
		TTL:    callTTL(ctx, timeout),
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
		return pr.outcome(id)
	case <-timer.C:
		c.abandonCall(id, pr)
		return nil, NewRPCException(ErrCodeTimeout, fmt.Sprintf("RPC call to %q timed out after %v", subject, timeout))
	case <-ctx.Done():
		c.abandonCall(id, pr)
		return nil, NewRPCException(ErrCodeTimeout, fmt.Sprintf("RPC call to %q context cancelled: %v", subject, ctx.Err()))
	}
}

// a reply that does not decode, or names another call, fails the call at once
func (pr *pendingRequest) outcome(id string) (any, error) {
	if pr.err != nil {
		return nil, pr.err
	}
	var resp RPCResponse
	err := DecodeMessageInto(pr.raw, &resp)
	if err == nil && resp.ID != id {
		err = fmt.Errorf("reply carries id %q", resp.ID)
	}
	if err != nil {
		return nil, NewRPCException(ErrCodeInternalError, fmt.Sprintf("Reply to %q failed: %v", pr.subject, err))
	}
	if resp.Error != nil {
		return nil, RPCExceptionFromError(resp.Error)
	}
	return resp.Result, nil
}

func (c *Client) abandonCall(id string, pr *pendingRequest) {
	c.pendingRequests.Delete(id)
	if key := pr.transferKey.Load(); key != nil {
		c.chunkingManager.discard(*key)
	}
}

// callOnceService is the legacy single-attempt call for service subjects
// (reply pattern `<subject>.reply.<id>`): per-call reply subscription plus a
// one-shot no-responder inbox. The rpc.* path is muxed — see callOnce.
func (c *Client) callOnceService(ctx context.Context, nc *nats.Conn, subject, direct string, timeout time.Duration, args ...any) (any, error) {
	id := c.generateID()
	timeout = c.callDeadline(ctx, timeout)

	frame, err := encodeFramePooled(RPCMessage{ID: id, Method: "call", Params: args})
	if err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	// once the publish returned, or when the call ends before it
	release := sync.OnceFunc(frame.release)
	defer release()
	c.mu.RLock()
	maxPayload := c.maxPayloadSize
	unchunked := c.unchunked
	c.mu.RUnlock()
	// the queue of a service with several instances would spread the chunks of
	// one request over them, the instance itself takes them on its own subject
	target := subject
	if direct != "" && frame.size > maxPayload {
		target = direct
	}
	replySubject := target + ".reply." + id

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
				pending.err = newLocalNoResponders(subject, "No responders for "+subject)
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

	pr := &pendingRequest{done: make(chan struct{}), cleanup: cleanup, subject: target}
	c.pendingRequests.Store(id, pr)

	err = publishFrame(nc, target, inbox, &frame, maxPayload, unchunked)
	release()
	if err != nil {
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

// a per-call override replaces the client-wide retry config; a set probe starts
// with the first delay and ends the retry in any wait once the method is missing
func (c *Client) withNoResponderRetry(ctx context.Context, override *NoResponderRetryOptions, probe methodProbe, fn func() error) error {
	err := fn()
	if err == nil {
		return nil
	}
	return c.retryNoResponders(ctx, override, probe, fn, err)
}

// apart from withNoResponderRetry, so the probe's defer costs a call that
// succeeds at once nothing
func (c *Client) retryNoResponders(ctx context.Context, override *NoResponderRetryOptions, probe methodProbe, fn func() error, err error) error {
	maxRetries, delays := c.retryPlan(override)
	var gone <-chan struct{}

	for attempt := 0; ; attempt++ {
		var local *localNoResponders
		if !errors.As(err, &local) {
			return err
		}
		if attempt >= maxRetries || c.IsClosed() {
			return local.err
		}
		delay := delays[min(attempt, len(delays)-1)]
		if attempt == 0 && probe.proxy != nil {
			var stop context.CancelFunc
			gone, stop = probe.start(ctx, delay)
			defer stop()
		}
		select {
		case <-gone:
			return local.err
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		if err = fn(); err == nil {
			return nil
		}
	}
}

// the short first steps catch a responder that is just subscribing, the
// budget until the error stays about 3.5 s; a caller that set either field
// keeps the old plan for the other
func (c *Client) retryPlan(override *NoResponderRetryOptions) (maxRetries int, delays []time.Duration) {
	src := c.Options.NoResponderRetry
	if override != nil {
		src = override
	}
	if src == nil || (src.MaxRetries == 0 && len(src.Delays) == 0) {
		return len(defaultRetryDelays), defaultRetryDelays
	}
	maxRetries, delays = 3, legacyRetryDelays
	if src.MaxRetries > 0 {
		maxRetries = src.MaxRetries
	} else if src.MaxRetries < 0 {
		maxRetries = 0
	}
	if len(src.Delays) > 0 {
		delays = src.Delays
	}
	return maxRetries, delays
}

// Suspend closes the connection and cleans up resources but preserves subscription
// metadata so that a subsequent Connect() restores them automatically.
// Unlike Disconnect(), Suspend() does NOT mark the client as closed. Calls the
// registered handlers already received are drained as in Disconnect. The
// isolated connections of handlers, proxies and channels are suspended too and
// come back with Connect.
func (c *Client) Suspend() error {
	regs := c.handlerRegistrations()
	c.drainHandlers(regs)

	c.mu.Lock()
	nc := c.nc
	c.closingConn = nc
	c.connLost = false
	c.mu.Unlock()

	c.endSession()

	// Unsubscribe all subscriptions but keep the entries — Connect() restores
	// them on the fresh transport.
	c.subMu.Lock()
	for _, entry := range c.subEntries {
		if entry.sub != nil {
			_ = entry.sub.Unsubscribe()
			entry.sub = nil
		}
		entry.chunks.close()
	}
	for _, sub := range c.requestSubs {
		_ = sub.Unsubscribe()
	}
	c.requestSubs = nil
	c.closeEndpointChunksLocked()
	c.subMu.Unlock()

	// isolated clients keep their subscriptions too, the next Connect restores them
	for _, ic := range c.isolatedChildren() {
		_ = ic.Suspend()
	}

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
	c.closingConn = nil
	c.mu.Unlock()

	// the closed connection delivers nothing more, so the restored subscriptions serve again
	for _, r := range regs {
		r.resume()
	}
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
	if c.nc != nil && !c.nc.IsClosed() {
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

	for _, child := range c.isolatedChildren() {
		if err := child.Reconfigure(overrides); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) isolatedChildren() []*Client {
	c.isolatedClientsMu.Lock()
	defer c.isolatedClientsMu.Unlock()
	return slices.Clone(c.isolatedClients)
}

// createIsolatedClient creates a new client with the same options but a different name.
func (c *Client) createIsolatedClient(suffix string) *Client {
	opts := c.Options
	opts.Name = c.Options.Name + "-" + suffix
	return NewClient(opts)
}

func (c *Client) releaseIsolated(child *Client) error {
	err := child.Disconnect()
	c.removeIsolatedClient(child)
	return err
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

func (c *Client) joinChunkScopeLocked(entry *subscriptionEntry) {
	if entry.queue == "" {
		entry.chunks = &chunkScope{}
		return
	}
	key := entry.pattern + "\x00" + entry.queue
	scope := c.queueChunks[key]
	if scope == nil {
		if c.queueChunks == nil {
			c.queueChunks = make(map[string]*chunkScope)
		}
		scope = &chunkScope{key: key}
		c.queueChunks[key] = scope
	}
	scope.refs++
	entry.chunks = scope
}

func (c *Client) leaveChunkScopeLocked(entry *subscriptionEntry) {
	scope := entry.chunks
	if scope == nil {
		return
	}
	if scope.key != "" {
		scope.refs--
		if scope.refs > 0 {
			return
		}
		if c.queueChunks[scope.key] == scope {
			delete(c.queueChunks, scope.key)
		}
	}
	scope.close()
}

// an endpoint's transfers end with the connection its service was added to
func (c *Client) addEndpointChunks(cm *ChunkingManager) {
	c.subMu.Lock()
	if c.endpointChunks == nil {
		c.endpointChunks = make(map[*ChunkingManager]struct{})
	}
	c.endpointChunks[cm] = struct{}{}
	c.subMu.Unlock()
}

func (c *Client) removeEndpointChunks(cm *ChunkingManager) {
	c.subMu.Lock()
	delete(c.endpointChunks, cm)
	c.subMu.Unlock()
}

func (c *Client) closeEndpointChunksLocked() {
	for cm := range c.endpointChunks {
		cm.Close()
	}
	clear(c.endpointChunks)
}

func (s *chunkScope) load(maxTransferSize int, budget *transferBudget) *ChunkingManager {
	for {
		if cm := s.manager.Load(); cm != nil {
			return cm
		}
		s.manager.CompareAndSwap(nil, newChunkingManager(maxTransferSize, budget))
	}
}

// "" is the scope's own manager
func (s *chunkScope) loadLane(lane string, maxTransferSize int, budget *transferBudget) *ChunkingManager {
	if lane == "" {
		return s.load(maxTransferSize, budget)
	}
	s.lanesMu.Lock()
	defer s.lanesMu.Unlock()
	cm := s.lanes[lane]
	if cm == nil {
		if s.lanes == nil {
			s.lanes = make(map[string]*ChunkingManager)
		}
		cm = newChunkingManager(maxTransferSize, budget)
		s.lanes[lane] = cm
	}
	return cm
}

func (s *chunkScope) loaded(lane string) *ChunkingManager {
	if lane == "" {
		return s.manager.Load()
	}
	s.lanesMu.Lock()
	defer s.lanesMu.Unlock()
	return s.lanes[lane]
}

func (s *chunkScope) close() {
	if s == nil {
		return
	}
	if cm := s.manager.Swap(nil); cm != nil {
		cm.Close()
	}
	s.lanesMu.Lock()
	lanes := s.lanes
	s.lanes = nil
	s.lanesMu.Unlock()
	for _, cm := range lanes {
		cm.Close()
	}
}

// a set per-call timeout wins in callDeadline, yet the call still ends at the
// ctx deadline when that comes first
func callTTL(ctx context.Context, timeout time.Duration) Milliseconds {
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(timeout, time.Until(deadline))
	}
	return Milliseconds(max(1, (timeout+time.Millisecond-1)/time.Millisecond))
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

func forbiddenException(subject string) *RPCException {
	return NewRPCException(ErrCodeForbidden, fmt.Sprintf("Permission denied for %q", subject), map[string]any{"subject": subject})
}

// nats.go reports a refused publish as ErrPermissionViolation with the server's text
func refusedPublishSubject(err error) (string, bool) {
	if !errors.Is(err, nats.ErrPermissionViolation) {
		return "", false
	}
	const marker = `Publish to "`
	text := err.Error()
	_, after, ok0 := strings.Cut(text, marker)
	if !ok0 {
		return "", false
	}
	subject, _, ok := strings.Cut(after, `"`)
	return subject, ok && subject != ""
}
