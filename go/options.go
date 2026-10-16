package rpc

import (
	"fmt"
	"maps"
	"time"
)

// NoResponderRetryOptions configures retry behavior for 503/no-responder errors.
// Only a no-responder status this client sees itself is retried, never an
// error a responder sent back. With neither field set a call retries 6 times
// after 25ms, 50ms, 100ms, 325ms, 1s and 2s, so a responder that is just
// subscribing is reached quickly and a missing one fails after about 3.5s.
type NoResponderRetryOptions struct {
	// MaxRetries is the maximum number of retry attempts (default: 3 when
	// Delays is set). A negative value turns the retry off, 0 keeps the default.
	MaxRetries int
	// Delays is the delay before each retry attempt, the last one repeats
	// (default: [500ms, 1s, 2s] when MaxRetries is set).
	Delays []time.Duration
}

// RequestOptions overrides client-wide defaults for a single Request / Call.
// Useful when one specific responder is known to be flaky (e.g. a child
// process that may be restarting) and needs a longer retry window than the
// rest of the client's traffic.
type RequestOptions struct {
	// Timeout overrides the client-wide default timeout for this call.
	// Zero value means use client default.
	Timeout time.Duration
	// NoResponderRetry overrides the client-wide retry config for this call.
	// nil means use client default.
	NoResponderRetry *NoResponderRetryOptions
}

// ClientOptions configures the RPC client.
type ClientOptions struct {
	// Servers is a list of NATS server URLs.
	Servers []string

	// Name identifies this client.
	Name string

	// ConnID scopes the client's reply subjects: when set, it MUST be the
	// first dot-separated segment of every generated id (a server-side
	// firewall can then allowlist `rpc.reply.<connId>.>`). When empty, a
	// random local prefix is generated instead. See Client.replyPrefix.
	ConnID string

	// Auth contains optional authentication credentials.
	Auth *AuthOptions

	// Timeout is the default RPC call timeout (default: 30s).
	Timeout time.Duration

	// Reconnect enables automatic reconnection. When nil or true, the client
	// reconnects after the connection drops. Default: true.
	Reconnect *bool

	// MaxReconnectAttempts is the maximum number of reconnection attempts (-1 for infinite, default: -1).
	MaxReconnectAttempts int

	// ReconnectWait is the delay between reconnection attempts (default: 2s),
	// plus up to 100 ms of jitter (1 s with TLS). The first attempt after a
	// connection that lived at least ReconnectWait runs at once.
	ReconnectWait time.Duration

	// ConnectTimeout bounds a single connection attempt. nats.go defaults to 2s,
	// which a host name resolving to a dead address first can use up entirely
	// before the working one is tried (default: 10s).
	ConnectTimeout time.Duration

	// TLS contains optional TLS configuration.
	TLS *TLSOptions

	// MaxPayloadSize caps the size of one message below the server's
	// max_payload, a larger message goes out in chunks. A value above the
	// server's limit is ignored. Set it when messages cross a leaf link to a
	// server with a lower limit (default: the server's max_payload). 8 KiB of
	// the limit is kept free for headers, the rest is the chunk size. Connect
	// refuses a value below 16384, so chunks stay at 8 KiB or more. With a
	// server limit below 16384 the client sends messages up to that limit
	// whole and fails a larger one with PAYLOAD_TOO_LARGE.
	MaxPayloadSize int

	// WriteBufferSize is the nats.go write buffer of the connection. A publish
	// that fits is written by the background flusher together with others, a
	// larger one is written to the socket inside Publish (default: 1 MiB). A
	// negative value keeps the nats.go default of 32 KiB.
	WriteBufferSize int

	// MaxTransferSize caps the size of a chunked message this client accepts.
	// A larger transfer is dropped before any buffer is allocated (default: 1 GiB).
	// It also caps the bytes that all chunked messages still being reassembled
	// by this client hold together; a transfer that would go over it is dropped
	// with its first chunk.
	MaxTransferSize int

	// WaitOnFirstConnect blocks Connect() until the first connection succeeds.
	// When nil or true, Connect() blocks. When false, Connect() returns immediately
	// and reconnects in the background — useful when Disconnect() must be able to
	// abort an in-flight connection attempt (e.g. browser URL switchover).
	// Default: true.
	WaitOnFirstConnect *bool

	// DisconnectTimeout is the maximum time to wait for the NATS connection
	// to fully close during Disconnect(). Default: 2s.
	DisconnectTimeout time.Duration

	// DrainTimeout bounds how long Disconnect, Suspend and the cleanup of
	// RegisterHandler let calls finish that a handler already received. The
	// handler's subscriptions are removed on the server first, so new calls go
	// to another instance or see no responders; calls still unanswered at the
	// deadline get a CONNECTION_CLOSED error. Default: 250ms. A negative value
	// turns the drain off: the subscriptions are removed at once, as before.
	DrainTimeout time.Duration

	// NoResponderRetry configures retry for 503/no-responder errors.
	// When nil, the defaults of NoResponderRetryOptions apply.
	NoResponderRetry *NoResponderRetryOptions

	// OnAsyncError receives the errors nats.go reports outside of a call, most
	// often a subscription that dropped messages because its handler fell
	// behind (nats.ErrSlowConsumer). Without it they go to stderr. It runs on
	// a goroutine of the connection and must return quickly.
	OnAsyncError func(err *AsyncError)

	// OrphanProbe sets how the handlers of this client find out that the
	// consumer of a callback, stream or pull iterator session went away
	// without cancelling it. When nil, the probe runs with its defaults.
	// WithOrphanReaping overrides it per RegisterHandler.
	OrphanProbe *OrphanProbeOptions
}

// OrphanProbeOptions configures the probe that ends a callback, stream or pull
// iterator session whose consumer is gone, for example a closed browser tab.
// At most once per Interval a session asks the NATS server whether anyone
// still receives its messages: it marks a data message it sends anyway, or
// sends a ping the consumer ignores while it has no data. When the server
// keeps answering that nobody does for longer than Grace, the session ends as
// if the consumer had cancelled it. A consumer that comes back within Grace
// keeps its session.
type OrphanProbeOptions struct {
	// Interval is the shortest time between two probes of a session (default: 30s, also for 0 or less).
	Interval time.Duration
	// Grace is how long the probes must find no consumer before the session ends (default: 5m, also for 0 or less).
	Grace time.Duration
	// Disabled turns the probe off, a session then runs until it is cancelled
	// or its handler, service or connection goes away.
	Disabled bool
}

// AsyncError is an error nats.go reports outside of a call.
type AsyncError struct {
	// Err is the error, nats.ErrSlowConsumer for dropped messages.
	Err error
	// Subject is the subject of the subscription, empty for an error of the connection.
	Subject string
	// Dropped is how many messages the subscription dropped so far.
	Dropped int
}

// AuthOptions contains NATS authentication credentials.
type AuthOptions struct {
	User     string
	Password string
}

// TLSOptions contains TLS configuration for the NATS connection.
type TLSOptions struct {
	CertFile string
	KeyFile  string
	CAFile   string
}

// HandlerOption configures handler registration.
type HandlerOption func(*handlerConfig)

type handlerConfig struct {
	isolatedConnection    bool
	withoutDecorators     bool
	queue                 string
	recoverPanics         bool
	concurrency           map[string]int
	orphanProbe           *OrphanProbeOptions
	namespaceSubscription bool
	pendingMsgs           int
	pendingBytes          int
	dropExpiredAll        bool
	dropExpired           map[string]bool
}

// Error describes the error with its subscription and dropped messages.
func (e *AsyncError) Error() string {
	switch {
	case e.Subject == "":
		return e.Err.Error()
	case e.Dropped > 0:
		return fmt.Sprintf("%v for subscription on %q, %d dropped", e.Err, e.Subject, e.Dropped)
	default:
		return fmt.Sprintf("%v for subscription on %q", e.Err, e.Subject)
	}
}

// Unwrap returns Err, so errors.Is(err, nats.ErrSlowConsumer) works.
func (e *AsyncError) Unwrap() error {
	return e.Err
}

func (c *handlerConfig) dropsExpired(method string) bool {
	return c.dropExpiredAll || c.dropExpired[method]
}

// WithIsolatedConnection creates a separate NATS connection for this handler.
func WithIsolatedConnection() HandlerOption {
	return func(c *handlerConfig) {
		c.isolatedConnection = true
	}
}

// WithoutDecorators disables decorator-based method extraction.
func WithoutDecorators() HandlerOption {
	return func(c *handlerConfig) {
		c.withoutDecorators = true
	}
}

// WithQueue sets the queue group for handler subscriptions.
func WithQueue(queue string) HandlerOption {
	return func(c *handlerConfig) {
		c.queue = queue
	}
}

// WithPanicRecovery keeps the process running after a handler panics. Without
// it the caller gets an INTERNAL_ERROR reply and the panic is raised again, so
// the process still crashes and restarts with clean state.
func WithPanicRecovery() HandlerOption {
	return func(c *handlerConfig) {
		c.recoverPanics = true
	}
}

// WithMethodConcurrency lets the listed methods (wire name to the most calls in
// flight) serve calls at once, see RPCMethodConcurrency. It overrides limits
// the handler returns from RPCConcurrency, a limit below 2 makes a method
// serial again.
func WithMethodConcurrency(limits map[string]int) HandlerOption {
	return func(c *handlerConfig) {
		if c.concurrency == nil {
			c.concurrency = make(map[string]int, len(limits))
		}
		maps.Copy(c.concurrency, limits)
	}
}

// WithNamespaceSubscription serves the namespace through one subscription on
// rpc.<namespace>.> instead of one per method, which saves memory and setup
// time for handlers with many methods. Each method still serves its calls one
// at a time in arrival order (or as WithMethodConcurrency allows), and methods
// do not wait for each other. A call for a method the handler does not have
// gets a METHOD_NOT_FOUND error at once instead of a no-responder error. The
// subscription catches every subject below the namespace, so no other
// process may serve a nested namespace or the same namespace without it.
// Calls are handed to a goroutine per method, which costs some throughput on
// a method under heavy concurrent load.
func WithNamespaceSubscription() HandlerOption {
	return func(c *handlerConfig) {
		c.namespaceSubscription = true
	}
}

// WithPendingLimits caps the messages and bytes each method of the handler
// holds while it is busy, in place of the nats.go defaults of 500,000 messages
// and 64 MiB. A request over the limit is dropped, its caller times out, and
// ClientOptions.OnAsyncError gets a nats.ErrSlowConsumer for the method's
// subject. A negative value lifts that limit.
func WithPendingLimits(msgs, bytes int) HandlerOption {
	return func(c *handlerConfig) {
		c.pendingMsgs = msgs
		c.pendingBytes = bytes
	}
}

// WithDropExpired skips a call of the listed methods, or of every method
// without a list, that waited longer for its turn than the caller waits for
// the reply (the __ttl callers send, plus 5ms). The caller has given up by
// then, so the handler does not run and nothing is answered. Use it for reads
// whose result is useless once late, never for a call whose side effect must
// happen. Calls from callers that do not send __ttl always run. Calls of these
// methods are handed to a goroutine per method, which costs some throughput
// on a method under heavy concurrent load.
//
// Example:
//
//	client.RegisterHandler("nvr", h, rpc.WithDropExpired("getEventThumbnails"))
func WithDropExpired(methods ...string) HandlerOption {
	return func(c *handlerConfig) {
		if len(methods) == 0 {
			c.dropExpiredAll = true
			return
		}
		if c.dropExpired == nil {
			c.dropExpired = make(map[string]bool, len(methods))
		}
		for _, m := range methods {
			c.dropExpired[m] = true
		}
	}
}

// WithOrphanReaping sets the orphan probe for the sessions of this handler,
// in place of ClientOptions.OrphanProbe. Zero durations keep the defaults.
//
// Example:
//
//	client.RegisterHandler("nvr", h, rpc.WithOrphanReaping(rpc.OrphanProbeOptions{Grace: time.Minute}))
func WithOrphanReaping(opts OrphanProbeOptions) HandlerOption {
	return func(c *handlerConfig) {
		c.orphanProbe = &opts
	}
}
