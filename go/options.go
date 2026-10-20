package rpc

import (
	"fmt"
	"maps"
	"time"
)

// NoResponderRetryOptions configures the retry of no-responder errors this client sees itself.
type NoResponderRetryOptions struct {
	// default 3 when Delays is set, a negative value turns the retry off
	MaxRetries int
	// the last one repeats, default [500ms, 1s, 2s] when MaxRetries is set
	Delays []time.Duration
}

// RequestOptions overrides client-wide defaults for a single Request or Call.
type RequestOptions struct {
	// zero keeps the client default
	Timeout time.Duration
	// nil keeps the client default
	NoResponderRetry *NoResponderRetryOptions
}

// ClientOptions configures the RPC client.
type ClientOptions struct {
	Servers []string

	Name string

	// first segment of every generated id, so a firewall can allowlist rpc.reply.<connId>.>; random when empty
	ConnID string

	Auth *AuthOptions

	// default 30s
	Timeout time.Duration

	// default true
	Reconnect *bool

	// -1 for infinite, default -1
	MaxReconnectAttempts int

	// default 2s plus up to 100ms jitter (1s with TLS)
	ReconnectWait time.Duration

	// default 10s, nats.go's 2s can be used up by a dead address resolved first
	ConnectTimeout time.Duration

	TLS *TLSOptions

	// default the server's max_payload, at least 16384; 8 KiB stay free for headers, a larger message goes out in chunks
	MaxPayloadSize int

	// default 1 MiB, a negative value keeps the nats.go default of 32 KiB
	WriteBufferSize int

	// default 1 GiB, per transfer and for all transfers in progress together
	MaxTransferSize int

	// default true, false returns at once and connects in the background
	WaitOnFirstConnect *bool

	// default 2s
	DisconnectTimeout time.Duration

	// default 250ms, a negative value removes the subscriptions at once
	DrainTimeout time.Duration

	// nil uses the NoResponderRetryOptions defaults
	NoResponderRetry *NoResponderRetryOptions

	// runs on a connection goroutine and must return quickly, without it errors go to stderr
	OnAsyncError func(err *AsyncError)

	// nil uses the defaults, WithOrphanReaping overrides it per handler
	OrphanProbe *OrphanProbeOptions
}

// OrphanProbeOptions configures the probe that ends a session whose consumer is gone, for example a closed browser tab.
type OrphanProbeOptions struct {
	// default 30s, also for 0 or less
	Interval time.Duration
	// default 5m, also for 0 or less
	Grace    time.Duration
	Disabled bool
}

// AsyncError is an error nats.go reports outside of a call.
type AsyncError struct {
	// nats.ErrSlowConsumer for dropped messages
	Err error
	// empty for an error of the connection
	Subject string
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

// WithPanicRecovery keeps the process running after a handler panics instead of raising the panic again.
func WithPanicRecovery() HandlerOption {
	return func(c *handlerConfig) {
		c.recoverPanics = true
	}
}

// WithMethodConcurrency lets the listed methods serve up to the given number of calls at once, overriding RPCConcurrency.
func WithMethodConcurrency(limits map[string]int) HandlerOption {
	return func(c *handlerConfig) {
		if c.concurrency == nil {
			c.concurrency = make(map[string]int, len(limits))
		}
		maps.Copy(c.concurrency, limits)
	}
}

// WithNamespaceSubscription serves the namespace through one subscription on rpc.<namespace>.> instead of one per method; no other process may then serve the same or a nested namespace.
func WithNamespaceSubscription() HandlerOption {
	return func(c *handlerConfig) {
		c.namespaceSubscription = true
	}
}

// WithPendingLimits caps the messages and bytes each method of the handler holds while busy, a negative value lifts the limit.
func WithPendingLimits(msgs, bytes int) HandlerOption {
	return func(c *handlerConfig) {
		c.pendingMsgs = msgs
		c.pendingBytes = bytes
	}
}

// WithDropExpired skips a call of the listed methods, or of every method without a list, that waited longer than its caller's __ttl.
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

// WithOrphanReaping sets the orphan probe for the sessions of this handler in place of ClientOptions.OrphanProbe.
func WithOrphanReaping(opts OrphanProbeOptions) HandlerOption {
	return func(c *handlerConfig) {
		c.orphanProbe = &opts
	}
}
