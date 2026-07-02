package rpc

import "time"

// NoResponderRetryOptions configures retry behavior for 503/no-responder errors.
type NoResponderRetryOptions struct {
	// MaxRetries is the maximum number of retry attempts (default: 3).
	MaxRetries int
	// Delays is the delay before each retry attempt (default: [500ms, 1s, 2s]).
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

	// Reconnect enables automatic reconnection (default: true).
	Reconnect bool

	// MaxReconnectAttempts is the maximum number of reconnection attempts (-1 for infinite, default: -1).
	MaxReconnectAttempts int

	// ReconnectWait is the delay between reconnection attempts (default: 2s).
	ReconnectWait time.Duration

	// TLS contains optional TLS configuration.
	TLS *TLSOptions

	// MaxPayloadSize overrides the NATS max payload size (default: auto-detect).
	MaxPayloadSize int

	// WaitOnFirstConnect blocks Connect() until the first connection succeeds.
	// When nil or true, Connect() blocks. When false, Connect() returns immediately
	// and reconnects in the background — useful when Disconnect() must be able to
	// abort an in-flight connection attempt (e.g. browser URL switchover).
	// Default: true.
	WaitOnFirstConnect *bool

	// DisconnectTimeout is the maximum time to wait for the NATS connection
	// to fully close during Disconnect(). Default: 2s.
	DisconnectTimeout time.Duration

	// NoResponderRetry configures retry for 503/no-responder errors.
	// When nil, defaults to 3 retries with delays [500ms, 1s, 2s].
	NoResponderRetry *NoResponderRetryOptions
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
	isolatedConnection bool
	withoutDecorators  bool
	queue              string
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
