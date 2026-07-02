package rpc

// RPCMessage is the wire-format for an RPC request.
//
// Discover is the method-discovery request marker (internal metadata for
// proxies): when true, the responder attaches the namespace's method list
// (__methods) to the response. Client proxies set it only while their method
// cache is empty. It lives on the envelope (not in params) so it never leaks
// into handler arguments; old responders ignore it.
type RPCMessage struct {
	ID       string `msgpack:"id"`
	Method   string `msgpack:"method"`
	Params   any    `msgpack:"params"`
	Discover bool   `msgpack:"__discover,omitempty"`
}

// RPCResponse is the wire-format for an RPC response.
type RPCResponse struct {
	ID      string    `msgpack:"id"`
	Result  any       `msgpack:"result"`
	Error   *RPCError `msgpack:"error,omitempty"`
	Methods []string  `msgpack:"__methods,omitempty"`
}

// RPCError is the wire-format for an error within an RPC response.
type RPCError struct {
	Code    string `msgpack:"code"`
	Message string `msgpack:"message"`
	Data    any    `msgpack:"data,omitempty"`
}

// StreamMessage is the wire-format for push-based streaming data.
type StreamMessage struct {
	ID    string    `msgpack:"id"`
	Type  string    `msgpack:"type"` // "data", "end", "error"
	Data  any       `msgpack:"data"`
	Error *RPCError `msgpack:"error,omitempty"`
}

// PullIteratorRequest is the wire-format for a pull-based iterator request.
type PullIteratorRequest struct {
	ID   string `msgpack:"id"`
	Type string `msgpack:"type"` // "next", "cancel"
}

// PullIteratorResponse is the wire-format for a pull-based iterator response.
type PullIteratorResponse struct {
	ID    string    `msgpack:"id"`
	Type  string    `msgpack:"type"` // "value", "done", "error"
	Value any       `msgpack:"value"`
	Error *RPCError `msgpack:"error,omitempty"`
}

// ChannelMessage is the wire-format for channel communication.
type ChannelMessage struct {
	Type   string `msgpack:"type"` // "message", "close", "error"
	Data   any    `msgpack:"data"`
	Error  string `msgpack:"error,omitempty"`
	Sender string `msgpack:"sender,omitempty"`
}

// ChunkedTransferHeader is the wire-format for a chunked transfer header.
type ChunkedTransferHeader struct {
	Type        string `msgpack:"type"` // always "chunked"
	TransferID  string `msgpack:"transferId"`
	TotalChunks int    `msgpack:"totalChunks"`
	TotalSize   int    `msgpack:"totalSize"`
	ChunkSize   int    `msgpack:"chunkSize"`
}

// ChunkData represents a single chunk in a chunked transfer.
type ChunkData struct {
	ID         string `msgpack:"id"`
	ChunkIndex int    `msgpack:"chunkIndex"`
	Data       []byte `msgpack:"data"`
	IsLast     bool   `msgpack:"isLast"`
}

// StreamParams are the special parameters sent when initiating a push stream.
type StreamParams struct {
	Stream        bool   `msgpack:"__stream"`
	StreamSubject string `msgpack:"__streamSubject"`
	Args          []any  `msgpack:"args"`
}

// PullIteratorParams are the special parameters sent when initiating a pull iterator.
type PullIteratorParams struct {
	PullIterator bool   `msgpack:"__pullIterator"`
	IteratorID   string `msgpack:"__iteratorId"`
	Args         []any  `msgpack:"args"`
}

// HandshakeData is sent during private channel initialization.
type HandshakeData struct {
	Handshake bool `msgpack:"__handshake"`
}

// CallbackParams are the special parameters sent when initiating a callback subscription.
type CallbackParams struct {
	Callback        bool   `msgpack:"__callback"`
	CallbackSubject string `msgpack:"__callbackSubject"`
	Args            []any  `msgpack:"args"`
}

// CallbackMessage is the wire-format for callback data pushed to subscribers.
type CallbackMessage struct {
	ID    string    `msgpack:"id"`
	Type  string    `msgpack:"type"` // "data", "error"
	Data  any       `msgpack:"data"`
	Error *RPCError `msgpack:"error,omitempty"`
}

// PullCallbackParams are the special parameters sent when initiating a
// pull-iterator-with-callbacks request. Combines pull-iterator flow control
// with oneway callbacks for low-latency item-level data delivery.
// See packages/rpc/PULL_CALLBACK_PROTOCOL.md.
type PullCallbackParams struct {
	PullCallback    bool     `msgpack:"__pullCallback"`
	IteratorID      string   `msgpack:"__iteratorId"`
	CallbackSubject string   `msgpack:"__callbackSubject"`
	CallbackMethods []string `msgpack:"__callbackMethods"`
	OnewayMethods   []string `msgpack:"__onewayMethods"`
	Args            []any    `msgpack:"args"`
}

// CallbackInvocation is a single oneway callback invocation pushed from
// server to client on the callback subject.
type CallbackInvocation struct {
	Method string `msgpack:"method"`
	Args   []any  `msgpack:"args"`
}
