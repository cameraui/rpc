package rpc

import "fmt"

// Error codes
const (
	ErrCodeMethodNotFound   = "METHOD_NOT_FOUND"
	ErrCodeInvalidParams    = "INVALID_PARAMS"
	ErrCodeInternalError    = "INTERNAL_ERROR"
	ErrCodeTimeout          = "TIMEOUT"
	ErrCodeConnectionClosed = "CONNECTION_CLOSED"
	ErrCodeStreamError      = "STREAM_ERROR"
	ErrCodePayloadTooLarge  = "PAYLOAD_TOO_LARGE"
	ErrCodeNotFound         = "NOT_FOUND"
)

// RPCException is a structured RPC error.
type RPCException struct {
	Code    string
	Msg     string
	Details any
}

func (e *RPCException) Error() string {
	if e.Details != nil {
		return fmt.Sprintf("RPCException(%s): %s (data: %v)", e.Code, e.Msg, e.Details)
	}
	return fmt.Sprintf("RPCException(%s): %s", e.Code, e.Msg)
}

// ToRPCError converts the exception to the wire-format RPCError.
func (e *RPCException) ToRPCError() *RPCError {
	return &RPCError{
		Code:    e.Code,
		Message: e.Msg,
		Data:    e.Details,
	}
}

// NewRPCException creates a new RPCException.
func NewRPCException(code, message string, data ...any) *RPCException {
	var d any
	if len(data) > 0 {
		d = data[0]
	}
	return &RPCException{Code: code, Msg: message, Details: d}
}

// RPCExceptionFromError converts an RPCError (wire format) to an RPCException.
func RPCExceptionFromError(e *RPCError) *RPCException {
	return &RPCException{
		Code:    e.Code,
		Msg:     e.Message,
		Details: e.Data,
	}
}

// FormatErrorObject converts any error into an RPCError for the wire.
func FormatErrorObject(err error) *RPCError {
	if rpcErr, ok := err.(*RPCException); ok {
		return rpcErr.ToRPCError()
	}
	return &RPCError{
		Code:    ErrCodeInternalError,
		Message: err.Error(),
	}
}
