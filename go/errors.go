package rpc

import (
	"errors"
	"fmt"
	"strings"
)

const upstreamUnavailable = "upstream unavailable"

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
	ErrCodeForbidden        = "FORBIDDEN"
)

// RPCException is a structured RPC error.
type RPCException struct {
	Code    string
	Msg     string
	Details any

	// the subject nobody served, set only on a no-responder error this client saw itself
	upstream string
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

// FormatErrorObject converts any error into an RPCError for the wire. A
// no-responder error this client saw itself, also wrapped with %w, goes out as
// INTERNAL_ERROR "upstream unavailable" with the subject in Data, so the caller
// does not take it for a no-responder of its own call and retry it.
func FormatErrorObject(err error) *RPCError {
	var local *RPCException
	if errors.As(err, &local) && local.upstream != "" {
		message := upstreamUnavailable
		if err != error(local) {
			message = strings.Replace(err.Error(), local.Error(), upstreamUnavailable, 1)
		}
		return &RPCError{
			Code:    ErrCodeInternalError,
			Message: message,
			Data:    map[string]any{"subject": local.upstream},
		}
	}
	if rpcErr, ok := err.(*RPCException); ok {
		return rpcErr.ToRPCError()
	}
	return &RPCError{
		Code:    ErrCodeInternalError,
		Message: err.Error(),
	}
}

// a no-responder failure this client saw itself (503 status or
// nats.ErrNoResponders); only these are retried, a remote NOT_FOUND is not
type localNoResponders struct {
	err *RPCException
}

func newLocalNoResponders(subject, message string) error {
	return &localNoResponders{err: noRespondersException(subject, message)}
}

func noRespondersException(subject, message string) *RPCException {
	return &RPCException{Code: ErrCodeNotFound, Msg: message, upstream: subject}
}

func (e *localNoResponders) Error() string {
	return e.err.Error()
}

func (e *localNoResponders) Unwrap() error {
	return e.err
}
