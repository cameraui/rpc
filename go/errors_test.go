package rpc

import (
	"errors"
	"testing"
)

func TestRPCException(t *testing.T) {
	exc := NewRPCException(ErrCodeMethodNotFound, "method not found")

	if exc.Code != ErrCodeMethodNotFound {
		t.Errorf("Code = %v, want %v", exc.Code, ErrCodeMethodNotFound)
	}
	if exc.Msg != "method not found" {
		t.Errorf("Msg = %v, want 'method not found'", exc.Msg)
	}
}

func TestRPCExceptionWithData(t *testing.T) {
	exc := NewRPCException(ErrCodeInternalError, "something broke", map[string]any{"detail": "stack trace"})

	if exc.Details == nil {
		t.Fatal("expected data, got nil")
	}
	m, ok := exc.Details.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", exc.Details)
	}
	if m["detail"] != "stack trace" {
		t.Errorf("detail = %v, want 'stack trace'", m["detail"])
	}
}

func TestRPCExceptionToRPCError(t *testing.T) {
	exc := NewRPCException(ErrCodeTimeout, "timed out")
	rpcErr := exc.ToRPCError()

	if rpcErr.Code != ErrCodeTimeout {
		t.Errorf("Code = %v, want %v", rpcErr.Code, ErrCodeTimeout)
	}
	if rpcErr.Message != "timed out" {
		t.Errorf("Message = %v, want 'timed out'", rpcErr.Message)
	}
}

func TestRPCExceptionFromError(t *testing.T) {
	rpcErr := &RPCError{
		Code:    ErrCodeNotFound,
		Message: "not found",
		Data:    "extra",
	}
	exc := RPCExceptionFromError(rpcErr)

	if exc.Code != ErrCodeNotFound {
		t.Errorf("Code = %v, want %v", exc.Code, ErrCodeNotFound)
	}
	if exc.Msg != "not found" {
		t.Errorf("Msg = %v, want 'not found'", exc.Msg)
	}
}

func TestFormatErrorObjectRPCException(t *testing.T) {
	exc := NewRPCException(ErrCodeStreamError, "stream failed")
	rpcErr := FormatErrorObject(exc)

	if rpcErr.Code != ErrCodeStreamError {
		t.Errorf("Code = %v, want %v", rpcErr.Code, ErrCodeStreamError)
	}
}

func TestFormatErrorObjectStdError(t *testing.T) {
	err := errors.New("something went wrong")
	rpcErr := FormatErrorObject(err)

	if rpcErr.Code != ErrCodeInternalError {
		t.Errorf("Code = %v, want %v", rpcErr.Code, ErrCodeInternalError)
	}
	if rpcErr.Message != "something went wrong" {
		t.Errorf("Message = %v, want 'something went wrong'", rpcErr.Message)
	}
}

func TestRPCExceptionErrorInterface(t *testing.T) {
	exc := NewRPCException(ErrCodeTimeout, "timed out")

	// Should implement error interface
	var err error = exc
	if err.Error() == "" {
		t.Error("expected non-empty error message")
	}
}
