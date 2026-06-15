package rpc

import (
	"strings"
	"testing"
)

func TestErrorCodesDistinct(t *testing.T) {
	codes := []string{
		ErrCodeMethodNotFound,
		ErrCodeInvalidParams,
		ErrCodeInternalError,
		ErrCodeTimeout,
		ErrCodeConnectionClosed,
		ErrCodeStreamError,
		ErrCodePayloadTooLarge,
		ErrCodeNotFound,
	}

	seen := make(map[string]bool, len(codes))
	for _, c := range codes {
		if c == "" {
			t.Error("empty error code")
		}
		if seen[c] {
			t.Errorf("duplicate error code: %s", c)
		}
		seen[c] = true
	}
}

func TestRPCExceptionErrorFormat(t *testing.T) {
	t.Run("without details", func(t *testing.T) {
		exc := NewRPCException(ErrCodeTimeout, "timed out")
		msg := exc.Error()
		if !strings.Contains(msg, ErrCodeTimeout) {
			t.Errorf("error %q missing code", msg)
		}
		if !strings.Contains(msg, "timed out") {
			t.Errorf("error %q missing message", msg)
		}
		if strings.Contains(msg, "data:") {
			t.Errorf("error %q should not include data section", msg)
		}
	})

	t.Run("with details", func(t *testing.T) {
		exc := NewRPCException(ErrCodeInternalError, "broke", map[string]any{"k": "v"})
		msg := exc.Error()
		if !strings.Contains(msg, "data:") {
			t.Errorf("error %q missing data section", msg)
		}
	})
}

func TestNewRPCExceptionNoData(t *testing.T) {
	exc := NewRPCException(ErrCodeNotFound, "missing")
	if exc.Details != nil {
		t.Errorf("Details = %v, want nil", exc.Details)
	}
}

func TestFormatErrorObjectPreservesData(t *testing.T) {
	exc := NewRPCException(ErrCodeInvalidParams, "bad", []any{int64(1), int64(2)})
	rpcErr := FormatErrorObject(exc)
	if rpcErr.Code != ErrCodeInvalidParams {
		t.Errorf("Code = %v, want %v", rpcErr.Code, ErrCodeInvalidParams)
	}
	data, ok := rpcErr.Data.([]any)
	if !ok {
		t.Fatalf("Data type = %T, want []any", rpcErr.Data)
	}
	if len(data) != 2 {
		t.Errorf("Data len = %d, want 2", len(data))
	}
}

func TestExceptionRoundTripThroughWire(t *testing.T) {
	exc := NewRPCException(ErrCodeStreamError, "stream broke", "detail")
	wire := exc.ToRPCError()
	back := RPCExceptionFromError(wire)

	if back.Code != exc.Code {
		t.Errorf("Code = %v, want %v", back.Code, exc.Code)
	}
	if back.Msg != exc.Msg {
		t.Errorf("Msg = %v, want %v", back.Msg, exc.Msg)
	}
	if back.Details != exc.Details {
		t.Errorf("Details = %v, want %v", back.Details, exc.Details)
	}
}
