package rpc

import "testing"

func applyOptions(opts ...HandlerOption) *handlerConfig {
	cfg := &handlerConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

func TestHandlerOptionDefaults(t *testing.T) {
	cfg := applyOptions()
	if cfg.isolatedConnection {
		t.Error("isolatedConnection should default to false")
	}
	if cfg.withoutDecorators {
		t.Error("withoutDecorators should default to false")
	}
	if cfg.queue != "" {
		t.Errorf("queue = %q, want empty", cfg.queue)
	}
}

func TestWithIsolatedConnection(t *testing.T) {
	cfg := applyOptions(WithIsolatedConnection())
	if !cfg.isolatedConnection {
		t.Error("isolatedConnection should be true")
	}
}

func TestWithoutDecorators(t *testing.T) {
	cfg := applyOptions(WithoutDecorators())
	if !cfg.withoutDecorators {
		t.Error("withoutDecorators should be true")
	}
}

func TestWithQueue(t *testing.T) {
	cfg := applyOptions(WithQueue("workers"))
	if cfg.queue != "workers" {
		t.Errorf("queue = %q, want workers", cfg.queue)
	}
}

func TestHandlerOptionsCombined(t *testing.T) {
	cfg := applyOptions(WithIsolatedConnection(), WithoutDecorators(), WithQueue("q1"))
	if !cfg.isolatedConnection || !cfg.withoutDecorators || cfg.queue != "q1" {
		t.Errorf("combined options not applied: %+v", cfg)
	}
}
