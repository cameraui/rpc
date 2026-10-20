package rpc

import (
	"context"
	"maps"
	"reflect"
	"slices"
	"sync"
)

var contextType = reflect.TypeFor[context.Context]()

type sessionKey struct {
	cleanups *sync.Map
	id       string
}

type sessionOwner struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	closed bool
	keys   map[sessionKey]struct{}
}

func newSessionOwner() *sessionOwner {
	ctx, cancel := context.WithCancel(context.Background())
	return &sessionOwner{ctx: ctx, cancel: cancel, keys: map[sessionKey]struct{}{}}
}

func (o *sessionOwner) track(cleanups *sync.Map, id string, cleanup func()) bool {
	key := sessionKey{cleanups, id}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return false
	}
	o.keys[key] = struct{}{}
	cleanups.Store(id, func() {
		o.forget(key)
		cleanup()
	})
	return true
}

func (o *sessionOwner) keep(cleanups *sync.Map, id string, cleanup func()) error {
	if o.track(cleanups, id, cleanup) {
		return nil
	}
	cleanup()
	return NewRPCException(ErrCodeConnectionClosed, "Connection closed")
}

func (o *sessionOwner) untrack(cleanups *sync.Map, id string) {
	o.forget(sessionKey{cleanups, id})
	cleanups.Delete(id)
}

func (o *sessionOwner) forget(key sessionKey) {
	o.mu.Lock()
	delete(o.keys, key)
	o.mu.Unlock()
}

func (o *sessionOwner) stop() {
	o.mu.Lock()
	o.closed = true
	keys := slices.Collect(maps.Keys(o.keys))
	clear(o.keys)
	o.mu.Unlock()

	for _, key := range keys {
		if v, ok := key.cleanups.LoadAndDelete(key.id); ok {
			if cleanup, ok := v.(func()); ok {
				cleanup()
			}
		}
	}
	// after the cleanups: a producer that stops on the context would otherwise close its channel first and look like a normal end
	o.cancel()
}

func takesContext(fnType reflect.Type) bool {
	return fnType.NumIn() > 0 && fnType.In(0) == contextType
}

func contextParams(fnType reflect.Type) int {
	if takesContext(fnType) {
		return 1
	}
	return 0
}

func callContext(parent context.Context, wanted bool) (context.Context, context.CancelFunc) {
	if !wanted {
		return parent, func() {}
	}
	return context.WithCancel(parent)
}

func drainChannel(rv reflect.Value) {
	if rv.IsNil() {
		return
	}
	// a producer blocked on its send reaches its defer only once someone receives
	for {
		if _, ok := rv.Recv(); !ok {
			return
		}
	}
}

func connectionClosedError() *RPCError {
	return &RPCError{Code: ErrCodeConnectionClosed, Message: "Connection closed"}
}
