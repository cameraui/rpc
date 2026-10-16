package rpc

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type pushSession struct {
	id      string
	subject string
	client  *Client
	cancel  context.CancelFunc
	stopped chan struct{}
	unsub   func()
	probe   *orphanProbe
	// orders every publish against the stop, so nothing follows the terminal message
	mu    sync.Mutex
	ended bool
}

// closed: a channel result was read to its end, so nothing is left to drain
func (s *pushSession) forward(rv reflect.Value) (closed bool, err error) {
	switch rv.Kind() {
	case reflect.Chan:
		for {
			if !s.client.IsConnected() {
				return false, nil
			}
			val, ok, stopped := recvUnlessDone(rv, s.stopped)
			if stopped {
				return false, nil
			}
			if !ok {
				return true, nil
			}
			if err := s.publish(StreamMessage{ID: s.id, Type: "data", Data: val.Interface()}); err != nil {
				return false, err
			}
		}

	case reflect.Slice, reflect.Array:
		for i := range rv.Len() {
			if isClosed(s.stopped) || !s.client.IsConnected() {
				return false, nil
			}
			if err := s.publish(StreamMessage{ID: s.id, Type: "data", Data: rv.Index(i).Interface()}); err != nil {
				return false, err
			}
		}
		return true, nil

	case reflect.Invalid:
		return false, NewRPCException(ErrCodeInternalError, "Handler must return a channel or slice for stream")

	default:
		return false, NewRPCException(ErrCodeInternalError, fmt.Sprintf("Handler must return a channel or slice for stream, got %s", rv.Kind()))
	}
}

func (s *pushSession) publish(msg StreamMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return nil
	}
	return s.client.publishInternal(s.subject, msg, s.probe.replyFor())
}

// every consumer skips a stream message of an unknown type
func (s *pushSession) ping(reply string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return nil
	}
	return s.client.publishInternal(s.subject, StreamMessage{ID: s.id, Type: "ping"}, reply)
}

func (s *pushSession) end(terminal *StreamMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	s.ended = true
	close(s.stopped)
	s.cancel()
	s.probe.stop()
	if terminal != nil && s.client.IsConnected() {
		_ = s.client.Publish(s.subject, *terminal)
	}
}

// the owner or the connection went away, the consumer would wait forever
func (s *pushSession) teardown() {
	s.end(&StreamMessage{ID: s.id, Type: "error", Error: connectionClosedError()})
	s.unsub()
}

type pullSession struct {
	id              string
	requestSubject  string
	responseSubject string
	client          *Client
	rv              reflect.Value
	pump            *pullPump
	cancel          context.CancelFunc
	invoker         *CallbackInvoker
	probe           *orphanProbe
	onFinished      func()
	// the callback flavour only marks batch boundaries
	values bool
	// orders every response against the terminal one, so no value follows it
	mu    sync.Mutex
	ended bool
	unsub func()
	// false while the handler sets the session up, a cancel then only marks cancelled
	serving   bool
	cancelled bool
	// a callback frame that could not go out, it answers the step
	failure *PullIteratorResponse
}

func newPullSession(id string, client *Client, cancel context.CancelFunc, probe *orphanProbe, onFinished func()) *pullSession {
	return &pullSession{
		id:              id,
		requestSubject:  fmt.Sprintf("_rpc.iterator.%s.request", id),
		responseSubject: fmt.Sprintf("_rpc.iterator.%s.response", id),
		client:          client,
		pump:            newPullPump(),
		cancel:          cancel,
		probe:           probe,
		onFinished:      onFinished,
	}
}

// subscribed before the handler runs, so a cancel during its setup counts
func (s *pullSession) listen() error {
	unsub, err := s.client.Subscribe(s.requestSubject, s.handleRequest)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.unsub = unsub
	s.mu.Unlock()
	return nil
}

// the handler failed, nothing was served
func (s *pullSession) abandon() {
	s.mu.Lock()
	unsub := s.unsub
	s.unsub = nil
	s.mu.Unlock()
	if unsub != nil {
		unsub()
	}
}

// false when the caller cancelled while the handler set up, the session is over then
func (s *pullSession) serve(rv reflect.Value) bool {
	s.rv = rv
	s.pump.rv = rv
	s.mu.Lock()
	s.serving = true
	cancelled := s.cancelled
	s.mu.Unlock()
	if cancelled {
		s.end(&PullIteratorResponse{ID: s.id, Type: "done"}, false, false)
		return false
	}
	s.probe.start(func() { s.end(&PullIteratorResponse{ID: s.id, Type: "done"}, false, true) })

	go s.pump.run(func(val reflect.Value, ok bool) {
		if !ok {
			s.end(&PullIteratorResponse{ID: s.id, Type: "done"}, true, true)
			return
		}
		resp := PullIteratorResponse{ID: s.id, Type: "value"}
		if s.values {
			resp.Value = val.Interface()
		}
		s.respond(resp)
	})
	return true
}

func (s *pullSession) handleRequest(data []byte) {
	var req PullIteratorRequest
	if err := DecodeMessageInto(data, &req); err != nil {
		return
	}
	s.probe.alive()
	switch req.Type {
	case "cancel":
		s.mu.Lock()
		serving := s.serving
		s.cancelled = true
		s.mu.Unlock()
		if serving {
			s.end(&PullIteratorResponse{ID: s.id, Type: "done"}, false, true)
		}
	case "next":
		if failure := s.next(); failure != nil {
			s.end(failure, false, true)
		}
	}
}

// under the lock, so fail sees a next that came in first; before the session
// is served a next only counts
func (s *pullSession) next() *PullIteratorResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failure != nil && s.serving {
		return s.failure
	}
	s.pump.request()
	return nil
}

// later frames are dropped at once, the error answers the step only once the
// consumer asked for it, so it never goes out before the consumer subscribed
func (s *pullSession) fail(err error) {
	s.mu.Lock()
	if s.ended || s.failure != nil {
		s.mu.Unlock()
		return
	}
	failure := &PullIteratorResponse{ID: s.id, Type: "error", Error: FormatErrorObject(err)}
	s.failure = failure
	asked := s.serving && s.pump.pending.Load() > 0
	s.mu.Unlock()
	s.invoker.stop()
	if asked {
		s.end(failure, false, true)
	}
}

// a value that cannot go out ends the session, the consumer's next waits for an answer
func (s *pullSession) respond(resp PullIteratorResponse) {
	s.mu.Lock()
	var err error
	if !s.ended {
		err = s.client.publishInternal(s.responseSubject, resp, s.probe.replyFor())
	}
	s.mu.Unlock()
	if err != nil && s.client.IsConnected() {
		s.end(&PullIteratorResponse{ID: s.id, Type: "error", Error: FormatErrorObject(err)}, false, true)
	}
}

// a pull consumer reads its response subject in order, the probe goes to its
// reply mux instead, which drops a message without an id
func (s *pullSession) ping(reply string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return nil
	}
	return s.client.publishInternal("rpc.reply."+s.id, orphanPing{Type: "ping"}, reply)
}

// closed: the producer closed its channel, otherwise it is drained so a send it
// blocks on returns; finished drops the caller's map entry, a teardown leaves
// that to the caller sweeping its map
func (s *pullSession) end(terminal *PullIteratorResponse, closed, finished bool) {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	s.cancel()
	s.probe.stop()
	if s.invoker != nil {
		s.invoker.stop()
	}
	s.pump.stop()
	if terminal != nil && s.client.IsConnected() {
		_ = s.client.Publish(s.responseSubject, *terminal)
	}
	unsub := s.unsub
	s.mu.Unlock()

	if unsub != nil {
		unsub()
	}
	if !closed {
		go drainChannel(s.rv)
	}
	if finished && s.onFinished != nil {
		s.onFinished()
	}
}

func (s *pullSession) teardown() {
	s.end(&PullIteratorResponse{ID: s.id, Type: "error", Error: connectionClosedError()}, false, false)
}

// one receive per next, in request order, off the subscription goroutine so a
// cancel is not queued behind a producer that has not yielded yet
type pullPump struct {
	rv      reflect.Value
	wake    chan struct{}
	done    chan struct{}
	pending atomic.Int64
}

// rv is set once the handler returned, a next before that only counts
func newPullPump() *pullPump {
	return &pullPump{wake: make(chan struct{}, 1), done: make(chan struct{})}
}

func (p *pullPump) request() {
	p.pending.Add(1)
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// stop must run once, pullSession.end guards it
func (p *pullPump) stop() {
	close(p.done)
}

func (p *pullPump) run(respond func(val reflect.Value, ok bool)) {
	for {
		select {
		case <-p.wake:
		case <-p.done:
			return
		}
		for p.pending.Load() > 0 {
			val, ok, stopped := recvUnlessDone(p.rv, p.done)
			if stopped {
				return
			}
			p.pending.Add(-1)
			respond(val, ok)
			if !ok {
				return
			}
		}
	}
}

// handleStreamRequestGo handles a push-based streaming request.
// It calls the handler, iterates over the returned channel, and publishes
// data/end/error messages to the stream subject.
func handleStreamRequestGo(owner *sessionOwner, fn reflect.Value, args []any, streamSubject, requestID string, client *Client, orphan *orphanConfig, recoverPanics bool) {
	if !isControlSubject(streamSubject, streamSubjectPrefix) {
		return
	}

	ctx, cancel := context.WithCancel(owner.ctx)
	s := &pushSession{id: requestID, subject: streamSubject, client: client, cancel: cancel, stopped: make(chan struct{})}
	s.probe = client.newOrphanProbe(orphan, s.ping)

	// subscribed before the handler runs, so a cancel during its setup counts
	unsub, err := client.Subscribe(streamSubject+".cancel", func([]byte) { s.end(nil) })
	if err != nil {
		s.end(&StreamMessage{
			ID:    requestID,
			Type:  "error",
			Error: FormatErrorObject(fmt.Errorf("failed to setup cancel listener: %w", err)),
		})
		return
	}
	s.unsub = unsub
	if !owner.track(&client.streamCleanups, streamSubject, s.teardown) {
		s.teardown()
		return
	}
	s.probe.start(func() { s.end(nil) })

	// a channel the producer may still be sending on
	var open reflect.Value
	defer func() {
		s.end(nil)
		unsub()
		owner.untrack(&client.streamCleanups, streamSubject)
		if open.IsValid() {
			go drainChannel(open)
		}
	}()

	defer func() {
		r := recover()
		if r == nil {
			return
		}
		s.end(&StreamMessage{ID: requestID, Type: "error", Error: &RPCError{
			Code:    ErrCodeInternalError,
			Message: fmt.Sprintf("handler panic: %v", r),
		}})
		if !recoverPanics {
			client.flushTimeout(time.Second)
			panic(r)
		}
	}()

	result, err := callHandler(ctx, fn, args)
	if err != nil {
		s.end(&StreamMessage{ID: requestID, Type: "error", Error: FormatErrorObject(err)})
		return
	}

	rv := reflect.ValueOf(result)
	if rv.Kind() == reflect.Chan {
		open = rv
	}
	closed, err := s.forward(rv)
	if closed {
		open = reflect.Value{}
	}
	if err != nil {
		s.end(&StreamMessage{ID: requestID, Type: "error", Error: FormatErrorObject(err)})
		return
	}
	s.end(&StreamMessage{ID: requestID, Type: "end"})
}

// handlePullIteratorRequestGo handles a pull-based iterator request.
// It calls the handler and sets up the request/response protocol for iteration.
// onFinished is invoked once when the iterator ends naturally (done/cancel) so
// the caller can drop its cleanup-map entry for this session.
// A nil teardown with a nil error: the caller cancelled during the setup and the session is over.
func handlePullIteratorRequestGo(parent context.Context, fn reflect.Value, args []any, iteratorID string, client *Client, orphan *orphanConfig, onFinished func()) (func(), error) {
	ctx, cancel := context.WithCancel(parent)
	var s *pullSession
	probe := client.newOrphanProbe(orphan, func(reply string) error { return s.ping(reply) })
	s = newPullSession(iteratorID, client, cancel, probe, onFinished)
	s.values = true
	// a panicking handler ends the context too
	started := false
	defer func() {
		if !started {
			s.abandon()
			cancel()
		}
	}()
	if err := s.listen(); err != nil {
		return nil, err
	}
	result, err := callHandler(ctx, fn, args)
	if err != nil {
		return nil, err
	}

	rv := reflect.ValueOf(result)
	if rv.Kind() != reflect.Chan {
		return nil, NewRPCException(ErrCodeInternalError, "Handler must return a channel for pull iterator")
	}

	started = true
	if !s.serve(rv) {
		return nil, nil
	}
	return s.teardown, nil
}

// select only when no value is ready, a reflect.Select per value costs streams
// of small items about 5 % throughput; a value that raced done is dropped
func recvUnlessDone(rv reflect.Value, done <-chan struct{}) (val reflect.Value, ok, stopped bool) {
	val, ok = rv.TryRecv()
	if !val.IsValid() {
		cases := [2]reflect.SelectCase{
			{Dir: reflect.SelectRecv, Chan: rv},
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(done)},
		}
		var chosen int
		chosen, val, ok = reflect.Select(cases[:])
		if chosen == 1 {
			return reflect.Value{}, false, true
		}
	}
	if isClosed(done) {
		return reflect.Value{}, false, true
	}
	return val, ok, false
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
