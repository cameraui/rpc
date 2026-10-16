package rpc

import (
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type laneItem struct {
	data    []byte
	arrived time.Time
}

// a method's requests in arrival order, served by a goroutine that runs only
// while some are queued; the limits stand in for the nats.go pending limits
// of the subscription the method would have on its own
type methodLane struct {
	subject  string
	run      func(data []byte, arrived time.Time)
	client   *Client
	maxMsgs  int
	maxBytes int

	mu      sync.Mutex
	queue   []laneItem
	bytes   int
	running bool
	closed  bool
	slow    bool
	dropped int
}

type laneSet struct {
	client   *Client
	maxMsgs  int
	maxBytes int
	// how a lane serves the requests of a method, nil for an unknown one
	runner func(method string) func(data []byte, arrived time.Time)

	mu     sync.Mutex
	lanes  map[string]*methodLane
	prefix string
	closed bool
}

func newLaneSet(client *Client, prefix string, maxMsgs, maxBytes int, runner func(method string) func([]byte, time.Time)) *laneSet {
	if maxMsgs == 0 {
		maxMsgs = nats.DefaultSubPendingMsgsLimit
	}
	if maxBytes == 0 {
		maxBytes = nats.DefaultSubPendingBytesLimit
	}
	return &laneSet{
		client:   client,
		maxMsgs:  maxMsgs,
		maxBytes: maxBytes,
		runner:   runner,
		lanes:    map[string]*methodLane{},
		prefix:   prefix,
	}
}

// nil for a method the set does not serve; created on its first request
func (s *laneSet) lane(method string) *methodLane {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l := s.lanes[method]; l != nil {
		return l
	}
	run := s.runner(method)
	if run == nil {
		return nil
	}
	l := &methodLane{
		subject:  s.prefix + method,
		run:      run,
		client:   s.client,
		maxMsgs:  s.maxMsgs,
		maxBytes: s.maxBytes,
		closed:   s.closed,
	}
	s.lanes[method] = l
	return l
}

func (s *laneSet) all() []*methodLane {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	lanes := make([]*methodLane, 0, len(s.lanes))
	for _, l := range s.lanes {
		lanes = append(lanes, l)
	}
	return lanes
}

func (s *laneSet) idle() bool {
	for _, l := range s.all() {
		if !l.idle() {
			return false
		}
	}
	return true
}

// queued requests go as the pending messages of a removed subscription do
func (s *laneSet) clear() {
	for _, l := range s.all() {
		l.clear()
	}
}

// after unregistering nothing queued runs anymore
func (s *laneSet) close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	for _, l := range s.all() {
		l.mu.Lock()
		l.closed = true
		l.mu.Unlock()
		l.clear()
	}
}

func (l *methodLane) push(data []byte, arrived time.Time) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	// a negative limit is no limit, as with nats.go
	if (l.maxMsgs > 0 && len(l.queue) >= l.maxMsgs) || (l.maxBytes > 0 && l.bytes+len(data) > l.maxBytes) {
		l.dropped++
		report := !l.slow
		l.slow = true
		dropped := l.dropped
		l.mu.Unlock()
		if report {
			l.client.slowConsumer(l.subject, dropped)
		}
		return
	}
	l.queue = append(l.queue, laneItem{data: data, arrived: arrived})
	l.bytes += len(data)
	if l.running {
		l.mu.Unlock()
		return
	}
	l.running = true
	l.mu.Unlock()
	go l.serve()
}

func (l *methodLane) serve() {
	for {
		l.mu.Lock()
		if len(l.queue) == 0 {
			l.running = false
			l.slow = false
			l.queue = nil
			l.mu.Unlock()
			return
		}
		item := l.queue[0]
		l.queue[0] = laneItem{}
		l.queue = l.queue[1:]
		l.bytes -= len(item.data)
		l.mu.Unlock()
		l.run(item.data, item.arrived)
	}
}

func (l *methodLane) idle() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.queue) == 0 && !l.running
}

func (l *methodLane) clear() {
	l.mu.Lock()
	clear(l.queue)
	l.queue = l.queue[:0]
	l.bytes = 0
	l.mu.Unlock()
}
