package rpc

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultOrphanProbeInterval = 30 * time.Second
	defaultOrphanGrace         = 5 * time.Minute
)

// every released consumer drops a reply-mux message without an id
type orphanPing struct {
	Type string `msgpack:"type"`
}

type orphanConfig struct {
	interval time.Duration
	grace    time.Duration
}

// probes ride on the session's own publishes and draw a 503 to the producer's
// reply mux once nobody receives them; an idle session sends ping instead
type orphanProbe struct {
	client   *Client
	interval time.Duration
	grace    time.Duration
	token    string
	reply    string
	ping     func(reply string) error
	// the next data publish carries the probe
	due atomic.Bool
	// data went out since the last tick
	sent atomic.Bool

	mu      sync.Mutex
	timer   *time.Timer
	reap    func()
	suspect time.Time
	stopped bool
}

// nil when the probe is off
func (c *Client) orphanConfig(override *OrphanProbeOptions) *orphanConfig {
	opts := c.Options.OrphanProbe
	if override != nil {
		opts = override
	}
	cfg := &orphanConfig{interval: defaultOrphanProbeInterval, grace: defaultOrphanGrace}
	if opts == nil {
		return cfg
	}
	if opts.Disabled {
		return nil
	}
	if opts.Interval > 0 {
		cfg.interval = opts.Interval
	}
	if opts.Grace > 0 {
		cfg.grace = opts.Grace
	}
	return cfg
}

// nil without a config, every method of a nil probe is a no-op
func (c *Client) newOrphanProbe(cfg *orphanConfig, ping func(reply string) error) *orphanProbe {
	if cfg == nil {
		return nil
	}
	token := c.generateID()
	return &orphanProbe{
		client:   c,
		interval: cfg.interval,
		grace:    cfg.grace,
		token:    token,
		reply:    "rpc.reply." + token,
		ping:     ping,
	}
}

// reap ends the session the way a cancel from the consumer does
func (p *orphanProbe) start(reap func()) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	p.reap = reap
	p.timer = time.AfterFunc(p.interval, p.tick)
}

// the reply subject for the next data publish, empty while no probe is due
func (p *orphanProbe) replyFor() string {
	if p == nil {
		return ""
	}
	p.sent.Store(true)
	if !p.due.Load() || !p.due.CompareAndSwap(true, false) {
		return ""
	}
	p.arm()
	return p.reply
}

// the consumer showed up, a request from it for example
func (p *orphanProbe) alive() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.suspect = time.Time{}
	p.mu.Unlock()
}

func (p *orphanProbe) stop() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
	if p.timer != nil {
		p.timer.Stop()
	}
	p.client.statusHandlers.Delete(p.token)
}

func (p *orphanProbe) tick() {
	if p.sent.Swap(false) {
		p.due.Store(true)
	} else {
		p.due.Store(false)
		p.arm()
		_ = p.ping(p.reply)
	}
	p.mu.Lock()
	if !p.stopped {
		p.timer.Reset(p.interval)
	}
	p.mu.Unlock()
}

// a handler still waiting means the previous probe reached someone; the 503
// of a probe comes back within a round trip, long before the next one
func (p *orphanProbe) arm() {
	if _, unanswered := p.client.statusHandlers.LoadAndDelete(p.token); unanswered {
		p.alive()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// under mu, so a stop in between cannot leave the handler behind
	if !p.stopped {
		p.client.statusHandlers.Store(p.token, p.noConsumer)
	}
}

func (p *orphanProbe) noConsumer(error) {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	now := time.Now()
	if p.suspect.IsZero() {
		p.suspect = now
		p.mu.Unlock()
		return
	}
	reap := p.reap
	if now.Sub(p.suspect) < p.grace || reap == nil {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	p.timer.Stop()
	p.mu.Unlock()
	// off the reply mux goroutine, ending a session publishes and unsubscribes
	go reap()
}
