package rpc

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeNATS struct {
	ln         net.Listener
	maxPayload int

	mu         sync.Mutex
	conns      map[*fakeConn]struct{}
	pubs       map[string]int
	requests   map[string]int
	routed     []string
	pongDelay  time.Duration
	lateNoResp time.Duration
	subDelay   time.Duration
	pings      int
	refuse     bool
}

type fakeConn struct {
	srv          *fakeNATS
	conn         net.Conn
	wmu          sync.Mutex
	noResponders bool
	subs         map[string]*fakeSub
}

type fakeSub struct {
	subject string
	queue   string
	sid     string
	left    int
}

func startFakeNATS(t *testing.T) *fakeNATS {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeNATS{ln: ln, maxPayload: 1 << 20, conns: map[*fakeConn]struct{}{}, pubs: map[string]int{}, requests: map[string]int{}}
	go s.accept()
	t.Cleanup(s.close)
	return s
}

func (s *fakeNATS) url() string {
	return "nats://" + s.ln.Addr().String()
}

func (s *fakeNATS) close() {
	_ = s.ln.Close()
	s.kick()
}

func (s *fakeNATS) kick() {
	s.mu.Lock()
	conns := make([]*fakeConn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	for _, c := range conns {
		_ = c.conn.Close()
	}
}

func (s *fakeNATS) connCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

func (s *fakeNATS) publishes(match func(subject string) bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for subject, count := range s.pubs {
		if match(subject) {
			n += count
		}
	}
	return n
}

// subscriptions whose subject starts with prefix, over all connections
func (s *fakeNATS) subsWithPrefix(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for c := range s.conns {
		for _, sub := range c.subs {
			if strings.HasPrefix(sub.subject, prefix) {
				n++
			}
		}
	}
	return n
}

// publishes to subject that named a reply subject
func (s *fakeNATS) requestsTo(subject string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[subject]
}

// subjects in the order they reached the server
func (s *fakeNATS) publishOrder() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.routed)
}

// a 503 for a request then reaches the client before its Flush returns
func (s *fakeNATS) delayPongs(d time.Duration) {
	s.mu.Lock()
	s.pongDelay = d
	s.mu.Unlock()
}

// a 503 then reaches the client after the Flush behind its request returned
func (s *fakeNATS) delayNoResponders(d time.Duration) {
	s.mu.Lock()
	s.lateNoResp = d
	s.mu.Unlock()
}

// INFO announces n as max_payload to connections that come later
func (s *fakeNATS) setMaxPayload(n int) {
	s.mu.Lock()
	s.maxPayload = n
	s.mu.Unlock()
}

// a SUB reaches the routing table only after d, like a busy server
func (s *fakeNATS) delaySubs(d time.Duration) {
	s.mu.Lock()
	s.subDelay = d
	s.mu.Unlock()
}

func (s *fakeNATS) pingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pings
}

// a refused connection is closed before INFO, so it counts as a failed attempt
func (s *fakeNATS) setRefuse(on bool) {
	s.mu.Lock()
	s.refuse = on
	s.mu.Unlock()
}

func (s *fakeNATS) accept() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		c := &fakeConn{srv: s, conn: conn, subs: map[string]*fakeSub{}}
		s.mu.Lock()
		refuse := s.refuse
		if !refuse {
			s.conns[c] = struct{}{}
		}
		s.mu.Unlock()
		if refuse {
			_ = conn.Close()
			continue
		}
		go c.serve()
	}
}

func (c *fakeConn) write(b []byte) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, _ = c.conn.Write(b)
}

func (c *fakeConn) serve() {
	defer func() {
		_ = c.conn.Close()
		c.srv.mu.Lock()
		delete(c.srv.conns, c)
		c.srv.mu.Unlock()
	}()
	c.srv.mu.Lock()
	maxPayload := c.srv.maxPayload
	c.srv.mu.Unlock()
	info, _ := json.Marshal(map[string]any{
		"server_id": "fake", "version": "2.10.0", "proto": 1, "headers": true,
		"max_payload": maxPayload, "host": "127.0.0.1", "port": 0,
	})
	c.write([]byte("INFO " + string(info) + "\r\n"))

	r := bufio.NewReaderSize(c.conn, 64*1024)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		fields := strings.Fields(strings.TrimRight(line, "\r\n"))
		if len(fields) == 0 {
			continue
		}
		switch strings.ToUpper(fields[0]) {
		case "CONNECT":
			var opts struct {
				NoResponders bool `json:"no_responders"`
			}
			_ = json.Unmarshal([]byte(strings.TrimPrefix(strings.TrimSpace(line), fields[0])), &opts)
			c.srv.mu.Lock()
			c.noResponders = opts.NoResponders
			c.srv.mu.Unlock()
		case "PING":
			c.srv.mu.Lock()
			delay := c.srv.pongDelay
			c.srv.pings++
			c.srv.mu.Unlock()
			time.Sleep(delay)
			c.write([]byte("PONG\r\n"))
		case "SUB":
			sub := &fakeSub{subject: fields[1], sid: fields[len(fields)-1]}
			if len(fields) == 4 {
				sub.queue = fields[2]
			}
			c.srv.mu.Lock()
			delay := c.srv.subDelay
			c.srv.mu.Unlock()
			time.Sleep(delay)
			c.srv.mu.Lock()
			c.subs[sub.sid] = sub
			c.srv.mu.Unlock()
		case "UNSUB":
			c.srv.mu.Lock()
			if len(fields) == 3 {
				if sub := c.subs[fields[1]]; sub != nil {
					sub.left, _ = strconv.Atoi(fields[2])
				}
			} else {
				delete(c.subs, fields[1])
			}
			c.srv.mu.Unlock()
		case "PUB", "HPUB":
			hdrLen, total := 0, 0
			args := fields[1:]
			if fields[0] == "HPUB" {
				hdrLen, _ = strconv.Atoi(args[len(args)-2])
				total, _ = strconv.Atoi(args[len(args)-1])
				args = args[:len(args)-2]
			} else {
				total, _ = strconv.Atoi(args[len(args)-1])
				args = args[:len(args)-1]
			}
			payload := make([]byte, total+2)
			if _, err := io.ReadFull(r, payload); err != nil {
				return
			}
			reply := ""
			if len(args) == 2 {
				reply = args[1]
			}
			c.srv.route(c, args[0], reply, payload[:hdrLen], payload[hdrLen:total])
		}
	}
}

func (s *fakeNATS) route(from *fakeConn, subject, reply string, hdr, data []byte) {
	type target struct {
		conn *fakeConn
		sid  string
	}
	var targets []target
	queues := map[string]bool{}

	s.mu.Lock()
	s.pubs[subject]++
	if reply != "" {
		s.requests[subject]++
	}
	s.routed = append(s.routed, subject)
	for c := range s.conns {
		for sid, sub := range c.subs {
			if !subjectMatches(sub.subject, subject) {
				continue
			}
			if sub.queue != "" {
				if queues[sub.subject+" "+sub.queue] {
					continue
				}
				queues[sub.subject+" "+sub.queue] = true
			}
			targets = append(targets, target{c, sid})
			if sub.left > 0 {
				sub.left--
				if sub.left == 0 {
					delete(c.subs, sid)
				}
			}
		}
	}
	noResponders := from.noResponders
	late := s.lateNoResp
	s.mu.Unlock()

	if len(targets) == 0 {
		if reply != "" && noResponders {
			status := []byte("NATS/1.0 503\r\n\r\n")
			if late > 0 {
				time.AfterFunc(late, func() { s.route(from, reply, "", status, nil) })
			} else {
				s.route(from, reply, "", status, nil)
			}
		}
		return
	}
	for _, tg := range targets {
		var frame []byte
		replyPart := ""
		if reply != "" {
			replyPart = " " + reply
		}
		if len(hdr) > 0 {
			frame = fmt.Appendf(nil, "HMSG %s %s%s %d %d\r\n", subject, tg.sid, replyPart, len(hdr), len(hdr)+len(data))
			frame = append(frame, hdr...)
		} else {
			frame = fmt.Appendf(nil, "MSG %s %s%s %d\r\n", subject, tg.sid, replyPart, len(data))
		}
		frame = append(frame, data...)
		frame = append(frame, '\r', '\n')
		tg.conn.write(frame)
	}
}

func subjectMatches(pattern, subject string) bool {
	p := strings.Split(pattern, ".")
	s := strings.Split(subject, ".")
	for i, tok := range p {
		if tok == ">" {
			return len(s) > i
		}
		if i >= len(s) || (tok != "*" && tok != s[i]) {
			return false
		}
	}
	return len(p) == len(s)
}

func connectFake(t *testing.T, s *fakeNATS, base *ClientOptions) *Client {
	t.Helper()
	var opts ClientOptions
	if base != nil {
		opts = *base
	}
	opts.Servers = []string{s.url()}
	if opts.ReconnectWait == 0 {
		opts.ReconnectWait = 20 * time.Millisecond
	}
	c := NewClient(opts)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Disconnect() })
	return c
}

func TestFakeNATSRoundTrip(t *testing.T) {
	s := startFakeNATS(t)
	server := connectFake(t, s, nil)
	if _, err := server.RegisterHandler("fake", map[string]any{"echo": func(v string) string { return v }}); err != nil {
		t.Fatal(err)
	}
	client := connectFake(t, s, nil)
	got, err := client.CreateProxy("fake").Invoke(context.Background(), "echo", "hi")
	if err != nil || got != "hi" {
		t.Fatalf("echo = %v, %v", got, err)
	}
}
