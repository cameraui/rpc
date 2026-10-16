package rpc

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
)

// the first retry delays are shorter than a remote namespace takes to answer
// __methods, the probe gets at least this long
const methodProbeWindow = 500 * time.Millisecond

type methodProbe struct {
	proxy  *Proxy
	method string
}

// Proxy provides a convenient way to make RPC calls to a specific namespace.
// Since Go doesn't have JS-style dynamic proxies, this uses explicit method calls.
type Proxy struct {
	client    *Client
	namespace string
	path      []string
	opts      *RequestOptions
}

// CreateProxy creates a new RPC proxy for the given namespace.
func (c *Client) CreateProxy(namespace string) *Proxy {
	return &Proxy{
		client:    c,
		namespace: namespace,
	}
}

// CreateIsolatedProxy creates a proxy with its own isolated NATS connection.
// Returns the proxy and a close function.
func (c *Client) CreateIsolatedProxy(namespace string) (*Proxy, func() error, error) {
	isolated := c.createIsolatedClient("proxy-" + namespace)
	if err := isolated.Connect(context.Background()); err != nil {
		return nil, nil, err
	}

	c.isolatedClientsMu.Lock()
	c.isolatedClients = append(c.isolatedClients, isolated)
	c.isolatedClientsMu.Unlock()

	proxy := &Proxy{
		client:    isolated,
		namespace: namespace,
	}

	closeFunc := func() error {
		return c.releaseIsolated(isolated)
	}

	return proxy, closeFunc, nil
}

// Sub creates a nested sub-proxy for the given name.
// Example: proxy.Sub("db").Invoke(ctx, "find", "key")
// This calls rpc.{namespace}.db.find
func (p *Proxy) Sub(name string) *Proxy {
	return &Proxy{
		client:    p.client,
		namespace: p.namespace,
		path:      append(append([]string{}, p.path...), name),
		opts:      p.opts,
	}
}

// WithOptions returns a copy of the proxy whose calls use opts for the
// per-call timeout and no-responder retry, as CallWithOptions does. Sub proxies
// of the copy inherit the options. Streams use only the retry part.
func (p *Proxy) WithOptions(opts RequestOptions) *Proxy {
	cp := *p
	cp.opts = &opts
	return &cp
}

// Invoke makes a normal RPC call through the proxy. When nobody serves the
// method but the namespace is up, the call fails at once with the no-responder
// error instead of retrying.
func (p *Proxy) Invoke(ctx context.Context, method string, args ...any) (any, error) {
	subject := p.buildSubject(method)
	return p.client.callWithOptions(ctx, subject, p.opts, methodProbe{p, method}, args...)
}

// InvokeStream makes a push-based streaming RPC call through the proxy. A
// method the namespace does not serve fails at once, as in Invoke.
func (p *Proxy) InvokeStream(ctx context.Context, method string, args ...any) (<-chan StreamValue, error) {
	subject := p.buildSubject(method)
	return p.client.callStreamWithOptions(ctx, subject, p.opts, methodProbe{p, method}, args...)
}

// InvokePullIterator makes a pull-based iterator RPC call through the proxy.
func (p *Proxy) InvokePullIterator(ctx context.Context, method string, args ...any) (<-chan PullValue, error) {
	subject := p.buildSubject(method)
	return p.client.callPullIterator(ctx, subject, p.opts, args...)
}

// InvokeCallback makes a callback subscription RPC call through the proxy.
// The callback is invoked for each value pushed by the handler.
// Returns an unsubscribe function.
func (p *Proxy) InvokeCallback(method string, args []any, callback func(any)) (func(), error) {
	subject := p.buildSubject(method)
	return callWithCallback(p.client, subject, p.opts, args, callback)
}

// InvokePullIteratorWithCallback makes a pull-iterator-with-callbacks call.
// See Client.CallPullIteratorWithCallback for semantics.
func (p *Proxy) InvokePullIteratorWithCallback(
	ctx context.Context,
	method string,
	callbacks PullCallbackMap,
	oneway []string,
	args ...any,
) (<-chan PullValue, error) {
	subject := p.buildSubject(method)
	return p.client.callPullIteratorWithCallback(ctx, subject, p.opts, callbacks, oneway, args...)
}

func (p *Proxy) buildSubject(method string) string {
	parts := make([]string, 0, 2+len(p.path)+1)
	parts = append(parts, "rpc", p.namespace)
	parts = append(parts, p.path...)
	parts = append(parts, method)
	return strings.Join(parts, ".")
}

// String returns a human-readable representation of the proxy.
func (p *Proxy) String() string {
	if len(p.path) > 0 {
		return fmt.Sprintf("[RPCProxy %s.%s]", p.namespace, strings.Join(p.path, "."))
	}
	return fmt.Sprintf("[RPCProxy %s]", p.namespace)
}

// gone closes once the namespace answers without the method, every later retry
// wait sees it; an old responder, a dead namespace or no answer leave it open
func (m methodProbe) start(ctx context.Context, delay time.Duration) (gone <-chan struct{}, stop context.CancelFunc) {
	probeCtx, cancel := context.WithTimeout(ctx, max(delay, methodProbeWindow))
	missing := make(chan struct{})
	go func() {
		if m.missing(probeCtx) {
			close(missing)
		}
	}()
	return missing, cancel
}

func (m methodProbe) missing(ctx context.Context) bool {
	name := m.method
	if len(m.proxy.path) > 0 {
		name = strings.Join(m.proxy.path, ".") + "." + m.method
	}
	result, err := m.proxy.client.callOnce(ctx, "rpc."+m.proxy.namespace+"."+methodsSubject, 0)
	if err != nil {
		return false
	}
	names, ok := result.([]any)
	return ok && !slices.Contains(names, any(name))
}

// ServiceProxy provides RPC calls to a discovered NATS micro service.
type ServiceProxy struct {
	client         *Client
	parentClient   *Client
	isolatedClient *Client
	info           ServiceInfoResponse
	path           []string
}

// CreateServiceProxy discovers a service and creates a proxy for it.
func (c *Client) CreateServiceProxy(ctx context.Context, serviceName string, opts ...ServiceProxyOption) (*ServiceProxy, error) {
	cfg := &serviceProxyConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	client := c
	if cfg.isolated {
		client = c.createIsolatedClient("service-" + serviceName)
		c.isolatedClientsMu.Lock()
		c.isolatedClients = append(c.isolatedClients, client)
		c.isolatedClientsMu.Unlock()
	}
	fail := func(err error) (*ServiceProxy, error) {
		if cfg.isolated {
			_ = c.releaseIsolated(client)
		}
		return nil, err
	}
	if cfg.isolated {
		if err := client.Connect(ctx); err != nil {
			return fail(err)
		}
	}

	monitor := NewServiceMonitor(client)
	services, err := monitor.Info(ctx, serviceName)
	if err != nil {
		return fail(fmt.Errorf("discover service: %w", err))
	}
	if len(services) == 0 {
		return fail(fmt.Errorf("no services found with name: %s", serviceName))
	}

	selected := services[0]
	if cfg.preferredID != "" {
		for _, s := range services {
			if s.ID == cfg.preferredID {
				selected = s
				break
			}
		}
	}

	sp := &ServiceProxy{
		client: client,
		info:   selected,
	}
	if cfg.isolated {
		sp.parentClient = c
		sp.isolatedClient = client
	}
	return sp, nil
}

// Close disconnects the isolated connection if one was created.
func (sp *ServiceProxy) Close() error {
	if sp.isolatedClient != nil {
		if sp.parentClient != nil {
			return sp.parentClient.releaseIsolated(sp.isolatedClient)
		}
		return sp.isolatedClient.Disconnect()
	}
	return nil
}

// Invoke calls a method on the service. A request too large for one message
// goes to the instance the proxy found, when that instance takes chunked
// requests on its own subject; with several instances behind one queue its
// chunks would otherwise be spread over them.
func (sp *ServiceProxy) Invoke(ctx context.Context, method string, args ...any) (any, error) {
	subject := sp.findEndpointSubject(method)
	if subject == "" {
		return nil, NewRPCException(ErrCodeMethodNotFound, fmt.Sprintf("endpoint %s not found", method))
	}
	direct := sp.directSubject(method)
	var result any
	err := sp.client.withNoResponderRetry(ctx, nil, methodProbe{}, func() error {
		var callErr error
		result, callErr = sp.client.callOnceTo(ctx, subject, direct, 0, args...)
		return callErr
	})
	return result, err
}

// InvokeStream calls a streaming method on the service.
func (sp *ServiceProxy) InvokeStream(ctx context.Context, method string, args ...any) (<-chan StreamValue, error) {
	subject := sp.findEndpointSubject(method)
	if subject == "" {
		return nil, NewRPCException(ErrCodeMethodNotFound, fmt.Sprintf("endpoint %s not found", method))
	}
	return sp.client.CallStream(ctx, subject, args...)
}

// InvokePullIterator calls a pull iterator method on the service.
func (sp *ServiceProxy) InvokePullIterator(ctx context.Context, method string, args ...any) (<-chan PullValue, error) {
	subject := sp.findEndpointSubject(method)
	if subject == "" {
		return nil, NewRPCException(ErrCodeMethodNotFound, fmt.Sprintf("endpoint %s not found", method))
	}
	return sp.client.CallPullIterator(ctx, subject, args...)
}

// InvokeCallback makes a callback subscription call on the service.
// The callback is invoked for each value pushed by the handler.
// Returns an unsubscribe function.
func (sp *ServiceProxy) InvokeCallback(method string, args []any, callback func(any)) (func(), error) {
	subject := sp.findEndpointSubject(method)
	if subject == "" {
		return nil, NewRPCException(ErrCodeMethodNotFound, fmt.Sprintf("endpoint %s not found", method))
	}
	return CallWithCallback(sp.client, subject, args, callback)
}

// InvokePullIteratorWithCallback makes a pull-iterator-with-callbacks call on
// the service. See Client.CallPullIteratorWithCallback for semantics.
func (sp *ServiceProxy) InvokePullIteratorWithCallback(
	ctx context.Context,
	method string,
	callbacks PullCallbackMap,
	oneway []string,
	args ...any,
) (<-chan PullValue, error) {
	subject := sp.findEndpointSubject(method)
	if subject == "" {
		return nil, NewRPCException(ErrCodeMethodNotFound, fmt.Sprintf("endpoint %s not found", method))
	}
	return sp.client.CallPullIteratorWithCallback(ctx, subject, callbacks, oneway, args...)
}

// Sub returns a sub-proxy scoped to a nested namespace.
func (sp *ServiceProxy) Sub(name string) *ServiceProxy {
	return &ServiceProxy{
		client: sp.client,
		info:   sp.info,
		path:   append(append([]string{}, sp.path...), name),
	}
}

func (sp *ServiceProxy) fullPath(method string) string {
	if len(sp.path) > 0 {
		return strings.Join(sp.path, ".") + "." + method
	}
	return method
}

// the subject of this service alone first, the bare one of an older responder
// may also reach another service with a method of the same name
func (sp *ServiceProxy) findEndpointSubject(method string) string {
	fullPath := sp.fullPath(method)
	own := svcSubjectPrefix + sp.info.Name + "." + fullPath
	for _, ep := range sp.info.Endpoints {
		if ep.Subject == own {
			return own
		}
	}

	for _, ep := range sp.info.Endpoints {
		if ep.Subject == fullPath {
			return ep.Subject
		}
		parts := strings.Split(ep.Subject, ".")
		if parts[len(parts)-1] == method {
			prefix := strings.Join(parts[:len(parts)-1], ".")
			pathPrefix := strings.Join(sp.path, ".")
			if prefix == pathPrefix {
				return ep.Subject
			}
		}
	}
	return ""
}

func (sp *ServiceProxy) directSubject(method string) string {
	if sp.info.ID == "" || sp.info.Metadata[chunkDirectKey] != "1" {
		return ""
	}
	return directSubjectPrefix + sp.info.ID + "." + sp.fullPath(method)
}

// ServiceProxyOption configures service proxy creation.
type ServiceProxyOption func(*serviceProxyConfig)

type serviceProxyConfig struct {
	preferredID string
	isolated    bool
}

// WithPreferredID selects a specific service instance by ID.
func WithPreferredID(id string) ServiceProxyOption {
	return func(c *serviceProxyConfig) {
		c.preferredID = id
	}
}

// WithIsolatedServiceProxy creates a separate NATS connection for the proxy.
func WithIsolatedServiceProxy() ServiceProxyOption {
	return func(c *serviceProxyConfig) {
		c.isolated = true
	}
}
