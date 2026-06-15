package rpc

import (
	"context"
	"fmt"
	"strings"
)

// Proxy provides a convenient way to make RPC calls to a specific namespace.
// Since Go doesn't have JS-style dynamic proxies, this uses explicit method calls.
type Proxy struct {
	client    *Client
	namespace string
	path      []string
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
		return isolated.Disconnect()
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
	}
}

// Invoke makes a normal RPC call through the proxy.
func (p *Proxy) Invoke(ctx context.Context, method string, args ...any) (any, error) {
	subject := p.buildSubject(method)
	return p.client.Call(ctx, subject, args...)
}

// InvokeStream makes a push-based streaming RPC call through the proxy.
func (p *Proxy) InvokeStream(ctx context.Context, method string, args ...any) (<-chan StreamValue, error) {
	subject := p.buildSubject(method)
	return p.client.CallStream(ctx, subject, args...)
}

// InvokePullIterator makes a pull-based iterator RPC call through the proxy.
func (p *Proxy) InvokePullIterator(ctx context.Context, method string, args ...any) (<-chan PullValue, error) {
	subject := p.buildSubject(method)
	return p.client.CallPullIterator(ctx, subject, args...)
}

// InvokeCallback makes a callback subscription RPC call through the proxy.
// The callback is invoked for each value pushed by the handler.
// Returns an unsubscribe function.
func (p *Proxy) InvokeCallback(method string, args []any, callback func(any)) (func(), error) {
	subject := p.buildSubject(method)
	return CallWithCallback(p.client, subject, args, callback)
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
	return p.client.CallPullIteratorWithCallback(ctx, subject, callbacks, oneway, args...)
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
		if err := client.Connect(ctx); err != nil {
			return nil, err
		}
	}

	monitor := NewServiceMonitor(client)
	services, err := monitor.Info(ctx, serviceName)
	if err != nil {
		return nil, fmt.Errorf("discover service: %w", err)
	}
	if len(services) == 0 {
		return nil, fmt.Errorf("no services found with name: %s", serviceName)
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
		err := sp.isolatedClient.Disconnect()
		if sp.parentClient != nil {
			sp.parentClient.removeIsolatedClient(sp.isolatedClient)
		}
		return err
	}
	return nil
}

// Invoke calls a method on the service.
func (sp *ServiceProxy) Invoke(ctx context.Context, method string, args ...any) (any, error) {
	subject := sp.findEndpointSubject(method)
	if subject == "" {
		return nil, NewRPCException(ErrCodeMethodNotFound, fmt.Sprintf("endpoint %s not found", method))
	}
	return sp.client.Call(ctx, subject, args...)
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

func (sp *ServiceProxy) findEndpointSubject(method string) string {
	fullPath := method
	if len(sp.path) > 0 {
		fullPath = strings.Join(sp.path, ".") + "." + method
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
