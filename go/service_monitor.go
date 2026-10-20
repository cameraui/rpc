package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// ServiceMonitor provides service discovery via NATS micro $SRV subjects.
type ServiceMonitor struct {
	client *Client
}

// NewServiceMonitor creates a new service monitor.
func NewServiceMonitor(client *Client) *ServiceMonitor {
	return &ServiceMonitor{client: client}
}

// ServicePing represents a ping response from a NATS micro service.
type ServicePing struct {
	Name    string `json:"name"`
	ID      string `json:"id"`
	Version string `json:"version"`
	Type    string `json:"type"`
}

// ServiceInfoResponse represents the info response from a NATS micro service.
type ServiceInfoResponse struct {
	Name        string            `json:"name"`
	ID          string            `json:"id"`
	Version     string            `json:"version"`
	Description string            `json:"description"`
	Type        string            `json:"type"`
	Endpoints   []ServiceEndpoint `json:"endpoints"`
	Metadata    map[string]string `json:"metadata"`
}

// ServiceEndpoint describes a single service endpoint.
type ServiceEndpoint struct {
	Name     string            `json:"name"`
	Subject  string            `json:"subject"`
	Metadata map[string]string `json:"metadata"`
}

// ServiceStatsResponse represents the stats response from a NATS micro service.
type ServiceStatsResponse struct {
	Name      string                `json:"name"`
	ID        string                `json:"id"`
	Version   string                `json:"version"`
	Type      string                `json:"type"`
	Endpoints []ServiceEndpointStat `json:"endpoints"`
	Started   string                `json:"started"`
	Metadata  map[string]string     `json:"metadata"`
}

// ServiceEndpointStat contains statistics for a single endpoint.
type ServiceEndpointStat struct {
	Name                string `json:"name"`
	Subject             string `json:"subject"`
	NumRequests         int    `json:"num_requests"`
	NumErrors           int    `json:"num_errors"`
	AverageProcessingMs int64  `json:"average_processing_time"`
	LastError           string `json:"last_error"`
}

// Ping discovers services by name using $SRV.PING.
func (m *ServiceMonitor) Ping(ctx context.Context, name string, timeout ...time.Duration) ([]ServicePing, error) {
	t := 2 * time.Second
	if len(timeout) > 0 {
		t = timeout[0]
	}

	subject := "$SRV.PING"
	if name != "" {
		subject = "$SRV.PING." + name
	}

	return collectResponses[ServicePing](m.client, ctx, subject, t)
}

// Info retrieves detailed information for services by name.
func (m *ServiceMonitor) Info(ctx context.Context, name string, timeout ...time.Duration) ([]ServiceInfoResponse, error) {
	t := 2 * time.Second
	if len(timeout) > 0 {
		t = timeout[0]
	}

	subject := "$SRV.INFO"
	if name != "" {
		subject = "$SRV.INFO." + name
	}

	return collectResponses[ServiceInfoResponse](m.client, ctx, subject, t)
}

// Stats retrieves statistics for services by name.
func (m *ServiceMonitor) Stats(ctx context.Context, name string, timeout ...time.Duration) ([]ServiceStatsResponse, error) {
	t := 2 * time.Second
	if len(timeout) > 0 {
		t = timeout[0]
	}

	subject := "$SRV.STATS"
	if name != "" {
		subject = "$SRV.STATS." + name
	}

	return collectResponses[ServiceStatsResponse](m.client, ctx, subject, t)
}

func collectResponses[T any](client *Client, ctx context.Context, subject string, timeout time.Duration) ([]T, error) {
	client.mu.RLock()
	nc := client.nc
	client.mu.RUnlock()
	if nc == nil {
		return nil, fmt.Errorf("not connected")
	}

	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}
	defer func() { _ = sub.Unsubscribe() }()

	if err := nc.PublishRequest(subject, inbox, nil); err != nil {
		return nil, err
	}

	const stall = 10 * time.Millisecond // after the first reply, each next one must follow within this
	results := make([]T, 0)
	deadline := time.Now().Add(timeout)
	wait := timeout
	for {
		left := time.Until(deadline)
		if left <= 0 {
			return results, nil
		}
		waitCtx, cancel := context.WithTimeout(ctx, min(wait, left))
		msg, err := sub.NextMsgWithContext(waitCtx)
		cancel()
		if ctx.Err() != nil {
			return results, ctx.Err()
		}
		// the wait ran out, or nats.ErrNoResponders: nobody serves the name
		if err != nil {
			return results, nil
		}
		var v T
		if err := json.Unmarshal(msg.Data, &v); err == nil {
			results = append(results, v)
		}
		wait = stall
	}
}
