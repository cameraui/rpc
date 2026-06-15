package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
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

// collectResponses sends a broadcast request and collects responses.
// Uses a short per-message timeout (10ms): once no new response arrives
// within that window, collection stops and results are returned immediately.
// The overall timeout acts as a hard upper bound.
func collectResponses[T any](client *Client, ctx context.Context, subject string, timeout time.Duration) ([]T, error) {
	nc := client.nc
	if nc == nil {
		return nil, fmt.Errorf("not connected")
	}

	inbox := nc.NewInbox()
	msgCh := make(chan *nats.Msg, 16)

	sub, err := nc.ChanSubscribe(inbox, msgCh)
	if err != nil {
		return nil, err
	}
	defer func() { _ = sub.Unsubscribe() }()

	if err := nc.PublishRequest(subject, inbox, nil); err != nil {
		return nil, err
	}
	_ = nc.Flush()

	results := make([]T, 0)
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	// Short per-message timeout
	const perMsgTimeout = 10 * time.Millisecond
	msgTimer := time.NewTimer(perMsgTimeout)
	defer msgTimer.Stop()

	for {
		select {
		case msg := <-msgCh:
			var v T
			if err := json.Unmarshal(msg.Data, &v); err == nil {
				results = append(results, v)
			}
			// Reset short timer for next potential message
			if !msgTimer.Stop() {
				select {
				case <-msgTimer.C:
				default:
				}
			}
			msgTimer.Reset(perMsgTimeout)
		case <-msgTimer.C:
			// No message for 10ms — all responders have replied
			return results, nil
		case <-deadline.C:
			// Hard timeout reached
			return results, nil
		case <-ctx.Done():
			return results, ctx.Err()
		}
	}
}
