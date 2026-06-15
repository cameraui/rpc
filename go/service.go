package rpc

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/nats-io/nats.go/micro"
)

// ServiceConfig configures a NATS micro service.
type ServiceConfig struct {
	Name        string
	Version     string
	Description string
	Queue       string
	Metadata    map[string]string
}

// RPCService manages NATS micro services with automatic RPC endpoint wrapping.
type RPCService struct {
	client   *Client
	mu       sync.Mutex
	services []micro.Service
}

// NewRPCService creates a new service manager.
func NewRPCService(client *Client) *RPCService {
	return &RPCService{
		client: client,
	}
}

// RegisterHandler registers an RPC handler as a NATS micro service.
func (s *RPCService) RegisterHandler(config ServiceConfig, handler any, isolated ...bool) (*Service, error) {
	client := s.client

	if len(isolated) > 0 && isolated[0] {
		client = s.client.createIsolatedClient("service-" + config.Name)
		if err := client.Connect(context.Background()); err != nil {
			return nil, fmt.Errorf("connect isolated: %w", err)
		}
	}

	nc := client.nc
	if nc == nil {
		return nil, fmt.Errorf("not connected")
	}

	methods := ExtractMethods(handler)

	microCfg := micro.Config{
		Name:        config.Name,
		Version:     config.Version,
		Description: config.Description,
	}
	if config.Queue != "" {
		microCfg.QueueGroup = config.Queue
	}
	if config.Metadata != nil {
		microCfg.Metadata = config.Metadata
	}

	svc, err := micro.AddService(nc, microCfg)
	if err != nil {
		return nil, fmt.Errorf("add service: %w", err)
	}

	s.mu.Lock()
	s.services = append(s.services, svc)
	s.mu.Unlock()

	// Create groups for nested paths
	groups := make(map[string]micro.Group)

	for path, fn := range methods {
		parts := strings.Split(path, ".")
		methodName := parts[len(parts)-1]
		fnCopy := fn

		var target interface {
			AddEndpoint(string, micro.Handler, ...micro.EndpointOpt) error
		}
		target = svc

		if len(parts) > 1 {
			groupPath := strings.Join(parts[:len(parts)-1], ".")
			if _, exists := groups[groupPath]; !exists {
				g := svc.AddGroup(parts[0])
				for i := 1; i < len(parts)-1; i++ {
					g = g.AddGroup(parts[i])
				}
				groups[groupPath] = g
			}
			target = groups[groupPath]
		}

		// processMessage handles a fully decoded RPC message for this endpoint.
		processMessage := func(data []byte, subject string) {
			var msg RPCMessage
			if err := Decode(data, &msg); err != nil {
				return
			}

			response := RPCResponse{ID: msg.ID}

			// Handle stream request
			if isStreamRequest(msg.Params) {
				streamSubject, args := extractStreamParams(msg.Params)
				go handleStreamRequestGo(fnCopy, args, streamSubject, msg.ID, client)
				return
			}

			// Handle pull iterator
			if isPullIteratorRequest(msg.Params) {
				iteratorID, args := extractPullIteratorParams(msg.Params, msg.ID)
				cleanup, err := handlePullIteratorRequestGo(fnCopy, args, iteratorID, client)
				if err != nil {
					response.Error = FormatErrorObject(err)
					replySubject := fmt.Sprintf("%s.reply.%s", subject, msg.ID)
					_ = client.Publish(replySubject, response)
					return
				}
				client.pullIteratorCleanups.Store(iteratorID, cleanup)
				response.Result = map[string]any{"iteratorId": iteratorID}
				replySubject := fmt.Sprintf("%s.reply.%s", subject, msg.ID)
				_ = client.Publish(replySubject, response)
				return
			}

			// Normal RPC
			result, err := callHandler(fnCopy, msg.Params)
			if err != nil {
				response.Error = FormatErrorObject(err)
			} else {
				response.Result = result
			}

			replySubject := fmt.Sprintf("%s.reply.%s", subject, msg.ID)
			_ = client.Publish(replySubject, response)
		}

		err := target.AddEndpoint(methodName, micro.HandlerFunc(func(req micro.Request) {
			headers := req.Headers()

			// Handle chunked transfer
			chunkType := ""
			if headers != nil {
				chunkType = headers.Get("x-chunked-transfer")
			}

			switch chunkType {
			case "header":
				var hdr ChunkedTransferHeader
				if err := Decode(req.Data(), &hdr); err != nil {
					return
				}
				chunkID := ""
				if headers != nil {
					chunkID = headers.Get("x-chunk-id")
				}
				if chunkID == "" || hdr.TransferID != chunkID {
					return
				}
				subject := req.Subject()
				client.chunkingManager.StartReceiving(
					hdr.TransferID, hdr.TotalChunks,
					func(data []byte) { processMessage(data, subject) },
					func(err error) {},
					hdr.TotalSize, hdr.ChunkSize,
				)
				return

			case "chunk":
				chunkID := ""
				chunkIndex := 0
				if headers != nil {
					chunkID = headers.Get("x-chunk-id")
					chunkIndex, _ = strconv.Atoi(headers.Get("x-chunk-index"))
				}
				if chunkID == "" {
					return
				}
				client.chunkingManager.ProcessChunk(ChunkData{
					ID:         chunkID,
					ChunkIndex: chunkIndex,
					Data:       req.Data(),
				})
				return
			}

			// Non-chunked: process directly
			processMessage(req.Data(), req.Subject())
		}))
		if err != nil {
			return nil, fmt.Errorf("add endpoint %s: %w", methodName, err)
		}
	}

	wrapped := &Service{svc: svc}
	if len(isolated) > 0 && isolated[0] {
		wrapped.isolatedClient = client
	}
	return wrapped, nil
}

// GetAllInfo returns info for all registered services.
func (s *RPCService) GetAllInfo() []micro.Info {
	s.mu.Lock()
	defer s.mu.Unlock()

	infos := make([]micro.Info, 0, len(s.services))
	for _, svc := range s.services {
		if !svc.Stopped() {
			infos = append(infos, svc.Info())
		}
	}
	return infos
}

// GetAllStats returns stats for all registered services.
func (s *RPCService) GetAllStats() []micro.Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	stats := make([]micro.Stats, 0, len(s.services))
	for _, svc := range s.services {
		if !svc.Stopped() {
			stats = append(stats, svc.Stats())
		}
	}
	return stats
}

// Monitor returns a ServiceMonitor for discovering services.
func (s *RPCService) Monitor() *ServiceMonitor {
	return NewServiceMonitor(s.client)
}

// StopAll stops all registered services.
func (s *RPCService) StopAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, svc := range s.services {
		_ = svc.Stop()
	}
	s.services = nil
}

// Stop stops a specific service by name.
func (s *RPCService) Stop(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, svc := range s.services {
		if svc.Info().Name == name {
			err := svc.Stop()
			s.services = append(s.services[:i], s.services[i+1:]...)
			return err
		}
	}
	return fmt.Errorf("service %s not found", name)
}

// Service wraps a NATS micro service.
type Service struct {
	svc            micro.Service
	isolatedClient *Client
}

// Info returns the service information.
func (s *Service) Info() micro.Info {
	return s.svc.Info()
}

// Stats returns service statistics.
func (s *Service) Stats() micro.Stats {
	return s.svc.Stats()
}

// Stop stops the service.
func (s *Service) Stop() error {
	err := s.svc.Stop()
	if s.isolatedClient != nil {
		_ = s.isolatedClient.Disconnect()
	}
	return err
}

// Stopped returns true if the service has been stopped.
func (s *Service) Stopped() bool {
	return s.svc.Stopped()
}
