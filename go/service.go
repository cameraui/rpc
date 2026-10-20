package rpc

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

const (
	// metadata key: each instance takes chunked requests on $CUI.SVC.<instance id>.<method path>
	chunkDirectKey      = "cui.chunk.direct"
	directSubjectPrefix = "$CUI.SVC."
	svcSubjectPrefix    = "svc."
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
	services []*Service
}

// NewRPCService creates a new service manager.
func NewRPCService(client *Client) *RPCService {
	return &RPCService{
		client: client,
	}
}

// RegisterHandler registers an RPC handler as a NATS micro service.
func (s *RPCService) RegisterHandler(config ServiceConfig, handler any, isolated ...bool) (_ *Service, err error) {
	client := s.client

	if len(isolated) > 0 && isolated[0] {
		// a service is bound to its connection, a parent's Suspend and Connect could not move it
		client = s.client.createIsolatedClient("service-" + config.Name)
		defer func() {
			if err != nil {
				_ = client.Disconnect()
			}
		}()
		if err := client.Connect(context.Background()); err != nil {
			return nil, fmt.Errorf("connect isolated: %w", err)
		}
	}

	client.mu.RLock()
	nc := client.nc
	client.mu.RUnlock()
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
	microCfg.Metadata = make(map[string]string, len(config.Metadata)+1)
	maps.Copy(microCfg.Metadata, config.Metadata)
	microCfg.Metadata[chunkDirectKey] = "1"

	svc, err := micro.AddService(nc, microCfg)
	if err != nil {
		return nil, fmt.Errorf("add service: %w", err)
	}
	directPrefix := directSubjectPrefix + svc.Info().ID + "."

	// stopping one service never ends another service's sessions
	owner := newSessionOwner()
	orphan := client.orphanConfig(nil)

	groups := make(map[string]micro.Group)
	endpointChunks := make([]*ChunkingManager, 0, len(methods))
	directSubs := make([]*nats.Subscription, 0, len(methods))
	release := func() {
		owner.stop()
		for _, sub := range directSubs {
			_ = sub.Unsubscribe()
		}
		for _, cm := range endpointChunks {
			client.removeEndpointChunks(cm)
			cm.Close()
		}
	}

	for path, fn := range methods {
		parts := strings.Split(path, ".")
		methodName := parts[len(parts)-1]
		fnCopy := fn
		takesCtx := takesContext(fn.Type())

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

		processMessage := func(data []byte, subject string) {
			var msg RPCMessage
			if err := DecodeMessageInto(data, &msg); err != nil {
				return
			}

			response := RPCResponse{ID: msg.ID}

			if isStreamRequest(msg.Params) {
				streamSubject, args := extractStreamParams(msg.Params)
				go handleStreamRequestGo(owner, fnCopy, args, streamSubject, msg.ID, client, orphan, false)
				return
			}

			replySubject := fmt.Sprintf("%s.reply.%s", subject, msg.ID)
			reply := func(result any, err error) {
				if err != nil {
					response.Error = FormatErrorObject(err)
				} else {
					response.Result = result
				}
				if pubErr := client.Publish(replySubject, response); pubErr != nil && client.IsConnected() {
					_ = client.Publish(replySubject, RPCResponse{ID: msg.ID, Error: FormatErrorObject(pubErr)})
				}
			}

			if isPullIteratorRequest(msg.Params) {
				iteratorID, args := extractPullIteratorParams(msg.Params, msg.ID)
				cleanup, err := handlePullIteratorRequestGo(owner.ctx, fnCopy, args, iteratorID, client, orphan, func() {
					owner.untrack(&client.pullIteratorCleanups, iteratorID)
				})
				if err == nil && cleanup != nil {
					err = owner.keep(&client.pullIteratorCleanups, iteratorID, cleanup)
				}
				reply(map[string]any{"iteratorId": iteratorID}, err)
				return
			}

			if isPullCallbackRequest(msg.Params) {
				iteratorID, callbackSubject, oneway, args := extractPullCallbackParams(msg.Params, msg.ID)
				cleanup, err := handlePullCallbackRequestGo(owner.ctx, fnCopy, args, iteratorID, callbackSubject, oneway, client, orphan, func() {
					owner.untrack(&client.pullIteratorCleanups, iteratorID)
				})
				if err == nil && cleanup != nil {
					err = owner.keep(&client.pullIteratorCleanups, iteratorID, cleanup)
				}
				reply(map[string]any{"iteratorId": iteratorID}, err)
				return
			}

			if isCallbackRequest(msg.Params) {
				callbackSubject, args := extractCallbackParams(msg.Params)
				requestID := msg.ID
				cleanup, err := handleCallbackRequestGo(owner.ctx, fnCopy, args, callbackSubject, requestID, client, orphan, func() {
					owner.untrack(&client.callbackCleanups, requestID)
				})
				if err == nil {
					err = owner.keep(&client.callbackCleanups, requestID, cleanup)
				}
				reply(map[string]any{"ok": true}, err)
				return
			}

			ctx, cancel := callContext(owner.ctx, takesCtx)
			defer cancel()
			reply(callHandler(ctx, fnCopy, msg.Params))
		}

		chunks := newChunkingManager(client.Options.MaxTransferSize, client.chunkBudget)
		client.addEndpointChunks(chunks)
		endpointChunks = append(endpointChunks, chunks)
		handle := func(headers nats.Header, data []byte, subject string) {
			chunkType := ""
			if headers != nil {
				chunkType = headers.Get("x-chunked-transfer")
			}

			switch chunkType {
			case "header":
				hdr, ok := decodeChunkHeader(headers, data)
				if !ok {
					return
				}
				chunks.StartReceiving(
					hdr.TransferID, hdr.TotalChunks,
					func(data []byte) { processMessage(data, subject) },
					func(err error) {},
					hdr.TotalSize, hdr.ChunkSize,
				)
				return

			case "chunk":
				chunk, err := decodeChunk(headers, data)
				if err != nil {
					chunks.fail(chunk.ID, err)
					return
				}
				chunks.ProcessChunk(chunk)
				return
			}

			processMessage(data, subject)
		}
		endpoint := micro.HandlerFunc(func(req micro.Request) {
			handle(nats.Header(req.Headers()), req.Data(), req.Subject())
		})

		// unbounded like the reply mux: a slow consumer error would stop the whole service
		unbounded := micro.WithEndpointPendingLimits(-1, -1)
		err := target.AddEndpoint(methodName, endpoint, unbounded)
		if err == nil {
			// services sharing a method name each keep their own subject
			err = svc.AddEndpoint(methodName, endpoint, unbounded, micro.WithEndpointSubject(svcSubjectPrefix+config.Name+"."+path))
		}
		if err == nil {
			var sub *nats.Subscription
			sub, err = nc.Subscribe(directPrefix+path, func(msg *nats.Msg) { handle(msg.Header, msg.Data, msg.Subject) })
			if err == nil {
				_ = sub.SetPendingLimits(-1, -1)
				directSubs = append(directSubs, sub)
			}
		}
		if err != nil {
			_ = svc.Stop()
			release()
			return nil, fmt.Errorf("add endpoint %s: %w", methodName, err)
		}
	}

	wrapped := &Service{
		svc:          svc,
		stopCleanups: release,
	}
	if len(isolated) > 0 && isolated[0] {
		wrapped.isolatedClient = client
	}

	s.mu.Lock()
	s.services = append(s.services, wrapped)
	s.mu.Unlock()

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
	stopCleanups   func()
}

// Info returns the service information.
func (s *Service) Info() micro.Info {
	return s.svc.Info()
}

// Stats returns service statistics.
func (s *Service) Stats() micro.Stats {
	return s.svc.Stats()
}

// Stop stops the service and ends its sessions, their clients get a CONNECTION_CLOSED error.
func (s *Service) Stop() error {
	if s.stopCleanups != nil {
		s.stopCleanups()
	}
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
