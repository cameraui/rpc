import { Svcm } from '@nats-io/services';

import { decode } from './codec.js';
import { extractNestedMethodsWithDecorators } from './decorators.js';
import { formatErrorObject, handleNormalRPC, handlePullIteratorRequest, handleStreamRequest } from './handler.js';

import type { Service as NATSService, ServiceClient, ServiceConfig, ServiceGroup, ServiceInfo, ServiceMsg, ServiceStats } from '@nats-io/services';
import type { NatsConnection } from '@nats-io/transport-node';
import type { RPCClient } from './client.js';
import type { RPCResponse } from './types.js';

/**
 * RPC Service wrapper with automatic encoding/decoding and streaming support
 */
export class RPCService {
  private svc?: Svcm;
  private services: NATSService[] = [];
  private nc?: NatsConnection;

  constructor(private client: RPCClient) {}

  /**
   * Initialize the service manager. Re-initializes when the underlying
   * connection changed (suspend()+connect() creates a fresh NatsConnection) —
   * a Svcm bound to the old, closed connection would break monitor() and
   * service discovery after every reconnect cycle.
   */
  public init(nc: NatsConnection): void {
    if (this.nc === nc) {
      return;
    }

    this.nc = nc;
    this.svc = new Svcm(nc);
    this.services = [];
  }

  /**
   * Add a service with automatic RPC endpoint wrapping
   * Supports objects, classes, nested structures, streaming, and chunking
   */
  public async registerHandler(config: ServiceConfig, handlers: object, options?: { isolatedConnection?: boolean }): Promise<Service> {
    if (!this.svc) {
      throw new Error('RPCService is not initialized. Call init() first.');
    }

    // Track pull iterator cleanups for this service
    const pullIteratorCleanups = new Map<string, () => Promise<void>>();

    // Use isolated connection if requested
    let serviceClient = this.client;
    let svc = this.svc;

    if (options?.isolatedConnection) {
      // Create isolated connection for this service
      serviceClient = this.client.createIsolatedClient({
        ...this.client.options,
        name: `${this.client.options.name}-service-${config.name}`,
      });

      // Initialize isolated client
      const nc = await serviceClient.connect();

      // Create service manager for isolated connection
      svc = new Svcm(nc);
    }

    const service = await svc.add(config);
    this.services.push(service);

    // Store reference to isolated connection if used
    if (options?.isolatedConnection) {
      (service as any)._isolatedClient = serviceClient;
    }

    // Extract all methods including nested ones
    const methods = extractNestedMethodsWithDecorators(handlers);

    // Create groups for nested paths
    const groups = new Map<string, ServiceGroup>();

    for (const [path, handler] of Object.entries(methods)) {
      const parts = path.split('.');
      const methodName = parts.pop()!;

      let target: NATSService | ServiceGroup = service;

      // Create nested groups if needed
      if (parts.length > 0) {
        const groupPath = parts.join('.');
        if (!groups.has(groupPath)) {
          let current: NATSService | ServiceGroup = service;
          for (const part of parts) {
            current = current.addGroup(part);
          }
          groups.set(groupPath, current);
        }
        target = groups.get(groupPath)!;
      }

      // Add endpoint with auto-encoding/decoding
      target.addEndpoint(methodName, async (err: Error | null, msg: ServiceMsg) => {
        if (err) {
          msg.respondError(500, err.message);
          return;
        }

        try {
          const chunkType = msg.headers?.get('x-chunked-transfer');

          if (chunkType === 'header') {
            // Chunked transfer header
            const data = decode(msg.data);
            const chunkId = msg.headers?.get('x-chunk-id');

            if (!chunkId || data.transferId !== chunkId) {
              console.error('Invalid chunk header');
              return;
            }

            // Setup chunk assembly with pre-allocated buffer (optimized)
            serviceClient.chunkingManager.startReceiving(
              data.transferId,
              data.totalChunks,
              async (assembledData) => {
                // Process the assembled RPC message
                await processRPCMessage(assembledData, msg);
              },
              (error) => {
                console.error(`Error assembling chunks for ${methodName}:`, error);
              },
              data.totalSize, // Pass totalSize for pre-allocated buffer optimization
              data.chunkSize, // Pass chunkSize for correct offset calculation
            );
          } else if (chunkType === 'chunk') {
            // Chunk data
            const chunkId = msg.headers?.get('x-chunk-id');
            const chunkIndex = parseInt(msg.headers?.get('x-chunk-index') ?? '0');

            if (!chunkId) {
              console.error('Chunk missing chunk ID');
              return;
            }

            // Process raw chunk data
            serviceClient.chunkingManager.processChunk({
              id: chunkId,
              chunkIndex,
              data: msg.data,
              isLast: false, // Determined by total chunks from header
            });
          } else {
            // Regular message - decode MessagePack data
            const rpcMsg = decode(msg.data);
            await processRPCMessage(rpcMsg, msg);
          }
        } catch (error) {
          console.error(`Error processing message for ${methodName}:`, error);
        }

        // Define the RPC message processor function
        async function processRPCMessage(rpcMsg: any, originalMsg: ServiceMsg) {
          const response: RPCResponse = { id: rpcMsg.id };

          try {
            // Handle stream request
            if (rpcMsg.params?.__stream && rpcMsg.params?.__streamSubject) {
              const streamSubject = rpcMsg.params.__streamSubject;
              const args = rpcMsg.params.args ?? [];

              await handleStreamRequest(handler, args, streamSubject, rpcMsg.id, serviceClient);
            } else if (
              // Check if it's a pull iterator request
              // Could be direct object or wrapped in array from call()
              rpcMsg.params?.__pullIterator ||
              (Array.isArray(rpcMsg.params) && rpcMsg.params[0]?.__pullIterator)
            ) {
              // Extract pull iterator params
              const pullParams = rpcMsg.params?.__pullIterator ? rpcMsg.params : rpcMsg.params[0];
              const args = pullParams.args ?? [];
              const iteratorId = pullParams.__iteratorId ?? rpcMsg.id;
              const cleanup = await handlePullIteratorRequest(handler, args, iteratorId, serviceClient, () => pullIteratorCleanups.delete(iteratorId));

              // Store cleanup function for later
              pullIteratorCleanups.set(iteratorId, cleanup);
              response.result = { iteratorId };

              // Send response with iterator ID
              const replySubject = `${originalMsg.subject}.reply.${rpcMsg.id}`;
              await serviceClient.publish(replySubject, response);
            } else {
              // Normal RPC call
              const result = await handleNormalRPC(handler, rpcMsg.params);
              response.result = result;

              // Send response
              const replySubject = `${originalMsg.subject}.reply.${rpcMsg.id}`;
              await serviceClient.publish(replySubject, response);
            }
          } catch (error) {
            response.error = formatErrorObject(error);

            try {
              const replySubject = `${originalMsg.subject}.reply.${rpcMsg.id}`;
              await serviceClient.publish(replySubject, response);
            } catch (publishError) {
              if (serviceClient.isClosed) {
                return; // Ignore if client is closed
              }

              console.error('Failed to send error response:', publishError);
            }
          }
        }
      });
    }

    // Extend service with cleanup management
    const extendedService = service as NATSService & { _pullIteratorCleanups?: Map<string, () => Promise<void>> };
    extendedService._pullIteratorCleanups = pullIteratorCleanups;

    // Override stop method to clean up pull iterators
    const originalStop = service.stop.bind(service);
    service.stop = async (err?: Error) => {
      // Clean up all pull iterators
      await Promise.allSettled(Array.from(pullIteratorCleanups.values()).map((cleanup) => cleanup()));
      pullIteratorCleanups.clear();

      // Call original stop
      return originalStop(err);
    };

    return new Service(service);
  }

  /**
   * Get service monitoring client
   */
  public monitor(): ServiceClient {
    if (!this.svc) {
      throw new Error('RPCService is not initialized. Call init() first.');
    }

    return this.svc.client({ strategy: 'stall', stall: 10, maxWait: 2000 });
  }

  /**
   * Stop all services and cleanup isolated connections
   */
  public async stopAll(): Promise<void> {
    await Promise.allSettled(
      this.services.map(async (s) => {
        await s.stop();
        // Disconnect isolated connection if present
        const isolatedClient: RPCClient | undefined = (s as any)._isolatedClient;
        if (isolatedClient) {
          await isolatedClient.disconnect();
        }
      }),
    );
    this.services = [];
  }

  /**
   * Stop a specific service by name
   */
  public async stop(serviceName: string): Promise<void> {
    const service = this.services.find((s) => s.info().name === serviceName);
    if (service) {
      try {
        await service.stop();
        // Disconnect isolated connection if present
        const isolatedClient: RPCClient | undefined = (service as any)._isolatedClient;
        if (isolatedClient) {
          await isolatedClient.disconnect();
        }
        this.services = this.services.filter((s) => s !== service);
      } catch {
        // Ignore errors during stop
      }
    }
  }

  /**
   * Get all services info
   */
  public getAllInfo(): ServiceInfo[] {
    return this.services.map((s) => s.info());
  }

  /**
   * Get info for a specific service
   */
  public getInfo(serviceName: string): ServiceInfo | undefined {
    const service = this.services.find((s) => s.info().name === serviceName);
    return service?.info();
  }

  /**
   * Get all services stats
   */
  public async getAllStats(): Promise<ServiceStats[]> {
    return Promise.all(this.services.map((s) => s.stats()));
  }

  /**
   * Get stats for a specific service
   */
  public async getStats(serviceName: string): Promise<ServiceStats | undefined> {
    const service = this.services.find((s) => s.info().name === serviceName);
    return service?.stats();
  }
}

export class Service {
  get isStopped(): boolean {
    return this.natsService.isStopped;
  }

  constructor(private natsService: NATSService) {}

  public info(): ServiceInfo {
    return this.natsService.info();
  }

  public stats(): Promise<ServiceStats> {
    return this.natsService.stats();
  }

  public async stop(err?: Error): Promise<void> {
    try {
      await this.natsService.stop(err);
    } catch {
      // Ignore errors during stop
    }
  }
}
