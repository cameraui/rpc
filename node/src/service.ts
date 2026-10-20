import { Svcm } from '@nats-io/services';

import { parseChunkIndex } from './chunking.js';
import { decode, decodeMessage } from './codec.js';
import { extractNestedMethodsWithDecorators } from './decorators.js';
import { formatErrorObject, handleNormalRPC, handleStreamRequest, openSession, probeSettings, SessionOwner, sessionKind } from './handler.js';

import type { Msg } from '@nats-io/nats-core';
import type { Service as NATSService, ServiceClient, ServiceConfig, ServiceGroup, ServiceInfo, ServiceMsg, ServiceStats } from '@nats-io/services';
import type { NatsConnection } from '@nats-io/transport-node';
import type { ChunkingManager } from './chunking.js';
import type { RPCClient } from './client.js';
import type { RPCResponse } from './types.js';

export const DIRECT_SUBJECT_METADATA = 'cui.chunk.direct';

export function directSubject(instanceId: string, path: string): string {
  return `$CUI.SVC.${instanceId}.${path}`;
}

export function serviceSubject(serviceName: string, path: string): string {
  return `svc.${serviceName}.${path}`;
}

export class RPCService {
  private svc?: Svcm;
  private services: NATSService[] = [];
  private nc?: NatsConnection;

  constructor(private client: RPCClient) {}

  public init(nc: NatsConnection): void {
    // suspend() and connect() bring a new connection, a Svcm on the old one breaks monitor() and discovery
    if (this.nc === nc) {
      return;
    }

    this.nc = nc;
    this.svc = new Svcm(nc);
    this.services = [];
  }

  public async registerHandler(config: ServiceConfig, handlers: object, options?: { isolatedConnection?: boolean }): Promise<Service> {
    if (!this.svc) {
      throw new Error('RPCService is not initialized. Call init() first.');
    }

    // the stream and pull sessions of this service, stop() ends them
    const owner = new SessionOwner();
    const probe = probeSettings(this.client.options.orphanProbe);
    const endpointChunking: ChunkingManager[] = [];

    let serviceClient = this.client;
    let svc = this.svc;

    if (options?.isolatedConnection) {
      serviceClient = this.client.createIsolatedClient({
        ...this.client.options,
        name: `${this.client.options.name}-service-${config.name}`,
      });

      const nc = await serviceClient.connect();

      svc = new Svcm(nc);
    }

    const service = await svc.add({ ...config, metadata: { ...config.metadata, [DIRECT_SUBJECT_METADATA]: '1' } });
    this.services.push(service);
    const instanceId = service.info().id;
    const directUnsubscribes: (() => void)[] = [];

    if (options?.isolatedConnection) {
      (service as any)._isolatedClient = serviceClient;
    }

    const methods = extractNestedMethodsWithDecorators(handlers);

    const groups = new Map<string, ServiceGroup>();

    for (const [path, handler] of Object.entries(methods)) {
      const parts = path.split('.');
      const methodName = parts.pop()!;

      let target: NATSService | ServiceGroup = service;

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

      // one manager per endpoint keeps the transfer cap per endpoint
      const chunking = serviceClient.createEndpointChunking();
      endpointChunking.push(chunking);

      const endpointHandler = async (err: Error | null, msg: ServiceMsg): Promise<void> => {
        if (err) {
          msg.respondError(500, err.message);
          return;
        }

        try {
          const chunkType = msg.headers?.get('x-chunked-transfer');

          if (chunkType === 'header') {
            const data = decode(msg.data);
            const chunkId = msg.headers?.get('x-chunk-id');

            if (!chunkId || data?.transferId !== chunkId) {
              console.error('Invalid chunk header');
              return;
            }

            chunking.startReceiving(
              data.transferId,
              data.totalChunks,
              async (assembledData) => {
                await processRPCMessage(assembledData, msg);
              },
              (error) => {
                console.error(`Error assembling chunks for ${methodName}:`, error);
              },
              data.totalSize,
              data.chunkSize,
            );
          } else if (chunkType === 'chunk') {
            const chunkId = msg.headers?.get('x-chunk-id');
            const chunkIndex = parseChunkIndex(msg.headers?.get('x-chunk-index'));

            if (!chunkId) {
              console.error('Chunk missing chunk ID');
              return;
            }

            chunking.processChunk({
              id: chunkId,
              chunkIndex,
              data: msg.data,
              isLast: false, // the header's totalChunks decides
            });
          } else {
            const rpcMsg = decodeMessage(msg.data);
            await processRPCMessage(rpcMsg, msg);
          }
        } catch (error) {
          console.error(`Error processing message for ${methodName}:`, error);
        }

        async function processRPCMessage(rpcMsg: any, originalMsg: ServiceMsg) {
          const response: RPCResponse = { id: rpcMsg.id };

          try {
            const kind = sessionKind(rpcMsg.params);
            if (kind === 'stream') {
              const streamSubject = rpcMsg.params.__streamSubject;
              const args = rpcMsg.params.args ?? [];

              await handleStreamRequest(handler, args, streamSubject, rpcMsg.id, serviceClient, owner, probe);
            } else {
              response.result = kind
                ? await openSession(kind, handler, rpcMsg.params, rpcMsg.id, serviceClient, owner, probe)
                : await handleNormalRPC(handler, rpcMsg.params);

              const replySubject = `${originalMsg.subject}.reply.${rpcMsg.id}`;
              await serviceClient.publish(replySubject, response);
            }
          } catch (error) {
            response.error = formatErrorObject(error);
            // a result that failed to go out would fail the error reply too
            delete response.result;

            try {
              const replySubject = `${originalMsg.subject}.reply.${rpcMsg.id}`;
              await serviceClient.publish(replySubject, response);
            } catch (publishError) {
              if (serviceClient.isClosed) {
                return;
              }

              console.error('Failed to send error response:', publishError);
            }
          }
        }
      };

      target.addEndpoint(methodName, endpointHandler);
      service.addEndpoint(methodName, { handler: endpointHandler, subject: serviceSubject(config.name, path) });
      directUnsubscribes.push(
        serviceClient.subscribeMessages(
          directSubject(instanceId, path),
          (msg: Msg) => {
            endpointHandler(null, msg as ServiceMsg);
          },
          false,
        ),
      );
    }

    // stop() also ends the open sessions and the direct subscriptions
    const originalStop = service.stop.bind(service);
    service.stop = async (err?: Error) => {
      for (const unsubscribe of directUnsubscribes.splice(0)) unsubscribe();
      await Promise.allSettled(owner.stop());

      try {
        return await originalStop(err);
      } finally {
        for (const manager of endpointChunking) serviceClient.releaseEndpointChunking(manager);
      }
    };

    return new Service(service);
  }

  public monitor(): ServiceClient {
    if (!this.svc) {
      throw new Error('RPCService is not initialized. Call init() first.');
    }

    return this.svc.client({ strategy: 'stall', stall: 10, maxWait: 2000 });
  }

  public async stopAll(): Promise<void> {
    await Promise.allSettled(
      this.services.map(async (s) => {
        await s.stop();
        const isolatedClient: RPCClient | undefined = (s as any)._isolatedClient;
        if (isolatedClient) {
          await isolatedClient.disconnect();
        }
      }),
    );
    this.services = [];
  }

  public async stop(serviceName: string): Promise<void> {
    const service = this.services.find((s) => s.info().name === serviceName);
    if (service) {
      try {
        await service.stop();
        const isolatedClient: RPCClient | undefined = (service as any)._isolatedClient;
        if (isolatedClient) {
          await isolatedClient.disconnect();
        }
        this.services = this.services.filter((s) => s !== service);
      } catch {
        // ignore
      }
    }
  }

  public getAllInfo(): ServiceInfo[] {
    return this.services.map((s) => s.info());
  }

  public getInfo(serviceName: string): ServiceInfo | undefined {
    const service = this.services.find((s) => s.info().name === serviceName);
    return service?.info();
  }

  public async getAllStats(): Promise<ServiceStats[]> {
    return Promise.all(this.services.map((s) => s.stats()));
  }

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
      // ignore
    }
  }
}
