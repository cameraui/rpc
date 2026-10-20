import { connect, createInbox, errors, headers } from '@nats-io/transport-node';

import { Channel, PrivateChannel } from './channel.js';
import { ChunkingManager, createChunks, parseChunkIndex, TransferBudget } from './chunking.js';
import { assembleFrame, decode, decodeMessage, encode, encodeMessage, encodeMessageParts } from './codec.js';
import { extractNestedMethodsWithDecorators, extractNestedMethodsWithoutDecorators } from './decorators.js';
import { RPCException, createError, markLocalNoResponders } from './errors.js';
import { callNormalRPC, formatErrorObject, handleStreamRequest, openSession, outsideSession, probeSettings, SessionOwner, sessionKind } from './handler.js';
import { flushOutbound, packSmallPublishes, presizeSplitPayloads, PUBLISH_FRAME_MIN_SIZE, publishFrame, publishFrameZeroCopy, shareReadBuffer } from './patches.js';
import { RPCService } from './service.js';
import { ERROR_CODES } from './types.js';
import { createBoundedDispatcher, createProxy, createSerialDispatcher, createServiceProxy, generateId, generateReplyPrefix, sleep } from './utils.js';

import type { Msg, MsgHdrs, NatsConnection, Status, Subscription } from '@nats-io/nats-core';
import type { ServiceInfo } from '@nats-io/services';
import type { NodeConnectionOptions } from '@nats-io/transport-node';
import type { MethodHandler, SessionKind } from './handler.js';
import type {
  CallbackInvocation,
  CallbackMessage,
  CallbackParams,
  ChunkedTransferHeader,
  OrphanProbeOptions,
  Promisify,
  PullCallbackCallOptions,
  PullCallbackParams,
  PullIteratorRequest,
  PullIteratorResponse,
  RPCAuthOptions,
  RPCCallTiming,
  RPCClient as RPCClientImpl,
  RPCClientOptions,
  RPCMessage,
  RPCResponse,
  RPCTiming,
  ProxyCallOptions,
  ProxyTimingReporter,
  StreamMessage,
} from './types.js';

// flush every 8th inline reply so callers with many calls in flight get their first replies early
const SYNC_REPLY_FLUSH_EVERY = 8;
const DEFAULT_DRAIN_TIMEOUT_MS = 250;
const SUBSCRIBE_FLUSH_TIMEOUT_MS = 2000;
const METHODS_PROBE = '__methods';
// used when the caller sets neither delays nor maxRetries, about 3.5 s in total
const DEFAULT_RETRY_DELAYS = [25, 50, 100, 325, 1000, 2000];
// for a caller that sets only one of delays and maxRetries
const LEGACY_RETRY_DELAYS = [500, 1000, 2000];
const LEGACY_MAX_RETRIES = 3;
// the probe gets this long even when the first retry delay is shorter
const METHOD_PROBE_WINDOW_MS = 500;
const EXPIRY_SLACK_MS = 5;
// NATS protocol overhead and headers per message
const PAYLOAD_RESERVE = 8192;
// below this nothing is chunked, so a chunk carries at least PAYLOAD_RESERVE bytes
const MIN_PAYLOAD_LIMIT = 16384;
// lets the sending channel end skip its own request in onRequest
const SENDER_HEADER = 'x-channel-sender';

export function createRPCClient(options: RPCClientOptions): RPCClientImpl {
  return new RPCClient(options);
}

interface ChunkScope {
  key: string;
  members: number;
  manager?: ChunkingManager;
  lanes?: Map<string, ChunkingManager>;
}

interface SubscriptionEntry {
  pattern: string;
  handler: (data: any) => void | Promise<void>;
  options?: { queue?: string };
  chunks?: ChunkScope;
  raw?: boolean;
  sub?: Subscription;
  flush?: () => Promise<void>;
  concurrency?: { limit: number; isShared: (data: any) => boolean };
  dispatcher?: { dispatch: (data: any) => void; flush: () => Promise<void> };
  receive?: (data: any) => void;
  route?: (data: any, subject: string) => void;
  lane?: (subject: string) => string | undefined;
}

interface Responder {
  entries: SubscriptionEntry[];
  methods?: SubscriptionEntry;
  open?: Set<RPCMessage>;
  onIdle?: () => void;
  draining?: Promise<void>;
}

interface MethodProbe {
  probeNamespace?: string;
  probeMethod?: string;
  onMethods?: (methods: string[]) => void;
}

function scopedInbox(connId?: string): string {
  return createInbox(connId ? `_INBOX.${connId}` : undefined);
}

export class RPCClient implements RPCClientImpl {
  public readonly service = new RPCService(this);
  public chunkingManager: ChunkingManager;
  private sessionTeardowns = new Set<() => Promise<void>>();
  private responders = new Set<Responder>();

  private nc?: NatsConnection;
  private closingNc?: NatsConnection;
  private subscriptionSeq = 0;
  private subscriptionEntries = new Map<number, SubscriptionEntry>();
  private pullIteratorSettles = new Set<() => void>();

  private readonly replyPrefix: string;

  private muxEntry?: SubscriptionEntry;

  private muxChunking: ChunkingManager;
  private chunkBudget: TransferBudget;
  private chunkScopes = new Map<string, ChunkScope>();
  private chunkScopeSeq = 0;
  private endpointChunking = new Set<ChunkingManager>();

  private statusHandlers = new Map<string, (err: Error) => void>();
  private _maxPayloadSize: number = 1024 * 1024;
  private unchunkableLimit?: number;
  private connectionPromise?: Promise<NatsConnection>;
  private closed = false;

  private isolatedClients: RPCClient[] = [];

  private syncReplies = 0;

  private linkWaiters?: (() => void)[];

  private pendingRequests = new Map<
    string,
    {
      resolve: (value: any) => void;
      reject: (error: any) => void;
      timeout?: NodeJS.Timeout;
      cleanup?: () => void;
      subject?: string;
      transferKey?: string;
    }
  >();

  private streamHandlers = new Map<
    string,
    {
      push: (value: any) => void;
      end: () => void;
      error: (error: any) => void;
      subject?: string;
      refuse?: (error: RPCException) => void;
    }
  >();

  get isConnected(): boolean {
    return this.nc?.isClosed() === false;
  }

  get isStale(): boolean {
    // isStale() exists only in the camera.ui nats.js fork
    const nc = this.nc as unknown as { isStale?: () => boolean } | undefined;
    return nc?.isStale?.() ?? false;
  }

  get isClosed(): boolean {
    return this.closed;
  }

  get maxPayloadSize(): number {
    return this._maxPayloadSize;
  }

  public status(): AsyncIterable<Status> | undefined {
    return this.nc?.status();
  }

  public async flush(timeoutMs = 5000): Promise<void> {
    if (!this.nc) {
      throw createError(ERROR_CODES.CONNECTION_CLOSED, 'Not connected');
    }
    let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        this.nc.flush(),
        new Promise<never>((_, reject) => {
          timeoutHandle = setTimeout(() => reject(createError(ERROR_CODES.TIMEOUT, `flush() timed out after ${timeoutMs}ms`)), timeoutMs);
        }),
      ]);
    } finally {
      if (timeoutHandle) clearTimeout(timeoutHandle);
    }
  }

  constructor(public options: RPCClientOptions) {
    const cap = options.maxPayloadSize;
    if (cap !== undefined && cap !== 0 && !(Number.isSafeInteger(cap) && cap >= MIN_PAYLOAD_LIMIT)) {
      throw new RangeError(`maxPayloadSize must be an integer of at least ${MIN_PAYLOAD_LIMIT}, got ${cap}`);
    }
    // with a connId the prefix must equal it, the firewall allowlists rpc.reply.<connId>.>
    this.replyPrefix = options.connId ?? generateReplyPrefix();
    this.chunkBudget = new TransferBudget(options.maxTransferSize);
    this.chunkingManager = this.newChunkingManager();
    // no transfer cap: pending calls bound the reply transfers
    this.muxChunking = this.newChunkingManager(Infinity);
  }

  public createIsolatedClient(options: RPCClientOptions): RPCClient {
    return new RPCClient(options);
  }

  public async connect(options?: { signal?: AbortSignal }): Promise<NatsConnection> {
    if (options?.signal?.aborted) throw new DOMException('Aborted', 'AbortError');

    presizeSplitPayloads();
    packSmallPublishes();
    shareReadBuffer();

    if (this.nc && !this.nc.isClosed()) {
      return this.nc;
    }

    const natsOptions = {
      servers: this.options.servers,
      user: this.options.auth?.user,
      pass: this.options.auth?.password,
      name: this.options.name,
      inboxPrefix: this.options.connId ? `_INBOX.${this.options.connId}` : undefined,
      reconnect: this.options.reconnect ?? true,
      timeout: this.options.connectTimeout ?? 10_000,
      maxPingOut: this.options.maxPingOut ?? 2,
      maxReconnectAttempts: this.options.maxReconnectAttempts ?? -1,
      reconnectTimeWait: this.options.reconnectTimeWait ?? 2000,
      reconnectJitter: this.options.reconnectJitter ?? 100,
      reconnectJitterTLS: this.options.reconnectJitterTLS ?? 1000,
      ignoreAuthErrorAbort: this.options.ignoreAuthErrorAbort ?? false,
      pingInterval: this.options.pingInterval ?? 120000,
      pingTimeout: this.options.pingTimeout ?? 0,
      reconnectionDelayMax: this.options.reconnectionDelayMax ?? 0,
      reconnectionRandomizationFactor: this.options.reconnectionRandomizationFactor ?? 0,
      tls: this.options.tls,
      debug: this.options.debug ?? false,
      noAsyncTraces: true,
      waitOnFirstConnect: this.options.waitOnFirstConnect ?? true,
      signal: options?.signal,
    } as unknown as NodeConnectionOptions;
    this.connectionPromise ??= outsideSession(() => connect(natsOptions));

    const innerPromise = this.connectionPromise;

    try {
      this.nc = await makeAbortableConnect(innerPromise, options?.signal);
    } finally {
      this.connectionPromise = undefined;
    }

    const nc = this.nc;
    const lost = (): void => this.onConnectionLost(nc);
    nc.closed().then(lost, lost);
    // a hold the previous connection left behind would never be released
    this.releaseLinkWaiters();
    this.watchLink(nc);

    // a disconnected client revives here, otherwise auto-connect and the no-responder retry stay off
    this.closed = false;

    this.service.init(this.nc);

    let serverLimit: number | undefined;
    try {
      serverLimit = this.nc.info?.max_payload;
    } catch {
      // no server info on this transport
    }

    const limit = payloadLimit(serverLimit, this.options.maxPayloadSize);
    // below MIN_PAYLOAD_LIMIT nothing is chunked, a bigger message fails in publishChunks
    this.unchunkableLimit = limit < MIN_PAYLOAD_LIMIT ? limit : undefined;
    this._maxPayloadSize = this.unchunkableLimit ?? limit - PAYLOAD_RESERVE;

    // registers only, the restore loop below subscribes it on first connect and after a suspend
    this.ensureMuxSubscription(false);

    // entries keep their identity, so unsubscribe closures held by callers stay valid
    let restored = 0;
    for (const entry of this.subscriptionEntries.values()) {
      if (!entry.sub || entry.sub.isClosed()) {
        this.natsSubscribe(entry);
        if (entry !== this.muxEntry) restored++;
      }
    }
    if (restored > 0) await this.awaitSubscriptions();

    // isolated clients that suspend() took down come back with their subscriptions
    await Promise.allSettled(this.isolatedClients.filter((child) => !child.isClosed).map((child) => child.connect({ signal: options?.signal })));

    return nc;
  }

  public async disconnect(): Promise<void> {
    // before anything closes: a handler that finishes may still make calls
    const drained = await this.drainAll();
    this.closed = true;
    this.closingNc = this.nc;

    const closedRejects: (() => void)[] | undefined = this.options.zeroCopyBinaries === true ? [] : undefined;
    const sessions = this.dropConnectionState(true, closedRejects);
    const rejectClosed = this.rejectAfterQueuedWrites(closedRejects);
    this.subscriptionEntries.clear();
    this.responders.clear();
    // entries are gone, the next connect() must register the mux again
    this.muxEntry = undefined;
    this.chunkScopes.clear();
    await Promise.allSettled([...drained, ...sessions]);

    await Promise.allSettled(this.isolatedClients.map((client) => client.disconnect()));
    this.isolatedClients = [];

    if (this.nc) {
      await this.flushBeforeClose(this.nc);
      try {
        await this.nc.close();
      } catch {
        // ignore
      }

      await this.waitClosed(this.nc);
      this.nc = undefined;
    }
    rejectClosed();
  }

  public reconfigure(overrides: { servers?: string[]; auth?: RPCAuthOptions }): void {
    if (this.nc && !this.nc.isClosed()) {
      throw new Error('Cannot reconfigure while connected. Call suspend() first.');
    }
    if (overrides.servers !== undefined) {
      this.options.servers = overrides.servers;
    }
    if (overrides.auth !== undefined) {
      this.options.auth = overrides.auth;
    }
    for (const child of this.isolatedClients) {
      child.reconfigure(overrides);
    }
  }

  public setServers(servers: string[]): void {
    this.options.servers = servers;
    if (this.nc && !this.nc.isClosed()) {
      this.nc.setServers(servers);
    }
    for (const child of this.isolatedClients) {
      child.setServers(servers);
    }
  }

  public async forceReconnect(): Promise<void> {
    const nc = this.nc as unknown as { forceReconnect?: () => Promise<void>; reconnect?: () => Promise<void>; isClosed: () => boolean } | undefined;
    if (!nc || nc.isClosed()) return;
    try {
      if (typeof nc.forceReconnect === 'function') {
        await nc.forceReconnect();
      } else if (typeof nc.reconnect === 'function') {
        await nc.reconnect();
      }
    } catch {
      // the connection may have raced into closed
    }
    await Promise.allSettled(this.isolatedClients.map((c) => c.forceReconnect()));
  }

  public abortClose(err?: Error): void {
    this.closed = true;
    this.closingNc = this.nc;
    Promise.allSettled(this.dropConnectionState(false));

    for (const child of this.isolatedClients) {
      try {
        child.abortClose(err);
      } catch {
        // ignore
      }
    }
    this.isolatedClients = [];
    const nc = this.nc as unknown as { abortClose?: (e?: Error) => void; close: () => Promise<void> } | undefined;
    if (nc) {
      try {
        if (typeof nc.abortClose === 'function') {
          nc.abortClose(err);
        } else {
          // server transport: fire-and-forget, the caller wants synchronous semantics
          nc.close().catch(() => {});
        }
      } catch {
        // ignore
      }
      this.nc = undefined;
    }
  }

  public async suspend(): Promise<void> {
    const drained = await this.drainAll();
    this.closingNc = this.nc;
    const closedRejects: (() => void)[] | undefined = this.options.zeroCopyBinaries === true ? [] : undefined;
    // entries stay, connect() restores them on the fresh transport
    const sessions = this.dropConnectionState(true, closedRejects);
    const rejectClosed = this.rejectAfterQueuedWrites(closedRejects);
    await Promise.allSettled([...drained, ...sessions]);

    await Promise.allSettled(this.isolatedClients.map((client) => client.suspend()));

    if (this.nc) {
      await this.flushBeforeClose(this.nc);
      try {
        await this.nc.close();
      } catch {
        // ignore
      }

      await this.waitClosed(this.nc);
      this.nc = undefined;
    }

    this.connectionPromise = undefined;
    rejectClosed();
  }

  public async publish<TMessage = any>(subject: string, data: TMessage, opts?: { headers?: MsgHdrs; reply?: string }): Promise<void> {
    return this.publishOrChunk(subject, data, opts);
  }

  public publishOrChunk<TMessage = any>(subject: string, data: TMessage, opts?: { headers?: MsgHdrs; reply?: string }, zeroCopy = false): Promise<void> | undefined {
    if (!this.nc) {
      throw new Error('Not connected');
    }

    const encoded = this.encodeOrPublishFrame(subject, data, opts, zeroCopy);
    if (!encoded) {
      return undefined;
    }

    if (encoded.length <= this._maxPayloadSize) {
      this.nc.publish(subject, encoded, opts);
      return undefined;
    }

    return this.publishChunks(this.nc, subject, encoded, opts);
  }

  public tryPublishSync<TMessage = any>(subject: string, data: TMessage, opts?: { headers?: MsgHdrs; reply?: string }): boolean {
    if (!this.nc) {
      throw new Error('Not connected');
    }

    const encoded = this.encodeOrPublishFrame(subject, data, opts);
    if (!encoded) {
      return true;
    }
    if (encoded.length > this._maxPayloadSize) {
      return false;
    }

    this.nc.publish(subject, encoded, opts);
    return true;
  }

  public createEndpointChunking(): ChunkingManager {
    const manager = this.newChunkingManager();
    this.endpointChunking.add(manager);
    return manager;
  }

  public releaseEndpointChunking(manager: ChunkingManager): void {
    manager.dispose();
    this.endpointChunking.delete(manager);
  }

  public trackSession(teardown: () => Promise<void>): () => void {
    this.sessionTeardowns.add(teardown);
    return () => {
      this.sessionTeardowns.delete(teardown);
    };
  }

  public statusReply(onNoResponders: () => void): { reply: string; forget: () => void } {
    this.ensureMuxSubscription();
    const key = generateId(this.replyPrefix);
    this.statusHandlers.set(key, onNoResponders);
    return {
      reply: `rpc.reply.${key}`,
      forget: () => {
        if (this.statusHandlers.get(key) === onNoResponders) this.statusHandlers.delete(key);
      },
    };
  }

  public subscribeMessages(pattern: string, handler: (msg: Msg) => void, restore = true): () => void {
    if (!this.nc) {
      throw new Error('Not connected');
    }
    const key = ++this.subscriptionSeq;
    const entry: SubscriptionEntry = { pattern, raw: true, handler: (msg: any) => handler(msg as Msg) };
    if (restore) this.subscriptionEntries.set(key, entry);
    this.natsSubscribe(entry);
    return () => {
      try {
        if (!entry.sub?.isClosed()) entry.sub?.unsubscribe();
      } catch {
        // ignore
      } finally {
        this.subscriptionEntries.delete(key);
      }
    };
  }

  public async subscribe<TResponse = any>(pattern: string, handler: (data: TResponse) => void | Promise<void>, options?: { queue?: string }): Promise<() => void> {
    const { unsubscribe } = await this.subscribeEntry(pattern, handler, options);
    return unsubscribe;
  }

  private async subscribeEntry<TResponse = any>(
    pattern: string,
    handler: (data: TResponse) => void | Promise<void>,
    options?: { queue?: string },
    concurrency?: SubscriptionEntry['concurrency'],
    receive?: SubscriptionEntry['receive'],
  ): Promise<{ entry: SubscriptionEntry; unsubscribe: () => void }> {
    return this.addSubscription(pattern, handler, options, concurrency, receive);
  }

  private addSubscription<TResponse = any>(
    pattern: string,
    handler: (data: TResponse) => void | Promise<void>,
    options?: { queue?: string },
    concurrency?: SubscriptionEntry['concurrency'],
    receive?: SubscriptionEntry['receive'],
    route?: SubscriptionEntry['route'],
    lane?: SubscriptionEntry['lane'],
  ): { entry: SubscriptionEntry; unsubscribe: () => void } {
    if (!this.nc) {
      throw new Error('Not connected');
    }

    const key = ++this.subscriptionSeq;
    const entry: SubscriptionEntry = { pattern, handler: handler as (data: any) => void | Promise<void>, options, concurrency, receive, route, lane };
    this.subscriptionEntries.set(key, entry);
    this.natsSubscribe(entry);

    const unsubscribe = () => {
      try {
        // a drained subscription is gone on the server already
        if (!entry.sub?.isClosed()) entry.sub?.unsubscribe();
      } catch {
        // ignore
      } finally {
        this.subscriptionEntries.delete(key);
        this.leaveChunkScope(entry);
      }
    };

    return { entry, unsubscribe };
  }

  private natsSubscribe(entry: SubscriptionEntry): void {
    const { pattern } = entry;

    // raw: the mux reads headers and subject and reassembles chunks itself
    if (entry.raw) {
      entry.sub = this.nc!.subscribe(pattern, {
        callback: (err, msg) => {
          if (err) {
            console.error(`Subscription error for ${pattern}:`, err);
            return;
          }
          try {
            entry.handler(msg);
          } catch (error) {
            console.error(`Error processing message for ${pattern}:`, error);
          }
        },
      });
      return;
    }

    // serial per subscription: an awaiting handler holds back the next message, callers rely on that for backpressure
    let runHandler: (data: any, msg: Msg) => void;
    if (entry.route) {
      const { route } = entry;
      // only a route reads the subject: nats.js decodes it on first access
      runHandler = (data: any, msg: Msg): void => route(data, msg.subject);
    } else {
      const onError = (error: unknown) => console.error(`Error in handler for ${pattern}:`, error);
      const dispatcher = entry.concurrency
        ? (entry.dispatcher ??= createBoundedDispatcher(entry.handler, entry.concurrency.limit, entry.concurrency.isShared, onError))
        : createSerialDispatcher(entry.handler, onError);
      const { receive } = entry;
      const dispatch = dispatcher.dispatch;
      runHandler = receive
        ? (data: any): void => {
            receive(data);
            dispatch(data);
          }
        : dispatch;

      entry.flush = dispatcher.flush;
    }

    const chunks = (entry.chunks ??= this.joinChunkScope(entry));
    const { lane } = entry;

    const processMessage = (err: Error | null, msg: Msg) => {
      if (err) {
        console.error(`Subscription error for ${pattern}:`, err);
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

          this.chunkManager(chunks, lane?.(msg.subject), true)!.startReceiving(
            chunkId,
            data.totalChunks,
            (assembledData) => {
              runHandler(assembledData, msg);
            },
            (error) => {
              console.error(`Error assembling chunks for ${pattern}:`, error);
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

          this.chunkManager(chunks, lane?.(msg.subject), false)?.processChunk({
            id: chunkId,
            chunkIndex,
            data: msg.data,
            isLast: false, // the manager knows the count from the header
          });
        } else {
          // the binary views into msg.data are safe, nats.js allocates one buffer per message
          const data = decodeMessage(msg.data);
          runHandler(data, msg);
        }
      } catch (error) {
        console.error(`Error processing message for ${pattern}:`, error);
      }
    };

    entry.sub = this.nc!.subscribe(pattern, {
      ...(entry.options?.queue ? { queue: entry.options.queue } : {}),
      callback: processMessage,
    });
  }

  private ensureMuxSubscription(subscribeNow = true): void {
    if (!this.muxEntry) {
      this.muxEntry = {
        pattern: `rpc.reply.${this.replyPrefix}.>`,
        raw: true,
        handler: (msg: any) => this.handleMuxMessage(msg as Msg),
      };
      this.subscriptionEntries.set(++this.subscriptionSeq, this.muxEntry);
    }
    if (subscribeNow && this.nc && (!this.muxEntry.sub || this.muxEntry.sub.isClosed())) {
      this.natsSubscribe(this.muxEntry);
    }
  }

  private handleMuxMessage(msg: Msg): void {
    // the reply subject is rpc.reply.<call id>, or the registered token for iterators and streams
    if (msg.data?.length === 0 && msg.headers?.code === 503) {
      const suffix = msg.subject.slice('rpc.reply.'.length);

      const statusHandler = this.statusHandlers.get(suffix);
      if (statusHandler) {
        this.statusHandlers.delete(suffix);
        try {
          statusHandler(new errors.NoRespondersError(suffix));
        } catch (error) {
          console.error('Error in no-responder status handler:', error);
        }
        return;
      }

      const pending = this.pendingRequests.get(suffix);
      if (pending) {
        this.pendingRequests.delete(suffix);
        if (pending.timeout) clearTimeout(pending.timeout);
        pending.reject(noRespondersError(pending.subject ?? suffix));
      }
      return;
    }

    const chunkType = msg.headers?.get('x-chunked-transfer');

    if (chunkType === 'header' || chunkType === 'chunk') {
      const callId = msg.subject.slice('rpc.reply.'.length);
      const pending = this.pendingRequests.get(callId);
      if (!pending) return;

      const chunkId = msg.headers?.get('x-chunk-id');
      if (!chunkId) {
        console.error(chunkType === 'header' ? 'Invalid chunk header on reply mux' : 'Chunk missing chunk ID on reply mux');
        return;
      }
      const transferKey = `${callId}|${chunkId}`;

      if (chunkType === 'header') {
        const data = decode(msg.data);
        if (data?.transferId !== chunkId) {
          console.error('Invalid chunk header on reply mux');
          return;
        }
        // one transfer per call, so pending calls bound the uncapped manager
        if (pending.transferKey && pending.transferKey !== transferKey) this.muxChunking.discard(pending.transferKey);
        pending.transferKey = transferKey;
        this.muxChunking.startReceiving(
          transferKey,
          data.totalChunks,
          (assembledData) => this.routeMuxResponse(assembledData),
          (error) => this.failChunkedReply(callId, error),
          data.totalSize,
          data.chunkSize,
        );
      } else {
        const chunkIndex = parseChunkIndex(msg.headers?.get('x-chunk-index'));
        this.muxChunking.processChunk({ id: transferKey, chunkIndex, data: msg.data, isLast: false });
      }
    } else {
      this.routeMuxResponse(decodeMessage(msg.data));
    }
  }

  private routeMuxResponse(data: any): void {
    const response = data as RPCResponse;
    const id = response?.id;
    if (!id) return;

    const pending = this.pendingRequests.get(id);
    if (!pending) return;

    this.pendingRequests.delete(id);
    if (pending.timeout) clearTimeout(pending.timeout);
    if (pending.transferKey) this.muxChunking.discard(pending.transferKey);

    if (response.error) {
      pending.reject(RPCException.fromJSON(response.error));
    } else {
      pending.resolve({ result: response.result, methods: response.__methods, timing: response.__timing });
    }
  }

  private failChunkedReply(callId: string, error: Error): void {
    const pending = this.pendingRequests.get(callId);
    if (!pending) return;

    this.pendingRequests.delete(callId);
    if (pending.timeout) clearTimeout(pending.timeout);
    if (pending.transferKey) this.muxChunking.discard(pending.transferKey);
    pending.reject(createError(ERROR_CODES.INTERNAL_ERROR, `Chunked reply to "${pending.subject ?? callId}" failed: ${error.message}`));
  }

  private settlePullIterators(): void {
    for (const settle of [...this.pullIteratorSettles]) {
      try {
        settle();
      } catch {
        // ignore
      }
    }
    this.pullIteratorSettles.clear();
  }

  private dropConnectionState(unsubscribe: boolean, closedRejects?: (() => void)[]): Promise<void>[] {
    const sessions = this.endHandlerSessions();

    for (const pending of this.pendingRequests.values()) {
      if (pending.timeout) clearTimeout(pending.timeout);
      pending.cleanup?.();
      const reject = (): void => pending.reject(createError(ERROR_CODES.CONNECTION_CLOSED, 'Connection closed'));
      if (closedRejects) closedRejects.push(reject);
      else reject();
    }
    this.pendingRequests.clear();
    this.statusHandlers.clear();
    // held requests find their call gone and send nothing
    this.releaseLinkWaiters();

    for (const handler of this.streamHandlers.values()) {
      try {
        handler.end();
      } catch {
        // ignore
      }
    }
    this.streamHandlers.clear();
    this.settlePullIterators();

    for (const entry of this.subscriptionEntries.values()) {
      // a drained subscription is gone on the server already
      if (unsubscribe && !entry.sub?.isClosed()) {
        try {
          entry.sub?.unsubscribe();
        } catch {
          // ignore
        }
      }
      entry.sub = undefined;
    }
    this.disposeChunking();
    return sessions;
  }

  private rejectAfterQueuedWrites(rejects: (() => void)[] | undefined): () => void {
    const rejectAll = (): void => {
      for (const reject of rejects?.splice(0) ?? []) reject();
    };
    if (rejects?.length && this.nc && !this.nc.isClosed()) {
      this.nc.flush().then(rejectAll, () => {});
    }
    return rejectAll;
  }

  private endHandlerSessions(): Promise<void>[] {
    const cleanups = [...this.sessionTeardowns];
    this.sessionTeardowns.clear();
    return cleanups.map((cleanup) => cleanup());
  }

  private async drainAll(): Promise<Promise<void>[]> {
    if (this.drainTimeoutMs() <= 0) return [];
    const sessions = this.endHandlerSessions();
    const children = this.isolatedClients.map((child) => child.drainAll());
    await this.drainResponders(this.responders);
    for (const child of await Promise.allSettled(children)) {
      if (child.status === 'fulfilled') sessions.push(...child.value);
    }
    return sessions;
  }

  private drainResponders(responders: Iterable<Responder>): Promise<void> {
    const timeoutMs = this.drainTimeoutMs();
    // without a connection nothing arrives and no reply goes out
    if (timeoutMs <= 0 || !this.isConnected) return Promise.resolve();
    const drains: Promise<void>[] = [];
    for (const responder of responders) {
      if (responder.open) drains.push((responder.draining ??= this.drainResponder(responder, timeoutMs)));
    }
    return Promise.all(drains).then(() => {});
  }

  private async drainResponder(responder: Responder, timeoutMs: number): Promise<void> {
    const open = responder.open!;
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const methods = responder.methods?.sub;
      if (methods && !methods.isClosed()) methods.unsubscribe();
      const intake = Promise.all(responder.entries.map((entry) => drainSubscription(entry.sub)));
      const idle = intake.then(() =>
        open.size === 0
          ? undefined
          : new Promise<void>((resolve) => {
              responder.onIdle = resolve;
            }),
      );
      await Promise.race([
        idle,
        new Promise<void>((resolve) => {
          timer = setTimeout(resolve, timeoutMs);
        }),
      ]);
    } finally {
      clearTimeout(timer);
      responder.onIdle = undefined;
      responder.draining = undefined;
    }

    const connected = this.isConnected && !this.linkWaiters;
    for (const msg of open) {
      if (connected) this.replyClosed(msg);
    }
    open.clear();
  }

  private replyClosed(msg: RPCMessage): void {
    const error = { code: ERROR_CODES.CONNECTION_CLOSED, message: 'Connection closed' };
    try {
      if (sessionKind(msg.params) === 'stream') {
        const subject = msg.params.__streamSubject;
        if (typeof subject === 'string' && subject.startsWith('stream.')) {
          this.publishOrChunk(subject, { id: msg.id, type: 'error', error } satisfies StreamMessage)?.catch(() => {});
        }
      } else {
        this.publishOrChunk(`rpc.reply.${msg.id}`, { id: msg.id, error } satisfies RPCResponse)?.catch(() => {});
      }
    } catch {
      // the connection went away
    }
  }

  private replyMissingMethod(msg: RPCMessage, method: string, methods: string[]): void {
    if (!isRequest(msg)) return;
    const error = { code: ERROR_CODES.METHOD_NOT_FOUND, message: `Method not found: ${method}` };
    try {
      if (sessionKind(msg.params) === 'stream') {
        const subject = msg.params.__streamSubject;
        if (typeof subject === 'string' && subject.startsWith('stream.')) {
          this.publishOrChunk(subject, { id: msg.id, type: 'error', error } satisfies StreamMessage)?.catch(() => {});
        }
      } else {
        this.publishOrChunk(`rpc.reply.${msg.id}`, { id: msg.id, error, __methods: methods } satisfies RPCResponse)?.catch(() => {});
      }
    } catch {
      // the connection went away
    }
  }

  private drainTimeoutMs(): number {
    return this.options.drainTimeoutMs ?? DEFAULT_DRAIN_TIMEOUT_MS;
  }

  private onConnectionLost(nc: NatsConnection): void {
    if (nc !== this.nc || nc === this.closingNc) return;
    this.nc = undefined;
    Promise.allSettled(this.dropConnectionState(false));
  }

  private watchLink(nc: NatsConnection): void {
    let statuses: AsyncIterable<Status>;
    try {
      statuses = nc.status();
    } catch {
      return;
    }
    (async () => {
      for await (const status of statuses) {
        // suspend() and disconnect() released their holds already, a new one would stay
        if (nc !== this.nc || nc === this.closingNc) break;
        if (status.type === 'disconnect' || status.type === 'staleConnection') {
          this.linkWaiters ??= [];
        } else if (status.type === 'reconnect') {
          this.releaseLinkWaiters();
        } else if (status.type === 'error') {
          const subject = refusedPublishSubject(status.error);
          if (subject !== undefined) this.failRefusedPublish(subject);
        }
      }
    })().catch(() => {});
  }

  private failRefusedPublish(subject: string): void {
    for (const pending of [...this.pendingRequests.values()]) {
      if (pending.subject !== subject) continue;
      pending.cleanup?.();
      if (pending.transferKey) this.muxChunking.discard(pending.transferKey);
      pending.reject(forbiddenError(subject));
    }
    for (const handler of [...this.streamHandlers.values()]) {
      if (handler.subject === subject) handler.refuse?.(forbiddenError(subject));
    }
  }

  private holdForLink(subject: string): Promise<void> | undefined {
    const timeout = this.options.timeout ?? 30000;
    return this.waitForLink(timeout)?.then((up) => {
      if (!up) throw createError(ERROR_CODES.TIMEOUT, `RPC call to "${subject}" timed out after ${timeout}ms`);
    });
  }

  private releaseLinkWaiters(): void {
    const waiters = this.linkWaiters;
    this.linkWaiters = undefined;
    for (const release of waiters ?? []) release();
  }

  private waitForLink(ms: number): Promise<boolean> | undefined {
    const waiters = this.linkWaiters;
    if (!waiters) return undefined;
    return new Promise<boolean>((resolve) => {
      const release = (): void => {
        clearTimeout(timer);
        resolve(true);
      };
      const timer = setTimeout(() => {
        const index = waiters.indexOf(release);
        if (index >= 0) waiters.splice(index, 1);
        resolve(false);
      }, ms);
      waiters.push(release);
    });
  }

  private async flushBeforeClose(nc: NatsConnection): Promise<void> {
    if (nc.isClosed()) return;
    // the Node TCP transport drops what is still queued on close()
    await this.flush(Math.min(this.options.disconnectTimeout ?? 2_000, 1_000)).catch(() => {});
  }

  private async awaitSubscriptions(): Promise<void> {
    await this.flush(SUBSCRIBE_FLUSH_TIMEOUT_MS).catch(() => {});
  }

  private async waitClosed(nc: NatsConnection): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        nc.closed(),
        new Promise<void>((r) => {
          timer = setTimeout(r, this.options.disconnectTimeout ?? 2_000);
        }),
      ]);
    } catch {
      // ignore
    } finally {
      clearTimeout(timer);
    }
  }

  private encodeOrPublishFrame(subject: string, data: unknown, opts?: { headers?: MsgHdrs; reply?: string }, zeroCopy = false): Uint8Array | undefined {
    const parts = encodeMessageParts(data);
    if (parts instanceof Uint8Array) {
      return parts;
    }
    if (parts.size >= PUBLISH_FRAME_MIN_SIZE && parts.size <= this._maxPayloadSize && !opts?.headers) {
      const nc = this.nc!;
      if ((zeroCopy && publishFrameZeroCopy(nc, subject, opts?.reply, parts)) || publishFrame(nc, subject, opts?.reply, parts)) {
        return undefined;
      }
    }
    return assembleFrame(parts);
  }

  private publishRequest(subject: string, message: RPCMessage, reply: string): Promise<void> {
    if (this.options.zeroCopyBinaries !== true) {
      return this.publish(subject, message, { reply });
    }
    try {
      return this.publishOrChunk(subject, message, { reply }, true) ?? Promise.resolve();
    } catch (error) {
      return Promise.reject(error);
    }
  }

  private async publishChunks(nc: NatsConnection, subject: string, encoded: Uint8Array, opts?: { headers?: MsgHdrs; reply?: string }): Promise<void> {
    if (this.unchunkableLimit !== undefined) {
      throw createError(ERROR_CODES.PAYLOAD_TOO_LARGE, `Payload limit ${this.unchunkableLimit} is too small for chunking`);
    }
    const transferId = generateId();

    const totalChunks = Math.ceil(encoded.length / this._maxPayloadSize);

    const headerMsg: ChunkedTransferHeader = {
      type: 'chunked',
      transferId,
      totalChunks,
      totalSize: encoded.length,
      chunkSize: this._maxPayloadSize,
    };

    const hdrs = copyHeaders(opts?.headers);
    hdrs.set('x-chunked-transfer', 'header');
    hdrs.set('x-chunk-id', transferId);

    nc.publish(subject, encode(headerMsg), { headers: hdrs, reply: opts?.reply });

    let chunkIndex = 0;
    for (const chunk of createChunks(encoded, transferId, this._maxPayloadSize)) {
      const chunkHdrs = headers();
      chunkHdrs.set('x-chunked-transfer', 'chunk');
      chunkHdrs.set('x-chunk-id', transferId);
      chunkHdrs.set('x-chunk-index', chunkIndex.toString());

      nc.publish(subject, chunk.data, { headers: chunkHdrs, reply: opts?.reply });

      if (chunkIndex > 0 && chunkIndex % 50 === 0) {
        await sleep(0);
      }
      chunkIndex++;
    }
  }

  private noteSyncReply(): void {
    if (this.syncReplies++ === 0) {
      queueMicrotask(() => {
        this.syncReplies = 0;
      });
    }
    if (this.syncReplies % SYNC_REPLY_FLUSH_EVERY === 0 && this.nc) {
      flushOutbound(this.nc);
    }
  }

  private disposeChunking(): void {
    this.chunkingManager.dispose();
    this.muxChunking.dispose();
    for (const scope of this.chunkScopes.values()) {
      disposeChunkScope(scope);
    }
    for (const manager of this.endpointChunking) {
      manager.dispose();
    }
    // service endpoints are not restored with the connection
    this.endpointChunking.clear();
  }

  private newChunkingManager(maxTransfers?: number): ChunkingManager {
    return new ChunkingManager(this.options.maxTransferSize, maxTransfers, this.chunkBudget);
  }

  private chunkManager(scope: ChunkScope, lane: string | undefined, create: boolean): ChunkingManager | undefined {
    if (lane === undefined) {
      if (create) scope.manager ??= this.newChunkingManager();
      return scope.manager;
    }
    let manager = scope.lanes?.get(lane);
    if (!manager && create) {
      manager = this.newChunkingManager();
      (scope.lanes ??= new Map()).set(lane, manager);
    }
    return manager;
  }

  private joinChunkScope(entry: SubscriptionEntry): ChunkScope {
    // overlapping subscriptions each get every chunk, queue members share a scope since NATS splits a transfer among them
    const key = entry.options?.queue ? `q:${entry.options.queue}:${entry.pattern}` : `s:${++this.chunkScopeSeq}`;
    let scope = this.chunkScopes.get(key);
    if (!scope) {
      scope = { key, members: 0 };
      this.chunkScopes.set(key, scope);
    }
    scope.members++;
    return scope;
  }

  private leaveChunkScope(entry: SubscriptionEntry): void {
    const scope = entry.chunks;
    if (!scope) {
      return;
    }
    entry.chunks = undefined;
    if (--scope.members > 0) {
      return;
    }
    disposeChunkScope(scope);
    if (this.chunkScopes.get(scope.key) === scope) {
      this.chunkScopes.delete(scope.key);
    }
  }

  public async request<TRequest = any, TResponse = any>(
    subject: string,
    data: TRequest,
    options?: { timeout?: number; headers?: MsgHdrs; noResponderRetry?: { maxRetries?: number; delays?: number[] }; sender?: string },
  ): Promise<TResponse> {
    try {
      return await this.withNoResponderRetry(() => this._requestOnce<TRequest, TResponse>(subject, data, options), options?.noResponderRetry);
    } catch (error) {
      if (error instanceof errors.NoRespondersError) {
        throw markLocalNoResponders(createError(ERROR_CODES.NOT_FOUND, 'No responders available'), subject);
      }
      throw error;
    }
  }

  private async _requestOnce<TRequest = any, TResponse = any>(
    subject: string,
    data: TRequest,
    options?: { timeout?: number; headers?: MsgHdrs; sender?: string },
  ): Promise<TResponse> {
    if (!this.nc) {
      throw new Error('Not connected');
    }

    const timeout = options?.timeout ?? 5000;
    const encoded = encodeMessage(data);

    let remaining = timeout;
    const held = this.waitForLink(timeout);
    if (held) {
      const startedAt = Date.now();
      if (!(await held)) {
        throw createError(ERROR_CODES.TIMEOUT, `Request to "${subject}" timed out after ${timeout}ms`);
      }
      remaining = Math.max(1, timeout - (Date.now() - startedAt));
      if (!this.nc) {
        throw new Error('Not connected');
      }
    }

    let hdrs = options?.headers;
    if (options?.sender !== undefined) {
      hdrs = copyHeaders(hdrs);
      hdrs.set(SENDER_HEADER, options.sender);
    }

    // only transport errors are mapped here: an error reply keeps its code and text
    let msg: Msg;
    try {
      msg = await this.nc.request(subject, encoded, {
        timeout: remaining,
        headers: hdrs,
        noMux: false,
      });
    } catch (error: any) {
      if (error instanceof errors.NoRespondersError || error?.cause instanceof errors.NoRespondersError) {
        throw new errors.NoRespondersError(subject);
      }
      if (error instanceof errors.TimeoutError) {
        throw createError(ERROR_CODES.TIMEOUT, `Request to "${subject}" timed out after ${timeout}ms`);
      }
      throw error;
    }

    if (msg.headers?.get('Nats-Service-Error-Code')) {
      const errorCode = msg.headers.get('Nats-Service-Error-Code') || '500';
      const errorMsg = msg.headers.get('Nats-Service-Error') || 'Service error';
      let errorData: any = null;

      if (msg.data) {
        try {
          errorData = decodeMessage(msg.data);
        } catch {
          // ignore
        }
      }

      throw createError(errorCode, errorMsg, errorData);
    }

    const decoded = decodeMessage(msg.data);

    if (decoded?.error) {
      const code = decoded.code ?? ERROR_CODES.INTERNAL_ERROR;
      const message = decoded.error ?? 'Unknown error';
      throw createError(code, message, decoded);
    }

    return decoded;
  }

  private async withNoResponderRetry<T>(fn: () => Promise<T>, override?: { maxRetries?: number; delays?: number[] }, probe?: MethodProbe): Promise<T> {
    const { maxRetries, delays } = this.retryPlan(override);
    let missing: Promise<boolean> | undefined;

    for (let attempt = 0; ; attempt++) {
      try {
        return await fn();
      } catch (err) {
        if (!(err instanceof errors.NoRespondersError) || attempt >= maxRetries || this.closed) {
          throw err;
        }
        const delay = delays[Math.min(attempt, delays.length - 1)];
        if (attempt === 0) missing = this.probeMethod(probe, delay);
        if (await this.retryWait(delay, missing)) throw err;
      }
    }
  }

  private retryPlan(override?: { maxRetries?: number; delays?: number[] }): { maxRetries: number; delays: number[] } {
    const maxRetries = override?.maxRetries ?? this.options.noResponderRetry?.maxRetries;
    const delays = override?.delays ?? this.options.noResponderRetry?.delays;
    if (maxRetries === undefined && delays === undefined) {
      return { maxRetries: DEFAULT_RETRY_DELAYS.length, delays: DEFAULT_RETRY_DELAYS };
    }
    return { maxRetries: maxRetries ?? LEGACY_MAX_RETRIES, delays: delays ?? LEGACY_RETRY_DELAYS };
  }

  private probeMethod(probe: MethodProbe | undefined, delay: number): Promise<boolean> | undefined {
    if (probe?.probeNamespace === undefined || probe.probeMethod === undefined) return undefined;
    return this.isMethodMissing(probe.probeNamespace, probe.probeMethod, Math.max(delay, METHOD_PROBE_WINDOW_MS), probe.onMethods);
  }

  private async retryWait(delay: number, missing?: Promise<boolean>, signal?: AbortSignal): Promise<boolean> {
    const wait = sleep(delay, signal);
    if (!missing) {
      await wait;
      return false;
    }
    if (await Promise.race([missing, wait.then(() => false)])) return true;
    await wait;
    return false;
  }

  private async isMethodMissing(namespace: string, method: string, timeoutMs: number, onMethods?: (methods: string[]) => void): Promise<boolean> {
    try {
      const { result } = await this._callOnce<unknown>(`rpc.${namespace}.${METHODS_PROBE}`, [], false, false, Math.max(1, timeoutMs));
      if (!Array.isArray(result) || !result.every((name) => typeof name === 'string')) return false;
      onMethods?.(result);
      return !result.includes(method);
    } catch {
      return false;
    }
  }

  public async call<TResponse = any>(subject: string, ...args: any[]): Promise<TResponse> {
    const { result } = await this.withNoResponderRetry(() => this._callOnce<TResponse>(subject, args));
    return result;
  }

  public async callWithMeta<TResponse = any>(
    subject: string,
    args: any[] = [],
    opts?: { discover?: boolean; timing?: boolean; probeNamespace?: string; probeMethod?: string; onMethods?: (methods: string[]) => void } & ProxyCallOptions,
  ): Promise<{ result: TResponse; methods?: string[]; timing?: RPCCallTiming }> {
    const startedAt = opts?.timing === true ? Date.now() : 0;
    const { result, methods, timing } = await this.withNoResponderRetry(
      () => this._callOnce<TResponse>(subject, args, opts?.discover === true, opts?.timing === true, opts?.timeout),
      opts?.noResponderRetry,
      opts,
    );
    // no stamps: the responder does not know __timing, and pure transport time would mislead
    if (!startedAt || !timing) return { result, methods };

    const totalMs = Date.now() - startedAt;
    // both stamps come from the responder's clock, so clock skew between the processes does not matter
    const handlerMs = Math.max(0, timing.end - timing.start);
    return { result, methods, timing: { totalMs, handlerMs, transportMs: Math.max(0, totalMs - handlerMs) } };
  }

  private async _callOnce<TResponse = any>(
    subject: string,
    args: any[],
    discover = false,
    timing = false,
    timeoutMs?: number,
  ): Promise<{ result: TResponse; methods?: string[]; timing?: RPCTiming }> {
    if (!this.isConnected && !this.isClosed) {
      await this.connect();
    }

    if (!this.nc) {
      throw new Error('Not connected');
    }

    // service subjects (<subject>.reply.<id>) keep a reply subscription per call
    if (!subject.startsWith('rpc.')) {
      return this._callOnceService<TResponse>(subject, args, timeoutMs);
    }

    // normally done by connect(), covers connections wired up out of band
    this.ensureMuxSubscription();

    const id = generateId(this.replyPrefix);
    const timeout = timeoutMs ?? this.options.timeout ?? 30000;
    // wire contract: responders reply to rpc.reply.<id>, the replyPrefix in the id routes it to the mux
    const replySubject = `rpc.reply.${id}`;

    const deadline = performance.now() + timeout;

    return new Promise<{ result: TResponse; methods?: string[]; timing?: RPCTiming }>((resolve, reject) => {
      let unhold: (() => void) | undefined;
      const timeoutHandle = setTimeout(() => {
        unhold?.();
        const pending = this.pendingRequests.get(id);
        if (!pending) return;
        this.pendingRequests.delete(id);
        if (pending.transferKey) this.muxChunking.discard(pending.transferKey);
        reject(createError(ERROR_CODES.TIMEOUT, `RPC call to "${subject}" timed out after ${timeout}ms`));
      }, timeout);

      this.pendingRequests.set(id, {
        resolve,
        reject,
        timeout: timeoutHandle,
        subject,
        cleanup: () => {
          clearTimeout(timeoutHandle);
          unhold?.();
          this.pendingRequests.delete(id);
        },
      });

      // the call's own reply subject, so a no-responder 503 lands where the response would
      const message: RPCMessage = { id, method: 'call', params: args };
      if (discover) {
        // an envelope marker, in params it would reach the handler args
        message.__discover = true;
      }
      if (timing) {
        message.__timing = true;
      }
      const send = (): void => {
        // what is left of the timeout once a held request goes out
        message.__ttl = Math.max(1, Math.ceil(deadline - performance.now()));
        this.publishRequest(subject, message, replySubject).catch((error) => {
          if (this.pendingRequests.delete(id)) {
            clearTimeout(timeoutHandle);
            reject(error);
          }
        });
      };
      // while nats.js reconnects the request waits for the link, its timeout keeps running
      const waiters = this.linkWaiters;
      if (waiters) {
        const release = (): void => {
          if (this.pendingRequests.has(id)) send();
        };
        waiters.push(release);
        // a call that settles while held must not keep its args alive until the link is back
        unhold = () => {
          const index = waiters.indexOf(release);
          if (index >= 0) waiters.splice(index, 1);
        };
      } else {
        send();
      }
    });
  }

  public async callService<TResponse = any>(subject: string, args: any[], direct?: string): Promise<TResponse> {
    const { result } = await this.withNoResponderRetry(async () => {
      if (!this.isConnected && !this.isClosed) {
        await this.connect();
      }
      if (!this.nc) {
        throw new Error('Not connected');
      }
      return this._callOnceService<TResponse>(subject, args, undefined, direct);
    });
    return result;
  }

  private async _callOnceService<TResponse = any>(subject: string, args: any[], timeoutMs?: number, direct?: string): Promise<{ result: TResponse; methods?: string[] }> {
    const id = generateId(this.replyPrefix);
    const timeout = timeoutMs ?? this.options.timeout ?? 30000;
    const message: RPCMessage = { id, method: 'call', params: args };
    const target = direct !== undefined && encodedSize(message) > this._maxPayloadSize ? direct : subject;
    const replySubject = `${target}.reply.${id}`;

    return new Promise<{ result: TResponse; methods?: string[] }>(async (resolve, reject) => {
      let sub: Subscription | undefined;
      let unsubscribe: (() => void) | undefined;

      const unsubscribeAll = async () => {
        if (this.pendingRequests.has(id)) {
          const pending = this.pendingRequests.get(id);
          if (pending?.timeout) {
            clearTimeout(pending.timeout);
          }
          this.pendingRequests.delete(id);
        }

        if (sub && !sub.isClosed()) {
          try {
            sub.unsubscribe();
          } catch {
            // ignore
          }
        }

        unsubscribe?.();
      };

      // drops the reply subscriptions too, otherwise every reconnect restores them
      const timeoutHandle = setTimeout(() => {
        if (this.pendingRequests.has(id)) {
          this.pendingRequests.delete(id);
          unsubscribeAll();
          reject(createError(ERROR_CODES.TIMEOUT, `RPC call to "${subject}" timed out after ${timeout}ms`));
        }
      }, timeout);

      this.pendingRequests.set(id, {
        resolve,
        reject,
        timeout: timeoutHandle,
        subject: target,
        cleanup: () => unsubscribeAll(),
      });

      const handleRpcResponse = async (data: any) => {
        const response = data as RPCResponse;

        if (response.id === id) {
          const pending = this.pendingRequests.get(response.id);

          if (pending) {
            this.pendingRequests.delete(response.id);
            if (pending.timeout) clearTimeout(pending.timeout);
            await unsubscribeAll();

            if (response.error) {
              pending.reject(RPCException.fromJSON(response.error));
            } else {
              pending.resolve({ result: response.result, methods: response.__methods, timing: response.__timing });
            }
          }
        }
      };

      const requestCallback = (err: Error | null, msg: Msg) => {
        if (msg && msg.data?.length === 0 && msg.headers?.code === 503) {
          reject(noRespondersError(target));
          unsubscribeAll();
        } else if (err) {
          reject(err);
          unsubscribeAll();
        }
      };

      try {
        unsubscribe = await this.subscribe(replySubject, handleRpcResponse);

        const inbox = scopedInbox(this.options.connId);
        sub = this.nc!.subscribe(inbox, {
          max: 1,
          callback: requestCallback,
        });

        const held = this.waitForLink(timeout);
        if (held) {
          await held;
          // timed out or the connection ended meanwhile
          if (!this.pendingRequests.has(id)) return;
        }

        await this.publishRequest(target, message, inbox);
      } catch (error) {
        await unsubscribeAll();
        reject(error);
      }
    });
  }

  public callStream<TResponse = any>(subject: string, ...args: any[]): AsyncGenerator<TResponse> {
    return this.streamWithRetry<TResponse>(subject, args);
  }

  public callStreamWithProbe<TResponse = any>(subject: string, args: any[], probe: MethodProbe): AsyncGenerator<TResponse> {
    return this.streamWithRetry<TResponse>(subject, args, probe);
  }

  private streamWithRetry<TResponse>(subject: string, args: any[], probe?: MethodProbe): AsyncGenerator<TResponse> {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const client = this;
    const { maxRetries, delays } = this.retryPlan();
    let attempt = 0;
    let yielded = false;
    let finished = false;
    let inner: AsyncGenerator<TResponse> | undefined;
    let missing: Promise<boolean> | undefined;
    let waiting: AbortController | undefined;
    let steps: Promise<unknown> = Promise.resolve();

    const step = async (): Promise<IteratorResult<TResponse, void>> => {
      while (!finished) {
        const once = (inner ??= client._callStreamOnce<TResponse>(subject, ...args));
        try {
          const result = await once.next();
          if (result.done || finished) break;
          yielded = true;
          return result;
        } catch (err) {
          inner = undefined;
          // a retry after delivered items would hand them to the consumer twice
          if (finished || yielded || !(err instanceof errors.NoRespondersError) || attempt >= maxRetries || client.closed) {
            finished = true;
            throw err;
          }
          const delay = delays[Math.min(attempt, delays.length - 1)];
          if (attempt === 0) missing = client.probeMethod(probe, delay);
          attempt++;
          waiting = new AbortController();
          const missingMethod = await client.retryWait(delay, missing, waiting.signal);
          waiting = undefined;
          if (missingMethod && !finished) {
            finished = true;
            throw err;
          }
        }
      }
      finished = true;
      return { value: undefined, done: true };
    };

    const iter: AsyncGenerator<TResponse, void, void> = {
      next(): Promise<IteratorResult<TResponse, void>> {
        const result = steps.then(step);
        steps = result.catch(() => {});
        return result;
      },

      async return(value?: void): Promise<IteratorResult<TResponse, void>> {
        // not queued behind a pending next(): ends the attempt now, which settles that next() as done
        if (!finished) {
          finished = true;
          waiting?.abort();
          await inner?.return(undefined);
        }
        return { value: value!, done: true };
      },

      async throw(err: unknown): Promise<IteratorResult<TResponse, void>> {
        await iter.return();
        throw err;
      },

      [Symbol.asyncIterator](): AsyncGenerator<TResponse, void, void> {
        return iter;
      },

      async [Symbol.asyncDispose](): Promise<void> {
        await iter.return();
      },
    };

    return iter;
  }

  private _callStreamOnce<TResponse = any>(subject: string, ...args: any[]): AsyncGenerator<TResponse> {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const client = this;

    let started = false;
    let returned = false;
    let cleanedUp = false;
    let ended = false;
    let error: any = null;

    let id = '';
    let streamSubject = '';
    let unsubscribe: (() => void) | undefined;
    // the stream request went out, a cancel has to follow it
    let requested = false;

    const queue: TResponse[] = [];
    let resolver: ((value: IteratorResult<TResponse>) => void) | null = null;

    const settlePendingAsDone = (): void => {
      const r = resolver;
      if (r) {
        resolver = null;
        r({ value: undefined, done: true });
      }
    };

    const handler = {
      push: (value: TResponse): void => {
        if (ended) return;
        if (resolver) {
          const r = resolver;
          resolver = null;
          r({ value, done: false });
        } else {
          queue.push(value);
        }
      },
      end: (): void => {
        ended = true;
        settlePendingAsDone();
      },
      error: (err: any): void => {
        error = err;
        ended = true;
        settlePendingAsDone();
      },
      subject,
      refuse: (err: RPCException): void => {
        handler.error(err);
        cleanupOnce();
      },
    };

    const sendCancel = async (): Promise<void> => {
      try {
        await client.publish(`${streamSubject}.cancel`, { id });
      } catch {
        // ignore
      }
    };

    // may run before setup is done, setup then removes what it adds later
    const cleanupOnce = async (): Promise<void> => {
      if (cleanedUp) return;
      cleanedUp = true;
      client.streamHandlers.delete(id);
      client.statusHandlers.delete(id);
      unsubscribe?.();
      if (!ended) {
        ended = true;
        if (requested) await sendCancel();
      }
    };

    const handleStreamMessage = async (msg: StreamMessage): Promise<void> => {
      if (msg.id !== id) return;
      const h = client.streamHandlers.get(msg.id);
      if (!h) return;
      switch (msg.type) {
        case 'data':
          h.push(msg.data);
          break;
        case 'end':
          h.end();
          await cleanupOnce();
          break;
        case 'error':
          h.error(RPCException.fromJSON(msg.error!));
          await cleanupOnce();
          break;
      }
    };

    const setup = async (): Promise<void> => {
      if (!client.isConnected && !client.isClosed) {
        await client.connect();
      }
      if (cleanedUp) return;
      if (!client.nc) throw new Error('Not connected');
      client.ensureMuxSubscription();

      id = generateId(client.replyPrefix);
      streamSubject = `stream.${subject}.${id}`;
      client.streamHandlers.set(id, handler);

      const unsub = await client.subscribe(streamSubject, handleStreamMessage);
      if (cleanedUp) {
        unsub();
        client.streamHandlers.delete(id);
        return;
      }
      unsubscribe = unsub;

      // rpc.reply.<id> only ever carries a 503 status, stream responders never reply there
      client.statusHandlers.set(id, () => {
        const h = client.streamHandlers.get(id);
        h?.error(noRespondersError(subject));
        cleanupOnce();
      });

      const held = client.holdForLink(subject);
      if (held) {
        await held;
        // ended while held: returned, or the connection closed
        if (ended) return;
      }

      const streamParams = { __stream: true, __streamSubject: streamSubject, args };
      const message: RPCMessage = { id, method: 'stream', params: streamParams };
      await client.publish(subject, message, { reply: `rpc.reply.${id}` });
      requested = true;
      if (cleanedUp) await sendCancel();
    };

    const iter: AsyncGenerator<TResponse, void, void> = {
      async next(): Promise<IteratorResult<TResponse, void>> {
        if (returned) return { value: undefined, done: true };
        if (!started) {
          started = true;
          try {
            await setup();
          } catch (err) {
            await cleanupOnce();
            throw err;
          }
          if (returned) {
            await cleanupOnce();
            return { value: undefined, done: true };
          }
        }

        if (error) {
          await cleanupOnce();
          throw error;
        }
        if (queue.length > 0) {
          return { value: queue.shift() as TResponse, done: false };
        }
        if (ended) {
          await cleanupOnce();
          return { value: undefined, done: true };
        }

        const result = await new Promise<IteratorResult<TResponse>>((resolve, reject) => {
          if (returned) {
            resolve({ value: undefined, done: true });
            return;
          }
          if (ended) {
            resolve({ value: undefined, done: true });
            return;
          }
          if (error) {
            reject(error);
            return;
          }
          resolver = resolve;
        });

        if (returned) {
          await cleanupOnce();
          return { value: undefined, done: true };
        }
        if (result.done) {
          if (error) {
            await cleanupOnce();
            throw error;
          }
          await cleanupOnce();
          return { value: undefined, done: true };
        }
        return { value: result.value, done: false };
      },

      async return(value?: void): Promise<IteratorResult<TResponse, void>> {
        if (returned) return { value: value!, done: true };
        returned = true;
        settlePendingAsDone();
        if (started) await cleanupOnce();
        return { value: value!, done: true };
      },

      async throw(err: unknown): Promise<IteratorResult<TResponse, void>> {
        if (returned) throw err;
        returned = true;
        settlePendingAsDone();
        if (started) await cleanupOnce();
        throw err;
      },

      [Symbol.asyncIterator](): AsyncGenerator<TResponse, void, void> {
        return iter;
      },

      async [Symbol.asyncDispose](): Promise<void> {
        await iter.return();
      },
    };

    return iter;
  }

  public callPullIterator<TResponse = any>(subject: string, ...args: any[]): AsyncGenerator<TResponse> {
    // hand-written iterator, so return() can settle a next() parked on the response
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const client = this;

    let started = false;
    let settingUp = false;
    let returned = false;
    let cleanedUp = false;
    let ended = false;
    // the init call went out, a cancel has to follow it
    let requested = false;
    let error: any = null;
    // the no-responder status of a next request, thrown as is to keep its mark
    let noResponders: PullIteratorResponse | undefined;

    let iteratorId = '';
    let requestSubject = '';
    let responseSubject = '';
    let statusInbox = '';
    let responseUnsub: (() => void) | undefined;

    const responseQueue: PullIteratorResponse[] = [];
    let responseResolver: ((value: PullIteratorResponse) => void) | null = null;

    const settlePendingAsDone = (): void => {
      const r = responseResolver;
      if (r) {
        responseResolver = null;
        r({ id: iteratorId, type: 'done' });
      }
    };

    // settles a parked next() on teardown, otherwise the consumer's for await hangs
    const settleOnDisconnect = (): void => {
      if (!ended) {
        ended = true;
        error = createError(ERROR_CODES.CONNECTION_CLOSED, 'Connection closed');
      }
      const r = responseResolver;
      if (r) {
        responseResolver = null;
        if (error) {
          r({ id: iteratorId, type: 'error', error: error.toJSON?.() ?? { code: ERROR_CODES.CONNECTION_CLOSED, message: 'Connection closed' } });
        } else {
          r({ id: iteratorId, type: 'done' });
        }
      }
    };

    const cleanupOnce = async (): Promise<void> => {
      if (cleanedUp) return;
      cleanedUp = true;
      client.pullIteratorSettles.delete(settleOnDisconnect);
      client.statusHandlers.delete(iteratorId);
      responseUnsub?.();
      if (!ended) {
        ended = true;
        if (!requested) return;
        try {
          const cancelRequest: PullIteratorRequest = { id: iteratorId, type: 'cancel' };
          await client.publish(requestSubject, cancelRequest);
        } catch {
          // ignore
        }
      }
    };

    const setup = async (): Promise<void> => {
      if (!client.isConnected && !client.isClosed) {
        await client.connect();
      }
      if (!client.nc) throw new Error('Not connected');
      client.ensureMuxSubscription();

      client.pullIteratorSettles.add(settleOnDisconnect);

      iteratorId = generateId(client.replyPrefix);
      requestSubject = `_rpc.iterator.${iteratorId}.request`;
      responseSubject = `_rpc.iterator.${iteratorId}.response`;
      // 503 inbox for next requests, under the mux wildcard since iteratorId starts with replyPrefix
      statusInbox = `rpc.reply.${iteratorId}`;

      if (returned) return;
      requested = true;
      const initResponse = await client.call<any>(subject, { __pullIterator: true, __iteratorId: iteratorId, args });
      if (initResponse?.iteratorId !== iteratorId) {
        throw new Error('Failed to initialize pull iterator');
      }

      responseUnsub = await client.subscribe(responseSubject, (msg: PullIteratorResponse) => {
        if (msg.type === 'error') {
          error = RPCException.fromJSON(msg.error!);
          ended = true;
        } else if (msg.type === 'done') {
          ended = true;
        }
        if (responseResolver) {
          const r = responseResolver;
          responseResolver = null;
          r(msg);
        } else {
          responseQueue.push(msg);
        }
      });

      client.statusHandlers.set(iteratorId, () => {
        const e = new errors.NoRespondersError(subject);
        ended = true;
        error = markLocalNoResponders(createError('503', e.message), subject);
        const response: PullIteratorResponse<any> = {
          type: 'error',
          id: iteratorId,
          error: error.toJSON(),
        };
        noResponders = response;
        if (responseResolver) {
          const r = responseResolver;
          responseResolver = null;
          r(response);
        } else {
          responseQueue.push(response);
        }
      });
    };

    const iter: AsyncGenerator<TResponse, void, void> = {
      async next(): Promise<IteratorResult<TResponse, void>> {
        if (returned) return { value: undefined, done: true };
        if (!started) {
          started = true;
          settingUp = true;
          try {
            await setup();
          } catch (err) {
            settingUp = false;
            await cleanupOnce();
            throw err;
          }
          settingUp = false;
          if (returned) {
            await cleanupOnce();
            return { value: undefined, done: true };
          }
        }

        const nextRequest: PullIteratorRequest = { id: iteratorId, type: 'next' };
        try {
          const held = client.holdForLink(subject);
          if (held) await held;
          // a connection that ended while held settled the iterator
          if (!ended) await client.publish(requestSubject, nextRequest, { reply: statusInbox });
        } catch (err) {
          await cleanupOnce();
          throw err;
        }

        const response = await new Promise<PullIteratorResponse>((resolve, reject) => {
          if (returned) {
            resolve({ id: iteratorId, type: 'done' });
            return;
          }
          if (responseQueue.length > 0) {
            resolve(responseQueue.shift()!);
          } else if (ended && error) {
            reject(error);
          } else if (ended) {
            resolve({ id: iteratorId, type: 'done' });
          } else {
            responseResolver = resolve;
          }
        });

        if (returned) {
          await cleanupOnce();
          return { value: undefined, done: true };
        }

        if (response.type === 'error') {
          await cleanupOnce();
          throw response === noResponders ? error : RPCException.fromJSON(response.error!);
        }
        if (response.type === 'done') {
          await cleanupOnce();
          return { value: undefined, done: true };
        }
        return { value: response.value as TResponse, done: false };
      },

      async return(value?: void): Promise<IteratorResult<TResponse, void>> {
        if (returned) return { value: value!, done: true };
        returned = true;
        settlePendingAsDone();
        // a setup in flight cleans up when it ends: a cancel ahead of the init reply finds no session to end
        if (started && !settingUp) await cleanupOnce();
        return { value: value!, done: true };
      },

      async throw(err: unknown): Promise<IteratorResult<TResponse, void>> {
        if (returned) throw err;
        returned = true;
        settlePendingAsDone();
        if (started && !settingUp) await cleanupOnce();
        throw err;
      },

      [Symbol.asyncIterator](): AsyncGenerator<TResponse, void, void> {
        return iter;
      },

      async [Symbol.asyncDispose](): Promise<void> {
        await iter.return();
      },
    };

    return iter;
  }

  public callPullIteratorWithCallback(
    subject: string,
    callbacks: Record<string, (...a: any[]) => any>,
    onewayMethods: string[] | PullCallbackCallOptions,
    ...args: any[]
  ): AsyncGenerator<void> {
    const callOptions = Array.isArray(onewayMethods) ? undefined : onewayMethods;
    const onewayMethodList = Array.isArray(onewayMethods) ? onewayMethods : onewayMethods.onewayMethods;
    const prefetch = callOptions?.prefetch ?? false;
    // hand-written iterator: return() cannot wake an async generator parked at an await, setup waits for the first next()
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const client = this;

    let started = false;
    let settingUp = false;
    let returned = false;
    let cleanedUp = false;
    let ended = false;
    // the init call went out, a cancel has to follow it
    let requested = false;
    // the no-responder status of a next request, thrown as is to keep its mark
    let noResponders: PullIteratorResponse | undefined;
    let error: any = null;

    let iteratorId = '';
    let requestSubject = '';
    let responseSubject = '';
    let callbackSubject = '';
    let statusInbox = '';
    let callbackUnsub: (() => void) | undefined;
    let callbackFlush: (() => Promise<void>) | undefined;
    let responseUnsub: (() => void) | undefined;

    const responseQueue: PullIteratorResponse[] = [];
    let responseResolver: ((value: PullIteratorResponse) => void) | null = null;
    let callbackChain: Promise<void> = Promise.resolve();
    // the callback chain as it stood when a batch boundary arrived
    const batchTails = new WeakMap<PullIteratorResponse, Promise<void>>();
    // set when prefetch already sent the next request, so next() does not send it again
    let prefetched = false;

    const settlePendingAsDone = (): void => {
      const r = responseResolver;
      if (r) {
        responseResolver = null;
        r({ id: iteratorId, type: 'done' });
      }
    };

    // settles a parked next() on teardown
    const settleOnDisconnect = (): void => {
      if (!ended) {
        ended = true;
        error = createError(ERROR_CODES.CONNECTION_CLOSED, 'Connection closed');
      }
      const r = responseResolver;
      if (r) {
        responseResolver = null;
        if (error) {
          r({ id: iteratorId, type: 'error', error: error.toJSON?.() ?? { code: ERROR_CODES.CONNECTION_CLOSED, message: 'Connection closed' } });
        } else {
          r({ id: iteratorId, type: 'done' });
        }
      }
    };

    const cleanupOnce = async (): Promise<void> => {
      if (cleanedUp) return;
      cleanedUp = true;
      client.pullIteratorSettles.delete(settleOnDisconnect);
      client.statusHandlers.delete(iteratorId);
      callbackUnsub?.();
      responseUnsub?.();
      if (!ended) {
        ended = true;
        if (!requested) return;
        try {
          const cancelRequest: PullIteratorRequest = { id: iteratorId, type: 'cancel' };
          await client.publish(requestSubject, cancelRequest);
        } catch {
          // ignore
        }
      }
    };

    const setup = async (): Promise<void> => {
      if (!client.isConnected && !client.isClosed) {
        await client.connect();
      }
      if (!client.nc) throw new Error('Not connected');
      client.ensureMuxSubscription();

      client.pullIteratorSettles.add(settleOnDisconnect);

      iteratorId = generateId(client.replyPrefix);
      requestSubject = `_rpc.iterator.${iteratorId}.request`;
      responseSubject = `_rpc.iterator.${iteratorId}.response`;
      callbackSubject = `_rpc.cb.${iteratorId}`;
      // 503 inbox for next requests
      statusInbox = `rpc.reply.${iteratorId}`;
      const callbackMethods = Object.keys(callbacks).filter((k) => typeof callbacks[k] === 'function');

      const cbSubscription = await client.subscribeEntry(callbackSubject, (msg: CallbackInvocation) => {
        // a frame after the end
        if (cleanedUp || returned) return;
        const fn = callbacks[msg.method];
        if (!fn) {
          console.error(`[rpc] Pull-callback: unknown method '${msg.method}'`);
          return;
        }
        callbackChain = callbackChain.then(async () => {
          // still queued when the iterator ended: return() waits for no callback, a callback may wait on return()
          if (cleanedUp || returned) return;
          try {
            await fn(...(msg.args ?? []));
          } catch (err) {
            console.error(`[rpc] Pull-callback handler '${msg.method}' threw:`, err);
          }
        });
      });
      callbackUnsub = cbSubscription.unsubscribe;
      callbackFlush = () => cbSubscription.entry.flush?.() ?? Promise.resolve();
      if (returned) return;

      const initParams: PullCallbackParams = {
        __pullCallback: true,
        __iteratorId: iteratorId,
        __callbackSubject: callbackSubject,
        __callbackMethods: callbackMethods,
        __onewayMethods: onewayMethodList,
        args,
      };

      let initResponse: any;
      requested = true;
      try {
        initResponse = await client.call<any>(subject, initParams);
      } catch (err) {
        callbackUnsub?.();
        callbackUnsub = undefined;
        throw err;
      }

      if (initResponse?.iteratorId !== iteratorId) {
        callbackUnsub?.();
        callbackUnsub = undefined;
        throw new Error('Failed to initialize pull-callback iterator');
      }

      responseUnsub = await client.subscribe(responseSubject, (msg: PullIteratorResponse) => {
        if (msg.type === 'error') {
          error = RPCException.fromJSON(msg.error!);
          ended = true;
        } else if (msg.type === 'done') {
          ended = true;
        } else {
          batchTails.set(msg, callbackChain);
        }

        if (responseResolver) {
          const r = responseResolver;
          responseResolver = null;
          r(msg);
        } else {
          responseQueue.push(msg);
        }
      });

      client.statusHandlers.set(iteratorId, () => {
        const e = new errors.NoRespondersError(subject);
        ended = true;
        error = markLocalNoResponders(createError('503', e.message), subject);

        const response: PullIteratorResponse<any> = {
          type: 'error',
          id: iteratorId,
          error: error.toJSON(),
        };
        noResponders = response;

        if (responseResolver) {
          const r = responseResolver;
          responseResolver = null;
          r(response);
        } else {
          responseQueue.push(response);
        }
      });
    };

    const iter: AsyncGenerator<void, void, void> = {
      async next(): Promise<IteratorResult<void, void>> {
        if (returned) return { value: undefined, done: true };
        if (!started) {
          started = true;
          settingUp = true;
          try {
            await setup();
          } catch (err) {
            settingUp = false;
            await cleanupOnce();
            throw err;
          }
          settingUp = false;
          if (returned) {
            await cleanupOnce();
            return { value: undefined, done: true };
          }
        }

        if (prefetched) {
          prefetched = false;
        } else {
          const nextRequest: PullIteratorRequest = { id: iteratorId, type: 'next' };
          try {
            const held = client.holdForLink(subject);
            if (held) await held;
            // a connection that ended while held settled the iterator
            if (!ended) await client.publish(requestSubject, nextRequest, { reply: statusInbox });
          } catch (err) {
            await cleanupOnce();
            throw err;
          }
        }

        const response = await new Promise<PullIteratorResponse>((resolve, reject) => {
          if (returned) {
            resolve({ id: iteratorId, type: 'done' });
            return;
          }
          if (responseQueue.length > 0) {
            resolve(responseQueue.shift()!);
          } else if (ended && error) {
            reject(error);
          } else if (ended) {
            resolve({ id: iteratorId, type: 'done' });
          } else {
            responseResolver = resolve;
          }
        });

        if (returned) {
          await cleanupOnce();
          return { value: undefined, done: true };
        }

        if (response.type === 'error') {
          await cleanupOnce();
          throw response === noResponders ? error : RPCException.fromJSON(response.error!);
        }
        if (response.type === 'done') {
          // drain the callbacks that arrived before the terminal response
          await callbackFlush?.();
          await callbackChain;
          await cleanupOnce();
          return { value: undefined, done: true };
        }

        // prefetch batch n+1 before draining this one, the next next() resends a failed or skipped request
        if (prefetch && !ended && !returned && !client.linkWaiters) {
          try {
            const prefetchRequest: PullIteratorRequest = { id: iteratorId, type: 'next' };
            await client.publish(requestSubject, prefetchRequest, { reply: statusInbox });
            prefetched = true;
          } catch {
            prefetched = false;
          }
        }
        // backpressure: wait for this batch's callbacks only, the chain at its boundary covers exactly this batch
        await (batchTails.get(response) ?? callbackChain);
        return { value: undefined, done: false };
      },

      async return(value?: void): Promise<IteratorResult<void, void>> {
        if (returned) return { value: value!, done: true };
        returned = true;
        settlePendingAsDone();
        // a setup in flight cleans up when it ends: a cancel ahead of the init reply finds no session to end
        if (started && !settingUp) await cleanupOnce();
        return { value: value!, done: true };
      },

      async throw(err: unknown): Promise<IteratorResult<void, void>> {
        if (returned) throw err;
        returned = true;
        settlePendingAsDone();
        if (started && !settingUp) await cleanupOnce();
        throw err;
      },

      [Symbol.asyncIterator](): AsyncGenerator<void, void, void> {
        return iter;
      },

      async [Symbol.asyncDispose](): Promise<void> {
        await iter.return();
      },
    };

    return iter;
  }

  public async callWithCallback<TResponse = any>(subject: string, args: any[], callback: (value: TResponse) => void | Promise<void>): Promise<() => void> {
    if (!this.isConnected && !this.isClosed) {
      await this.connect();
    }

    if (!this.nc) {
      throw new Error('Not connected');
    }

    const id = generateId(this.replyPrefix);
    const callbackSubject = `rpc.cb.${id}`;

    const unsub = await this.subscribe(callbackSubject, async (msg: CallbackMessage) => {
      if (msg.type === 'data') {
        try {
          await callback(msg.data);
        } catch (err) {
          console.error('[rpc] Callback error:', err);
        }
      } else if (msg.type === 'error') {
        console.error('[rpc] Callback error:', msg.error);
      }
    });

    const callbackParams: CallbackParams = {
      __callback: true,
      __callbackSubject: callbackSubject,
      args,
    };

    try {
      await this.call(subject, callbackParams);
    } catch (err) {
      unsub();
      throw err;
    }

    const unsubscribe = () => {
      this.publish(`${callbackSubject}.cancel`, { id }).catch(() => {});
      unsub();
    };

    return unsubscribe;
  }

  public async registerHandler(
    namespace: string,
    handlers: object,
    options?: {
      isolatedConnection?: boolean;
      withoutDecorators?: boolean;
      queue?: string;
      concurrency?: Record<string, number>;
      orphanProbe?: OrphanProbeOptions | false;
      namespaceSubscription?: boolean;
      dropExpired?: boolean | string[];
    },
  ): Promise<() => Promise<void>> {
    if (options?.namespaceSubscription === true && !namespaceSubscribable(namespace)) {
      throw new Error(`Namespace "${namespace}" overlaps internal subjects and cannot take a namespace subscription`);
    }
    if (!this.nc && !options?.isolatedConnection) {
      throw new Error('Not connected');
    }

    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let client: RPCClient = this;

    if (options?.isolatedConnection) {
      client = this.createIsolatedClient({
        ...this.options,
        name: `${this.options.name}-handler-${namespace}`,
      });

      await client.connect();
      this.isolatedClients.push(client);
    }

    const unsubscribers: (() => void)[] = [];
    // only the sessions this namespace created, other registrations keep theirs
    const owner = new SessionOwner();

    const handlersMap = options?.withoutDecorators ? extractNestedMethodsWithoutDecorators(handlers) : extractNestedMethodsWithDecorators(handlers);
    const methodNames = Object.keys(handlersMap);
    const probe = probeSettings(options?.orphanProbe ?? client.options.orphanProbe);

    const open = client.drainTimeoutMs() > 0 ? new Set<RPCMessage>() : undefined;
    const responder: Responder = { entries: [], open };
    const receive = open
      ? (msg: unknown): void => {
          if (isRequest(msg)) open.add(msg);
        }
      : undefined;
    // false when a drain deadline has answered the request already
    const claim = (msg: RPCMessage): boolean => {
      if (!open || !isRequest(msg)) return true;
      if (!open.delete(msg)) return false;
      if (open.size === 0) responder.onIdle?.();
      return true;
    };

    const replyError = async (replySubject: string, response: RPCResponse, error: unknown): Promise<void> => {
      response.error = formatErrorObject(error);
      // a result that failed to go out would fail the error reply too
      delete response.result;

      // a METHOD_NOT_FOUND error always carries the method list
      if (response.error.code === ERROR_CODES.METHOD_NOT_FOUND) {
        response.__methods = methodNames;
      }

      try {
        await client.publish(replySubject, response);
      } catch (publishError) {
        if (client.isClosed) {
          return;
        }

        console.error('Failed to send error response:', publishError);
      }
    };

    const handleSession = async (kind: SessionKind, handler: MethodHandler, method: string, msg: RPCMessage, response: RPCResponse): Promise<void> => {
      const replySubject = `rpc.reply.${msg.id}`;
      // a stream has no reply of its own, its session answers on the stream subject
      let claimed = kind === 'stream' && claim(msg);
      try {
        if (kind === 'stream') {
          const streamSubject = msg.params.__streamSubject;
          const args = msg.params.args ?? [];

          // not awaited: the stream runs in the background, awaiting would block the subscription
          handleStreamRequest(handler, args, streamSubject, msg.id, client, owner, probe).catch((err) => {
            console.error(`Stream request error for ${method}:`, err);
          });
          return;
        } else {
          response.result = await openSession(kind, handler, msg.params, msg.id, client, owner, probe);
          if ((claimed = claim(msg))) await client.publish(replySubject, response);
        }
      } catch (error) {
        if (claimed || claim(msg)) await replyError(replySubject, response, error);
      }
    };

    // a plain return value is answered inline, so the serial dispatcher stays on its sync path
    const handleCall = (handler: MethodHandler, msg: RPCMessage, response: RPCResponse): void | Promise<void> => {
      const replySubject = `rpc.reply.${msg.id}`;
      const wantsTiming = msg.__timing === true;
      const start = wantsTiming ? Date.now() : 0;

      let result: unknown;
      try {
        result = callNormalRPC(handler, msg.params);
      } catch (error) {
        return claim(msg) ? replyError(replySubject, response, error) : undefined;
      }

      if (isThenable(result)) {
        return (async () => {
          let value: unknown;
          try {
            value = await result;
          } catch (error) {
            if (claim(msg)) await replyError(replySubject, response, error);
            return;
          }
          if (!claim(msg)) return;
          if (wantsTiming) {
            response.__timing = { start, end: Date.now() };
          }
          response.result = value;
          try {
            await client.publish(replySubject, response);
          } catch (error) {
            await replyError(replySubject, response, error);
          }
        })();
      }

      if (!claim(msg)) return;
      if (wantsTiming) {
        response.__timing = { start, end: Date.now() };
      }
      response.result = result;

      let chunked: Promise<void> | undefined;
      try {
        chunked = client.publishOrChunk(replySubject, response);
      } catch (error) {
        return replyError(replySubject, response, error);
      }
      if (!chunked) {
        client.noteSyncReply();
      }
      return chunked?.catch((error) => replyError(replySubject, response, error));
    };

    // when the requests of a dropExpired method arrived, in performance.now() time
    const arrivals = new WeakMap<RPCMessage, number>();
    const dropExpired = options?.dropExpired;
    const expiring = Array.isArray(dropExpired) ? new Set(dropExpired) : undefined;
    const dropsExpired = (method: string): boolean => dropExpired === true || expiring?.has(method) === true;
    const receiveFor = (method: string): ((msg: unknown) => void) | undefined => {
      if (!dropsExpired(method)) return receive;
      return (msg: unknown): void => {
        if (isRequest(msg)) arrivals.set(msg, performance.now());
        receive?.(msg);
      };
    };
    const expired = (msg: RPCMessage): boolean => {
      const ttl = msg.__ttl;
      const arrived = arrivals.get(msg);
      return typeof ttl === 'number' && ttl > 0 && arrived !== undefined && performance.now() - arrived > ttl + EXPIRY_SLACK_MS;
    };

    const serve = (method: string, handler: MethodHandler): ((msg: RPCMessage) => void | Promise<void>) => {
      const drops = dropsExpired(method);
      return (msg: RPCMessage): void | Promise<void> => {
        // answered with CONNECTION_CLOSED by a drain deadline before it could start
        if (open && isRequest(msg) && !open.has(msg)) return;
        if (drops && expired(msg)) {
          claim(msg);
          return;
        }
        const response: RPCResponse = { id: msg.id };

        // the method list only on request, old clients never send __discover
        if (msg.__discover === true) {
          response.__methods = methodNames;
        }

        const kind = sessionKind(msg.params);
        return kind ? handleSession(kind, handler, method, msg, response) : handleCall(handler, msg, response);
      };
    };
    const concurrencyOf = (method: string): SubscriptionEntry['concurrency'] => {
      const limit = Math.floor(options?.concurrency?.[method] ?? 1);
      return limit >= 2 ? { limit, isShared: (msg: RPCMessage) => sessionKind(msg?.params) === undefined } : undefined;
    };
    const answerMethods = (msg: RPCMessage): void => {
      if (!isRequest(msg)) return;
      try {
        client.publishOrChunk(`rpc.reply.${msg.id}`, { id: msg.id, result: methodNames } satisfies RPCResponse)?.catch(() => {});
      } catch {
        // not connected
      }
    };
    const queue = options?.queue ? { queue: options.queue } : undefined;

    if (options?.namespaceSubscription === true) {
      const prefix = `rpc.${namespace}.`;
      // one serial lane per method, made on its first request
      const lanes = new Map<string, { dispatch: (data: any) => void; receive?: (msg: unknown) => void }>();
      const route = (msg: RPCMessage, subject: string): void => {
        const method = subject.slice(prefix.length);
        if (!Object.hasOwn(handlersMap, method)) {
          if (method === METHODS_PROBE) answerMethods(msg);
          else client.replyMissingMethod(msg, method, methodNames);
          return;
        }
        let lane = lanes.get(method);
        if (!lane) {
          const handle = serve(method, handlersMap[method]);
          const onError = (error: unknown) => console.error(`Error in handler for ${subject}:`, error);
          const concurrency = concurrencyOf(method);
          const dispatcher = concurrency ? createBoundedDispatcher(handle, concurrency.limit, concurrency.isShared, onError) : createSerialDispatcher(handle, onError);
          lane = { dispatch: dispatcher.dispatch, receive: receiveFor(method) };
          lanes.set(method, lane);
        }
        lane.receive?.(msg);
        lane.dispatch(msg);
      };
      // unknown methods share the subscription's transfers, so lanes stay bounded by the registered methods
      const chunkLane = (subject: string): string | undefined => {
        const method = subject.slice(prefix.length);
        return Object.hasOwn(handlersMap, method) ? method : undefined;
      };
      const { entry, unsubscribe } = client.addSubscription(`${prefix}>`, () => {}, queue, undefined, undefined, route, chunkLane);
      responder.entries.push(entry);
      unsubscribers.push(unsubscribe);
    } else {
      for (const [method, handler] of Object.entries(handlersMap)) {
        const { entry, unsubscribe } = client.addSubscription(`rpc.${namespace}.${method}`, serve(method, handler), queue, concurrencyOf(method), receiveFor(method));
        responder.entries.push(entry);
        unsubscribers.push(unsubscribe);
      }

      // subscribed after the methods: once it answers, every method of this registration is served
      if (!Object.hasOwn(handlersMap, METHODS_PROBE)) {
        const { entry, unsubscribe } = client.addSubscription(`rpc.${namespace}.${METHODS_PROBE}`, answerMethods, queue);
        responder.methods = entry;
        unsubscribers.unshift(unsubscribe);
      }
    }
    client.responders.add(responder);
    // a call from another connection right after the return finds every method
    await client.awaitSubscriptions();

    const cleanup = async () => {
      if (open) {
        // sessions end at once, the requests received so far get until drainTimeoutMs
        await Promise.allSettled([client.drainResponders([responder]), ...owner.stop()]);
      }
      client.responders.delete(responder);

      for (const unsub of unsubscribers) {
        unsub();
      }

      // also ends the sessions whose handler still sets up
      await Promise.allSettled(owner.stop());

      if (options?.isolatedConnection) {
        await client.disconnect();
        this.forgetIsolated(client);
      }
    };

    return cleanup;
  }

  public async onRequest<TRequest = any, TResponse = any>(
    pattern: string,
    handler: (data: TRequest) => Promise<TResponse> | TResponse,
    options?: { ignoreSender?: string },
  ): Promise<() => void> {
    if (!this.nc) {
      throw new Error('Not connected');
    }

    return this.subscribeMessages(pattern, (msg) => {
      if (options?.ignoreSender !== undefined && msg.headers?.get(SENDER_HEADER) === options.ignoreSender) return;
      (async () => {
        try {
          const data = decodeMessage(msg.data);

          const result = await handler(data);

          if (msg.reply) {
            const response = encodeMessage(result);
            msg.respond(response);
          }
        } catch (error) {
          if (msg.reply) {
            const { code, message } = formatErrorObject(error);
            const errorResponse = encodeMessage({ error: message, code });
            msg.respond(errorResponse);
          }
          console.error(`Error in request handler for ${pattern}:`, error);
        }
      })();
    });
  }

  public async channel(channelId: string, options?: { isolatedConnection?: boolean }): Promise<Channel> {
    const client = options?.isolatedConnection ? this.isolatedChannelClient(`${this.options.name}-channel-${channelId}`) : this;
    return this.openChannel(client, new Channel(client, channelId));
  }

  public async privateChannel(channelId: string, targetClientId: string, options?: { isolatedConnection?: boolean }): Promise<PrivateChannel> {
    const client = options?.isolatedConnection ? this.isolatedChannelClient(`${this.options.name}-private-${channelId}`) : this;
    return this.openChannel(client, new PrivateChannel(client, channelId, targetClientId));
  }

  private isolatedChannelClient(name: string): RPCClient {
    const client = this.createIsolatedClient({ ...this.options, name });
    this.isolatedClients.push(client);
    return client;
  }

  private async openChannel<C extends Channel>(client: RPCClient, channel: C): Promise<C> {
    const isolated = client !== this;
    try {
      if (isolated) await client.connect();
      await channel.init();
    } catch (error) {
      if (isolated) {
        await client.disconnect().catch(() => {});
        this.forgetIsolated(client);
      }
      throw error;
    }
    if (isolated) {
      (channel as any)._isolatedClient = client;
      (channel as any)._releaseIsolated = () => this.forgetIsolated(client);
    }
    return channel;
  }

  private forgetIsolated(client: RPCClient): void {
    const index = this.isolatedClients.indexOf(client);
    if (index >= 0) this.isolatedClients.splice(index, 1);
  }

  public createProxy<T extends object>(namespace: string): Promisify<T>;
  public createProxy<T extends object>(namespace: string, options: { isolatedConnection?: false; onTiming?: ProxyTimingReporter } & ProxyCallOptions): Promisify<T>;
  public createProxy<T extends object>(
    namespace: string,
    options: { isolatedConnection: true; onTiming?: ProxyTimingReporter } & ProxyCallOptions,
  ): {
    proxy: Promisify<T>;
    close: () => Promise<void>;
  };
  // prettier-ignore
  public createProxy<T extends object>(
    namespace: string,
    options?: { isolatedConnection?: boolean; onTiming?: ProxyTimingReporter } & ProxyCallOptions,
  ):
    | {
      proxy: Promisify<T>;
      close: () => Promise<void>;
    }
    | Promisify<T> {
    let client: RPCClient;

    if (options?.isolatedConnection) {
      client = this.createIsolatedClient({
        ...this.options,
        name: `${this.options.name}-proxy-${namespace}`,
      });
      this.isolatedClients.push(client);
    } else {
      // eslint-disable-next-line @typescript-eslint/no-this-alias
      client = this;
    }

    const proxy = createProxy<T>(client, namespace, [], undefined, options?.onTiming, { noResponderRetry: options?.noResponderRetry, timeout: options?.timeout });

    if (options?.isolatedConnection) {
      (proxy as any)._isolatedClient = client;

      return {
        proxy,
        close: async () => {
          await client.disconnect();
          this.forgetIsolated(client);
        },
      };
    } else {
      return proxy;
    }
  }

  public async createServiceProxy<T extends object>(serviceName: string): Promise<Promisify<T>>;
  public async createServiceProxy<T extends object>(
    serviceName: string,
    options: { preferredId?: string; timeout?: number; isolatedConnection?: false },
  ): Promise<Promisify<T>>;
  public async createServiceProxy<T extends object>(
    serviceName: string,
    options: { preferredId?: string; timeout?: number; isolatedConnection?: true },
  ): Promise<{ proxy: Promisify<T>; close: () => Promise<void> }>;
  public async createServiceProxy<T extends object>(
    serviceName: string,
    options?: {
      preferredId?: string;
      timeout?: number;
      isolatedConnection?: boolean;
    },
  ): Promise<{ proxy: Promisify<T>; close: () => Promise<void> } | Promisify<T>> {
    let client: RPCClient;

    if (options?.isolatedConnection) {
      client = this.createIsolatedClient({
        ...this.options,
        name: `${this.options.name}-service-${serviceName}`,
      });
      this.isolatedClients.push(client);
      await client.connect();
    } else {
      // eslint-disable-next-line @typescript-eslint/no-this-alias
      client = this;
    }

    const monitor = client.service.monitor();
    const services: ServiceInfo[] = [];

    for await (const info of await monitor.info(serviceName)) {
      services.push(info);
    }

    if (services.length === 0) {
      if (options?.isolatedConnection) {
        await client.disconnect();
        this.forgetIsolated(client);
      }
      throw new Error(`No services found with name: ${serviceName}`);
    }

    const selected = options?.preferredId ? (services.find((s) => s.id === options.preferredId) ?? services[0]) : services[0];

    const proxy = createServiceProxy<T>(client, selected, options?.timeout);

    if (options?.isolatedConnection) {
      (proxy as any)._isolatedClient = client;

      return {
        proxy,
        close: async () => {
          await client.disconnect();
          this.forgetIsolated(client);
        },
      };
    } else {
      return proxy;
    }
  }
}

function makeAbortableConnect(p: Promise<NatsConnection>, signal: AbortSignal | undefined): Promise<NatsConnection> {
  if (!signal) return p;
  if (signal.aborted) {
    p.then((nc) => closeLeaked(nc)).catch(() => {});
    return Promise.reject(new DOMException('Aborted', 'AbortError'));
  }
  return new Promise<NatsConnection>((resolve, reject) => {
    let settled = false;
    const onAbort = (): void => {
      if (settled) return;
      settled = true;
      p.then((nc) => closeLeaked(nc)).catch(() => {});
      reject(new DOMException('Aborted', 'AbortError'));
    };
    signal.addEventListener('abort', onAbort, { once: true });
    p.then(
      (val) => {
        if (settled) {
          closeLeaked(val);
          return;
        }
        settled = true;
        signal.removeEventListener('abort', onAbort);
        resolve(val);
      },
      (err) => {
        if (settled) return;
        settled = true;
        signal.removeEventListener('abort', onAbort);
        reject(err);
      },
    );
  });
}

function encodedSize(message: unknown): number {
  const parts = encodeMessageParts(message);
  return parts instanceof Uint8Array ? parts.length : parts.size;
}

function copyHeaders(source: MsgHdrs | undefined): MsgHdrs {
  const hdrs = headers();
  for (const key of source?.keys() ?? []) {
    for (const value of source!.values(key)) hdrs.append(key, value);
  }
  return hdrs;
}

function payloadLimit(server: number | undefined, cap: number | undefined): number {
  const limits = [server, cap].filter((limit): limit is number => typeof limit === 'number' && limit > 0);
  return limits.length > 0 ? Math.min(...limits) : 1024 * 1024;
}

function disposeChunkScope(scope: ChunkScope): void {
  scope.manager?.dispose();
  for (const manager of scope.lanes?.values() ?? []) manager.dispose();
}

function closeLeaked(nc: NatsConnection): void {
  const withAbort = nc as NatsConnection & { abortClose?: (err?: Error) => void };
  try {
    if (typeof withAbort.abortClose === 'function') {
      withAbort.abortClose();
    } else {
      nc.close().catch(() => {});
    }
  } catch {
    // ignore
  }
}

function drainSubscription(sub: Subscription | undefined): Promise<void> {
  if (!sub || sub.isClosed()) return Promise.resolve();
  if (typeof sub.drain !== 'function') {
    sub.unsubscribe();
    return Promise.resolve();
  }
  return sub.drain().catch(() => {
    // closed already, nothing more arrives
  });
}

function noRespondersError(subject: string): InstanceType<typeof errors.NoRespondersError> {
  return markLocalNoResponders(new errors.NoRespondersError(subject), subject);
}

function forbiddenError(subject: string): RPCException {
  return createError(ERROR_CODES.FORBIDDEN, `Permission denied for "${subject}"`, { subject });
}

function refusedPublishSubject(error: unknown): string | undefined {
  // by name: the browser build may hand over the error class of another nats.js copy
  const refused = error as { name?: string; operation?: string; subject?: string } | undefined;
  if (refused?.name !== 'PermissionViolationError' || refused.operation !== 'publish' || !refused.subject) return undefined;
  return refused.subject;
}

function namespaceSubscribable(namespace: string): boolean {
  const first = namespace.split('.', 1)[0];
  return first !== 'reply' && first !== 'cb';
}

function isRequest(msg: unknown): msg is RPCMessage {
  return typeof (msg as RPCMessage | null | undefined)?.id === 'string';
}

function isThenable(value: unknown): value is PromiseLike<unknown> {
  return value !== null && (typeof value === 'object' || typeof value === 'function') && typeof (value as { then?: unknown }).then === 'function';
}
