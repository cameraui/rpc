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

// inline replies of one socket read would leave in a single write; writing every 8th keeps a caller
// with many calls in flight working on the first replies instead of waiting for the whole batch
const SYNC_REPLY_FLUSH_EVERY = 8;
const DEFAULT_DRAIN_TIMEOUT_MS = 250;
// bounds the PING/PONG that makes new subscriptions visible to other connections
const SUBSCRIBE_FLUSH_TIMEOUT_MS = 2000;
// rpc.<ns>.__methods answers with the method names of a registration
const METHODS_PROBE = '__methods';
// no-responder retry when the caller sets neither delays nor maxRetries: about 3.5 s, first retry soon
const DEFAULT_RETRY_DELAYS = [25, 50, 100, 325, 1000, 2000];
// what a caller gets that sets only one of the two
const LEGACY_RETRY_DELAYS = [500, 1000, 2000];
const LEGACY_MAX_RETRIES = 3;
// the __methods probe may answer during this long, also when the first retry delay is shorter
const METHOD_PROBE_WINDOW_MS = 500;
// dropExpired skips a request that waited longer than its __ttl plus this
const EXPIRY_SLACK_MS = 5;
// room for NATS protocol overhead and headers in every message
const PAYLOAD_RESERVE = 8192;
// smallest limit that still chunks, so a chunk carries at least PAYLOAD_RESERVE bytes
const MIN_PAYLOAD_LIMIT = 16384;
// the channel end that sent a request, so its own onRequest leaves it to the other end
const SENDER_HEADER = 'x-channel-sender';

export function createRPCClient(options: RPCClientOptions): RPCClientImpl {
  return new RPCClient(options);
}

/** Chunk reassembly of one subscription, or of the queue members of one group on this client. */
interface ChunkScope {
  key: string;
  members: number;
  /** Created with the first chunked header */
  manager?: ChunkingManager;
  /** One per method of a namespace subscription, created with its first chunked header */
  lanes?: Map<string, ChunkingManager>;
}

interface SubscriptionEntry {
  pattern: string;
  handler: (data: any) => void | Promise<void>;
  options?: { queue?: string };
  /** Chunk reassembly of this subscription, kept across restores */
  chunks?: ChunkScope;
  /** Raw mode: the handler receives the undecoded NATS Msg (with headers and
   * subject) instead of decoded payload data. No chunk reassembly, no serial
   * dispatcher — used by the muxed reply inbox which routes chunks itself. */
  raw?: boolean;
  sub?: Subscription;
  /** Awaits the subscription's handler chain as of call time — i.e. every
   * message dispatched so far has had its handler executed. */
  flush?: () => Promise<void>;
  /** Up to `limit` handlers in flight for messages `isShared` accepts; the
   * rest stay serial. Without it the subscription is fully serial. */
  concurrency?: { limit: number; isShared: (data: any) => boolean };
  /** Bounded dispatcher of a concurrent entry, kept across restores so the
   * in-flight count survives a suspend/connect cycle. */
  dispatcher?: { dispatch: (data: any) => void; flush: () => Promise<void> };
  /** Sees every message when it arrives, before the dispatcher queues it. */
  receive?: (data: any) => void;
  /** Takes every decoded or reassembled message with the subject it came on,
   * in place of the dispatcher. */
  route?: (data: any, subject: string) => void;
  /** The method a namespace subscription serves on `subject`, undefined for
   * one it does not have. Each method reassembles its chunks on its own, so it
   * keeps the transfer cap it has with a subscription of its own. */
  lane?: (subject: string) => string | undefined;
}

/** The method subscriptions of one registerHandler() call. */
interface Responder {
  entries: SubscriptionEntry[];
  /** rpc.<ns>.__methods, stopped before the methods */
  methods?: SubscriptionEntry;
  /** Requests received and not answered yet; undefined with drainTimeoutMs 0 */
  open?: Set<RPCMessage>;
  /** Set while a drain waits for `open` to empty */
  onIdle?: () => void;
  /** The drain in progress, shared by overlapping callers */
  draining?: Promise<void>;
}

/** A proxy call's method, checked against rpc.<namespace>.__methods after its first no-responder status. */
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
  /** Teardowns of every handler session this client serves. */
  private sessionTeardowns = new Set<() => Promise<void>>();
  /** The handler registrations served on this connection. */
  private responders = new Set<Responder>();

  private nc?: NatsConnection;
  /** The connection suspend(), disconnect() or abortClose() closes: its close
   * event is not a connection loss. */
  private closingNc?: NatsConnection;
  private subscriptionSeq = 0;
  private subscriptionEntries = new Map<number, SubscriptionEntry>();
  private pullIteratorSettles = new Set<() => void>();

  /** First dot-separated segment of every id this client generates. Equals
   * the connId when one is configured (browser clients: the firewall
   * allowlists `rpc.reply.<connId>.>`), otherwise a local random prefix.
   * All reply subjects derived from those ids therefore fall under one
   * wildcard: `rpc.reply.<replyPrefix>.>` — the muxed reply inbox. */
  private readonly replyPrefix: string;

  /** The single persistent reply-mux subscription entry (wildcard
   * `rpc.reply.<replyPrefix>.>`). Lives in subscriptionEntries so
   * suspend()/connect() restore it like any other subscription. */
  private muxEntry?: SubscriptionEntry;

  /** Chunked replies on the mux, separate from handler traffic so foreign
   * headers on handler subjects cannot use up the reply transfers. */
  private muxChunking: ChunkingManager;
  /** Bytes the buffers of all chunked transfers into this client may hold
   * together, shared by every manager. */
  private chunkBudget: TransferBudget;
  /** Chunk scopes of the subscriptions, so the transfer cap applies per
   * subscription instead of per client. */
  private chunkScopes = new Map<string, ChunkScope>();
  private chunkScopeSeq = 0;
  /** Chunk managers of the service endpoints on this connection, one per endpoint. */
  private endpointChunking = new Set<ChunkingManager>();

  /** 503/no-responder handlers keyed by reply-subject suffix (the part after
   * `rpc.reply.`). Used by pull-iterator/stream paths whose per-message
   * `reply` subject only ever carries no-responder statuses. One-shot:
   * the mux dispatcher removes an entry when it fires (max:1 semantics). */
  private statusHandlers = new Map<string, (err: Error) => void>();
  private _maxPayloadSize: number = 1024 * 1024; // Default 1MB
  /** The payload limit of the connection while it is below MIN_PAYLOAD_LIMIT: nothing is chunked then. */
  private unchunkableLimit?: number;
  private connectionPromise?: Promise<NatsConnection>;
  private closed = false;

  private isolatedClients: RPCClient[] = [];

  /** Replies answered inline during the current socket read, reset by a microtask. */
  private syncReplies = 0;

  /** Set while nats.js reconnects: requests wait here in order instead of
   * going into an outbound buffer the next dial attempt discards. */
  private linkWaiters?: (() => void)[];

  private pendingRequests = new Map<
    string,
    {
      resolve: (value: any) => void;
      reject: (error: any) => void;
      timeout?: NodeJS.Timeout;
      cleanup?: () => void;
      /** Request subject — used for the NoRespondersError message when a
       * 503 status arrives on the call's muxed reply subject. */
      subject?: string;
      /** Key of the chunked reply being assembled for this call, if any. */
      transferKey?: string;
    }
  >();

  private streamHandlers = new Map<
    string,
    {
      push: (value: any) => void;
      end: () => void;
      error: (error: any) => void;
      /** Request subject of the stream start. */
      subject?: string;
      /** The server refused the stream start. */
      refuse?: (error: RPCException) => void;
    }
  >();

  /**
   * Check connection status
   */
  get isConnected(): boolean {
    return this.nc?.isClosed() === false;
  }

  /**
   * True when the connection is stale by wall-clock (heartbeat deadline passed
   * while the timer was throttled/frozen in the background). Detects a dead
   * socket on resume without waiting for the ping timeout to fire.
   */
  get isStale(): boolean {
    // isStale() is a camera.ui fork addition not present on the published
    const nc = this.nc as unknown as { isStale?: () => boolean } | undefined;
    return nc?.isStale?.() ?? false;
  }

  get isClosed(): boolean {
    return this.closed;
  }

  /**
   * Get the maximum payload size
   */
  get maxPayloadSize(): number {
    return this._maxPayloadSize;
  }

  /**
   * Access the underlying NATS connection status events.
   * Yields events like 'reconnect', 'disconnect', 'reconnecting'.
   */
  public status(): AsyncIterable<Status> | undefined {
    return this.nc?.status();
  }

  /**
   * Active liveness probe via NATS PING/PONG round-trip. Resolves when a PONG
   * arrives, rejects on timeout. Caller treats timeout as "connection is dead"
   * — the underlying socket may still report OPEN in that case (silent-dead
   * TCP). Use a tight timeout (a few seconds) so a stale connection is
   * detected promptly.
   */
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
    // With a connId the prefix MUST be exactly the connId — the server-side
    // firewall allowlists `rpc.reply.<connId>.>` for browser clients.
    this.replyPrefix = options.connId ?? generateReplyPrefix();
    this.chunkBudget = new TransferBudget(options.maxTransferSize);
    this.chunkingManager = this.newChunkingManager();
    // no transfer cap: a reply transfer needs a pending call, so those bound it
    this.muxChunking = this.newChunkingManager(Infinity);
  }

  /**
   * Create a new isolated RPC client
   * @param options - Options for the isolated client
   */
  public createIsolatedClient(options: RPCClientOptions): RPCClient {
    return new RPCClient(options);
  }

  /**
   * Connect to NATS server. Accepts an AbortSignal so callers can cancel
   * an in-flight handshake when another candidate has won.
   * The underlying nats-js connect() does not itself accept a signal,
   * so we wrap it: on abort we synchronously close any late-arriving connection
   * via the fork's abortClose() and reject with AbortError.
   */
  public async connect(options?: { signal?: AbortSignal }): Promise<NatsConnection> {
    if (options?.signal?.aborted) throw new DOMException('Aborted', 'AbortError');

    presizeSplitPayloads();
    packSmallPublishes();
    shareReadBuffer();

    if (this.nc && !this.nc.isClosed()) {
      // Already connected
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
      // noEcho: true,
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
    void nc.closed().then(lost, lost);
    // a hold the previous connection left behind would never be released
    this.releaseLinkWaiters();
    this.watchLink(nc);

    // A client that was disconnect()ed or abortClose()d is revivable by an
    // explicit connect(). Without this reset, auto-connect in _callOnce and
    // the no-responder retry loop stay permanently disabled.
    this.closed = false;

    this.service.init(this.nc);

    let serverLimit: number | undefined;
    try {
      serverLimit = this.nc.info?.max_payload;
    } catch {
      // no server info on this transport
    }

    const limit = payloadLimit(serverLimit, this.options.maxPayloadSize);
    // a message goes out whole while it fits the limit, a bigger one fails in publishChunks
    this.unchunkableLimit = limit < MIN_PAYLOAD_LIMIT ? limit : undefined;
    this._maxPayloadSize = this.unchunkableLimit ?? limit - PAYLOAD_RESERVE;

    // Register the muxed reply inbox (idempotent). Registered as a normal
    // subscription entry so the restore loop below (re-)subscribes it on
    // first connect and after every suspend cycle alike.
    this.ensureMuxSubscription(false);

    // Restore subscriptions after reconnect (from suspend). Entries keep
    // their identity so unsubscribe closures held by callers stay valid.
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

  /**
   * Disconnect from NATS server
   * Cleans up resources without rejecting pending operations
   */
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
    // Entries were dropped — a revive-connect() must re-register the mux.
    this.muxEntry = undefined;
    this.chunkScopes.clear();
    await Promise.allSettled([...drained, ...sessions]);

    // Disconnect isolated clients
    await Promise.allSettled(this.isolatedClients.map((client) => client.disconnect()));
    this.isolatedClients = [];

    // Drain and close connection
    if (this.nc) {
      await this.flushBeforeClose(this.nc);
      try {
        await this.nc.close();
      } catch {
        // Ignore errors
      }

      await this.waitClosed(this.nc);
      this.nc = undefined;
    }
    rejectClosed();
  }

  /**
   * Update connection options between suspend() and connect(). Used for token
   * rotation / endpoint switching: subscription metadata is preserved by
   * suspend(), reconfigure() points the next connect() at the new server, and
   * connect() re-subscribes everything on the fresh transport.
   */
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

  /**
   * Hot-swap the server pool without dropping the live connection.
   * Use this when only the URL needs to change but the existing connection
   * is still authenticated and serving traffic. The new pool kicks in on the next auto-reconnect.
   *
   * For a forced switch (e.g. endpoint host changed), use suspend() +
   * reconfigure() + connect() instead.
   *
   * Limitation: has no effect on a connect() that is still dialing its FIRST
   * connection (waitOnFirstConnect) — the dial loop captured the server list
   * at connect() time. Tear the client down (abortClose) and build a fresh
   * one to redirect an initial dial.
   */
  public setServers(servers: string[]): void {
    this.options.servers = servers;
    if (this.nc && !this.nc.isClosed()) {
      this.nc.setServers(servers);
    }
    for (const child of this.isolatedClients) {
      child.setServers(servers);
    }
  }

  /**
   * Force the current dial loop (if any) to immediately retry with the latest
   * server pool — including from a stuck mid-handshake or sleeping-on-delay
   * state. Falls back to standard `reconnect()` on transports that don't ship
   * the fork's `forceReconnect()` (e.g. server-side TCP via npm `@nats-io/nats-core`).
   */
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
      // Either method may reject if the connection raced into closed —
      // not actionable from here.
    }
    await Promise.allSettled(this.isolatedClients.map((c) => c.forceReconnect()));
  }

  /**
   * Synchronous force-tear-down without awaiting the transport's close
   * handshake. Use during network changes when the WS is half-open and a
   * normal `close()` would hang for 2s+ on the dead socket. Falls back to
   * fire-and-forget `close()` on transports without the fork patch.
   */
  public abortClose(err?: Error): void {
    this.closed = true;
    this.closingNc = this.nc;
    void Promise.allSettled(this.dropConnectionState(false));

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
          // Server fallback: fire-and-forget. nc.close() returns a Promise we
          // intentionally don't await — caller wants synchronous semantics.
          nc.close().catch(() => {});
        }
      } catch {
        // ignore
      }
      this.nc = undefined;
    }
  }

  /**
   * Suspend the connection without clearing subscription metadata.
   * After calling suspend(), connect() will restore previous subscriptions,
   * also those of the isolated connections, which suspend and reconnect with it.
   */
  public async suspend(): Promise<void> {
    const drained = await this.drainAll();
    this.closingNc = this.nc;
    const closedRejects: (() => void)[] | undefined = this.options.zeroCopyBinaries === true ? [] : undefined;
    // entries stay, connect() restores them on the fresh transport
    const sessions = this.dropConnectionState(true, closedRejects);
    const rejectClosed = this.rejectAfterQueuedWrites(closedRejects);
    await Promise.allSettled([...drained, ...sessions]);

    // Suspend isolated clients
    await Promise.allSettled(this.isolatedClients.map((client) => client.suspend()));

    // Drain and close connection
    if (this.nc) {
      await this.flushBeforeClose(this.nc);
      try {
        await this.nc.close();
      } catch {
        // Ignore errors
      }

      await this.waitClosed(this.nc);
      this.nc = undefined;
    }

    this.connectionPromise = undefined;
    rejectClosed();
  }

  /**
   * Public publish method
   */
  public async publish<TMessage = any>(subject: string, data: TMessage, opts?: { headers?: MsgHdrs; reply?: string }): Promise<void> {
    return this.publishOrChunk(subject, data, opts);
  }

  /**
   * Publish without a promise when the encoded payload fits one message. A
   * payload above maxPayloadSize is chunked from the bytes already encoded:
   * returns undefined once the message is queued, otherwise the promise of
   * the chunked publish. Throws synchronously when not connected or when the
   * data cannot be encoded. Wire format is identical to publish().
   *
   * With `zeroCopy` a large frame is written without copying its binaries
   * (see publishFrameZeroCopy); only call requests pass it.
   *
   * Internal: the handler reply paths use it, so an oversized result is
   * encoded once. Not part of the public RPCClient interface.
   */
  public publishOrChunk<TMessage = any>(subject: string, data: TMessage, opts?: { headers?: MsgHdrs; reply?: string }, zeroCopy = false): Promise<void> | undefined {
    if (!this.nc) {
      throw new Error('Not connected');
    }

    const encoded = this.encodeOrPublishFrame(subject, data, opts, zeroCopy);
    if (!encoded) {
      return undefined;
    }

    // Small enough to send directly
    if (encoded.length <= this._maxPayloadSize) {
      this.nc.publish(subject, encoded, opts);
      return undefined;
    }

    return this.publishChunks(this.nc, subject, encoded, opts);
  }

  /**
   * Synchronous publish fast path for small payloads.
   *
   * Encodes and publishes in one synchronous step — no promise allocation,
   * no microtask hop. Returns false when the encoded payload exceeds
   * maxPayloadSize and needs chunking; the caller must then fall back to
   * the async publish(). Wire format is identical to publish().
   *
   * Internal: used by hot oneway paths (callback proxy, pull-iterator
   * responses). Not part of the public RPCClient interface.
   */
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

  /**
   * Chunk reassembly for one service endpoint: a transfer cap of its own on
   * the budget of this client, like a subscription. suspend() and close drop
   * its transfers.
   *
   * Internal: RPCService uses it. Not part of the public RPCClient interface.
   */
  public createEndpointChunking(): ChunkingManager {
    const manager = this.newChunkingManager();
    this.endpointChunking.add(manager);
    return manager;
  }

  /**
   * Drop the transfers of a manager from createEndpointChunking() once its
   * endpoint stops.
   *
   * Internal: RPCService uses it. Not part of the public RPCClient interface.
   */
  public releaseEndpointChunking(manager: ChunkingManager): void {
    manager.dispose();
    this.endpointChunking.delete(manager);
  }

  /**
   * Register the teardown of a handler session, so disconnect(), suspend()
   * and a lost connection end it too. Returns the function that removes it.
   *
   * Internal: the session handlers use it. Not part of the public RPCClient interface.
   */
  public trackSession(teardown: () => Promise<void>): () => void {
    this.sessionTeardowns.add(teardown);
    return () => {
      this.sessionTeardowns.delete(teardown);
    };
  }

  /**
   * A reply subject on this client's reply inbox. When the server answers a
   * publish that carries it with a no-responder status, `onNoResponders` runs
   * once. `forget` drops it unused. Connection loss, suspend() and
   * disconnect() drop it too.
   *
   * Internal: the session probes use it. Not part of the public RPCClient interface.
   */
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

  /**
   * Subscribe with a handler that gets the NATS message as it arrived, with
   * subject, reply and headers and the payload not decoded. No chunk
   * reassembly and no dispatcher. connect() restores it like every
   * subscription; with `restore` false it ends with the connection, like a
   * service endpoint.
   *
   * Internal: onRequest() and services use it. Not part of the public RPCClient interface.
   */
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

  /**
   * Public subscribe method
   */
  public async subscribe<TResponse = any>(pattern: string, handler: (data: TResponse) => void | Promise<void>, options?: { queue?: string }): Promise<() => void> {
    const { unsubscribe } = await this.subscribeEntry(pattern, handler, options);
    return unsubscribe;
  }

  /**
   * Internal subscribe that also exposes the SubscriptionEntry, so callers
   * (e.g. the pull-callback iterator) can flush the per-subscription handler
   * chain deterministically.
   */
  private async subscribeEntry<TResponse = any>(
    pattern: string,
    handler: (data: TResponse) => void | Promise<void>,
    options?: { queue?: string },
    concurrency?: SubscriptionEntry['concurrency'],
    receive?: SubscriptionEntry['receive'],
  ): Promise<{ entry: SubscriptionEntry; unsubscribe: () => void }> {
    return this.addSubscription(pattern, handler, options, concurrency, receive);
  }

  /** subscribeEntry without the promise, so a registration queues all its SUBs in one pass. */
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
        // Ignore unsubscribe errors
      } finally {
        this.subscriptionEntries.delete(key);
        this.leaveChunkScope(entry);
      }
    };

    return { entry, unsubscribe };
  }

  /**
   * Create the NATS subscription for an entry. Called from subscribe() and
   * again from connect() when restoring entries after a suspend cycle.
   */
  private natsSubscribe(entry: SubscriptionEntry): void {
    const { pattern } = entry;

    // Raw mode (muxed reply inbox): hand the undecoded Msg to the handler —
    // it needs headers (503 status, chunk markers) and the subject, and does
    // its own chunk reassembly and routing. Handlers are synchronous; no
    // serial dispatcher needed.
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

    // Serialize handlers per subscription. This matches Python's behavior
    // (client.py:434 awaits the handler) and is what backpressure-sensitive
    // callers rely on: an awaiting handler blocks the next message from
    // being dispatched, which transitively stalls the producer.
    //
    // createSerialDispatcher keeps that ordering guarantee but skips the
    // promise chain entirely while no async handler is pending — sync
    // handlers run inline (2 fewer promises per message on the hot path).
    // Entries with a concurrency limit (registerHandler opt-in) use the
    // bounded dispatcher instead: plain calls run up to the limit at once,
    // everything else stays serial.
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

      // Expose a flush hook: awaiting it settles the chain as of call time,
      // i.e. every message dispatched so far has had its handler executed.
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
          // Chunked transfer header
          const data = decode(msg.data);
          const chunkId = msg.headers?.get('x-chunk-id');

          if (!chunkId || data?.transferId !== chunkId) {
            console.error('Invalid chunk header');
            return;
          }

          // Setup chunk assembly, the buffer is allocated with the first chunk
          this.chunkManager(chunks, lane?.(msg.subject), true)!.startReceiving(
            chunkId,
            data.totalChunks,
            (assembledData) => {
              runHandler(assembledData, msg);
            },
            (error) => {
              console.error(`Error assembling chunks for ${pattern}:`, error);
            },
            data.totalSize, // Pass totalSize for the single assembly buffer
            data.chunkSize, // Pass chunkSize for correct offset calculation
          );
        } else if (chunkType === 'chunk') {
          // Chunk data
          const chunkId = msg.headers?.get('x-chunk-id');
          const chunkIndex = parseChunkIndex(msg.headers?.get('x-chunk-index'));

          if (!chunkId) {
            console.error('Chunk missing chunk ID');
            return;
          }

          // Process raw chunk data
          this.chunkManager(chunks, lane?.(msg.subject), false)?.processChunk({
            id: chunkId,
            chunkIndex,
            data: msg.data,
            isLast: false, // Determined by total chunks from header
          });
        } else {
          // Regular message - decode wire message (zero-copy views into
          // msg.data for out-of-band binaries; nats.js allocates one buffer
          // per message, so the views are safe).
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

  /**
   * Muxed reply inbox: register (and by default subscribe) the single
   * persistent wildcard subscription `rpc.reply.<replyPrefix>.>` that
   * receives every RPC reply of this client — real responses, chunked
   * responses and 503/no-responder statuses. Idempotent.
   *
   * connect() passes subscribeNow=false: it only registers the entry and
   * lets its restore loop create the actual subscription alongside all
   * other entries (also after suspend cycles).
   */
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

  /**
   * Dispatcher for the muxed reply inbox. Routes by message kind:
   * - 503/no-responder status (empty payload + code 503): the call/iterator
   *   is identified by the SUBJECT (`rpc.reply.<suffix>`) — the server echoes
   *   the request's reply subject, there is no payload to decode.
   * - chunked transfer header/chunk: reassemble via muxChunking while the
   *   call is pending, then route the assembled response by envelope id. A
   *   reply that cannot be assembled rejects the call at once.
   * - regular response: decode and route by envelope id.
   */
  private handleMuxMessage(msg: Msg): void {
    // No-responder status. Wire detail: the reply subject of an RPC call is
    // exactly `rpc.reply.<call id>`, so the suffix IS the call id. For
    // iterator/stream status inboxes the suffix is the registered token.
    if (msg.data?.length === 0 && msg.headers?.code === 503) {
      const suffix = msg.subject.slice('rpc.reply.'.length);

      const statusHandler = this.statusHandlers.get(suffix);
      if (statusHandler) {
        // One-shot (mirrors the previous per-iterator `max: 1` inboxes).
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
      // the subject suffix is the call id; without a pending call nobody reads the reply, so it is dropped
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

  /**
   * Settle the pending request a (possibly reassembled) RPC response belongs
   * to. Unknown ids are dropped silently — late replies after a timeout, or
   * traffic of another client sharing the same connId prefix.
   */
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
      // __methods (proxy method discovery) travels as a side channel next to
      // the result — the result object itself stays untouched.
      pending.resolve({ result: response.result, methods: response.__methods, timing: response.__timing });
    }
  }

  /**
   * Reject a call whose chunked reply cannot be assembled (refused header,
   * bad chunk, stalled or undecodable transfer) right away instead of at its
   * timeout.
   */
  private failChunkedReply(callId: string, error: Error): void {
    const pending = this.pendingRequests.get(callId);
    if (!pending) return;

    this.pendingRequests.delete(callId);
    if (pending.timeout) clearTimeout(pending.timeout);
    if (pending.transferKey) this.muxChunking.discard(pending.transferKey);
    pending.reject(createError(ERROR_CODES.INTERNAL_ERROR, `Chunked reply to "${pending.subject ?? callId}" failed: ${error.message}`));
  }

  /**
   * Force-settle every client-side pull iterator parked in next(). Runs on
   * disconnect/suspend/abortClose so consumers' `for await` loops terminate
   * with a connection error instead of hanging forever.
   */
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

  /**
   * The synchronous part of every session end (suspend, disconnect,
   * abortClose, connection closed for good): handler sessions are torn down,
   * pending calls reject with CONNECTION_CLOSED, client streams end, pull
   * iterators settle, entries lose their subscription but stay, and every
   * chunk manager drops its transfers. Returns the session cleanups still
   * running. A per-call reply entry of the service path is dropped by its
   * pending cleanup, so it is not restored as a dead subscription.
   *
   * With `closedRejects` the rejections of pending calls are collected there
   * instead, for rejectAfterQueuedWrites: a zero-copy request can still sit
   * in the socket queue.
   */
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

  /**
   * Run the rejections dropConnectionState collected once the socket is done
   * with the requests queued so far: when the server answers a PING sent
   * behind them, at the latest when the returned function runs after the
   * close. It does not wait for the session teardowns, which may wait for
   * these calls.
   */
  private rejectAfterQueuedWrites(rejects: (() => void)[] | undefined): () => void {
    const rejectAll = (): void => {
      for (const reject of rejects?.splice(0) ?? []) reject();
    };
    if (rejects?.length && this.nc && !this.nc.isClosed()) {
      this.nc.flush().then(rejectAll, () => {});
    }
    return rejectAll;
  }

  /**
   * Start the teardown of every session this client serves and forget them.
   * Each one stops publishing right here, before any await.
   */
  private endHandlerSessions(): Promise<void>[] {
    const cleanups = [...this.sessionTeardowns];
    this.sessionTeardowns.clear();
    return cleanups.map((cleanup) => cleanup());
  }

  /**
   * The drain of disconnect() and suspend(): every registration of this
   * client and of its isolated clients at once. Sessions end right away;
   * their teardowns are returned, not awaited, since one may wait for a
   * call that only the close rejects.
   */
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

  /**
   * Stop the intake of these registrations and give the requests they
   * received until drainTimeoutMs to finish. The subscriptions are drained:
   * the UNSUB reaches the server first, requests already on their way are
   * still dispatched. Requests unanswered at the deadline get one
   * CONNECTION_CLOSED reply and do not start any more.
   */
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

  /** Answer a request that will not run with CONNECTION_CLOSED, a stream request on its stream subject. */
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

  /**
   * Answer a request for a method a namespace subscription does not have with
   * METHOD_NOT_FOUND and the method list, a stream request on its stream subject.
   */
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

  /**
   * The current connection closed for good without suspend(), disconnect()
   * or abortClose(): tear down at once, the closed flag stays false, so the
   * next call or connect() dials again and restores every entry.
   */
  private onConnectionLost(nc: NatsConnection): void {
    if (nc !== this.nc || nc === this.closingNc) return;
    this.nc = undefined;
    void Promise.allSettled(this.dropConnectionState(false));
  }

  /**
   * Follow the link state of a connection: a 'disconnect' or
   * 'staleConnection' status holds new requests, 'reconnect' releases them.
   * nats.js keeps a publish made while the link is down in a buffer that its
   * next dial attempt discards.
   */
  private watchLink(nc: NatsConnection): void {
    let statuses: AsyncIterable<Status>;
    try {
      statuses = nc.status();
    } catch {
      return;
    }
    void (async () => {
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

  /**
   * nats-server refused a publish of this connection to `subject`: the calls
   * and stream starts waiting there would only run into their timeout, so
   * they fail with FORBIDDEN now. Nothing retries a refused subject.
   */
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

  /**
   * Undefined while the link is up. Otherwise waits while nats.js
   * reconnects, bounded by the call timeout of the client, and rejects with
   * TIMEOUT when the link stays down that long.
   */
  private holdForLink(subject: string): Promise<void> | undefined {
    const timeout = this.options.timeout ?? 30000;
    return this.waitForLink(timeout)?.then((up) => {
      if (!up) throw createError(ERROR_CODES.TIMEOUT, `RPC call to "${subject}" timed out after ${timeout}ms`);
    });
  }

  /** Let the held requests go, in the order they arrived. */
  private releaseLinkWaiters(): void {
    const waiters = this.linkWaiters;
    this.linkWaiters = undefined;
    for (const release of waiters ?? []) release();
  }

  /**
   * Undefined while the link is up. Otherwise resolves true once nats.js has
   * reconnected or the connection ended, false when `ms` passed first.
   */
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

  /**
   * One PING/PONG round trip before close: the Node TCP transport destroys
   * the socket on close() and drops whatever is still queued for writing.
   * Bounded to 1 s (or disconnectTimeout when lower), a dead socket only
   * delays the close.
   */
  private async flushBeforeClose(nc: NatsConnection): Promise<void> {
    if (nc.isClosed()) return;
    await this.flush(Math.min(this.options.disconnectTimeout ?? 2_000, 1_000)).catch(() => {});
  }

  /**
   * One PING/PONG round trip after SUBs were queued: once the server answered,
   * it routes to them, also calls from other connections. Bounded and never
   * rejects, a slow or lost connection only delays the caller.
   */
  private async awaitSubscriptions(): Promise<void> {
    await this.flush(SUBSCRIBE_FLUSH_TIMEOUT_MS).catch(() => {});
  }

  /**
   * Ensure the connection is truly closed. With waitOnFirstConnect: false the
   * underlying WebSocket transport may still be mid-handshake when close() is
   * called. nc.closed() resolves once the NATS protocol is fully shut down,
   * preventing zombie connections that survive a disconnect/reconnect cycle.
   */
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
      // Ignore errors
    } finally {
      clearTimeout(timer);
    }
  }

  /**
   * Encode `data` for publishing. A frame with out-of-band binaries of at
   * least PUBLISH_FRAME_MIN_SIZE that fits one message and carries no headers
   * is published right here as a single buffer (see publishFrame), with
   * `zeroCopy` without copying its binaries where possible; then nothing is
   * returned.
   */
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

  /** Publish the request of a call, without copying large binaries when zeroCopyBinaries is set. */
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

    // Calculate chunk count without materializing the generator
    const totalChunks = Math.ceil(encoded.length / this._maxPayloadSize);

    // Send header message first (MessagePack encoded)
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

    // Send chunks directly from generator (no Array.from() - saves memory)
    let chunkIndex = 0;
    for (const chunk of createChunks(encoded, transferId, this._maxPayloadSize)) {
      const chunkHdrs = headers();
      chunkHdrs.set('x-chunked-transfer', 'chunk');
      chunkHdrs.set('x-chunk-id', transferId);
      chunkHdrs.set('x-chunk-index', chunkIndex.toString());

      // Send raw chunk data (not encoded)
      nc.publish(subject, chunk.data, { headers: chunkHdrs, reply: opts?.reply });

      // Yield every 50 chunks to prevent blocking
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
    // overlapping subscriptions each get every chunk, so each has its own scope;
    // queue members share one because NATS splits a transfer among them
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

  /**
   * Native NATS request/reply
   * @param subject - The subject to send the request to
   * @param data - The request data
   * @param options - Request options including timeout and per-call retry override; `sender`
   * (internal, for channels) marks the request with the channel end that sends it
   */
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
        noMux: false, // Allow request multiplexing
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

    // Check for NATS micro service error response
    if (msg.headers?.get('Nats-Service-Error-Code')) {
      const errorCode = msg.headers.get('Nats-Service-Error-Code') || '500';
      const errorMsg = msg.headers.get('Nats-Service-Error') || 'Service error';
      let errorData: any = null;

      // Try to decode error data
      if (msg.data) {
        try {
          errorData = decodeMessage(msg.data);
        } catch {
          // Ignore decoding errors
        }
      }

      throw createError(errorCode, errorMsg, errorData);
    }

    const decoded = decodeMessage(msg.data);

    // Check if response contains an error field (for request handlers)
    if (decoded?.error) {
      const code = decoded.code ?? ERROR_CODES.INTERNAL_ERROR;
      const message = decoded.error ?? 'Unknown error';
      throw createError(code, message, decoded);
    }

    return decoded;
  }

  /**
   * Retry helper for no-responder errors this client observed itself (a 503
   * status for its own request). An error reply from a responder is never
   * retried, whatever its code. The per-call `override` lets a request that
   * targets a known-flaky responder (e.g. a child process that may be
   * restarting) extend the wait window without affecting the client-wide
   * default.
   */
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

  /**
   * Retries and delays for a no-responder retry, from the call's override
   * field by field, else the client option. A caller that sets neither gets
   * DEFAULT_RETRY_DELAYS with one retry per delay, one that sets only one of
   * them gets 3 retries or the delays 500, 1000 and 2000 ms for the other.
   */
  private retryPlan(override?: { maxRetries?: number; delays?: number[] }): { maxRetries: number; delays: number[] } {
    const maxRetries = override?.maxRetries ?? this.options.noResponderRetry?.maxRetries;
    const delays = override?.delays ?? this.options.noResponderRetry?.delays;
    if (maxRetries === undefined && delays === undefined) {
      return { maxRetries: DEFAULT_RETRY_DELAYS.length, delays: DEFAULT_RETRY_DELAYS };
    }
    return { maxRetries: maxRetries ?? LEGACY_MAX_RETRIES, delays: delays ?? LEGACY_RETRY_DELAYS };
  }

  /**
   * Ask rpc.<namespace>.__methods for the method of a proxy call, started
   * with the first retry delay; undefined without a probe.
   */
  private probeMethod(probe: MethodProbe | undefined, delay: number): Promise<boolean> | undefined {
    if (probe?.probeNamespace === undefined || probe.probeMethod === undefined) return undefined;
    return this.isMethodMissing(probe.probeNamespace, probe.probeMethod, Math.max(delay, METHOD_PROBE_WINDOW_MS), probe.onMethods);
  }

  /**
   * Sleep one retry delay, or until `signal` aborts. With the answer of a
   * method probe pending or in, true as soon as it says the namespace is
   * served without the method, so the caller fails at once. Anything else
   * waits out the delay.
   */
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

  /**
   * One call to rpc.<namespace>.__methods without retry. True when the
   * namespace answers and does not have `method`. A namespace without
   * responders, a responder without __methods (older libs), an error or no
   * answer within `timeoutMs` give false.
   */
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

  /**
   * Make an RPC call
   */
  public async call<TResponse = any>(subject: string, ...args: any[]): Promise<TResponse> {
    const { result } = await this.withNoResponderRetry(() => this._callOnce<TResponse>(subject, args));
    return result;
  }

  /**
   * Make an RPC call and additionally return the response's __methods
   * metadata as a side channel. The result object is passed through
   * untouched — __methods is never spliced into it. Used by createProxy for
   * method discovery; plain call() discards the metadata.
   *
   * With `discover: true` the request envelope carries `__discover: true`,
   * asking the responder to attach its method list to the response. Proxies
   * request this only while their method cache is empty.
   *
   * With `probeNamespace` and `probeMethod` the first no-responder status is
   * checked against rpc.<namespace>.__methods while the first retry delay
   * runs: when the namespace is served but the method is missing, the
   * no-responder error is thrown at once instead of after every retry. The
   * method list that comes back goes to `onMethods`.
   *
   * Internal: not part of the public RPCClient interface.
   */
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
    // no stamps back means the responder does not know __timing: reporting a
    // round trip as pure transport would be worse than reporting nothing
    if (!startedAt || !timing) return { result, methods };

    const totalMs = Date.now() - startedAt;
    // both stamps come from the responder's clock, so the difference holds
    // even when the two processes disagree about the time of day
    const handlerMs = Math.max(0, timing.end - timing.start);
    return { result, methods, timing: { totalMs, handlerMs, transportMs: Math.max(0, totalMs - handlerMs) } };
  }

  /**
   * Make an RPC call (single attempt)
   */
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

    // Service calls (`<subject>.reply.<id>`) keep the legacy per-call
    // subscription flow — separate refactor later.
    if (!subject.startsWith('rpc.')) {
      return this._callOnceService<TResponse>(subject, args, timeoutMs);
    }

    // Normally established by connect(); covers clients whose connection was
    // wired up out-of-band (tests). No-op when already subscribed.
    this.ensureMuxSubscription();

    const id = generateId(this.replyPrefix);
    const timeout = timeoutMs ?? this.options.timeout ?? 30000;
    // The reply subject is derived from the id by pure string concatenation —
    // this is the wire contract with every responder implementation (Node,
    // Go, Python): they publish the response to `rpc.reply.${msg.id}` and
    // treat the id as opaque. Because the id starts with our replyPrefix,
    // the muxed reply inbox (`rpc.reply.<replyPrefix>.>`) catches it.
    const replySubject = `rpc.reply.${id}`;

    const deadline = performance.now() + timeout;

    return new Promise<{ result: TResponse; methods?: string[]; timing?: RPCTiming }>((resolve, reject) => {
      // No per-call subscriptions: responses (plain or chunked) and
      // no-responder statuses all arrive on the muxed reply inbox, which
      // routes them back here via pendingRequests. cleanup only has to drop
      // the map entry and the timer.
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

      // Send request. `reply` is set to the call's own reply subject so the
      // NATS server delivers a no-responder 503 status to the SAME subject
      // the real response would use — the mux catches both.
      const message: RPCMessage = { id, method: 'call', params: args };
      if (discover) {
        // Envelope marker (never in params — must not leak into handler
        // args): ask the responder for its __methods list.
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

  /**
   * A plain call to a service endpoint. A request too large for one message
   * goes to `direct`, the subject of the one instance the proxy picked, when
   * the service has one: a queue group would hand its chunks to different
   * instances.
   *
   * Internal: the service proxy uses it. Not part of the public RPCClient interface.
   */
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

  /**
   * Legacy single-attempt call for service subjects (reply pattern
   * `<subject>.reply.<id>`): per-call reply subscription + one-shot
   * no-responder inbox. The rpc.* path is muxed — see _callOnce.
   */
  private async _callOnceService<TResponse = any>(subject: string, args: any[], timeoutMs?: number, direct?: string): Promise<{ result: TResponse; methods?: string[] }> {
    const id = generateId(this.replyPrefix);
    const timeout = timeoutMs ?? this.options.timeout ?? 30000;
    const message: RPCMessage = { id, method: 'call', params: args };
    const target = direct !== undefined && encodedSize(message) > this._maxPayloadSize ? direct : subject;
    const replySubject = `${target}.reply.${id}`;

    return new Promise<{ result: TResponse; methods?: string[] }>(async (resolve, reject) => {
      // Initialize variables
      let sub: Subscription | undefined;
      let unsubscribe: (() => void) | undefined;

      // Unsubscribe function to clean up
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
            // Ignore unsubscribe errors
          }
        }

        unsubscribe?.();
      };

      // Setup timeout. Must tear down the reply/inbox subscriptions too —
      // a timed-out call otherwise leaks both for the client's lifetime
      // (and they'd be restored again on every reconnect).
      const timeoutHandle = setTimeout(() => {
        if (this.pendingRequests.has(id)) {
          this.pendingRequests.delete(id);
          unsubscribeAll();
          reject(createError(ERROR_CODES.TIMEOUT, `RPC call to "${subject}" timed out after ${timeout}ms`));
        }
      }, timeout);

      // Store pending request
      this.pendingRequests.set(id, {
        resolve,
        reject,
        timeout: timeoutHandle,
        subject: target,
        cleanup: () => unsubscribeAll(),
      });

      // Subscribe to reply
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
              // __methods (proxy method discovery) travels as a side channel
              // next to the result — the result object itself stays untouched.
              pending.resolve({ result: response.result, methods: response.__methods, timing: response.__timing });
            }
          }
        }
      };

      const requestCallback = (err: Error | null, msg: Msg) => {
        // Check for no responders status (empty message with 503 status)
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

  /**
   * Make a streaming RPC call
   */
  public callStream<TResponse = any>(subject: string, ...args: any[]): AsyncGenerator<TResponse> {
    return this.streamWithRetry<TResponse>(subject, args);
  }

  /**
   * callStream with the method probe of callWithMeta: when the first
   * no-responder status comes from a namespace that is served but does not
   * have the method, the stream fails at once instead of after every retry.
   *
   * Internal: createProxy uses it. Not part of the public RPCClient interface.
   */
  public callStreamWithProbe<TResponse = any>(subject: string, args: any[], probe: MethodProbe): AsyncGenerator<TResponse> {
    return this.streamWithRetry<TResponse>(subject, args, probe);
  }

  /**
   * The stream of callStream(): a no-responder status before the first item
   * starts the stream again after the retry delay. A hand-written iterator,
   * so return() ends a stream parked between items or in a retry wait at
   * once instead of after its next step.
   */
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

      // not queued behind a pending next(): ends the attempt now, which settles that next() as done
      async return(value?: void): Promise<IteratorResult<TResponse, void>> {
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

  /**
   * Make a streaming RPC call (single attempt).
   *
   * Manual iterator — see callPullIteratorWithCallback for rationale.
   * The push-based stream still parks the client at `await resolver`
   * while waiting for the next stream message; if the server stops
   * sending without an `end` frame, the generator would hang on
   * iter.return(). Force-settling the pending resolver lets return()
   * run cleanup cleanly.
   */
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
        void cleanupOnce();
      },
    };

    const sendCancel = async (): Promise<void> => {
      try {
        await client.publish(`${streamSubject}.cancel`, { id });
      } catch {
        // ignore
      }
    };

    // a return() during setup runs this before setup is done, setup then removes what it adds later
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

      // No-responder detection via the muxed reply inbox: the request's
      // reply subject `rpc.reply.<id>` only ever carries a 503 status —
      // stream responders never publish a direct RPC response.
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

  /**
   * Make a pull-based iterator RPC call
   */
  public callPullIterator<TResponse = any>(subject: string, ...args: any[]): AsyncGenerator<TResponse> {
    // Manual iterator — see callPullIteratorWithCallback for rationale.
    // `iter.return()` force-settles the pending response promise so
    // cleanup can run even when the server is parked at yield.
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

    // Registered with the client for the iterator's lifetime: a next() parked
    // on the response resolver must be force-settled when the connection is
    // torn down (disconnect/suspend/abortClose), or the consumer's `for await`
    // hangs forever.
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
          // ignore cleanup errors
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
      // 503 status inbox for `next` requests, served by the muxed reply
      // inbox: iteratorId starts with replyPrefix, so this subject falls
      // under the mux wildcard. Real iterator responses keep arriving on
      // responseSubject — only no-responder statuses land here.
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

  /**
   * Pull-iterator-with-callbacks call.
   *
   * Combines a client-driven pull iterator (1 RTT per batch) with a oneway
   * callback channel (fire-and-forget server → client) for low-latency
   * frame-level data delivery with coarse-grained backpressure.
   *
   * The returned async generator yields `undefined` for each batch boundary
   * the server produces. Meaningful data is dispatched through the provided
   * callback object. return() ends it without waiting for callbacks, so a
   * callback may call it: one that runs finishes, the queued ones are dropped.
   *
   * In place of the plain onewayMethods array, a PullCallbackCallOptions
   * object can be passed (direct client API only — the proxy path always
   * passes the array form). `prefetch: true` sends the next `next` request
   * as soon as a batch boundary arrives, before the batch's callbacks are
   * drained — hides one RTT per batch at the cost of strict backpressure.
   */
  public callPullIteratorWithCallback(
    subject: string,
    callbacks: Record<string, (...a: any[]) => any>,
    onewayMethods: string[] | PullCallbackCallOptions,
    ...args: any[]
  ): AsyncGenerator<void> {
    const callOptions = Array.isArray(onewayMethods) ? undefined : onewayMethods;
    const onewayMethodList = Array.isArray(onewayMethods) ? onewayMethods : onewayMethods.onewayMethods;
    const prefetch = callOptions?.prefetch ?? false;
    // Manual iterator implementation (not `async function*`). Rationale:
    // an async generator parked at an `await` cannot be woken by
    // `iter.return()` — per spec, return() queues behind the pending
    // await. When the server is parked at yield and no new response
    // message is in flight, that await never settles and return() hangs
    // forever. Implementing the iterator protocol by hand lets us
    // force-settle the pending response resolver from within return()/
    // throw() and run cleanup synchronously.
    //
    // Setup is deferred to the first next() call to preserve the lazy
    // semantics of the previous `async function*` implementation —
    // factories that are never iterated shouldn't open NATS subs.
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
    // True when the `next` request for the upcoming batch was already sent
    // by the prefetch path — the following next() call must not send again.
    let prefetched = false;

    const settlePendingAsDone = (): void => {
      const r = responseResolver;
      if (r) {
        responseResolver = null;
        r({ id: iteratorId, type: 'done' });
      }
    };

    // See callPullIterator: force-settle a parked next() on connection
    // teardown so the consumer's `for await` terminates instead of hanging.
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
          // ignore cleanup errors
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
      // 503 status inbox for `next` requests — see callPullIterator.
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
          // The request for this batch was already sent by the prefetch
          // path right after the previous boundary arrived.
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
          // Drain callbacks received before the terminal response
          // (mirrors Go's drainCallbacks() on "done").
          await callbackFlush?.();
          await callbackChain;
          await cleanupOnce();
          return { value: undefined, done: true };
        }

        // Opt-in n+1 prefetch: request the next batch BEFORE draining this
        // batch's callbacks, so the server produces batch n+1 while the
        // client processes batch n (hides one RTT per batch, Go behavior).
        // On publish failure fall back silently — the next next() call
        // re-sends the request and surfaces the error there.
        // while nats.js reconnects the next next() sends the request once the link is back
        if (prefetch && !ended && !returned && !client.linkWaiters) {
          try {
            const prefetchRequest: PullIteratorRequest = { id: iteratorId, type: 'next' };
            await client.publish(requestSubject, prefetchRequest, { reply: statusInbox });
            prefetched = true;
          } catch {
            prefetched = false;
          }
        }
        // 'value': wait for the callback handlers of this batch to finish. A
        // slow handler stalls here, so the next request waits and the server
        // parks at its own yield: end-to-end backpressure. Every callback of
        // the batch arrived before its boundary (same connection, publish
        // order) and the callback subscription runs its handler inline, which
        // appends to callbackChain at once, so the chain as it stood when the
        // boundary arrived covers exactly this batch. Callbacks of later
        // batches, sent early by a prefetching or eager producer, do not hold
        // up this one.
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

  /**
   * Make an RPC call with a callback subscription.
   * Returns an unsubscribe function.
   */
  public async callWithCallback<TResponse = any>(subject: string, args: any[], callback: (value: TResponse) => void | Promise<void>): Promise<() => void> {
    if (!this.isConnected && !this.isClosed) {
      await this.connect();
    }

    if (!this.nc) {
      throw new Error('Not connected');
    }

    const id = generateId(this.replyPrefix);
    const callbackSubject = `rpc.cb.${id}`;

    // Subscribe to callback messages
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

    // Send RPC request with callback marker
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

    // Return unsubscribe function
    const unsubscribe = () => {
      this.publish(`${callbackSubject}.cancel`, { id }).catch(() => {});
      unsub();
    };

    return unsubscribe;
  }

  /**
   * Register RPC handlers
   *
   * Calls of one method run one after another by default. `concurrency` lets
   * chosen methods run up to the given number of calls at once, keyed by
   * method name (`{ detect: 4 }`); values below 2 keep the serial default.
   * Calls still start in arrival order, and stream, pull-iterator and
   * callback setups of such a method stay serial.
   *
   * `namespaceSubscription: true` serves the namespace with one subscription
   * on rpc.<namespace>.> instead of one per method, which saves setup time and
   * server interest on namespaces with many methods. Each method keeps its
   * own lane. A call for a method the registration does not have gets
   * METHOD_NOT_FOUND at once instead of no responders, and every subject below
   * the namespace reaches this registration, also one another process serves.
   * A namespace whose first segment is `reply` or `cb` cannot take it.
   *
   * `dropExpired` (true for every method, or a list of method names) skips a
   * request without an answer once it waited in the method's queue longer
   * than its caller waits for it (__ttl of the request plus 5 ms). Only for
   * reads whose answer is worthless once the caller gave up, never for a
   * method whose side effect must happen.
   *
   * Stream, pull-iterator and pull-callback producers end with their session
   * and can watch sessionSignal() to stop while they wait. A session whose
   * consumer is gone ends after the grace of `orphanProbe` (client option by
   * default). On the returned cleanup, disconnect() and suspend() the
   * registration first stops taking requests, then the calls it already
   * received get drainTimeoutMs to finish; unanswered ones get
   * CONNECTION_CLOSED. A plain call that runs when the connection is lost
   * finishes, its reply is lost.
   */
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

    // Use isolated connection if requested
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    let client: RPCClient = this;

    if (options?.isolatedConnection) {
      // Create isolated connection for this handler namespace
      client = this.createIsolatedClient({
        ...this.options,
        name: `${this.options.name}-handler-${namespace}`,
      });

      // Connect the isolated client
      await client.connect();
      this.isolatedClients.push(client);
    }

    const unsubscribers: (() => void)[] = [];
    // only the sessions this namespace created, other registrations keep theirs
    const owner = new SessionOwner();

    // Extract methods based on option
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

      // Diagnostic aid: a METHOD_NOT_FOUND error always carries the
      // method list, discovery requested or not (rare, small).
      if (response.error.code === ERROR_CODES.METHOD_NOT_FOUND) {
        response.__methods = methodNames;
      }

      try {
        await client.publish(replySubject, response);
      } catch (publishError) {
        if (client.isClosed) {
          return; // Ignore publish errors if client is closed
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

          // Don't await stream requests - they run in background and send data via streamSubject
          // Awaiting would block the subscription handler and prevent processing of new messages
          handleStreamRequest(handler, args, streamSubject, msg.id, client, owner, probe).catch((err) => {
            console.error(`Stream request error for ${method}:`, err);
          });
          return; // Don't send RPC response for stream requests
        } else {
          response.result = await openSession(kind, handler, msg.params, msg.id, client, owner, probe);
          if ((claimed = claim(msg))) await client.publish(replySubject, response);
        }
      } catch (error) {
        if (claimed || claim(msg)) await replyError(replySubject, response, error);
      }
    };

    // a handler that returns a plain value is answered inline: no promise, so
    // the serial dispatcher stays on its sync fast path
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
    // waited in the queue longer than the caller waits for it
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

        // Method discovery on demand: only a request whose envelope carries
        // __discover (a proxy with an empty method cache) pays for the
        // namespace's method list — attaching it to every response would be
        // dead wire weight once the proxy cache is filled. Old clients
        // never send __discover and never read __methods on this path.
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

      // Unsubscribe all handlers
      for (const unsub of unsubscribers) {
        unsub();
      }

      // also ends the sessions whose handler still sets up
      await Promise.allSettled(owner.stop());

      // Disconnect isolated connection if used
      if (options?.isolatedConnection) {
        await client.disconnect();
        this.forgetIsolated(client);
      }
    };

    return cleanup;
  }

  /**
   * Setup a request handler (responder). connect() restores it after suspend().
   * @param pattern - The subject pattern to listen for requests
   * @param handler - The handler function that receives data and subject
   * @param options - `ignoreSender` (internal, for channels) leaves the
   * requests a request() with this sender sent to the other subscribers
   */
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
          // Decode request
          const data = decodeMessage(msg.data);

          // Call handler with subject
          const result = await handler(data);

          // Send response
          if (msg.reply) {
            const response = encodeMessage(result);
            msg.respond(response);
          }
        } catch (error) {
          // Send error response
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

  /**
   * Create or join a bidirectional channel
   * @param channelId - Unique channel identifier
   * @param options - Optional configuration
   */
  public async channel(channelId: string, options?: { isolatedConnection?: boolean }): Promise<Channel> {
    const client = options?.isolatedConnection ? this.isolatedChannelClient(`${this.options.name}-channel-${channelId}`) : this;
    return this.openChannel(client, new Channel(client, channelId));
  }

  /**
   * Create a private 1:1 channel
   * @param channelId - Unique channel identifier
   * @param targetClientId - Target client to connect to
   * @param options - Optional configuration
   */
  public async privateChannel(channelId: string, targetClientId: string, options?: { isolatedConnection?: boolean }): Promise<PrivateChannel> {
    const client = options?.isolatedConnection ? this.isolatedChannelClient(`${this.options.name}-private-${channelId}`) : this;
    return this.openChannel(client, new PrivateChannel(client, channelId, targetClientId));
  }

  private isolatedChannelClient(name: string): RPCClient {
    const client = this.createIsolatedClient({ ...this.options, name });
    this.isolatedClients.push(client);
    return client;
  }

  /** Set up a channel; an isolated client of its own goes away with it, also when the setup fails. */
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

  /** Drop an isolated client from the list suspend(), connect() and disconnect() go through. */
  private forgetIsolated(client: RPCClient): void {
    const index = this.isolatedClients.indexOf(client);
    if (index >= 0) this.isolatedClients.splice(index, 1);
  }

  /**
   * Create proxy for type-safe RPC calls
   * @param namespace - The namespace for RPC calls
   * @param options - Optional configuration. `noResponderRetry` and `timeout`
   * apply to every plain call of this proxy and its nested proxies.
   */
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
      // Create an isolated proxy with its own connection
      client = this.createIsolatedClient({
        ...this.options,
        name: `${this.options.name}-proxy-${namespace}`,
      });
      this.isolatedClients.push(client);
    } else {
      // Use the current client instance
      // eslint-disable-next-line @typescript-eslint/no-this-alias
      client = this;
    }

    const proxy = createProxy<T>(client, namespace, [], undefined, options?.onTiming, { noResponderRetry: options?.noResponderRetry, timeout: options?.timeout });

    if (options?.isolatedConnection) {
      // Store reference for potential cleanup
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

  /**
   * Create a service client proxy with automatic service discovery
   */
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
      // Create a new isolated client for this service proxy
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

    // Discover available services
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

    // Select service (prefer specific ID if provided)
    const selected = options?.preferredId ? (services.find((s) => s.id === options.preferredId) ?? services[0]) : services[0];

    // Create the proxy
    const proxy = createServiceProxy<T>(client, selected, options?.timeout);

    // If isolated, store reference for potential cleanup
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

// Wraps a connect() promise so that an external AbortSignal can synchronously
// reject the wait. If the underlying connect resolves AFTER abort, the
// resulting NatsConnection is force-closed via the fork's abortClose() (or
// the standard close() as a fallback) — otherwise we'd leak a live WS that
// nobody owns.
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

// bytes of the message on the wire, before any chunking
function encodedSize(message: unknown): number {
  const parts = encodeMessageParts(message);
  return parts instanceof Uint8Array ? parts.length : parts.size;
}

// a new header set with the caller's headers, which the caller may send again unchanged
function copyHeaders(source: MsgHdrs | undefined): MsgHdrs {
  const hdrs = headers();
  for (const key of source?.keys() ?? []) {
    for (const value of source!.values(key)) hdrs.append(key, value);
  }
  return hdrs;
}

// the smaller of the server's max_payload and the configured cap, 1 MiB without either
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
      void nc.close().catch(() => {});
    }
  } catch {
    // ignore
  }
}

// drain() sends the UNSUB and cancels after a PING round trip, messages that arrive before the PONG are delivered
function drainSubscription(sub: Subscription | undefined): Promise<void> {
  if (!sub || sub.isClosed()) return Promise.resolve();
  if (typeof sub.drain !== 'function') {
    sub.unsubscribe();
    return Promise.resolve();
  }
  return sub.drain().catch(() => {
    // closed connection or subscription, nothing more arrives
  });
}

// marked: a handler that passes it on reports 'upstream unavailable' on the wire
function noRespondersError(subject: string): InstanceType<typeof errors.NoRespondersError> {
  return markLocalNoResponders(new errors.NoRespondersError(subject), subject);
}

function forbiddenError(subject: string): RPCException {
  return createError(ERROR_CODES.FORBIDDEN, `Permission denied for "${subject}"`, { subject });
}

// by name: the browser build may hand over the error class of another nats.js copy
function refusedPublishSubject(error: unknown): string | undefined {
  const refused = error as { name?: string; operation?: string; subject?: string } | undefined;
  if (refused?.name !== 'PermissionViolationError' || refused.operation !== 'publish' || !refused.subject) return undefined;
  return refused.subject;
}

// rpc.reply.> and rpc.cb.> carry the replies and callbacks of every call on the server
function namespaceSubscribable(namespace: string): boolean {
  const first = namespace.split('.', 1)[0];
  return first !== 'reply' && first !== 'cb';
}

// a registerHandler request; anything else bypasses the drain bookkeeping
function isRequest(msg: unknown): msg is RPCMessage {
  return typeof (msg as RPCMessage | null | undefined)?.id === 'string';
}

// await semantics: any object or function with a then method is adopted
function isThenable(value: unknown): value is PromiseLike<unknown> {
  return value !== null && (typeof value === 'object' || typeof value === 'function') && typeof (value as { then?: unknown }).then === 'function';
}
