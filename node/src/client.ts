import { connect, createInbox, errors, headers } from '@nats-io/transport-node';

import { Channel, PrivateChannel } from './channel.js';
import { ChunkingManager, createChunks } from './chunking.js';
import { decode, decodeMessage, encode, encodeMessage } from './codec.js';
import { extractNestedMethodsWithDecorators, extractNestedMethodsWithoutDecorators } from './decorators.js';
import { RPCException, createError } from './errors.js';
import { formatErrorObject, handleCallbackRequest, handleNormalRPC, handlePullCallbackRequest, handlePullIteratorRequest, handleStreamRequest } from './handler.js';
import { RPCService } from './service.js';
import { ERROR_CODES } from './types.js';
import { createProxy, createSerialDispatcher, createServiceProxy, generateId, generateReplyPrefix, sleep } from './utils.js';

import type { Msg, MsgHdrs, NatsConnection, Status, Subscription } from '@nats-io/nats-core';
import type { ServiceInfo } from '@nats-io/services';
import type { NodeConnectionOptions } from '@nats-io/transport-node';
import type {
  CallbackInvocation,
  CallbackMessage,
  CallbackParams,
  ChunkedTransferHeader,
  Promisify,
  PullCallbackCallOptions,
  PullCallbackParams,
  PullIteratorRequest,
  PullIteratorResponse,
  RPCAuthOptions,
  RPCClient as RPCClientImpl,
  RPCClientOptions,
  RPCMessage,
  RPCResponse,
  StreamMessage,
} from './types.js';

export function createRPCClient(options: RPCClientOptions): RPCClientImpl {
  return new RPCClient(options);
}

interface SubscriptionEntry {
  pattern: string;
  handler: (data: any) => void | Promise<void>;
  options?: { queue?: string };
  /** Raw mode: the handler receives the undecoded NATS Msg (with headers and
   * subject) instead of decoded payload data. No chunk reassembly, no serial
   * dispatcher — used by the muxed reply inbox which routes chunks itself. */
  raw?: boolean;
  sub?: Subscription;
  /** Awaits the subscription's handler chain as of call time — i.e. every
   * message dispatched so far has had its handler executed. */
  flush?: () => Promise<void>;
}

function scopedInbox(connId?: string): string {
  return createInbox(connId ? `_INBOX.${connId}` : undefined);
}

export class RPCClient implements RPCClientImpl {
  public readonly service = new RPCService(this);
  public chunkingManager = new ChunkingManager();
  private pullIteratorCleanups = new Map<string, () => Promise<void>>();
  private callbackCleanups = new Map<string, () => Promise<void>>();

  private nc?: NatsConnection;
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

  /** 503/no-responder handlers keyed by reply-subject suffix (the part after
   * `rpc.reply.`). Used by pull-iterator/stream paths whose per-message
   * `reply` subject only ever carries no-responder statuses. One-shot:
   * the mux dispatcher removes an entry when it fires (max:1 semantics). */
  private statusHandlers = new Map<string, (err: Error) => void>();
  private _maxPayloadSize: number = 1024 * 1024; // Default 1MB
  private connectionPromise?: Promise<NatsConnection>;
  private closed = false;

  private isolatedClients: RPCClient[] = [];

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
    }
  >();

  private streamHandlers = new Map<
    string,
    {
      push: (value: any) => void;
      end: () => void;
      error: (error: any) => void;
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
    // With a connId the prefix MUST be exactly the connId — the server-side
    // firewall allowlists `rpc.reply.<connId>.>` for browser clients.
    this.replyPrefix = options.connId ?? generateReplyPrefix();
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
    this.connectionPromise ??= connect(natsOptions);

    const innerPromise = this.connectionPromise;

    try {
      this.nc = await makeAbortableConnect(innerPromise, options?.signal);
    } finally {
      this.connectionPromise = undefined;
    }

    // A client that was disconnect()ed or abortClose()d is revivable by an
    // explicit connect(). Without this reset, auto-connect in _callOnce and
    // the no-responder retry loop stay permanently disabled.
    this.closed = false;

    this.service.init(this.nc);

    // Get max_payload from server info
    try {
      const serverInfo = this.nc.info;
      this._maxPayloadSize = serverInfo?.max_payload ?? this.options.maxPayloadSize ?? 1024 * 1024;
    } catch {
      this._maxPayloadSize = this.options.maxPayloadSize ?? 1024 * 1024;
    }

    // Reserve 8KB for NATS protocol overhead and MsgPack envelope per message
    this._maxPayloadSize = this._maxPayloadSize - 8192;

    // Register the muxed reply inbox (idempotent). Registered as a normal
    // subscription entry so the restore loop below (re-)subscribes it on
    // first connect and after every suspend cycle alike.
    this.ensureMuxSubscription(false);

    // Restore subscriptions after reconnect (from suspend). Entries keep
    // their identity so unsubscribe closures held by callers stay valid.
    for (const entry of this.subscriptionEntries.values()) {
      if (!entry.sub || entry.sub.isClosed()) {
        this.natsSubscribe(entry);
      }
    }

    return this.nc;
  }

  /**
   * Disconnect from NATS server
   * Cleans up resources without rejecting pending operations
   */
  public async disconnect(): Promise<void> {
    this.closed = true;

    // Cleanup pending requests
    for (const [, pending] of this.pendingRequests) {
      if (pending.timeout) {
        clearTimeout(pending.timeout);
      }
      pending.cleanup?.();
      pending.reject(createError(ERROR_CODES.CONNECTION_CLOSED, 'Connection closed'));
    }
    this.pendingRequests.clear();
    this.statusHandlers.clear();

    // Cleanup stream handlers
    for (const [, handler] of this.streamHandlers) {
      try {
        handler.end();
      } catch {
        // Ignore errors during cleanup
      }
    }
    this.streamHandlers.clear();

    this.settlePullIterators();

    await Promise.allSettled(Array.from(this.pullIteratorCleanups.values()).map((cleanup) => cleanup()));
    this.pullIteratorCleanups.clear();

    // Cleanup callbacks
    await Promise.allSettled(Array.from(this.callbackCleanups.values()).map((cleanup) => cleanup()));
    this.callbackCleanups.clear();

    // Unsubscribe all subscriptions
    for (const entry of this.subscriptionEntries.values()) {
      try {
        entry.sub?.unsubscribe();
      } catch {
        // Ignore errors during cleanup
      }
    }
    this.subscriptionEntries.clear();
    // Entries were dropped — a revive-connect() must re-register the mux.
    this.muxEntry = undefined;

    // Clear chunking manager
    this.chunkingManager = new ChunkingManager();

    // Disconnect isolated clients
    await Promise.allSettled(this.isolatedClients.map((client) => client.disconnect()));
    this.isolatedClients = [];

    // Drain and close connection
    if (this.nc) {
      try {
        await this.nc.close();
      } catch {
        // Ignore errors
      }

      // Ensure the connection is truly closed. With waitOnFirstConnect: false the
      // underlying WebSocket transport may still be mid-handshake when close() is
      // called. nc.closed() resolves once the NATS protocol is fully shut down,
      // preventing zombie connections that survive a disconnect/reconnect cycle.
      try {
        const timeout = this.options.disconnectTimeout ?? 2_000;
        await Promise.race([this.nc.closed(), new Promise<void>((r) => setTimeout(r, timeout))]);
      } catch {
        // Ignore errors
      }

      this.nc = undefined;
    }
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
    for (const [, pending] of this.pendingRequests) {
      if (pending.timeout) clearTimeout(pending.timeout);
      pending.cleanup?.();
      pending.reject(createError(ERROR_CODES.CONNECTION_CLOSED, 'Connection closed'));
    }
    this.pendingRequests.clear();
    this.statusHandlers.clear();

    for (const [, handler] of this.streamHandlers) {
      try {
        handler.end();
      } catch {
        // ignore
      }
    }
    this.streamHandlers.clear();

    this.settlePullIterators();

    for (const cleanup of this.pullIteratorCleanups.values()) {
      try {
        cleanup();
      } catch {
        // ignore
      }
    }
    this.pullIteratorCleanups.clear();

    for (const cleanup of this.callbackCleanups.values()) {
      try {
        cleanup();
      } catch {
        // ignore
      }
    }
    this.callbackCleanups.clear();

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
   * After calling suspend(), connect() will restore previous subscriptions.
   */
  public async suspend(): Promise<void> {
    // Cleanup pending requests. RPC calls are muxed (no per-call
    // subscription); service-path calls still hold a per-call reply entry
    // which pending.cleanup drops — otherwise a suspended in-flight call
    // would be restored as a dead reply subscription on the next connect().
    for (const [, pending] of this.pendingRequests) {
      if (pending.timeout) {
        clearTimeout(pending.timeout);
      }
      pending.cleanup?.();
      pending.reject(createError(ERROR_CODES.CONNECTION_CLOSED, 'Connection closed'));
    }
    this.pendingRequests.clear();
    this.statusHandlers.clear();

    // Cleanup stream handlers
    for (const [, handler] of this.streamHandlers) {
      try {
        handler.end();
      } catch {
        // Ignore errors during cleanup
      }
    }
    this.streamHandlers.clear();

    this.settlePullIterators();

    await Promise.allSettled(Array.from(this.pullIteratorCleanups.values()).map((cleanup) => cleanup()));
    this.pullIteratorCleanups.clear();

    // Cleanup callbacks
    await Promise.allSettled(Array.from(this.callbackCleanups.values()).map((cleanup) => cleanup()));
    this.callbackCleanups.clear();

    // Unsubscribe all subscriptions but keep the entries — connect() restores
    // them on the fresh transport.
    for (const entry of this.subscriptionEntries.values()) {
      try {
        entry.sub?.unsubscribe();
      } catch {
        // Ignore errors during cleanup
      }
      entry.sub = undefined;
    }

    // Clear chunking manager
    this.chunkingManager = new ChunkingManager();

    // Suspend isolated clients
    await Promise.allSettled(this.isolatedClients.map((client) => client.suspend()));

    // Drain and close connection
    if (this.nc) {
      try {
        await this.nc.close();
      } catch {
        // Ignore errors
      }

      try {
        const timeout = this.options.disconnectTimeout ?? 2_000;
        await Promise.race([this.nc.closed(), new Promise<void>((r) => setTimeout(r, timeout))]);
      } catch {
        // Ignore errors
      }

      this.nc = undefined;
    }

    this.connectionPromise = undefined;
  }

  /**
   * Public publish method
   */
  public async publish<TMessage = any>(subject: string, data: TMessage, opts?: { headers?: MsgHdrs; reply?: string }): Promise<void> {
    if (!this.nc) {
      throw new Error('Not connected');
    }

    const encoded = encodeMessage(data);

    // Small enough to send directly
    if (encoded.length <= this._maxPayloadSize) {
      this.nc.publish(subject, encoded, opts);
      return;
    }

    // Message is too large, chunk it
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

    // Header message includes original headers if any
    const hdrs = opts?.headers ?? headers();
    hdrs.set('x-chunked-transfer', 'header');
    hdrs.set('x-chunk-id', transferId);

    this.nc.publish(subject, encode(headerMsg), { headers: hdrs, reply: opts?.reply });

    // Send chunks directly from generator (no Array.from() - saves memory)
    let chunkIndex = 0;
    for (const chunk of createChunks(encoded, transferId, this._maxPayloadSize)) {
      const chunkHdrs = headers();
      chunkHdrs.set('x-chunked-transfer', 'chunk');
      chunkHdrs.set('x-chunk-id', transferId);
      chunkHdrs.set('x-chunk-index', chunkIndex.toString());

      // Send raw chunk data (not encoded)
      this.nc.publish(subject, chunk.data, { headers: chunkHdrs, reply: opts?.reply });

      // Yield every 50 chunks to prevent blocking
      if (chunkIndex > 0 && chunkIndex % 50 === 0) {
        await sleep(0);
      }
      chunkIndex++;
    }
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

    const encoded = encodeMessage(data);
    if (encoded.length > this._maxPayloadSize) {
      return false;
    }

    this.nc.publish(subject, encoded, opts);
    return true;
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
  ): Promise<{ entry: SubscriptionEntry; unsubscribe: () => void }> {
    if (!this.nc) {
      throw new Error('Not connected');
    }

    const key = ++this.subscriptionSeq;
    const entry: SubscriptionEntry = { pattern, handler: handler as (data: any) => void | Promise<void>, options };
    this.subscriptionEntries.set(key, entry);
    this.natsSubscribe(entry);

    const unsubscribe = () => {
      try {
        entry.sub?.unsubscribe();
      } catch {
        // Ignore unsubscribe errors
      } finally {
        this.subscriptionEntries.delete(key);
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
    const dispatcher = createSerialDispatcher(entry.handler, (error) => console.error(`Error in handler for ${pattern}:`, error));
    const runHandler = dispatcher.dispatch;

    // Expose a flush hook: awaiting it settles the chain as of call time,
    // i.e. every message dispatched so far has had its handler executed.
    entry.flush = dispatcher.flush;

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

          if (!chunkId || data.transferId !== chunkId) {
            console.error('Invalid chunk header');
            return;
          }

          // Setup chunk assembly with pre-allocated buffer (optimized)
          this.chunkingManager.startReceiving(
            data.transferId,
            data.totalChunks,
            (assembledData) => {
              runHandler(assembledData);
            },
            (error) => {
              console.error(`Error assembling chunks for ${pattern}:`, error);
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
          this.chunkingManager.processChunk({
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
          runHandler(data);
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
   * - chunked transfer header/chunk: reassemble via chunkingManager, then
   *   route the assembled response by envelope id.
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
        pending.reject(new errors.NoRespondersError(pending.subject ?? suffix));
      }
      return;
    }

    const chunkType = msg.headers?.get('x-chunked-transfer');

    if (chunkType === 'header') {
      const data = decode(msg.data);
      const chunkId = msg.headers?.get('x-chunk-id');
      if (!chunkId || data.transferId !== chunkId) {
        console.error('Invalid chunk header on reply mux');
        return;
      }
      this.chunkingManager.startReceiving(
        data.transferId,
        data.totalChunks,
        (assembledData) => this.routeMuxResponse(assembledData),
        (error) => console.error('Error assembling chunked RPC response:', error),
        data.totalSize,
        data.chunkSize,
      );
    } else if (chunkType === 'chunk') {
      const chunkId = msg.headers?.get('x-chunk-id');
      const chunkIndex = parseInt(msg.headers?.get('x-chunk-index') ?? '0');
      if (!chunkId) {
        console.error('Chunk missing chunk ID on reply mux');
        return;
      }
      this.chunkingManager.processChunk({ id: chunkId, chunkIndex, data: msg.data, isLast: false });
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

    if (response.error) {
      pending.reject(RPCException.fromJSON(response.error));
    } else {
      // __methods (proxy method discovery) travels as a side channel next to
      // the result — the result object itself stays untouched.
      pending.resolve({ result: response.result, methods: response.__methods });
    }
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
   * Native NATS request/reply
   * @param subject - The subject to send the request to
   * @param data - The request data
   * @param options - Request options including timeout and per-call retry override
   */
  public async request<TRequest = any, TResponse = any>(
    subject: string,
    data: TRequest,
    options?: { timeout?: number; headers?: MsgHdrs; noResponderRetry?: { maxRetries?: number; delays?: number[] } },
  ): Promise<TResponse> {
    return this.withNoResponderRetry(() => this._requestOnce<TRequest, TResponse>(subject, data, options), options?.noResponderRetry);
  }

  private async _requestOnce<TRequest = any, TResponse = any>(subject: string, data: TRequest, options?: { timeout?: number; headers?: MsgHdrs }): Promise<TResponse> {
    if (!this.nc) {
      throw new Error('Not connected');
    }

    const timeout = options?.timeout ?? 5000;
    const encoded = encodeMessage(data);

    try {
      // Use native NATS request
      const msg = await this.nc.request(subject, encoded, {
        timeout,
        headers: options?.headers,
        noMux: false, // Allow request multiplexing
      });

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
    } catch (error: any) {
      if (error.code === '503' || error.message?.includes('no responders')) {
        throw createError(ERROR_CODES.NOT_FOUND, 'No responders available');
      }
      if (error.code === 'TIMEOUT' || error.message?.includes('timeout')) {
        throw createError(ERROR_CODES.TIMEOUT, `Request to "${subject}" timed out after ${timeout}ms`);
      }
      throw error;
    }
  }

  /**
   * Retry helper for 503 / no-responder errors. The per-call `override`
   * lets a request that targets a known-flaky responder (e.g. a child
   * process that may be restarting) extend the wait window without
   * affecting the client-wide default.
   */
  private async withNoResponderRetry<T>(fn: () => Promise<T>, override?: { maxRetries?: number; delays?: number[] }): Promise<T> {
    const maxRetries = override?.maxRetries ?? this.options.noResponderRetry?.maxRetries ?? 3;
    const delays = override?.delays ?? this.options.noResponderRetry?.delays ?? [500, 1000, 2000];

    for (let attempt = 0; ; attempt++) {
      try {
        return await fn();
      } catch (err) {
        const isNoResponder = err instanceof errors.NoRespondersError || (err instanceof RPCException && err.code === ERROR_CODES.NOT_FOUND);

        if (!isNoResponder || attempt >= maxRetries || this.closed) {
          throw err;
        }

        const delay = delays[Math.min(attempt, delays.length - 1)];
        await sleep(delay);
      }
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
   * Internal: not part of the public RPCClient interface.
   */
  public async callWithMeta<TResponse = any>(subject: string, args: any[] = [], opts?: { discover?: boolean }): Promise<{ result: TResponse; methods?: string[] }> {
    return this.withNoResponderRetry(() => this._callOnce<TResponse>(subject, args, opts?.discover === true));
  }

  /**
   * Make an RPC call (single attempt)
   */
  private async _callOnce<TResponse = any>(subject: string, args: any[], discover = false): Promise<{ result: TResponse; methods?: string[] }> {
    if (!this.isConnected && !this.isClosed) {
      await this.connect();
    }

    if (!this.nc) {
      throw new Error('Not connected');
    }

    // Service calls (`<subject>.reply.<id>`) keep the legacy per-call
    // subscription flow — separate refactor later.
    if (!subject.startsWith('rpc.')) {
      return this._callOnceService<TResponse>(subject, args);
    }

    // Normally established by connect(); covers clients whose connection was
    // wired up out-of-band (tests). No-op when already subscribed.
    this.ensureMuxSubscription();

    const id = generateId(this.replyPrefix);
    const timeout = this.options.timeout ?? 30000;
    // The reply subject is derived from the id by pure string concatenation —
    // this is the wire contract with every responder implementation (Node,
    // Go, Python): they publish the response to `rpc.reply.${msg.id}` and
    // treat the id as opaque. Because the id starts with our replyPrefix,
    // the muxed reply inbox (`rpc.reply.<replyPrefix>.>`) catches it.
    const replySubject = `rpc.reply.${id}`;

    return new Promise<{ result: TResponse; methods?: string[] }>((resolve, reject) => {
      // No per-call subscriptions: responses (plain or chunked) and
      // no-responder statuses all arrive on the muxed reply inbox, which
      // routes them back here via pendingRequests. cleanup only has to drop
      // the map entry and the timer.
      const timeoutHandle = setTimeout(() => {
        if (this.pendingRequests.delete(id)) {
          reject(createError(ERROR_CODES.TIMEOUT, `RPC call to "${subject}" timed out after ${timeout}ms`));
        }
      }, timeout);

      this.pendingRequests.set(id, {
        resolve,
        reject,
        timeout: timeoutHandle,
        subject,
        cleanup: () => {
          clearTimeout(timeoutHandle);
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
      this.publish(subject, message, { reply: replySubject }).catch((error) => {
        if (this.pendingRequests.delete(id)) {
          clearTimeout(timeoutHandle);
          reject(error);
        }
      });
    });
  }

  /**
   * Legacy single-attempt call for service subjects (reply pattern
   * `<subject>.reply.<id>`): per-call reply subscription + one-shot
   * no-responder inbox. The rpc.* path is muxed — see _callOnce.
   */
  private async _callOnceService<TResponse = any>(subject: string, args: any[]): Promise<{ result: TResponse; methods?: string[] }> {
    const id = generateId(this.replyPrefix);
    const timeout = this.options.timeout ?? 30000;
    const replySubject = `${subject}.reply.${id}`;

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
          void unsubscribeAll();
          reject(createError(ERROR_CODES.TIMEOUT, `RPC call to "${subject}" timed out after ${timeout}ms`));
        }
      }, timeout);

      // Store pending request
      this.pendingRequests.set(id, {
        resolve,
        reject,
        timeout: timeoutHandle,
        cleanup: () => void unsubscribeAll(),
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
              pending.resolve({ result: response.result, methods: response.__methods });
            }
          }
        }
      };

      const requestCallback = (err: Error | null, msg: Msg) => {
        // Check for no responders status (empty message with 503 status)
        if (msg && msg.data?.length === 0 && msg.headers?.code === 503) {
          reject(new errors.NoRespondersError(subject));
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

        // Send request
        const message: RPCMessage = { id, method: 'call', params: args };
        await this.publish(subject, message, { reply: inbox });
      } catch (error) {
        await unsubscribeAll();
        reject(error);
      }
    });
  }

  /**
   * Make a streaming RPC call
   */
  public async *callStream<TResponse = any>(subject: string, ...args: any[]): AsyncGenerator<TResponse> {
    const maxRetries = this.options.noResponderRetry?.maxRetries ?? 3;
    const delays = this.options.noResponderRetry?.delays ?? [500, 1000, 2000];

    for (let attempt = 0; ; attempt++) {
      try {
        yield* this._callStreamOnce<TResponse>(subject, ...args);
        return;
      } catch (err) {
        const isNoResponder = err instanceof errors.NoRespondersError || (err instanceof RPCException && err.code === ERROR_CODES.NOT_FOUND);

        if (!isNoResponder || attempt >= maxRetries || this.closed) {
          throw err;
        }

        await sleep(delays[Math.min(attempt, delays.length - 1)]);
      }
    }
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
    };

    const cleanupOnce = async (): Promise<void> => {
      if (cleanedUp) return;
      cleanedUp = true;
      client.streamHandlers.delete(id);
      client.statusHandlers.delete(id);
      unsubscribe?.();
      if (!ended) {
        ended = true;
        try {
          await client.publish(`${streamSubject}.cancel`, { id });
        } catch {
          // ignore
        }
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
      if (!client.nc) throw new Error('Not connected');
      client.ensureMuxSubscription();

      id = generateId(client.replyPrefix);
      streamSubject = `stream.${subject}.${id}`;
      client.streamHandlers.set(id, handler);

      unsubscribe = await client.subscribe(streamSubject, handleStreamMessage);

      // No-responder detection via the muxed reply inbox: the request's
      // reply subject `rpc.reply.<id>` only ever carries a 503 status —
      // stream responders never publish a direct RPC response.
      client.statusHandlers.set(id, () => {
        const h = client.streamHandlers.get(id);
        h?.error(new errors.NoRespondersError(subject));
        void cleanupOnce();
      });

      const streamParams = { __stream: true, __streamSubject: streamSubject, args };
      const message: RPCMessage = { id, method: 'stream', params: streamParams };
      await client.publish(subject, message, { reply: `rpc.reply.${id}` });
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
    let returned = false;
    let cleanedUp = false;
    let ended = false;
    let error: any = null;

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
        error = createError('503', e.message);
        const response: PullIteratorResponse<any> = {
          type: 'error',
          id: iteratorId,
          error: error.toJSON(),
        };
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

        const nextRequest: PullIteratorRequest = { id: iteratorId, type: 'next' };
        try {
          await client.publish(requestSubject, nextRequest, { reply: statusInbox });
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
          throw RPCException.fromJSON(response.error!);
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
   * Pull-iterator-with-callbacks call.
   *
   * Combines a client-driven pull iterator (1 RTT per batch) with a oneway
   * callback channel (fire-and-forget server → client) for low-latency
   * frame-level data delivery with coarse-grained backpressure.
   *
   * The returned async generator yields `undefined` for each batch boundary
   * the server produces. Meaningful data is dispatched through the provided
   * callback object.
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
    let returned = false;
    let cleanedUp = false;
    let ended = false;
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
        const fn = callbacks[msg.method];
        if (!fn) {
          console.error(`[rpc] Pull-callback: unknown method '${msg.method}'`);
          return;
        }
        callbackChain = callbackChain.then(async () => {
          try {
            await fn(...(msg.args ?? []));
          } catch (err) {
            console.error(`[rpc] Pull-callback handler '${msg.method}' threw:`, err);
          }
        });
      });
      callbackUnsub = cbSubscription.unsubscribe;
      callbackFlush = () => cbSubscription.entry.flush?.() ?? Promise.resolve();

      const initParams: PullCallbackParams = {
        __pullCallback: true,
        __iteratorId: iteratorId,
        __callbackSubject: callbackSubject,
        __callbackMethods: callbackMethods,
        __onewayMethods: onewayMethodList,
        args,
      };

      let initResponse: any;
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
        error = createError('503', e.message);

        const response: PullIteratorResponse<any> = {
          type: 'error',
          id: iteratorId,
          error: error.toJSON(),
        };

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

        if (prefetched) {
          // The request for this batch was already sent by the prefetch
          // path right after the previous boundary arrived.
          prefetched = false;
        } else {
          const nextRequest: PullIteratorRequest = { id: iteratorId, type: 'next' };
          try {
            await client.publish(requestSubject, nextRequest, { reply: statusInbox });
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
          throw RPCException.fromJSON(response.error!);
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
        if (prefetch && !ended && !returned) {
          try {
            const prefetchRequest: PullIteratorRequest = { id: iteratorId, type: 'next' };
            await client.publish(requestSubject, prefetchRequest, { reply: statusInbox });
            prefetched = true;
          } catch {
            prefetched = false;
          }
        }
        // 'value' — wait for all callback handlers queued for the batch
        // to finish. A slow handler stalls here → stalls next()
        // request → server parks at its own yield. End-to-end backpressure.
        //
        // Flush the callback subscription's dispatch chain first: every
        // callback message of this batch arrived before the boundary
        // response (same connection, publish order), so it is already
        // queued in the subscription chain — but may not have been
        // appended to callbackChain yet, because each subscription
        // serializes its handlers on its own chain. Without the flush the
        // last callbacks of a batch can lose the microtask race against
        // the boundary and leak past next() (mirrors Go's
        // drainCallbacks() before yield).
        await callbackFlush?.();
        await callbackChain;
        return { value: undefined, done: false };
      },

      async return(value?: void): Promise<IteratorResult<void, void>> {
        if (returned) return { value: value!, done: true };
        returned = true;
        settlePendingAsDone();
        if (started) await cleanupOnce();
        return { value: value!, done: true };
      },

      async throw(err: unknown): Promise<IteratorResult<void, void>> {
        if (returned) throw err;
        returned = true;
        settlePendingAsDone();
        if (started) await cleanupOnce();
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
   */
  public async registerHandler(
    namespace: string,
    handlers: object,
    options?: { isolatedConnection?: boolean; withoutDecorators?: boolean; queue?: string },
  ): Promise<() => Promise<void>> {
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
    const pullIteratorIds: string[] = [];
    const callbackIds: string[] = [];

    // Extract methods based on option
    const handlersMap = options?.withoutDecorators ? extractNestedMethodsWithoutDecorators(handlers) : extractNestedMethodsWithDecorators(handlers);
    const methodNames = Object.keys(handlersMap);

    for (const [method, handler] of Object.entries(handlersMap)) {
      const subject = `rpc.${namespace}.${method}`;

      const unsubscribe = await client.subscribe(
        subject,
        async (msg: RPCMessage) => {
          const response: RPCResponse = { id: msg.id };

          // Method discovery on demand: only a request whose envelope carries
          // __discover (a proxy with an empty method cache) pays for the
          // namespace's method list — attaching it to every response would be
          // dead wire weight once the proxy cache is filled. Old clients
          // never send __discover and never read __methods on this path.
          if (msg.__discover === true) {
            response.__methods = methodNames;
          }

          try {
            // Handle stream request
            if (msg.params?.__stream && msg.params?.__streamSubject) {
              const streamSubject = msg.params.__streamSubject;
              const args = msg.params.args ?? [];

              // Don't await stream requests - they run in background and send data via streamSubject
              // Awaiting would block the subscription handler and prevent processing of new messages
              handleStreamRequest(handler, args, streamSubject, msg.id, client).catch((err) => {
                console.error(`Stream request error for ${method}:`, err);
              });
              return; // Don't send RPC response for stream requests
            } else if (
              // Check if it's a pull iterator request
              // Could be direct object or wrapped in array from call()
              msg.params?.__pullIterator ||
              (Array.isArray(msg.params) && msg.params[0]?.__pullIterator)
            ) {
              // Extract pull iterator params
              const pullParams = msg.params?.__pullIterator ? msg.params : msg.params[0];
              const args = pullParams.args ?? [];
              const iteratorId = pullParams.__iteratorId ?? msg.id;
              const cleanup = await handlePullIteratorRequest(handler, args, iteratorId, client, () => client.pullIteratorCleanups.delete(iteratorId));

              // Store cleanup function for later
              client.pullIteratorCleanups.set(iteratorId, cleanup);
              pullIteratorIds.push(iteratorId);
              response.result = { iteratorId };

              // Send response with iterator ID
              const replySubject = `rpc.reply.${msg.id}`;
              await client.publish(replySubject, response);
            } else if (
              // Check if it's a pull-iterator-with-callbacks request
              msg.params?.__pullCallback ||
              (Array.isArray(msg.params) && msg.params[0]?.__pullCallback)
            ) {
              const pcParams = msg.params?.__pullCallback ? msg.params : msg.params[0];
              const args = pcParams.args ?? [];
              const iteratorId = pcParams.__iteratorId ?? msg.id;
              const callbackSubject = pcParams.__callbackSubject;
              const callbackMethods: string[] = pcParams.__callbackMethods ?? [];
              const onewayMethods: string[] = pcParams.__onewayMethods ?? [];

              const cleanup = await handlePullCallbackRequest(handler, args, iteratorId, callbackSubject, callbackMethods, onewayMethods, client, () =>
                client.pullIteratorCleanups.delete(iteratorId),
              );

              client.pullIteratorCleanups.set(iteratorId, cleanup);
              pullIteratorIds.push(iteratorId);
              response.result = { iteratorId };

              const replySubject = `rpc.reply.${msg.id}`;
              await client.publish(replySubject, response);
            } else if (
              // Check if it's a callback subscription request
              (msg.params && typeof msg.params === 'object' && !Array.isArray(msg.params) && msg.params.__callback && msg.params.__callbackSubject) ||
              (Array.isArray(msg.params) && msg.params.length > 0 && typeof msg.params[0] === 'object' && msg.params[0]?.__callback)
            ) {
              // Handle callback subscription request
              const cbParams = Array.isArray(msg.params) && msg.params[0]?.__callback ? msg.params[0] : msg.params;
              const callbackSubject = cbParams.__callbackSubject;
              const cbArgs = cbParams.args ?? [];

              const cleanup = await handleCallbackRequest(handler, cbArgs, callbackSubject, msg.id, client, () => client.callbackCleanups.delete(msg.id));
              client.callbackCleanups.set(msg.id, cleanup);
              callbackIds.push(msg.id);

              response.result = { ok: true };
              const replySubject = `rpc.reply.${msg.id}`;
              await client.publish(replySubject, response);
            } else {
              // Normal RPC call
              const result = await handleNormalRPC(handler, msg.params);
              response.result = result;

              // Send response
              const replySubject = `rpc.reply.${msg.id}`;
              await client.publish(replySubject, response);
            }
          } catch (error) {
            response.error = formatErrorObject(error);

            // Diagnostic aid: a METHOD_NOT_FOUND error always carries the
            // method list, discovery requested or not (rare, small).
            if (response.error.code === ERROR_CODES.METHOD_NOT_FOUND) {
              response.__methods = methodNames;
            }

            try {
              const replySubject = `rpc.reply.${msg.id}`;
              await client.publish(replySubject, response);
            } catch (publishError) {
              if (client.isClosed) {
                return; // Ignore publish errors if client is closed
              }

              console.error('Failed to send error response:', publishError);
            }
          }
        },
        options?.queue ? { queue: options.queue } : undefined,
      );

      unsubscribers.push(unsubscribe);
    }

    const cleanup = async () => {
      // Unsubscribe all handlers
      for (const unsub of unsubscribers) {
        unsub();
      }

      // Cleanup pull iterators — only the ones this namespace created. The
      // client maps are shared across registerHandler calls; sweeping them
      // wholesale would kill the live sessions of every other namespace.
      await Promise.allSettled(
        pullIteratorIds.map((id) => {
          const fn = client.pullIteratorCleanups.get(id);
          client.pullIteratorCleanups.delete(id);
          return fn ? fn() : Promise.resolve();
        }),
      );

      // Cleanup callbacks
      await Promise.allSettled(
        callbackIds.map((id) => {
          const fn = client.callbackCleanups.get(id);
          client.callbackCleanups.delete(id);
          return fn ? fn() : Promise.resolve();
        }),
      );

      // Disconnect isolated connection if used
      if (options?.isolatedConnection) {
        await client.disconnect();
        const index = this.isolatedClients.indexOf(client);
        if (index >= 0) {
          this.isolatedClients.splice(index, 1);
        }
      }
    };

    return cleanup;
  }

  /**
   * Setup a request handler (responder)
   * @param pattern - The subject pattern to listen for requests
   * @param handler - The handler function that receives data and subject
   */
  public async onRequest<TRequest = any, TResponse = any>(pattern: string, handler: (data: TRequest) => Promise<TResponse> | TResponse): Promise<() => void> {
    if (!this.nc) {
      throw new Error('Not connected');
    }

    const sub = this.nc.subscribe(pattern, {
      callback: (_err, msg) => {
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
              const errorResponse = encodeMessage({
                error: error instanceof Error ? error.message : 'Internal error',
                code: error instanceof RPCException ? error.code : ERROR_CODES.INTERNAL_ERROR,
              });
              msg.respond(errorResponse);
            }
            console.error(`Error in request handler for ${pattern}:`, error);
          }
        })();
      },
    });

    // Return unsubscribe function
    return () => {
      sub.unsubscribe();
    };
  }

  /**
   * Create or join a bidirectional channel
   * @param channelId - Unique channel identifier
   * @param options - Optional configuration
   */
  public async channel(channelId: string, options?: { isolatedConnection?: boolean }): Promise<Channel> {
    let client: RPCClient;

    if (options?.isolatedConnection) {
      // Create a new isolated client for this channel
      client = this.createIsolatedClient({
        ...this.options,
        name: `${this.options.name}-channel-${channelId}`,
      });
      this.isolatedClients.push(client);
      await client.connect();
    } else {
      // eslint-disable-next-line @typescript-eslint/no-this-alias
      client = this;
    }

    const channel = new Channel(client, channelId);
    await channel.init();

    // Store reference for cleanup if isolated
    if (options?.isolatedConnection) {
      (channel as any)._isolatedClient = client;
    }

    return channel;
  }

  /**
   * Create a private 1:1 channel
   * @param channelId - Unique channel identifier
   * @param targetClientId - Target client to connect to
   * @param options - Optional configuration
   */
  public async privateChannel(channelId: string, targetClientId: string, options?: { isolatedConnection?: boolean }): Promise<PrivateChannel> {
    let client: RPCClient;

    if (options?.isolatedConnection) {
      // Create a new isolated client for this channel
      client = this.createIsolatedClient({
        ...this.options,
        name: `${this.options.name}-private-${channelId}`,
      });
      this.isolatedClients.push(client);
      await client.connect();
    } else {
      // eslint-disable-next-line @typescript-eslint/no-this-alias
      client = this;
    }

    const channel = new PrivateChannel(client, channelId, targetClientId);
    await channel.init();

    // Store reference for cleanup if isolated
    if (options?.isolatedConnection) {
      (channel as any)._isolatedClient = client;
    }

    return channel;
  }

  /**
   * Create proxy for type-safe RPC calls
   * @param namespace - The namespace for RPC calls
   * @param options - Optional configuration
   */
  public createProxy<T extends object>(namespace: string): Promisify<T>;
  public createProxy<T extends object>(namespace: string, options: { isolatedConnection: false }): Promisify<T>;
  public createProxy<T extends object>(
    namespace: string,
    options: { isolatedConnection: true },
  ): {
    proxy: Promisify<T>;
    close: () => Promise<void>;
  };
  // prettier-ignore
  public createProxy<T extends object>(
    namespace: string,
    options?: { isolatedConnection?: boolean },
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

    const proxy = createProxy<T>(client, namespace);

    if (options?.isolatedConnection) {
      // Store reference for potential cleanup
      (proxy as any)._isolatedClient = client;

      return {
        proxy,
        close: async () => {
          await client.disconnect();
          const index = this.isolatedClients.indexOf(client);
          if (index >= 0) this.isolatedClients.splice(index, 1);
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
        const index = this.isolatedClients.indexOf(client);
        if (index >= 0) this.isolatedClients.splice(index, 1);
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
          const index = this.isolatedClients.indexOf(client);
          if (index >= 0) this.isolatedClients.splice(index, 1);
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
