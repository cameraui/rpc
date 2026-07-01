import { connect, createInbox, errors, headers } from '@nats-io/transport-node';

import { Channel, PrivateChannel } from './channel.js';
import { ChunkingManager, createChunks } from './chunking.js';
import { decode, encode } from './codec.js';
import { extractNestedMethodsWithDecorators, extractNestedMethodsWithoutDecorators } from './decorators.js';
import { RPCException, createError } from './errors.js';
import { formatErrorObject, handleCallbackRequest, handleNormalRPC, handlePullCallbackRequest, handlePullIteratorRequest, handleStreamRequest } from './handler.js';
import { RPCService } from './service.js';
import { ERROR_CODES } from './types.js';
import { createProxy, createServiceProxy, generateId, isPromise, sleep } from './utils.js';

import type { Msg, MsgHdrs, NatsConnection, Status, Subscription } from '@nats-io/nats-core';
import type { ServiceInfo } from '@nats-io/services';
import type { NodeConnectionOptions } from '@nats-io/transport-node';
import type {
  CallbackInvocation,
  CallbackMessage,
  CallbackParams,
  ChunkedTransferHeader,
  Promisify,
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

function scopedInbox(connId?: string): string {
  return createInbox(connId ? `_INBOX.${connId}` : undefined);
}

export class RPCClient implements RPCClientImpl {
  public readonly service = new RPCService(this);
  public chunkingManager = new ChunkingManager();
  private pullIteratorCleanups = new Map<string, () => Promise<void>>();
  private callbackCleanups = new Map<string, () => Promise<void>>();

  private nc?: NatsConnection;
  private subscriptions = new Map<string, Subscription[]>();
  private _subscriptionMeta = new Map<string, { pattern: string; handler: (data: any) => void | Promise<void>; options?: { queue?: string } }>();
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

  constructor(public options: RPCClientOptions) {}

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

    // Restore subscriptions after reconnect (from suspend)
    if (this._subscriptionMeta.size > 0) {
      const metas = [...this._subscriptionMeta.values()];
      this._subscriptionMeta.clear();
      for (const meta of metas) {
        await this.subscribe(meta.pattern, meta.handler, meta.options);
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
      pending.reject(createError(ERROR_CODES.CONNECTION_CLOSED, 'Connection closed'));
    }
    this.pendingRequests.clear();

    // Cleanup stream handlers
    for (const [, handler] of this.streamHandlers) {
      try {
        handler.end();
      } catch {
        // Ignore errors during cleanup
      }
    }
    this.streamHandlers.clear();

    await Promise.allSettled(Array.from(this.pullIteratorCleanups.values()).map((cleanup) => cleanup()));
    this.pullIteratorCleanups.clear();

    // Cleanup callbacks
    await Promise.allSettled(Array.from(this.callbackCleanups.values()).map((cleanup) => cleanup()));
    this.callbackCleanups.clear();

    // Unsubscribe all subscriptions
    for (const subs of this.subscriptions.values()) {
      for (const sub of subs) {
        try {
          sub.unsubscribe();
        } catch {
          // Ignore errors during cleanup
        }
      }
    }
    this.subscriptions.clear();
    this._subscriptionMeta.clear();

    // Clear chunking manager
    this.chunkingManager = new ChunkingManager();

    // Disconnect isolated clients
    await Promise.allSettled(this.isolatedClients.map((client) => client.disconnect()));

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
      pending.reject(createError(ERROR_CODES.CONNECTION_CLOSED, 'Connection closed'));
    }
    this.pendingRequests.clear();

    for (const [, handler] of this.streamHandlers) {
      try {
        handler.end();
      } catch {
        // ignore
      }
    }
    this.streamHandlers.clear();

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
    // Cleanup pending requests
    for (const [, pending] of this.pendingRequests) {
      if (pending.timeout) {
        clearTimeout(pending.timeout);
      }
      pending.reject(createError(ERROR_CODES.CONNECTION_CLOSED, 'Connection closed'));
    }
    this.pendingRequests.clear();

    // Cleanup stream handlers
    for (const [, handler] of this.streamHandlers) {
      try {
        handler.end();
      } catch {
        // Ignore errors during cleanup
      }
    }
    this.streamHandlers.clear();

    await Promise.allSettled(Array.from(this.pullIteratorCleanups.values()).map((cleanup) => cleanup()));
    this.pullIteratorCleanups.clear();

    // Cleanup callbacks
    await Promise.allSettled(Array.from(this.callbackCleanups.values()).map((cleanup) => cleanup()));
    this.callbackCleanups.clear();

    // Unsubscribe all subscriptions
    for (const subs of this.subscriptions.values()) {
      for (const sub of subs) {
        try {
          sub.unsubscribe();
        } catch {
          // Ignore errors during cleanup
        }
      }
    }
    this.subscriptions.clear();

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

    const encoded = encode(data);

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
   * Public subscribe method
   */
  public async subscribe<TResponse = any>(pattern: string, handler: (data: TResponse) => void | Promise<void>, options?: { queue?: string }): Promise<() => void> {
    if (!this.nc) {
      throw new Error('Not connected');
    }

    // Serialize async handlers via a per-subscription promise chain. This
    // matches Python's behavior (client.py:434 awaits the handler) and is
    // what backpressure-sensitive callers (e.g. pull-callback iterators)
    // rely on: an awaiting handler blocks the next message from being
    // dispatched, which transitively stalls the producer.
    //
    // Sync handlers bypass the chain and stay fire-and-forget.
    let handlerChain: Promise<void> = Promise.resolve();

    const runHandler = (data: TResponse) => {
      let result: void | Promise<void>;
      try {
        result = handler(data);
      } catch (error) {
        console.error(`Error in handler for ${pattern}:`, error);
        return;
      }
      if (isPromise(result)) {
        const pending = result;
        handlerChain = handlerChain.then(() => pending).catch((error) => console.error(`Error in handler for ${pattern}:`, error));
      }
    };

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
              runHandler(assembledData as TResponse);
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
          // Regular message - decode MessagePack data
          const data = decode(msg.data);
          runHandler(data);
        }
      } catch (error) {
        console.error(`Error processing message for ${pattern}:`, error);
      }
    };

    const sub = this.nc.subscribe(pattern, {
      ...(options?.queue ? { queue: options.queue } : {}),
      callback: processMessage,
    });
    this.subscriptions.set(pattern, [sub]);
    this._subscriptionMeta.set(pattern, { pattern, handler, options });

    const unsubscribe = () => {
      try {
        sub.unsubscribe();
      } catch {
        // Ignore unsubscribe errors
      } finally {
        this.subscriptions.delete(pattern);
        this._subscriptionMeta.delete(pattern);
      }
    };

    // Return unsubscribe function
    return unsubscribe;
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
    const encoded = encode(data);

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
            errorData = decode(msg.data);
          } catch {
            // Ignore decoding errors
          }
        }

        throw createError(errorCode, errorMsg, errorData);
      }

      const decoded = decode(msg.data);

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
    return this.withNoResponderRetry(() => this._callOnce<TResponse>(subject, ...args));
  }

  /**
   * Make an RPC call (single attempt)
   */
  private async _callOnce<TResponse = any>(subject: string, ...args: any[]): Promise<TResponse> {
    if (!this.isConnected && !this.isClosed) {
      await this.connect();
    }

    if (!this.nc) {
      throw new Error('Not connected');
    }

    const id = generateId(this.options.connId);
    const timeout = this.options.timeout ?? 30000;
    // Use different reply patterns for RPC vs service calls
    const replySubject = subject.startsWith('rpc.') ? `rpc.reply.${id}` : `${subject}.reply.${id}`;

    return new Promise<TResponse>(async (resolve, reject) => {
      // Initialize variables
      let sub: Subscription | undefined;
      let unsubscribe: (() => void) | undefined;

      // Setup timeout
      const timeoutHandle = setTimeout(() => {
        if (this.pendingRequests.has(id)) {
          this.pendingRequests.delete(id);
          reject(createError(ERROR_CODES.TIMEOUT, `RPC call to "${subject}" timed out after ${timeout}ms`));
        }
      }, timeout);

      // Store pending request
      this.pendingRequests.set(id, {
        resolve,
        reject,
        timeout: timeoutHandle,
      });

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
              const result = response.result;
              // Attach __methods to result for proxy method discovery
              // Skip binary data types (Uint8Array, ArrayBuffer)
              const isBinaryData = result instanceof Uint8Array || result instanceof ArrayBuffer || (typeof Buffer !== 'undefined' && Buffer.isBuffer(result));

              if (response.__methods && result !== null && typeof result === 'object' && !isBinaryData) {
                result.__methods = response.__methods;
              }
              pending.resolve(result);
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
    let sub: Subscription | undefined;
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
      if (sub && !sub.isClosed()) {
        try {
          sub.unsubscribe();
        } catch {
          // ignore
        }
      }
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

    const requestCallback = (err: Error | null, msg: Msg): void => {
      if (msg && msg.data?.length === 0 && msg.headers?.code === 503) {
        const h = client.streamHandlers.get(id);
        h?.error(new errors.NoRespondersError(subject));
        cleanupOnce();
      } else if (err) {
        const h = client.streamHandlers.get(id);
        h?.error(err);
        cleanupOnce();
      }
    };

    const setup = async (): Promise<void> => {
      if (!client.isConnected && !client.isClosed) {
        await client.connect();
      }
      if (!client.nc) throw new Error('Not connected');

      id = generateId(client.options.connId);
      streamSubject = `stream.${subject}.${id}`;
      client.streamHandlers.set(id, handler);

      unsubscribe = await client.subscribe(streamSubject, handleStreamMessage);
      const inbox = scopedInbox(client.options.connId);
      sub = client.nc.subscribe(inbox, { max: 1, callback: requestCallback });

      const streamParams = { __stream: true, __streamSubject: streamSubject, args };
      const message: RPCMessage = { id, method: 'stream', params: streamParams };
      await client.publish(subject, message, { reply: inbox });
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
    let sub: Subscription | undefined;
    let inbox = '';
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

    const cleanupOnce = async (): Promise<void> => {
      if (cleanedUp) return;
      cleanedUp = true;
      responseUnsub?.();
      if (sub && !sub.isClosed()) {
        try {
          sub.unsubscribe();
        } catch {
          // ignore
        }
      }
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

      iteratorId = generateId(client.options.connId);
      requestSubject = `_rpc.iterator.${iteratorId}.request`;
      responseSubject = `_rpc.iterator.${iteratorId}.response`;

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

      const requestCallback = (err: Error | null, msg: Msg): void => {
        let isError = false;
        if (msg && msg.data?.length === 0 && msg.headers?.code === 503) {
          const e = new errors.NoRespondersError(subject);
          isError = true;
          ended = true;
          error = createError('503', e.message);
        } else if (err) {
          isError = true;
          ended = true;
          error = createError(ERROR_CODES.INTERNAL_ERROR, err.message);
        }
        if (isError) {
          const response: PullIteratorResponse<any> = {
            type: 'error',
            id: iteratorId,
            error: error ? error.toJSON() : undefined,
          };
          if (responseResolver) {
            const r = responseResolver;
            responseResolver = null;
            r(response);
          } else {
            responseQueue.push(response);
          }
        }
      };

      inbox = scopedInbox(client.options.connId);
      sub = client.nc.subscribe(inbox, { max: 1, callback: requestCallback });
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
          await client.publish(requestSubject, nextRequest, { reply: inbox });
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
   */
  public callPullIteratorWithCallback(subject: string, callbacks: Record<string, (...a: any[]) => any>, onewayMethods: string[], ...args: any[]): AsyncGenerator<void> {
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
    let sub: Subscription | undefined;
    let inbox = '';
    let callbackUnsub: (() => void) | undefined;
    let responseUnsub: (() => void) | undefined;

    const responseQueue: PullIteratorResponse[] = [];
    let responseResolver: ((value: PullIteratorResponse) => void) | null = null;
    let callbackChain: Promise<void> = Promise.resolve();

    const settlePendingAsDone = (): void => {
      const r = responseResolver;
      if (r) {
        responseResolver = null;
        r({ id: iteratorId, type: 'done' });
      }
    };

    const cleanupOnce = async (): Promise<void> => {
      if (cleanedUp) return;
      cleanedUp = true;
      callbackUnsub?.();
      responseUnsub?.();
      if (sub && !sub.isClosed()) {
        try {
          sub.unsubscribe();
        } catch {
          // ignore
        }
      }
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

      iteratorId = generateId(client.options.connId);
      requestSubject = `_rpc.iterator.${iteratorId}.request`;
      responseSubject = `_rpc.iterator.${iteratorId}.response`;
      callbackSubject = `_rpc.cb.${iteratorId}`;
      const callbackMethods = Object.keys(callbacks).filter((k) => typeof callbacks[k] === 'function');

      callbackUnsub = await client.subscribe(callbackSubject, (msg: CallbackInvocation) => {
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

      const initParams: PullCallbackParams = {
        __pullCallback: true,
        __iteratorId: iteratorId,
        __callbackSubject: callbackSubject,
        __callbackMethods: callbackMethods,
        __onewayMethods: onewayMethods,
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

      const requestCallback = (err: Error | null, msg: Msg): void => {
        let isError = false;

        if (msg && msg.data?.length === 0 && msg.headers?.code === 503) {
          const e = new errors.NoRespondersError(subject);
          isError = true;
          ended = true;
          error = createError('503', e.message);
        } else if (err) {
          isError = true;
          ended = true;
          error = createError(ERROR_CODES.INTERNAL_ERROR, err.message);
        }

        if (isError) {
          const response: PullIteratorResponse<any> = {
            type: 'error',
            id: iteratorId,
            error: error ? error.toJSON() : undefined,
          };

          if (responseResolver) {
            const r = responseResolver;
            responseResolver = null;
            r(response);
          } else {
            responseQueue.push(response);
          }
        }
      };

      inbox = scopedInbox(client.options.connId);
      sub = client.nc.subscribe(inbox, { max: 1, callback: requestCallback });
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

        const nextRequest: PullIteratorRequest = { id: iteratorId, type: 'next' };
        try {
          await client.publish(requestSubject, nextRequest, { reply: inbox });
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
        // 'value' — wait for all callback handlers queued for the batch
        // to finish. A slow handler stalls here → stalls next()
        // request → server parks at its own yield. End-to-end backpressure.
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

    const id = generateId(this.options.connId);
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

    // Extract methods based on option
    const handlersMap = options?.withoutDecorators ? extractNestedMethodsWithoutDecorators(handlers) : extractNestedMethodsWithDecorators(handlers);
    const methodNames = Object.keys(handlersMap);

    for (const [method, handler] of Object.entries(handlersMap)) {
      const subject = `rpc.${namespace}.${method}`;

      const unsubscribe = await client.subscribe(
        subject,
        async (msg: RPCMessage) => {
          const response: RPCResponse = { id: msg.id, __methods: methodNames };

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
              const cleanup = await handlePullIteratorRequest(handler, args, iteratorId, client);

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

              const cleanup = await handlePullCallbackRequest(handler, args, iteratorId, callbackSubject, callbackMethods, onewayMethods, client);

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

              const cleanup = await handleCallbackRequest(handler, cbArgs, callbackSubject, msg.id, client);
              client.callbackCleanups.set(msg.id, cleanup);

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

      // Cleanup pull iterators
      await Promise.allSettled(
        Array.from(client.pullIteratorCleanups.entries()).map(([id, cleanup]) => {
          client.pullIteratorCleanups.delete(id);
          return cleanup();
        }),
      );

      // Cleanup callbacks
      await Promise.allSettled(
        Array.from(client.callbackCleanups.entries()).map(([id, cleanup]) => {
          client.callbackCleanups.delete(id);
          return cleanup();
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
            const data = decode(msg.data);

            // Call handler with subject
            const result = await handler(data);

            // Send response
            if (msg.reply) {
              const response = encode(result);
              msg.respond(response);
            }
          } catch (error) {
            // Send error response
            if (msg.reply) {
              const errorResponse = encode({
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
