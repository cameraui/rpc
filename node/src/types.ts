import type { NatsConnection, Status } from '@nats-io/nats-core';
import type { Channel, PrivateChannel } from './channel.js';
import type { RPCService } from './service.js';

export interface RPCClient {
  readonly service: RPCService;
  readonly isConnected: boolean;
  readonly isClosed: boolean;
  readonly maxPayloadSize: number;
  readonly isStale: boolean;
  connect(options?: { signal?: AbortSignal }): Promise<NatsConnection>;
  disconnect(): Promise<void>;
  suspend(): Promise<void>;
  reconfigure(overrides: { servers?: string[]; auth?: RPCAuthOptions }): void;
  setServers(servers: string[]): void;
  forceReconnect(): Promise<void>;
  abortClose(err?: Error): void;
  status(): AsyncIterable<Status> | undefined;
  flush(timeoutMs?: number): Promise<void>;
  publish<TMessage = any>(subject: string, data: TMessage): Promise<void>;
  subscribe<TResponse = any>(pattern: string, handler: (data: TResponse) => void | Promise<void>, options?: { queue?: string }): Promise<() => void>;
  request<TRequest = any, TResponse = any>(
    subject: string,
    data: TRequest,
    options?: { timeout?: number; noResponderRetry?: { maxRetries?: number; delays?: number[] } },
  ): Promise<TResponse>;
  onRequest<TRequest = any, TResponse = any>(pattern: string, handler: (data: TRequest) => Promise<TResponse> | TResponse): Promise<() => void>;
  registerHandler(
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
  ): Promise<() => Promise<void>>;
  callWithCallback<TResponse = any>(subject: string, args: any[], callback: (value: TResponse) => void | Promise<void>): Promise<() => void>;
  callPullIteratorWithCallback(
    subject: string,
    callbacks: Record<string, (...a: any[]) => any>,
    onewayMethods: string[] | PullCallbackCallOptions,
    ...args: any[]
  ): AsyncGenerator<void>;
  channel(channelId: string, options?: { isolatedConnection?: boolean }): Promise<Channel>;
  privateChannel(channelId: string, targetClientId: string, options?: { isolatedConnection?: boolean }): Promise<PrivateChannel>;
  createProxy<T extends object>(namespace: string): Promisify<T>;
  createProxy<T extends object>(namespace: string, options: { isolatedConnection?: false; onTiming?: ProxyTimingReporter } & ProxyCallOptions): Promisify<T>;
  createProxy<T extends object>(
    namespace: string,
    options: { isolatedConnection: true } & ProxyCallOptions,
  ): {
    proxy: Promisify<T>;
    close: () => Promise<void>;
  };
  createServiceProxy<T extends object>(serviceName: string): Promise<Promisify<T>>;
  createServiceProxy<T extends object>(serviceName: string, options: { preferredId?: string; timeout?: number; isolatedConnection?: false }): Promise<Promisify<T>>;
  createServiceProxy<T extends object>(
    serviceName: string,
    options: { preferredId?: string; timeout?: number; isolatedConnection?: true },
  ): Promise<{ proxy: Promisify<T>; close: () => Promise<void> }>;
}

export interface RPCAuthOptions {
  /** Username for authentication */
  user: string;

  /** Password for authentication */
  password: string;
}

/**
 * Configuration options for RPC client
 */
export interface RPCClientOptions {
  /** NATS server URLs */
  servers: string[];

  /** Client name for identification */
  name: string;

  /** Per-connection isolation token. When set, it is folded as a fixed-position
   *  token into every caller-generated reply/callback/stream/iterator subject
   */
  connId?: string;

  /** Authentication credentials */
  auth?: RPCAuthOptions;

  /** Default RPC call timeout in milliseconds */
  timeout?: number;

  /**
   * Per-dial-attempt connect timeout in milliseconds (NATS `timeout`).
   * Bounds how long a single (re)connect attempt may hang before the dial
   * loop retries. Distinct from `timeout`, which is the RPC call timeout.
   */
  connectTimeout?: number;

  /** Enable automatic reconnection */
  reconnect?: boolean;

  /** Max number of pings the client will allow unanswered before raising a stale connection error */
  maxPingOut?: number;

  /** Number of milliseconds between client-sent pings */
  pingInterval?: number;

  /** Number of milliseconds to wait for a ping response before considering the connection stale and initiating a reconnect */
  pingTimeout?: number;

  /** Maximum reconnection attempts (-1 for infinite) */
  maxReconnectAttempts?: number;

  /** Delay between reconnection attempts in milliseconds */
  reconnectTimeWait?: number;

  /** Maximum delay between reconnection attempts in milliseconds (default: 0 - disabled — legacy linear delay) */
  reconnectionDelayMax?: number;

  /** Randomization factor for reconnection delay (default: 0 - no multiplicative jitter) */
  reconnectionRandomizationFactor?: number;

  /** Random jitter in ms added to `reconnectTimeWait` */
  reconnectJitter?: number;

  /** Random jitter in ms added to `reconnectTimeWait` for TLS connections */
  reconnectJitterTLS?: number;

  /** Don't abort the connection after two consecutive auth errors. Important
   *  for clients with token rotation: a brief window where the proxy still
   *  sees the previous token would otherwise kill reconnection permanently
   *  and require a full page reload */
  ignoreAuthErrorAbort?: boolean;

  /** Print NATS protocol traffic to the console. Development only. */
  debug?: boolean;

  /** TLS configuration */
  tls?: {
    cert: string;
    key: string;
    ca: string;
  };

  /** Cap on the message size in bytes (default and 0: the server's max_payload).
   *  The client uses the smaller of this and the server's limit, less 8 KiB,
   *  and sends bigger messages in chunks. Set it below the server's limit
   *  when messages cross a leaf link to a server with a smaller max_payload.
   *  Any other value below 16384 or not an integer throws. With a server
   *  limit below 16384 nothing is chunked: a message up to that limit goes
   *  out whole, a bigger one fails with PAYLOAD_TOO_LARGE. */
  maxPayloadSize?: number;

  /** Largest chunked message this client accepts, in bytes (default and 0: 1 GiB),
   *  and the most all chunked messages it is receiving may hold together. A
   *  chunk header announcing more is dropped before anything is allocated. */
  maxTransferSize?: number;

  /** Write large binary arguments of call() and proxy calls to the socket
   *  without copying them (default: false). A binary argument must not be
   *  changed or released until its call settles; disconnect() and suspend()
   *  settle pending calls only once the socket is closed. A call that timed
   *  out can still have its request in the socket queue: do not reuse or
   *  release that buffer, leave it to the garbage collector.
   *  publish(), streams, callbacks and replies always copy. Node only. */
  zeroCopyBinaries?: boolean;

  /** How long requests that a handler registration has already received may
   *  take to finish on disconnect(), suspend() and the cleanup returned by
   *  registerHandler(), in ms (default: 250, 0: stop at once without
   *  answering them). The registration stops taking requests first, so a
   *  caller sees no responder and its retry can reach another instance.
   *  Requests still unanswered at the deadline get CONNECTION_CLOSED.
   *  Stream, pull and callback sessions end at once. */
  drainTimeoutMs?: number;

  /** How a handler notices that the consumer of a stream, callback,
   *  pull-iterator or pull-callback session went away without cancelling
   *  (default: on, see OrphanProbeOptions). false turns it off.
   *  registerHandler() takes the same option for one registration. */
  orphanProbe?: OrphanProbeOptions | false;

  /** Block connect() until the first connection succeeds (default: true).
   *  Set to false for browser clients where disconnect() must be able to
   *  abort an in-flight connection attempt immediately. */
  waitOnFirstConnect?: boolean;

  /** Maximum time in ms to wait for the NATS connection to fully close
   *  during disconnect(). Relevant when waitOnFirstConnect is false, as the
   *  underlying transport may still be mid-handshake when close() is called.
   *  Default: 2000 */
  disconnectTimeout?: number;

  /** Retry configuration for 503 / no-responder errors. Without both fields
   *  a call retries after 25, 50, 100, 325, 1000 and 2000 ms, about 3.5 s in
   *  all. With only delays it retries 3 times, with only maxRetries it waits
   *  500, 1000 and then 2000 ms. */
  noResponderRetry?: {
    /** Maximum number of retries (default: 6 without delays, 3 with delays) */
    maxRetries?: number;
    /** Delay in ms before each retry attempt, the last one repeats (default: [25, 50, 100, 325, 1000, 2000], or [500, 1000, 2000] when only maxRetries is set) */
    delays?: number[];
  };
}

/**
 * Liveness probe of the stream, callback, pull-iterator and pull-callback
 * sessions a handler serves. At most once per intervalMs a publish of the
 * session asks the server to report it when nobody listens. A session without
 * a publish in that time sends a probe instead: a stream or callback session
 * a message of type 'ping' on its subject, a pull session `{ type: 'ping' }`
 * to rpc.reply.<iterator id>, the consumer's reply inbox. Consumers drop
 * them. When the probes keep finding no responders for graceMs, the session
 * ends as if the consumer had cancelled. A consumer gone for longer than
 * graceMs, also one that reconnects later, finds its session ended.
 */
export interface OrphanProbeOptions {
  /** Least time between two probes of a session, in ms (default: 30000, also for 0) */
  intervalMs?: number;

  /** How long probes must find no responders before the session ends, in ms (default: 300000, also for 0) */
  graceMs?: number;
}

/**
 * RPC request message format
 */
export interface RPCMessage<T = any> {
  /** Unique request ID */
  id: string;

  /** Method name to call */
  method: string;

  /** Method parameters */
  params: T;

  /** Optional error (unused in requests) */
  error?: RPCError;

  /**
   * Method-discovery request (internal metadata for proxies). When true, the
   * responder attaches the namespace's `__methods` list to the response.
   * Proxies set this only while their method cache is empty — afterwards the
   * list would be dead wire weight on every response. Lives on the envelope
   * (not in params) so it never leaks into handler arguments; old responders
   * ignore it.
   */
  __discover?: boolean;

  /**
   * Timing request (internal metadata). When true, the responder stamps the
   * clock right before and right after the handler runs and returns both in
   * `__timing`. Lives on the envelope so it never reaches handler arguments;
   * old responders ignore it and the caller simply sees no timing back.
   */
  __timing?: boolean;

  /**
   * How long the caller still waits for the answer when it sends the request,
   * in ms (internal metadata). A responder that opted in with dropExpired
   * skips a request that waited longer than this in its queue. Old
   * responders ignore it.
   */
  __ttl?: number;
}

/**
 * RPC response message format
 */
export interface RPCResponse<T = any> {
  /** Request ID this response belongs to */
  id: string;

  /** Result data (if successful) */
  result?: T;

  /** Error information (if failed) */
  error?: RPCError;

  /** Available methods on namespace (internal metadata for proxy) */
  __methods?: string[];

  /**
   * Responder-side clock around the handler, in epoch milliseconds, only when
   * the request asked for it. Both stamps come from the same clock, so their
   * difference is free of any offset against the caller's clock.
   */
  __timing?: RPCTiming;
}

/** Responder clock stamps around a handler invocation. */
export interface RPCTiming {
  /** Right before the handler is invoked */
  start: number;

  /** Right after the handler returned */
  end: number;
}

/** Receives what every call of a timed proxy cost. */
export type ProxyTimingReporter = (method: string, timing: RPCCallTiming) => void;

/**
 * Per-proxy settings for the plain calls of a proxy and its nested proxies.
 * Streams, pull iterators and callback subscriptions keep the client options.
 */
export interface ProxyCallOptions {
  /** Retry for no-responder errors, overrides the client option. `maxRetries: 0` turns it off. */
  noResponderRetry?: {
    /** Maximum number of retries */
    maxRetries?: number;
    /** Delay in ms before each retry attempt */
    delays?: number[];
  };

  /** Timeout in ms for each attempt, overrides the client's `timeout` */
  timeout?: number;
}

/** What a timed call cost, split into the responder's share and the rest. */
export interface RPCCallTiming {
  /** Round trip measured by the caller */
  totalMs: number;

  /** Time inside the responder's handler */
  handlerMs: number;

  /** Round trip minus handler: encode, transport, decode, both ways */
  transportMs: number;
}

/**
 * RPC error format
 */
export interface RPCError {
  /** Error code (see ERROR_CODES) */
  code: string;

  /** Human-readable error message */
  message: string;

  /** Additional error data */
  data?: any;
}

/**
 * Convert service interface to async client interface
 * - Regular methods become Promise-returning
 * - AsyncGenerator methods stay as AsyncGenerators
 * - Generator methods become AsyncGenerators
 * - Promise<Generator> becomes AsyncGenerator
 * - Promise<AsyncGenerator> becomes AsyncGenerator
 * - Generic methods preserve their generic parameters
 */
type PromisifyMethod<T> = T extends (...args: infer Args) => infer R
  ? R extends AsyncGenerator<infer Y, any, any>
    ? (...args: Args) => AsyncGenerator<Y, any, any>
    : R extends Generator<infer Y, any, any>
      ? (...args: Args) => AsyncGenerator<Y, any, any>
      : R extends Promise<infer U>
        ? U extends AsyncGenerator<infer Y, any, any>
          ? (...args: Args) => AsyncGenerator<Y, any, any>
          : U extends Generator<infer Y, any, any>
            ? (...args: Args) => AsyncGenerator<Y, any, any>
            : T
        : (...args: Args) => Promise<R>
  : T;

export type Promisify<T> = {
  [K in keyof T]: T[K] extends ((...args: any[]) => any) | undefined
    ? T[K] extends undefined
      ? undefined
      : PromisifyMethod<Exclude<T[K], undefined>> | (T[K] extends undefined ? undefined : never)
    : T[K] extends (...args: any[]) => any
      ? PromisifyMethod<T[K]>
      : T[K];
};

/**
 * Message format for streaming data
 */
export interface StreamMessage<T = any> {
  /** Stream ID (same as request ID) */
  id: string;

  /** Message type, 'ping' is the liveness probe of an idle stream, consumers drop it */
  type: 'data' | 'end' | 'error' | 'ping';

  /** Data payload (for 'data' type) */
  data?: T;

  /** Error information (for 'error' type) */
  error?: RPCError;
}

/**
 * Pull iterator request message
 */
export interface PullIteratorRequest {
  /** Iterator ID */
  id: string;

  /** Request type */
  type: 'next' | 'cancel';
}

/**
 * Pull iterator response message
 */
export interface PullIteratorResponse<T = any> {
  /** Iterator ID */
  id: string;

  /** Response type */
  type: 'value' | 'done' | 'error';

  /** Value (for 'value' type) */
  value?: T;

  /** Error (for 'error' type) */
  error?: RPCError;
}

/**
 * Parameters for callback subscription request
 */
export interface CallbackParams {
  __callback: true;
  __callbackSubject: string;
  args: any[];
}

/**
 * Message format for callback data pushed to subscribers
 */
export interface CallbackMessage<T = any> {
  id: string;
  /** 'ping' is the liveness probe of an idle session, consumers drop it */
  type: 'data' | 'error' | 'ping';
  data?: T;
  error?: RPCError;
}

/**
 * Parameters for pull-iterator-with-callbacks request.
 * Combines pull-iterator flow control with oneway callbacks for frame-level delivery.
 */
export interface PullCallbackParams {
  __pullCallback: true;
  __iteratorId: string;
  __callbackSubject: string;
  __callbackMethods: string[];
  __onewayMethods: string[];
  args: any[];
}

/**
 * A single callback invocation pushed from server to client on the callback subject.
 * Oneway: no reply expected.
 */
export interface CallbackInvocation {
  method: string;
  args: any[];
}

/**
 * Options for callPullIteratorWithCallback. Passed in place of the plain
 * onewayMethods string[] (the array form stays supported — the proxy path
 * always uses it). Client-local behavior only, no wire/protocol change.
 */
export interface PullCallbackCallOptions {
  /** Oneway callback method names (same as passing a plain string[]). */
  onewayMethods: string[];

  /** Opt-in n+1 prefetch: send the next `next` request immediately after a
   * batch boundary (`value`) arrives, BEFORE the batch's callbacks are
   * drained — hides one RTT per batch (matches the Go client's behavior).
   * Default false = strict per-batch backpressure. */
  prefetch?: boolean;
}

/**
 * Standard error codes
 */
export const ERROR_CODES = {
  METHOD_NOT_FOUND: 'METHOD_NOT_FOUND',
  INVALID_PARAMS: 'INVALID_PARAMS',
  INTERNAL_ERROR: 'INTERNAL_ERROR',
  TIMEOUT: 'TIMEOUT',
  CONNECTION_CLOSED: 'CONNECTION_CLOSED',
  STREAM_ERROR: 'STREAM_ERROR',
  PAYLOAD_TOO_LARGE: 'PAYLOAD_TOO_LARGE',
  NOT_FOUND: 'NOT_FOUND',
  /** The server does not let this connection publish to the request's subject. */
  FORBIDDEN: 'FORBIDDEN',
} as const;

/**
 * Error code type
 */
export type ErrorCode = (typeof ERROR_CODES)[keyof typeof ERROR_CODES];

/**
 * Chunked transfer header
 */
export interface ChunkedTransferHeader {
  type: 'chunked';
  transferId: string;
  totalChunks: number;
  totalSize: number;
  chunkSize: number;
}

/**
 * Individual chunk message
 */
export interface ChunkData {
  transferId: string;
  index: number;
  data: Uint8Array;
}
