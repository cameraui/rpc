import type { NatsConnection, Status } from '@nats-io/nats-core';
import type { Channel, PrivateChannel } from './channel.js';
import type { RPCService } from './service.js';

export interface RPCClient {
  readonly service: RPCService;
  readonly isConnected: boolean;
  readonly isClosed: boolean;
  readonly maxPayloadSize: number;
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
    options?: { isolatedConnection?: boolean; withoutDecorators?: boolean; queue?: string },
  ): Promise<() => Promise<void>>;
  callWithCallback<TResponse = any>(subject: string, args: any[], callback: (value: TResponse) => void | Promise<void>): Promise<() => void>;
  callPullIteratorWithCallback(subject: string, callbacks: Record<string, (...a: any[]) => any>, onewayMethods: string[], ...args: any[]): AsyncGenerator<void>;
  channel(channelId: string, options?: { isolatedConnection?: boolean }): Promise<Channel>;
  privateChannel(channelId: string, targetClientId: string, options?: { isolatedConnection?: boolean }): Promise<PrivateChannel>;
  createProxy<T extends object>(namespace: string): Promisify<T>;
  createProxy<T extends object>(namespace: string, options: { isolatedConnection: false }): Promisify<T>;
  createProxy<T extends object>(
    namespace: string,
    options: { isolatedConnection: true },
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

  /** Maximum payload size in bytes (default: auto-detect from NATS server) */
  maxPayloadSize?: number;

  /** Block connect() until the first connection succeeds (default: true).
   *  Set to false for browser clients where disconnect() must be able to
   *  abort an in-flight connection attempt immediately. */
  waitOnFirstConnect?: boolean;

  /** Maximum time in ms to wait for the NATS connection to fully close
   *  during disconnect(). Relevant when waitOnFirstConnect is false, as the
   *  underlying transport may still be mid-handshake when close() is called.
   *  Default: 2000 */
  disconnectTimeout?: number;

  /** Retry configuration for 503 / no-responder errors. */
  noResponderRetry?: {
    /** Maximum number of retries (default: 3) */
    maxRetries?: number;
    /** Delay in ms before each retry attempt (default: [500, 1000, 2000]) */
    delays?: number[];
  };
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

  /** Message type */
  type: 'data' | 'end' | 'error';

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
  type: 'data' | 'error';
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
