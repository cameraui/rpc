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
  user: string;

  password: string;
}

export interface RPCClientOptions {
  servers: string[];

  name: string;

  connId?: string;

  auth?: RPCAuthOptions;

  timeout?: number; // ms

  connectTimeout?: number; // ms, per dial attempt

  reconnect?: boolean;

  maxPingOut?: number;

  pingInterval?: number; // ms

  pingTimeout?: number; // ms

  maxReconnectAttempts?: number; // -1: infinite

  reconnectTimeWait?: number; // ms

  reconnectionDelayMax?: number; // ms, 0 keeps the linear delay

  reconnectionRandomizationFactor?: number;

  reconnectJitter?: number; // ms

  reconnectJitterTLS?: number; // ms

  ignoreAuthErrorAbort?: boolean;

  debug?: boolean;

  tls?: {
    cert: string;
    key: string;
    ca: string;
  };

  maxPayloadSize?: number; // bytes, 0: the server's max_payload

  maxTransferSize?: number; // bytes, 0: 1 GiB

  // a binary argument must stay unchanged until its call settles
  zeroCopyBinaries?: boolean;

  drainTimeoutMs?: number; // 0: stop at once without answering

  orphanProbe?: OrphanProbeOptions | false;

  waitOnFirstConnect?: boolean;

  disconnectTimeout?: number; // ms

  noResponderRetry?: {
    maxRetries?: number;
    delays?: number[]; // ms, the last one repeats
  };
}

export interface OrphanProbeOptions {
  intervalMs?: number; // default and 0: 30000

  graceMs?: number; // default and 0: 300000
}

export interface RPCMessage<T = any> {
  id: string;

  method: string;

  params: T;

  error?: RPCError;

  __discover?: boolean;

  __timing?: boolean;

  __ttl?: number; // ms
}

export interface RPCResponse<T = any> {
  id: string;

  result?: T;

  error?: RPCError;

  __methods?: string[];

  __timing?: RPCTiming;
}

export interface RPCTiming {
  start: number;

  end: number;
}

export type ProxyTimingReporter = (method: string, timing: RPCCallTiming) => void;

export interface ProxyCallOptions {
  noResponderRetry?: {
    maxRetries?: number;
    delays?: number[]; // ms
  };

  timeout?: number; // ms per attempt
}

export interface RPCCallTiming {
  totalMs: number;

  handlerMs: number;

  transportMs: number;
}

export interface RPCError {
  code: string;

  message: string;

  data?: any;
}

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

export interface StreamMessage<T = any> {
  id: string;

  type: 'data' | 'end' | 'error' | 'ping';

  data?: T;

  error?: RPCError;
}

export interface PullIteratorRequest {
  id: string;

  type: 'next' | 'cancel';
}

export interface PullIteratorResponse<T = any> {
  id: string;

  type: 'value' | 'done' | 'error';

  value?: T;

  error?: RPCError;
}

export interface CallbackParams {
  __callback: true;
  __callbackSubject: string;
  args: any[];
}

export interface CallbackMessage<T = any> {
  id: string;
  type: 'data' | 'error' | 'ping';
  data?: T;
  error?: RPCError;
}

export interface PullCallbackParams {
  __pullCallback: true;
  __iteratorId: string;
  __callbackSubject: string;
  __callbackMethods: string[];
  __onewayMethods: string[];
  args: any[];
}

export interface CallbackInvocation {
  method: string;
  args: any[];
}

export interface PullCallbackCallOptions {
  onewayMethods: string[];

  prefetch?: boolean;
}

export const ERROR_CODES = {
  METHOD_NOT_FOUND: 'METHOD_NOT_FOUND',
  INVALID_PARAMS: 'INVALID_PARAMS',
  INTERNAL_ERROR: 'INTERNAL_ERROR',
  TIMEOUT: 'TIMEOUT',
  CONNECTION_CLOSED: 'CONNECTION_CLOSED',
  STREAM_ERROR: 'STREAM_ERROR',
  PAYLOAD_TOO_LARGE: 'PAYLOAD_TOO_LARGE',
  NOT_FOUND: 'NOT_FOUND',
  FORBIDDEN: 'FORBIDDEN',
} as const;

export type ErrorCode = (typeof ERROR_CODES)[keyof typeof ERROR_CODES];

export interface ChunkedTransferHeader {
  type: 'chunked';
  transferId: string;
  totalChunks: number;
  totalSize: number;
  chunkSize: number;
}

export interface ChunkData {
  transferId: string;
  index: number;
  data: Uint8Array;
}
