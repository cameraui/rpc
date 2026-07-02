import type { EndpointInfo, ServiceInfo } from '@nats-io/services';
import type { RPCClient } from './client.js';
import type { Promisify } from './types.js';

/**
 * Symbol keys used to mark objects as rpc callback bundles.
 * Hidden from Object.keys() / JSON serialization.
 */
const RPC_CALLBACKS_MARKER = Symbol.for('rpc.callbacks');
const RPC_CALLBACKS_ONEWAY = Symbol.for('rpc.callbacks.oneway');

/**
 * Mark a plain object of functions as a callback bundle that can be passed
 * as an argument to pull-callback RPC methods. The proxy detects the marker
 * and routes the call through callPullIteratorWithCallback().
 *
 * @example
 *   const cbs = rpcCallbacks({
 *     onItem:    (data) => queue.push(data),
 *     onEndOfBatch: () => batchEnd.resolve(),
 *   }, { oneway: ['onItem', 'onEndOfBatch'] });
 *
 *   for await (const _ of service.pullBatches(count, cbs)) {
 *     // batch boundary — apply backpressure here
 *   }
 */
// eslint-disable-next-line space-before-function-paren
export function rpcCallbacks<T extends Record<string, (...a: any[]) => any>>(obj: T, options?: { oneway?: (keyof T)[] }): T {
  Object.defineProperty(obj, RPC_CALLBACKS_MARKER, { value: true, enumerable: false });
  Object.defineProperty(obj, RPC_CALLBACKS_ONEWAY, {
    value: options?.oneway ?? Object.keys(obj),
    enumerable: false,
  });
  return obj;
}

/** Test whether a value was produced by rpcCallbacks(). */
export function isRpcCallbacks(v: any): v is Record<string, (...a: any[]) => any> {
  return !!v && typeof v === 'object' && v[RPC_CALLBACKS_MARKER] === true;
}

/** Read the oneway-method list from a callback bundle. */
export function getCallbacksOneway(v: any): string[] {
  return v?.[RPC_CALLBACKS_ONEWAY] ?? [];
}

/**
 * Promise check. Used internally to detect async handlers
 * that need to be awaited so dispatch stays ordered.
 */
export function isPromise(value: unknown): value is Promise<unknown> {
  if (value instanceof Promise) return true;
  if (value === null || typeof value !== 'object') return false;
  const v = value as { then?: unknown; catch?: unknown };
  return typeof v.then === 'function' && typeof v.catch === 'function';
}

/**
 * Create a proxy for RPC calls with support for nested objects
 * Works for both flat and deep object structures
 *
 * @param client - RPC client with call and callStream methods
 * @param namespace - RPC namespace
 * @param path - Current path in proxy hierarchy (internal use)
 * @param methodCache - Shared method cache for method discovery
 * @returns Proxy that intercepts property access and method calls
 *
 * @example
 * // Flat usage
 * const service = client.createProxy<MyService>('myservice');
 * await service.someMethod();
 *
 * // Nested usage
 * const app = client.createProxy<AppService>('app');
 * await app.db.find('key');
 *
 * // Streaming
 * for await (const item of service.dataStream()) {
 *   console.log(item);
 * }
 *
 * // Optional chaining for unknown methods
 * const result = await service.maybeMethod?.(); // undefined if method doesn't exist
 */
export function createProxy<T extends object>(client: RPCClient, namespace: string, path: string[] = [], methodCache: Set<string> | null = null): Promisify<T> {
  // Shared cache reference that can be updated
  let cache = methodCache;

  const updateCache = (methods: string[]) => {
    cache ??= new Set(methods);
  };

  // Call via the meta side channel: the response's __methods travel next to
  // the result instead of being spliced into it, so the result object is
  // passed through untouched (no mutation, no spread copy). Discovery is
  // requested only while the cache is empty — once the first response filled
  // it, responses stay lean (no __methods payload).
  const callAndCache = async (subject: string, args: any[]): Promise<any> => {
    const { result, methods } = await client.callWithMeta(subject, args, { discover: cache === null });
    if (methods) {
      updateCache(methods);
    }
    return result;
  };

  return new Proxy({} as T, {
    get(_target, prop: string) {
      // Handle promise-like detection (for async/await)
      if (prop === 'then' || prop === 'catch' || prop === 'finally') {
        return undefined;
      }

      // Handle inspection and debugging
      if (typeof prop === 'symbol' || prop === 'toString') {
        return () => `[RPCProxy ${namespace}${path.length ? '.' + path.join('.') : ''}]`;
      }

      // If we have cached methods and this method doesn't exist, return undefined
      // This enables: proxy.nonExistent?.() → undefined
      if (cache && path.length === 0 && !cache.has(prop)) {
        return undefined;
      }

      // Build the full path
      const fullPath = [...path, prop];

      // Return a callable proxy that can also act as a thenable
      return new Proxy(function () {}, {
        // Handle method calls
        apply(_target, _thisArg, args: any[]) {
          const method = fullPath.join('.');
          const subject = `rpc.${namespace}.${method}`;
          const isPullIterator = prop.toLowerCase().includes('pull');

          // rpcCallbacks() bundle in args → pull-callback mode.
          // The bundle itself is the unambiguous marker — a name heuristic
          // would only add a false negative for methods whose name doesn't
          // happen to contain "pull".
          const callbacksIdx = args.findIndex((a) => isRpcCallbacks(a));
          if (callbacksIdx !== -1) {
            const cbs = args[callbacksIdx];
            const oneway = getCallbacksOneway(cbs);
            const otherArgs = args.filter((_, i) => i !== callbacksIdx);
            return client.callPullIteratorWithCallback(subject, cbs, oneway, ...otherArgs);
          }

          // Detect plain function argument → classic callback subscription
          const callbackIdx = args.findIndex((a) => typeof a === 'function');
          if (callbackIdx !== -1) {
            const callback = args[callbackIdx];
            const otherArgs = args.filter((_, i) => i !== callbackIdx);
            return client.callWithCallback(subject, otherArgs, callback);
          }

          const isGenerator = prop.toLowerCase().includes('generate');

          if (isGenerator) {
            return client.callStream(subject, ...args);
          } else if (isPullIterator) {
            return client.callPullIterator(subject, ...args);
          } else {
            // Route through the meta channel so __methods feeds the cache
            // without touching the result object.
            return callAndCache(subject, args);
          }
        },

        // Handle nested property access and promise resolution
        get(_target, nestedProp: string) {
          // Check if this is a promise method (then, catch, finally)
          if (nestedProp === 'then' || nestedProp === 'catch' || nestedProp === 'finally') {
            // This property is being awaited - make the RPC call
            const method = fullPath.join('.');
            const promise = (async () => {
              if (!client.isConnected && !client.isClosed) {
                await client.connect();
              }
              return callAndCache(`rpc.${namespace}.${method}`, []);
            })();
            // @ts-ignore
            return promise[nestedProp as keyof Promise<any>].bind(promise);
          }

          // Otherwise, return another proxy for nested access (share cache)
          return createProxy<any>(client, namespace, fullPath, cache)[nestedProp];
        },
      });
    },
  }) as Promisify<T>;
}

/**
 * Create a service proxy with proper streaming support
 * Handles NATS service endpoints with automatic discovery
 *
 * @param client - RPC client instance
 * @param selected - Selected service info from discovery
 * @param timeout - Optional timeout for requests
 * @param path - Current path in proxy hierarchy (internal use)
 * @returns Proxy that intercepts property access and method calls
 *
 * @example
 * const service = await client.createServiceProxy<MyService>('myservice');
 * await service.someMethod();
 *
 * // Nested endpoints
 * await service.config.get();
 *
 * // Streaming
 * for await (const item of service.generateData()) {
 *   console.log(item);
 * }
 */
export function createServiceProxy<T extends object>(client: RPCClient, selected: ServiceInfo, timeout?: number, path: string[] = []): Promisify<T> {
  const target = {} as T;

  return new Proxy(target, {
    get(target, prop: string) {
      // Handle special properties
      if (prop === 'then' || prop === 'catch' || prop === 'finally') {
        return undefined;
      }

      if (typeof prop === 'symbol' || prop === 'toString') {
        return () => `[ServiceProxy ${selected.name}${path.length ? '.' + path.join('.') : ''}]`;
      }

      // Check if property exists on target (for added methods like disconnect)
      if (prop in target) {
        return target[prop as keyof T];
      }

      const fullPath = [...path, prop];
      const fullPathStr = fullPath.join('.');

      // Check if this is an endpoint
      const endpoint = selected.endpoints.find((e: EndpointInfo) => {
        // Match exact path or last segment
        if (e.subject === fullPathStr) return true;
        const parts = e.subject.split('.');
        return parts[parts.length - 1] === prop && parts.slice(0, -1).join('.') === path.join('.');
      });

      if (endpoint) {
        // Return a callable proxy similar to createProxy
        return new Proxy(function () {}, {
          apply(_target, _thisArg, args: any[]) {
            const isPullIterator = prop.toLowerCase().includes('pull');

            // rpcCallbacks() bundle in args → pull-callback mode
            // (bundle marker is unambiguous, no name heuristic needed).
            const callbacksIdx = args.findIndex((a) => isRpcCallbacks(a));
            if (callbacksIdx !== -1) {
              const cbs = args[callbacksIdx];
              const oneway = getCallbacksOneway(cbs);
              const otherArgs = args.filter((_, i) => i !== callbacksIdx);
              return client.callPullIteratorWithCallback(endpoint.subject, cbs, oneway, ...otherArgs);
            }

            // Detect plain function argument → callback mode
            const callbackIdx = args.findIndex((a) => typeof a === 'function');
            if (callbackIdx !== -1) {
              const callback = args[callbackIdx];
              const otherArgs = args.filter((_, i) => i !== callbackIdx);
              return client.callWithCallback(endpoint.subject, otherArgs, callback);
            }

            const isGenerator = prop.toLowerCase().includes('generate');

            if (isGenerator) {
              return client.callStream(endpoint.subject, ...args);
            } else if (isPullIterator) {
              return client.callPullIterator(endpoint.subject, ...args);
            } else {
              return client.call(endpoint.subject, ...args);
            }
          },
        });
      }

      // Check if this is a nested namespace
      const prefix = fullPathStr + '.';
      const hasNested = selected.endpoints.some((e: EndpointInfo) => e.subject.startsWith(prefix));

      if (hasNested) {
        return createServiceProxy(client, selected, timeout, fullPath);
      }

      return undefined;
    },
  }) as Promisify<T>;
}

// 9-char base36 suffix split into a per-process random prefix (5 chars,
// drawn once) and a monotonically increasing counter (4 chars, random start,
// wraps at 36^4 ≈ 1.68M). Within a process ids are collision-free per
// millisecond by construction; across processes the per-pair collision
// probability is 36^-5 (prefix) × 36^-4 (counter) = 36^-9 — identical to the
// previous fully random 9-char suffix, without a Math.random() per call.
const ID_COUNTER_SPACE = 36 ** 4;
const idPrefix = Math.floor(Math.random() * 36 ** 5)
  .toString(36)
  .padStart(5, '0');
let idCounter = Math.floor(Math.random() * ID_COUNTER_SPACE);

export function generateId(replyPrefix?: string): string {
  idCounter = (idCounter + 1) % ID_COUNTER_SPACE;
  const id = `${Date.now()}-${idPrefix}${idCounter.toString(36).padStart(4, '0')}`;
  return replyPrefix ? `${replyPrefix}.${id}` : id;
}

/**
 * Generate a client-local reply prefix: ~10 base36 chars (48 random bits),
 * crypto-backed where available. Used as the first dot-separated segment of
 * every call id when no connId is configured, so all reply subjects of a
 * client share one wildcard-subscribable prefix (`rpc.reply.<prefix>.>`).
 */
export function generateReplyPrefix(): string {
  let value: number;
  const cryptoObj = (globalThis as { crypto?: Crypto }).crypto;
  if (cryptoObj?.getRandomValues) {
    const bytes = new Uint8Array(6);
    cryptoObj.getRandomValues(bytes);
    value = bytes.reduce((acc, b) => acc * 256 + b, 0);
  } else {
    value = Math.floor(Math.random() * 2 ** 48);
  }
  return value.toString(36).padStart(10, '0');
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Per-subscription serial dispatcher with a sync fast path.
 *
 * Guarantees: handlers run strictly serially in dispatch order — also for
 * mixed sync/async handlers. This matches the previous "chain every call"
 * behavior (and Python's awaited dispatch) that backpressure-sensitive
 * callers rely on: an awaiting handler blocks the next message.
 *
 * Optimization: when the chain is idle (no async handler still pending),
 * the handler is invoked inline. Only if it returns a promise does the
 * chain become busy; purely synchronous traffic never allocates promises.
 * While the chain is busy, every subsequent dispatch is appended to the
 * chain so ordering is preserved.
 *
 * flush() resolves once every message dispatched so far has had its
 * handler executed (resolved immediately when the chain is idle).
 */
export function createSerialDispatcher(
  handler: (data: any) => void | Promise<void>,
  onError: (error: unknown) => void,
): { dispatch: (data: any) => void; flush: () => Promise<void> } {
  let chain: Promise<void> = Promise.resolve();
  // Number of handler completions still pending in the chain.
  let busy = 0;

  const dispatch = (data: any): void => {
    if (busy === 0) {
      // Chain idle — run inline. Sync handlers complete right here.
      let result: void | Promise<void>;
      try {
        result = handler(data);
      } catch (error) {
        onError(error);
        return;
      }
      if (isPromise(result)) {
        busy++;
        chain = result.then(
          () => {
            busy--;
          },
          (error) => {
            busy--;
            onError(error);
          },
        );
      }
      return;
    }

    // Chain busy — append so this message runs after all previous ones.
    busy++;
    chain = chain
      .then(() => handler(data))
      .then(
        () => {
          busy--;
        },
        (error) => {
          busy--;
          onError(error);
        },
      );
  };

  const flush = (): Promise<void> => (busy === 0 ? Promise.resolve() : chain);

  return { dispatch, flush };
}
