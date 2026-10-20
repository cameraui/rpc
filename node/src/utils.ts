import { DIRECT_SUBJECT_METADATA, directSubject, serviceSubject } from './service.js';

import type { EndpointInfo, ServiceInfo } from '@nats-io/services';
import type { RPCClient } from './client.js';
import type { Promisify, ProxyCallOptions, ProxyTimingReporter } from './types.js';

const RPC_CALLBACKS_MARKER = Symbol.for('rpc.callbacks');
const RPC_CALLBACKS_ONEWAY = Symbol.for('rpc.callbacks.oneway');

// eslint-disable-next-line space-before-function-paren
export function rpcCallbacks<T extends Record<string, (...a: any[]) => any>>(obj: T, options?: { oneway?: (keyof T)[] }): T {
  Object.defineProperty(obj, RPC_CALLBACKS_MARKER, { value: true, enumerable: false });
  Object.defineProperty(obj, RPC_CALLBACKS_ONEWAY, {
    value: options?.oneway ?? Object.keys(obj),
    enumerable: false,
  });
  return obj;
}

export function isRpcCallbacks(v: any): v is Record<string, (...a: any[]) => any> {
  return !!v && typeof v === 'object' && v[RPC_CALLBACKS_MARKER] === true;
}

export function getCallbacksOneway(v: any): string[] {
  return v?.[RPC_CALLBACKS_ONEWAY] ?? [];
}

export interface ProxyMethodCache {
  methods: Set<string> | null;
  roots: Set<string> | null;
  calls: number;
}

export function isPromise(value: unknown): value is Promise<unknown> {
  if (value instanceof Promise) return true;
  if (value === null || typeof value !== 'object') return false;
  const v = value as { then?: unknown; catch?: unknown };
  return typeof v.then === 'function' && typeof v.catch === 'function';
}

export function createProxy<T extends object>(
  client: RPCClient,
  namespace: string,
  path: string[] = [],
  cache: ProxyMethodCache = { methods: null, roots: null, calls: 0 },
  onTiming?: ProxyTimingReporter,
  callOptions?: ProxyCallOptions,
): Promisify<T> {
  const updateCache = (methods: string[]) => {
    if (cache.methods !== null) return;
    cache.methods = new Set(methods);
    cache.roots = null;
  };

  // nested handlers are listed as `db.find`, so `db` passes the root check
  const roots = (methods: Set<string>): Set<string> => (cache.roots ??= new Set([...methods].map((method) => method.split('.', 1)[0])));

  // __methods travel beside the result, asked for from the second call while the cache is empty
  const callAndCache = async (subject: string, args: any[], method: string): Promise<any> => {
    const { result, methods, timing } = await client.callWithMeta(subject, args, {
      ...callOptions,
      discover: cache.methods === null && cache.calls++ >= 1,
      timing: onTiming !== undefined,
      probeNamespace: namespace,
      probeMethod: method,
      onMethods: updateCache,
    });
    if (methods) {
      updateCache(methods);
    }
    if (timing && onTiming) {
      onTiming(method ?? subject, timing);
    }
    return result;
  };

  return new Proxy({} as T, {
    get(_target, prop: string) {
      // not a thenable, so awaiting the proxy itself calls nothing
      if (prop === 'then' || prop === 'catch' || prop === 'finally') {
        return undefined;
      }

      if (typeof prop === 'symbol' || prop === 'toString') {
        return () => `[RPCProxy ${namespace}${path.length ? '.' + path.join('.') : ''}]`;
      }

      // lets proxy.unknownMethod?.() return undefined
      if (cache.methods && path.length === 0 && !roots(cache.methods).has(prop)) {
        return undefined;
      }

      const fullPath = [...path, prop];

      return new Proxy(function () {}, {
        apply(_target, _thisArg, args: any[]) {
          const method = fullPath.join('.');
          const subject = `rpc.${namespace}.${method}`;
          const isPullIterator = prop.toLowerCase().includes('pull');

          const callbacksIdx = args.findIndex((a) => isRpcCallbacks(a));
          if (callbacksIdx !== -1) {
            const cbs = args[callbacksIdx];
            const oneway = getCallbacksOneway(cbs);
            const otherArgs = args.filter((_, i) => i !== callbacksIdx);
            return client.callPullIteratorWithCallback(subject, cbs, oneway, ...otherArgs);
          }

          const callbackIdx = args.findIndex((a) => typeof a === 'function');
          if (callbackIdx !== -1) {
            const callback = args[callbackIdx];
            const otherArgs = args.filter((_, i) => i !== callbackIdx);
            return client.callWithCallback(subject, otherArgs, callback);
          }

          const isGenerator = prop.toLowerCase().includes('generate');

          if (isGenerator) {
            return client.callStreamWithProbe(subject, args, { probeNamespace: namespace, probeMethod: method, onMethods: updateCache });
          } else if (isPullIterator) {
            return client.callPullIterator(subject, ...args);
          } else {
            return callAndCache(subject, args, method);
          }
        },

        get(_target, nestedProp: string) {
          if (nestedProp === 'then' || nestedProp === 'catch' || nestedProp === 'finally') {
            // awaiting a nested path calls it without arguments
            const method = fullPath.join('.');
            const promise = (async () => {
              if (!client.isConnected && !client.isClosed) {
                await client.connect();
              }
              return callAndCache(`rpc.${namespace}.${method}`, [], method);
            })();
            // @ts-ignore
            return promise[nestedProp as keyof Promise<any>].bind(promise);
          }

          return createProxy<any>(client, namespace, fullPath, cache, onTiming, callOptions)[nestedProp];
        },
      });
    },
  }) as Promisify<T>;
}

export function createServiceProxy<T extends object>(client: RPCClient, selected: ServiceInfo, timeout?: number, path: string[] = []): Promisify<T> {
  const target = {} as T;

  return new Proxy(target, {
    get(target, prop: string) {
      if (prop === 'then' || prop === 'catch' || prop === 'finally') {
        return undefined;
      }

      if (typeof prop === 'symbol' || prop === 'toString') {
        return () => `[ServiceProxy ${selected.name}${path.length ? '.' + path.join('.') : ''}]`;
      }

      // methods added to the target, such as disconnect
      if (prop in target) {
        return target[prop as keyof T];
      }

      const fullPath = [...path, prop];
      const fullPathStr = fullPath.join('.');

      // the subject of this service only, then the exact path or last segment
      const own = serviceSubject(selected.name, fullPathStr);
      const endpoint =
        selected.endpoints.find((e: EndpointInfo) => e.subject === own) ??
        selected.endpoints.find((e: EndpointInfo) => {
          if (e.subject === fullPathStr) return true;
          const parts = e.subject.split('.');
          return parts[parts.length - 1] === prop && parts.slice(0, -1).join('.') === path.join('.');
        });
      const direct = selected.metadata?.[DIRECT_SUBJECT_METADATA] === '1' ? directSubject(selected.id, fullPathStr) : undefined;

      if (endpoint) {
        return new Proxy(function () {}, {
          apply(_target, _thisArg, args: any[]) {
            const isPullIterator = prop.toLowerCase().includes('pull');

            const callbacksIdx = args.findIndex((a) => isRpcCallbacks(a));
            if (callbacksIdx !== -1) {
              const cbs = args[callbacksIdx];
              const oneway = getCallbacksOneway(cbs);
              const otherArgs = args.filter((_, i) => i !== callbacksIdx);
              return client.callPullIteratorWithCallback(endpoint.subject, cbs, oneway, ...otherArgs);
            }

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
              return client.callService(endpoint.subject, args, direct);
            }
          },
        });
      }

      const prefix = fullPathStr + '.';
      const hasNested = selected.endpoints.some((e: EndpointInfo) => e.subject.startsWith(prefix));

      if (hasNested) {
        return createServiceProxy(client, selected, timeout, fullPath);
      }

      return undefined;
    },
  }) as Promisify<T>;
}

// 5-char random prefix per process plus a 4-char counter: the collision odds of 9 random chars, no Math.random() per call
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

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (!signal) return new Promise((resolve) => setTimeout(resolve, ms));
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const timer = setTimeout(done, ms);
    signal.addEventListener('abort', done, { once: true });
    function done(): void {
      clearTimeout(timer);
      signal!.removeEventListener('abort', done);
      resolve();
    }
  });
}

export function createSerialDispatcher(
  handler: (data: any) => void | Promise<void>,
  onError: (error: unknown) => void,
): { dispatch: (data: any) => void; flush: () => Promise<void> } {
  let chain: Promise<void> = Promise.resolve();
  let busy = 0;

  const dispatch = (data: any): void => {
    if (busy === 0) {
      // idle: run inline, a sync handler allocates no promise
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

export function createBoundedDispatcher(
  handler: (data: any) => void | Promise<void>,
  limit: number,
  isShared: (data: any) => boolean,
  onError: (error: unknown) => void,
): { dispatch: (data: any) => void; flush: () => Promise<void> } {
  const queue: ({ data: any; exclusive: boolean } | undefined)[] = [];
  let head = 0;
  let active = 0;
  let exclusiveActive = false;
  let pumping = false;
  let idle: { promise: Promise<void>; resolve: () => void } | undefined;

  const canStart = (exclusive: boolean): boolean => (exclusive ? active === 0 : active < limit && !exclusiveActive);

  const finish = (exclusive: boolean): void => {
    active--;
    if (exclusive) exclusiveActive = false;
    pump();
  };

  const start = (data: any, exclusive: boolean): void => {
    active++;
    if (exclusive) exclusiveActive = true;
    let result: void | Promise<void>;
    try {
      result = handler(data);
    } catch (error) {
      onError(error);
      finish(exclusive);
      return;
    }
    if (isPromise(result)) {
      result.then(
        () => finish(exclusive),
        (error) => {
          onError(error);
          finish(exclusive);
        },
      );
    } else {
      finish(exclusive);
    }
  };

  function pump(): void {
    // a sync handler re-enters through finish(), the loop below picks up the freed slot
    if (pumping) return;
    pumping = true;
    try {
      while (head < queue.length) {
        const next = queue[head]!;
        if (!canStart(next.exclusive)) break;
        queue[head++] = undefined;
        start(next.data, next.exclusive);
      }
    } finally {
      pumping = false;
    }
    if (head === queue.length) {
      queue.length = 0;
      head = 0;
    } else if (head > 1024 && head * 2 > queue.length) {
      queue.splice(0, head);
      head = 0;
    }
    if (idle && active === 0 && queue.length === 0) {
      idle.resolve();
      idle = undefined;
    }
  }

  const dispatch = (data: any): void => {
    const exclusive = !isShared(data);
    if (head === queue.length && !pumping && canStart(exclusive)) {
      start(data, exclusive);
      return;
    }
    queue.push({ data, exclusive });
  };

  const flush = (): Promise<void> => {
    if (active === 0 && head === queue.length) return Promise.resolve();
    if (!idle) {
      let resolve!: () => void;
      const promise = new Promise<void>((r) => {
        resolve = r;
      });
      idle = { promise, resolve };
    }
    return idle.promise;
  };

  return { dispatch, flush };
}
