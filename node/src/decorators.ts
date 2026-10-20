import 'reflect-metadata';

import type { MethodHandler } from './handler.js';

const RPC_METHODS_KEY = Symbol('rpc:methods');
const RPC_NESTED_KEY = Symbol('rpc:nested');
const RPC_EXPOSE_ALL_KEY = Symbol('rpc:exposeAll');

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export function RPCMethod(target: any, propertyKey: string, descriptor?: PropertyDescriptor) {
  const methods = Reflect.getMetadata(RPC_METHODS_KEY, target) ?? [];
  Reflect.defineMetadata(RPC_METHODS_KEY, [...methods, propertyKey], target);
}

export function RPCNested(target: any, propertyKey: string) {
  const nested = Reflect.getMetadata(RPC_NESTED_KEY, target) ?? [];
  Reflect.defineMetadata(RPC_NESTED_KEY, [...nested, propertyKey], target);
}

export function RPCClass(constructor: Function) {
  Reflect.defineMetadata(RPC_EXPOSE_ALL_KEY, true, constructor.prototype);
}

export function RPCProperty(target: any, propertyKey: string) {
  const methods = Reflect.getMetadata(RPC_METHODS_KEY, target) ?? [];
  Reflect.defineMetadata(RPC_METHODS_KEY, [...methods, propertyKey], target);

  const setterName = `set${propertyKey.charAt(0).toUpperCase()}${propertyKey.slice(1)}`;

  const privateKey = `_rpc_${propertyKey}`;

  Object.defineProperty(target, propertyKey, {
    get() {
      return this[privateKey];
    },
    set(value) {
      this[privateKey] = value;
    },
    enumerable: true,
    configurable: true,
  });

  target[setterName] = function (value: any) {
    this[propertyKey] = value;
  };

  Reflect.defineMetadata(RPC_METHODS_KEY, [...methods, propertyKey, setterName], target);
}

export function extractNestedMethodsWithoutDecorators(
  obj: any,
  path: string[] = [],
  handlers: Record<string, MethodHandler> = {},
  visited = new WeakSet(),
): Record<string, MethodHandler> {
  if (!obj || typeof obj !== 'object') return handlers;
  if (visited.has(obj)) return handlers;
  visited.add(obj);

  const instanceProps = Object.getOwnPropertyNames(obj);

  const protoProps: string[] = [];
  let proto = Object.getPrototypeOf(obj);
  while (proto && proto !== Object.prototype) {
    protoProps.push(...Object.getOwnPropertyNames(proto));
    proto = Object.getPrototypeOf(proto);
  }

  const allProps = [...new Set([...instanceProps, ...protoProps])];

  for (const prop of allProps) {
    if (prop === 'constructor' || prop.startsWith('_') || prop.startsWith('#')) {
      continue;
    }

    try {
      if (obj.constructor && Object.prototype.hasOwnProperty.call(obj.constructor, prop)) {
        continue;
      }

      let descriptor: PropertyDescriptor | undefined = Object.getOwnPropertyDescriptor(obj, prop);
      if (!descriptor) {
        let p = Object.getPrototypeOf(obj);
        while (p && p !== Object.prototype && !descriptor) {
          descriptor = Object.getOwnPropertyDescriptor(p, prop);
          p = Object.getPrototypeOf(p);
        }
      }

      if (descriptor && (descriptor.get || descriptor.set) && !descriptor.value) {
        const value = obj[prop];
        if (typeof value === 'function') {
          const fullPath = [...path, prop].join('.');
          handlers[fullPath] = value.bind(obj);
        }
        continue;
      }

      const value = obj[prop];
      const fullPath = [...path, prop].join('.');

      if (typeof value === 'function') {
        handlers[fullPath] = value.bind(obj);
      }
      // no recursion in this mode, nested objects need @RPCNested
    } catch {
      // ignore
    }
  }

  return handlers;
}

export function extractNestedMethodsWithDecorators(
  obj: any,
  path: string[] = [],
  handlers: Record<string, MethodHandler> = {},
  visited = new WeakSet(),
): Record<string, MethodHandler> {
  if (visited.has(obj)) return handlers;
  visited.add(obj);

  const isPlainObject = obj.constructor === Object || obj.constructor === undefined;

  const proto = Object.getPrototypeOf(obj);
  const hasDecorators =
    Reflect.hasMetadata(RPC_METHODS_KEY, obj) ||
    Reflect.hasMetadata(RPC_METHODS_KEY, proto) ||
    Reflect.hasMetadata(RPC_NESTED_KEY, obj) ||
    Reflect.hasMetadata(RPC_NESTED_KEY, proto) ||
    Reflect.hasMetadata(RPC_EXPOSE_ALL_KEY, proto);

  if (isPlainObject && !hasDecorators) {
    const props = Object.getOwnPropertyNames(obj);

    for (const prop of props) {
      if (prop === 'constructor' || prop.startsWith('_')) continue;

      try {
        const value = obj[prop];
        const fullPath = [...path, prop].join('.');

        if (typeof value === 'function') {
          handlers[fullPath] = value.bind(obj);
        } else if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
          extractNestedMethodsWithDecorators(value, [...path, prop], handlers, visited);
        } else {
          handlers[fullPath] = async () => value;
        }
      } catch {
        // ignore
      }
    }
  } else {
    const exposeAll = proto && Reflect.getMetadata(RPC_EXPOSE_ALL_KEY, proto);
    const rpcMethods = [...(Reflect.getMetadata(RPC_METHODS_KEY, obj) ?? []), ...((proto && Reflect.getMetadata(RPC_METHODS_KEY, proto)) ?? [])];
    const RPCNested = [...(Reflect.getMetadata(RPC_NESTED_KEY, obj) ?? []), ...((proto && Reflect.getMetadata(RPC_NESTED_KEY, proto)) ?? [])];

    const props = Object.getOwnPropertyNames(obj);
    if (proto) {
      props.push(...Object.getOwnPropertyNames(proto).filter((p) => p !== 'constructor'));
    }

    for (const prop of props) {
      if (prop === 'constructor') continue;

      try {
        const value = obj[prop] ?? proto?.[prop];
        if (!value) continue;

        const fullPath = [...path, prop].join('.');

        if (typeof value === 'function') {
          if (exposeAll && !prop.startsWith('_')) {
            handlers[fullPath] = value.bind(obj);
          } else if (rpcMethods.includes(prop)) {
            handlers[fullPath] = value.bind(obj);
          }
        } else if (rpcMethods.includes(prop)) {
          // a property marked with @RPCProperty
          handlers[fullPath] = async () => obj[prop];
        } else if (typeof value === 'object' && value !== null && RPCNested.includes(prop)) {
          extractNestedMethodsWithDecorators(value, [...path, prop], handlers, visited);
        }
      } catch {
        // ignore
      }
    }
  }

  return handlers;
}
