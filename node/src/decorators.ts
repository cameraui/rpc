import 'reflect-metadata';

// Metadata keys
const RPC_METHODS_KEY = Symbol('rpc:methods');
const RPC_NESTED_KEY = Symbol('rpc:nested');
const RPC_EXPOSE_ALL_KEY = Symbol('rpc:exposeAll');

// Mark a method for RPC exposure
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export function RPCMethod(target: any, propertyKey: string, descriptor?: PropertyDescriptor) {
  const methods = Reflect.getMetadata(RPC_METHODS_KEY, target) ?? [];
  Reflect.defineMetadata(RPC_METHODS_KEY, [...methods, propertyKey], target);
}

// Mark a property containing nested RPC objects
export function RPCNested(target: any, propertyKey: string) {
  const nested = Reflect.getMetadata(RPC_NESTED_KEY, target) ?? [];
  Reflect.defineMetadata(RPC_NESTED_KEY, [...nested, propertyKey], target);
}

// Mark entire class for RPC exposure (all public methods)
export function RPCClass(constructor: Function) {
  Reflect.defineMetadata(RPC_EXPOSE_ALL_KEY, true, constructor.prototype);
}

// Mark a property for RPC exposure - exposes as direct property access
export function RPCProperty(target: any, propertyKey: string) {
  // Mark the property itself as an RPC method
  const methods = Reflect.getMetadata(RPC_METHODS_KEY, target) ?? [];
  Reflect.defineMetadata(RPC_METHODS_KEY, [...methods, propertyKey], target);

  // Also create a setter if needed
  const setterName = `set${propertyKey.charAt(0).toUpperCase()}${propertyKey.slice(1)}`;

  // Store original property value with a private key
  const privateKey = `_rpc_${propertyKey}`;

  // Replace property with getter/setter
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

  // Create a setter method
  target[setterName] = function (value: any) {
    this[propertyKey] = value;
  };

  // Mark setter as RPC method too
  Reflect.defineMetadata(RPC_METHODS_KEY, [...methods, propertyKey, setterName], target);
}

export function extractNestedMethodsWithoutDecorators(
  obj: any,
  path: string[] = [],
  handlers: Record<string, Function> = {},
  visited = new WeakSet(),
): Record<string, Function> {
  if (!obj || typeof obj !== 'object') return handlers;
  if (visited.has(obj)) return handlers;
  visited.add(obj);

  // Collect all property names from instance
  const instanceProps = Object.getOwnPropertyNames(obj);

  // Walk up the entire prototype chain to collect all methods
  const protoProps: string[] = [];
  let proto = Object.getPrototypeOf(obj);
  while (proto && proto !== Object.prototype) {
    protoProps.push(...Object.getOwnPropertyNames(proto));
    proto = Object.getPrototypeOf(proto);
  }

  // Combine and deduplicate
  const allProps = [...new Set([...instanceProps, ...protoProps])];

  for (const prop of allProps) {
    // Skip private, internal, and special properties
    if (prop === 'constructor' || prop.startsWith('_') || prop.startsWith('#')) {
      continue;
    }

    try {
      // Check if it's a static property on the constructor
      if (obj.constructor && Object.prototype.hasOwnProperty.call(obj.constructor, prop)) {
        continue; // Skip static properties
      }

      // Find descriptor in prototype chain
      let descriptor: PropertyDescriptor | undefined = Object.getOwnPropertyDescriptor(obj, prop);
      if (!descriptor) {
        let p = Object.getPrototypeOf(obj);
        while (p && p !== Object.prototype && !descriptor) {
          descriptor = Object.getOwnPropertyDescriptor(p, prop);
          p = Object.getPrototypeOf(p);
        }
      }

      // Skip getters/setters that are not simple value properties
      if (descriptor && (descriptor.get || descriptor.set) && !descriptor.value) {
        // It's a getter/setter - try to get the value
        const value = obj[prop];
        if (typeof value === 'function') {
          const fullPath = [...path, prop].join('.');
          handlers[fullPath] = value.bind(obj);
        }
        // Don't recurse into nested objects from getters
        continue;
      }

      const value = obj[prop];
      const fullPath = [...path, prop].join('.');

      if (typeof value === 'function') {
        // It's a method - bind and expose
        handlers[fullPath] = value.bind(obj);
      }
      // Don't recurse into nested objects for withoutDecorators mode
      // Only direct methods should be exposed, nested objects require explicit @RPCNested decorator
    } catch {
      // Ignore errors in property access
    }
  }

  return handlers;
}

export function extractNestedMethodsWithDecorators(
  obj: any,
  path: string[] = [],
  handlers: Record<string, Function> = {},
  visited = new WeakSet(),
): Record<string, Function> {
  if (visited.has(obj)) return handlers;
  visited.add(obj);

  // Check if it's a plain object
  const isPlainObject = obj.constructor === Object || obj.constructor === undefined;

  // Check for decorators
  const proto = Object.getPrototypeOf(obj);
  const hasDecorators =
    Reflect.hasMetadata(RPC_METHODS_KEY, obj) ||
    Reflect.hasMetadata(RPC_METHODS_KEY, proto) ||
    Reflect.hasMetadata(RPC_NESTED_KEY, obj) ||
    Reflect.hasMetadata(RPC_NESTED_KEY, proto) ||
    Reflect.hasMetadata(RPC_EXPOSE_ALL_KEY, proto);

  if (isPlainObject && !hasDecorators) {
    // Plain object mode - expose all functions
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
          // For non-function values (like strings, numbers), create a getter function
          handlers[fullPath] = async () => value;
        }
      } catch {
        // Ignore errors in property access
      }
    }
  } else {
    // Decorator mode
    const exposeAll = proto && Reflect.getMetadata(RPC_EXPOSE_ALL_KEY, proto);
    const rpcMethods = [...(Reflect.getMetadata(RPC_METHODS_KEY, obj) ?? []), ...((proto && Reflect.getMetadata(RPC_METHODS_KEY, proto)) ?? [])];
    const RPCNested = [...(Reflect.getMetadata(RPC_NESTED_KEY, obj) ?? []), ...((proto && Reflect.getMetadata(RPC_NESTED_KEY, proto)) ?? [])];

    // Get all properties
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

        // Check methods and properties
        if (typeof value === 'function') {
          if (exposeAll && !prop.startsWith('_')) {
            handlers[fullPath] = value.bind(obj);
          } else if (rpcMethods.includes(prop)) {
            handlers[fullPath] = value.bind(obj);
          }
        } else if (rpcMethods.includes(prop)) {
          // It's a property marked with @RPCProperty
          // Create a getter function
          handlers[fullPath] = async () => obj[prop];
        } else if (typeof value === 'object' && value !== null && RPCNested.includes(prop)) {
          // Check nested objects
          extractNestedMethodsWithDecorators(value, [...path, prop], handlers, visited);
        }
      } catch {
        // Ignore errors in property access
      }
    }
  }

  return handlers;
}
