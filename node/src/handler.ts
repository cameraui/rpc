import { createError, RPCException } from './errors.js';
import { ERROR_CODES } from './types.js';
import { sleep } from './utils.js';

import type { RPCClient } from './client.js';
import type { CallbackInvocation, CallbackMessage, PullIteratorRequest, PullIteratorResponse, RPCError, StreamMessage } from './types.js';

/**
 * Handle streaming request - common logic for client and service
 */
export async function handleStreamRequest(handler: Function, args: any[], streamSubject: string, requestId: string, client: RPCClient): Promise<void> {
  let generator: AsyncGenerator<any, void, unknown>;

  // Check if handler is async or sync
  const handlerResult = handler(...args);

  // Check if the handler returned a Promise (async function)
  if (handlerResult && typeof handlerResult.then === 'function') {
    // Await the async result
    const result = await handlerResult;
    // Check if result is async generator
    if (result && typeof result[Symbol.asyncIterator] === 'function') {
      generator = result;
    } else if (result && typeof result[Symbol.iterator] === 'function') {
      // Sync generator/iterator - convert to async
      generator = (async function* () {
        for (const value of result) {
          yield value;
        }
      })();
    } else {
      throw createError(ERROR_CODES.INTERNAL_ERROR, 'Handler must return a generator for stream');
    }
  } else if (handlerResult && typeof handlerResult[Symbol.asyncIterator] === 'function') {
    // Direct async generator
    generator = handlerResult;
  } else if (handlerResult && typeof handlerResult[Symbol.iterator] === 'function') {
    // Direct sync generator/iterator - convert to async
    generator = (async function* () {
      for (const value of handlerResult) {
        yield value;
      }
    })();
  } else {
    throw createError(ERROR_CODES.INTERNAL_ERROR, 'Handler must return a generator for stream');
  }

  // Listen for cancellation
  let cancelled = false;
  const cancelUnsub = await client.subscribe(`${streamSubject}.cancel`, () => {
    cancelled = true;
  });

  // Give client time to set up subscription
  await sleep(0);

  // Stream values
  try {
    for await (const value of generator) {
      if (cancelled || !client.isConnected) break;

      const streamMsg: StreamMessage = {
        id: requestId,
        type: 'data',
        data: value,
      };
      await client.publish(streamSubject, streamMsg);
    }

    if (!cancelled && client.isConnected) {
      const endMsg: StreamMessage = {
        id: requestId,
        type: 'end',
      };
      await client.publish(streamSubject, endMsg);
    }
  } catch (error) {
    if (!cancelled && client.isConnected) {
      try {
        const errorMsg: StreamMessage = {
          id: requestId,
          type: 'error',
          error: formatErrorObject(error),
        };
        await client.publish(streamSubject, errorMsg);
      } catch {
        // Ignore publish errors during disconnect
      }
    }
  } finally {
    cancelUnsub();
    // Ensure generator is closed
    if (generator.return) {
      try {
        await generator.return();
      } catch {
        // Ignore errors when closing generator
      }
    }
  }
}

/**
 * Handle normal RPC call - common logic for client and service
 */
export async function handleNormalRPC(handler: Function, params: any): Promise<any> {
  // Ensure params is an array
  const args = Array.isArray(params) ? params : params != null ? [params] : [];

  // Call the handler
  return handler(...args);
}

/**
 * Format exception as error object
 */
export function formatErrorObject(error: any): RPCError {
  if (error instanceof RPCException) {
    return error.toJSON();
  } else {
    return {
      code: ERROR_CODES.INTERNAL_ERROR,
      message: error instanceof Error ? error.message : 'Internal error',
    };
  }
}

/**
 * Handle pull-based iterator request
 */
export async function handlePullIteratorRequest(
  handler: Function,
  args: any[],
  iteratorId: string,
  client: RPCClient,
  onFinished?: () => void,
): Promise<() => Promise<void>> {
  let generator: AsyncGenerator<any, void, unknown>;

  // Check if handler is async or sync
  const handlerResult = handler(...args);

  // Check if the handler returned a Promise (async function)
  if (handlerResult && typeof handlerResult.then === 'function') {
    // Await the async result
    const result = await handlerResult;
    // Check if result is async generator
    if (result && typeof result[Symbol.asyncIterator] === 'function') {
      generator = result;
    } else if (result && typeof result[Symbol.iterator] === 'function') {
      // Sync generator/iterator - convert to async
      generator = (async function* () {
        for (const value of result) {
          yield value;
        }
      })();
    } else {
      throw createError(ERROR_CODES.INTERNAL_ERROR, 'Handler must return a generator for pull iterator');
    }
  } else if (handlerResult && typeof handlerResult[Symbol.asyncIterator] === 'function') {
    // Direct async generator
    generator = handlerResult;
  } else if (handlerResult && typeof handlerResult[Symbol.iterator] === 'function') {
    // Direct sync generator/iterator - convert to async
    generator = (async function* () {
      for (const value of handlerResult) {
        yield value;
      }
    })();
  } else {
    throw createError(ERROR_CODES.INTERNAL_ERROR, 'Handler must return a generator for pull iterator');
  }

  // Set up request/response subjects
  const requestSubject = `_rpc.iterator.${iteratorId}.request`;
  const responseSubject = `_rpc.iterator.${iteratorId}.response`;

  // Track if iterator is active
  let active = true;
  let subUnsub: (() => void) | undefined = undefined;

  // Natural end (done/cancel/error): drop the request subscription and let
  // the client remove its cleanup-map entry. Without this, every finished
  // session leaves its `_rpc.iterator.*.request` subscription and cleanup
  // closure behind until the whole client disconnects.
  const finish = (): void => {
    if (!active) return;
    active = false;
    subUnsub?.();
    onFinished?.();
  };

  // Subscribe to iterator requests
  const unsub = await client.subscribe(requestSubject, async (msg: PullIteratorRequest) => {
    if (!active) return;

    try {
      if (msg.type === 'cancel') {
        finish();
        // Close the generator explicitly
        if (generator.return) {
          await generator.return();
        }
        const response: PullIteratorResponse = {
          id: iteratorId,
          type: 'done',
        };
        await client.publish(responseSubject, response);
      } else if (msg.type === 'next') {
        const { value, done } = await generator.next();

        if (done) {
          finish();
          const response: PullIteratorResponse = {
            id: iteratorId,
            type: 'done',
          };
          await client.publish(responseSubject, response);
        } else {
          const response: PullIteratorResponse = {
            id: iteratorId,
            type: 'value',
            value,
          };
          await client.publish(responseSubject, response);
        }
      }
    } catch (error) {
      finish();
      const response: PullIteratorResponse = {
        id: iteratorId,
        type: 'error',
        error: formatErrorObject(error),
      };
      await client.publish(responseSubject, response);
    }
  });
  subUnsub = unsub;

  // Return cleanup function
  const cleanup = async () => {
    active = false;
    unsub();
    // Ensure generator is closed
    if (generator.return) {
      try {
        await generator.return();
      } catch {
        // Ignore errors when closing generator
      }
    }
  };

  return cleanup;
}

/**
 * Handle a pull-iterator-with-callbacks request.
 *
 * Builds a callback proxy whose methods publish oneway to `callbackSubject`,
 * invokes the user handler with (...args, callbackProxy), then drives the
 * returned async generator from iterator `next`/`cancel` requests.
 *
 * The iterator yields `undefined` — its sole purpose is client-driven flow
 * control. Meaningful data travels over the callback channel.
 */
export async function handlePullCallbackRequest(
  handler: Function,
  args: any[],
  iteratorId: string,
  callbackSubject: string,
  callbackMethods: string[],
  onewayMethods: string[],
  client: RPCClient,
  onFinished?: () => void,
): Promise<() => Promise<void>> {
  // Build callback proxy. Each method publishes a CallbackInvocation oneway.
  // Methods not in onewayMethods reject with NotImplemented (request-reply is v2).
  const onewaySet = new Set(onewayMethods);
  const callbackProxy: Record<string, (...a: any[]) => void> = {};
  let proxyActive = true;

  for (const method of callbackMethods) {
    const isOneway = onewaySet.has(method);
    if (isOneway) {
      callbackProxy[method] = (...a: any[]) => {
        if (!proxyActive || !client.isConnected) return;
        const msg: CallbackInvocation = { method, args: a };
        client.publish(callbackSubject, msg).catch(() => {
          // Swallow publish errors (disconnect mid-flight, client already gone).
        });
      };
    } else {
      callbackProxy[method] = () => {
        throw createError(ERROR_CODES.INTERNAL_ERROR, `Request-reply callback '${method}' not supported in v1`);
      };
    }
  }

  // Invoke the handler with callback proxy appended as last argument.
  let generator: AsyncGenerator<any, void, unknown>;
  const handlerResult = handler(...args, callbackProxy);

  const coerceToAsyncGenerator = (r: any): AsyncGenerator<any, void, unknown> => {
    if (r && typeof r[Symbol.asyncIterator] === 'function') return r;
    if (r && typeof r[Symbol.iterator] === 'function') {
      return (async function* () {
        for (const value of r) yield value;
      })();
    }
    throw createError(ERROR_CODES.INTERNAL_ERROR, 'Handler must return a generator for pull-callback iterator');
  };

  if (handlerResult && typeof handlerResult.then === 'function') {
    generator = coerceToAsyncGenerator(await handlerResult);
  } else {
    generator = coerceToAsyncGenerator(handlerResult);
  }

  // Iterator request/response subjects (shared with plain pull iterator).
  const requestSubject = `_rpc.iterator.${iteratorId}.request`;
  const responseSubject = `_rpc.iterator.${iteratorId}.response`;

  let active = true;
  let subUnsub: (() => void) | undefined = undefined;

  // Natural end: see handlePullIteratorRequest — drop the request
  // subscription and the client's cleanup-map entry per finished session.
  const finish = (): void => {
    if (!active) return;
    active = false;
    proxyActive = false;
    subUnsub?.();
    onFinished?.();
  };

  const unsub = await client.subscribe(requestSubject, async (msg: PullIteratorRequest) => {
    if (!active) return;

    try {
      if (msg.type === 'cancel') {
        finish();
        if (generator.return) {
          await generator.return();
        }
        const response: PullIteratorResponse = { id: iteratorId, type: 'done' };
        await client.publish(responseSubject, response);
      } else if (msg.type === 'next') {
        const { done } = await generator.next();

        if (done) {
          finish();
          const response: PullIteratorResponse = { id: iteratorId, type: 'done' };
          await client.publish(responseSubject, response);
        } else {
          // Batch boundary reached. Value is ignored by the protocol.
          const response: PullIteratorResponse = { id: iteratorId, type: 'value' };
          await client.publish(responseSubject, response);
        }
      }
    } catch (error) {
      finish();
      const response: PullIteratorResponse = {
        id: iteratorId,
        type: 'error',
        error: formatErrorObject(error),
      };
      await client.publish(responseSubject, response);
    }
  });
  subUnsub = unsub;

  const cleanup = async () => {
    active = false;
    proxyActive = false;
    unsub();
    if (generator.return) {
      try {
        await generator.return();
      } catch {
        // Ignore errors when closing generator
      }
    }
  };

  return cleanup;
}

/**
 * Handle callback subscription request.
 * Creates a wrapper function, calls the handler with it,
 * and manages the subscription lifecycle.
 */
export async function handleCallbackRequest(
  handler: Function,
  args: any[],
  callbackSubject: string,
  requestId: string,
  client: RPCClient,
  onFinished?: () => void,
): Promise<() => Promise<void>> {
  // Create wrapper callback that publishes to callbackSubject
  const wrapperCallback = async (value: any) => {
    if (!client.isConnected) return;
    const msg: CallbackMessage = {
      id: requestId,
      type: 'data',
      data: value,
    };
    await client.publish(callbackSubject, msg);
  };

  // Call handler with args + wrapper callback
  let handlerCleanup: (() => void | Promise<void>) | undefined;
  try {
    const result = handler(...args, wrapperCallback);
    // Handler may return cleanup sync or async
    if (result && typeof result.then === 'function') {
      handlerCleanup = await result;
    } else if (typeof result === 'function') {
      handlerCleanup = result;
    }
  } catch (error) {
    // Send error to subscriber
    const errorMsg: CallbackMessage = {
      id: requestId,
      type: 'error',
      error: formatErrorObject(error),
    };
    await client.publish(callbackSubject, errorMsg);
    throw error;
  }

  // Handler cleanup must run exactly once — the cancel message and a later
  // registerHandler/disconnect cleanup would otherwise both invoke it.
  let cleanedUp = false;
  const runHandlerCleanup = async (): Promise<void> => {
    if (cleanedUp) return;
    cleanedUp = true;
    if (handlerCleanup) {
      await handlerCleanup();
    }
  };

  // Subscribe to cancel subject
  let cancelUnsub: (() => void) | undefined = undefined;
  cancelUnsub = await client.subscribe(`${callbackSubject}.cancel`, async () => {
    cancelUnsub?.();
    onFinished?.();
    await runHandlerCleanup();
  });

  // Return combined cleanup
  return async () => {
    cancelUnsub?.();
    await runHandlerCleanup();
  };
}
