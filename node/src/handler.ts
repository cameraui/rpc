import { createError, localNoRespondersSubject, RPCException } from './errors.js';
import { ERROR_CODES } from './types.js';

import type { RPCClient } from './client.js';
import type { CallbackInvocation, CallbackMessage, OrphanProbeOptions, PullIteratorRequest, PullIteratorResponse, RPCError, StreamMessage } from './types.js';

const STREAM_SUBJECT_PREFIX = 'stream.';
const CALLBACK_SUBJECT_PREFIX = 'rpc.cb.';
const PULL_CALLBACK_SUBJECT_PREFIX = '_rpc.cb.';
// a push stream yields to the event loop after publishing this long, so its cancel gets read
const STREAM_YIELD_MS = 2;
const STOPPED = Symbol('stopped');
const ORPHAN_PROBE_INTERVAL_MS = 30_000;
const ORPHAN_PROBE_GRACE_MS = 300_000;

type Producer = AsyncGenerator<any, void, unknown>;

export type SessionKind = 'stream' | 'pull' | 'pullCallback' | 'callback';

export type MethodHandler = (...args: any[]) => unknown;

interface SessionStore {
  run<R>(signal: AbortSignal, fn: () => R): R;
  exit<R>(fn: () => R): R;
  getStore(): AbortSignal | undefined;
}

const sessionStore = createSessionStore();

const yieldToEventLoop: () => Promise<void> =
  typeof setImmediate === 'function' ? () => new Promise((resolve) => setImmediate(resolve)) : () => new Promise((resolve) => setTimeout(resolve, 0));

export function sessionSignal(): AbortSignal | undefined {
  return sessionStore?.getStore();
}

export function outsideSession<R>(fn: () => R): R {
  return sessionStore ? sessionStore.exit(fn) : fn();
}

export class SessionOwner {
  private stopped = false;
  private readonly teardowns = new Set<() => Promise<void>>();

  public add(teardown: () => Promise<void>): boolean {
    if (this.stopped) return false;
    this.teardowns.add(teardown);
    return true;
  }

  public delete(teardown: () => Promise<void>): void {
    this.teardowns.delete(teardown);
  }

  public stop(): Promise<void>[] {
    this.stopped = true;
    const teardowns = [...this.teardowns];
    this.teardowns.clear();
    return teardowns.map((teardown) => teardown());
  }
}

export interface ProbeSettings {
  intervalMs: number;
  graceMs: number;
}

export function probeSettings(option: OrphanProbeOptions | false | undefined): ProbeSettings | undefined {
  if (option === false) return undefined;
  const intervalMs = option?.intervalMs;
  const graceMs = option?.graceMs;
  return {
    intervalMs: intervalMs !== undefined && intervalMs > 0 ? intervalMs : ORPHAN_PROBE_INTERVAL_MS,
    graceMs: graceMs !== undefined && graceMs > 0 ? graceMs : ORPHAN_PROBE_GRACE_MS,
  };
}

class OrphanProbe {
  public idle: (reply: string) => boolean = () => false;
  public orphaned: () => void = () => {};

  private lastProbe = performance.now();
  private lastPublish = this.lastProbe;
  private forget?: () => void;
  private noRespondersSince?: number;
  private timer?: ReturnType<typeof setTimeout>;
  private stopped = false;

  constructor(
    private readonly client: RPCClient,
    private readonly settings: ProbeSettings,
  ) {}

  public reply(): string | undefined {
    const now = performance.now();
    this.lastPublish = now;
    if (this.stopped || now - this.lastProbe < this.settings.intervalMs) return undefined;
    return this.open();
  }

  public start(): void {
    if (!this.stopped && !this.timer) this.arm();
  }

  public stop(): void {
    this.stopped = true;
    clearTimeout(this.timer);
    this.forget?.();
    this.forget = undefined;
  }

  private open(): string {
    // the last probe came back clean within a whole interval
    if (this.forget) {
      this.forget();
      this.noRespondersSince = undefined;
    }
    this.lastProbe = performance.now();
    const { reply, forget } = this.client.statusReply(() => this.noResponders());
    this.forget = forget;
    return reply;
  }

  private noResponders(): void {
    this.forget = undefined;
    const now = performance.now();
    this.noRespondersSince ??= now;
    if (this.stopped || now - this.noRespondersSince < this.settings.graceMs) return;
    this.stop();
    this.orphaned();
  }

  private arm(): void {
    const wait = Math.max(1, Math.max(this.lastProbe, this.lastPublish) + this.settings.intervalMs - performance.now());
    this.timer = setTimeout(() => this.tick(), wait);
    this.timer.unref?.();
  }

  private tick(): void {
    this.timer = undefined;
    if (this.stopped) return;
    const now = performance.now();
    const { intervalMs } = this.settings;
    if (this.client.isConnected && now - this.lastProbe >= intervalMs && now - this.lastPublish >= intervalMs) {
      const reply = this.open();
      let sent = false;
      try {
        sent = this.idle(reply);
      } catch {
        // not connected
      }
      if (!sent) {
        // nothing went out, so no answer can tell anything
        this.forget?.();
        this.forget = undefined;
      }
    }
    this.arm();
  }
}

export async function handleStreamRequest(
  handler: Function,
  args: any[],
  streamSubject: string,
  requestId: string,
  client: RPCClient,
  owner?: SessionOwner,
  probeOptions?: ProbeSettings,
): Promise<void> {
  assertControlSubject(streamSubject, STREAM_SUBJECT_PREFIX);

  const controller = new AbortController();
  const probe = probeOptions && new OrphanProbe(client, probeOptions);
  let active = true;
  let generator: Producer | undefined;
  let closing: Promise<void> | undefined;
  // set while generator.next() runs, ends that step early
  let wake: ((step: typeof STOPPED) => void) | undefined;

  const stop = (): void => {
    if (!active) return;
    active = false;
    controller.abort();
    probe?.stop();
    wake?.(STOPPED);
  };
  const close = (): Promise<void> => (generator ? (closing ??= closeGenerator(generator)) : Promise.resolve());
  const teardown = async (): Promise<void> => {
    if (!active) return;
    const midStep = wake !== undefined;
    stop();
    if (client.isConnected) {
      sendQuietly(client, streamSubject, { id: requestId, type: 'error', error: connectionClosedError() } satisfies StreamMessage);
    }
    if (!midStep) await close();
  };

  const publishItems = async (producer: Producer): Promise<boolean> => {
    let lastYield = performance.now();
    while (active) {
      let step: IteratorResult<unknown, void> | typeof STOPPED;
      try {
        step = await new Promise<IteratorResult<unknown, void> | typeof STOPPED>((resolve, reject) => {
          wake = resolve;
          producer.next().then(resolve, reject);
        });
      } finally {
        wake = undefined;
      }
      if (step === STOPPED) return true;
      if (!active || !client.isConnected) return false;

      if (step.done) {
        // ended first, a teardown right after must not send a second terminal message
        stop();
        sendQuietly(client, streamSubject, { id: requestId, type: 'end' } satisfies StreamMessage);
        return false;
      }
      const reply = probe?.reply();
      const chunked = client.publishOrChunk(streamSubject, { id: requestId, type: 'data', data: step.value } satisfies StreamMessage, reply ? { reply } : undefined);
      if (chunked) await chunked;

      if (performance.now() - lastYield >= STREAM_YIELD_MS) {
        await yieldToEventLoop();
        lastYield = performance.now();
      }
    }
    return false;
  };

  const cancelUnsub = await client.subscribe(`${streamSubject}.cancel`, stop);
  const untrack = track(client, owner, teardown);
  if (!untrack) await teardown();
  if (probe && active) {
    probe.idle = (reply) => {
      client.publishOrChunk(streamSubject, { id: requestId, type: 'ping' } satisfies StreamMessage, { reply })?.catch(() => {});
      return true;
    };
    probe.orphaned = stop;
    probe.start();
  }

  let midStep = false;
  try {
    if (active) {
      const producer = await openGenerator(handler, args, controller, 'stream');
      generator = producer;
      if (active) {
        midStep = await inSession(controller.signal, () => publishItems(producer));
      }
    }
  } catch (error) {
    if (active && client.isConnected) {
      sendQuietly(client, streamSubject, { id: requestId, type: 'error', error: formatErrorObject(error) } satisfies StreamMessage);
    }
  } finally {
    stop();
    cancelUnsub();
    untrack?.();
    const closed = close();
    if (!midStep) await closed;
  }
}

export function sessionKind(params: any): SessionKind | undefined {
  if (params?.__stream && params?.__streamSubject) return 'stream';
  const first = Array.isArray(params) ? params[0] : undefined;
  if (params?.__pullIterator || first?.__pullIterator) return 'pull';
  if (params?.__pullCallback || first?.__pullCallback) return 'pullCallback';
  if (params && typeof params === 'object' && !Array.isArray(params) && params.__callback && params.__callbackSubject) return 'callback';
  if (first !== null && typeof first === 'object' && first.__callback) return 'callback';
  return undefined;
}

export async function openSession(
  kind: Exclude<SessionKind, 'stream'>,
  handler: MethodHandler,
  params: any,
  requestId: string,
  client: RPCClient,
  owner?: SessionOwner,
  probe?: ProbeSettings,
): Promise<{ iteratorId: string } | { ok: true }> {
  if (kind === 'callback') {
    const setup = Array.isArray(params) && params[0]?.__callback ? params[0] : params;
    await handleCallbackRequest(handler, setup.args ?? [], setup.__callbackSubject, requestId, client, owner, probe);
    return { ok: true };
  }
  const setup = params?.__pullIterator || params?.__pullCallback ? params : params[0];
  const iteratorId: string = setup.__iteratorId ?? requestId;
  if (kind === 'pull') {
    await handlePullIteratorRequest(handler, setup.args ?? [], iteratorId, client, owner, probe);
  } else {
    const callbackMethods: string[] = setup.__callbackMethods ?? [];
    const onewayMethods: string[] = setup.__onewayMethods ?? [];
    await handlePullCallbackRequest(handler, setup.args ?? [], iteratorId, setup.__callbackSubject, callbackMethods, onewayMethods, client, owner, probe);
  }
  return { iteratorId };
}

export async function handleNormalRPC(handler: Function, params: any): Promise<any> {
  return callNormalRPC(handler, params);
}

export function callNormalRPC(handler: Function, params: any): unknown {
  const args = Array.isArray(params) ? params : params != null ? [params] : [];
  return handler(...args);
}

export function formatErrorObject(error: any): RPCError {
  const upstream = localNoRespondersSubject(error);
  if (upstream !== undefined) {
    return { code: ERROR_CODES.INTERNAL_ERROR, message: 'upstream unavailable', data: { subject: upstream } };
  }
  if (error instanceof RPCException) {
    return error.toJSON();
  } else {
    return {
      code: ERROR_CODES.INTERNAL_ERROR,
      message: error instanceof Error ? error.message : 'Internal error',
    };
  }
}

export async function handlePullIteratorRequest(
  handler: Function,
  args: any[],
  iteratorId: string,
  client: RPCClient,
  owner?: SessionOwner,
  probe?: ProbeSettings,
): Promise<() => Promise<void>> {
  const controller = new AbortController();
  return servePullSession(
    client,
    iteratorId,
    controller,
    owner,
    () => openGenerator(handler, args, controller, 'pull iterator'),
    (value) => ({ id: iteratorId, type: 'value', value }),
    probe && new OrphanProbe(client, probe),
  );
}

export async function handlePullCallbackRequest(
  handler: Function,
  args: any[],
  iteratorId: string,
  callbackSubject: string,
  callbackMethods: string[],
  onewayMethods: string[],
  client: RPCClient,
  owner?: SessionOwner,
  probeOptions?: ProbeSettings,
): Promise<() => Promise<void>> {
  assertControlSubject(callbackSubject, PULL_CALLBACK_SUBJECT_PREFIX);

  const controller = new AbortController();
  const { signal } = controller;
  const probe = probeOptions && new OrphanProbe(client, probeOptions);
  let lastYield = performance.now();

  const onewaySet = new Set(onewayMethods);
  const callbackProxy: Record<string, (...a: any[]) => Promise<void> | undefined> = {};

  for (const method of callbackMethods) {
    const isOneway = onewaySet.has(method);
    if (isOneway) {
      callbackProxy[method] = (...a: any[]) => {
        if (signal.aborted || !client.isConnected) return;
        const msg: CallbackInvocation = { method, args: a };
        try {
          const reply = probe?.reply();
          // per-frame hot path: a small payload publishes without a promise
          client.publishOrChunk(callbackSubject, msg, reply ? { reply } : undefined)?.catch((error: unknown) => {
            // a frame the payload limit cannot carry ends the session, other errors mean the connection is gone
            if (error instanceof RPCException && error.code === ERROR_CODES.PAYLOAD_TOO_LARGE) controller.abort(error);
          });
        } catch (error) {
          // a frame that does not encode ends the session too, a closed connection only drops it
          if (client.isConnected) controller.abort(RPCException.fromJSON(formatErrorObject(error)));
        }
        const now = performance.now();
        if (now - lastYield < STREAM_YIELD_MS) return;
        lastYield = now;
        return yieldToEventLoop();
      };
    } else {
      callbackProxy[method] = () => {
        throw createError(ERROR_CODES.INTERNAL_ERROR, `Request-reply callback '${method}' not supported in v1`);
      };
    }
  }

  return servePullSession(
    client,
    iteratorId,
    controller,
    owner,
    () => openGenerator(handler, [...args, callbackProxy], controller, 'pull-callback iterator'),
    // batch boundary, the protocol ignores the value
    () => ({ id: iteratorId, type: 'value' }),
    probe,
    true,
  );
}

export async function handleCallbackRequest(
  handler: Function,
  args: any[],
  callbackSubject: string,
  requestId: string,
  client: RPCClient,
  owner?: SessionOwner,
  probeOptions?: ProbeSettings,
): Promise<() => Promise<void>> {
  assertControlSubject(callbackSubject, CALLBACK_SUBJECT_PREFIX);

  const controller = new AbortController();
  const probe = probeOptions && new OrphanProbe(client, probeOptions);
  let handlerCleanup: (() => void | Promise<void>) | undefined;
  let cancelUnsub: (() => void) | undefined = undefined;
  let untrack: (() => void) | undefined = undefined;

  // runs once, the cancel message and a later unregister or disconnect would both invoke it
  let cleanedUp = false;
  const runHandlerCleanup = async (): Promise<void> => {
    if (cleanedUp) return;
    cleanedUp = true;
    controller.abort();
    probe?.stop();
    cancelUnsub?.();
    untrack?.();
    if (handlerCleanup) {
      await handlerCleanup();
    }
  };

  untrack = track(client, owner, runHandlerCleanup);
  if (!untrack) throw connectionClosedException();

  const wrapperCallback = async (value: any) => {
    if (!client.isConnected) return;
    const msg: CallbackMessage = {
      id: requestId,
      type: 'data',
      data: value,
    };
    const reply = probe?.reply();
    await client.publish(callbackSubject, msg, reply ? { reply } : undefined);
  };

  try {
    const result = inSession(controller.signal, () => handler(...args, wrapperCallback));
    if (result && typeof result.then === 'function') {
      handlerCleanup = await result;
    } else if (typeof result === 'function') {
      handlerCleanup = result;
    }
  } catch (error) {
    runHandlerCleanup();
    const errorMsg: CallbackMessage = {
      id: requestId,
      type: 'error',
      error: formatErrorObject(error),
    };
    await client.publish(callbackSubject, errorMsg);
    throw error;
  }

  // torn down while the handler set up
  if (cleanedUp) {
    if (handlerCleanup) await handlerCleanup();
    throw connectionClosedException();
  }

  try {
    cancelUnsub = await client.subscribe(`${callbackSubject}.cancel`, runHandlerCleanup);
  } catch (error) {
    await runHandlerCleanup();
    throw error;
  }
  if (cleanedUp) {
    cancelUnsub();
    throw connectionClosedException();
  }

  if (probe) {
    probe.idle = (reply) => {
      client.publishOrChunk(callbackSubject, { id: requestId, type: 'ping' } satisfies CallbackMessage, { reply })?.catch(() => {});
      return true;
    };
    probe.orphaned = () => {
      runHandlerCleanup();
    };
    probe.start();
  }

  return runHandlerCleanup;
}

function assertControlSubject(subject: unknown, prefix: string): void {
  // the caller names this subject and the handler publishes there with its own credentials
  if (typeof subject !== 'string' || subject.length <= prefix.length || !subject.startsWith(prefix)) {
    throw createError(ERROR_CODES.INVALID_PARAMS, `Control subject must start with '${prefix}'`);
  }
}

async function servePullSession(
  client: RPCClient,
  iteratorId: string,
  controller: AbortController,
  owner: SessionOwner | undefined,
  open: () => Promise<Producer>,
  valueResponse: (value: unknown) => PullIteratorResponse,
  probe?: OrphanProbe,
  eager = false,
): Promise<() => Promise<void>> {
  const requestSubject = `_rpc.iterator.${iteratorId}.request`;
  const responseSubject = `_rpc.iterator.${iteratorId}.response`;

  let active = true;
  let stepping = false;
  let generator: Producer | undefined;
  let steps: Promise<void> | undefined;
  // the first step of an eager session, started before the consumer asks for it
  let early: Promise<PullIteratorResponse> | undefined;
  let closing: Promise<void> | undefined;
  let unsub: (() => void) | undefined = undefined;
  let untrack: (() => void) | undefined = undefined;
  // read while the handler set up, the session ends once it is set up
  let cancelled = false;

  const close = (): Promise<void> => (generator ? (closing ??= closeGenerator(generator)) : Promise.resolve());

  // also on a natural end, or the request subscription stays until the client disconnects
  const finish = (): void => {
    if (!active) return;
    active = false;
    controller.abort();
    probe?.stop();
    unsub?.();
    untrack?.();
  };

  // a session aborted with an RPC error answers its step with it
  const failed = (): PullIteratorResponse | undefined => {
    const reason: unknown = controller.signal.reason;
    return reason instanceof RPCException ? { id: iteratorId, type: 'error', error: formatErrorObject(reason) } : undefined;
  };

  const advance = async (producer: Producer): Promise<PullIteratorResponse> => {
    // a frame that failed between steps, the producer does not run again
    const failure = failed();
    if (failure) return failure;
    stepping = true;
    try {
      const { value, done } = await inSession(controller.signal, () => producer.next());
      return failed() ?? (done ? { id: iteratorId, type: 'done' } : valueResponse(value));
    } catch (error) {
      return failed() ?? { id: iteratorId, type: 'error', error: formatErrorObject(error) };
    } finally {
      stepping = false;
    }
  };

  const step = async (): Promise<void> => {
    if (!active || !generator) return;
    const started = early;
    early = undefined;
    const response = await (started ?? advance(generator));
    // a cancel or teardown during the step has answered already
    if (!active) return;
    if (response.type !== 'value') {
      finish();
      // a session aborted with an RPC error leaves its producer at a yield
      close();
    }

    try {
      const reply = response.type === 'value' ? probe?.reply() : undefined;
      const chunked = client.publishOrChunk(responseSubject, response, reply ? { reply } : undefined);
      if (chunked) await chunked;
    } catch (error) {
      // e.g. a value that does not encode
      if (response.type === 'value' && active) {
        finish();
        sendQuietly(client, responseSubject, { id: iteratorId, type: 'error', error: formatErrorObject(error) } satisfies PullIteratorResponse);
        close();
      }
    }
  };

  const cancel = (): void => {
    finish();
    sendQuietly(client, responseSubject, { id: iteratorId, type: 'done' } satisfies PullIteratorResponse);
    close();
  };

  const onRequest = (msg: PullIteratorRequest): void => {
    if (!active) return;
    if (msg.type === 'cancel') {
      if (generator) cancel();
      else cancelled = true;
    } else if (msg.type === 'next') {
      steps = steps ? steps.then(step) : step();
    }
  };

  const teardown = async (): Promise<void> => {
    if (!active) return;
    const midStep = stepping;
    finish();
    if (client.isConnected) {
      sendQuietly(client, responseSubject, { id: iteratorId, type: 'error', error: connectionClosedError() } satisfies PullIteratorResponse);
    }
    const closed = close();
    if (!midStep) await closed;
  };

  untrack = track(client, owner, teardown);
  if (!untrack) throw connectionClosedException();

  try {
    // subscribed before the handler runs, so a cancel during its setup counts
    unsub = await client.subscribe(requestSubject, onRequest);
    if (active) generator = await open();
  } catch (error) {
    const tornDown = !active;
    finish();
    close();
    throw tornDown ? connectionClosedException() : error;
  }

  // torn down while the handler set up
  if (!active) {
    unsub?.();
    close();
    throw connectionClosedException();
  }
  if (cancelled) {
    cancel();
    return teardown;
  }

  if (eager && generator) early = advance(generator);
  if (probe) {
    // a ping on the response subject would answer the step, a consumer drops one without an id on its reply inbox
    probe.idle = (reply) => {
      client.publishOrChunk(`rpc.reply.${iteratorId}`, { type: 'ping' }, { reply })?.catch(() => {});
      return true;
    };
    probe.orphaned = cancel;
    probe.start();
  }
  return teardown;
}

async function openGenerator(handler: Function, args: any[], controller: AbortController, kind: string): Promise<Producer> {
  try {
    const result = inSession(controller.signal, () => handler(...args));
    return toProducer(result && typeof result.then === 'function' ? await result : result, kind);
  } catch (error) {
    controller.abort();
    throw error;
  }
}

function toProducer(value: any, kind: string): Producer {
  if (value && typeof value[Symbol.asyncIterator] === 'function') {
    // an async generator returns itself, a stream or another async iterable a new iterator
    return value[Symbol.asyncIterator]();
  }
  if (value && typeof value[Symbol.iterator] === 'function') {
    return (async function* () {
      for (const item of value) {
        yield item;
      }
    })();
  }
  throw createError(ERROR_CODES.INTERNAL_ERROR, `Handler must return a generator for ${kind}`);
}

async function closeGenerator(generator: Producer): Promise<void> {
  try {
    // waits for a next() in flight, callers await it only between steps
    await generator.return?.();
  } catch {
    // ignore
  }
}

function sendQuietly(client: RPCClient, subject: string, message: unknown): void {
  try {
    client.publishOrChunk(subject, message)?.catch(() => {});
  } catch {
    // not connected
  }
}

function connectionClosedError(): RPCError {
  return { code: ERROR_CODES.CONNECTION_CLOSED, message: 'Connection closed' };
}

function connectionClosedException(): RPCException {
  return createError(ERROR_CODES.CONNECTION_CLOSED, 'Connection closed');
}

function track(client: RPCClient, owner: SessionOwner | undefined, teardown: () => Promise<void>): (() => void) | undefined {
  if (owner && !owner.add(teardown)) return undefined;
  const untrack = client.trackSession(teardown);
  return () => {
    untrack();
    owner?.delete(teardown);
  };
}

function inSession<R>(signal: AbortSignal, fn: () => R): R {
  return sessionStore ? sessionStore.run(signal, fn) : fn();
}

function createSessionStore(): SessionStore | undefined {
  const runtime = (globalThis as { process?: { getBuiltinModule?: (id: string) => unknown } }).process;
  // no static import, the browser build bundles this file
  const hooks = runtime?.getBuiltinModule?.('node:async_hooks') as { AsyncLocalStorage?: new () => SessionStore } | undefined;
  const Store = hooks?.AsyncLocalStorage;
  // the async_hooks implementation (--no-async-context-frame) slows every await of the process
  return Store && !('_propagate' in Store.prototype) ? new Store() : undefined;
}
