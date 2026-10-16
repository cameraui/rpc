import { createError, localNoRespondersSubject, RPCException } from './errors.js';
import { ERROR_CODES } from './types.js';

import type { RPCClient } from './client.js';
import type { CallbackInvocation, CallbackMessage, OrphanProbeOptions, PullIteratorRequest, PullIteratorResponse, RPCError, StreamMessage } from './types.js';

const STREAM_SUBJECT_PREFIX = 'stream.';
const CALLBACK_SUBJECT_PREFIX = 'rpc.cb.';
const PULL_CALLBACK_SUBJECT_PREFIX = '_rpc.cb.';
// a push stream lets the event loop run after publishing for this long, so its cancel is read
const STREAM_YIELD_MS = 2;
const STOPPED = Symbol('stopped');
const ORPHAN_PROBE_INTERVAL_MS = 30_000;
const ORPHAN_PROBE_GRACE_MS = 300_000;

type Producer = AsyncGenerator<any, void, unknown>;

/** What a request sets up besides a plain call. */
export type SessionKind = 'stream' | 'pull' | 'pullCallback' | 'callback';

/** A registered method, as registerHandler and services find it on the handler object. */
export type MethodHandler = (...args: any[]) => unknown;

interface SessionStore {
  run<R>(signal: AbortSignal, fn: () => R): R;
  exit<R>(fn: () => R): R;
  getStore(): AbortSignal | undefined;
}

const sessionStore = createSessionStore();

const yieldToEventLoop: () => Promise<void> =
  typeof setImmediate === 'function' ? () => new Promise((resolve) => setImmediate(resolve)) : () => new Promise((resolve) => setTimeout(resolve, 0));

/**
 * The AbortSignal of the stream, pull-iterator, pull-callback or callback
 * session whose handler or generator runs right now. It aborts when the
 * session ends: the client cancels, the generator finishes or throws, a
 * value or frame does not fit the payload limit, a frame does not encode,
 * the handler is unregistered, the service stops, or the client
 * disconnects, suspends or loses its connection. A producer that waits for
 * data can hand it on and stop at once instead of at its next yield.
 *
 * Undefined outside a session (plain calls included), where
 * AsyncLocalStorage is not available (browsers), and where it runs on async
 * hooks (--no-async-context-frame), since that slows down every await of the
 * process.
 *
 * @example
 * async *frames(cameraId: string) {
 *   const signal = sessionSignal();
 *   while (true) yield await nextFrame(cameraId, { signal });
 * }
 */
export function sessionSignal(): AbortSignal | undefined {
  return sessionStore?.getStore();
}

/**
 * Run `fn` outside of any session, so a connection it opens does not carry
 * the session of the code that triggered it.
 */
export function outsideSession<R>(fn: () => R): R {
  return sessionStore ? sessionStore.exit(fn) : fn();
}

/**
 * The sessions of one handler registration or service. Its stop() reaches
 * every session, also one whose handler still sets up: that one ends with
 * CONNECTION_CLOSED once its handler returns.
 *
 * Internal: client.ts and service.ts use it.
 */
export class SessionOwner {
  private stopped = false;
  private readonly teardowns = new Set<() => Promise<void>>();

  /**
   * Add the teardown of a session. False once the owner stopped, the session
   * then ends on its own.
   */
  public add(teardown: () => Promise<void>): boolean {
    if (this.stopped) return false;
    this.teardowns.add(teardown);
    return true;
  }

  public delete(teardown: () => Promise<void>): void {
    this.teardowns.delete(teardown);
  }

  /**
   * End every session. Each stops publishing before this returns, the
   * promises settle once their generators are closed.
   */
  public stop(): Promise<void>[] {
    this.stopped = true;
    const teardowns = [...this.teardowns];
    this.teardowns.clear();
    return teardowns.map((teardown) => teardown());
  }
}

/** Orphan probe settings with the defaults applied. */
export interface ProbeSettings {
  intervalMs: number;
  graceMs: number;
}

/**
 * The settings of an orphanProbe option, undefined when it turns probing off.
 *
 * Internal: client.ts and service.ts use it.
 */
export function probeSettings(option: OrphanProbeOptions | false | undefined): ProbeSettings | undefined {
  if (option === false) return undefined;
  const intervalMs = option?.intervalMs;
  const graceMs = option?.graceMs;
  return {
    intervalMs: intervalMs !== undefined && intervalMs > 0 ? intervalMs : ORPHAN_PROBE_INTERVAL_MS,
    graceMs: graceMs !== undefined && graceMs > 0 ? graceMs : ORPHAN_PROBE_GRACE_MS,
  };
}

/**
 * Asks the server whether the consumer of a session still listens. At most
 * once per intervalMs a publish of the session carries a reply subject on
 * the client's reply inbox, so a subject nobody subscribes to comes back as a
 * no-responder status. When the session published nothing for intervalMs it
 * sends its idle probe instead. Once the probes have found no responders for
 * graceMs, `orphaned` runs; a probe that comes back clean resets that.
 */
class OrphanProbe {
  /** Publishes the idle probe with `reply`, false when it has nowhere to go */
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

  /** The reply subject for the next publish of the session, when a probe is due. */
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
    // the last probe had a whole interval to come back without responders and did not
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

  // a session that published within the interval gets its probe with the next publish
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

/**
 * Handle streaming request - common logic for client and service.
 *
 * The cancel listener exists before the handler runs. A cancel or a teardown
 * ends the session at once, also while the producer waits for its next item:
 * nothing is published after it except the CONNECTION_CLOSED error a teardown
 * sends, and the generator is closed without waiting for a step in flight.
 * A handler that fails ends the stream with an error message.
 *
 * @param owner - The registration or service the session belongs to
 */
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

/**
 * The session a request sets up, undefined for a plain call. Pull and
 * callback params come as an object or wrapped in the args array of call().
 *
 * Internal: client.ts and service.ts use it.
 */
export function sessionKind(params: any): SessionKind | undefined {
  if (params?.__stream && params?.__streamSubject) return 'stream';
  const first = Array.isArray(params) ? params[0] : undefined;
  if (params?.__pullIterator || first?.__pullIterator) return 'pull';
  if (params?.__pullCallback || first?.__pullCallback) return 'pullCallback';
  if (params && typeof params === 'object' && !Array.isArray(params) && params.__callback && params.__callbackSubject) return 'callback';
  if (first !== null && typeof first === 'object' && first.__callback) return 'callback';
  return undefined;
}

/**
 * Open the pull-iterator, pull-callback or callback session a request sets
 * up and return the result its setup call answers with.
 *
 * Internal: client.ts and service.ts use it.
 */
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

/**
 * Handle normal RPC call - common logic for client and service
 */
export async function handleNormalRPC(handler: Function, params: any): Promise<any> {
  return callNormalRPC(handler, params);
}

/**
 * Invoke the handler of a normal RPC call and return what it returns, a
 * promise or a plain value. Throws synchronously when the handler does.
 */
export function callNormalRPC(handler: Function, params: any): unknown {
  // Ensure params is an array
  const args = Array.isArray(params) ? params : params != null ? [params] : [];

  // Call the handler
  return handler(...args);
}

/**
 * Format exception as error object. A no-responder error this client saw
 * itself becomes INTERNAL_ERROR 'upstream unavailable' with the subject in
 * `data`: the caller's responder exists, only its own upstream is missing.
 */
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

/**
 * Handle pull-based iterator request
 */
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

/**
 * Handle a pull-iterator-with-callbacks request.
 *
 * Builds a callback proxy whose methods publish oneway to `callbackSubject`,
 * invokes the user handler with (...args, callbackProxy), then drives the
 * returned async generator from iterator `next`/`cancel` requests. The proxy
 * publishes nothing once the session ended. A frame bigger than a payload
 * limit too small for chunking ends the session: the batch it belongs to is
 * answered with PAYLOAD_TOO_LARGE. A frame that does not encode ends it the
 * same way with INTERNAL_ERROR; the producer is closed at its next yield
 * and its sessionSignal() aborts with the error.
 *
 * Once 2 ms passed since the last time, a proxy call returns a promise that
 * lets the event loop run. A producer that awaits its callbacks has its
 * cancel read during a batch, also when it waits for nothing else; one that
 * does not await them stops at its next wait for I/O or a timer.
 *
 * The iterator yields `undefined` — its sole purpose is client-driven flow
 * control. Meaningful data travels over the callback channel. The first
 * batch starts once the session is set up, before the consumer asks for it.
 */
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

  // Build callback proxy. Each method publishes a CallbackInvocation oneway.
  // Methods not in onewayMethods reject with NotImplemented (request-reply is v2).
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
          // per-frame hot path: small payloads publish without a promise,
          // oversized ones are chunked asynchronously
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

/**
 * Handle callback subscription request.
 * Creates a wrapper function, calls the handler with it,
 * and manages the subscription lifecycle. The handler sees the session's
 * signal through sessionSignal(), it aborts when the subscription ends.
 */
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

  // Handler cleanup must run exactly once — the cancel message and a later
  // registerHandler/disconnect cleanup would otherwise both invoke it. While
  // the handler sets up it only ends the session, the setup cleans up after it.
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

  // Create wrapper callback that publishes to callbackSubject
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

  // Call handler with args + wrapper callback
  try {
    const result = inSession(controller.signal, () => handler(...args, wrapperCallback));
    // Handler may return cleanup sync or async
    if (result && typeof result.then === 'function') {
      handlerCleanup = await result;
    } else if (typeof result === 'function') {
      handlerCleanup = result;
    }
  } catch (error) {
    void runHandlerCleanup();
    // Send error to subscriber
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
    probe.orphaned = () => void runHandlerCleanup();
    probe.start();
  }

  return runHandlerCleanup;
}

// the caller names this subject and the handler publishes there with its own credentials
function assertControlSubject(subject: unknown, prefix: string): void {
  if (typeof subject !== 'string' || subject.length <= prefix.length || !subject.startsWith(prefix)) {
    throw createError(ERROR_CODES.INVALID_PARAMS, `Control subject must start with '${prefix}'`);
  }
}

/**
 * Open a pull session, serve its `next` and `cancel` requests and return its
 * teardown. The request callback never waits, so a cancel is read while a
 * step runs: it answers 'done' at once and whatever the step yields is
 * dropped. Steps run one after another in request order; an eager session
 * starts its first step right after subscribing and answers the first `next`
 * with it, so the consumer waits one round trip less. The teardown sends
 * one CONNECTION_CLOSED error while connected and waits only for a generator
 * that is not in a step. It is tracked and subscribed before the handler
 * runs; a session torn down while its handler sets up fails with
 * CONNECTION_CLOSED, one cancelled meanwhile ends right after the setup.
 */
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

  // Natural end (done/cancel/error) and teardown: drop the request
  // subscription and leave the client and the owner. Without this, every
  // finished session leaves its `_rpc.iterator.*.request` subscription and
  // teardown behind until the whole client disconnects.
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
      void close();
    }

    try {
      // Sync fast path for small values; large values are chunked
      // asynchronously.
      const reply = response.type === 'value' ? probe?.reply() : undefined;
      const chunked = client.publishOrChunk(responseSubject, response, reply ? { reply } : undefined);
      if (chunked) await chunked;
    } catch (error) {
      // e.g. a value that does not encode
      if (response.type === 'value' && active) {
        finish();
        sendQuietly(client, responseSubject, { id: iteratorId, type: 'error', error: formatErrorObject(error) } satisfies PullIteratorResponse);
        void close();
      }
    }
  };

  const cancel = (): void => {
    finish();
    sendQuietly(client, responseSubject, { id: iteratorId, type: 'done' } satisfies PullIteratorResponse);
    void close();
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
    void close();
    throw tornDown ? connectionClosedException() : error;
  }

  // torn down while the handler set up
  if (!active) {
    unsub?.();
    void close();
    throw connectionClosedException();
  }
  if (cancelled) {
    cancel();
    return teardown;
  }

  if (eager && generator) early = advance(generator);
  if (probe) {
    // a pull consumer takes everything on its response subject for a step, so
    // the probe goes to its reply inbox, where every consumer drops a message without an id
    probe.idle = (reply) => {
      client.publishOrChunk(`rpc.reply.${iteratorId}`, { type: 'ping' }, { reply })?.catch(() => {});
      return true;
    };
    probe.orphaned = cancel;
    probe.start();
  }
  return teardown;
}

/**
 * Invoke the handler of a session inside it and coerce what it returns to an
 * async generator. A handler that fails ends the session.
 */
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
    // Sync generator/iterator - convert to async
    return (async function* () {
      for (const item of value) {
        yield item;
      }
    })();
  }
  throw createError(ERROR_CODES.INTERNAL_ERROR, `Handler must return a generator for ${kind}`);
}

// return() of an async generator runs only after a next() in flight settled, callers await it only between steps
async function closeGenerator(generator: Producer): Promise<void> {
  try {
    await generator.return?.();
  } catch {
    // ignore errors when closing the generator
  }
}

// the session ends whether or not this message goes out
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

// the client ends the session on disconnect, suspend and connection loss, the owner on unregister and stop;
// undefined once the owner stopped
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

// no static import of node:async_hooks, the browser build bundles this file. The async_hooks
// implementation (--no-async-context-frame) slows every await of the process once used
function createSessionStore(): SessionStore | undefined {
  const runtime = (globalThis as { process?: { getBuiltinModule?: (id: string) => unknown } }).process;
  const hooks = runtime?.getBuiltinModule?.('node:async_hooks') as { AsyncLocalStorage?: new () => SessionStore } | undefined;
  const Store = hooks?.AsyncLocalStorage;
  return Store && !('_propagate' in Store.prototype) ? new Store() : undefined;
}
