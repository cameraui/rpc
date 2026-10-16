import { AsyncLocalStorage } from 'node:async_hooks';
import { execFileSync } from 'node:child_process';
import { Readable } from 'node:stream';
import { setTimeout as sleep } from 'node:timers/promises';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { decode, encode } from '../src/codec.js';
import { handlePullCallbackRequest, handlePullIteratorRequest, handleStreamRequest, SessionOwner, sessionSignal } from '../src/handler.js';
import { createClient, drainMicrotasks, muxSub } from './mock-nc.js';

import type { NatsConnection } from '@nats-io/transport-node';

const STREAM = 'stream.rpc.ns.generateItems.s1';
const REQUEST = '_rpc.iterator.p1.request';
const RESPONSE = '_rpc.iterator.p1.response';
const CLOSED = { code: 'CONNECTION_CLOSED', message: 'Connection closed' };
// sessions carry a signal only where AsyncLocalStorage runs on AsyncContextFrame, not with --no-async-context-frame
const SIGNALS = !('_propagate' in AsyncLocalStorage.prototype);

function sessionClient() {
  const { client, nc } = createClient();
  // messages published to `subject`, decoded
  const sent = (subject: string) => nc.published.filter((p) => p.subject === subject).map((p) => decode(p.data));
  // hands a message to the live subscription of `subject`, through the client's dispatcher
  const deliver = (subject: string, message: unknown) => {
    nc.subs.find((sub) => sub.pattern === subject && !sub.closed)?.callback(null, { subject, data: encode(message) });
  };
  return { client, nc, sent, deliver };
}

function gate() {
  let open!: () => void;
  const opened = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { opened, open };
}

async function until(condition: () => boolean, ms = 1000): Promise<void> {
  const deadline = performance.now() + ms;
  while (!condition()) {
    if (performance.now() > deadline) throw new Error('condition not met in time');
    await sleep(1);
  }
}

describe('handleStreamRequest', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('sends the first item without waiting for a timer', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'setImmediate'] });
    const { client, sent } = sessionClient();
    async function* gen() {
      yield 'first';
      yield 'second';
    }

    const done = handleStreamRequest(gen, [], STREAM, 's1', client);
    await drainMicrotasks();
    expect(sent(STREAM)[0]).toEqual({ id: 's1', type: 'data', data: 'first' });

    await vi.runAllTimersAsync();
    await done;
    expect(sent(STREAM)).toEqual([
      { id: 's1', type: 'data', data: 'first' },
      { id: 's1', type: 'data', data: 'second' },
      { id: 's1', type: 'end' },
    ]);
  });

  it('subscribes the cancel listener before it invokes the handler', async () => {
    const { client, nc } = sessionClient();
    let listening: boolean | undefined;
    function handler() {
      listening = nc.subs.some((sub) => sub.pattern === `${STREAM}.cancel`);
      return [1];
    }

    await handleStreamRequest(handler, [], STREAM, 's1', client);
    expect(listening).toBe(true);
  });

  it('honours a cancel that arrives while an async handler sets up', async () => {
    const { client, sent, deliver } = sessionClient();
    const setup = gate();
    let finished = false;
    async function handler() {
      await setup.opened;
      return (async function* () {
        try {
          yield 1;
          yield 2;
        } finally {
          finished = true;
        }
      })();
    }

    const done = handleStreamRequest(handler, [], STREAM, 's1', client);
    await drainMicrotasks();
    deliver(`${STREAM}.cancel`, { id: 's1' });

    setup.open();
    await done;
    expect(sent(STREAM)).toEqual([]);
    expect(finished).toBe(false);
  });

  it.skipIf(!SIGNALS)('aborts the signal a handler sees while it sets up when the client cancels', async () => {
    const { client, deliver } = sessionClient();
    const setup = gate();
    let setupSignal: AbortSignal | undefined;
    async function handler() {
      setupSignal = sessionSignal();
      await setup.opened;
      return [1];
    }

    const done = handleStreamRequest(handler, [], STREAM, 's1', client);
    await drainMicrotasks();
    expect(setupSignal?.aborted).toBe(false);
    deliver(`${STREAM}.cancel`, { id: 's1' });
    expect(setupSignal?.aborted).toBe(true);
    setup.open();
    await done;
  });

  it.skipIf(!SIGNALS)('ends a parked stream at once on cancel and stops a producer that watches its signal', async () => {
    const { client, sent, deliver } = sessionClient();
    let finallyAt = 0;
    async function* handler() {
      const signal = sessionSignal()!;
      try {
        yield 'first';
        await sleep(5000, undefined, { signal });
        yield 'late';
      } finally {
        finallyAt = performance.now();
      }
    }

    const done = handleStreamRequest(handler, [], STREAM, 's1', client);
    await until(() => sent(STREAM).length === 1);
    const cancelledAt = performance.now();
    deliver(`${STREAM}.cancel`, { id: 's1' });
    await done;
    await until(() => finallyAt > 0);

    expect(finallyAt - cancelledAt).toBeLessThan(50);
    expect(sent(STREAM)).toEqual([{ id: 's1', type: 'data', data: 'first' }]);
  });

  it('returns on cancel without waiting for a producer that ignores its signal, and drops what it yields', async () => {
    const { client, sent, deliver } = sessionClient();
    let finallyAt = 0;
    async function* handler() {
      try {
        yield 'first';
        await sleep(150);
        yield 'late';
      } finally {
        finallyAt = performance.now();
      }
    }

    const done = handleStreamRequest(handler, [], STREAM, 's1', client);
    await until(() => sent(STREAM).length === 1);
    const cancelledAt = performance.now();
    deliver(`${STREAM}.cancel`, { id: 's1' });
    await done;
    expect(performance.now() - cancelledAt).toBeLessThan(50);

    await until(() => finallyAt > 0);
    expect(sent(STREAM)).toEqual([{ id: 's1', type: 'data', data: 'first' }]);
  });

  it('reads a cancel while a producer yields without ever waiting for a timer', async () => {
    const { client, nc, deliver } = sessionClient();
    const total = 100_000;
    async function* handler() {
      for (let i = 0; i < total; i++) yield i;
    }
    let atCancel = -1;
    setTimeout(() => {
      atCancel = nc.published.length;
      deliver(`${STREAM}.cancel`, { id: 's1' });
    }, 5);

    await handleStreamRequest(handler, [], STREAM, 's1', client);
    expect(atCancel).toBeGreaterThan(0);
    expect(nc.published.length).toBe(atCancel);
    expect(nc.published.length).toBeLessThan(total);
  });

  it('sends one CONNECTION_CLOSED error on teardown without waiting for the producer', async () => {
    const { client, sent } = sessionClient();
    const owner = new SessionOwner();
    const parked = gate();
    let finished = false;
    async function* handler() {
      try {
        yield 'first';
        await parked.opened;
        yield 'late';
      } finally {
        finished = true;
      }
    }

    const done = handleStreamRequest(handler, [], STREAM, 's1', client, owner);
    await until(() => sent(STREAM).length === 1);
    expect((client as any).sessionTeardowns.size).toBe(1);

    const startedAt = performance.now();
    const teardowns = owner.stop();
    expect(teardowns).toHaveLength(1);
    await Promise.all(teardowns);
    expect(performance.now() - startedAt).toBeLessThan(50);
    await done;
    expect((client as any).sessionTeardowns.size).toBe(0);

    parked.open();
    await until(() => finished);
    expect(sent(STREAM)).toEqual([
      { id: 's1', type: 'data', data: 'first' },
      { id: 's1', type: 'error', error: CLOSED },
    ]);
  });

  it('sends nothing after the end when a teardown follows it at once', async () => {
    const { client, nc, sent } = sessionClient();
    const owner = new SessionOwner();
    const publish = nc.publish.bind(nc);
    nc.publish = (subject, data, opts) => {
      publish(subject, data, opts);
      if (subject === STREAM && (decode(data) as { type: string }).type === 'end') queueMicrotask(() => void Promise.all(owner.stop()));
    };
    async function* handler() {
      yield 1;
    }

    await handleStreamRequest(handler, [], STREAM, 's1', client, owner);
    await drainMicrotasks();
    expect(sent(STREAM)).toEqual([
      { id: 's1', type: 'data', data: 1 },
      { id: 's1', type: 'end' },
    ]);
  });

  class OnlyIterable {
    async *[Symbol.asyncIterator]() {
      yield 'a';
      yield 'b';
    }
  }

  it.each([
    ['a Readable', () => Readable.from(['a', 'b'])],
    [
      'a web ReadableStream',
      () =>
        new ReadableStream({
          start(controller) {
            controller.enqueue('a');
            controller.enqueue('b');
            controller.close();
          },
        }),
    ],
    ['an object with only [Symbol.asyncIterator]', () => new OnlyIterable()],
  ])('streams %s, an async iterable that is not its own iterator', async (_name, handler) => {
    const { client, sent } = sessionClient();
    await handleStreamRequest(handler, [], STREAM, 's1', client);
    expect(sent(STREAM)).toEqual([
      { id: 's1', type: 'data', data: 'a' },
      { id: 's1', type: 'data', data: 'b' },
      { id: 's1', type: 'end' },
    ]);
  });

  it.each([
    [
      'throws while it sets up',
      async () => {
        await Promise.resolve();
        throw new Error('camera offline');
      },
      'camera offline',
    ],
    ['returns no generator', () => 42, 'Handler must return a generator for stream'],
  ])('ends the stream with an error when the handler %s', async (_name, handler, message) => {
    const { client, nc, sent } = sessionClient();
    await handleStreamRequest(handler, [], STREAM, 's1', client);
    expect(sent(STREAM)).toEqual([{ id: 's1', type: 'error', error: { code: 'INTERNAL_ERROR', message } }]);
    expect(nc.subs.filter((sub) => !sub.closed)).toEqual([]);
    expect((client as any).sessionTeardowns.size).toBe(0);
  });
});

describe('pull sessions', () => {
  it('answers a cancel at once while a next waits on a slow producer and drops its value', async () => {
    const { client, sent, deliver } = sessionClient();
    const slow = gate();
    let finished = false;
    async function* handler() {
      try {
        yield 'first';
        await slow.opened;
        yield 'late';
      } finally {
        finished = true;
      }
    }

    await handlePullIteratorRequest(handler, [], 'p1', client);
    deliver(REQUEST, { id: 'p1', type: 'next' });
    await until(() => sent(RESPONSE).length === 1);
    deliver(REQUEST, { id: 'p1', type: 'next' });
    await drainMicrotasks();
    deliver(REQUEST, { id: 'p1', type: 'cancel' });
    expect(sent(RESPONSE)).toEqual([
      { id: 'p1', type: 'value', value: 'first' },
      { id: 'p1', type: 'done' },
    ]);

    slow.open();
    await until(() => finished);
    await drainMicrotasks();
    expect(sent(RESPONSE)).toHaveLength(2);
  });

  it.skipIf(!SIGNALS)('stops a producer that watches its signal as soon as the client cancels', async () => {
    const { client, sent, deliver } = sessionClient();
    let finallyAt = 0;
    async function* handler() {
      try {
        yield 'first';
        await sleep(5000, undefined, { signal: sessionSignal() });
        yield 'late';
      } finally {
        finallyAt = performance.now();
      }
    }

    await handlePullIteratorRequest(handler, [], 'p1', client);
    deliver(REQUEST, { id: 'p1', type: 'next' });
    await until(() => sent(RESPONSE).length === 1);
    deliver(REQUEST, { id: 'p1', type: 'next' });
    const cancelledAt = performance.now();
    deliver(REQUEST, { id: 'p1', type: 'cancel' });
    await until(() => finallyAt > 0);

    expect(finallyAt - cancelledAt).toBeLessThan(50);
    expect(sent(RESPONSE).map((r: any) => r.type)).toEqual(['value', 'done']);
  });

  it('sends one CONNECTION_CLOSED error on teardown without waiting for a step in flight', async () => {
    const { client, sent, deliver } = sessionClient();
    const slow = gate();
    async function* handler() {
      await slow.opened;
      yield 'late';
    }

    const teardown = await handlePullIteratorRequest(handler, [], 'p1', client);
    deliver(REQUEST, { id: 'p1', type: 'next' });
    await drainMicrotasks();

    const ended = await Promise.race([teardown().then(() => true), sleep(100).then(() => false)]);
    expect(ended).toBe(true);
    expect(sent(RESPONSE)).toEqual([{ id: 'p1', type: 'error', error: CLOSED }]);

    slow.open();
    await sleep(5);
    expect(sent(RESPONSE)).toHaveLength(1);
  });

  it('publishes no callback of a running batch once the client cancels', async () => {
    const { client, sent, deliver } = sessionClient();
    const callbacks = '_rpc.cb.p1';
    async function* handler(cb: { onFrame: (i: number) => void }) {
      for (let batch = 0; batch < 3; batch++) {
        for (let frame = 0; frame < 30; frame++) {
          await sleep(2);
          cb.onFrame(frame);
        }
        yield;
      }
    }

    await handlePullCallbackRequest(handler, [], 'p1', callbacks, ['onFrame'], ['onFrame'], client);
    deliver(REQUEST, { id: 'p1', type: 'next' });
    await until(() => sent(callbacks).length >= 3);
    deliver(REQUEST, { id: 'p1', type: 'cancel' });
    const atCancel = sent(callbacks).length;
    expect(sent(RESPONSE)).toEqual([{ id: 'p1', type: 'done' }]);

    await sleep(30);
    expect(sent(callbacks)).toHaveLength(atCancel);
    expect(sent(RESPONSE)).toHaveLength(1);
  });

  it('reads a cancel during a batch that never waits for a timer when the producer awaits its callbacks', async () => {
    const { client, nc, sent, deliver } = sessionClient();
    const callbacks = '_rpc.cb.p1';
    const frames = 200_000;
    const published = () => nc.published.filter((p) => p.subject === callbacks).length;
    async function* handler(cb: { onFrame: (i: number) => Promise<void> | undefined }) {
      for (;;) {
        for (let i = 0; i < frames; i++) await cb.onFrame(i);
        yield;
      }
    }

    await handlePullCallbackRequest(handler, [], 'p1', callbacks, ['onFrame'], ['onFrame'], client);
    let atCancel = -1;
    setTimeout(() => {
      atCancel = published();
      deliver(REQUEST, { id: 'p1', type: 'cancel' });
    }, 5);
    deliver(REQUEST, { id: 'p1', type: 'next' });
    await until(() => atCancel >= 0);

    expect(atCancel).toBeLessThan(frames);
    expect(sent(RESPONSE)).toEqual([{ id: 'p1', type: 'done' }]);
    await sleep(5);
    expect(published()).toBe(atCancel);
  });

  it('serves an async iterable that is not its own iterator', async () => {
    const { client, sent, deliver } = sessionClient();
    await handlePullIteratorRequest(() => Readable.from(['a']), [], 'p1', client);
    deliver(REQUEST, { id: 'p1', type: 'next' });
    await until(() => sent(RESPONSE).length === 1);
    deliver(REQUEST, { id: 'p1', type: 'next' });
    await until(() => sent(RESPONSE).length === 2);
    expect(sent(RESPONSE)).toEqual([
      { id: 'p1', type: 'value', value: 'a' },
      { id: 'p1', type: 'done' },
    ]);
  });
});

describe('sessionSignal()', () => {
  afterEach(() => {
    vi.resetModules();
  });

  it.skipIf(!SIGNALS)('is undefined outside a session and in plain calls, and aborts when a session ends', async () => {
    const { client, sent, deliver } = sessionClient();
    expect(sessionSignal()).toBeUndefined();

    let inPlainCall: AbortSignal | null = null;
    let inGenerator: AbortSignal | undefined;
    await client.registerHandler('ns', {
      plain: () => {
        inPlainCall = sessionSignal() ?? null;
        return 1;
      },
      async *pullItems() {
        inGenerator = sessionSignal();
        yield 1;
      },
    });
    deliver('rpc.ns.plain', { id: 'c1', method: 'call', params: [] });
    expect(inPlainCall).toBeNull();

    deliver('rpc.ns.pullItems', { id: 'p1', method: 'call', params: [{ __pullIterator: true, __iteratorId: 'p1', args: [] }] });
    await until(() => sent('rpc.reply.p1').length === 1);
    deliver(REQUEST, { id: 'p1', type: 'next' });
    await until(() => sent(RESPONSE).length === 1);
    expect(inGenerator?.aborted).toBe(false);
    deliver(REQUEST, { id: 'p1', type: 'next' });
    await until(() => sent(RESPONSE).length === 2);
    expect(inGenerator?.aborted).toBe(true);
  });

  it.skipIf(!SIGNALS)('is the signal of a callback subscription and aborts when the client cancels', async () => {
    const { client, sent, deliver } = sessionClient();
    let seen: AbortSignal | undefined;
    await client.registerHandler('ns', {
      watch(_cb: (value: unknown) => void) {
        seen = sessionSignal();
        return () => {};
      },
    });

    deliver('rpc.ns.watch', { id: 'c1', method: 'call', params: [{ __callback: true, __callbackSubject: 'rpc.cb.c1', args: [] }] });
    await until(() => sent('rpc.reply.c1').length === 1);
    expect(seen?.aborted).toBe(false);
    deliver('rpc.cb.c1.cancel', { id: 'c1' });
    expect(seen?.aborted).toBe(true);
  });

  it('is undefined and streams still work where AsyncLocalStorage is missing, as in browsers', async () => {
    const getBuiltinModule = process.getBuiltinModule;
    (process as { getBuiltinModule?: unknown }).getBuiltinModule = undefined;
    let handler: typeof import('../src/handler.js');
    try {
      vi.resetModules();
      handler = await import('../src/handler.js');
    } finally {
      process.getBuiltinModule = getBuiltinModule;
    }

    const { client, sent } = sessionClient();
    let seen: AbortSignal | null = null;
    async function* gen() {
      seen = handler.sessionSignal() ?? null;
      yield 'only';
    }
    await handler.handleStreamRequest(gen, [], STREAM, 's1', client);
    expect(seen).toBeNull();
    expect(sent(STREAM)).toEqual([
      { id: 's1', type: 'data', data: 'only' },
      { id: 's1', type: 'end' },
    ]);
  });

  it('leaves an AsyncLocalStorage that runs on async hooks unused', async () => {
    const run = vi.fn();
    class HookStore {
      run = run;
      _propagate(): void {}
    }
    const getBuiltinModule = process.getBuiltinModule;
    process.getBuiltinModule = ((id: string) => (id === 'node:async_hooks' ? { AsyncLocalStorage: HookStore } : getBuiltinModule(id))) as typeof getBuiltinModule;
    let handler: typeof import('../src/handler.js');
    try {
      vi.resetModules();
      handler = await import('../src/handler.js');
    } finally {
      process.getBuiltinModule = getBuiltinModule;
    }

    const { client, sent } = sessionClient();
    await handler.handleStreamRequest(() => ['only'], [], STREAM, 's1', client);
    expect(run).not.toHaveBeenCalled();
    expect(sent(STREAM)).toHaveLength(2);
  });

  // the store check relies on this: a Node release that drops _propagate from the async_hooks
  // implementation, or drops that implementation, must fail here
  it('tells the async_hooks AsyncLocalStorage from the AsyncContextFrame one by _propagate', () => {
    const major = Number(process.versions.node.split('.')[0]);
    const probe = "process.stdout.write(String('_propagate' in require('node:async_hooks').AsyncLocalStorage.prototype))";
    const env = { ...process.env, NODE_OPTIONS: '' };
    const asyncHooks = major >= 24 ? ['--no-async-context-frame'] : [];
    const contextFrame = major >= 24 ? [] : ['--experimental-async-context-frame'];
    expect(execFileSync(process.execPath, [...asyncHooks, '-e', probe], { env }).toString()).toBe('true');
    expect(execFileSync(process.execPath, [...contextFrame, '-e', probe], { env }).toString()).toBe('false');
  });
});

describe('session teardown', () => {
  async function openSessions() {
    const ctx = sessionClient();
    const parked = gate();
    const finished: string[] = [];
    const unregister = await ctx.client.registerHandler('ns', {
      async *generateItems() {
        try {
          yield 'a';
          await parked.opened;
          yield 'late';
        } finally {
          finished.push('stream');
        }
      },
      async *pullItems() {
        try {
          yield 'a';
          await parked.opened;
          yield 'late';
        } finally {
          finished.push('pull');
        }
      },
    });
    ctx.deliver('rpc.ns.generateItems', { id: 's1', method: 'stream', params: { __stream: true, __streamSubject: STREAM, args: [] } });
    ctx.deliver('rpc.ns.pullItems', { id: 'p1', method: 'call', params: [{ __pullIterator: true, __iteratorId: 'p1', args: [] }] });
    await until(() => ctx.sent(STREAM).length === 1 && ctx.sent('rpc.reply.p1').length === 1);
    ctx.deliver(REQUEST, { id: 'p1', type: 'next' });
    await until(() => ctx.sent(RESPONSE).length === 1);
    ctx.deliver(REQUEST, { id: 'p1', type: 'next' });
    await drainMicrotasks();
    return { ...ctx, parked, finished, unregister };
  }

  function expectClosedSessions(ctx: Awaited<ReturnType<typeof openSessions>>) {
    expect(ctx.sent(STREAM)).toEqual([
      { id: 's1', type: 'data', data: 'a' },
      { id: 's1', type: 'error', error: CLOSED },
    ]);
    expect(ctx.sent(RESPONSE)).toEqual([
      { id: 'p1', type: 'value', value: 'a' },
      { id: 'p1', type: 'error', error: CLOSED },
    ]);
  }

  it('unregister ends open push and pull sessions with CONNECTION_CLOSED and does not wait for their producers', async () => {
    const ctx = await openSessions();
    const startedAt = performance.now();
    const ended = await Promise.race([ctx.unregister().then(() => true), sleep(200).then(() => false)]);
    expect(ended).toBe(true);
    expect(performance.now() - startedAt).toBeLessThan(50);
    expectClosedSessions(ctx);

    ctx.parked.open();
    await until(() => ctx.finished.length === 2);
    await drainMicrotasks();
    expectClosedSessions(ctx);
  });

  it.each(['disconnect', 'suspend'] as const)('%s() ends every session before it closes the connection', async (method) => {
    const ctx = await openSessions();
    let publishedAtClose = -1;
    const close = ctx.nc.close;
    ctx.nc.close = () => {
      publishedAtClose = ctx.nc.published.length;
      return close();
    };

    const ended = await Promise.race([ctx.client[method]().then(() => true), sleep(500).then(() => false)]);
    expect(ended).toBe(true);
    expectClosedSessions(ctx);
    expect(publishedAtClose).toBe(ctx.nc.published.length);

    ctx.parked.open();
    await until(() => ctx.finished.length === 2);
  });

  it('unregister ends the sessions whose handler still sets up, and they fail with CONNECTION_CLOSED once it returns', async () => {
    const { client, nc, sent, deliver } = sessionClient();
    const setup = gate();
    const signals: (AbortSignal | undefined)[] = [];
    const cleanups: string[] = [];
    let started = 0;
    async function settingUp<T>(result: T): Promise<T> {
      signals.push(sessionSignal());
      await setup.opened;
      return result;
    }
    async function* producer() {
      started++;
      yield 'never';
    }
    const unregister = await client.registerHandler('ns', {
      generateItems: () => settingUp(producer()),
      pullItems: () => settingUp(producer()),
      pullFrames: (_cb: unknown) => settingUp(producer()),
      watch: (_cb: unknown) => settingUp(() => void cleanups.push('watch')),
    });
    deliver('rpc.ns.generateItems', { id: 's1', method: 'stream', params: { __stream: true, __streamSubject: STREAM, args: [] } });
    deliver('rpc.ns.pullItems', { id: 'p1', method: 'call', params: [{ __pullIterator: true, __iteratorId: 'p1', args: [] }] });
    const pullCallback = {
      __pullCallback: true,
      __iteratorId: 'p2',
      __callbackSubject: '_rpc.cb.p2',
      __callbackMethods: ['onFrame'],
      __onewayMethods: ['onFrame'],
      args: [],
    };
    deliver('rpc.ns.pullFrames', { id: 'p2', method: 'call', params: [pullCallback] });
    deliver('rpc.ns.watch', { id: 'c1', method: 'call', params: [{ __callback: true, __callbackSubject: 'rpc.cb.c1', args: [] }] });
    await drainMicrotasks();
    expect(signals).toHaveLength(4);

    await unregister();
    if (SIGNALS) expect(signals.map((signal) => signal?.aborted)).toEqual([true, true, true, true]);
    setup.open();
    await until(() => ['p1', 'p2', 'c1'].every((id) => sent(`rpc.reply.${id}`).length === 1));
    await drainMicrotasks();

    for (const id of ['p1', 'p2', 'c1']) expect(sent(`rpc.reply.${id}`)).toMatchObject([{ id, error: CLOSED }]);
    expect(sent(STREAM)).toEqual([{ id: 's1', type: 'error', error: CLOSED }]);
    expect(cleanups).toEqual(['watch']);
    expect(started).toBe(0);
    expect(nc.subs.filter((sub) => !sub.closed).map((sub) => sub.pattern)).toEqual([]);
    expect((client as any).sessionTeardowns.size).toBe(0);
  });

  it('service stop ends open stream and pull sessions, also one still setting up, and does not wait for their producers', async () => {
    const { client, nc } = createClient();
    Object.assign(nc, { addCloseListener() {}, removeCloseListener() {} });
    // the service manager awaits sub.closed as a promise, so a live subscription is told by gone
    nc.subscribe = ((pattern: string, opts: { callback: (err: Error | null, msg: any) => void }) => {
      const sub = {
        pattern,
        callback: opts?.callback,
        gone: false,
        closed: new Promise(() => {}),
        drain: () => Promise.resolve(),
        unsubscribe() {
          sub.gone = true;
        },
        isClosed: () => sub.gone,
      };
      nc.subs.push(sub as any);
      return sub as any;
    }) as any;
    client.service.init(nc as unknown as NatsConnection);
    const sent = (subject: string) => nc.published.filter((p) => p.subject === subject).map((p) => decode(p.data));
    const live = (pattern: string) => nc.subs.find((sub) => sub.pattern === pattern && !(sub as any).gone);

    const parked = gate();
    const setup = gate();
    const finished: string[] = [];
    async function* parkedProducer(kind: string) {
      try {
        yield 'a';
        await parked.opened;
        yield 'late';
      } finally {
        finished.push(kind);
      }
    }
    const service = await client.service.registerHandler(
      { name: 'svc', version: '1.0.0' },
      {
        gen: () => parkedProducer('stream'),
        pull: () => parkedProducer('pull'),
        async pullSlow() {
          await setup.opened;
          return parkedProducer('slow');
        },
      },
    );
    const request = (subject: string, payload: unknown) =>
      void live(subject)!.callback(null, { subject, data: encode(payload), headers: undefined, respond() {}, respondError() {} });
    request('gen', { id: 's1', method: 'stream', params: { __stream: true, __streamSubject: 'stream.svc.gen.s1', args: [] } });
    request('pull', { id: 'p1', method: 'call', params: [{ __pullIterator: true, __iteratorId: 'p1', args: [] }] });
    request('pullSlow', { id: 'p2', method: 'call', params: [{ __pullIterator: true, __iteratorId: 'p2', args: [] }] });
    await until(() => sent('pull.reply.p1').length === 1 && sent('stream.svc.gen.s1').length === 1);
    live(REQUEST)!.callback(null, { subject: REQUEST, data: encode({ id: 'p1', type: 'next' }) });
    await until(() => sent(RESPONSE).length === 1);
    live(REQUEST)!.callback(null, { subject: REQUEST, data: encode({ id: 'p1', type: 'next' }) });
    await drainMicrotasks();

    const startedAt = performance.now();
    await service.stop();
    expect(performance.now() - startedAt).toBeLessThan(50);
    expect(sent('stream.svc.gen.s1')).toEqual([
      { id: 's1', type: 'data', data: 'a' },
      { id: 's1', type: 'error', error: CLOSED },
    ]);
    expect(sent(RESPONSE)).toEqual([
      { id: 'p1', type: 'value', value: 'a' },
      { id: 'p1', type: 'error', error: CLOSED },
    ]);
    expect((client as any).sessionTeardowns.size).toBe(0);

    setup.open();
    await until(() => sent('pullSlow.reply.p2').length === 1);
    expect(sent('pullSlow.reply.p2')).toMatchObject([{ id: 'p2', error: CLOSED }]);
    expect(live('_rpc.iterator.p2.request')).toBeUndefined();

    parked.open();
    await until(() => finished.length === 2);
    expect(finished.sort()).toEqual(['pull', 'stream']);
  });
});

describe('pull-callback batches', () => {
  async function* batches(callbacks: { onFrame: (frame: string) => void }) {
    started.push('started');
    callbacks.onFrame('a');
    yield;
    callbacks.onFrame('b');
    yield;
  }
  const started: string[] = [];

  afterEach(() => {
    started.length = 0;
  });

  it('starts the first batch before the first next and answers that next with it', async () => {
    const { client, sent, deliver } = sessionClient();

    await handlePullCallbackRequest(batches, [], 'p1', '_rpc.cb.p1', ['onFrame'], ['onFrame'], client);
    await drainMicrotasks();
    expect(started).toEqual(['started']);
    expect(sent('_rpc.cb.p1')).toEqual([{ method: 'onFrame', args: ['a'] }]);
    expect(sent(RESPONSE)).toEqual([]);

    deliver(REQUEST, { id: 'p1', type: 'next' });
    await until(() => sent(RESPONSE).length === 1);
    expect(sent(RESPONSE)).toEqual([{ id: 'p1', type: 'value' }]);
    expect(started).toEqual(['started']);
  });

  it('answers a cancel before the first next with one done and drops the early batch', async () => {
    const { client, sent, deliver } = sessionClient();

    await handlePullCallbackRequest(batches, [], 'p1', '_rpc.cb.p1', ['onFrame'], ['onFrame'], client);
    deliver(REQUEST, { id: 'p1', type: 'cancel' });
    deliver(REQUEST, { id: 'p1', type: 'next' });
    await sleep(10);
    expect(sent(RESPONSE)).toEqual([{ id: 'p1', type: 'done' }]);
  });

  it('answers the first next with the error of an early batch that failed, without an unhandled rejection', async () => {
    const { client, sent, deliver } = sessionClient();
    const unhandled = vi.fn();
    process.on('unhandledRejection', unhandled);
    async function* failing() {
      throw new Error('no recording');
    }

    try {
      await handlePullCallbackRequest(failing, [], 'p1', '_rpc.cb.p1', [], [], client);
      await sleep(10);
      deliver(REQUEST, { id: 'p1', type: 'next' });
      await until(() => sent(RESPONSE).length === 1);
      expect(sent(RESPONSE)).toEqual([{ id: 'p1', type: 'error', error: { code: 'INTERNAL_ERROR', message: 'no recording' } }]);
      expect(unhandled).not.toHaveBeenCalled();
    } finally {
      process.off('unhandledRejection', unhandled);
    }
  });

  it.each([
    ['a bigint over 64 bits', 2n ** 70n, 'too large to fit in MessagePack'],
    ['a symbol', Symbol('frame'), 'Unknown type: symbol'],
  ])('ends the session with the error of a frame that does not encode, %s', async (_kind, bad, message) => {
    const { client, sent, deliver } = sessionClient();
    let closed = false;
    async function* handler(callbacks: { onFrame: (frame: unknown) => void }) {
      try {
        for (let batch = 0; batch < 3; batch++) {
          callbacks.onFrame(`small ${batch}`);
          callbacks.onFrame(bad);
          yield;
        }
      } finally {
        closed = true;
      }
    }

    await handlePullCallbackRequest(handler, [], 'p1', '_rpc.cb.p1', ['onFrame'], ['onFrame'], client);
    deliver(REQUEST, { id: 'p1', type: 'next' });
    await until(() => sent(RESPONSE).length === 1);
    await until(() => closed);
    deliver(REQUEST, { id: 'p1', type: 'next' });
    await sleep(10);

    expect(sent(RESPONSE)).toEqual([{ id: 'p1', type: 'error', error: { code: 'INTERNAL_ERROR', message: expect.stringContaining(message) } }]);
    expect(sent('_rpc.cb.p1')).toEqual([{ method: 'onFrame', args: ['small 0'] }]);
  });

  it('answers the next step with the error of a frame that failed between steps, without running the producer', async () => {
    const { client, nc, sent, deliver } = sessionClient();
    const batches: number[] = [];
    let closed = false;
    let late!: (frame: unknown) => void;
    async function* handler(callbacks: { onFrame: (frame: unknown) => void }) {
      late = callbacks.onFrame;
      try {
        for (let batch = 0; batch < 3; batch++) {
          batches.push(batch);
          yield;
        }
      } finally {
        closed = true;
      }
    }

    await handlePullCallbackRequest(handler, [], 'p1', '_rpc.cb.p1', ['onFrame'], ['onFrame'], client);
    deliver(REQUEST, { id: 'p1', type: 'next' });
    await until(() => sent(RESPONSE).length === 1);
    late(Symbol('late'));
    deliver(REQUEST, { id: 'p1', type: 'next' });
    await until(() => sent(RESPONSE).length === 2);
    await until(() => closed);

    expect(sent(RESPONSE)).toEqual([
      { id: 'p1', type: 'value' },
      { id: 'p1', type: 'error', error: { code: 'INTERNAL_ERROR', message: 'Unknown type: symbol' } },
    ]);
    expect(batches).toEqual([0]);
    expect(sent('_rpc.cb.p1')).toEqual([]);
    expect(nc.subs.find((sub) => sub.pattern === REQUEST)?.closed).toBe(true);
  });

  it.skipIf(!SIGNALS)('aborts the signal of a producer whose frame does not encode with the error', async () => {
    const { client, deliver } = sessionClient();
    let signal: AbortSignal | undefined;
    async function* handler(callbacks: { onFrame: (frame: unknown) => void }) {
      signal = sessionSignal();
      callbacks.onFrame(Symbol('frame'));
      yield;
    }

    await handlePullCallbackRequest(handler, [], 'p1', '_rpc.cb.p1', ['onFrame'], ['onFrame'], client);
    deliver(REQUEST, { id: 'p1', type: 'next' });
    await until(() => signal?.aborted === true);
    expect(signal?.reason).toMatchObject({ code: 'INTERNAL_ERROR', message: 'Unknown type: symbol' });
  });

  // a pull-callback iterator past its setup, with the subscriptions of its session
  async function openedIterator({ client, nc, sent }: ReturnType<typeof sessionClient>, iter: AsyncGenerator<void>) {
    const first = iter.next();
    await until(() => sent('rpc.ns.play').length === 1);
    const init = sent('rpc.ns.play')[0];
    const iteratorId: string = init.params[0].__iteratorId;
    muxSub(client, nc).callback(null, { subject: `rpc.reply.${init.id}`, data: encode({ id: init.id, result: { iteratorId } }) });
    await until(() => sent(`_rpc.iterator.${iteratorId}.request`).length === 1);
    // kept after the iterator unsubscribes: a frame may already be on its way
    const callbacks = nc.subs.find((sub) => sub.pattern === `_rpc.cb.${iteratorId}`)!;
    const responses = nc.subs.find((sub) => sub.pattern === `_rpc.iterator.${iteratorId}.response`)!;
    const frame = (frame: string) => callbacks.callback(null, { data: encode({ method: 'onFrame', args: [frame] }) });
    const boundary = () => responses.callback(null, { data: encode({ id: iteratorId, type: 'value' }) });
    return { first, iteratorId, frame, boundary };
  }

  it('returns without waiting for a running callback, which finishes, and drops the queued ones', async () => {
    const session = sessionClient();
    const { client } = session;
    const { opened, open } = gate();
    const begun: string[] = [];
    const seen: string[] = [];
    const iter = client.callPullIteratorWithCallback(
      'rpc.ns.play',
      {
        onFrame: async (frame: string) => {
          begun.push(frame);
          if (frame === 'b2') await opened;
          seen.push(frame);
        },
      },
      ['onFrame'],
    );

    const { first, frame, boundary } = await openedIterator(session, iter);
    frame('b1');
    boundary();
    // the producer runs ahead and sends frames of the next batch
    frame('b2');
    frame('b3');
    await first;
    await until(() => begun.includes('b2'));

    const result = await Promise.race([iter.return(undefined), sleep(100).then(() => 'pending')]);
    expect(result).toEqual({ value: undefined, done: true });
    open();
    frame('b4');
    await sleep(5);
    expect(begun).toEqual(['b1', 'b2']);
    expect(seen).toEqual(['b1', 'b2']);
  });

  it('ends the iteration when a callback awaits return()', async () => {
    const session = sessionClient();
    const { client, sent } = session;
    let returnedInside = false;
    const iter: AsyncGenerator<void> = client.callPullIteratorWithCallback(
      'rpc.ns.play',
      {
        onFrame: async (frame: string) => {
          if (frame !== 'stop') return;
          await iter.return(undefined);
          returnedInside = true;
        },
      },
      ['onFrame'],
    );

    const { first, iteratorId, frame, boundary } = await openedIterator(session, iter);
    frame('stop');
    boundary();
    const result = await Promise.race([first, sleep(100).then(() => 'pending')]);
    expect(result).not.toBe('pending');
    expect(returnedInside).toBe(true);
    await expect(iter.next()).resolves.toEqual({ value: undefined, done: true });
    expect(sent(`_rpc.iterator.${iteratorId}.request`)).toContainEqual({ id: iteratorId, type: 'cancel' });
  });

  it('lets the consumer go on once the callbacks before a boundary ran, not those sent after it', async () => {
    const { client, nc, sent } = sessionClient();
    let release!: () => void;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const seen: string[] = [];
    const iter = client.callPullIteratorWithCallback(
      'rpc.ns.play',
      {
        onFrame: async (frame: string) => {
          if (frame === 'b2') await held;
          seen.push(frame);
        },
      },
      ['onFrame'],
    );

    const first = iter.next();
    await until(() => sent('rpc.ns.play').length === 1);
    const init = sent('rpc.ns.play')[0];
    const iteratorId: string = init.params[0].__iteratorId;
    muxSub(client, nc).callback(null, { subject: `rpc.reply.${init.id}`, data: encode({ id: init.id, result: { iteratorId } }) });
    await until(() => sent(`_rpc.iterator.${iteratorId}.request`).length === 1);
    const live = (pattern: string) => nc.subs.find((sub) => sub.pattern === pattern && !sub.closed)!;

    live(`_rpc.cb.${iteratorId}`).callback(null, { data: encode({ method: 'onFrame', args: ['b1'] }) });
    live(`_rpc.iterator.${iteratorId}.response`).callback(null, { data: encode({ id: iteratorId, type: 'value' }) });
    live(`_rpc.cb.${iteratorId}`).callback(null, { data: encode({ method: 'onFrame', args: ['b2'] }) });

    const result = await Promise.race([first, sleep(100).then(() => 'pending')]);
    expect(result).toEqual({ value: undefined, done: false });
    expect(seen).toEqual(['b1']);

    release();
    live(`_rpc.iterator.${iteratorId}.response`).callback(null, { data: encode({ id: iteratorId, type: 'done' }) });
    await expect(iter.next()).resolves.toEqual({ value: undefined, done: true });
    expect(seen).toEqual(['b1', 'b2']);
  });
});
