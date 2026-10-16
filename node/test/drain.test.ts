import { connect } from '@nats-io/transport-node';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { RPCClient } from '../src/client.js';
import { decode, encode } from '../src/codec.js';
import { sleep } from '../src/utils.js';
import { createClient, createMockNc, drainMicrotasks } from './mock-nc.js';

import type { NatsConnection } from '@nats-io/transport-node';
import type { RPCResponse, StreamMessage } from '../src/types.js';

vi.mock('@nats-io/transport-node', async (importOriginal) => ({ ...(await importOriginal<typeof import('@nats-io/transport-node')>()), connect: vi.fn() }));

type Ctx = ReturnType<typeof createClient>;

// like nats.js: a subscription that is gone on the client drops what still arrives
function deliver({ nc }: Ctx, method: string, id: string, params: unknown = []): boolean {
  const sub = nc.subs.findLast((s) => s.pattern === `rpc.ns.${method}`);
  if (!sub || sub.isClosed()) return false;
  sub.callback(null, { subject: `rpc.ns.${method}`, data: encode({ id, method: 'call', params }) });
  return true;
}

function replies({ nc }: Ctx): RPCResponse[] {
  return nc.published.filter((p) => p.subject.startsWith('rpc.reply.')).map((p) => decode(p.data) as RPCResponse);
}

async function connected(options: Partial<ConstructorParameters<typeof RPCClient>[0]>): Promise<Ctx> {
  const nc = createMockNc();
  vi.mocked(connect).mockResolvedValueOnce(nc as unknown as NatsConnection);
  const client = new RPCClient({ servers: ['nats://127.0.0.1:4222'], name: 'drain', noResponderRetry: { maxRetries: 0, delays: [1] }, ...options });
  await client.connect();
  return { client, nc };
}

function pullCallbackParams(iteratorId: string) {
  return {
    __pullCallback: true,
    __iteratorId: iteratorId,
    __callbackSubject: `_rpc.cb.${iteratorId}`,
    __callbackMethods: ['onFrame'],
    __onewayMethods: ['onFrame'],
    args: [],
  };
}

function slow(ms: number, log?: string[]) {
  return async (id: string) => {
    log?.push(`start ${id}`);
    await sleep(ms);
    log?.push(`end ${id}`);
    return `done ${id}`;
  };
}

describe('handler drain on cleanup, disconnect() and suspend()', () => {
  afterEach(() => {
    vi.mocked(connect).mockReset();
  });

  it('cleanup drains the subscriptions and dispatches what arrives before the PONG', async () => {
    const ctx = createClient();
    let pong!: () => void;
    ctx.nc.onDrain = () =>
      new Promise<void>((resolve) => {
        pong = resolve;
      });
    const cleanup = await ctx.client.registerHandler('ns', { echo: (v: string) => v });

    const done = cleanup();
    await drainMicrotasks();
    expect(ctx.nc.subs.find((s) => s.pattern === 'rpc.ns.echo')!.ended).toBe('drain');
    // on the wire before the UNSUB reached the server
    expect(deliver(ctx, 'echo', 'a.1', ['late'])).toBe(true);
    pong();
    await done;

    expect(replies(ctx)).toEqual([{ id: 'a.1', result: 'late' }]);
    expect(deliver(ctx, 'echo', 'a.2', ['gone'])).toBe(false);
  });

  it('disconnect() answers the running and the queued call before it closes', async () => {
    const ctx = createClient();
    await ctx.client.registerHandler('ns', { work: slow(20) });
    ctx.nc.calls.length = 0;
    deliver(ctx, 'work', 'a.1', ['a.1']);
    deliver(ctx, 'work', 'a.2', ['a.2']);

    const started = Date.now();
    await ctx.client.disconnect();

    expect(replies(ctx)).toEqual([
      { id: 'a.1', result: 'done a.1' },
      { id: 'a.2', result: 'done a.2' },
    ]);
    expect(Date.now() - started).toBeLessThan(200);
    expect(ctx.nc.calls).toEqual(['drain rpc.ns.work', 'flush', 'close']);
  });

  it('answers what is unanswered at the deadline once with CONNECTION_CLOSED and does not start it', async () => {
    const ctx = createClient({ drainTimeoutMs: 40 });
    const log: string[] = [];
    await ctx.client.registerHandler('ns', { work: slow(150, log) });
    deliver(ctx, 'work', 'a.1', ['a.1']);
    deliver(ctx, 'work', 'a.2', ['a.2']);

    const started = Date.now();
    await ctx.client.suspend();
    expect(Date.now() - started).toBeLessThan(140);
    const closed = { code: 'CONNECTION_CLOSED', message: 'Connection closed' };
    expect(replies(ctx)).toEqual([
      { id: 'a.1', error: closed },
      { id: 'a.2', error: closed },
    ]);

    await sleep(300);
    expect(log).toEqual(['start a.1', 'end a.1']);
    expect(replies(ctx).length).toBe(2);
  });

  it('keeps the registration usable after suspend() and connect()', async () => {
    const ctx = createClient({ drainTimeoutMs: 40 });
    await ctx.client.registerHandler('ns', { echo: (v: string) => v });
    await ctx.client.suspend();

    const next = createMockNc();
    vi.mocked(connect).mockResolvedValueOnce(next as unknown as NatsConnection);
    await ctx.client.connect();
    const sub = next.subs.find((s) => s.pattern === 'rpc.ns.echo')!;
    sub.callback(null, { subject: 'rpc.ns.echo', data: encode({ id: 'b.1', method: 'call', params: ['again'] }) });
    expect(next.published.map((p) => decode(p.data))).toEqual([{ id: 'b.1', result: 'again' }]);
  });

  it('answers a stream request that never started on its stream subject', async () => {
    const ctx = createClient({ drainTimeoutMs: 30 });
    async function* items() {
      yield 1;
    }
    await ctx.client.registerHandler('ns', { work: slow(150), items });
    deliver(ctx, 'work', 'a.1', ['a.1']);
    // queued behind the running call of the same method
    ctx.nc.subs
      .find((s) => s.pattern === 'rpc.ns.work')!
      .callback(null, {
        subject: 'rpc.ns.work',
        data: encode({ id: 's.1', method: 'stream', params: { __stream: true, __streamSubject: 'stream.rpc.ns.work.s1', args: [] } }),
      });

    await ctx.client.disconnect();
    const frames = ctx.nc.published.filter((p) => p.subject === 'stream.rpc.ns.work.s1').map((p) => decode(p.data) as StreamMessage);
    expect(frames).toEqual([{ id: 's.1', type: 'error', error: { code: 'CONNECTION_CLOSED', message: 'Connection closed' } }]);
  });

  it('ends a running stream session at once, as before', async () => {
    const ctx = createClient();
    async function* items() {
      yield 1;
      await sleep(1000);
      yield 2;
    }
    await ctx.client.registerHandler('ns', { items, work: slow(100) });
    ctx.nc.subs
      .find((s) => s.pattern === 'rpc.ns.items')!
      .callback(null, {
        subject: 'rpc.ns.items',
        data: encode({ id: 's.1', method: 'stream', params: { __stream: true, __streamSubject: 'stream.rpc.ns.items.s1', args: [] } }),
      });
    await vi.waitFor(() => expect(ctx.nc.published.length).toBe(1));
    deliver(ctx, 'work', 'a.1', ['a.1']);

    const done = ctx.client.disconnect();
    await drainMicrotasks();
    const frames = ctx.nc.published.filter((p) => p.subject === 'stream.rpc.ns.items.s1').map((p) => decode(p.data) as StreamMessage);
    expect(frames.at(-1)).toEqual({ id: 's.1', type: 'error', error: { code: 'CONNECTION_CLOSED', message: 'Connection closed' } });
    expect(replies(ctx)).toEqual([]);
    await done;
    expect(replies(ctx)).toEqual([{ id: 'a.1', result: 'done a.1' }]);
  });

  it('lets a draining handler still make calls', async () => {
    const ctx = createClient();
    await ctx.client.registerHandler('ns', {
      relay: async () => ctx.client.call('rpc.up.value'),
    });
    deliver(ctx, 'relay', 'a.1');
    await drainMicrotasks();

    const done = ctx.client.disconnect();
    await vi.waitFor(() => expect(ctx.nc.published.some((p) => p.subject === 'rpc.up.value')).toBe(true));
    const req = decode(ctx.nc.published.find((p) => p.subject === 'rpc.up.value')!.data) as { id: string };
    const mux = ctx.nc.subs.find((s) => s.pattern.startsWith('rpc.reply.'))!;
    mux.callback(null, { subject: `rpc.reply.${req.id}`, data: encode({ id: req.id, result: 42 }) });
    await done;
    expect(replies(ctx)).toContainEqual({ id: 'a.1', result: 42 });
  });

  it('drains a registration on an isolated connection with its cleanup', async () => {
    const ctx = createClient();
    const isolated = createMockNc();
    vi.mocked(connect).mockResolvedValueOnce(isolated as unknown as NatsConnection);
    const cleanup = await ctx.client.registerHandler('ns', { work: slow(20) }, { isolatedConnection: true });
    isolated.calls.length = 0;
    isolated.subs.find((s) => s.pattern === 'rpc.ns.work')!.callback(null, { subject: 'rpc.ns.work', data: encode({ id: 'a.1', method: 'call', params: ['a.1'] }) });

    await cleanup();
    expect(isolated.published.map((p) => decode(p.data))).toEqual([{ id: 'a.1', result: 'done a.1' }]);
    expect(isolated.calls).toEqual(['drain rpc.ns.work', 'flush', 'close']);
  });

  it.each(['cleanup', 'disconnect'] as const)('never answers an answered callback or pull-callback setup a second time (%s)', async (how) => {
    const ctx = createClient({ drainTimeoutMs: 1000 });
    async function* frames() {
      yield;
    }
    const cleanup = await ctx.client.registerHandler('ns', { watch: () => () => {}, pullFrames: () => frames() });
    deliver(ctx, 'watch', 'c.1', [{ __callback: true, __callbackSubject: 'rpc.cb.c1', args: [] }]);
    deliver(ctx, 'pullFrames', 'p.1', [pullCallbackParams('p.1')]);
    await vi.waitFor(() => expect(replies(ctx).length).toBe(2));

    const started = Date.now();
    await (how === 'cleanup' ? cleanup() : ctx.client.disconnect());
    expect(Date.now() - started).toBeLessThan(500);
    expect(replies(ctx)).toEqual([
      { id: 'c.1', result: { ok: true } },
      { id: 'p.1', result: { iteratorId: 'p.1' } },
    ]);
  });

  it('answers a callback or pull-callback setup that outlives the deadline once with CONNECTION_CLOSED', async () => {
    const ctx = createClient({ drainTimeoutMs: 40 });
    async function* frames() {
      yield;
    }
    const cleanup = await ctx.client.registerHandler('ns', {
      watch: async () => {
        await sleep(150);
        return () => {};
      },
      pullFrames: async () => {
        await sleep(150);
        return frames();
      },
    });
    deliver(ctx, 'watch', 'c.1', [{ __callback: true, __callbackSubject: 'rpc.cb.c1', args: [] }]);
    deliver(ctx, 'pullFrames', 'p.1', [pullCallbackParams('p.1')]);
    await drainMicrotasks();

    await cleanup();
    await sleep(250);
    const closed = { code: 'CONNECTION_CLOSED', message: 'Connection closed' };
    expect(replies(ctx)).toEqual([
      { id: 'c.1', error: closed },
      { id: 'p.1', error: closed },
    ]);
  });

  it('publishes no CONNECTION_CLOSED while nats.js reconnects, and drops the late result', async () => {
    const ctx = await connected({ drainTimeoutMs: 40 });
    const cleanup = await ctx.client.registerHandler('ns', { work: slow(100) });
    deliver(ctx, 'work', 'a.1', ['a.1']);
    deliver(ctx, 'work', 'a.2', ['a.2']);
    ctx.nc.emitStatus({ type: 'disconnect' });
    await drainMicrotasks();

    await cleanup();
    expect(replies(ctx)).toEqual([]);
    await sleep(150);
    expect(replies(ctx)).toEqual([]);
  });

  it('skips the drain once the connection is gone', async () => {
    const ctx = await connected({ drainTimeoutMs: 400 });
    const cleanup = await ctx.client.registerHandler('ns', { work: () => new Promise(() => {}) });
    deliver(ctx, 'work', 'a.1');
    await drainMicrotasks();
    ctx.nc.lose();
    await drainMicrotasks();

    const started = Date.now();
    await cleanup();
    expect(Date.now() - started).toBeLessThan(200);
    expect(replies(ctx)).toEqual([]);
  });

  it('drains the registrations of its isolated clients within the same deadline on disconnect()', async () => {
    const ctx = createClient({ drainTimeoutMs: 30 });
    const isolated = createMockNc();
    vi.mocked(connect).mockResolvedValueOnce(isolated as unknown as NatsConnection);
    await ctx.client.registerHandler('ns', { work: () => new Promise(() => {}) }, { isolatedConnection: true });
    isolated.subs.find((s) => s.pattern === 'rpc.ns.work')!.callback(null, { subject: 'rpc.ns.work', data: encode({ id: 'a.1', method: 'call', params: [] }) });

    const order: string[] = [];
    const publish = isolated.publish.bind(isolated);
    isolated.publish = (subject, data, opts) => {
      if (subject === 'rpc.reply.a.1') order.push('isolated request answered');
      publish(subject, data, opts);
    };
    ctx.client.call('rpc.up.value').catch(() => order.push('own call rejected'));
    await drainMicrotasks();

    await ctx.client.disconnect();
    // the own connection state drops only after the deadline of every drain
    expect(order).toEqual(['isolated request answered', 'own call rejected']);
    expect(isolated.published.map((p) => decode(p.data))).toEqual([{ id: 'a.1', error: { code: 'CONNECTION_CLOSED', message: 'Connection closed' } }]);
  });

  it('keeps the old behaviour with drainTimeoutMs 0', async () => {
    const ctx = createClient({ drainTimeoutMs: 0 });
    const cleanup = await ctx.client.registerHandler('ns', { echo: (v: string) => v });
    await cleanup();
    expect(ctx.nc.subs.find((s) => s.pattern === 'rpc.ns.echo')!.ended).toBe('unsubscribe');

    await ctx.client.registerHandler('ns', { work: slow(20) });
    ctx.nc.calls.length = 0;
    deliver(ctx, 'work', 'a.1', ['a.1']);
    await ctx.client.disconnect();
    expect(ctx.nc.calls).toEqual(['flush', 'close']);
    expect(replies(ctx)).toEqual([]);
  });
});
