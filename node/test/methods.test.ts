import { errors } from '@nats-io/transport-node';
import { describe, expect, it, vi } from 'vitest';

import { decode, encode } from '../src/codec.js';
import { RPCMethod } from '../src/decorators.js';
import { createClient, emptyHeaders, muxSub } from './mock-nc.js';

import type { RPCMessage, RPCResponse } from '../src/types.js';

type Ctx = ReturnType<typeof createClient>;

function requests({ nc }: Ctx, subject: string): RPCMessage[] {
  return nc.published.filter((p) => p.subject === subject).map((p) => decode(p.data) as RPCMessage);
}

function answer({ client, nc }: Ctx, req: RPCMessage, body: Partial<RPCResponse> | 503) {
  const msg =
    body === 503
      ? { subject: `rpc.reply.${req.id}`, data: new Uint8Array(0), headers: emptyHeaders(503) }
      : { subject: `rpc.reply.${req.id}`, data: encode({ id: req.id, ...body }) };
  muxSub(client, nc).callback(null, msg);
}

describe('rpc.<ns>.__methods on the responder', () => {
  it('is subscribed after the methods, in the same queue, and answers with the method names', async () => {
    const ctx = createClient();
    await ctx.client.registerHandler('ns', { ping: () => 'pong', db: { find: () => 1 } }, { queue: 'q' });

    expect(ctx.nc.subs.map((s) => s.pattern)).toEqual(['rpc.ns.ping', 'rpc.ns.db.find', 'rpc.ns.__methods']);
    const sub = ctx.nc.subs.find((s) => s.pattern === 'rpc.ns.__methods')!;
    sub.callback(null, { subject: 'rpc.ns.__methods', data: encode({ id: 'a.1', method: 'call', params: [] }) });

    expect(ctx.nc.published.map((p) => ({ subject: p.subject, body: decode(p.data) }))).toEqual([
      { subject: 'rpc.reply.a.1', body: { id: 'a.1', result: ['ping', 'db.find'] } },
    ]);
  });

  it('passes the queue group on', async () => {
    const ctx = createClient();
    const subscribe = vi.spyOn(ctx.nc, 'subscribe');
    await ctx.client.registerHandler('ns', { ping: () => 'pong' }, { queue: 'workers' });
    expect(subscribe.mock.calls.map(([pattern, opts]) => [pattern, (opts as { queue?: string }).queue])).toEqual([
      ['rpc.ns.ping', 'workers'],
      ['rpc.ns.__methods', 'workers'],
    ]);
  });

  it.each([250, 0])('is stopped first by the cleanup (drainTimeoutMs %i)', async (drainTimeoutMs) => {
    const ctx = createClient({ drainTimeoutMs });
    const order: string[] = [];
    const cleanup = await ctx.client.registerHandler('ns', { ping: () => 'pong' });
    for (const sub of ctx.nc.subs) {
      const { unsubscribe, drain } = sub;
      sub.unsubscribe = function () {
        order.push(`unsubscribe ${sub.pattern}`);
        unsubscribe.call(this);
      };
      sub.drain = function () {
        order.push(`drain ${sub.pattern}`);
        return drain.call(this);
      };
    }
    await cleanup();
    expect(order[0]).toBe('unsubscribe rpc.ns.__methods');
    expect(order).toContain(drainTimeoutMs > 0 ? 'drain rpc.ns.ping' : 'unsubscribe rpc.ns.ping');
  });

  it('leaves a handler method of that name alone', async () => {
    const ctx = createClient();
    class Api {
      __methods() {
        return 'mine';
      }
    }
    RPCMethod(Api.prototype, '__methods');
    await ctx.client.registerHandler('ns', new Api());
    expect(ctx.nc.subs.map((s) => s.pattern)).toEqual(['rpc.ns.__methods']);

    ctx.nc.subs[0].callback(null, { subject: 'rpc.ns.__methods', data: encode({ id: 'a.1', method: 'call', params: [] }) });
    await vi.waitFor(() => expect(ctx.nc.published.length).toBe(1));
    expect(decode(ctx.nc.published[0].data)).toEqual({ id: 'a.1', result: 'mine' });
  });
});

describe('proxy probe of rpc.<ns>.__methods', () => {
  const RETRY = { noResponderRetry: { maxRetries: 3, delays: [500, 1000, 2000] } };

  it('fails a method the namespace does not have at once and fills the method cache', async () => {
    const ctx = createClient(RETRY);
    const proxy = ctx.client.createProxy<any>('ns');
    const started = Date.now();
    const call = proxy.missing(1);
    await vi.waitFor(() => expect(requests(ctx, 'rpc.ns.missing').length).toBe(1));
    answer(ctx, requests(ctx, 'rpc.ns.missing')[0], 503);

    await vi.waitFor(() => expect(requests(ctx, 'rpc.ns.__methods').length).toBe(1));
    answer(ctx, requests(ctx, 'rpc.ns.__methods')[0], { result: ['ping'] });

    const error = await call.catch((e: unknown) => e);
    expect(error).toBeInstanceOf(errors.NoRespondersError);
    expect(Date.now() - started).toBeLessThan(400);
    expect(requests(ctx, 'rpc.ns.missing').length).toBe(1);
    expect(proxy.missing).toBeUndefined();
    expect(typeof proxy.ping).toBe('function');
  });

  it('fails a stream method the namespace does not have at once', async () => {
    const ctx = createClient(RETRY);
    const proxy = ctx.client.createProxy<any>('ns');
    const started = Date.now();
    const next = proxy.generateMissing(1).next();
    await vi.waitFor(() => expect(requests(ctx, 'rpc.ns.generateMissing').length).toBe(1));
    answer(ctx, requests(ctx, 'rpc.ns.generateMissing')[0], 503);

    await vi.waitFor(() => expect(requests(ctx, 'rpc.ns.__methods').length).toBe(1));
    answer(ctx, requests(ctx, 'rpc.ns.__methods')[0], { result: ['ping'] });

    const error = await next.catch((e: unknown) => e);
    expect(error).toBeInstanceOf(errors.NoRespondersError);
    expect(Date.now() - started).toBeLessThan(400);
    expect(requests(ctx, 'rpc.ns.generateMissing').length).toBe(1);
    expect(proxy.generateMissing).toBeUndefined();
  });

  it('retries a stream as before when the method is listed, and plain callStream() never probes', async () => {
    const ctx = createClient({ noResponderRetry: { maxRetries: 1, delays: [60] } });
    const next = ctx.client.createProxy<any>('ns').generateItems().next();
    await vi.waitFor(() => expect(requests(ctx, 'rpc.ns.generateItems').length).toBe(1));
    answer(ctx, requests(ctx, 'rpc.ns.generateItems')[0], 503);
    await vi.waitFor(() => expect(requests(ctx, 'rpc.ns.__methods').length).toBe(1));
    answer(ctx, requests(ctx, 'rpc.ns.__methods')[0], { result: ['generateItems'] });

    await vi.waitFor(() => expect(requests(ctx, 'rpc.ns.generateItems').length).toBe(2));
    const retry = requests(ctx, 'rpc.ns.generateItems')[1];
    ctx.nc.subs.find((s) => s.pattern === retry.params.__streamSubject)!.callback(null, { data: encode({ id: retry.id, type: 'data', data: 'a' }) });
    await expect(next).resolves.toEqual({ value: 'a', done: false });

    const plain = ctx.client.callStream('rpc.ns.generateOther').next();
    await vi.waitFor(() => expect(requests(ctx, 'rpc.ns.generateOther').length).toBe(1));
    answer(ctx, requests(ctx, 'rpc.ns.generateOther')[0], 503);
    await vi.waitFor(() => expect(requests(ctx, 'rpc.ns.generateOther').length).toBe(2));
    answer(ctx, requests(ctx, 'rpc.ns.generateOther')[1], 503);
    await expect(plain).rejects.toBeInstanceOf(errors.NoRespondersError);
    expect(requests(ctx, 'rpc.ns.__methods').length).toBe(1);
  });

  it('retries as before when the method is listed', async () => {
    const ctx = createClient({ noResponderRetry: { maxRetries: 1, delays: [60] } });
    const proxy = ctx.client.createProxy<any>('ns');
    const started = Date.now();
    const call = proxy.ping();
    await vi.waitFor(() => expect(requests(ctx, 'rpc.ns.ping').length).toBe(1));
    answer(ctx, requests(ctx, 'rpc.ns.ping')[0], 503);
    await vi.waitFor(() => expect(requests(ctx, 'rpc.ns.__methods').length).toBe(1));
    answer(ctx, requests(ctx, 'rpc.ns.__methods')[0], { result: ['ping'] });

    await vi.waitFor(() => expect(requests(ctx, 'rpc.ns.ping').length).toBe(2));
    // the probe runs inside the first delay, it adds no wait of its own
    expect(Date.now() - started).toBeLessThan(150);
    answer(ctx, requests(ctx, 'rpc.ns.ping')[1], { result: 'pong' });
    await expect(call).resolves.toBe('pong');
  });

  it.each([
    ['a no-responder status (namespace gone or an older responder)', 503 as const],
    ['an error reply', { error: { code: 'METHOD_NOT_FOUND', message: 'unknown' } }],
    ['a result that is not a method list', { result: { methods: 1 } }],
  ])('retries as before after %s', async (_, body) => {
    const ctx = createClient({ noResponderRetry: { maxRetries: 1, delays: [40] } });
    const call = ctx.client.createProxy<any>('ns').ping();
    await vi.waitFor(() => expect(requests(ctx, 'rpc.ns.ping').length).toBe(1));
    answer(ctx, requests(ctx, 'rpc.ns.ping')[0], 503);
    await vi.waitFor(() => expect(requests(ctx, 'rpc.ns.__methods').length).toBe(1));
    answer(ctx, requests(ctx, 'rpc.ns.__methods')[0], body);

    await vi.waitFor(() => expect(requests(ctx, 'rpc.ns.ping').length).toBe(2));
    answer(ctx, requests(ctx, 'rpc.ns.ping')[1], { result: 'pong' });
    await expect(call).resolves.toBe('pong');
  });

  it('retries after the first delay when the probe gets no answer', async () => {
    const ctx = createClient({ noResponderRetry: { maxRetries: 1, delays: [40] } });
    const started = Date.now();
    const call = ctx.client.createProxy<any>('ns').ping();
    await vi.waitFor(() => expect(requests(ctx, 'rpc.ns.ping').length).toBe(1));
    answer(ctx, requests(ctx, 'rpc.ns.ping')[0], 503);

    await vi.waitFor(() => expect(requests(ctx, 'rpc.ns.ping').length).toBe(2));
    expect(Date.now() - started).toBeLessThan(200);
    answer(ctx, requests(ctx, 'rpc.ns.ping')[1], { result: 'pong' });
    await expect(call).resolves.toBe('pong');
  });

  it('is not sent without a retry to save, nor by plain call()', async () => {
    const ctx = createClient({ noResponderRetry: { maxRetries: 1, delays: [1] } });
    const proxied = ctx.client.createProxy<any>('ns', { noResponderRetry: { maxRetries: 0 } }).ping();
    await vi.waitFor(() => expect(requests(ctx, 'rpc.ns.ping').length).toBe(1));
    answer(ctx, requests(ctx, 'rpc.ns.ping')[0], 503);
    await expect(proxied).rejects.toBeInstanceOf(errors.NoRespondersError);

    const plain = ctx.client.call('rpc.ns.ping');
    await vi.waitFor(() => expect(requests(ctx, 'rpc.ns.ping').length).toBe(2));
    answer(ctx, requests(ctx, 'rpc.ns.ping')[1], 503);
    await vi.waitFor(() => expect(requests(ctx, 'rpc.ns.ping').length).toBe(3));
    answer(ctx, requests(ctx, 'rpc.ns.ping')[2], 503);
    await expect(plain).rejects.toBeInstanceOf(errors.NoRespondersError);
    expect(requests(ctx, 'rpc.ns.__methods')).toEqual([]);
  });
});
