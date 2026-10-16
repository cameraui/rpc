import { connect } from '@nats-io/transport-node';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { RPCClient } from '../src/client.js';
import { decode, encode } from '../src/codec.js';
import { sleep } from '../src/utils.js';
import { createMockNc, drainMicrotasks, emptyHeaders, muxSub } from './mock-nc.js';

import type { NatsConnection } from '@nats-io/transport-node';
import type { RPCMessage } from '../src/types.js';

vi.mock('@nats-io/transport-node', async (importOriginal) => ({ ...(await importOriginal<typeof import('@nats-io/transport-node')>()), connect: vi.fn() }));

async function connected(options: Partial<ConstructorParameters<typeof RPCClient>[0]> = {}) {
  const nc = createMockNc();
  vi.mocked(connect).mockResolvedValueOnce(nc as unknown as NatsConnection);
  const client = new RPCClient({ servers: ['nats://127.0.0.1:4222'], name: 'link', noResponderRetry: { maxRetries: 0, delays: [1] }, ...options });
  await client.connect();
  return { client, nc };
}

function sent(nc: ReturnType<typeof createMockNc>, subject: string): RPCMessage[] {
  return nc.published.filter((p) => p.subject === subject).map((p) => decode(p.data) as RPCMessage);
}

function reply(client: RPCClient, nc: ReturnType<typeof createMockNc>, req: RPCMessage, result: unknown) {
  muxSub(client, nc).callback(null, { subject: `rpc.reply.${req.id}`, data: encode({ id: req.id, result }) });
}

// what the promise settles with within `ms`, 'pending' otherwise
function within(promise: Promise<unknown>, ms: number): Promise<unknown> {
  return Promise.race([
    promise.then(
      (value) => value,
      (error: unknown) => error,
    ),
    sleep(ms).then(() => 'pending'),
  ]);
}

// answers the init call of a pull iterator and waits for its first next request
async function openPull(client: RPCClient, nc: ReturnType<typeof createMockNc>, iter: AsyncGenerator<any>, subject: string) {
  const first = iter.next();
  await vi.waitFor(() => expect(sent(nc, subject).length).toBe(1));
  const [init] = sent(nc, subject);
  const iteratorId: string = init.params[0].__iteratorId;
  reply(client, nc, init, { iteratorId });
  const nextRequests = () => sent(nc, `_rpc.iterator.${iteratorId}.request`).filter((req) => (req as { type?: string }).type === 'next').length;
  await vi.waitFor(() => expect(nextRequests()).toBe(1));
  const respond = (body: { type: string; value?: unknown }) =>
    nc.subs.find((s) => s.pattern === `_rpc.iterator.${iteratorId}.response`)!.callback(null, { data: encode({ id: iteratorId, ...body }) });
  return { iter, first, nextRequests, respond };
}

describe('requests while nats.js reconnects', () => {
  afterEach(() => {
    vi.mocked(connect).mockReset();
  });

  it('holds a call until the link is back and sends it then', async () => {
    const { client, nc } = await connected();
    nc.emitStatus({ type: 'disconnect' });
    await drainMicrotasks();

    const call = client.call('rpc.ns.ping', 1);
    await drainMicrotasks();
    expect(sent(nc, 'rpc.ns.ping')).toEqual([]);

    nc.emitStatus({ type: 'reconnect' });
    await drainMicrotasks();
    const [req] = sent(nc, 'rpc.ns.ping');
    expect(req.params).toEqual([1]);
    reply(client, nc, req, 'pong');
    await expect(call).resolves.toBe('pong');
  });

  it('sends held calls in the order they were made', async () => {
    const { client, nc } = await connected();
    nc.emitStatus({ type: 'staleConnection' });
    await drainMicrotasks();

    for (let i = 0; i < 5; i++) client.call(`rpc.ns.m${i}`).catch(() => {});
    await drainMicrotasks();
    expect(nc.published).toEqual([]);

    nc.emitStatus({ type: 'reconnect' });
    await drainMicrotasks();
    expect(nc.published.map((p) => p.subject)).toEqual(['rpc.ns.m0', 'rpc.ns.m1', 'rpc.ns.m2', 'rpc.ns.m3', 'rpc.ns.m4']);
  });

  it('times out a held call with the usual TIMEOUT and never sends it', async () => {
    const { client, nc } = await connected({ timeout: 30 });
    nc.emitStatus({ type: 'disconnect' });
    await drainMicrotasks();

    await expect(client.call('rpc.ns.slow')).rejects.toMatchObject({ code: 'TIMEOUT', message: 'RPC call to "rpc.ns.slow" timed out after 30ms' });
    // its args are not kept until the link is back
    expect((client as any).linkWaiters).toEqual([]);
    nc.emitStatus({ type: 'reconnect' });
    await drainMicrotasks();
    expect(sent(nc, 'rpc.ns.slow')).toEqual([]);
  });

  it('rejects a held call with CONNECTION_CLOSED when the connection ends and sends nothing', async () => {
    const { client, nc } = await connected();
    nc.emitStatus({ type: 'disconnect' });
    await drainMicrotasks();

    const call = client.call('rpc.ns.ping');
    nc.lose();
    await expect(call).rejects.toMatchObject({ code: 'CONNECTION_CLOSED' });
    expect(nc.published).toEqual([]);
  });

  it('keeps oneway publishes at most once instead of holding them', async () => {
    const { client, nc } = await connected();
    nc.emitStatus({ type: 'disconnect' });
    await drainMicrotasks();

    await client.publish('events.motion', { on: true });
    expect(nc.published.map((p) => p.subject)).toEqual(['events.motion']);
  });

  it('holds a retry attempt after a no-responder status', async () => {
    const { client, nc } = await connected({ noResponderRetry: { maxRetries: 1, delays: [1] } });
    const call = client.call('rpc.ns.ping');
    await drainMicrotasks();
    const [first] = sent(nc, 'rpc.ns.ping');

    nc.emitStatus({ type: 'disconnect' });
    await drainMicrotasks();
    muxSub(client, nc).callback(null, { subject: `rpc.reply.${first.id}`, data: new Uint8Array(0), headers: emptyHeaders(503) });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(sent(nc, 'rpc.ns.ping').length).toBe(1);

    nc.emitStatus({ type: 'reconnect' });
    await drainMicrotasks();
    const retry = sent(nc, 'rpc.ns.ping');
    expect(retry.length).toBe(2);
    reply(client, nc, retry[1], 'pong');
    await expect(call).resolves.toBe('pong');
  });

  it('holds request() and counts the wait against its timeout', async () => {
    const { client, nc } = await connected();
    nc.onRequest = async () => ({ data: encode({ ok: true }) });
    nc.emitStatus({ type: 'disconnect' });
    await drainMicrotasks();

    const held = client.request('channel.c.request', { q: 1 }, { timeout: 1000 });
    await drainMicrotasks();
    expect(nc.requests).toEqual([]);
    await sleep(30);
    nc.emitStatus({ type: 'reconnect' });
    await expect(held).resolves.toEqual({ ok: true });
    expect(nc.requests.length).toBe(1);
    const { timeout } = nc.requests[0].opts as { timeout: number };
    expect(timeout).toBeGreaterThan(0);
    expect(timeout).toBeLessThanOrEqual(975);

    nc.emitStatus({ type: 'disconnect' });
    await drainMicrotasks();
    await expect(client.request('channel.c.request', {}, { timeout: 30 })).rejects.toMatchObject({
      code: 'TIMEOUT',
      message: 'Request to "channel.c.request" timed out after 30ms',
    });
    expect(nc.requests.length).toBe(1);
  });

  it('holds the start of a stream', async () => {
    const { client, nc } = await connected();
    nc.emitStatus({ type: 'disconnect' });
    await drainMicrotasks();

    const next = client.callStream('rpc.ns.generate').next();
    await drainMicrotasks();
    expect(sent(nc, 'rpc.ns.generate')).toEqual([]);

    nc.emitStatus({ type: 'reconnect' });
    await drainMicrotasks();
    const [req] = sent(nc, 'rpc.ns.generate');
    const stream = nc.subs.find((s) => s.pattern === req.params.__streamSubject)!;
    stream.callback(null, { data: encode({ id: req.id, type: 'data', data: 'a' }) });
    await expect(next).resolves.toEqual({ value: 'a', done: false });
  });

  it('holds the next request of a pull iterator', async () => {
    const { client, nc } = await connected();
    const pull = await openPull(client, nc, client.callPullIterator('rpc.ns.pullItems'), 'rpc.ns.pullItems');
    pull.respond({ type: 'value', value: 1 });
    await expect(pull.first).resolves.toEqual({ value: 1, done: false });

    nc.emitStatus({ type: 'disconnect' });
    await drainMicrotasks();
    const second = pull.iter.next();
    await drainMicrotasks();
    expect(pull.nextRequests()).toBe(1);

    nc.emitStatus({ type: 'reconnect' });
    await drainMicrotasks();
    expect(pull.nextRequests()).toBe(2);
    pull.respond({ type: 'value', value: 2 });
    await expect(second).resolves.toEqual({ value: 2, done: false });
  });

  it('fails a held stream start with TIMEOUT when the link stays down past the client timeout', async () => {
    const { client, nc } = await connected({ timeout: 30 });
    nc.emitStatus({ type: 'disconnect' });
    await drainMicrotasks();

    const outcome = await within(client.callStream('rpc.ns.generate').next(), 300);
    expect(outcome).toMatchObject({ code: 'TIMEOUT', message: 'RPC call to "rpc.ns.generate" timed out after 30ms' });
    expect(sent(nc, 'rpc.ns.generate')).toEqual([]);
  });

  it('fails a held pull iterator next with TIMEOUT when the link stays down past the client timeout', async () => {
    const { client, nc } = await connected({ timeout: 30 });
    const pull = await openPull(client, nc, client.callPullIterator('rpc.ns.pullItems'), 'rpc.ns.pullItems');
    pull.respond({ type: 'value', value: 1 });
    await expect(pull.first).resolves.toEqual({ value: 1, done: false });

    nc.emitStatus({ type: 'disconnect' });
    await drainMicrotasks();
    const outcome = await within(pull.iter.next(), 300);
    expect(outcome).toMatchObject({ code: 'TIMEOUT' });
    expect(pull.nextRequests()).toBe(1);
  });

  it('holds a call on the service path', async () => {
    const { client, nc } = await connected();
    nc.emitStatus({ type: 'disconnect' });
    await drainMicrotasks();

    const call = client.call('svc.ping', 1);
    await sleep(10);
    expect(sent(nc, 'svc.ping')).toEqual([]);

    nc.emitStatus({ type: 'reconnect' });
    await vi.waitFor(() => expect(sent(nc, 'svc.ping').length).toBe(1));
    const [req] = sent(nc, 'svc.ping');
    expect(req.params).toEqual([1]);
    nc.subs.find((s) => s.pattern === `svc.ping.reply.${req.id}`)!.callback(null, { subject: `svc.ping.reply.${req.id}`, data: encode({ id: req.id, result: 'pong' }) });
    await expect(call).resolves.toBe('pong');
  });

  it('holds the next request of a pull-callback iterator', async () => {
    const { client, nc } = await connected();
    const pull = await openPull(client, nc, client.callPullIteratorWithCallback('rpc.ns.pullFrames', { onFrame: () => {} }, ['onFrame']), 'rpc.ns.pullFrames');
    pull.respond({ type: 'value' });
    await expect(pull.first).resolves.toEqual({ value: undefined, done: false });

    nc.emitStatus({ type: 'disconnect' });
    await drainMicrotasks();
    const second = pull.iter.next();
    await sleep(10);
    expect(pull.nextRequests()).toBe(1);

    nc.emitStatus({ type: 'reconnect' });
    await vi.waitFor(() => expect(pull.nextRequests()).toBe(2));
    pull.respond({ type: 'value' });
    await expect(second).resolves.toEqual({ value: undefined, done: false });
  });

  it('sends no pull-callback prefetch while the link is down, the next next() sends the request', async () => {
    const { client, nc } = await connected();
    const iter = client.callPullIteratorWithCallback('rpc.ns.pullFrames', { onFrame: () => {} }, { onewayMethods: ['onFrame'], prefetch: true });
    const pull = await openPull(client, nc, iter, 'rpc.ns.pullFrames');

    nc.emitStatus({ type: 'disconnect' });
    await drainMicrotasks();
    pull.respond({ type: 'value' });
    await expect(pull.first).resolves.toEqual({ value: undefined, done: false });
    await sleep(10);
    expect(pull.nextRequests()).toBe(1);

    nc.emitStatus({ type: 'reconnect' });
    await drainMicrotasks();
    const second = iter.next();
    await vi.waitFor(() => expect(pull.nextRequests()).toBe(2));
    pull.respond({ type: 'done' });
    await expect(second).resolves.toEqual({ value: undefined, done: true });
  });

  it.each(['suspend', 'disconnect'] as const)('does not hold requests on the next connection after a link loss during %s()', async (method) => {
    const { client, nc } = await connected();
    let pong!: () => void;
    nc.onFlush = () =>
      new Promise<void>((resolve) => {
        pong = resolve;
      });
    const closing = client[method]();
    await vi.waitFor(() => expect(nc.calls).toContain('flush'));
    // the link of the closing connection drops while its pre-close flush waits
    nc.emitStatus({ type: 'disconnect' });
    await drainMicrotasks();
    pong();
    await closing;
    expect((client as any).linkWaiters).toBeUndefined();

    const next = createMockNc();
    vi.mocked(connect).mockResolvedValueOnce(next as unknown as NatsConnection);
    await client.connect();
    client.call('rpc.ns.ping').catch(() => {});
    await drainMicrotasks();
    expect(sent(next, 'rpc.ns.ping').length).toBe(1);
  });

  it('stops following a connection that suspend() replaced', async () => {
    const { client, nc } = await connected();
    await client.suspend();
    const next = createMockNc();
    vi.mocked(connect).mockResolvedValueOnce(next as unknown as NatsConnection);
    await client.connect();

    nc.emitStatus({ type: 'disconnect' });
    await drainMicrotasks();
    client.call('rpc.ns.ping').catch(() => {});
    await drainMicrotasks();
    expect(sent(next, 'rpc.ns.ping').length).toBe(1);
  });
});
