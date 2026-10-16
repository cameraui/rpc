import { connect } from '@nats-io/transport-node';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { RPCClient } from '../src/client.js';
import { decode, encode } from '../src/codec.js';
import { chunkedMessages, createClient, createMockNc, drainMicrotasks, muxSub } from './mock-nc.js';

import type { NatsConnection } from '@nats-io/transport-node';
import type { ChunkingManager, TransferBudget } from '../src/chunking.js';
import type { RPCMessage } from '../src/types.js';

vi.mock('@nats-io/transport-node', async (importOriginal) => ({ ...(await importOriginal<typeof import('@nats-io/transport-node')>()), connect: vi.fn() }));

describe('disconnect() and suspend()', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it.each(['disconnect', 'suspend'] as const)('%s() flushes pending writes before closing', async (method) => {
    const { client, nc } = createClient();
    await client[method]();
    expect(nc.calls).toEqual(['flush', 'close']);
  });

  it('bounds the flush by disconnectTimeout on a dead socket', async () => {
    const { client, nc } = createClient({ disconnectTimeout: 50 });
    nc.onFlush = () => new Promise(() => {});
    const started = Date.now();
    await client.disconnect();
    expect(Date.now() - started).toBeLessThan(1000);
    expect(nc.calls).toEqual(['flush', 'close']);
  });

  it.each(['disconnect', 'suspend'] as const)('%s() leaves no timer behind once closed', async (method) => {
    vi.useFakeTimers();
    const { client } = createClient();
    await client[method]();
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(['disconnect', 'suspend'] as const)('%s() drops open chunked transfers and their sweep timers', async (method) => {
    vi.useFakeTimers();
    const { client } = createClient();
    const mux = (client as any).muxChunking as ChunkingManager;
    client.chunkingManager.startReceiving(
      't',
      2,
      () => {},
      () => {},
      200,
      100,
    );
    mux.startReceiving(
      'm',
      2,
      () => {},
      () => {},
      200,
      100,
    );
    expect(vi.getTimerCount()).toBe(2);

    await client[method]();
    expect(client.chunkingManager.size).toBe(0);
    expect(mux.size).toBe(0);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('keeps one chunk scope per subscription across suspend() and connect()', async () => {
    const { client } = createClient();
    const unsubscribes = [
      await client.subscribe('test.plain', () => {}),
      await client.subscribe('test.queue', () => {}, { queue: 'workers' }),
      await client.subscribe('test.queue', () => {}, { queue: 'workers' }),
    ];
    const scopes = (client as any).chunkScopes as Map<string, { members: number }>;
    const members = () => [...scopes.values()].map((scope) => scope.members);
    expect(members()).toEqual([1, 2]);

    await client.suspend();
    vi.mocked(connect).mockResolvedValueOnce(createMockNc() as unknown as NatsConnection);
    await client.connect();
    expect(members()).toEqual([1, 2]);

    for (const unsubscribe of unsubscribes) unsubscribe();
    expect(scopes.size).toBe(0);
  });
});

describe('connection closed for good', () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.mocked(connect).mockReset();
  });

  async function connected(...ncs: ReturnType<typeof createMockNc>[]) {
    for (const nc of ncs) vi.mocked(connect).mockResolvedValueOnce(nc as unknown as NatsConnection);
    const client = new RPCClient({ servers: ['nats://127.0.0.1:4222'], name: 'lost', disconnectTimeout: 20, noResponderRetry: { maxRetries: 0, delays: [1] } });
    await client.connect();
    return client;
  }

  it('tears down at the close event and dials again with the next call', async () => {
    vi.useFakeTimers();
    const [first, second] = [createMockNc(), createMockNc()];
    const client = await connected(first, second);
    const budget = (client as any).chunkBudget as TransferBudget;
    const mux = (client as any).muxChunking as ChunkingManager;

    await client.subscribe('test.scope', () => {});
    const scope = chunkedMessages('test.scope', { a: 'x'.repeat(500) }, 'scope', 100);
    const scopeSub = first.subs.find((sub) => sub.pattern === 'test.scope')!;
    scopeSub.callback(null, scope.header);
    scopeSub.callback(null, scope.chunks[0]);

    let failure: unknown;
    client.call('rpc.test.big').catch((error) => {
      failure = error;
    });
    await drainMicrotasks();
    const req = decode(first.published.at(-1)!.data) as RPCMessage;
    const reply = chunkedMessages(`rpc.reply.${req.id}`, { id: req.id, result: 'y'.repeat(500) }, 'reply', 100);
    muxSub(client, first).callback(null, reply.header);
    muxSub(client, first).callback(null, reply.chunks[0]);

    const endpoint = client.createEndpointChunking();
    endpoint.startReceiving(
      'endpoint',
      2,
      () => {},
      () => {},
      200,
      100,
    );
    endpoint.processChunk({ id: 'endpoint', chunkIndex: 0, data: new Uint8Array(100), isLast: false });
    expect(budget.inUse).toBeGreaterThan(0);

    first.lose();
    await drainMicrotasks();

    expect(failure).toMatchObject({ code: 'CONNECTION_CLOSED' });
    expect(budget.inUse).toBe(0);
    expect(mux.size).toBe(0);
    expect(endpoint.size).toBe(0);
    const scopes = [...((client as any).chunkScopes as Map<string, { manager?: ChunkingManager }>).values()];
    expect(scopes.map((s) => s.manager?.size ?? 0)).toEqual([0]);
    expect(vi.getTimerCount()).toBe(0);
    const entries = [...((client as any).subscriptionEntries as Map<number, { pattern: string; sub?: unknown }>).values()];
    expect(entries.map((entry) => entry.pattern).sort()).toEqual([`rpc.reply.${(client as any).replyPrefix}.>`, 'test.scope']);
    expect(entries.every((entry) => entry.sub === undefined)).toBe(true);
    expect(client.isClosed).toBe(false);
    expect(client.isConnected).toBe(false);

    client.call('rpc.test.again').catch(() => {});
    await drainMicrotasks();
    expect(connect).toHaveBeenCalledTimes(2);
    expect(second.subs.map((sub) => sub.pattern).sort()).toEqual(entries.map((entry) => entry.pattern).sort());
    expect(second.published).toHaveLength(1);
    expect(client.isConnected).toBe(true);
  });

  it('ends client streams and pull iterators and tears down served sessions at the close event', async () => {
    const first = createMockNc();
    const client = await connected(first);
    const sent = (subject: string) => first.published.filter((p) => p.subject === subject).map((p) => decode(p.data));
    let release!: () => void;
    const parked = new Promise<void>((resolve) => {
      release = resolve;
    });
    let finished = false;
    await client.registerHandler('ns', {
      async *items() {
        try {
          yield 'a';
          await parked;
          yield 'late';
        } finally {
          finished = true;
        }
      },
    });
    const served = 'stream.rpc.ns.items.s1';
    const method = first.subs.find((sub) => sub.pattern === 'rpc.ns.items')!;
    method.callback(null, { subject: 'rpc.ns.items', data: encode({ id: 's1', method: 'stream', params: { __stream: true, __streamSubject: served, args: [] } }) });

    const settled = (promise: Promise<unknown>) =>
      promise.then(
        (r) => ({ r }),
        (e) => ({ e }),
      );
    const streamNext = settled(client.callStream('rpc.ns.gen').next());
    const pullNext = settled(client.callPullIterator('rpc.ns.pull').next());
    await drainMicrotasks();
    const init = first.published.map((p) => decode(p.data) as RPCMessage).find((m) => m.method === 'call' && m.params?.[0]?.__pullIterator)!;
    muxSub(client, first).callback(null, { subject: `rpc.reply.${init.id}`, data: encode({ id: init.id, result: { iteratorId: init.params[0].__iteratorId } }) });
    await drainMicrotasks();
    expect(first.published.some((p) => p.subject === `_rpc.iterator.${init.params[0].__iteratorId}.request`)).toBe(true);
    expect(sent(served)).toEqual([{ id: 's1', type: 'data', data: 'a' }]);

    first.lose();
    await drainMicrotasks();

    expect(await streamNext).toEqual({ r: { value: undefined, done: true } });
    expect(await pullNext).toMatchObject({ e: { code: 'CONNECTION_CLOSED' } });
    expect((client as any).sessionTeardowns.size).toBe(0);
    release();
    await vi.waitFor(() => expect(finished).toBe(true));
    expect(sent(served)).toEqual([{ id: 's1', type: 'data', data: 'a' }]);
  });

  it.each(['suspend', 'disconnect', 'abortClose'] as const)('is not torn down again by the close of its own %s()', async (method) => {
    const client = await connected(createMockNc());
    const teardown = vi.spyOn(client as any, 'dropConnectionState');
    await client[method]();
    await drainMicrotasks();
    expect(teardown).toHaveBeenCalledTimes(1);
  });

  it('ignores a late close of a connection that was already replaced', async () => {
    const [first, second] = [createMockNc(), createMockNc()];
    let closeFirst!: () => void;
    const late = new Promise<void>((resolve) => {
      closeFirst = resolve;
    });
    first.closed = () => late;
    const client = await connected(first, second);
    await client.subscribe('test.kept', () => {});
    await client.suspend();
    await client.connect();

    closeFirst();
    await drainMicrotasks();
    expect(client.isConnected).toBe(true);
    expect(second.subs.filter((sub) => sub.pattern === 'test.kept' && !sub.closed)).toHaveLength(1);
  });
});
