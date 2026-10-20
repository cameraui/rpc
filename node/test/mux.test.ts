import { errors } from '@nats-io/transport-node';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { createChunks } from '../src/chunking.js';
import { decode, encode } from '../src/codec.js';
import { RPCException } from '../src/errors.js';
import { chunkedMessages, createClient, emptyHeaders, headersWith, muxSub } from './mock-nc.js';

import type { NatsConnection } from '@nats-io/transport-node';
import type { ChunkingManager } from '../src/chunking.js';
import type { RPCMessage } from '../src/types.js';
import type { MockSub } from './mock-nc.js';

describe('muxed reply inbox', () => {
  it('routes a response to its pending call by envelope id', async () => {
    const { client, nc } = createClient();
    const prefix = (client as any).replyPrefix as string;

    const promise = client.call('rpc.test.echo', 'hello');

    expect(nc.published.length).toBe(1);
    const request = decode(nc.published[0].data) as RPCMessage;
    expect(request.id.startsWith(`${prefix}.`)).toBe(true);
    expect(nc.published[0].opts?.reply).toBe(`rpc.reply.${request.id}`);
    expect(nc.published[0].subject).toBe('rpc.test.echo');
    expect(request.params).toEqual(['hello']);

    const sub = muxSub(client, nc);
    sub.callback(null, {
      subject: `rpc.reply.${request.id}`,
      data: encode({ id: request.id, result: 'world' }),
      headers: undefined,
    });

    await expect(promise).resolves.toBe('world');
    expect(((client as any).pendingRequests as Map<string, any>).size).toBe(0);
  });

  it('creates exactly one mux subscription across many calls', async () => {
    const { client, nc } = createClient();

    const p1 = client.call('rpc.a.one');
    const p2 = client.call('rpc.a.two');
    const p3 = client.call('rpc.a.three');

    const sub = muxSub(client, nc); // asserts a single mux subscription
    for (const pub of nc.published) {
      const req = decode(pub.data) as RPCMessage;
      sub.callback(null, { subject: `rpc.reply.${req.id}`, data: encode({ id: req.id, result: req.params }) });
    }
    await Promise.all([p1, p2, p3]);
    expect(nc.subs.length).toBe(1);
  });

  it('routes out-of-order responses of concurrent calls correctly', async () => {
    const { client, nc } = createClient();

    const p1 = client.call('rpc.test.first');
    const p2 = client.call('rpc.test.second');

    const [req1, req2] = nc.published.map((p) => decode(p.data) as RPCMessage);
    const sub = muxSub(client, nc);

    // answer the second call first
    sub.callback(null, { subject: `rpc.reply.${req2.id}`, data: encode({ id: req2.id, result: 2 }) });
    sub.callback(null, { subject: `rpc.reply.${req1.id}`, data: encode({ id: req1.id, result: 1 }) });

    await expect(p2).resolves.toBe(2);
    await expect(p1).resolves.toBe(1);
  });

  it('rejects with RPCException on an error response', async () => {
    const { client, nc } = createClient();

    const promise = client.call('rpc.test.fail');
    const req = decode(nc.published[0].data) as RPCMessage;

    muxSub(client, nc).callback(null, {
      subject: `rpc.reply.${req.id}`,
      data: encode({ id: req.id, error: { code: 'METHOD_NOT_FOUND', message: 'nope' } }),
    });

    await expect(promise).rejects.toMatchObject({ code: 'METHOD_NOT_FOUND', message: 'nope' });
    await expect(promise).rejects.toBeInstanceOf(RPCException);
  });

  it('passes __methods through the callWithMeta side channel without touching the result', async () => {
    const { client, nc } = createClient();

    const promise = client.callWithMeta('rpc.test.meta');
    const req = decode(nc.published[0].data) as RPCMessage;

    muxSub(client, nc).callback(null, {
      subject: `rpc.reply.${req.id}`,
      data: encode({ id: req.id, result: { value: 7 }, __methods: ['meta', 'other'] }),
    });

    const { result, methods } = await promise;
    expect(result).toEqual({ value: 7 });
    expect(methods).toEqual(['meta', 'other']);
  });

  it('rejects the pending call with NoRespondersError on a 503 status (routed by subject)', async () => {
    const { client, nc } = createClient();

    const promise = client.call('rpc.ghost.method');
    const req = decode(nc.published[0].data) as RPCMessage;

    // a no-responder status: empty payload, code 503
    muxSub(client, nc).callback(null, {
      subject: `rpc.reply.${req.id}`,
      data: new Uint8Array(0),
      headers: emptyHeaders(503),
    });

    await expect(promise).rejects.toBeInstanceOf(errors.NoRespondersError);
    expect(((client as any).pendingRequests as Map<string, any>).size).toBe(0);
  });

  it('routes a 503 status to a registered status handler (one-shot) instead of pendingRequests', async () => {
    const { client, nc } = createClient();

    (client as any).ensureMuxSubscription();
    const prefix = (client as any).replyPrefix as string;
    const token = `${prefix}.iter-token`;

    const received: Error[] = [];
    ((client as any).statusHandlers as Map<string, (err: Error) => void>).set(token, (err) => received.push(err));

    const sub = muxSub(client, nc);
    sub.callback(null, { subject: `rpc.reply.${token}`, data: new Uint8Array(0), headers: emptyHeaders(503) });

    expect(received.length).toBe(1);
    expect(received[0]).toBeInstanceOf(errors.NoRespondersError);
    // one-shot, a second status is dropped
    expect(((client as any).statusHandlers as Map<string, any>).size).toBe(0);
    sub.callback(null, { subject: `rpc.reply.${token}`, data: new Uint8Array(0), headers: emptyHeaders(503) });
    expect(received.length).toBe(1);
  });

  it('reassembles chunked responses and routes them by envelope id', async () => {
    const { client, nc } = createClient();

    const promise = client.call('rpc.test.big');
    const req = decode(nc.published[0].data) as RPCMessage;
    const replySubject = `rpc.reply.${req.id}`;
    const sub = muxSub(client, nc);

    const bigResult = 'x'.repeat(1000);
    const encoded = encode({ id: req.id, result: bigResult });
    const chunkSize = 100;
    const transferId = 'transfer-1';
    const totalChunks = Math.ceil(encoded.length / chunkSize);

    sub.callback(null, {
      subject: replySubject,
      data: encode({ type: 'chunked', transferId, totalChunks, totalSize: encoded.length, chunkSize }),
      headers: headersWith({ 'x-chunked-transfer': 'header', 'x-chunk-id': transferId }),
    });

    for (const chunk of createChunks(encoded, transferId, chunkSize)) {
      sub.callback(null, {
        subject: replySubject,
        data: chunk.data,
        headers: headersWith({
          'x-chunked-transfer': 'chunk',
          'x-chunk-id': transferId,
          'x-chunk-index': chunk.chunkIndex.toString(),
        }),
      });
    }

    await expect(promise).resolves.toBe(bigResult);
  });

  it('drops responses for unknown ids silently', async () => {
    const { client, nc } = createClient();

    const promise = client.call('rpc.test.keep');
    const req = decode(nc.published[0].data) as RPCMessage;
    const sub = muxSub(client, nc);

    // a foreign response must not settle or throw
    sub.callback(null, { subject: 'rpc.reply.someone.else', data: encode({ id: 'someone.else', result: 'nope' }) });

    sub.callback(null, { subject: `rpc.reply.${req.id}`, data: encode({ id: req.id, result: 'mine' }) });
    await expect(promise).resolves.toBe('mine');
  });

  it('times out and removes the pending entry; a late response is ignored', async () => {
    const { client, nc } = createClient({ timeout: 20 });

    const promise = client.call('rpc.test.slow');
    const req = decode(nc.published[0].data) as RPCMessage;

    await expect(promise).rejects.toMatchObject({ code: 'TIMEOUT' });
    expect(((client as any).pendingRequests as Map<string, any>).size).toBe(0);

    // a late response after the timeout is a no-op
    muxSub(client, nc).callback(null, { subject: `rpc.reply.${req.id}`, data: encode({ id: req.id, result: 'late' }) });
  });

  it('uses the connId as reply prefix when configured', async () => {
    const { client, nc } = createClient({ connId: 'conn42' });

    const promise = client.call('rpc.test.scoped');
    const req = decode(nc.published[0].data) as RPCMessage;

    expect((client as any).replyPrefix).toBe('conn42');
    expect(req.id.startsWith('conn42.')).toBe(true);
    expect(nc.subs[0].pattern).toBe('rpc.reply.conn42.>');

    muxSub(client, nc).callback(null, { subject: `rpc.reply.${req.id}`, data: encode({ id: req.id, result: true }) });
    await expect(promise).resolves.toBe(true);
  });
});

function openTransfers(client: ReturnType<typeof createClient>['client']): number {
  let open = 0;
  for (const scope of (client as any).chunkScopes.values()) open += scope.manager?.size ?? 0;
  return open;
}

function headerOnly(subject: string, transferId: string, totalSize: number, chunkSize: number) {
  return {
    subject,
    data: encode({ type: 'chunked', transferId, totalChunks: Math.ceil(totalSize / chunkSize), totalSize, chunkSize }),
    headers: headersWith({ 'x-chunked-transfer': 'header', 'x-chunk-id': transferId }),
  };
}

function withIndex<T extends { headers: ReturnType<typeof headersWith> }>(chunk: T, index: string): T {
  return { ...chunk, headers: headersWith({ 'x-chunked-transfer': 'chunk', 'x-chunk-id': chunk.headers.get('x-chunk-id')!, 'x-chunk-index': index }) };
}

function resized<T extends { data: Uint8Array }>(chunk: T, delta: -1 | 1): T {
  return { ...chunk, data: delta < 0 ? chunk.data.subarray(1) : Uint8Array.from([...chunk.data, 0]) };
}

type Chunk = ReturnType<typeof chunkedMessages>['chunks'][number];

describe('chunked receive', () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('starts no reply transfer without a pending call and drops its chunks silently', () => {
    const { client, nc } = createClient();
    (client as any).ensureMuxSubscription();
    const prefix = (client as any).replyPrefix as string;
    const warn = vi.spyOn(console, 'warn');
    const error = vi.spyOn(console, 'error');

    const { header, chunks } = chunkedMessages(`rpc.reply.${prefix}.gone`, { id: `${prefix}.gone`, result: 'x'.repeat(500) }, 'late', 100);
    const sub = muxSub(client, nc);
    sub.callback(null, header);
    for (const chunk of chunks) sub.callback(null, chunk);

    expect((client as any).muxChunking.size).toBe(0);
    expect(warn).not.toHaveBeenCalled();
    expect(error).not.toHaveBeenCalled();
  });

  it('discards the half-assembled reply when the call times out', async () => {
    const { client, nc } = createClient({ timeout: 20 });
    const promise = client.call('rpc.test.big');
    const req = decode(nc.published[0].data) as RPCMessage;
    const { header, chunks } = chunkedMessages(`rpc.reply.${req.id}`, { id: req.id, result: 'x'.repeat(500) }, 'slow', 100);
    const sub = muxSub(client, nc);
    sub.callback(null, header);
    sub.callback(null, chunks[0]);
    expect((client as any).muxChunking.size).toBe(1);

    await expect(promise).rejects.toMatchObject({ code: 'TIMEOUT' });
    expect((client as any).muxChunking.size).toBe(0);
  });

  it('delivers more than 32 chunked replies at once', async () => {
    const { client, nc } = createClient({ timeout: 1000 });
    const calls = Array.from({ length: 33 }, () => client.call('rpc.test.big'));
    const results = calls.map((_, i) => `r${i}`.padEnd(300, '.'));
    const replies = nc.published.map((p, i) => {
      const req = decode(p.data) as RPCMessage;
      return chunkedMessages(`rpc.reply.${req.id}`, { id: req.id, result: results[i] }, `t${i}`, 100);
    });
    const sub = muxSub(client, nc);
    for (const { header } of replies) sub.callback(null, header);
    for (const { chunks } of replies) for (const chunk of chunks) sub.callback(null, chunk);

    await expect(Promise.all(calls)).resolves.toEqual(results);
  });

  it('rejects the call at once when its reply header is refused', async () => {
    const { client, nc } = createClient({ maxTransferSize: 200, timeout: 1000 });
    const promise = client.call('rpc.test.big');
    const req = decode(nc.published[0].data) as RPCMessage;
    const { header } = chunkedMessages(`rpc.reply.${req.id}`, { id: req.id, result: 'x'.repeat(500) }, 'huge', 100);
    muxSub(client, nc).callback(null, header);

    await expect(promise).rejects.toMatchObject({
      code: 'INTERNAL_ERROR',
      message: expect.stringMatching(/^Chunked reply to "rpc\.test\.big" failed: Invalid chunk header: totalSize \d+ out of range \(max 200\)$/),
    });
    expect((client as any).pendingRequests.size).toBe(0);
    expect((client as any).muxChunking.size).toBe(0);
  });

  it('rejects the call at once when a chunk of its reply does not fit', async () => {
    const { client, nc } = createClient({ timeout: 1000 });
    const promise = client.call('rpc.test.big');
    const req = decode(nc.published[0].data) as RPCMessage;
    const { header, chunks } = chunkedMessages(`rpc.reply.${req.id}`, { id: req.id, result: 'x'.repeat(500) }, 'bad', 100);
    const sub = muxSub(client, nc);
    sub.callback(null, header);
    sub.callback(null, { ...chunks[0], data: chunks[0].data.subarray(1) });

    await expect(promise).rejects.toMatchObject({ code: 'INTERNAL_ERROR', message: expect.stringContaining('Invalid length 99 for chunk 0') });
    expect((client as any).muxChunking.size).toBe(0);
  });

  it.each([
    ['an index that is not a number', (chunks: Chunk[]) => withIndex(chunks[1], 'x')],
    ['a signed index', (chunks: Chunk[]) => withIndex(chunks[1], '+1')],
    ['a negative index', (chunks: Chunk[]) => withIndex(chunks[1], '-1')],
    ['an index past the last chunk', (chunks: Chunk[]) => withIndex(chunks[1], String(chunks.length))],
    ['a long chunk', (chunks: Chunk[]) => resized(chunks[1], 1)],
    ['a short last chunk', (chunks: Chunk[]) => resized(chunks.at(-1)!, -1)],
    ['a long last chunk', (chunks: Chunk[]) => resized(chunks.at(-1)!, 1)],
    ['a duplicate with the wrong length', (chunks: Chunk[]) => resized(chunks[0], -1)],
  ])('rejects the call at once on %s in its reply', async (_name, bad) => {
    const { client, nc } = createClient({ timeout: 1000 });
    const promise = client.call('rpc.test.big');
    const req = decode(nc.published[0].data) as RPCMessage;
    const { header, chunks } = chunkedMessages(`rpc.reply.${req.id}`, { id: req.id, result: 'x'.repeat(500) }, 'bad', 100);
    expect(chunks.at(-1)!.data.length).toBeGreaterThan(1);
    expect(chunks.at(-1)!.data.length).toBeLessThan(100);
    const sub = muxSub(client, nc);
    sub.callback(null, header);
    sub.callback(null, chunks[0]);
    sub.callback(null, bad(chunks));

    await expect(promise).rejects.toMatchObject({ code: 'INTERNAL_ERROR', message: expect.stringMatching(/^Chunked reply to "rpc\.test\.big" failed: /) });
    expect((client as any).muxChunking.size).toBe(0);
    expect((client as any).chunkBudget.inUse).toBe(0);
  });

  it('rejects the call at once when its reassembled reply does not decode', async () => {
    const { client, nc } = createClient({ timeout: 1000 });
    const promise = client.call('rpc.test.big');
    const req = decode(nc.published[0].data) as RPCMessage;
    const subject = `rpc.reply.${req.id}`;
    const garbage = new Uint8Array(250).fill(0xc1);
    const sub = muxSub(client, nc);
    sub.callback(null, headerOnly(subject, 'garbage', garbage.length, 100));
    for (const chunk of createChunks(garbage, 'garbage', 100)) {
      sub.callback(null, { subject, data: chunk.data, headers: headersWith({ 'x-chunked-transfer': 'chunk', 'x-chunk-id': 'garbage', 'x-chunk-index': String(chunk.chunkIndex) }) });
    }

    await expect(promise).rejects.toMatchObject({ code: 'INTERNAL_ERROR', message: expect.stringMatching(/^Chunked reply to "rpc\.test\.big" failed: /) });
    expect((client as any).muxChunking.size).toBe(0);
    expect((client as any).chunkBudget.inUse).toBe(0);
  });

  it('rejects the call when its reply transfer stalls, before the call times out', async () => {
    vi.useFakeTimers();
    const { client, nc } = createClient({ timeout: 120_000 });
    const promise = client.call('rpc.test.big');
    const req = decode(nc.published[0].data) as RPCMessage;
    const { header, chunks } = chunkedMessages(`rpc.reply.${req.id}`, { id: req.id, result: 'x'.repeat(500) }, 'stalled', 100);
    const sub = muxSub(client, nc);
    sub.callback(null, header);
    sub.callback(null, chunks[0]);
    const rejected = expect(promise).rejects.toMatchObject({ code: 'INTERNAL_ERROR', message: 'Chunked reply to "rpc.test.big" failed: Transfer timed out' });

    await vi.advanceTimersByTimeAsync(30_001);
    await rejected;
    expect((client as any).muxChunking.size).toBe(0);
    expect((client as any).chunkBudget.inUse).toBe(0);
  });

  it('ignores the chunks of a reply transfer a later header replaced and resolves with the new one', async () => {
    const { client, nc } = createClient({ timeout: 1000 });
    const promise = client.call('rpc.test.big');
    const req = decode(nc.published[0].data) as RPCMessage;
    const result = 'x'.repeat(500);
    const replaced = chunkedMessages(`rpc.reply.${req.id}`, { id: req.id, result: 'y'.repeat(500) }, 'replaced', 100);
    const current = chunkedMessages(`rpc.reply.${req.id}`, { id: req.id, result }, 'current', 100);
    const sub = muxSub(client, nc);
    sub.callback(null, replaced.header);
    sub.callback(null, replaced.chunks[0]);
    sub.callback(null, current.header);
    sub.callback(null, withIndex(replaced.chunks[1], 'x'));
    for (const chunk of replaced.chunks) sub.callback(null, chunk);
    for (const chunk of current.chunks) sub.callback(null, chunk);

    await expect(promise).resolves.toBe(result);
  });

  it('ignores chunks of another transfer on the reply mux and still resolves the call', async () => {
    const { client, nc } = createClient({ timeout: 1000 });
    const promise = client.call('rpc.test.big');
    const req = decode(nc.published[0].data) as RPCMessage;
    const result = 'x'.repeat(500);
    const { header, chunks } = chunkedMessages(`rpc.reply.${req.id}`, { id: req.id, result }, 'mine', 100);
    const stray = chunkedMessages(`rpc.reply.${req.id}`, { id: req.id, result: 'y'.repeat(500) }, 'other', 100);
    const sub = muxSub(client, nc);
    sub.callback(null, header);
    sub.callback(null, withIndex(stray.chunks[0], 'x'));
    for (const chunk of chunks) sub.callback(null, chunk);

    await expect(promise).resolves.toBe(result);
  });

  it('drops the whole transfer of a handler subscription on a bad chunk', async () => {
    const { client, nc } = createClient();
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const got: unknown[] = [];
    await client.subscribe('test.bad', (data) => {
      got.push(data);
    });
    const { header, chunks } = chunkedMessages('test.bad', { a: 'x'.repeat(500) }, 'bad', 100);
    nc.subs[0].callback(null, header);
    nc.subs[0].callback(null, chunks[0]);
    nc.subs[0].callback(null, withIndex(chunks[1], 'x'));
    expect(openTransfers(client)).toBe(0);
    expect((client as any).chunkBudget.inUse).toBe(0);

    for (const chunk of chunks) nc.subs[0].callback(null, chunk);
    expect(got).toEqual([]);
    expect(error).toHaveBeenCalledTimes(1);
    expect(openTransfers(client)).toBe(0);
  });

  it('applies maxTransferSize to handler subscriptions', async () => {
    const { client, nc } = createClient({ maxTransferSize: 1000 });
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    await client.subscribe('test.limit', () => {});
    const header = (totalSize: number) => ({
      subject: 'test.limit',
      data: encode({ type: 'chunked', transferId: `t${totalSize}`, totalChunks: Math.ceil(totalSize / 100), totalSize, chunkSize: 100 }),
      headers: headersWith({ 'x-chunked-transfer': 'header', 'x-chunk-id': `t${totalSize}` }),
    });

    nc.subs[0].callback(null, header(1001));
    expect(openTransfers(client)).toBe(0);
    expect(error).toHaveBeenCalledTimes(1);
    nc.subs[0].callback(null, header(1000));
    expect(openTransfers(client)).toBe(1);
    client.abortClose();
  });

  it('delivers a chunked message to every overlapping subscription', async () => {
    const { client, nc } = createClient();
    const a: unknown[] = [];
    const b: unknown[] = [];
    await client.subscribe('test.overlap', (data) => {
      a.push(data);
    });
    await client.subscribe('test.overlap', (data) => {
      b.push(data);
    });
    const payload = { big: 'y'.repeat(700) };
    const { header, chunks } = chunkedMessages('test.overlap', payload, 'shared', 128);

    // NATS hands every message to each matching subscription
    for (const msg of [header, ...chunks]) {
      for (const sub of nc.subs) sub.callback(null, msg);
    }

    expect(a).toEqual([payload]);
    expect(b).toEqual([payload]);
  });

  it('reassembles a transfer split across queue subscriptions of one client', async () => {
    const { client, nc } = createClient();
    const got: unknown[] = [];
    const collect = (data: unknown) => {
      got.push(data);
    };
    await client.subscribe('test.queue', collect, { queue: 'workers' });
    await client.subscribe('test.queue', collect, { queue: 'workers' });
    const payload = { big: 'z'.repeat(700) };
    const { header, chunks } = chunkedMessages('test.queue', payload, 'split', 128);

    // the queue group hands each message to one member only
    [header, ...chunks].forEach((msg, i) => nc.subs[i % 2].callback(null, msg));

    expect(got).toEqual([payload]);
  });

  it('drops a forged header on a handler subject and stops the sweep timer on teardown', async () => {
    vi.useFakeTimers();
    const { client, nc } = createClient();
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    await client.subscribe('test.victim', () => {});
    const forged = {
      subject: 'test.victim',
      data: encode({ type: 'chunked', transferId: 'evil', totalChunks: 1, totalSize: 2 ** 40, chunkSize: 2 ** 40 }),
      headers: headersWith({ 'x-chunked-transfer': 'header', 'x-chunk-id': 'evil' }),
    };
    nc.subs[0].callback(null, forged);
    expect(openTransfers(client)).toBe(0);
    expect(error).toHaveBeenCalledTimes(1);

    const { header } = chunkedMessages('test.victim', { a: 'b'.repeat(300) }, 'open', 100);
    nc.subs[0].callback(null, header);
    expect(openTransfers(client)).toBe(1);
    expect(vi.getTimerCount()).toBe(1);

    client.abortClose();
    expect(openTransfers(client)).toBe(0);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('caps open transfers per subscription instead of per client', async () => {
    const { client, nc } = createClient();
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    for (let i = 0; i < 2; i++) await client.subscribe(`test.cap${i}`, () => {});

    for (let i = 0; i < 33; i++) nc.subs[0].callback(null, headerOnly('test.cap0', `a${i}`, 200, 100));
    for (let i = 0; i < 32; i++) nc.subs[1].callback(null, headerOnly('test.cap1', `b${i}`, 200, 100));

    expect(openTransfers(client)).toBe(64);
    expect(error).toHaveBeenCalledTimes(1);
    expect(String(error.mock.calls[0][1])).toContain('Too many concurrent chunked transfers (32)');
    client.abortClose();
  });

  it('delivers a chunked reply on the legacy service path while handler subscriptions are full', async () => {
    const { client, nc } = createClient({ timeout: 1000 });
    vi.spyOn(console, 'error').mockImplementation(() => {});
    await client.subscribe('test.busy', () => {});
    for (let i = 0; i < 32; i++) nc.subs[0].callback(null, headerOnly('test.busy', `t${i}`, 200, 100));

    const promise = client.call('svc.method');
    await vi.waitFor(() => expect(nc.subs.some((s) => s.pattern.startsWith('svc.method.reply.'))).toBe(true));
    const reply = nc.subs.find((s) => s.pattern.startsWith('svc.method.reply.'))!;
    const id = reply.pattern.slice('svc.method.reply.'.length);
    const result = 'r'.repeat(500);
    const { header, chunks } = chunkedMessages(reply.pattern, { id, result }, 'svc', 100);
    for (const msg of [header, ...chunks]) reply.callback(null, msg);

    await expect(promise).resolves.toBe(result);
    client.abortClose();
  });

  it('holds no budget for headers without chunks and shares the budget between subscriptions', async () => {
    const { client, nc } = createClient({ maxTransferSize: 1000 });
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const got: unknown[] = [];
    await client.subscribe('test.headers', () => {});
    await client.subscribe('test.a', (data) => {
      got.push(data);
    });
    await client.subscribe('test.b', (data) => {
      got.push(data);
    });

    // headers alone allocate nothing, so they cannot use up the budget
    for (let i = 0; i < 40; i++) nc.subs[0].callback(null, headerOnly('test.headers', `h${i}`, 1000, 100));
    expect(openTransfers(client)).toBe(32);
    expect((client as any).chunkBudget.inUse).toBe(0);

    const a = chunkedMessages('test.a', { a: 'x'.repeat(700) }, 'big', 100);
    const b = chunkedMessages('test.b', { b: 'y'.repeat(400) }, 'small', 100);
    nc.subs[1].callback(null, a.header);
    nc.subs[1].callback(null, a.chunks[0]);
    expect((client as any).chunkBudget.inUse).toBeGreaterThan(700);
    nc.subs[2].callback(null, b.header);
    nc.subs[2].callback(null, b.chunks[0]);
    expect(error.mock.calls.some((call) => String(call[1]).includes('Transfers in progress would hold more than 1000 bytes'))).toBe(true);

    for (const chunk of a.chunks.slice(1)) nc.subs[1].callback(null, chunk);
    expect(got).toEqual([{ a: 'x'.repeat(700) }]);
    expect((client as any).chunkBudget.inUse).toBe(0);

    for (const msg of [b.header, ...b.chunks]) nc.subs[2].callback(null, msg);
    expect(got).toEqual([{ a: 'x'.repeat(700) }, { b: 'y'.repeat(400) }]);
    client.abortClose();
  });

  it('returns the budget of a subscription when it is unsubscribed', async () => {
    const { client, nc } = createClient();
    const unsubscribe = await client.subscribe('test.gone', () => {});
    const { header, chunks } = chunkedMessages('test.gone', { a: 'x'.repeat(500) }, 'half', 100);
    nc.subs[0].callback(null, header);
    nc.subs[0].callback(null, chunks[0]);
    expect((client as any).chunkBudget.inUse).toBeGreaterThan(0);

    unsubscribe();

    expect((client as any).chunkBudget.inUse).toBe(0);
    expect((client as any).chunkScopes.size).toBe(0);
  });

  it('drops the half-assembled reply when a plain reply settles the call', async () => {
    const { client, nc } = createClient({ timeout: 1000 });
    const promise = client.call('rpc.test.big');
    const req = decode(nc.published[0].data) as RPCMessage;
    const { header, chunks } = chunkedMessages(`rpc.reply.${req.id}`, { id: req.id, result: 'x'.repeat(500) }, 'cut', 100);
    const sub = muxSub(client, nc);
    sub.callback(null, header);
    sub.callback(null, chunks[0]);
    expect((client as any).chunkBudget.inUse).toBeGreaterThan(0);

    sub.callback(null, { subject: `rpc.reply.${req.id}`, data: encode({ id: req.id, error: { code: 'INTERNAL_ERROR', message: 'send failed' } }) });

    await expect(promise).rejects.toMatchObject({ code: 'INTERNAL_ERROR', message: 'send failed' });
    expect((client as any).muxChunking.size).toBe(0);
    expect((client as any).chunkBudget.inUse).toBe(0);
  });

  it('keeps a transfer of a queue group when another member unsubscribes', async () => {
    const { client, nc } = createClient();
    const got: unknown[] = [];
    const collect = (data: unknown) => {
      got.push(data);
    };
    await client.subscribe('test.queue', collect, { queue: 'workers' });
    const leave = await client.subscribe('test.queue', collect, { queue: 'workers' });
    const payload = { big: 'z'.repeat(700) };
    const { header, chunks } = chunkedMessages('test.queue', payload, 'stay', 128);
    nc.subs[1].callback(null, header);
    nc.subs[0].callback(null, chunks[0]);

    leave();
    for (const chunk of chunks.slice(1)) nc.subs[0].callback(null, chunk);

    expect(got).toEqual([payload]);
    expect((client as any).chunkScopes.size).toBe(1);
  });

  it('refuses a chunked reply while handler subscriptions hold the budget', async () => {
    const { client, nc } = createClient({ maxTransferSize: 1000, timeout: 1000 });
    vi.spyOn(console, 'error').mockImplementation(() => {});
    await client.subscribe('test.hold', () => {});
    const held = chunkedMessages('test.hold', { a: 'x'.repeat(700) }, 'held', 100);
    nc.subs[0].callback(null, held.header);
    nc.subs[0].callback(null, held.chunks[0]);

    const promise = client.call('rpc.test.big');
    const req = decode(nc.published[0].data) as RPCMessage;
    const reply = chunkedMessages(`rpc.reply.${req.id}`, { id: req.id, result: 'y'.repeat(400) }, 'reply', 100);
    const sub = muxSub(client, nc);
    sub.callback(null, reply.header);
    sub.callback(null, reply.chunks[0]);

    await expect(promise).rejects.toMatchObject({
      code: 'INTERNAL_ERROR',
      message: expect.stringContaining('Transfers in progress would hold more than 1000 bytes'),
    });
    client.abortClose();
  });
});

describe('service endpoints', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('cap open transfers per endpoint and drop them when the service stops', async () => {
    const { client, nc } = createClient();
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    // the parts of a connection and its subscriptions the nats services API uses
    Object.assign(nc, { addCloseListener() {}, removeCloseListener() {} });
    nc.subscribe = (pattern: string, opts: { callback: (err: Error | null, msg: any) => void }) => {
      const sub = { pattern, callback: opts.callback, closed: new Promise(() => {}), drain: () => Promise.resolve(), unsubscribe() {}, isClosed: () => false };
      nc.subs.push(sub as unknown as MockSub);
      return sub as unknown as MockSub;
    };
    client.service.init(nc as unknown as NatsConnection);
    const service = await client.service.registerHandler({ name: 'svc', version: '1.0.0' }, { a: () => 1, b: () => 2 });
    const endpoint = (subject: string) => nc.subs.find((sub) => sub.pattern === subject)!;
    const open = () => [...((client as any).endpointChunking as Set<ChunkingManager>)].reduce((sum, manager) => sum + manager.size, 0);

    for (let i = 0; i < 32; i++) endpoint('a').callback(null, headerOnly('a', `a${i}`, 200, 100));
    endpoint('b').callback(null, headerOnly('b', 'b0', 200, 100));
    expect(open()).toBe(33);
    expect(error).not.toHaveBeenCalled();

    endpoint('a').callback(null, headerOnly('a', 'a32', 200, 100));
    expect(open()).toBe(33);
    expect(String(error.mock.calls[0][1])).toContain('Too many concurrent chunked transfers (32)');

    await service.stop();
    expect(open()).toBe(0);
    expect((client as any).endpointChunking.size).toBe(0);
  });
});
