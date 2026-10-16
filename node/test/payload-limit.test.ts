import { connect } from '@nats-io/transport-node';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { createRPCClient, RPCClient } from '../src/client.js';
import { decode, encode } from '../src/codec.js';
import { sleep } from '../src/utils.js';
import { createMockNc, hostServices } from './mock-nc.js';

import type { NatsConnection } from '@nats-io/transport-node';

vi.mock('@nats-io/transport-node', async (importOriginal) => ({ ...(await importOriginal<typeof import('@nats-io/transport-node')>()), connect: vi.fn() }));

const BASE = { servers: ['nats://127.0.0.1:4222'], name: 'payload-limit', noResponderRetry: { maxRetries: 0 } };
const TOO_SMALL = { code: 'PAYLOAD_TOO_LARGE', message: 'Payload limit 4096 is too small for chunking' };

async function connected(serverMaxPayload: number, maxPayloadSize?: number) {
  const nc = createMockNc();
  Object.assign(nc, { info: { max_payload: serverMaxPayload } });
  vi.mocked(connect).mockResolvedValueOnce(nc as unknown as NatsConnection);
  const client = new RPCClient({ ...BASE, maxPayloadSize });
  await client.connect();
  const sentTo = (subject: string) => nc.published.filter((p) => p.subject === subject);
  return { client, nc, sentTo };
}

// hands a message to the live subscription of `subject`
function deliver(nc: ReturnType<typeof createMockNc>, subject: string, message: unknown): void {
  nc.subs.find((sub) => sub.pattern === subject && !sub.isClosed())!.callback(null, { subject, data: encode(message), respond() {}, respondError() {} });
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

describe('payload limit floor', () => {
  afterEach(() => {
    vi.mocked(connect).mockReset();
  });

  it('refuses a maxPayloadSize below 16384 or not an integer where the client is created', () => {
    for (const size of [1, 8192, 16383, -1, Number.NaN, 20000.5, Number.POSITIVE_INFINITY]) {
      expect(() => new RPCClient({ ...BASE, maxPayloadSize: size })).toThrow(RangeError);
      expect(() => createRPCClient({ ...BASE, maxPayloadSize: size })).toThrow(`maxPayloadSize must be an integer of at least 16384, got ${size}`);
    }
    for (const size of [undefined, 0, 16384, 1024 * 1024]) {
      expect(() => new RPCClient({ ...BASE, maxPayloadSize: size })).not.toThrow();
    }
  });

  it('sends a message that fits a server max_payload below 16384 whole and fails a bigger one at once', async () => {
    const { client, nc, sentTo } = await connected(4096);
    expect(client.maxPayloadSize).toBe(4096);

    await client.publish('test.small', { text: 'x'.repeat(100) });
    await client.publish('test.edge', 'y'.repeat(4000));
    expect(sentTo('test.small').map((p) => [decode(p.data), p.opts?.headers])).toEqual([[{ text: 'x'.repeat(100) }, undefined]]);
    expect(sentTo('test.edge')).toHaveLength(1);

    await expect(client.publish('test.big', 'z'.repeat(5000))).rejects.toMatchObject({
      code: 'PAYLOAD_TOO_LARGE',
      message: 'Payload limit 4096 is too small for chunking',
    });
    expect(sentTo('test.big')).toEqual([]);
    expect(nc.published.some((p) => p.opts?.headers?.get?.('x-chunked-transfer'))).toBe(false);
    await client.disconnect();
  });

  it('no longer chunks below 16384 where the limit left a few KiB per chunk', async () => {
    const { client, sentTo } = await connected(12_000);

    await client.publish('test.whole', 'w'.repeat(10_000));
    expect(sentTo('test.whole')).toHaveLength(1);
    expect(sentTo('test.whole')[0].opts?.headers).toBeUndefined();
    await expect(client.publish('test.big', 'z'.repeat(13_000))).rejects.toMatchObject({
      code: 'PAYLOAD_TOO_LARGE',
      message: 'Payload limit 12000 is too small for chunking',
    });
    await client.disconnect();
  });

  it('fails a call whose request needs chunking at once instead of at its timeout', async () => {
    const { client, sentTo } = await connected(4096);

    const outcome = await within(client.call('rpc.ns.echo', 'z'.repeat(5000)), 200);
    expect(outcome).toMatchObject({ code: 'PAYLOAD_TOO_LARGE', message: 'Payload limit 4096 is too small for chunking' });
    expect(sentTo('rpc.ns.echo')).toEqual([]);
    expect((client as any).pendingRequests.size).toBe(0);
    await client.disconnect();
  });

  it('answers a call whose result needs chunking with PAYLOAD_TOO_LARGE', async () => {
    const { client, nc, sentTo } = await connected(4096);
    await client.registerHandler('ns', { big: () => 'b'.repeat(5000), later: async () => 'l'.repeat(5000) });
    const sub = (method: string) => nc.subs.find((s) => s.pattern === `rpc.ns.${method}`)!;

    sub('big').callback(null, { subject: 'rpc.ns.big', data: encode({ id: 'a.1', method: 'call', params: [] }) });
    sub('later').callback(null, { subject: 'rpc.ns.later', data: encode({ id: 'a.2', method: 'call', params: [] }) });

    const error = { code: 'PAYLOAD_TOO_LARGE', message: 'Payload limit 4096 is too small for chunking' };
    await vi.waitFor(() => expect(sentTo('rpc.reply.a.2')).toHaveLength(1));
    expect(sentTo('rpc.reply.a.1').map((p) => decode(p.data))).toEqual([{ id: 'a.1', error }]);
    expect(sentTo('rpc.reply.a.2').map((p) => decode(p.data))).toEqual([{ id: 'a.2', error }]);
    await client.disconnect();
  });

  it('ends a stream whose item needs chunking with PAYLOAD_TOO_LARGE', async () => {
    const { client, nc, sentTo } = await connected(4096);
    await client.registerHandler('ns', {
      async *items() {
        yield 'small';
        yield 'i'.repeat(5000);
        yield 'never';
      },
    });
    const streamSubject = 'stream.rpc.ns.items.s1';

    nc.subs
      .find((s) => s.pattern === 'rpc.ns.items')!
      .callback(null, { subject: 'rpc.ns.items', data: encode({ id: 's1', method: 'stream', params: { __stream: true, __streamSubject: streamSubject, args: [] } }) });

    await vi.waitFor(() => expect(sentTo(streamSubject)).toHaveLength(2));
    expect(sentTo(streamSubject).map((p) => decode(p.data))).toEqual([
      { id: 's1', type: 'data', data: 'small' },
      { id: 's1', type: 'error', error: { code: 'PAYLOAD_TOO_LARGE', message: 'Payload limit 4096 is too small for chunking' } },
    ]);
    await client.disconnect();
  });

  it('answers a service call whose result needs chunking with PAYLOAD_TOO_LARGE', async () => {
    const { client, nc, sentTo } = await connected(4096);
    hostServices(nc);
    await client.service.registerHandler({ name: 'alpha', version: '1.0.0' }, { big: () => 'b'.repeat(5000) });

    deliver(nc, 'svc.alpha.big', { id: 'a.1', method: 'call', params: [] });

    await vi.waitFor(() => expect(sentTo('svc.alpha.big.reply.a.1')).toHaveLength(1));
    expect(sentTo('svc.alpha.big.reply.a.1').map((p) => decode(p.data))).toEqual([{ id: 'a.1', error: TOO_SMALL }]);
    await client.disconnect();
  });

  it('ends a pull-callback session whose frame needs chunking with PAYLOAD_TOO_LARGE', async () => {
    const { client, nc, sentTo } = await connected(4096);
    await client.registerHandler('ns', {
      async *play(callbacks: { onFrame(frame: string): void }) {
        for (let batch = 0; batch < 3; batch++) {
          callbacks.onFrame(`small ${batch}`);
          callbacks.onFrame('f'.repeat(5000));
          yield;
        }
      },
    });

    deliver(nc, 'rpc.ns.play', { id: 'c.1', method: 'call', params: [pullCallbackParams('p1')] });
    await vi.waitFor(() => expect(sentTo('rpc.reply.c.1')).toHaveLength(1));
    deliver(nc, '_rpc.iterator.p1.request', { id: 'p1', type: 'next' });

    await vi.waitFor(() => expect(sentTo('_rpc.iterator.p1.response')).toHaveLength(1));
    await sleep(20);
    expect(sentTo('_rpc.iterator.p1.response').map((p) => decode(p.data))).toEqual([{ id: 'p1', type: 'error', error: TOO_SMALL }]);
    expect(sentTo('_rpc.cb.p1').map((p) => decode(p.data))).toEqual([{ method: 'onFrame', args: ['small 0'] }]);
    await client.disconnect();
  });

  it.each([
    ['done', async () => {}],
    [
      'its own error',
      async () => {
        await sleep(5);
        throw new Error('stopped');
      },
    ],
  ])('answers the last step of a pull-callback session with PAYLOAD_TOO_LARGE instead of %s', async (_end, after) => {
    const { client, nc, sentTo } = await connected(4096);
    await client.registerHandler('ns', {
      async *play(callbacks: { onFrame(frame: string): void }) {
        callbacks.onFrame('f'.repeat(5000));
        await after();
      },
    });

    deliver(nc, 'rpc.ns.play', { id: 'c.1', method: 'call', params: [pullCallbackParams('p1')] });
    await vi.waitFor(() => expect(sentTo('rpc.reply.c.1')).toHaveLength(1));
    deliver(nc, '_rpc.iterator.p1.request', { id: 'p1', type: 'next' });

    await vi.waitFor(() => expect(sentTo('_rpc.iterator.p1.response')).toHaveLength(1));
    expect(sentTo('_rpc.iterator.p1.response').map((p) => decode(p.data))).toEqual([{ id: 'p1', type: 'error', error: TOO_SMALL }]);
    await client.disconnect();
  });

  it('closes the producer of a pull-callback session that a frame ended', async () => {
    const { client, nc, sentTo } = await connected(4096);
    let closed = false;
    await client.registerHandler('ns', {
      async *play(callbacks: { onFrame(frame: string): void }) {
        try {
          for (;;) {
            callbacks.onFrame('f'.repeat(5000));
            yield;
          }
        } finally {
          closed = true;
        }
      },
    });

    deliver(nc, 'rpc.ns.play', { id: 'c.1', method: 'call', params: [pullCallbackParams('p1')] });
    await vi.waitFor(() => expect(sentTo('rpc.reply.c.1')).toHaveLength(1));
    deliver(nc, '_rpc.iterator.p1.request', { id: 'p1', type: 'next' });

    await vi.waitFor(() => expect(sentTo('_rpc.iterator.p1.response')).toHaveLength(1));
    expect(sentTo('_rpc.iterator.p1.response').map((p) => decode(p.data))).toEqual([{ id: 'p1', type: 'error', error: TOO_SMALL }]);
    await vi.waitFor(() => expect(closed).toBe(true));
    await client.disconnect();
  });

  it('closes the producer of a pull iterator whose value needs chunking', async () => {
    const { client, nc, sentTo } = await connected(4096);
    let closed = false;
    await client.registerHandler('ns', {
      async *items() {
        try {
          for (;;) yield 'v'.repeat(5000);
        } finally {
          closed = true;
        }
      },
    });

    deliver(nc, 'rpc.ns.items', { id: 'c.1', method: 'call', params: { __pullIterator: true, __iteratorId: 'p1', args: [] } });
    await vi.waitFor(() => expect(sentTo('rpc.reply.c.1')).toHaveLength(1));
    deliver(nc, '_rpc.iterator.p1.request', { id: 'p1', type: 'next' });

    await vi.waitFor(() => expect(sentTo('_rpc.iterator.p1.response')).toHaveLength(1));
    expect(sentTo('_rpc.iterator.p1.response').map((p) => decode(p.data))).toEqual([{ id: 'p1', type: 'error', error: TOO_SMALL }]);
    await vi.waitFor(() => expect(closed).toBe(true));
    await client.disconnect();
  });

  it('keeps chunking from 16384 on, with the 8 KiB reserve', async () => {
    const { client, sentTo } = await connected(1024 * 1024, 16384);
    expect(client.maxPayloadSize).toBe(8192);

    await client.publish('test.chunked', 'c'.repeat(20_000));
    const sent = sentTo('test.chunked');
    expect(sent[0].opts?.headers?.get('x-chunked-transfer')).toBe('header');
    expect(decode(sent[0].data)).toMatchObject({ chunkSize: 8192, totalChunks: 3 });
    expect(sent).toHaveLength(4);
    await client.disconnect();
  });
});
