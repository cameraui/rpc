import { describe, expect, it } from 'vitest';

import { decode, encode } from '../src/codec.js';
import { handlePullCallbackRequest, handlePullIteratorRequest } from '../src/handler.js';
import { createClient, drainMicrotasks, muxSub } from './mock-nc.js';

import type { RPCClient } from '../src/client.js';

function iteratorClient() {
  const { client, nc } = createClient();
  const sent = (subject: string) => nc.published.filter((p) => p.subject === subject).map((p) => decode(p.data) as any);
  const open = (pattern: string) => nc.subs.filter((sub) => sub.pattern === pattern && !sub.closed);
  return { client, nc, sent, open };
}

type Opener = (client: RPCClient) => AsyncGenerator<unknown>;

const flavours: [string, Opener][] = [
  ['pull iterator', (client) => client.callPullIterator('rpc.ns.play', 'cam')],
  ['pull-callback iterator', (client) => client.callPullIteratorWithCallback('rpc.ns.play', { onFrame: () => {} }, ['onFrame'], 'cam')],
];

describe.each(flavours)('%s return() while the init call is in flight', (_name, openIterator) => {
  it('sends the cancel once after the init reply and leaves nothing behind', async () => {
    const { client, nc, sent, open } = iteratorClient();
    const iter = openIterator(client);
    const first = iter.next();
    await drainMicrotasks();
    const [init] = sent('rpc.ns.play');
    const iteratorId: string = init.params[0].__iteratorId;
    const requests = `_rpc.iterator.${iteratorId}.request`;

    await expect(iter.return(undefined)).resolves.toEqual({ value: undefined, done: true });
    await drainMicrotasks();
    expect(sent(requests)).toEqual([]);

    muxSub(client, nc).callback(null, { subject: `rpc.reply.${init.id}`, data: encode({ id: init.id, result: { iteratorId } }) });
    await expect(first).resolves.toEqual({ value: undefined, done: true });
    await drainMicrotasks();

    expect(sent(requests)).toEqual([{ id: iteratorId, type: 'cancel' }]);
    expect(open(`_rpc.iterator.${iteratorId}.response`)).toEqual([]);
    expect(open(`_rpc.cb.${iteratorId}`)).toEqual([]);
    expect((client as any).statusHandlers.has(iteratorId)).toBe(false);
    expect((client as any).pullIteratorSettles.size).toBe(0);
  });

  it('cleans up once when the init call fails after return()', async () => {
    const { client, nc, sent, open } = iteratorClient();
    const iter = openIterator(client);
    const first = iter.next();
    await drainMicrotasks();
    const [init] = sent('rpc.ns.play');
    const iteratorId: string = init.params[0].__iteratorId;

    await iter.return(undefined);
    muxSub(client, nc).callback(null, { subject: `rpc.reply.${init.id}`, data: encode({ id: init.id, error: { code: 'INTERNAL_ERROR', message: 'no recording' } }) });
    await expect(first).rejects.toMatchObject({ message: 'no recording' });
    await drainMicrotasks();

    expect(sent(`_rpc.iterator.${iteratorId}.request`)).toEqual([{ id: iteratorId, type: 'cancel' }]);
    expect(open(`_rpc.iterator.${iteratorId}.response`)).toEqual([]);
    expect(open(`_rpc.cb.${iteratorId}`)).toEqual([]);
    expect((client as any).pullIteratorSettles.size).toBe(0);
  });
});

describe('pull-callback iterator callbacks after return()', () => {
  it('are dropped when they arrive before the init reply', async () => {
    const { client, nc, sent, open } = iteratorClient();
    const frames: unknown[] = [];
    const iter = client.callPullIteratorWithCallback('rpc.ns.play', { onFrame: (frame: unknown) => void frames.push(frame) }, ['onFrame'], 'cam');
    const first = iter.next();
    await drainMicrotasks();
    const [init] = sent('rpc.ns.play');
    const iteratorId: string = init.params[0].__iteratorId;

    await iter.return(undefined);
    const [callbacks] = open(`_rpc.cb.${iteratorId}`);
    callbacks.callback(null, { subject: `_rpc.cb.${iteratorId}`, data: encode({ method: 'onFrame', args: ['gop0'] }) });
    await drainMicrotasks();
    muxSub(client, nc).callback(null, { subject: `rpc.reply.${init.id}`, data: encode({ id: init.id, result: { iteratorId } }) });
    await first;
    await drainMicrotasks();

    expect(frames).toEqual([]);
  });
});

describe('pull-callback iterator return() before the init call went out', () => {
  it('sends neither the init nor a cancel', async () => {
    const { client, nc, open } = iteratorClient();
    const iter = client.callPullIteratorWithCallback('rpc.ns.play', { onFrame: () => {} }, ['onFrame'], 'cam');
    const first = iter.next();
    await iter.return(undefined);

    await expect(first).resolves.toEqual({ value: undefined, done: true });
    await drainMicrotasks();
    expect(nc.published.map((p) => p.subject)).toEqual([]);
    expect(nc.subs.filter((sub) => sub.pattern.startsWith('_rpc.cb.'))).toHaveLength(1);
    expect(open(nc.subs.find((sub) => sub.pattern.startsWith('_rpc.cb.'))!.pattern)).toEqual([]);
    expect((client as any).pullIteratorSettles.size).toBe(0);
  });
});

type Serve = (client: RPCClient, handler: (...args: any[]) => Promise<AsyncGenerator<unknown>>) => Promise<() => Promise<void>>;

const servers: [string, Serve][] = [
  ['pull session', (client, handler) => handlePullIteratorRequest(handler, [], 'p1', client)],
  ['pull-callback session', (client, handler) => handlePullCallbackRequest(handler, [], 'p1', '_rpc.cb.p1', ['onFrame'], ['onFrame'], client)],
];

describe.each(servers)('%s cancelled while its handler sets up', (_name, serve) => {
  it('ends right after the setup and still answers the init call', async () => {
    const { client, nc, sent, open } = iteratorClient();
    let release!: () => void;
    const setUp = new Promise<void>((resolve) => {
      release = resolve;
    });
    async function* produce() {
      for (;;) yield 'frame';
    }
    const producer = produce();
    const opening = serve(client, async () => {
      await setUp;
      return producer;
    });
    await drainMicrotasks();

    const request = open('_rpc.iterator.p1.request');
    expect(request).toHaveLength(1);
    request[0].callback(null, { subject: '_rpc.iterator.p1.request', data: encode({ id: 'p1', type: 'cancel' }) });
    await drainMicrotasks();
    release();

    await expect(opening).resolves.toBeTypeOf('function');
    await drainMicrotasks();
    expect(sent('_rpc.iterator.p1.response')).toEqual([{ id: 'p1', type: 'done' }]);
    expect(open('_rpc.iterator.p1.request')).toEqual([]);
    await expect(producer.next()).resolves.toEqual({ value: undefined, done: true });
    expect(nc.published.filter((p) => p.subject === '_rpc.cb.p1')).toEqual([]);
  });
});
