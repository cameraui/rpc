import { errors } from '@nats-io/transport-node';
import { describe, expect, it } from 'vitest';

import { createChunks } from '../src/chunking.js';
import { RPCClient } from '../src/client.js';
import { decode, encode } from '../src/codec.js';
import { RPCException } from '../src/errors.js';

import type { RPCMessage } from '../src/types.js';

/**
 * Minimal NATS connection mock. Captures subscriptions and published
 * messages; messages are delivered manually by invoking the captured
 * subscription callbacks.
 */
interface MockSub {
  pattern: string;
  callback: (err: Error | null, msg: any) => void;
  closed: boolean;
  unsubscribe(): void;
  isClosed(): boolean;
}

interface Published {
  subject: string;
  data: Uint8Array;
  opts?: { reply?: string; headers?: any };
}

function createMockNc() {
  const subs: MockSub[] = [];
  const published: Published[] = [];
  return {
    subs,
    published,
    subscribe(pattern: string, opts: { callback: (err: Error | null, msg: any) => void }): MockSub {
      const sub: MockSub = {
        pattern,
        callback: opts.callback,
        closed: false,
        unsubscribe() {
          this.closed = true;
        },
        isClosed() {
          return this.closed;
        },
      };
      subs.push(sub);
      return sub;
    },
    publish(subject: string, data: Uint8Array, opts?: { reply?: string; headers?: any }): void {
      published.push({ subject, data, opts });
    },
    isClosed: () => false,
  };
}

function createClient(options?: Partial<ConstructorParameters<typeof RPCClient>[0]>) {
  const client = new RPCClient({
    servers: ['nats://127.0.0.1:4222'],
    name: 'mux-test',
    // Retries would re-publish and slow the 503 tests down — disable.
    noResponderRetry: { maxRetries: 0, delays: [1] },
    ...options,
  });
  const nc = createMockNc();
  (client as any).nc = nc;
  return { client, nc };
}

/** The single wildcard mux subscription of a client. */
function muxSub(client: RPCClient, nc: ReturnType<typeof createMockNc>): MockSub {
  const prefix = (client as any).replyPrefix as string;
  const matches = nc.subs.filter((s) => s.pattern === `rpc.reply.${prefix}.>`);
  expect(matches.length).toBe(1);
  return matches[0];
}

function emptyHeaders(code = 0) {
  return { code, get: (_k: string) => undefined as string | undefined };
}

function headersWith(entries: Record<string, string>) {
  return { code: 0, get: (k: string) => entries[k] };
}

describe('muxed reply inbox', () => {
  it('routes a response to its pending call by envelope id', async () => {
    const { client, nc } = createClient();
    const prefix = (client as any).replyPrefix as string;

    const promise = client.call('rpc.test.echo', 'hello');

    // Request went out with reply = the call's own muxed reply subject.
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
    // Settled call leaves no pending entry behind.
    expect(((client as any).pendingRequests as Map<string, any>).size).toBe(0);
  });

  it('creates exactly one mux subscription across many calls', async () => {
    const { client, nc } = createClient();

    const p1 = client.call('rpc.a.one');
    const p2 = client.call('rpc.a.two');
    const p3 = client.call('rpc.a.three');

    const sub = muxSub(client, nc); // asserts count === 1
    for (const pub of nc.published) {
      const req = decode(pub.data) as RPCMessage;
      sub.callback(null, { subject: `rpc.reply.${req.id}`, data: encode({ id: req.id, result: req.params }) });
    }
    await Promise.all([p1, p2, p3]);
    // Only the wildcard mux — no per-call subscriptions.
    expect(nc.subs.length).toBe(1);
  });

  it('routes out-of-order responses of concurrent calls correctly', async () => {
    const { client, nc } = createClient();

    const p1 = client.call('rpc.test.first');
    const p2 = client.call('rpc.test.second');

    const [req1, req2] = nc.published.map((p) => decode(p.data) as RPCMessage);
    const sub = muxSub(client, nc);

    // Answer the second call first.
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

    // NATS no-responder status: empty payload + code 503 on the reply subject.
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

    // Force mux creation without an RPC call.
    (client as any).ensureMuxSubscription();
    const prefix = (client as any).replyPrefix as string;
    const token = `${prefix}.iter-token`;

    const received: Error[] = [];
    ((client as any).statusHandlers as Map<string, (err: Error) => void>).set(token, (err) => received.push(err));

    const sub = muxSub(client, nc);
    sub.callback(null, { subject: `rpc.reply.${token}`, data: new Uint8Array(0), headers: emptyHeaders(503) });

    expect(received.length).toBe(1);
    expect(received[0]).toBeInstanceOf(errors.NoRespondersError);
    // One-shot: the registration is consumed (max:1 inbox semantics) …
    expect(((client as any).statusHandlers as Map<string, any>).size).toBe(0);
    // … so a second status is dropped silently.
    sub.callback(null, { subject: `rpc.reply.${token}`, data: new Uint8Array(0), headers: emptyHeaders(503) });
    expect(received.length).toBe(1);
  });

  it('reassembles chunked responses and routes them by envelope id', async () => {
    const { client, nc } = createClient();

    const promise = client.call('rpc.test.big');
    const req = decode(nc.published[0].data) as RPCMessage;
    const replySubject = `rpc.reply.${req.id}`;
    const sub = muxSub(client, nc);

    // Build a chunked response exactly like publish() would.
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

    // Foreign/late response — must not settle or throw.
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

    // Late response after the timeout must be a no-op.
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
