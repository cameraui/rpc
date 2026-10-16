import { connect } from '@nats-io/transport-node';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { RPCClient } from '../src/client.js';
import { decode, encode } from '../src/codec.js';
import { RPCException } from '../src/errors.js';
import { sleep } from '../src/utils.js';
import { chunkedMessages, createClient, createMockNc } from './mock-nc.js';

import type { NatsConnection } from '@nats-io/transport-node';
import type { RPCResponse } from '../src/types.js';

vi.mock('@nats-io/transport-node', async (importOriginal) => ({ ...(await importOriginal<typeof import('@nats-io/transport-node')>()), connect: vi.fn() }));

async function register(handlers: object, options?: Parameters<ReturnType<typeof createClient>['client']['registerHandler']>[2]) {
  const ctx = createClient();
  await ctx.client.registerHandler('ns', handlers, options);
  const deliver = (method: string, id: string, params: unknown[] = [], extra: Record<string, unknown> = {}) => {
    const sub = ctx.nc.subs.findLast((s) => s.pattern === `rpc.ns.${method}` && !s.isClosed());
    expect(sub).toBeDefined();
    sub!.callback(null, { subject: `rpc.ns.${method}`, data: encode({ id, method: 'call', params, ...extra }) });
  };
  const replies = () => ctx.nc.published.filter((p) => p.subject.startsWith('rpc.reply.')).map((p) => ({ subject: p.subject, body: decode(p.data) as RPCResponse }));
  return { ...ctx, deliver, replies };
}

describe('registerHandler normal calls', () => {
  it('answers a handler that returns a plain value before the callback returns', async () => {
    const { deliver, replies } = await register({ echo: (v: unknown) => v });

    deliver('echo', 'a.1', ['hi']);

    expect(replies()).toEqual([{ subject: 'rpc.reply.a.1', body: { id: 'a.1', result: 'hi' } }]);
  });

  it('keeps per-method order when an async call is followed by a sync one', async () => {
    const { deliver, replies } = await register({
      work: async (ms: number) => {
        await sleep(ms);
        return ms;
      },
    });
    const order: string[] = [];

    deliver('work', 'a.1', [15]);
    deliver('work', 'a.2', [0]);
    await vi.waitFor(() => expect(replies().length).toBe(2));
    for (const reply of replies()) order.push(reply.body.id);

    expect(order).toEqual(['a.1', 'a.2']);
  });

  it('awaits a thenable that is not a Promise', async () => {
    const { deliver, replies } = await register({
      lazy: () => ({ then: (resolve: (v: unknown) => void) => setTimeout(() => resolve('later'), 1) }),
    });

    deliver('lazy', 'a.1');
    await vi.waitFor(() => expect(replies().length).toBe(1));

    expect(replies()[0].body).toEqual({ id: 'a.1', result: 'later' });
  });

  it('turns a synchronous throw into an error reply, with the method list for METHOD_NOT_FOUND', async () => {
    const { deliver, replies } = await register({
      fail: () => {
        throw new RPCException('METHOD_NOT_FOUND', 'gone');
      },
      boom: () => {
        throw new Error('plain');
      },
    });

    deliver('fail', 'a.1');
    deliver('boom', 'a.2');
    await vi.waitFor(() => expect(replies().length).toBe(2));

    expect(replies()[0].body).toEqual({ id: 'a.1', error: { code: 'METHOD_NOT_FOUND', message: 'gone' }, __methods: ['fail', 'boom'] });
    expect(replies()[1].body).toEqual({ id: 'a.2', error: { code: 'INTERNAL_ERROR', message: 'plain' } });
  });

  it('stamps __timing and attaches __methods on request for a sync handler', async () => {
    const { deliver, replies } = await register({ echo: (v: unknown) => v });

    deliver('echo', 'a.1', [1], { __timing: true, __discover: true });

    const body = replies()[0].body;
    expect(body.result).toBe(1);
    expect(body.__methods).toEqual(['echo']);
    expect(body.__timing!.end).toBeGreaterThanOrEqual(body.__timing!.start);
  });

  it('falls back to a chunked publish for a large sync result', async () => {
    const { client, nc, deliver } = await register({ big: () => 'x'.repeat(500) });
    (client as any)._maxPayloadSize = 100;

    deliver('big', 'a.1');
    await vi.waitFor(() => expect(nc.published.length).toBeGreaterThan(1));

    const header = nc.published[0];
    expect(header.subject).toBe('rpc.reply.a.1');
    expect(header.opts?.headers?.get('x-chunked-transfer')).toBe('header');
  });
});

describe('registerHandler concurrency option', () => {
  function slowHandlers() {
    const inFlight = { slow: 0, slowSerial: 0 };
    const max = { slow: 0, slowSerial: 0 };
    const tracked = (name: keyof typeof inFlight) => async () => {
      inFlight[name]++;
      max[name] = Math.max(max[name], inFlight[name]);
      await sleep(20);
      inFlight[name]--;
      return 'done';
    };
    return { handlers: { slow: tracked('slow'), slowSerial: tracked('slowSerial') }, max };
  }

  it('runs an opted-in method up to its limit and keeps others serial', async () => {
    const h = slowHandlers();
    const { deliver, replies } = await register(h.handlers, { concurrency: { slow: 3 } });

    for (let i = 0; i < 6; i++) deliver('slow', `a.${i}`);
    for (let i = 0; i < 3; i++) deliver('slowSerial', `s.${i}`);
    await vi.waitFor(() => expect(replies().length).toBe(9));
    expect(h.max).toEqual({ slow: 3, slowSerial: 1 });

    const serial = slowHandlers();
    const second = await register(serial.handlers, { concurrency: { slow: 1 } });
    for (let i = 0; i < 3; i++) second.deliver('slowSerial', `b.${i}`);
    for (let i = 0; i < 3; i++) second.deliver('slow', `c.${i}`);
    await vi.waitFor(() => expect(second.replies().length).toBe(6));
    expect(serial.max).toEqual({ slow: 1, slowSerial: 1 });
  });

  it('keeps the in-flight count when the subscription is restored', async () => {
    const h = slowHandlers();
    const { client, deliver, replies } = await register(h.handlers, { concurrency: { slow: 2 } });
    const entry = [...((client as any).subscriptionEntries as Map<number, any>).values()].find((e) => e.pattern === 'rpc.ns.slow');
    const dispatcher = entry.dispatcher;
    expect(dispatcher).toBeDefined();

    for (let i = 0; i < 2; i++) deliver('slow', `a.${i}`);
    entry.sub.unsubscribe();
    (client as any).natsSubscribe(entry);
    expect(entry.dispatcher).toBe(dispatcher);
    for (let i = 2; i < 4; i++) deliver('slow', `a.${i}`);

    await vi.waitFor(() => expect(replies().length).toBe(4));
    expect(h.max.slow).toBe(2);
  });

  it('runs a callback setup of an opted-in method alone', async () => {
    let inFlight = 0;
    const starts: string[] = [];
    const { deliver, replies } = await register(
      {
        sub: async (...args: unknown[]) => {
          const setup = typeof args[args.length - 1] === 'function';
          starts.push(`${setup ? 'setup' : 'call'}:${inFlight}`);
          inFlight++;
          await sleep(20);
          inFlight--;
          return setup ? () => {} : 'ok';
        },
      },
      { concurrency: { sub: 2 } },
    );

    deliver('sub', 'a.1');
    deliver('sub', 'a.2');
    deliver('sub', 'a.3', [{ __callback: true, __callbackSubject: 'rpc.cb.test.1', args: [] }]);
    deliver('sub', 'a.4');
    deliver('sub', 'a.5');
    await vi.waitFor(() => expect(replies().length).toBe(5));

    // the number is how many handlers were already running when this one started
    expect(starts).toEqual(['call:0', 'call:1', 'setup:0', 'call:0', 'call:1']);
  });
});

describe('registerHandler readiness', () => {
  afterEach(() => {
    vi.mocked(connect).mockReset();
  });

  // a promise whose settle functions the test holds
  function deferred() {
    let resolve!: () => void;
    let reject!: (error: Error) => void;
    const promise = new Promise<void>((res, rej) => {
      resolve = res;
      reject = rej;
    });
    return { promise, resolve, reject };
  }

  it('queues every SUB first and returns once the server answered one PING', async () => {
    const { client, nc } = createClient();
    const pong = deferred();
    let subsAtFlush = -1;
    nc.onFlush = () => {
      subsAtFlush = nc.subs.length;
      return pong.promise;
    };
    let ready = false;

    const registering = client.registerHandler('ns', { a: () => 1, b: () => 2, c: () => 3 }).then(() => {
      ready = true;
    });
    await sleep(5);
    expect(ready).toBe(false);
    // three methods plus rpc.ns.__methods, then a single flush
    expect(nc.subs.map((s) => s.pattern)).toEqual(['rpc.ns.a', 'rpc.ns.b', 'rpc.ns.c', 'rpc.ns.__methods']);
    expect(subsAtFlush).toBe(4);
    expect(nc.calls).toEqual(['flush']);

    pong.resolve();
    await registering;
    expect(ready).toBe(true);
  });

  it('does not fail when the PING is not answered', async () => {
    const { client, nc } = createClient();
    nc.onFlush = () => Promise.reject(new Error('connection closed'));

    await expect(client.registerHandler('ns', { a: () => 1 })).resolves.toBeTypeOf('function');
  });

  it('flushes once after connect() restored the subscriptions of a suspended client', async () => {
    const first = createMockNc();
    vi.mocked(connect).mockResolvedValueOnce(first as unknown as NatsConnection);
    const client = new RPCClient({ servers: ['nats://127.0.0.1:4222'], name: 'ready' });
    await client.connect();
    // a fresh connection has only the reply inbox to subscribe, nothing waits for it
    expect(first.calls).toEqual([]);
    await client.registerHandler('ns', { a: () => 1, b: () => 2 });
    await client.suspend();

    const second = createMockNc();
    const pong = deferred();
    second.onFlush = () => pong.promise;
    vi.mocked(connect).mockResolvedValueOnce(second as unknown as NatsConnection);
    let connected = false;
    const connecting = client.connect().then(() => {
      connected = true;
    });
    await sleep(5);
    expect(second.subs.map((s) => s.pattern).filter((p) => p.startsWith('rpc.ns.'))).toEqual(['rpc.ns.a', 'rpc.ns.b', 'rpc.ns.__methods']);
    expect(second.calls).toEqual(['flush']);
    expect(connected).toBe(false);

    pong.resolve();
    await connecting;
    expect(connected).toBe(true);
  });
});

describe('registerHandler namespaceSubscription', () => {
  async function registerNs(handlers: object, options: Parameters<RPCClient['registerHandler']>[2] = {}) {
    const ctx = createClient();
    const subscribe = vi.spyOn(ctx.nc, 'subscribe');
    const cleanup = await ctx.client.registerHandler('ns', handlers, { namespaceSubscription: true, ...options });
    const deliver = (subject: string, message: unknown, headers?: unknown) => {
      const sub = ctx.nc.subs.find((s) => s.pattern === 'rpc.ns.>' && !s.isClosed());
      expect(sub).toBeDefined();
      sub!.callback(null, { subject, data: message instanceof Uint8Array ? message : encode(message), headers });
    };
    const call = (method: string, id: string, params: unknown[] = []) => deliver(`rpc.ns.${method}`, { id, method: 'call', params });
    const replies = () => ctx.nc.published.filter((p) => p.subject.startsWith('rpc.reply.')).map((p) => decode(p.data) as RPCResponse);
    return { ...ctx, subscribe, cleanup, deliver, call, replies };
  }

  it('serves every method, also a nested one, through one subscription that keeps the queue', async () => {
    const { subscribe, call, replies } = await registerNs({ echo: (v: unknown) => v, db: { find: (key: string) => `found ${key}` } }, { queue: 'workers' });

    expect(subscribe.mock.calls.map(([pattern, opts]) => [pattern, (opts as { queue?: string }).queue])).toEqual([['rpc.ns.>', 'workers']]);
    call('echo', 'a.1', ['hi']);
    call('db.find', 'a.2', ['k']);
    expect(replies()).toEqual([
      { id: 'a.1', result: 'hi' },
      { id: 'a.2', result: 'found k' },
    ]);
  });

  it('answers a method it does not have at once with METHOD_NOT_FOUND and the method list, and __methods as before', async () => {
    const { call, deliver, nc, replies } = await registerNs({ echo: (v: unknown) => v });

    call('missing', 'a.1');
    call('__methods', 'a.2');
    deliver('rpc.ns.gone', { id: 's.1', method: 'stream', params: { __stream: true, __streamSubject: 'stream.rpc.ns.gone.s.1', args: [] } });

    expect(replies()).toEqual([
      { id: 'a.1', error: { code: 'METHOD_NOT_FOUND', message: 'Method not found: missing' }, __methods: ['echo'] },
      { id: 'a.2', result: ['echo'] },
    ]);
    expect(nc.published.filter((p) => p.subject === 'stream.rpc.ns.gone.s.1').map((p) => decode(p.data))).toEqual([
      { id: 's.1', type: 'error', error: { code: 'METHOD_NOT_FOUND', message: 'Method not found: gone' } },
    ]);
  });

  it('keeps the calls of one method in order without holding up another method', async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const order: string[] = [];
    const { call, replies } = await registerNs({
      slow: async (id: string) => {
        order.push(`start ${id}`);
        await gate;
        order.push(`end ${id}`);
        return id;
      },
      quick: (id: string) => {
        order.push(`quick ${id}`);
        return id;
      },
    });

    call('slow', 's.1', ['s.1']);
    call('slow', 's.2', ['s.2']);
    call('quick', 'q.1', ['q.1']);
    expect(order).toEqual(['start s.1', 'quick q.1']);
    expect(replies().map((r) => r.id)).toEqual(['q.1']);

    release();
    await vi.waitFor(() => expect(replies().length).toBe(3));
    expect(order).toEqual(['start s.1', 'quick q.1', 'end s.1', 'start s.2', 'end s.2']);
  });

  it('applies the concurrency option per method', async () => {
    let inFlight = 0;
    let max = 0;
    const { call, replies } = await registerNs(
      {
        detect: async () => {
          max = Math.max(max, ++inFlight);
          await sleep(10);
          inFlight--;
          return 'ok';
        },
      },
      { concurrency: { detect: 3 } },
    );

    for (let i = 0; i < 6; i++) call('detect', `d.${i}`);
    await vi.waitFor(() => expect(replies().length).toBe(6));
    expect(max).toBe(3);
  });

  it('routes a chunked request by the subject it came on', async () => {
    const { deliver, replies } = await registerNs({ echo: (v: unknown) => v });
    const big = 'x'.repeat(500);
    const { header, chunks } = chunkedMessages('rpc.ns.echo', { id: 'a.1', method: 'call', params: [big] }, 't1', 64);

    deliver(header.subject, header.data, header.headers);
    for (const chunk of chunks) deliver(chunk.subject, chunk.data, chunk.headers);
    await vi.waitFor(() => expect(replies().length).toBe(1));
    expect(replies()[0]).toEqual({ id: 'a.1', result: big });
  });

  it('gives every method its own 32 open chunked transfers', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const { client, cleanup, deliver, replies } = await registerNs({ a: (v: unknown) => v, b: (v: unknown) => v });
    const request = (method: string, id: string, payload: string) => chunkedMessages(`rpc.ns.${method}`, { id, method: 'call', params: [payload] }, id, 64);

    for (let i = 0; i < 33; i++) {
      const { header } = request('a', `a.${i}`, 'x'.repeat(200));
      deliver(header.subject, header.data, header.headers);
    }
    expect(error).toHaveBeenCalledTimes(1);
    expect(String(error.mock.calls[0][1])).toContain('Too many concurrent chunked transfers (32)');

    const big = 'y'.repeat(500);
    const { header, chunks } = request('b', 'b.1', big);
    for (const msg of [header, ...chunks]) deliver(msg.subject, msg.data, msg.headers);
    await vi.waitFor(() => expect(replies()).toEqual([{ id: 'b.1', result: big }]));
    expect(error).toHaveBeenCalledTimes(1);

    const scope = [...(client as any).chunkScopes.values()][0];
    expect([...scope.lanes.keys()]).toEqual(['a', 'b']);
    expect(scope.lanes.get('a').size).toBe(32);
    await cleanup();
    expect(scope.lanes.get('a').size).toBe(0);
    error.mockRestore();
  });

  it('opens no transfer lane for a method it does not have and still answers its chunked request', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const { client, deliver, replies } = await registerNs({ echo: (v: unknown) => v });

    for (let i = 0; i < 40; i++) {
      const { header } = chunkedMessages(`rpc.ns.m${i}`, { id: `u.${i}`, method: 'call', params: ['x'.repeat(200)] }, `u.${i}`, 64);
      deliver(header.subject, header.data, header.headers);
    }
    const scope = [...(client as any).chunkScopes.values()][0];
    expect(scope.lanes).toBeUndefined();
    expect(scope.manager.size).toBe(32);

    scope.manager.dispose();
    const { header, chunks } = chunkedMessages('rpc.ns.missing', { id: 'm.1', method: 'call', params: ['z'.repeat(200)] }, 'm.1', 64);
    for (const msg of [header, ...chunks]) deliver(msg.subject, msg.data, msg.headers);
    expect(replies()).toEqual([{ id: 'm.1', error: { code: 'METHOD_NOT_FOUND', message: 'Method not found: missing' }, __methods: ['echo'] }]);
    expect(scope.lanes).toBeUndefined();
    error.mockRestore();
  });

  it('drains the namespace subscription on cleanup', async () => {
    const { cleanup, nc } = await registerNs({ echo: (v: unknown) => v });

    await cleanup();
    expect(nc.subs.find((s) => s.pattern === 'rpc.ns.>')!.ended).toBe('drain');
  });

  it('refuses a namespace whose first segment is reply or cb, before it subscribes', async () => {
    const { client, nc } = createClient();

    for (const namespace of ['reply', 'cb', 'reply.inbox', 'cb.x']) {
      await expect(client.registerHandler(namespace, { x: () => 1 }, { namespaceSubscription: true })).rejects.toThrow(
        `Namespace "${namespace}" overlaps internal subjects`,
      );
    }
    expect(nc.subs).toEqual([]);
    await client.registerHandler('replies', { x: () => 1 }, { namespaceSubscription: true });
    expect(nc.subs.map((s) => s.pattern)).toEqual(['rpc.replies.>']);
  });

  it('reads the subject of a message only where a namespace subscription routes by it', async () => {
    const plain = await register({ echo: (v: unknown) => v });
    const routed = await registerNs({ echo: (v: unknown) => v });
    // nats.js decodes the subject on first access
    let reads = 0;
    const message = (id: string) => ({
      data: encode({ id, method: 'call', params: ['hi'] }),
      get subject() {
        reads++;
        return 'rpc.ns.echo';
      },
    });

    plain.nc.subs.find((s) => s.pattern === 'rpc.ns.echo')!.callback(null, message('a.1'));
    expect(reads).toBe(0);
    routed.nc.subs.find((s) => s.pattern === 'rpc.ns.>')!.callback(null, message('a.2'));
    expect(reads).toBe(1);
    expect(plain.replies().map((r) => r.body)).toEqual([{ id: 'a.1', result: 'hi' }]);
    expect(routed.replies()).toEqual([{ id: 'a.2', result: 'hi' }]);
  });
});

describe('expired requests', () => {
  it('stamps what is left of the timeout on rpc requests when they go out, not on service calls', async () => {
    const { client, nc } = createClient({ timeout: 5000 });
    const sentTo = (subject: string) => nc.published.filter((p) => p.subject === subject).map((p) => decode(p.data) as { __ttl?: number });

    void client.call('rpc.ns.read').catch(() => {});
    expect(sentTo('rpc.ns.read')[0].__ttl).toBeGreaterThan(4990);
    expect(sentTo('rpc.ns.read')[0].__ttl).toBeLessThanOrEqual(5000);

    (client as any).linkWaiters = [];
    void client.call('rpc.ns.held').catch(() => {});
    await sleep(60);
    (client as any).releaseLinkWaiters();
    expect(sentTo('rpc.ns.held')[0].__ttl).toBeLessThanOrEqual(4945);

    void client.call('svc.read').catch(() => {});
    await vi.waitFor(() => expect(sentTo('svc.read').length).toBe(1));
    expect(sentTo('svc.read')[0].__ttl).toBeUndefined();
    await client.abortClose();
  });

  async function busyMethods(options: Parameters<RPCClient['registerHandler']>[2]) {
    let release!: () => void;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const ran: string[] = [];
    const ctx = createClient();
    const handlers = {
      read: async (id: string) => {
        ran.push(id);
        if (id === 'r.1') await held;
        return id;
      },
      write: async (id: string) => {
        ran.push(id);
        if (id === 'w.1') await held;
        return id;
      },
    };
    const cleanup = await ctx.client.registerHandler('ns', handlers, options);
    const send = (method: string, id: string, ttl?: number) => {
      const subject = `rpc.ns.${method}`;
      const sub = ctx.nc.subs.find((s) => (s.pattern === subject || s.pattern === 'rpc.ns.>') && !s.isClosed())!;
      sub.callback(null, { subject, data: encode({ id, method: 'call', params: [id], ...(ttl === undefined ? {} : { __ttl: ttl }) }) });
    };
    const replied = () => ctx.nc.published.filter((p) => p.subject.startsWith('rpc.reply.')).map((p) => (decode(p.data) as RPCResponse).id);
    return { ...ctx, cleanup, ran, release, send, replied };
  }

  it('skips a request of an opted-in method that waited longer than its ttl, without an answer', async () => {
    const { ran, release, send, replied } = await busyMethods({ dropExpired: ['read'] });

    send('read', 'r.1', 1000);
    send('read', 'r.2', 10);
    send('read', 'r.3');
    send('read', 'r.4', 1000);
    send('write', 'w.1', 1000);
    send('write', 'w.2', 10);
    await sleep(40);
    release();

    await vi.waitFor(() => expect(replied().length).toBe(5));
    expect(ran.filter((id) => id.startsWith('r.'))).toEqual(['r.1', 'r.3', 'r.4']);
    expect(ran.filter((id) => id.startsWith('w.'))).toEqual(['w.1', 'w.2']);
    expect(replied().sort()).toEqual(['r.1', 'r.3', 'r.4', 'w.1', 'w.2']);
  });

  it('does the same through a namespace subscription and leaves a skipped request out of the drain', async () => {
    const { cleanup, ran, release, send, replied } = await busyMethods({ dropExpired: true, namespaceSubscription: true });

    send('read', 'r.1', 1000);
    send('read', 'r.2', 10);
    await sleep(40);
    release();
    await vi.waitFor(() => expect(replied()).toEqual(['r.1']));

    await cleanup();
    expect(ran).toEqual(['r.1']);
    expect(replied()).toEqual(['r.1']);
  });
});

describe('payload limit', () => {
  afterEach(() => {
    vi.mocked(connect).mockReset();
  });

  async function limitWith(serverLimit: number | undefined, maxPayloadSize: number | undefined): Promise<number> {
    const nc = Object.assign(createMockNc(), { info: serverLimit === undefined ? undefined : { max_payload: serverLimit } });
    vi.mocked(connect).mockResolvedValueOnce(nc as unknown as NatsConnection);
    const client = new RPCClient({ servers: ['nats://127.0.0.1:4222'], name: 'limit', maxPayloadSize });
    await client.connect();
    return client.maxPayloadSize;
  }

  it('takes the smaller of the server limit and the option, less 8 KiB', async () => {
    const MiB = 1024 * 1024;
    expect(await limitWith(4 * MiB, undefined)).toBe(4 * MiB - 8192);
    expect(await limitWith(4 * MiB, MiB)).toBe(MiB - 8192);
    expect(await limitWith(MiB, 4 * MiB)).toBe(MiB - 8192);
    expect(await limitWith(undefined, 2 * MiB)).toBe(2 * MiB - 8192);
    expect(await limitWith(undefined, undefined)).toBe(MiB - 8192);
  });
});

describe('isolated clients across suspend() and connect()', () => {
  afterEach(() => {
    vi.mocked(connect).mockReset();
  });

  it('reconnects isolated registrations and proxies and restores their subscriptions, not closed ones', async () => {
    const ncs = Array.from({ length: 5 }, () => createMockNc());
    for (const nc of ncs) vi.mocked(connect).mockResolvedValueOnce(nc as unknown as NatsConnection);
    const client = new RPCClient({ servers: ['nats://127.0.0.1:4222'], name: 'parent' });
    await client.connect();
    await client.registerHandler('iso', { ping: () => 'pong' }, { isolatedConnection: true });
    const closedProxy = client.createProxy<{ ping(): Promise<string> }>('gone', { isolatedConnection: true });
    await closedProxy.close();

    await client.suspend();
    expect(ncs[1].isClosed()).toBe(true);
    await client.connect();

    expect(vi.mocked(connect)).toHaveBeenCalledTimes(4);
    const isolated = ncs[3];
    expect(isolated.subs.filter((s) => !s.isClosed()).map((s) => s.pattern)).toContain('rpc.iso.ping');
    expect(ncs[2].subs.map((s) => s.pattern)).not.toContain('rpc.iso.ping');
  });
});
