import { errors } from '@nats-io/transport-node';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { decode, encode } from '../src/codec.js';
import { isNoRespondersError, RPCException } from '../src/errors.js';
import { createClient, emptyHeaders, muxSub } from './mock-nc.js';

import type { RPCMessage } from '../src/types.js';

const RETRY = { noResponderRetry: { maxRetries: 3, delays: [1] } };

function lastRequest(nc: ReturnType<typeof createClient>['nc']): RPCMessage {
  return decode(nc.published[nc.published.length - 1].data) as RPCMessage;
}

describe('no-responder retry on call()', () => {
  it('does not retry a NOT_FOUND error reply from the responder', async () => {
    const { client, nc } = createClient({ ...RETRY, timeout: 200 });
    const promise = client.call('rpc.test.find');
    const req = lastRequest(nc);

    muxSub(client, nc).callback(null, {
      subject: `rpc.reply.${req.id}`,
      data: encode({ id: req.id, error: { code: 'NOT_FOUND', message: 'camera not found' } }),
    });

    await expect(promise).rejects.toMatchObject({ code: 'NOT_FOUND', message: 'camera not found' });
    expect(nc.published.length).toBe(1);
  });

  it('retries a 503 status it observed itself and keeps the error shape', async () => {
    const { client, nc } = createClient({ noResponderRetry: { maxRetries: 2, delays: [1] } });
    const sub = (() => {
      (client as any).ensureMuxSubscription();
      return muxSub(client, nc);
    })();
    const promise = client.call('rpc.ghost.method');

    for (let attempt = 1; attempt <= 3; attempt++) {
      await vi.waitFor(() => expect(nc.published.length).toBe(attempt));
      sub.callback(null, { subject: `rpc.reply.${lastRequest(nc).id}`, data: new Uint8Array(0), headers: emptyHeaders(503) });
    }

    const error = await promise.catch((e: unknown) => e);
    expect(error).toBeInstanceOf(errors.NoRespondersError);
    expect(isNoRespondersError(error)).toBe(true);
    expect(nc.published.length).toBe(3);
  });
});

describe('request()', () => {
  it('keeps code and text of an error reply that mentions a timeout', async () => {
    const { client, nc } = createClient(RETRY);
    nc.onRequest = async () => ({ data: encode({ error: 'ONVIF connect timeout', code: 'INTERNAL_ERROR' }) });

    await expect(client.request('svc.onvif', {})).rejects.toMatchObject({ code: 'INTERNAL_ERROR', message: 'ONVIF connect timeout' });
  });

  it('does not retry an error reply that mentions no responders or carries NOT_FOUND', async () => {
    const { client, nc } = createClient(RETRY);
    nc.onRequest = async () => ({ data: encode({ error: 'downstream: no responders', code: 'NOT_FOUND' }) });

    const error = await client.request('svc.middle', {}).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(RPCException);
    expect(error).toMatchObject({ code: 'NOT_FOUND', message: 'downstream: no responders' });
    expect(nc.requests.length).toBe(1);
  });

  it('retries a local no-responders error and reports it as NOT_FOUND', async () => {
    const { client, nc } = createClient(RETRY);
    nc.onRequest = async (subject) => {
      throw new errors.RequestError('no responders', { cause: new errors.NoRespondersError(subject) });
    };

    await expect(client.request('svc.gone', {})).rejects.toMatchObject({ code: 'NOT_FOUND', message: 'No responders available' });
    expect(nc.requests.length).toBe(4);
  });

  it('maps a transport timeout to TIMEOUT without retrying', async () => {
    const { client, nc } = createClient(RETRY);
    nc.onRequest = async () => {
      throw new errors.TimeoutError();
    };

    await expect(client.request('svc.slow', {}, { timeout: 50 })).rejects.toMatchObject({ code: 'TIMEOUT', message: 'Request to "svc.slow" timed out after 50ms' });
    expect(nc.requests.length).toBe(1);
  });
});

describe('callStream retry', () => {
  function streamSub(nc: ReturnType<typeof createClient>['nc'], streamSubject: string) {
    const subs = nc.subs.filter((s) => s.pattern === streamSubject && !s.isClosed());
    expect(subs.length).toBe(1);
    return subs[0];
  }

  async function collect(iter: AsyncGenerator<unknown>, limitMs: number): Promise<{ items: unknown[]; error?: unknown }> {
    const items: unknown[] = [];
    const run = (async () => {
      try {
        for await (const item of iter) items.push(item);
        return { items };
      } catch (error) {
        return { items, error };
      }
    })();
    const hung = new Promise<{ items: unknown[]; error: unknown }>((resolve) => setTimeout(() => resolve({ items, error: new Error('stream hung') }), limitMs));
    return Promise.race([run, hung]);
  }

  it('does not repeat delivered items when the stream fails after them', async () => {
    const { client, nc } = createClient(RETRY);
    const result = collect(client.callStream('rpc.test.generate'), 500);

    await vi.waitFor(() => expect(nc.published.length).toBe(1));
    const req = lastRequest(nc);
    const sub = streamSub(nc, req.params.__streamSubject);
    sub.callback(null, { data: encode({ id: req.id, type: 'data', data: 0 }) });
    sub.callback(null, { data: encode({ id: req.id, type: 'data', data: 1 }) });
    sub.callback(null, { data: encode({ id: req.id, type: 'error', error: { code: 'NOT_FOUND', message: 'downstream gone' } }) });

    const { items, error } = await result;
    expect(items).toEqual([0, 1]);
    expect(error).toMatchObject({ code: 'NOT_FOUND', message: 'downstream gone' });
    expect(nc.published.filter((p) => p.subject === 'rpc.test.generate').length).toBe(1);
  });

  it('retries a 503 status that arrives before the first item', async () => {
    const { client, nc } = createClient(RETRY);
    (client as any).ensureMuxSubscription();
    const result = collect(client.callStream('rpc.test.generate'), 500);

    await vi.waitFor(() => expect(nc.published.length).toBe(1));
    const first = lastRequest(nc);
    muxSub(client, nc).callback(null, { subject: `rpc.reply.${first.id}`, data: new Uint8Array(0), headers: emptyHeaders(503) });

    await vi.waitFor(() => expect(nc.published.filter((p) => p.subject === 'rpc.test.generate').length).toBe(2));
    const second = lastRequest(nc);
    const sub = streamSub(nc, second.params.__streamSubject);
    sub.callback(null, { data: encode({ id: second.id, type: 'data', data: 'a' }) });
    sub.callback(null, { data: encode({ id: second.id, type: 'end' }) });

    const { items, error } = await result;
    expect(error).toBeUndefined();
    expect(items).toEqual(['a']);
  });

  it('does not retry a 503 status that arrives after an item', async () => {
    const { client, nc } = createClient(RETRY);
    (client as any).ensureMuxSubscription();
    const result = collect(client.callStream('rpc.test.generate'), 500);

    await vi.waitFor(() => expect(nc.published.length).toBe(1));
    const req = lastRequest(nc);
    streamSub(nc, req.params.__streamSubject).callback(null, { data: encode({ id: req.id, type: 'data', data: 'a' }) });
    muxSub(client, nc).callback(null, { subject: `rpc.reply.${req.id}`, data: new Uint8Array(0), headers: emptyHeaders(503) });

    const { items, error } = await result;
    expect(items).toEqual(['a']);
    expect(error).toBeInstanceOf(errors.NoRespondersError);
    expect(nc.published.filter((p) => p.subject === 'rpc.test.generate').length).toBe(1);
  });
});

describe('createProxy retry and timeout options', () => {
  // the method requests, without the __methods probe a proxy sends after a no-responder status
  function calls(nc: ReturnType<typeof createClient>['nc']) {
    return nc.published.filter((p) => !p.subject.endsWith('.__methods'));
  }

  function lastCall(nc: ReturnType<typeof createClient>['nc']): RPCMessage {
    return decode(calls(nc).at(-1)!.data) as RPCMessage;
  }

  function answer503(client: ReturnType<typeof createClient>['client'], nc: ReturnType<typeof createClient>['nc']) {
    const req = lastCall(nc);
    muxSub(client, nc).callback(null, { subject: `rpc.reply.${req.id}`, data: new Uint8Array(0), headers: emptyHeaders(503) });
  }

  it('turns the no-responder retry off with maxRetries 0, also for nested proxies', async () => {
    const { client, nc } = createClient({ noResponderRetry: { maxRetries: 3, delays: [20] } });
    const proxy = client.createProxy<any>('optional', { noResponderRetry: { maxRetries: 0 } });

    const flat = proxy.snapshot();
    await vi.waitFor(() => expect(calls(nc).length).toBe(1));
    answer503(client, nc);
    await expect(flat).rejects.toBeInstanceOf(errors.NoRespondersError);

    const nested = proxy.camera.snapshot();
    await vi.waitFor(() => expect(calls(nc).length).toBe(2));
    answer503(client, nc);
    await expect(nested).rejects.toBeInstanceOf(errors.NoRespondersError);

    await new Promise((resolve) => setTimeout(resolve, 60));
    expect(calls(nc).length).toBe(2);
  });

  it('uses its own retry delays and count', async () => {
    const { client, nc } = createClient({ noResponderRetry: { maxRetries: 0, delays: [5000] } });
    const proxy = client.createProxy<any>('flaky', { noResponderRetry: { maxRetries: 1, delays: [1] } });

    const promise = proxy.ping();
    await vi.waitFor(() => expect(calls(nc).length).toBe(1));
    answer503(client, nc);
    await vi.waitFor(() => expect(calls(nc).length).toBe(2));
    const req = lastCall(nc);
    muxSub(client, nc).callback(null, { subject: `rpc.reply.${req.id}`, data: encode({ id: req.id, result: 'pong' }) });

    await expect(promise).resolves.toBe('pong');
  });

  it('applies its timeout to each call instead of the client timeout', async () => {
    const { client } = createClient({ timeout: 10_000 });
    const proxy = client.createProxy<any>('slow', { timeout: 30 });

    const started = Date.now();
    await expect(proxy.work()).rejects.toMatchObject({ code: 'TIMEOUT', message: 'RPC call to "rpc.slow.work" timed out after 30ms' });
    expect(Date.now() - started).toBeLessThan(1000);
  });

  it('keeps the client defaults without options', async () => {
    const { client, nc } = createClient({ noResponderRetry: { maxRetries: 1, delays: [1] } });
    const promise = client.createProxy<any>('plain').call();
    await vi.waitFor(() => expect(calls(nc).length).toBe(1));
    answer503(client, nc);
    await vi.waitFor(() => expect(calls(nc).length).toBe(2));
    answer503(client, nc);
    await expect(promise).rejects.toBeInstanceOf(errors.NoRespondersError);
  });
});

describe('no-responder retry plan', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  // answers every request to `subject` with a no-responder status and records when each went out
  function noResponders({ client, nc }: ReturnType<typeof createClient>, subject: string): number[] {
    const times: number[] = [];
    const publish = nc.publish.bind(nc);
    nc.publish = (to, data, opts) => {
      publish(to, data, opts);
      if (to !== subject) return;
      times.push(Date.now());
      const reply = opts?.reply ?? `rpc.reply.${(decode(data) as RPCMessage).id}`;
      queueMicrotask(() => muxSub(client, nc).callback(null, { subject: reply, data: new Uint8Array(0), headers: emptyHeaders(503) }));
    };
    return times;
  }

  function gaps(times: number[]): number[] {
    return times.slice(1).map((time, i) => time - times[i]);
  }

  it('retries after 25, 50, 100, 325, 1000 and 2000 ms when neither delays nor maxRetries are set', async () => {
    vi.useFakeTimers();
    const ctx = createClient({ noResponderRetry: undefined });
    const times = noResponders(ctx, 'rpc.ghost.call');

    const error = ctx.client.call('rpc.ghost.call').catch((e: unknown) => e);
    await vi.runAllTimersAsync();

    expect(await error).toBeInstanceOf(errors.NoRespondersError);
    expect(gaps(times)).toEqual([25, 50, 100, 325, 1000, 2000]);
  });

  it('keeps 3 retries for delays without maxRetries and the old delays for maxRetries without delays', async () => {
    vi.useFakeTimers();
    const delaysOnly = createClient({ noResponderRetry: { delays: [10] } });
    const delayTimes = noResponders(delaysOnly, 'rpc.ghost.call');
    const retriesOnly = createClient({ noResponderRetry: { maxRetries: 2 } });
    const retryTimes = noResponders(retriesOnly, 'rpc.ghost.call');

    const first = delaysOnly.client.call('rpc.ghost.call').catch(() => {});
    const second = retriesOnly.client.call('rpc.ghost.call').catch(() => {});
    await vi.runAllTimersAsync();
    await Promise.all([first, second]);

    expect(gaps(delayTimes)).toEqual([10, 10, 10]);
    expect(gaps(retryTimes)).toEqual([500, 1000]);
  });

  it('takes a field the call does not set from the client option', async () => {
    vi.useFakeTimers();
    const ctx = createClient({ noResponderRetry: { delays: [40] } });
    const times = noResponders(ctx, 'rpc.ghost.call');

    const proxy = ctx.client.createProxy<{ call(): Promise<void> }>('ghost', { noResponderRetry: { maxRetries: 1 } });
    const call = proxy.call().catch(() => {});
    await vi.runAllTimersAsync();
    await call;

    expect(gaps(times)).toEqual([40]);
  });

  it('uses the same plan for a stream', async () => {
    vi.useFakeTimers();
    const ctx = createClient({ noResponderRetry: undefined });
    const times = noResponders(ctx, 'rpc.ghost.generateItems');

    const next = ctx.client
      .callStream('rpc.ghost.generateItems')
      .next()
      .catch((e: unknown) => e);
    await vi.runAllTimersAsync();

    expect(await next).toBeInstanceOf(errors.NoRespondersError);
    expect(gaps(times)).toEqual([25, 50, 100, 325, 1000, 2000]);
  });

  it('still takes a method probe answer that comes after a short first delay', async () => {
    vi.useFakeTimers();
    const ctx = createClient({ noResponderRetry: { maxRetries: 6, delays: [25, 50, 100, 325, 1000, 2000] } });
    const times = noResponders(ctx, 'rpc.ns.missing');
    const proxy = ctx.client.createProxy<{ missing(): Promise<void> }>('ns');

    const error = proxy.missing().catch((e: unknown) => e);
    await vi.advanceTimersByTimeAsync(100);
    const probe = ctx.nc.published.filter((p) => p.subject === 'rpc.ns.__methods').map((p) => decode(p.data) as RPCMessage);
    expect(probe.length).toBe(1);
    muxSub(ctx.client, ctx.nc).callback(null, { subject: `rpc.reply.${probe[0].id}`, data: encode({ id: probe[0].id, result: ['other'] }) });
    await vi.advanceTimersByTimeAsync(0);

    expect(await error).toBeInstanceOf(errors.NoRespondersError);
    expect(times.length).toBe(3);
  });
});

describe('callStream return()', () => {
  function streamClient(options: Parameters<typeof createClient>[0] = {}) {
    const ctx = createClient(options);
    const requests = () => ctx.nc.published.filter((p) => p.subject === 'rpc.ns.generateItems').map((p) => decode(p.data) as RPCMessage);
    const cancels = () => ctx.nc.published.filter((p) => p.subject.endsWith('.cancel'));
    const push = (id: string, message: object) => {
      const subject = `stream.rpc.ns.generateItems.${id}`;
      ctx.nc.subs.find((s) => s.pattern === subject && !s.isClosed())!.callback(null, { subject, data: encode({ id, ...message }) });
    };
    return { ...ctx, requests, cancels, push };
  }

  it('ends a stream parked between items at once and cancels it', async () => {
    const { client, requests, cancels, push } = streamClient();
    const stream = client.callStream('rpc.ns.generateItems');

    const first = stream.next();
    await vi.waitFor(() => expect(requests().length).toBe(1));
    push(requests()[0].id, { type: 'data', data: 1 });
    expect(await first).toEqual({ value: 1, done: false });

    const parked = stream.next();
    const started = performance.now();
    await stream.return(undefined);
    expect(performance.now() - started).toBeLessThan(50);
    expect(await parked).toEqual({ value: undefined, done: true });
    expect(cancels().length).toBe(1);
    expect((client as any).statusHandlers.size).toBe(0);
  });

  it('ends a retry wait at once without another attempt', async () => {
    const { client, nc, requests } = streamClient({ noResponderRetry: { maxRetries: 3, delays: [1000] } });
    const stream = client.callStream('rpc.ns.generateItems');

    const next = stream.next();
    await vi.waitFor(() => expect(requests().length).toBe(1));
    muxSub(client, nc).callback(null, { subject: `rpc.reply.${requests()[0].id}`, data: new Uint8Array(0), headers: emptyHeaders(503) });
    await new Promise((resolve) => setTimeout(resolve, 10));

    const started = performance.now();
    await stream.return(undefined);
    expect(performance.now() - started).toBeLessThan(50);
    expect(await next).toEqual({ value: undefined, done: true });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(requests().length).toBe(1);
  });

  it('leaves nothing behind when it comes while the request waits for the link, and sends no request', async () => {
    const { client, nc, requests } = streamClient();
    (client as any).linkWaiters = [];
    const stream = client.callStream('rpc.ns.generateItems');

    const next = stream.next();
    await vi.waitFor(() => expect(nc.subs.some((s) => s.pattern.startsWith('stream.rpc.ns.generateItems.'))).toBe(true));
    await stream.return(undefined);
    (client as any).releaseLinkWaiters();
    expect(await next).toEqual({ value: undefined, done: true });

    expect(requests()).toEqual([]);
    expect(nc.subs.filter((s) => s.pattern.startsWith('stream.') && !s.isClosed())).toEqual([]);
    expect((client as any).statusHandlers.size).toBe(0);
    expect((client as any).streamHandlers.size).toBe(0);
  });

  it('sends the cancel after the request when it comes while the request goes out', async () => {
    const { client, nc } = streamClient();
    const stream = client.callStream('rpc.ns.generateItems');
    const publish = nc.publish.bind(nc);
    nc.publish = (subject, data, opts) => {
      publish(subject, data, opts);
      if (subject === 'rpc.ns.generateItems') void stream.return(undefined);
    };

    expect(await stream.next()).toEqual({ value: undefined, done: true });
    await vi.waitFor(() => expect(nc.published.some((p) => p.subject.endsWith('.cancel'))).toBe(true));
    const subjects = nc.published.map((p) => p.subject);
    expect(subjects.indexOf('rpc.ns.generateItems')).toBeLessThan(subjects.findIndex((s) => s.endsWith('.cancel')));
    expect(nc.subs.filter((s) => s.pattern.startsWith('stream.') && !s.isClosed())).toEqual([]);
  });
});
