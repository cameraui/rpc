import { afterEach, describe, expect, it, vi } from 'vitest';

import { decode, encode } from '../src/codec.js';
import { handleCallbackRequest, handlePullCallbackRequest, handlePullIteratorRequest, handleStreamRequest, probeSettings, sessionSignal } from '../src/handler.js';
import { sleep } from '../src/utils.js';
import { createClient, emptyHeaders, muxSub } from './mock-nc.js';

import type { Published } from './mock-nc.js';

const PROBE = { intervalMs: 20, graceMs: 60 };
const REQUEST = '_rpc.iterator.p1.request';
const RESPONSE = '_rpc.iterator.p1.response';

// a producer client whose server answers a publish with reply on `gone` subjects with a no-responder status
function producer() {
  const { client, nc } = createClient();
  const gone = new Set<string>();
  const probes: Published[] = [];
  const publish = nc.publish.bind(nc);
  nc.publish = (subject, data, opts) => {
    publish(subject, data, opts);
    const reply = opts?.reply;
    if (!reply?.startsWith('rpc.reply.')) return;
    probes.push({ subject, data, opts });
    if (gone.has(subject)) {
      queueMicrotask(() => muxSub(client, nc).callback(null, { subject: reply, data: new Uint8Array(0), headers: emptyHeaders(503) }));
    }
  };
  const sent = (subject: string) => nc.published.filter((p) => p.subject === subject).map((p) => decode(p.data));
  const request = (type: 'next' | 'cancel', reply?: string) =>
    nc.subs.find((sub) => sub.pattern === REQUEST && !sub.closed)?.callback(null, { subject: REQUEST, reply, data: encode({ id: 'p1', type }) });
  return { client, nc, gone, probes, sent, request };
}

describe('orphan probe of callback sessions', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('ends a callback session whose consumer stays gone for the grace, through its cancel path', async () => {
    const { client, gone, probes, nc } = producer();
    let cleanups = 0;
    const subject = 'rpc.cb.c1';
    gone.add(subject);

    await handleCallbackRequest(() => () => void cleanups++, [], subject, 'c1', client, undefined, PROBE);
    expect(nc.subs.some((sub) => sub.pattern === `${subject}.cancel` && !sub.closed)).toBe(true);

    await vi.waitFor(() => expect(cleanups).toBe(1), { timeout: 1000 });
    expect(probes.length).toBeGreaterThanOrEqual(3);
    expect(probes.every((p) => p.subject === subject)).toBe(true);
    expect(decode(probes[0].data)).toEqual({ id: 'c1', type: 'ping' });
    expect(nc.subs.some((sub) => sub.pattern === `${subject}.cancel` && !sub.closed)).toBe(false);
    expect((client as any).statusHandlers.size).toBe(0);

    const count = probes.length;
    await sleep(60);
    expect(probes.length).toBe(count);
  });

  it('carries the probe on a data publish instead of a ping while the session publishes', async () => {
    const { client, probes } = producer();
    let push!: (value: unknown) => Promise<void>;
    const cleanup = await handleCallbackRequest(
      (callback: (value: unknown) => Promise<void>) => {
        push = callback;
        return () => {};
      },
      [],
      'rpc.cb.c2',
      'c2',
      client,
      undefined,
      PROBE,
    );

    for (let i = 0; i < 8; i++) {
      await push(i);
      await sleep(5);
    }
    await cleanup();
    expect(probes.length).toBeGreaterThanOrEqual(1);
    expect(probes.every((p) => decode(p.data).type === 'data')).toBe(true);
  });

  it('keeps a session whose consumer comes back before the grace ends', async () => {
    const { client, gone } = producer();
    let cleanups = 0;
    const subject = 'rpc.cb.c3';
    gone.add(subject);

    const cleanup = await handleCallbackRequest(() => () => void cleanups++, [], subject, 'c3', client, undefined, { intervalMs: 20, graceMs: 200 });
    await sleep(70);
    gone.delete(subject);
    await sleep(250);
    expect(cleanups).toBe(0);

    await cleanup();
    expect(cleanups).toBe(1);
  });
});

describe('orphan probe of pull sessions', () => {
  it('ends a parked pull-callback session through the cancel path, probing the reply inbox of the consumer', async () => {
    const { client, gone, probes, sent, request } = producer();
    const closed: string[] = [];
    async function* frames(callbacks: { onFrame: (n: number) => void }) {
      try {
        callbacks.onFrame(1);
        yield;
        yield;
      } finally {
        closed.push('closed');
      }
    }

    await handlePullCallbackRequest(frames, [], 'p1', '_rpc.cb.p1', ['onFrame'], ['onFrame'], client, undefined, PROBE);
    request('next', 'rpc.reply.p1');
    await vi.waitFor(() => expect(sent(RESPONSE)).toEqual([{ id: 'p1', type: 'value' }]));

    // the consumer goes away while the session is parked
    gone.add('rpc.reply.p1');
    await vi.waitFor(
      () =>
        expect(sent(RESPONSE)).toEqual([
          { id: 'p1', type: 'value' },
          { id: 'p1', type: 'done' },
        ]),
      { timeout: 1000 },
    );
    await vi.waitFor(() => expect(closed).toEqual(['closed']));
    const idle = probes.filter((p) => p.subject === 'rpc.reply.p1');
    expect(idle.length).toBeGreaterThanOrEqual(3);
    expect(decode(idle[0].data)).toEqual({ type: 'ping' });
    expect((client as any).statusHandlers.size).toBe(0);
  });

  it('ends a pull iterator whose value responses find no consumer', async () => {
    const { client, gone, sent, request } = producer();
    gone.add(RESPONSE);
    async function* items() {
      for (let i = 0; ; i++) yield i;
    }

    await handlePullIteratorRequest(items, [], 'p1', client, undefined, { intervalMs: 5, graceMs: 30 });
    const pump = setInterval(() => request('next'), 2);
    try {
      await vi.waitFor(() => expect(sent(RESPONSE).at(-1)).toEqual({ id: 'p1', type: 'done' }), { timeout: 1000 });
    } finally {
      clearInterval(pump);
    }
  });
});

describe('orphan probe of push streams', () => {
  it('ends a stream that waits for its next item once the consumer is gone for the grace', async () => {
    const { client, nc, gone, probes } = producer();
    const subject = 'stream.rpc.ns.generateItems.s1';
    gone.add(subject);
    let signal: AbortSignal | undefined;
    async function* items() {
      yield 'first';
      await new Promise(() => {});
    }

    // returns once the session ended, a producer parked in a step is closed when the step settles
    await handleStreamRequest(
      (...args: unknown[]) => {
        signal = sessionSignal();
        return items(...(args as []));
      },
      [],
      subject,
      's1',
      client,
      undefined,
      PROBE,
    );
    if (signal) expect(signal.aborted).toBe(true);
    expect(nc.subs.find((sub) => sub.pattern === `${subject}.cancel`)!.closed).toBe(true);
    expect(probes.map((p) => decode(p.data))).toContainEqual({ id: 's1', type: 'ping' });
    expect((client as any).statusHandlers.size).toBe(0);
  });
});

describe('orphan probe option', () => {
  it('takes 0 as not set', () => {
    expect(probeSettings({ intervalMs: 0, graceMs: 0 })).toEqual({ intervalMs: 30_000, graceMs: 300_000 });
    expect(probeSettings({ intervalMs: 5, graceMs: 7 })).toEqual({ intervalMs: 5, graceMs: 7 });
  });

  it('does not probe with orphanProbe false', async () => {
    const { client, nc, gone } = producer();
    gone.add('rpc.cb.c4');
    let cleanups = 0;

    const cleanup = await client.registerHandler('ns', { watch: () => () => void cleanups++ }, { orphanProbe: false });
    nc.subs
      .find((sub) => sub.pattern === 'rpc.ns.watch')!
      .callback(null, { subject: 'rpc.ns.watch', data: encode({ id: 'w.1', method: 'call', params: [{ __callback: true, __callbackSubject: 'rpc.cb.c4', args: [] }] }) });
    await vi.waitFor(() => expect(nc.published.some((p) => p.subject === 'rpc.reply.w.1')).toBe(true));
    await sleep(100);
    expect(nc.published.filter((p) => p.subject === 'rpc.cb.c4')).toEqual([]);
    expect(cleanups).toBe(0);
    await cleanup();
  });

  it('probes by default with the client option of the registration', async () => {
    const { client, nc, gone } = producer();
    client.options.orphanProbe = PROBE;
    gone.add('rpc.cb.c5');
    let cleanups = 0;

    await client.registerHandler('ns', { watch: () => () => void cleanups++ });
    nc.subs
      .find((sub) => sub.pattern === 'rpc.ns.watch')!
      .callback(null, { subject: 'rpc.ns.watch', data: encode({ id: 'w.2', method: 'call', params: [{ __callback: true, __callbackSubject: 'rpc.cb.c5', args: [] }] }) });
    await vi.waitFor(() => expect(cleanups).toBe(1), { timeout: 1000 });
  });
});
