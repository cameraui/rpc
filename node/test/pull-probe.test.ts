import { describe, expect, it, vi } from 'vitest';

import { decode } from '../src/codec.js';
import { sleep } from '../src/utils.js';
import { createClient, emptyHeaders } from './mock-nc.js';

import type { Published } from './mock-nc.js';

const REQUEST = /^_rpc\.iterator\..+\.request$/;
const RESPONSE = /^_rpc\.iterator\..+\.response$/;

function subjectMatches(pattern: string, subject: string): boolean {
  const want = pattern.split('.');
  const have = subject.split('.');
  for (const [i, token] of want.entries()) {
    if (token === '>') return have.length > i;
    if (i >= have.length || (token !== '*' && token !== have[i])) return false;
  }
  return want.length === have.length;
}

// two clients on one wire, a message can disappear like one written into a dead connection: no delivery, no 503
function wire() {
  const ends: ReturnType<typeof createClient>[] = [];
  const sent: Published[] = [];
  let lose: ((subject: string) => boolean) | undefined;

  function deliver(subject: string, data: Uint8Array, opts?: Published['opts']): boolean {
    let delivered = false;
    for (const { nc } of ends) {
      for (const sub of nc.subs) {
        if (sub.closed || !subjectMatches(sub.pattern, subject)) continue;
        delivered = true;
        setImmediate(() => {
          if (!sub.closed) sub.callback(null, { subject, reply: opts?.reply, data, headers: opts?.headers ?? emptyHeaders() });
        });
      }
    }
    return delivered;
  }

  function join(): ReturnType<typeof createClient>['client'] {
    const end = createClient();
    end.nc.publish = (subject, data, opts) => {
      const copy = data.slice();
      sent.push({ subject, data: copy, opts });
      if (lose?.(subject)) {
        lose = undefined;
        return;
      }
      if (!deliver(subject, copy, opts) && opts?.reply) deliver(opts.reply, new Uint8Array(0), { headers: emptyHeaders(503) });
    };
    ends.push(end);
    return end.client;
  }

  const requests = (type: string) => sent.filter((p) => REQUEST.test(p.subject) && (decode(p.data) as { type: string }).type === type).length;

  return {
    join,
    requests,
    loseNext(match: (subject: string) => boolean) {
      lose = match;
    },
  };
}

function consumerOf(net: ReturnType<typeof wire>) {
  const consumer = net.join();
  (consumer as any).pullProbeMs = 20;
  return consumer;
}

function counting(slowFrom = Infinity, delayMs = 0) {
  const state = { ended: false };
  async function* items() {
    try {
      for (let i = 0; ; i++) {
        if (i >= slowFrom) await sleep(delayMs);
        yield i;
      }
    } finally {
      state.ended = true;
    }
  }
  return { items, state };
}

describe('pull step that lost a message', () => {
  it('ends with an error when its request got lost, and ends the session of the handler', async () => {
    const net = wire();
    const { items, state } = counting();
    await net.join().registerHandler('ns', { items });

    const iter = consumerOf(net).callPullIterator<number>('rpc.ns.items');
    expect((await iter.next()).value).toBe(0);

    net.loseNext((subject) => REQUEST.test(subject));
    await expect(iter.next()).rejects.toMatchObject({ code: 'STREAM_ERROR', message: expect.stringContaining('lost a message') });
    await vi.waitFor(() => expect(state.ended).toBe(true));
    expect(net.requests('cancel')).toBe(1);

    const probes = net.requests('probe');
    await sleep(60);
    expect(net.requests('probe')).toBe(probes);
  });

  it('ends with an error when its answer got lost', async () => {
    const net = wire();
    const { items, state } = counting();
    await net.join().registerHandler('ns', { items });

    const iter = consumerOf(net).callPullIterator<number>('rpc.ns.items');
    expect((await iter.next()).value).toBe(0);

    net.loseNext((subject) => RESPONSE.test(subject));
    await expect(iter.next()).rejects.toMatchObject({ code: 'STREAM_ERROR', message: expect.stringContaining('lost a message') });
    await vi.waitFor(() => expect(state.ended).toBe(true));
  });

  it('ends with no responders when the last word of the handler got lost', async () => {
    const net = wire();
    async function* one() {
      yield 0;
    }
    await net.join().registerHandler('ns', { one });

    const iter = consumerOf(net).callPullIterator<number>('rpc.ns.one');
    expect((await iter.next()).value).toBe(0);

    net.loseNext((subject) => RESPONSE.test(subject));
    await expect(iter.next()).rejects.toMatchObject({ code: '503' });
  });

  it('ends a pull-callback session whose request got lost, with and without a request sent ahead', async () => {
    for (const prefetch of [false, true]) {
      const net = wire();
      const state = { ended: false };
      async function* frames(callbacks: { onFrame: (n: number) => void }) {
        try {
          for (let i = 0; ; i++) {
            callbacks.onFrame(i);
            yield;
          }
        } finally {
          state.ended = true;
        }
      }
      await net.join().registerHandler('ns', { frames });

      const seen: number[] = [];
      const iter = consumerOf(net).callPullIteratorWithCallback('rpc.ns.frames', { onFrame: (n: number) => seen.push(n) }, { onewayMethods: ['onFrame'], prefetch });
      net.loseNext((subject) => REQUEST.test(subject) && seen.length > 0);
      await expect(
        (async () => {
          for (let i = 0; i < 4; i++) await iter.next();
        })(),
      ).rejects.toMatchObject({ code: 'STREAM_ERROR', message: expect.stringContaining('lost a message') });
      await vi.waitFor(() => expect(state.ended).toBe(true));
    }
  });
});

describe('pull step that only takes long', () => {
  it('outlives its probes while the source is slow', async () => {
    const net = wire();
    const { items, state } = counting(1, 150);
    await net.join().registerHandler('ns', { items });

    const iter = consumerOf(net).callPullIterator<number>('rpc.ns.items');
    for (const want of [0, 1, 2]) expect((await iter.next()).value).toBe(want);
    expect(net.requests('probe')).toBeGreaterThanOrEqual(6);

    await iter.return();
    await vi.waitFor(() => expect(state.ended).toBe(true));
  });

  it('sends no probe while the consumer takes its time between steps', async () => {
    const net = wire();
    const { items } = counting();
    await net.join().registerHandler('ns', { items });

    const iter = consumerOf(net).callPullIterator<number>('rpc.ns.items');
    for (const want of [0, 1, 2]) {
      expect((await iter.next()).value).toBe(want);
      await sleep(60);
    }
    expect(net.requests('probe')).toBe(0);
    await iter.return();
  });

  it('sends no probe while a pull-callback consumer drains a batch whose successor it asked for ahead', async () => {
    const net = wire();
    async function* frames(callbacks: { onFrame: (n: number) => void }) {
      for (let i = 0; ; i++) {
        callbacks.onFrame(i);
        yield;
      }
    }
    await net.join().registerHandler('ns', { frames });

    const iter = consumerOf(net).callPullIteratorWithCallback('rpc.ns.frames', { onFrame: () => sleep(80) }, { onewayMethods: ['onFrame'], prefetch: true });
    for (let i = 0; i < 3; i++) expect((await iter.next()).done).toBe(false);
    expect(net.requests('probe')).toBe(0);
    await iter.return();
  });

  it('ignores idle answers to earlier steps, as after probes that waited in a stalled connection', async () => {
    const net = wire();
    const { items } = counting();
    await net.join().registerHandler('ns', { items });
    const consumer = consumerOf(net);
    (consumer as any).pullProbeMs = 60_000;

    const iter = consumer.callPullIterator<number>('rpc.ns.items');
    expect((await iter.next()).value).toBe(0);
    const id: string = [...(consumer as any).pullSessions][0];
    for (const want of [1, 2, 3]) {
      const step = iter.next();
      // value 0 was step 1, so the step before the one waiting here has the number of its value
      for (const earlier of [want, want, 0]) (consumer as any).probePullSession(id, earlier);
      expect((await step).value).toBe(want);
    }
    await iter.return();
  });
});
