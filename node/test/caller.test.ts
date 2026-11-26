import { setTimeout as sleep } from 'node:timers/promises';
import { describe, expect, it, vi } from 'vitest';

import { decode, encode } from '../src/codec.js';
import { callerIsViewer, CALLER_ROLE_HEADER } from '../src/handler.js';
import { chunkedMessages, createClient, drainMicrotasks, headersWith } from './mock-nc.js';

import type { RPCClient } from '../src/client.js';

const VIEWER = headersWith({ [CALLER_ROLE_HEADER]: 'viewer' });

async function ask(headers?: ReturnType<typeof headersWith>, chunked = false, options?: Parameters<RPCClient['registerHandler']>[2]) {
  const ctx = createClient();
  await ctx.client.registerHandler(
    'who',
    {
      viewer: async () => {
        await Promise.resolve();
        return callerIsViewer();
      },
    },
    options,
  );
  const sub = ctx.nc.subs.find((s) => s.pattern === 'rpc.who.viewer' || s.pattern === 'rpc.who.>')!;
  const request = { id: 'a.1', method: 'call', params: [] };
  if (chunked) {
    const { header, chunks } = chunkedMessages('rpc.who.viewer', request, 't1', 8);
    sub.callback(null, { ...header, headers: headersWith({ 'x-chunked-transfer': 'header', 'x-chunk-id': 't1', [CALLER_ROLE_HEADER]: 'viewer' }) });
    for (const chunk of chunks) sub.callback(null, chunk);
  } else {
    sub.callback(null, { subject: 'rpc.who.viewer', data: encode(request), headers });
  }
  await drainMicrotasks();
  return (decode(ctx.nc.published.find((p) => p.subject === 'rpc.reply.a.1')!.data) as { result: boolean }).result;
}

async function until(condition: () => boolean, ms = 1000): Promise<void> {
  const deadline = performance.now() + ms;
  while (!condition()) {
    if (performance.now() > deadline) throw new Error('condition not met in time');
    await sleep(1);
  }
}

describe('the caller of a handler', () => {
  it('is no viewer without the role header', async () => {
    expect(await ask()).toBe(false);
  });

  it('is a viewer with the role header, even next to a forged one', async () => {
    expect(await ask(headersWith({ [CALLER_ROLE_HEADER]: ['admin', 'viewer'] }))).toBe(true);
  });

  it('is a viewer for a chunked request', async () => {
    expect(await ask(undefined, true)).toBe(true);
  });

  it.each([{ namespaceSubscription: true }, { dropExpired: true }])('is told apart with %o', async (options) => {
    expect(await ask(VIEWER, false, options)).toBe(true);
    expect(await ask(undefined, false, options)).toBe(false);
  });

  it('is a viewer on every step of a pull session', async () => {
    const { client, nc } = createClient();
    const seen: boolean[] = [];
    await client.registerHandler('who', {
      async *pull() {
        seen.push(callerIsViewer());
        yield 1;
        seen.push(callerIsViewer());
        yield 2;
      },
    });
    const deliver = (subject: string, message: unknown, headers?: ReturnType<typeof headersWith>) => {
      nc.subs.find((s) => s.pattern === subject && !s.closed)!.callback(null, { subject, data: encode(message), headers });
    };
    const responses = () => nc.published.filter((p) => p.subject === '_rpc.iterator.p1.response').length;

    deliver('rpc.who.pull', { id: 'p1', method: 'call', params: [{ __pullIterator: true, __iteratorId: 'p1', args: [] }] }, VIEWER);
    await until(() => nc.published.some((p) => p.subject === 'rpc.reply.p1'));
    for (const step of [1, 2]) {
      deliver('_rpc.iterator.p1.request', { id: 'p1', type: 'next' });
      await until(() => responses() === step);
    }
    expect(seen).toEqual([true, true]);
  });

  it('is unknown outside a handler', () => {
    expect(callerIsViewer()).toBe(false);
  });
});

describe('the caller without AsyncLocalStorage', () => {
  it('is a viewer until the first await of the handler', async () => {
    vi.resetModules();
    vi.spyOn(process, 'getBuiltinModule').mockReturnValue(undefined);
    const handler = await import('../src/handler.js');
    vi.restoreAllMocks();

    const seen: boolean[] = [];
    await handler.asViewer(async () => {
      seen.push(handler.callerIsViewer());
      handler.outsideSession(() => seen.push(handler.callerIsViewer()));
      await Promise.resolve();
      seen.push(handler.callerIsViewer());
    });

    expect(seen).toEqual([true, false, false]);
    expect(handler.callerIsViewer()).toBe(false);
  });
});
