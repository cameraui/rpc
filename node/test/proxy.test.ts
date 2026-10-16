import { describe, expect, it } from 'vitest';

import { decode, encode } from '../src/codec.js';
import { createClient, muxSub } from './mock-nc.js';

import type { RPCMessage } from '../src/types.js';

const METHODS = ['ping', 'db.find'];

// answers every rpc.ns.* call with the method name, and with the method list when asked for it
function served() {
  const ctx = createClient();
  const seen: { method: string; discover: boolean }[] = [];
  const publish = ctx.nc.publish.bind(ctx.nc);
  ctx.nc.publish = (subject, data, opts) => {
    publish(subject, data, opts);
    if (!subject.startsWith('rpc.ns.')) return;
    const req = decode(data) as RPCMessage;
    const method = subject.slice('rpc.ns.'.length);
    seen.push({ method, discover: req.__discover === true });
    queueMicrotask(() => {
      const body = METHODS.includes(method) ? { result: method } : { error: { code: 'METHOD_NOT_FOUND', message: method } };
      muxSub(ctx.client, ctx.nc).callback(null, {
        subject: `rpc.reply.${req.id}`,
        data: encode({ id: req.id, ...body, ...(req.__discover ? { __methods: METHODS } : {}) }),
      });
    });
  };
  return { ...ctx, seen };
}

describe('createProxy method cache', () => {
  it('keeps nested handlers reachable once the cache is filled', async () => {
    const { client } = served();
    const proxy = client.createProxy<any>('ns');

    expect([await proxy.db.find(), await proxy.ping(), await proxy.db.find(), await proxy.db.find()]).toEqual(['db.find', 'ping', 'db.find', 'db.find']);
    expect(proxy.missing).toBeUndefined();
  });

  it('shares what a nested proxy learned with the root and the other way round', async () => {
    const { client, seen } = served();
    const proxy = client.createProxy<any>('ns');

    await proxy.db.find();
    await proxy.db.find();
    expect(proxy.missing).toBeUndefined();
    expect(seen.map((s) => s.discover)).toEqual([false, true]);

    await proxy.ping();
    await proxy.db.find();
    expect(seen.map((s) => s.discover)).toEqual([false, true, false, false]);
  });

  it('asks for the method list from the second call on, a one-call proxy never', async () => {
    const { client, seen } = served();
    for (let i = 0; i < 3; i++) await client.createProxy<any>('ns').ping();
    expect(seen.map((s) => s.discover)).toEqual([false, false, false]);

    const proxy = client.createProxy<any>('ns');
    await proxy.ping();
    await proxy.ping();
    await proxy.ping();
    expect(seen.slice(3).map((s) => s.discover)).toEqual([false, true, false]);
  });

  it('sends a missing method under a known prefix to the server', async () => {
    const { client, seen } = served();
    const proxy = client.createProxy<any>('ns');
    await proxy.ping();
    await proxy.ping();

    await expect(proxy.db.nope()).rejects.toMatchObject({ code: 'METHOD_NOT_FOUND' });
    expect(seen.at(-1)!.method).toBe('db.nope');
  });
});
