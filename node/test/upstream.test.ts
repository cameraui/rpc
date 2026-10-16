import { errors } from '@nats-io/transport-node';
import { describe, expect, it, vi } from 'vitest';

import { decode, encode } from '../src/codec.js';
import { createError, isNoRespondersError, RPCException } from '../src/errors.js';
import { formatErrorObject } from '../src/handler.js';
import { createClient, emptyHeaders, muxSub } from './mock-nc.js';

import type { RPCMessage, RPCResponse, StreamMessage } from '../src/types.js';

type Ctx = ReturnType<typeof createClient>;

const UPSTREAM = (subject: string) => ({ code: 'INTERNAL_ERROR', message: 'upstream unavailable', data: { subject } });

function deliver({ nc }: Ctx, method: string, id: string, params: unknown = []) {
  nc.subs.find((s) => s.pattern === `rpc.ns.${method}`)!.callback(null, { subject: `rpc.ns.${method}`, data: encode({ id, method: 'call', params }) });
}

// answers the last request to `subject` with a no-responder status
async function noResponders({ client, nc }: Ctx, subject: string) {
  await vi.waitFor(() => expect(nc.published.some((p) => p.subject === subject)).toBe(true));
  const req = decode(nc.published.findLast((p) => p.subject === subject)!.data) as RPCMessage;
  muxSub(client, nc).callback(null, { subject: `rpc.reply.${req.id}`, data: new Uint8Array(0), headers: emptyHeaders(503) });
}

async function replyTo({ nc }: Ctx, id: string): Promise<RPCResponse> {
  await vi.waitFor(() => expect(nc.published.some((p) => p.subject === `rpc.reply.${id}`)).toBe(true));
  return decode(nc.published.find((p) => p.subject === `rpc.reply.${id}`)!.data) as RPCResponse;
}

describe('no-responder errors a handler passes on', () => {
  it('go out as INTERNAL_ERROR upstream unavailable, the handler sees the original error', async () => {
    const ctx = createClient();
    let seen: unknown;
    await ctx.client.registerHandler('ns', {
      relay: async () => {
        try {
          return await ctx.client.call('rpc.up.value');
        } catch (error) {
          seen = error;
          throw error;
        }
      },
    });
    deliver(ctx, 'relay', 'a.1');
    await noResponders(ctx, 'rpc.up.value');

    expect((await replyTo(ctx, 'a.1')).error).toEqual(UPSTREAM('rpc.up.value'));
    expect(seen).toBeInstanceOf(errors.NoRespondersError);
    expect(isNoRespondersError(seen)).toBe(true);
  });

  it('cover request() and keep its NOT_FOUND for the handler', async () => {
    const ctx = createClient();
    ctx.nc.onRequest = async (subject) => {
      throw new errors.RequestError('no responders', { cause: new errors.NoRespondersError(subject) });
    };
    let seen: unknown;
    await ctx.client.registerHandler('ns', {
      relay: async () => {
        try {
          return await ctx.client.request('svc.up', {});
        } catch (error) {
          seen = error;
          throw error;
        }
      },
    });
    deliver(ctx, 'relay', 'a.1');

    expect((await replyTo(ctx, 'a.1')).error).toEqual(UPSTREAM('svc.up'));
    expect(seen).toMatchObject({ code: 'NOT_FOUND', message: 'No responders available' });
  });

  it('leave an error reply from a responder as it is, also NOT_FOUND with no responders in its text', async () => {
    const ctx = createClient();
    await ctx.client.registerHandler('ns', {
      relay: async () => ctx.client.call('rpc.up.value'),
    });
    deliver(ctx, 'relay', 'a.1');
    await vi.waitFor(() => expect(ctx.nc.published.some((p) => p.subject === 'rpc.up.value')).toBe(true));
    const req = decode(ctx.nc.published.find((p) => p.subject === 'rpc.up.value')!.data) as RPCMessage;
    muxSub(ctx.client, ctx.nc).callback(null, {
      subject: `rpc.reply.${req.id}`,
      data: encode({ id: req.id, error: { code: 'NOT_FOUND', message: "no responders: 'rpc.far.away'" } }),
    });

    expect((await replyTo(ctx, 'a.1')).error).toEqual({ code: 'NOT_FOUND', message: "no responders: 'rpc.far.away'" });
  });

  it('end a stream with upstream unavailable', async () => {
    const ctx = createClient();
    await ctx.client.registerHandler('ns', {
      async *items() {
        yield await ctx.client.call('rpc.up.value');
      },
    });
    ctx.nc.subs
      .find((s) => s.pattern === 'rpc.ns.items')!
      .callback(null, {
        subject: 'rpc.ns.items',
        data: encode({ id: 's.1', method: 'stream', params: { __stream: true, __streamSubject: 'stream.rpc.ns.items.s1', args: [] } }),
      });
    await noResponders(ctx, 'rpc.up.value');

    await vi.waitFor(() => expect(ctx.nc.published.some((p) => p.subject === 'stream.rpc.ns.items.s1')).toBe(true));
    const frame = decode(ctx.nc.published.find((p) => p.subject === 'stream.rpc.ns.items.s1')!.data) as StreamMessage;
    expect(frame).toEqual({ id: 's.1', type: 'error', error: UPSTREAM('rpc.up.value') });
  });

  it('cover the no-responder status of a pull iterator next request', async () => {
    const ctx = createClient();
    const iter = ctx.client.callPullIterator('rpc.up.pullItems');
    const first = iter.next().catch((error: unknown) => error);
    await vi.waitFor(() => expect(ctx.nc.published.length).toBe(1));
    const init = decode(ctx.nc.published[0].data) as RPCMessage;
    const iteratorId = init.params[0].__iteratorId;
    muxSub(ctx.client, ctx.nc).callback(null, { subject: `rpc.reply.${init.id}`, data: encode({ id: init.id, result: { iteratorId } }) });
    await vi.waitFor(() => expect(ctx.nc.published.length).toBe(2));
    muxSub(ctx.client, ctx.nc).callback(null, { subject: `rpc.reply.${iteratorId}`, data: new Uint8Array(0), headers: emptyHeaders(503) });

    const error = await first;
    expect(error).toMatchObject({ code: '503' });
    expect(formatErrorObject(error)).toEqual(UPSTREAM('rpc.up.pullItems'));
  });

  it('cover the no-responder status of a stream the handler starts', async () => {
    const ctx = createClient();
    await ctx.client.registerHandler('ns', {
      relay: async () => {
        for await (const item of ctx.client.callStream('rpc.up.generateItems')) return item;
      },
    });
    deliver(ctx, 'relay', 'a.1');
    await noResponders(ctx, 'rpc.up.generateItems');

    expect((await replyTo(ctx, 'a.1')).error).toEqual(UPSTREAM('rpc.up.generateItems'));
  });

  it('cover the no-responder status of a call on the service path', async () => {
    const ctx = createClient();
    await ctx.client.registerHandler('ns', {
      relay: async () => ctx.client.call('svc.up.value'),
    });
    deliver(ctx, 'relay', 'a.1');
    await vi.waitFor(() => expect(ctx.nc.published.some((p) => p.subject === 'svc.up.value')).toBe(true));
    const inbox = ctx.nc.published.find((p) => p.subject === 'svc.up.value')!.opts!.reply!;
    ctx.nc.subs.find((s) => s.pattern === inbox)!.callback(null, { subject: inbox, data: new Uint8Array(0), headers: emptyHeaders(503) });

    expect((await replyTo(ctx, 'a.1')).error).toEqual(UPSTREAM('svc.up.value'));
  });

  it('cover the no-responder status of a pull-callback next request', async () => {
    const ctx = createClient();
    await ctx.client.registerHandler('ns', {
      relay: async () => {
        for await (const _ of ctx.client.callPullIteratorWithCallback('rpc.up.pullFrames', { onFrame: () => {} }, ['onFrame'])) return 'batch';
      },
    });
    deliver(ctx, 'relay', 'a.1');
    await vi.waitFor(() => expect(ctx.nc.published.some((p) => p.subject === 'rpc.up.pullFrames')).toBe(true));
    const init = decode(ctx.nc.published.find((p) => p.subject === 'rpc.up.pullFrames')!.data) as RPCMessage;
    const iteratorId = init.params[0].__iteratorId;
    muxSub(ctx.client, ctx.nc).callback(null, { subject: `rpc.reply.${init.id}`, data: encode({ id: init.id, result: { iteratorId } }) });
    await vi.waitFor(() => expect(ctx.nc.published.some((p) => p.subject === `_rpc.iterator.${iteratorId}.request`)).toBe(true));
    muxSub(ctx.client, ctx.nc).callback(null, { subject: `rpc.reply.${iteratorId}`, data: new Uint8Array(0), headers: emptyHeaders(503) });

    expect((await replyTo(ctx, 'a.1')).error).toEqual(UPSTREAM('rpc.up.pullFrames'));
  });

  it('keep the formatting of every other error', () => {
    expect(formatErrorObject(createError('NOT_FOUND', 'camera gone', { id: 1 }))).toEqual({ code: 'NOT_FOUND', message: 'camera gone', data: { id: 1 } });
    expect(formatErrorObject(RPCException.fromJSON({ code: 'NOT_FOUND', message: 'no responders' }))).toEqual({ code: 'NOT_FOUND', message: 'no responders' });
    expect(formatErrorObject(new Error('plain'))).toEqual({ code: 'INTERNAL_ERROR', message: 'plain' });
    expect(formatErrorObject('text')).toEqual({ code: 'INTERNAL_ERROR', message: 'Internal error' });
  });

  it('reach onRequest replies too', async () => {
    const ctx = createClient();
    await ctx.client.onRequest('channel.c.request', async () => ctx.client.call('rpc.up.value'));
    const respond = vi.fn();
    ctx.nc.subs.find((s) => s.pattern === 'channel.c.request')!.callback(null, { data: encode({}), reply: '_INBOX.x', respond });
    await noResponders(ctx, 'rpc.up.value');

    await vi.waitFor(() => expect(respond).toHaveBeenCalled());
    expect(decode(respond.mock.calls[0][0])).toEqual({ error: 'upstream unavailable', code: 'INTERNAL_ERROR' });
  });
});
