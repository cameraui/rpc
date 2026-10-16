import { describe, expect, it } from 'vitest';

import { decode, encode } from '../src/codec.js';
import { createServiceProxy, rpcCallbacks } from '../src/utils.js';
import { createClient, hostServices } from './mock-nc.js';

import type { ServiceInfo } from '@nats-io/services';
import type { NatsConnection } from '@nats-io/transport-node';

function serviceClient() {
  const ctx = createClient();
  const { nc } = ctx;
  hostServices(nc);
  ctx.client.service.init(nc as unknown as NatsConnection);
  const live = (pattern: string) => nc.subs.find((sub) => sub.pattern === pattern && !(sub as any).gone) as any;
  const sent = (subject: string) => nc.published.filter((p) => p.subject === subject).map((p) => decode(p.data));
  const request = (subject: string, payload: unknown) => live(subject)!.callback(null, { subject, data: encode(payload), respond() {}, respondError() {} });
  return { ...ctx, live, sent, request };
}

async function until(condition: () => boolean, ms = 1000): Promise<void> {
  const deadline = performance.now() + ms;
  while (!condition()) {
    if (performance.now() > deadline) throw new Error('condition not met in time');
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

describe('service endpoints', () => {
  it('serves each method on its bare subject, on svc.<service>.<path> and on the subject of its instance', async () => {
    const { client, live, sent, request } = serviceClient();
    const service = await client.service.registerHandler({ name: 'alpha', version: '1.0.0' }, { whoami: () => 'alpha', config: { get: (key: string) => `v:${key}` } });
    const info = service.info();

    expect(info.metadata).toMatchObject({ 'cui.chunk.direct': '1' });
    expect(info.endpoints.map((e) => e.subject).sort()).toEqual(['config.get', 'svc.alpha.config.get', 'svc.alpha.whoami', 'whoami']);
    expect(live(`$CUI.SVC.${info.id}.whoami`)).toBeDefined();
    expect(live(`$CUI.SVC.${info.id}.whoami`).queue).toBeUndefined();

    request('svc.alpha.config.get', { id: 'a.1', method: 'call', params: ['k'] });
    request(`$CUI.SVC.${info.id}.whoami`, { id: 'a.2', method: 'call', params: [] });
    await until(() => sent('svc.alpha.config.get.reply.a.1').length === 1 && sent(`$CUI.SVC.${info.id}.whoami.reply.a.2`).length === 1);
    expect(sent('svc.alpha.config.get.reply.a.1')).toEqual([{ id: 'a.1', result: 'v:k' }]);
    expect(sent(`$CUI.SVC.${info.id}.whoami.reply.a.2`)).toEqual([{ id: 'a.2', result: 'alpha' }]);

    await service.stop();
    expect(live(`$CUI.SVC.${info.id}.whoami`)).toBeUndefined();
  });

  it('serves callback and pull-callback sessions', async () => {
    const { client, sent, request } = serviceClient();
    let push!: (value: unknown) => Promise<void>;
    let cleanups = 0;
    const service = await client.service.registerHandler(
      { name: 'events', version: '1.0.0' },
      {
        watch(callback: (value: unknown) => Promise<void>) {
          push = callback;
          return () => void cleanups++;
        },
        async *play(callbacks: { onFrame: (n: number) => void }) {
          callbacks.onFrame(1);
          yield;
        },
      },
    );

    request('watch', { id: 'c.1', method: 'call', params: [{ __callback: true, __callbackSubject: 'rpc.cb.c1', args: [] }] });
    await until(() => sent('watch.reply.c.1').length === 1);
    expect(sent('watch.reply.c.1')).toEqual([{ id: 'c.1', result: { ok: true } }]);
    await push('event');
    expect(sent('rpc.cb.c1')).toEqual([{ id: 'c.1', type: 'data', data: 'event' }]);

    const params = { __pullCallback: true, __iteratorId: 'p1', __callbackSubject: '_rpc.cb.p1', __callbackMethods: ['onFrame'], __onewayMethods: ['onFrame'], args: [] };
    request('play', { id: 'c.2', method: 'call', params: [params] });
    await until(() => sent('play.reply.c.2').length === 1);
    expect(sent('play.reply.c.2')).toEqual([{ id: 'c.2', result: { iteratorId: 'p1' } }]);
    await until(() => sent('_rpc.cb.p1').length === 1);
    expect(sent('_rpc.cb.p1')).toEqual([{ method: 'onFrame', args: [1] }]);

    await service.stop();
    expect(cleanups).toBe(1);
  });
});

describe('service proxy', () => {
  function info(overrides: Partial<ServiceInfo>): ServiceInfo {
    return {
      type: 'io.nats.micro.v1.info_response',
      name: 'alpha',
      id: 'INST1',
      version: '1.0.0',
      description: '',
      metadata: {},
      endpoints: [
        { name: 'whoami', subject: 'whoami', queue_group: 'q', metadata: {} },
        { name: 'whoami', subject: 'svc.alpha.whoami', queue_group: 'q', metadata: {} },
      ],
      ...overrides,
    } as ServiceInfo;
  }

  it('calls the subject of its service before the bare one', async () => {
    const { client, nc } = createClient();
    const proxy = createServiceProxy<{ whoami(): Promise<string> }>(client, info({}));

    void proxy.whoami().catch(() => {});
    await until(() => nc.published.length === 1);
    expect(nc.published[0].subject).toBe('svc.alpha.whoami');
    expect(nc.subs.map((s) => s.pattern)).toContain(`svc.alpha.whoami.reply.${(decode(nc.published[0].data) as { id: string }).id}`);
  });

  it('sends a request too large for one message to the subject of the instance it picked', async () => {
    const { client, nc } = createClient();
    (client as any)._maxPayloadSize = 1000;
    const proxy = createServiceProxy<{ whoami(blob: Uint8Array): Promise<string> }>(client, info({ metadata: { 'cui.chunk.direct': '1' } }));

    void proxy.whoami(new Uint8Array(5000)).catch(() => {});
    await until(() => nc.published.length > 2);
    const target = '$CUI.SVC.INST1.whoami';
    expect(new Set(nc.published.map((p) => p.subject))).toEqual(new Set([target]));
    expect(nc.published[0].opts?.headers?.get('x-chunked-transfer')).toBe('header');
    expect(nc.subs.some((s) => s.pattern.startsWith(`${target}.reply.`))).toBe(true);

    nc.published.length = 0;
    void proxy.whoami(new Uint8Array(10)).catch(() => {});
    await until(() => nc.published.length === 1);
    expect(nc.published[0].subject).toBe('svc.alpha.whoami');
  });

  it('keeps the queue subject for a large request when the instance has no subject of its own', async () => {
    const { client, nc } = createClient();
    (client as any)._maxPayloadSize = 1000;
    const proxy = createServiceProxy<{ whoami(blob: Uint8Array): Promise<string> }>(client, info({}));

    void proxy.whoami(new Uint8Array(5000)).catch(() => {});
    await until(() => nc.published.length > 2);
    expect(new Set(nc.published.map((p) => p.subject))).toEqual(new Set(['svc.alpha.whoami']));
  });

  it('routes a callback bundle of a service method to a pull-callback session', async () => {
    const { client, nc } = createClient();
    const proxy = createServiceProxy<any>(client, info({ endpoints: [{ name: 'play', subject: 'svc.alpha.play', queue_group: 'q', metadata: {} } as any] }));

    const iter = proxy.play(rpcCallbacks({ onFrame: () => {} }));
    void iter.next().catch(() => {});
    await until(() => nc.published.some((p) => p.subject === 'svc.alpha.play'));
    const init = decode(nc.published.find((p) => p.subject === 'svc.alpha.play')!.data) as { params: [{ __pullCallback: boolean }] };
    expect(init.params[0].__pullCallback).toBe(true);
    await iter.return(undefined);
  });
});
