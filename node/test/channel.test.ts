import { connect } from '@nats-io/transport-node';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { RPCClient } from '../src/client.js';
import { encode } from '../src/codec.js';
import { createClient, createMockNc } from './mock-nc.js';

import type { NatsConnection } from '@nats-io/transport-node';

vi.mock('@nats-io/transport-node', async (importOriginal) => ({ ...(await importOriginal<typeof import('@nats-io/transport-node')>()), connect: vi.fn() }));

function deliverRequest(nc: ReturnType<typeof createClient>['nc'], index = 0) {
  const { subject, data, opts } = nc.requests[index];
  return nc.subs
    .filter((s) => s.pattern === subject && !s.isClosed())
    .map((s) => {
      const respond = vi.fn();
      s.callback(null, { subject, reply: `_INBOX.peer.${index}`, data, headers: opts?.headers, respond });
      return respond;
    });
}

async function flushAsync(): Promise<void> {
  for (let i = 0; i < 20; i++) await Promise.resolve();
}

describe('channel requests', () => {
  it('leaves its own requests to the other end, also when both ends share a connection', async () => {
    const { client, nc } = createClient();
    const asking = await client.channel('c1');
    const answering = await client.channel('c1');
    const askingHandler = vi.fn(() => 'asking');
    const answeringHandler = vi.fn(() => 'answering');
    await asking.onRequest(askingHandler);
    await answering.onRequest(answeringHandler);

    asking.request({ q: 1 }).catch(() => {});
    await vi.waitFor(() => expect(nc.requests.length).toBe(1));
    deliverRequest(nc);
    await flushAsync();

    expect(askingHandler).not.toHaveBeenCalled();
    expect(answeringHandler).toHaveBeenCalledTimes(1);
  });

  it('answers a request without a sender, as peers in other languages send them', async () => {
    const { client, nc } = createClient();
    const channel = await client.channel('c1');
    const handler = vi.fn(() => 'answer');
    await channel.onRequest(handler);

    const respond = vi.fn();
    nc.subs.find((s) => s.pattern === 'channel.c1.request')!.callback(null, { subject: 'channel.c1.request', reply: '_INBOX.go.1', data: encode({ q: 1 }), respond });
    await flushAsync();
    expect(handler).toHaveBeenCalledTimes(1);
    expect(respond).toHaveBeenCalledTimes(1);
  });

  it('keeps answering own requests on a plain onRequest', async () => {
    const { client, nc } = createClient();
    await client.onRequest('svc.echo', (data) => data);

    client.request('svc.echo', { q: 1 }).catch(() => {});
    await vi.waitFor(() => expect(nc.requests.length).toBe(1));
    const [respond] = deliverRequest(nc);
    await flushAsync();
    expect(respond).toHaveBeenCalledTimes(1);
  });

  it('stops the request handler of a private channel when it closes', async () => {
    const { client, nc } = createClient();
    const channel = await client.privateChannel('p1', 'peer');
    await channel.onRequest(() => 'answer');
    const subject = 'channel.private.p1.mux-test.peer.request';
    expect(nc.subs.some((s) => s.pattern === subject && !s.isClosed())).toBe(true);

    await channel.close();
    expect(nc.subs.some((s) => s.pattern === subject && !s.isClosed())).toBe(false);
  });
});

describe('channel lifecycle', () => {
  afterEach(() => {
    vi.mocked(connect).mockReset();
  });

  it('restores an onRequest handler after suspend() and connect()', async () => {
    const first = createMockNc();
    const second = createMockNc();
    vi.mocked(connect)
      .mockResolvedValueOnce(first as unknown as NatsConnection)
      .mockResolvedValueOnce(second as unknown as NatsConnection);
    const client = new RPCClient({ servers: ['nats://127.0.0.1:4222'], name: 'restore' });
    await client.connect();
    await client.onRequest('svc.echo', (data) => data);

    await client.suspend();
    await client.connect();
    expect(second.subs.some((s) => s.pattern === 'svc.echo' && !s.isClosed())).toBe(true);
  });

  it('lets go of the isolated client of a channel when it closes, and when its setup fails', async () => {
    vi.mocked(connect).mockResolvedValueOnce(createMockNc() as unknown as NatsConnection);
    const { client } = createClient();
    const isolated = () => (client as any).isolatedClients as RPCClient[];

    const channel = await client.channel('c2', { isolatedConnection: true });
    expect(isolated().length).toBe(1);
    await channel.close();
    expect(isolated().length).toBe(0);

    vi.mocked(connect).mockRejectedValueOnce(new Error('refused'));
    await expect(client.privateChannel('p2', 'peer', { isolatedConnection: true })).rejects.toThrow('refused');
    expect(isolated().length).toBe(0);
  });
});
