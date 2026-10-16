import { connect, errors } from '@nats-io/transport-node';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { RPCClient } from '../src/client.js';
import { decode } from '../src/codec.js';
import { sleep } from '../src/utils.js';
import { createMockNc } from './mock-nc.js';

import type { NatsConnection } from '@nats-io/transport-node';

vi.mock('@nats-io/transport-node', async (importOriginal) => ({ ...(await importOriginal<typeof import('@nats-io/transport-node')>()), connect: vi.fn() }));

const DENIED = 'rpc.sensors.rpc.getSensors';

async function connected() {
  const nc = createMockNc();
  vi.mocked(connect).mockResolvedValueOnce(nc as unknown as NatsConnection);
  const client = new RPCClient({ servers: ['nats://127.0.0.1:4222'], name: 'viewer', timeout: 10_000 });
  await client.connect();
  const refuse = (subject: string, operation: 'publish' | 'subscription' = 'publish') => {
    const verb = operation === 'publish' ? 'Publish' : 'Subscription';
    nc.emitStatus({ type: 'error', error: new errors.PermissionViolationError(`Permissions Violation for ${verb} to "${subject}"`, operation, subject) } as never);
  };
  const sentTo = (subject: string) => nc.published.filter((p) => p.subject === subject);
  return { client, nc, refuse, sentTo };
}

// what the promise settles with within `ms`, 'pending' otherwise
function within(promise: Promise<unknown>, ms: number): Promise<unknown> {
  return Promise.race([
    promise.then(
      (value) => value,
      (error: unknown) => error,
    ),
    sleep(ms).then(() => 'pending'),
  ]);
}

describe('a publish the server refuses', () => {
  afterEach(() => {
    vi.mocked(connect).mockReset();
  });

  it('fails every call waiting on that subject with FORBIDDEN at once and sends none again', async () => {
    const { client, refuse, sentTo } = await connected();
    const first = client.call(DENIED);
    const second = client.call(DENIED);
    const other = client.call('rpc.sensors.rpc.getSensorStates');
    await vi.waitFor(() => expect(sentTo(DENIED)).toHaveLength(2));

    refuse(DENIED);

    for (const call of [first, second]) {
      const error = (await within(call, 100)) as { code?: string; data?: unknown };
      expect(error.code).toBe('FORBIDDEN');
      expect(error.data).toEqual({ subject: DENIED });
    }
    expect(await within(other, 50)).toBe('pending');
    await sleep(50);
    expect(sentTo(DENIED)).toHaveLength(2);
    expect((client as any).pendingRequests.size).toBe(1);
    await client.disconnect();
  });

  it('fails a stream start and a pull iterator init on that subject', async () => {
    const { client, nc, refuse, sentTo } = await connected();
    const stream = client.callStream(DENIED).next();
    const pull = client.callPullIterator(DENIED).next();
    await vi.waitFor(() => expect(sentTo(DENIED)).toHaveLength(2));

    refuse(DENIED);

    expect(await within(stream, 100)).toMatchObject({ code: 'FORBIDDEN' });
    expect(await within(pull, 100)).toMatchObject({ code: 'FORBIDDEN' });
    await sleep(20);
    expect(sentTo(DENIED)).toHaveLength(2);
    expect(nc.subs.filter((sub) => sub.pattern.startsWith('stream.') && !sub.closed)).toEqual([]);
    expect((client as any).streamHandlers.size).toBe(0);
  });

  it('fails a call through a proxy without a method probe or retry', async () => {
    const { client, refuse, sentTo } = await connected();
    const proxy = client.createProxy<{ getSensors(): Promise<unknown> }>('sensors.rpc');
    const call = proxy.getSensors();
    await vi.waitFor(() => expect(sentTo(DENIED)).toHaveLength(1));
    const [request] = sentTo(DENIED).map((p) => decode(p.data) as { method: string });
    expect(request.method).toBe('call');

    refuse(DENIED);

    expect(await within(call, 100)).toMatchObject({ code: 'FORBIDDEN' });
    await sleep(50);
    expect(sentTo(DENIED)).toHaveLength(1);
    expect(sentTo('rpc.sensors.rpc.__methods')).toEqual([]);
  });

  it('leaves calls alone when the refusal is for a subscription', async () => {
    const { client, refuse, sentTo } = await connected();
    const call = client.call(DENIED);
    await vi.waitFor(() => expect(sentTo(DENIED)).toHaveLength(1));

    refuse(DENIED, 'subscription');

    expect(await within(call, 50)).toBe('pending');
    await client.disconnect();
  });
});
