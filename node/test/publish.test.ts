import { createServer } from 'node:net';
import { headers } from '@nats-io/nats-core';
import { NatsConnectionImpl, ProtocolHandler } from '@nats-io/nats-core/internal';
import { connect } from '@nats-io/transport-node';
import { describe, expect, it, vi } from 'vitest';

import { RPCClient } from '../src/client.js';
import { assembleFrame, decode, decodeMessage, encode, encodeMessageParts } from '../src/codec.js';
import { handlePullCallbackRequest, handlePullIteratorRequest } from '../src/handler.js';
import { flushOutbound, PUBLISH_FRAME_MIN_SIZE, publishFrame, publishFrameZeroCopy } from '../src/patches.js';
import { createClient } from './mock-nc.js';

import type { AddressInfo, Socket } from 'node:net';
import type { FrameParts } from '../src/codec.js';

vi.mock('../src/codec.js', async (importOriginal) => {
  const codec = await importOriginal<typeof import('../src/codec.js')>();
  return { ...codec, encodeMessageParts: vi.fn(codec.encodeMessageParts) };
});

const MAX_PAYLOAD = 4 * 1024 * 1024;

// a real nats-core connection whose transport only records what would go to the socket
function fakeConnection() {
  const sent: Uint8Array[] = [];
  const nc = new (NatsConnectionImpl as unknown as new (opts: object) => any)({ servers: '127.0.0.1:1' });
  nc.protocol = new ProtocolHandler(nc.options, nc);
  // counts the copies nats-core makes when it packs several queued buffers into one
  let packCopies = 0;
  const outbound = nc.protocol.outbound;
  const pack = outbound.pack.bind(outbound);
  outbound.pack = () => {
    if (outbound.buffers.length > 1) packCopies++;
    pack();
  };
  Object.assign(nc.protocol, {
    connected: true,
    infoReceived: true,
    info: { max_payload: MAX_PAYLOAD, headers: true },
    transport: { send: (frame: Uint8Array) => sent.push(frame) },
  });
  const bytes = () => Buffer.concat(sent.map((b) => Buffer.from(b)));
  return { nc, sent, bytes, packCopies: () => packCopies };
}

function frameParts(size: number, fill = 7): FrameParts {
  const parts = encodeMessageParts({ id: 'x.1', method: 'call', params: [{ data: new Uint8Array(size).fill(fill) }] });
  if (parts instanceof Uint8Array) throw new Error('expected a CUIB frame');
  return parts;
}

async function settle(): Promise<void> {
  for (let i = 0; i < 5; i++) await Promise.resolve();
}

function sameMemory(a: Uint8Array, b: Uint8Array): boolean {
  return a.buffer === b.buffer && a.byteOffset === b.byteOffset && a.byteLength === b.byteLength;
}

// the payload of the first PUB to `subject` in a byte stream
function pubPayload(stream: Buffer, subject: string): Buffer {
  const text = stream.toString('latin1');
  const match = new RegExp(`PUB ${subject} (?:\\S+ )?(\\d+)\r\n`).exec(text);
  if (!match) throw new Error(`no PUB ${subject}`);
  const start = match.index + match[0].length;
  return stream.subarray(start, start + Number(match[1]));
}

// a NATS server that sends INFO, answers PING until stopPongs() and records everything it receives
async function fakeNatsServer() {
  const received: Buffer[] = [];
  const sockets: Socket[] = [];
  let pongs = true;
  const server = createServer((socket) => {
    sockets.push(socket);
    let partialLine = '';
    socket.write(`INFO {"server_id":"fake","version":"2.11.0","proto":1,"headers":true,"max_payload":${MAX_PAYLOAD}}\r\n`);
    socket.on('data', (chunk: Buffer) => {
      received.push(chunk);
      const lines = (partialLine + chunk.toString('latin1')).split('\r\n');
      partialLine = lines.pop()!;
      for (const line of lines) if (line === 'PING' && pongs) socket.write('PONG\r\n');
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  return {
    port: (server.address() as AddressInfo).port,
    bytes: () => Buffer.concat(received),
    stopPongs: () => {
      pongs = false;
    },
    close: () => {
      for (const socket of sockets) socket.destroy();
      return new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

describe('publishFrame', () => {
  it('works with the installed nats-core', () => {
    // fails when a nats-core update changes the internals publishFrame relies on
    const { nc, sent } = fakeConnection();
    expect(publishFrame(nc, 'a.b', undefined, frameParts(20_000))).toBe(true);
    expect(sent.length).toBe(1);
  });

  it('sends the same bytes as the stock publish', async () => {
    const cases = [
      { subject: 'rpc.ns.detect', reply: undefined, size: 16_384 },
      { subject: 'rpc.ns.detect', reply: 'rpc.reply.abc.1', size: 307_200 },
      { subject: 'rpc.kamera.größe', reply: 'rpc.reply.ü.2', size: 58_000 },
      { subject: 'rpc.ns.big', reply: 'rpc.reply.abc.3', size: MAX_PAYLOAD - 8192 - 200 },
    ];
    for (const { subject, reply, size } of cases) {
      const parts = frameParts(size, size & 0xff);
      const stock = fakeConnection();
      stock.nc.publish(subject, assembleFrame(parts), reply ? { reply } : undefined);
      await settle();
      const fast = fakeConnection();
      expect(publishFrame(fast.nc, subject, reply, parts)).toBe(true);
      await settle();

      expect(fast.bytes().equals(stock.bytes())).toBe(true);
      expect(fast.nc.protocol.outMsgs).toBe(stock.nc.protocol.outMsgs);
      expect(fast.nc.protocol.outBytes).toBe(stock.nc.protocol.outBytes);
    }
  });

  it('hands the command to the transport as one buffer, behind earlier queued commands', async () => {
    const { nc, sent, bytes } = fakeConnection();
    nc.publish('small.one', new Uint8Array([1, 2, 3]));
    const parts = frameParts(100_000);
    publishFrame(nc, 'big', undefined, parts);
    await settle();

    expect(sent.length).toBe(2);
    expect(Buffer.from(sent[0]).toString('latin1')).toBe('PUB small.one 3\r\n\x01\x02\x03\r\n');
    const head = `PUB big ${parts.size}\r\n`;
    expect(sent[1].length).toBe(head.length + parts.size + 2);
    expect(decodeMessage<any>(bytes().subarray(sent[0].length + head.length, bytes().length - 2)).params[0].data.length).toBe(100_000);
  });

  it('keeps the command queued in order while disconnected', async () => {
    const { nc, sent } = fakeConnection();
    nc.protocol.connected = false;
    nc.publish('first', new Uint8Array([9]));
    publishFrame(nc, 'second', undefined, frameParts(20_000));
    await settle();
    expect(sent.length).toBe(0);

    nc.protocol.connected = true;
    nc.protocol.flushPending();
    const text = Buffer.from(sent[0]).toString('latin1');
    expect(text.startsWith('PUB first 1\r\n\x09\r\nPUB second ')).toBe(true);
  });

  it('refuses a frame above max_payload and keeps the stock checks', () => {
    const { nc } = fakeConnection();
    nc.protocol.info.max_payload = 10_000;
    expect(publishFrame(nc, 'big', undefined, frameParts(20_000))).toBe(false);
    nc.protocol.info.max_payload = MAX_PAYLOAD;
    expect(() => publishFrame(nc, 'bad subject', undefined, frameParts(20_000))).toThrow();
  });
});

describe('publishFrameZeroCopy', () => {
  it('writes the caller segments in one writev on a real nats-core connection', async () => {
    // fails when a nats-core update changes the internals publishFrameZeroCopy relies on
    const server = await fakeNatsServer();
    const nc = await connect({ servers: `127.0.0.1:${server.port}`, reconnect: false });
    try {
      const socket = (nc as any).protocol.transport.socket;
      const writev = vi.spyOn(socket, '_writev');
      const parts = frameParts(100_000);
      nc.publish('stock', assembleFrame(parts), { reply: 'r.1' });
      expect(publishFrameZeroCopy(nc, 'fast', 'r.1', parts)).toBe(true);

      expect(writev).toHaveBeenCalledTimes(1);
      // node passes the buffers themselves when every chunk is one, otherwise { chunk, encoding } entries
      const chunks = (writev.mock.calls[0][0] as (Uint8Array | { chunk: Uint8Array })[]).map((entry) => (entry instanceof Uint8Array ? entry : entry.chunk));
      expect(chunks.some((chunk) => sameMemory(chunk, parts.segments[0]))).toBe(true);

      await nc.flush();
      expect(pubPayload(server.bytes(), 'fast').equals(pubPayload(server.bytes(), 'stock'))).toBe(true);
    } finally {
      await nc.close();
      await server.close();
    }
  });

  it('sends the same bytes as the stock publish', async () => {
    const cases = [
      { subject: 'rpc.ns.detect', reply: undefined, data: [new Uint8Array(40_000).fill(3)] },
      { subject: 'rpc.ns.detect', reply: 'rpc.reply.abc.1', data: [new Uint8Array(307_200).fill(9)] },
      { subject: 'rpc.kamera.größe', reply: 'rpc.reply.ü.2', data: [new Uint8Array(20_000).fill(1), new Uint8Array(70_000).fill(2)] },
    ];
    for (const { subject, reply, data } of cases) {
      const parts = encodeMessageParts({ id: 'x.1', method: 'call', params: data.map((d) => ({ data: d, w: d.length })) }) as FrameParts;
      expect(parts.segments.length).toBe(data.length);
      const stock = fakeConnection();
      stock.nc.publish(subject, assembleFrame(parts), reply ? { reply } : undefined);
      await settle();
      const fast = fakeConnection();
      expect(publishFrameZeroCopy(fast.nc, subject, reply, parts)).toBe(true);
      await settle();

      expect(fast.bytes().equals(stock.bytes())).toBe(true);
      expect(fast.nc.protocol.outMsgs).toBe(stock.nc.protocol.outMsgs);
      expect(fast.nc.protocol.outBytes).toBe(stock.nc.protocol.outBytes);
    }
  });

  it('hands each segment to the transport as it is, behind earlier queued commands', () => {
    const { nc, sent, packCopies } = fakeConnection();
    nc.publish('small.one', new Uint8Array([1, 2, 3]));
    const parts = frameParts(100_000);
    publishFrameZeroCopy(nc, 'big', undefined, parts);

    expect(sent.length).toBe(4);
    expect(Buffer.from(sent[0]).toString('latin1')).toBe('PUB small.one 3\r\n\x01\x02\x03\r\n');
    expect(Buffer.from(sent[1].subarray(0, 8)).toString('latin1')).toBe('PUB big ');
    expect(sameMemory(sent[2], parts.segments[0])).toBe(true);
    expect(Buffer.from(sent[3]).toString('latin1')).toBe('\r\n');
    expect(nc.protocol.outbound.size()).toBe(0);
    expect(packCopies()).toBe(1);
  });

  it('corks the socket around the frame', () => {
    const { nc, sent } = fakeConnection();
    const events: string[] = [];
    nc.protocol.transport = {
      socket: { cork: () => events.push('cork'), uncork: () => events.push('uncork') },
      send: (frame: Uint8Array) => {
        events.push('send');
        sent.push(frame);
      },
    };
    publishFrameZeroCopy(nc, 'big', undefined, frameParts(50_000));
    expect(events).toEqual(['cork', 'send', 'send', 'send', 'uncork']);
  });

  it('refuses while not connected and above max_payload, and keeps the stock checks', () => {
    const { nc, sent } = fakeConnection();
    nc.protocol.connected = false;
    expect(publishFrameZeroCopy(nc, 'big', undefined, frameParts(50_000))).toBe(false);
    nc.protocol.connected = true;
    nc.protocol.infoReceived = false;
    expect(publishFrameZeroCopy(nc, 'big', undefined, frameParts(50_000))).toBe(false);
    nc.protocol.infoReceived = true;
    nc.protocol.info.max_payload = 10_000;
    expect(publishFrameZeroCopy(nc, 'big', undefined, frameParts(20_000))).toBe(false);
    nc.protocol.info.max_payload = MAX_PAYLOAD;
    expect(() => publishFrameZeroCopy(nc, 'bad subject', undefined, frameParts(50_000))).toThrow();
    expect(() => publishFrameZeroCopy(nc, 'big', 'bad reply', frameParts(50_000))).toThrow();
    expect(sent.length).toBe(0);
    expect(nc.protocol.outMsgs).toBe(0);
  });
});

describe('flushOutbound', () => {
  it('writes the queued commands at once with the installed nats-core', () => {
    // fails when a nats-core update changes the internals flushOutbound relies on
    const { nc, sent } = fakeConnection();
    nc.publish('a.b', new Uint8Array([1]));
    expect(sent.length).toBe(0);
    flushOutbound(nc);
    expect(sent.length).toBe(1);
  });
});

describe('RPCClient publish of large binaries', () => {
  function client() {
    const conn = fakeConnection();
    const rpc = new RPCClient({ servers: ['nats://127.0.0.1:1'], name: 'publish-test' });
    (rpc as any).nc = conn.nc;
    (rpc as any)._maxPayloadSize = MAX_PAYLOAD - 8192;
    return { rpc, ...conn };
  }

  it('sends a frame with out-of-band binaries without a second copy in nats-core', async () => {
    const { rpc, sent, packCopies } = client();
    await rpc.publish('rpc.ns.detect', { data: new Uint8Array(50_000) }, { reply: 'rpc.reply.x.1' });
    expect(rpc.tryPublishSync('rpc.ns.detect', { data: new Uint8Array(50_000) })).toBe(true);
    await settle();
    expect(sent.length).toBe(2);
    expect(Buffer.from(sent[0].subarray(0, 4)).toString()).toBe('PUB ');
    expect(packCopies()).toBe(0);
  });

  it('leaves frames below 32 KiB to the stock path, which packs them with queued messages', async () => {
    const { rpc, sent, packCopies } = client();
    rpc.tryPublishSync('rpc.cb.x', { data: new Uint8Array(20_000) });
    rpc.tryPublishSync('rpc.cb.x', { data: new Uint8Array(20_000) });
    await settle();
    expect(sent.length).toBe(1);
    expect(packCopies()).toBe(1);

    rpc.tryPublishSync('rpc.cb.x', { data: new Uint8Array(PUBLISH_FRAME_MIN_SIZE) });
    await settle();
    expect(sent.length).toBe(2);
    expect(packCopies()).toBe(1);
  });

  it('chunks a frame above the client max payload instead of writing it in one piece', async () => {
    const { rpc, sent } = client();
    (rpc as any)._maxPayloadSize = 10_000;
    await rpc.publish('rpc.ns.detect', { data: new Uint8Array(50_000) });
    await settle();
    const text = Buffer.concat(sent.map((b) => Buffer.from(b))).toString('latin1');
    expect(text.startsWith('HPUB rpc.ns.detect ')).toBe(true);
    expect(text).toMatch(/x-chunked-transfer: header/i);
    expect(text).not.toMatch(/(^|\n)PUB /);
  });

  it('keeps the stock path for small messages and for messages with headers', async () => {
    const { rpc, sent } = client();
    await rpc.publish('small', { a: 1 });
    const hdrs = headers();
    hdrs.set('k', 'v');
    await rpc.publish('withHeaders', { data: new Uint8Array(50_000) }, { headers: hdrs });
    await settle();
    const text = Buffer.concat(sent.map((b) => Buffer.from(b))).toString('latin1');
    expect(text.startsWith('PUB small ')).toBe(true);
    expect(text).toContain('HPUB withHeaders ');
  });
});

describe('RPCClient zeroCopyBinaries', () => {
  function client(zeroCopyBinaries?: boolean) {
    const conn = fakeConnection();
    const rpc = new RPCClient({ servers: ['nats://127.0.0.1:1'], name: 'zero-copy-test', timeout: 20, noResponderRetry: { maxRetries: 0 }, zeroCopyBinaries });
    (rpc as any).nc = conn.nc;
    (rpc as any)._maxPayloadSize = MAX_PAYLOAD - 8192;
    return { rpc, ...conn };
  }

  function holds(sent: Uint8Array[], data: Uint8Array): boolean {
    return sent.some((buffer) => sameMemory(buffer, data));
  }

  it('writes a binary call argument without a copy, before the call can settle', async () => {
    const { rpc, sent, nc } = client(true);
    const data = new Uint8Array(100_000).fill(5);
    const call = rpc.call('rpc.ns.detect', { data });
    await settle();

    expect(holds(sent, data)).toBe(true);
    expect(nc.protocol.outbound.size()).toBe(0);
    expect(Buffer.from(decodeMessage<any>(pubPayload(Buffer.concat(sent), 'rpc\\.ns\\.detect')).params[0].data).equals(Buffer.from(data))).toBe(true);
    await expect(call).rejects.toThrow(/timed out/);
  });

  it('covers proxy calls and service calls', async () => {
    const { rpc, sent } = client(true);
    const first = new Uint8Array(60_000);
    const second = new Uint8Array(60_000);
    const proxy = rpc.createProxy<{ detect(frame: unknown): Promise<unknown> }>('ns');
    const calls = [proxy.detect({ data: first }), rpc.call('svc.detect', { data: second })];
    await settle();

    expect(holds(sent, first)).toBe(true);
    expect(holds(sent, second)).toBe(true);
    await Promise.allSettled(calls);
  });

  it('copies without the option, for publish() and for frames below 32 KiB', async () => {
    const off = client();
    const offData = new Uint8Array(100_000);
    const offCall = off.rpc.call('rpc.ns.detect', { data: offData });
    await settle();
    expect(holds(off.sent, offData)).toBe(false);
    await expect(offCall).rejects.toThrow(/timed out/);

    const on = client(true);
    const published = new Uint8Array(100_000);
    await on.rpc.publish('rpc.ns.event', { data: published });
    expect(on.rpc.tryPublishSync('rpc.ns.event', { data: published })).toBe(true);
    const small = new Uint8Array(20_000);
    const smallCall = on.rpc.call('rpc.ns.detect', { data: small });
    await settle();
    expect(holds(on.sent, published)).toBe(false);
    expect(holds(on.sent, small)).toBe(false);
    await expect(smallCall).rejects.toThrow(/timed out/);
  });

  it('fails the call at once when the request cannot be written', async () => {
    const { rpc } = client(true);
    await expect(rpc.call('rpc.bad subject', { data: new Uint8Array(100_000) })).rejects.toThrow(/subject/i);
    expect((rpc as any).pendingRequests.size).toBe(0);
  });

  it('copies a request that has to wait for the connection', async () => {
    const { rpc, sent, nc } = client(true);
    const data = new Uint8Array(100_000).fill(5);
    nc.protocol.connected = false;
    const call = rpc.call('rpc.ns.detect', { data });
    await settle();
    data.fill(0);

    nc.protocol.connected = true;
    nc.protocol.flushPending();
    expect(holds(sent, data)).toBe(false);
    const request = decodeMessage<any>(pubPayload(Buffer.concat(sent), 'rpc\\.ns\\.detect'));
    expect(Buffer.from(request.params[0].data).equals(Buffer.alloc(100_000, 5))).toBe(true);
    await expect(call).rejects.toThrow(/timed out/);
  });

  async function connectedClient(disconnectTimeout?: number) {
    const server = await fakeNatsServer();
    const rpc = new RPCClient({
      servers: [`127.0.0.1:${server.port}`],
      name: 'zero-copy-close',
      reconnect: false,
      disconnectTimeout,
      noResponderRetry: { maxRetries: 0 },
      zeroCopyBinaries: true,
    });
    await rpc.connect();
    return { server, rpc };
  }

  it.each(['disconnect', 'suspend'] as const)('settles a pending call on %s only once the socket sent its request', async (end) => {
    const { server, rpc } = await connectedClient();
    try {
      const data = new Uint8Array(1_000_000).fill(0xaa);
      // the socket takes the request only later, as on a busy connection
      let writtenFromCaller = false;
      const socket = (rpc as any).nc.protocol.transport.socket;
      const writev = socket._writev.bind(socket);
      socket._writev = (chunks: (Uint8Array | { chunk: Uint8Array })[], cb: (error?: Error | null) => void) => {
        writtenFromCaller ||= chunks.some((entry) => sameMemory(entry instanceof Uint8Array ? entry : entry.chunk, data));
        setTimeout(() => writev(chunks, cb), 50);
      };

      const call = rpc.call('rpc.ns.detect', { data }).catch((error: unknown) => {
        data.fill(0xbb);
        return error;
      });
      await rpc[end]();

      expect(writtenFromCaller).toBe(true);
      expect(await call).toMatchObject({ code: 'CONNECTION_CLOSED' });
      const request = decodeMessage<any>(pubPayload(server.bytes(), 'rpc\\.ns\\.detect'));
      expect(Buffer.from(request.params[0].data).equals(Buffer.alloc(1_000_000, 0xaa))).toBe(true);
    } finally {
      await rpc.disconnect();
      await server.close();
    }
  });

  it.each(['disconnect', 'suspend'] as const)('finishes %s while a session teardown waits for a pending call', async (end) => {
    const { server, rpc } = await connectedClient();
    try {
      const call = rpc.call('rpc.ns.detect', { data: new Uint8Array(100_000) }).catch((error: unknown) => error);
      rpc.trackSession(async () => {
        await call;
      });
      await rpc[end]();
      expect(await call).toMatchObject({ code: 'CONNECTION_CLOSED' });
    } finally {
      await rpc.disconnect();
      await server.close();
    }
  });

  it.each(['disconnect', 'suspend'] as const)('rejects a pending call on %s when the server stopped answering', async (end) => {
    const { server, rpc } = await connectedClient(100);
    try {
      server.stopPongs();
      const call = rpc.call('rpc.ns.detect', { data: new Uint8Array(100_000) }).catch((error: unknown) => error);
      await rpc[end]();
      expect(await call).toMatchObject({ code: 'CONNECTION_CLOSED' });
    } finally {
      await rpc.disconnect();
      await server.close();
    }
  });
});

describe('oversized handler replies', () => {
  const OVERSIZED = 50_000;

  function chunkedClient() {
    const ctx = createClient();
    (ctx.client as any)._maxPayloadSize = 10_000;
    vi.mocked(encodeMessageParts).mockClear();
    return ctx;
  }

  // resolves once the header and every chunk it announces went out on subject
  async function chunkedTransfer(published: ReturnType<typeof createClient>['nc']['published'], subject: string) {
    await vi.waitFor(() => {
      const messages = published.filter((p) => p.subject === subject && p.opts?.headers);
      expect(messages.length).toBeGreaterThan(0);
      expect(messages.length).toBe(decode(messages[0].data).totalChunks + 1);
    });
    return decode(published.find((p) => p.subject === subject)!.data);
  }

  it('encodes a sync handler result once before chunking it', async () => {
    const { client, nc } = chunkedClient();
    await client.registerHandler('ns', { big: () => ({ data: new Uint8Array(OVERSIZED) }) });
    vi.mocked(encodeMessageParts).mockClear();

    const sub = nc.subs.find((s) => s.pattern === 'rpc.ns.big')!;
    sub.callback(null, { subject: 'rpc.ns.big', data: encode({ id: 'a.1', method: 'call', params: [] }) });
    const header = await chunkedTransfer(nc.published, 'rpc.reply.a.1');

    expect(encodeMessageParts).toHaveBeenCalledTimes(1);
    expect(header.totalSize).toBeGreaterThan(OVERSIZED);
  });

  it('encodes a pull-iterator value once before chunking it', async () => {
    const { client, nc } = chunkedClient();
    async function* values() {
      yield new Uint8Array(OVERSIZED);
    }
    await handlePullIteratorRequest(values, [], 'it1', client);

    const sub = nc.subs.find((s) => s.pattern === '_rpc.iterator.it1.request')!;
    sub.callback(null, { subject: sub.pattern, data: encode({ id: 'it1', type: 'next' }) });
    await chunkedTransfer(nc.published, '_rpc.iterator.it1.response');

    expect(encodeMessageParts).toHaveBeenCalledTimes(1);
  });

  it('encodes a oneway callback payload once before chunking it', async () => {
    const { client, nc } = chunkedClient();
    let send: ((frame: Uint8Array) => void) | undefined;
    function session(cb: { onFrame: (frame: Uint8Array) => void }) {
      send = cb.onFrame;
      return (async function* () {
        yield;
      })();
    }
    await handlePullCallbackRequest(session, [], 'it2', '_rpc.cb.it2', ['onFrame'], ['onFrame'], client);

    send!(new Uint8Array(OVERSIZED));
    await chunkedTransfer(nc.published, '_rpc.cb.it2');

    expect(encodeMessageParts).toHaveBeenCalledTimes(1);
  });
});

describe('inline replies of one socket read', () => {
  async function server() {
    const conn = fakeConnection();
    const rpc = new RPCClient({ servers: ['nats://127.0.0.1:1'], name: 'reply-test' });
    (rpc as any).nc = conn.nc;
    const callbacks = new Map<string, (err: Error | null, msg: unknown) => void>();
    const subscribe = conn.nc.subscribe.bind(conn.nc);
    conn.nc.subscribe = (subject: string, opts: { callback: (err: Error | null, msg: unknown) => void }) => {
      callbacks.set(subject, opts.callback);
      return subscribe(subject, opts);
    };
    await rpc.registerHandler('ns', { echo: (v: unknown) => v });
    await settle();
    // one socket read: nats-core dispatches its messages back to back
    function read(count: number) {
      for (let i = 0; i < count; i++) {
        callbacks.get('rpc.ns.echo')!(null, { subject: 'rpc.ns.echo', data: encode({ id: `a.${i}`, method: 'call', params: [i] }) });
      }
    }
    return { ...conn, read };
  }

  it('writes every 8th reply at once and the rest with the queued flush', async () => {
    const { sent, bytes, read } = await server();
    const before = sent.length;

    read(20);
    expect(sent.length - before).toBe(2);
    await settle();

    expect(sent.length - before).toBe(3);
    const replies = bytes()
      .toString('latin1')
      .match(/PUB rpc\.reply\.a\.\d+ /g);
    expect(replies?.length).toBe(20);
  });

  it('counts the replies of each socket read on their own', async () => {
    const { sent, read } = await server();
    const before = sent.length;

    read(5);
    await settle();
    read(5);
    await settle();

    expect(sent.length - before).toBe(2);
  });
});

describe('chunked publish with headers', () => {
  it('leaves the caller headers as they were, so the next small message is no chunk header', async () => {
    const { client, nc } = createClient();
    (client as any)._maxPayloadSize = 1000;
    const h = headers();
    h.set('x-trace', 'abc');

    await client.publish('test.big', { blob: 'x'.repeat(5000) }, { headers: h });
    await client.publish('test.small', { ok: true }, { headers: h });

    expect(h.keys()).toEqual(['x-trace']);
    const header = nc.published.find((p) => p.subject === 'test.big')!;
    expect(header.opts?.headers?.get('x-chunked-transfer')).toBe('header');
    expect(header.opts?.headers?.get('x-trace')).toBe('abc');
    const small = nc.published.find((p) => p.subject === 'test.small')!;
    expect(small.opts?.headers?.get('x-chunked-transfer')).toBe('');
    expect(small.opts?.headers?.get('x-trace')).toBe('abc');
  });
});
