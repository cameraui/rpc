import { createServer } from 'node:net';
import { setTimeout as sleep } from 'node:timers/promises';
import { headers } from '@nats-io/nats-core';
import { NatsConnectionImpl, Parser, ProtocolHandler } from '@nats-io/nats-core/internal';
import { connect } from '@nats-io/transport-node';
import { NodeTransport } from '@nats-io/transport-node/lib/node_transport.js';
import { describe, expect, it, vi } from 'vitest';

import { packSmallPublishes, presizeSplitPayloads, readsShared, shareReadBuffer } from '../src/patches.js';
import { createClient } from './mock-nc.js';

import type { AddressInfo, Socket } from 'node:net';

interface Event {
  kind: number;
  msg?: { subject: Uint8Array; sid: number; size: number; hdr: number };
  data?: Uint8Array;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function msgFrame(subject: string, payload: Uint8Array, header?: string): Uint8Array {
  const hdr = header === undefined ? undefined : encoder.encode(`NATS/1.0\r\n${header}\r\n\r\n`);
  const size = (hdr?.length ?? 0) + payload.length;
  const line = hdr ? `HMSG ${subject} 1 ${hdr.length} ${size}\r\n` : `MSG ${subject} 1 ${size}\r\n`;
  const head = encoder.encode(line);
  const frame = new Uint8Array(head.length + size + 2);
  frame.set(head, 0);
  if (hdr) frame.set(hdr, head.length);
  frame.set(payload, head.length + (hdr?.length ?? 0));
  frame.set([13, 10], head.length + size);
  return frame;
}

function parseIn(parts: Uint8Array[]): Event[] {
  const events: Event[] = [];
  const parser = new Parser({ push: (event: Event) => events.push(event) } as unknown as ConstructorParameters<typeof Parser>[0]);
  for (const part of parts) parser.parse(part);
  return events.filter((e) => e.msg);
}

function payload(n: number): Uint8Array {
  return Uint8Array.from({ length: n }, (_, i) => (i * 31 + 7) & 0xff);
}

describe('presizeSplitPayloads', () => {
  it('is installed by connect() and active with the installed nats-core', async () => {
    const stockParse = Parser.prototype.parse;
    await createClient().client.connect();
    expect(Parser.prototype.parse).not.toBe(stockParse);
    // fails when a nats-core update changes the parser internals the patch relies on
    expect(presizeSplitPayloads()).toBe(true);
  });

  it('delivers the same message for every split offset, MSG and HMSG', () => {
    presizeSplitPayloads();
    for (const header of [undefined, 'k: v']) {
      for (const size of [0, 1, 7, 300]) {
        const body = payload(size);
        const frame = msgFrame('a.b', body, header);
        for (let k = 0; k <= frame.length; k++) {
          for (const m of [k, Math.min(frame.length, k + 3)]) {
            const events = parseIn([frame.subarray(0, k), frame.subarray(k, m), frame.subarray(m)]);
            expect(events.length).toBe(1);
            const { msg, data } = events[0];
            expect(decoder.decode(msg!.subject)).toBe('a.b');
            expect(msg!.size).toBe(data!.length);
            expect(Buffer.from(data!.subarray(msg!.hdr > 0 ? msg!.hdr : 0))).toEqual(Buffer.from(body));
          }
        }
      }
    }
  });

  it('keeps back-to-back messages apart when the second one is split', () => {
    presizeSplitPayloads();
    const first = msgFrame('one', payload(10));
    const second = msgFrame('two', payload(70_000));
    const stream = new Uint8Array(first.length + second.length);
    stream.set(first, 0);
    stream.set(second, first.length);
    const events = parseIn([stream.subarray(0, 65_536), stream.subarray(65_536)]);
    expect(events.map((e) => decoder.decode(e.msg!.subject))).toEqual(['one', 'two']);
    expect(Buffer.from(events[1].data!)).toEqual(Buffer.from(payload(70_000)));
  });

  it('allocates the payload buffer of a split message at its full size', () => {
    presizeSplitPayloads();
    const frame = msgFrame('big', payload(200_000));
    const parser = new Parser({ push: () => {} } as unknown as ConstructorParameters<typeof Parser>[0]);
    parser.parse(frame.subarray(0, 65_536));
    expect(parser.msgBuf?.capacity).toBe(200_000);
  });

  it('stays off when nats-core no longer exports the parser', async () => {
    vi.resetModules();
    vi.doMock('@nats-io/nats-core/internal', async (importOriginal) => ({ ...(await importOriginal<object>()), Parser: undefined }));
    try {
      const patches = await import('../src/patches.js');
      expect(patches.presizeSplitPayloads()).toBe(false);
    } finally {
      vi.doUnmock('@nats-io/nats-core/internal');
      vi.resetModules();
    }
  });
});

function protocolOn(node: boolean, writes: Buffer[]) {
  const nc = new (NatsConnectionImpl as unknown as new (opts: object) => any)({ servers: '127.0.0.1:1' });
  const protocol = new ProtocolHandler(nc.options, nc) as any;
  const record = (frame: Uint8Array) => writes.push(Buffer.from(frame));
  const transport = node ? Object.assign(Object.create(NodeTransport.prototype), { done: false, options: {}, socket: { write: record } }) : { send: record };
  Object.assign(protocol, { connected: true, infoReceived: true, info: { max_payload: 4 * 1024 * 1024, headers: true }, transport });
  return protocol;
}

async function settle(): Promise<void> {
  for (let i = 0; i < 5; i++) await Promise.resolve();
}

describe('packSmallPublishes', () => {
  it('is installed by connect() and active with the installed nats-core', async () => {
    await createClient().client.connect();
    // fails when a nats-core or transport-node update changes the internals the patch relies on
    expect(packSmallPublishes()).toBe(true);
  });

  it('writes the same bytes as the stock path for PUB, HPUB, replies, UTF-8 and every size', async () => {
    packSmallPublishes();
    const patched: Buffer[] = [];
    const stock: Buffer[] = [];
    const runs = [protocolOn(true, patched), protocolOn(false, stock)];
    for (const protocol of runs) {
      for (const subject of ['a.b', 'rpc.kamera.größe.摄像头']) {
        for (const size of [0, 1, 120, 5_000, 70_000, 900_000]) {
          const data = Uint8Array.from({ length: size }, (_, i) => (i * 7) & 0xff);
          protocol.publish(subject, data);
          protocol.publish(subject, data, { reply: 'rpc.reply.ü.1' });
          const h = headers();
          h.set('x-k', `v${size}`);
          protocol.publish(subject, data, { headers: h, reply: 'r.2' });
          await settle();
        }
      }
    }
    expect(Buffer.concat(patched).equals(Buffer.concat(stock))).toBe(true);
    expect(runs[0].outBytes).toBe(runs[1].outBytes);
    expect(runs[0].outMsgs).toBe(runs[1].outMsgs);
  });

  it('writes one buffer per flush and flushes inline at the pending limit', async () => {
    packSmallPublishes();
    const writes: Buffer[] = [];
    const protocol = protocolOn(true, writes);
    for (let i = 0; i < 10; i++) protocol.publish('a.b', new Uint8Array(10));
    expect(writes.length).toBe(0);
    await settle();
    expect(writes.length).toBe(1);

    protocol.pendingLimit = 100;
    for (let i = 0; i < 10; i++) protocol.publish('a.b', new Uint8Array(30));
    expect(writes.length).toBe(4);
    await settle();
    expect(
      Buffer.concat(writes.slice(1))
        .toString('latin1')
        .match(/PUB a\.b 30\r\n/g)?.length,
    ).toBe(10);
  });

  it('keeps what is published while disconnected until the link is up, a new dial drops it', async () => {
    packSmallPublishes();
    const writes: Buffer[] = [];
    const protocol = protocolOn(true, writes);
    protocol.connected = false;
    protocol.publish('held.one', Uint8Array.of(1));
    protocol.publish('held.two', Uint8Array.of(2));
    await settle();
    expect(writes.length).toBe(0);
    protocol.connected = true;
    protocol.flushPending();
    expect(writes.map((w) => w.toString('latin1'))).toEqual(['PUB held.one 1\r\n\x01\r\nPUB held.two 1\r\n\x02\r\n']);

    protocol.connected = false;
    protocol.publish('dropped', Uint8Array.of(3));
    protocol.resetOutbound();
    protocol.connected = true;
    protocol.flushPending();
    expect(writes.length).toBe(1);
  });

  it('swallows a socket write error like the stock send', () => {
    packSmallPublishes();
    const transport = Object.assign(Object.create(NodeTransport.prototype), {
      done: false,
      options: {},
      socket: {
        write: () => {
          throw new Error('EPIPE');
        },
      },
    });
    expect(() => transport.send(new Uint8Array(1))).not.toThrow();
  });

  it('stays off when nats-core no longer exports the protocol handler', async () => {
    vi.resetModules();
    vi.doMock('@nats-io/nats-core/internal', async (importOriginal) => ({ ...(await importOriginal<object>()), ProtocolHandler: undefined }));
    try {
      const patches = await import('../src/patches.js');
      expect(patches.packSmallPublishes()).toBe(false);
    } finally {
      vi.doUnmock('@nats-io/nats-core/internal');
      vi.resetModules();
    }
  });
});

async function messageServer(messages: Uint8Array[], info: Record<string, unknown> = {}, piece = 7_000) {
  const connections: Buffer[][] = [];
  const sockets: Socket[] = [];
  const server = createServer((socket) => {
    sockets.push(socket);
    const received: Buffer[] = [];
    connections.push(received);
    socket.write(`INFO ${JSON.stringify({ server_id: 'fake', version: '2.11.0', proto: 1, headers: true, max_payload: 4 * 1024 * 1024, ...info })}\r\n`);
    let pending = '';
    socket.on('data', (chunk: Buffer) => {
      received.push(chunk);
      const lines = (pending + chunk.toString('latin1')).split('\r\n');
      pending = lines.pop()!;
      for (const line of lines) {
        if (line === 'PING') socket.write('PONG\r\n');
        const sub = /^SUB (\S+) (\d+)$/.exec(line);
        if (sub) {
          const stream = Buffer.concat(messages.flatMap((m) => [Buffer.from(`MSG ${sub[1]} ${sub[2]} ${m.length}\r\n`), Buffer.from(m), Buffer.from('\r\n')]));
          for (let at = 0; at < stream.length; at += piece) socket.write(stream.subarray(at, at + piece));
        }
      }
    });
    socket.on('error', () => {});
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  return {
    port: (server.address() as AddressInfo).port,
    connections,
    close: () => {
      for (const socket of sockets) socket.destroy();
      return new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

async function quietServer() {
  const sockets: Socket[] = [];
  const server = createServer((socket) => {
    sockets.push(socket);
    socket.on('error', () => {});
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  return {
    port: (server.address() as AddressInfo).port,
    sockets,
    close: () => {
      for (const socket of sockets) socket.destroy();
      return new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

describe('shareReadBuffer', () => {
  it('is installed by connect() and active with the installed transport-node', async () => {
    await createClient().client.connect();
    // fails when a transport-node update changes the internals the patch relies on
    expect(shareReadBuffer()).toBe(true);
  });

  it('delivers every message intact through the shared buffer, also split over many reads', async () => {
    shareReadBuffer();
    const messages = [0, 1, 200, 70_000, 300_000, 5].map((size, n) => Uint8Array.from({ length: size }, (_, i) => (i * 31 + n) & 0xff));
    const server = await messageServer(messages);
    const nc = await connect({ servers: `127.0.0.1:${server.port}` });
    try {
      expect(readsShared((nc as any).protocol.transport.socket)).toBe(true);
      const received: Uint8Array[] = [];
      const done = new Promise<void>((resolve) => {
        nc.subscribe('read.test', {
          callback: (_err, msg) => {
            received.push(msg.data);
            if (received.length === messages.length) resolve();
          },
        });
      });
      await done;
      // compared after the last read: a view into the reused buffer would show bytes of later reads by now
      expect(received.map((data, i) => Buffer.from(data).equals(Buffer.from(messages[i])))).toEqual(messages.map(() => true));
    } finally {
      await nc.close();
      await server.close();
    }
  });

  it('dials the stock way when the server asks for TLS, which cannot start on a shared-buffer socket', async () => {
    shareReadBuffer();
    const server = await messageServer([], { tls_required: true });
    try {
      await expect(connect({ servers: `127.0.0.1:${server.port}`, timeout: 500, reconnect: false })).rejects.toBeTruthy();
      expect(server.connections.length).toBeGreaterThanOrEqual(2);
      // the first socket never sent anything, the second started a TLS handshake
      expect(Buffer.concat(server.connections[0]).length).toBe(0);
      expect(Buffer.concat(server.connections[1])[0]).toBe(0x16);
    } finally {
      await server.close();
    }
  });

  it('leaves a connection with TLS options on the stock read path', async () => {
    shareReadBuffer();
    const transport = Object.assign(Object.create(NodeTransport.prototype), { options: { tls: {} } });
    const server = await messageServer([]);
    try {
      const socket = await transport.dial({ hostname: '127.0.0.1', port: server.port });
      expect(readsShared(socket)).toBe(false);
      socket.destroy();
    } finally {
      await server.close();
    }
  });

  it('keeps reads that arrive before peekInfo or setupHandlers listen and hands them on in order', async () => {
    shareReadBuffer();
    const server = await quietServer();
    const transport = new NodeTransport();
    try {
      const socket = await transport.dial({ hostname: '127.0.0.1', port: server.port });
      transport.socket = socket;
      expect(readsShared(socket)).toBe(true);
      await vi.waitFor(() => expect(server.sockets.length).toBe(1));
      const info = `INFO ${JSON.stringify({ server_id: 'quiet' })}\r\n`;
      server.sockets[0].write(`${info}PING\r\n`);
      await vi.waitFor(() => expect(socket.bytesRead).toBe(info.length + 6));
      await expect(Promise.race([transport.peekInfo(), sleep(500).then(() => 'never answered')])).resolves.toMatchObject({ server_id: 'quiet' });

      server.sockets[0].write('PONG\r\n');
      await vi.waitFor(() => expect(socket.bytesRead).toBe(info.length + 12));
      transport.setupHandlers();
      expect(Buffer.concat(transport.yields).toString()).toBe(`${info}PING\r\nPONG\r\n`);
    } finally {
      transport.socket?.destroy();
      await server.close();
    }
  });

  it('destroys the socket when a data listener throws instead of throwing out of the read callback', async () => {
    shareReadBuffer();
    const server = await quietServer();
    const transport = new NodeTransport();
    try {
      const socket = await transport.dial({ hostname: '127.0.0.1', port: server.port });
      const failed = new Promise<Error>((resolve) => socket.once('error', resolve));
      socket.on('data', () => {
        throw new Error('listener failed');
      });
      await vi.waitFor(() => expect(server.sockets.length).toBe(1));
      server.sockets[0].write('PING\r\n');
      await expect(failed).resolves.toMatchObject({ message: 'listener failed' });
      expect(socket.destroyed).toBe(true);
    } finally {
      await server.close();
    }
  });

  it('stays off where Node cannot load the transport', async () => {
    vi.resetModules();
    const getBuiltinModule = process.getBuiltinModule;
    process.getBuiltinModule = ((id: string) => (id === 'node:module' ? undefined : getBuiltinModule(id))) as typeof process.getBuiltinModule;
    try {
      const patches = await import('../src/patches.js');
      expect(patches.shareReadBuffer()).toBe(false);
    } finally {
      process.getBuiltinModule = getBuiltinModule;
      vi.resetModules();
    }
  });
});
