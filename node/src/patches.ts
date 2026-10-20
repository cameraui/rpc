import * as natsInternal from '@nats-io/nats-core/internal';

import { frameHeadSize, writeFrame, writeFrameHead } from './codec.js';

import type { NatsConnection } from '@nats-io/nats-core';
import type { FrameParts } from './codec.js';

interface ParserEvent {
  kind: number;
  msg?: unknown;
  data?: Uint8Array;
}

interface ProtocolInternals {
  sendCommand(cmd: Uint8Array): void;
  flushPending(): void;
  outBytes: number;
  outMsgs: number;
  connected?: boolean;
  infoReceived?: boolean;
  info?: { max_payload?: number };
  transport?: { socket?: unknown };
}

interface CorkableSocket {
  cork(): void;
  uncork(): void;
}

interface OutboundInternals {
  buffers: (Uint8Array | string)[];
  byteLength: number;
}

interface SendPathInternals {
  outbound: OutboundInternals;
  pendingLimit: number;
  infoReceived: boolean;
  connected: boolean;
  transport?: unknown;
  sendCommand(cmd: string | Uint8Array, ...payloads: Uint8Array[]): void;
  flushPending(): void;
  publish(subject: string, payload: Uint8Array, options?: { reply?: string }): void;
}

interface NodeTransportInternals {
  readonly isClosed: boolean;
  socket?: { write(frame: Uint8Array): unknown };
  options?: { debug?: boolean };
  send(frame: Uint8Array): void;
}

type NodeTransportClass = (abstract new (...args: never[]) => unknown) & { prototype: NodeTransportInternals };

interface ServerAddress {
  hostname: string;
  port: number;
}

interface ReadPathInternals {
  socket?: ReadSocket;
  options?: { tls?: unknown };
  yields: Uint8Array[];
  connect(hp: ServerAddress, options: unknown): Promise<void>;
  dial(hp: ServerAddress): Promise<ReadSocket>;
  peekInfo(): Promise<unknown>;
  setupHandlers(): void;
  startTLS(): Promise<ReadSocket>;
}

interface ReadSocket {
  emit(event: 'data', frame: Uint8Array): boolean;
  on(event: 'error' | 'close', listener: (error?: Error) => void): unknown;
  listenerCount(event: string): number;
  removeAllListeners(): unknown;
  setNoDelay(noDelay: boolean): unknown;
  destroy(error?: Error): unknown;
}

interface NetModule {
  createConnection(
    options: { port: number; host: string; onread: { buffer: Uint8Array; callback: (bytes: number, buffer: Uint8Array) => void } },
    connected: () => void,
  ): ReadSocket;
}

class TlsNeedsStockReads extends Error {}

interface ConnectionInternals {
  protocol?: unknown;
  _check?: (subject: string, sub: boolean, pub: boolean) => void;
}

const isNode = typeof process !== 'undefined' && !!process.versions?.node;
const encoder = new TextEncoder();
const CRLF = Uint8Array.of(13, 10);

// smaller frames cost less packed with their neighbours than in a socket write each
export const PUBLISH_FRAME_MIN_SIZE = 32 * 1024;
const READ_BUFFER_SIZE = 64 * 1024;

// read via the namespace: a name dropped by a newer nats-core turns the patch off instead of failing the module link
const { DataBuffer, DenoBuffer, Parser, ProtocolHandler, State } = natsInternal as Partial<typeof natsInternal>;

let parserPatched: boolean | undefined;
let sendPathPatched: boolean | undefined;
let readPathPatched: boolean | undefined;
const sharedReadSockets = new WeakMap<object, Uint8Array[]>();
const tlsServers = new Set<string>();

export function publishFrame(nc: NatsConnection, subject: string, reply: string | undefined, parts: FrameParts): boolean {
  const conn = nc as unknown as ConnectionInternals;
  const protocol = conn.protocol;
  if (!isNode || typeof conn._check !== 'function' || !hasProtocolShape(protocol)) return false;
  if (protocol.info?.max_payload !== undefined && parts.size > protocol.info.max_payload) return false;

  conn._check(subject, false, true);
  if (reply) conn._check(reply, false, true);

  const head = encoder.encode(reply ? `PUB ${subject} ${reply} ${parts.size}\r\n` : `PUB ${subject} ${parts.size}\r\n`);
  const command = Buffer.allocUnsafe(head.length + parts.size + 2);
  command.set(head, 0);
  writeFrame(parts, command, head.length);
  command[command.length - 2] = 13;
  command[command.length - 1] = 10;

  // flushing first leaves the frame alone in the queue, so it is sent without packing
  protocol.flushPending();
  protocol.outBytes += parts.size;
  protocol.outMsgs++;
  protocol.sendCommand(command);
  protocol.flushPending();
  return true;
}

export function publishFrameZeroCopy(nc: NatsConnection, subject: string, reply: string | undefined, parts: FrameParts): boolean {
  const conn = nc as unknown as ConnectionInternals;
  const protocol = conn.protocol;
  if (!isNode || typeof conn._check !== 'function' || !hasProtocolShape(protocol)) return false;
  if (protocol.connected !== true || protocol.infoReceived !== true) return false;
  if (protocol.info?.max_payload !== undefined && parts.size > protocol.info.max_payload) return false;

  conn._check(subject, false, true);
  if (reply) conn._check(reply, false, true);

  const line = encoder.encode(reply ? `PUB ${subject} ${reply} ${parts.size}\r\n` : `PUB ${subject} ${parts.size}\r\n`);
  const head = Buffer.allocUnsafe(line.length + frameHeadSize(parts));
  head.set(line, 0);
  writeFrameHead(parts, head, line.length);

  const socket = corkableSocket(protocol.transport?.socket);
  socket?.cork();
  try {
    protocol.flushPending();
    protocol.outBytes += parts.size;
    protocol.outMsgs++;
    sendNow(protocol, head);
    for (const segment of parts.segments) sendNow(protocol, segment);
    sendNow(protocol, CRLF);
  } finally {
    socket?.uncork();
  }
  return true;
}

export function flushOutbound(nc: NatsConnection): void {
  const protocol = (nc as unknown as ConnectionInternals).protocol;
  if (isNode && hasProtocolShape(protocol)) {
    protocol.flushPending();
  }
}

export function presizeSplitPayloads(): boolean {
  parserPatched ??= installParserPatch();
  return parserPatched;
}

function installParserPatch(): boolean {
  if (!isNode || !Parser || !State || !DenoBuffer || !hasParserShape()) return false;

  const proto = Parser.prototype;
  const stockParse: (this: natsInternal.Parser, buf: Uint8Array) => void = Reflect.get(proto, 'parse');
  proto.parse = function (this: natsInternal.Parser, buf: Uint8Array): void {
    const before = this.msgBuf;
    stockParse.call(this, buf);
    const msgBuf = this.msgBuf;
    if (msgBuf === undefined || msgBuf === before || this.state !== State.MSG_PAYLOAD) return;

    const part = msgBuf.bytes({ copy: false });
    if (part.length >= this.ma.size) return;
    const store = new Uint8Array(this.ma.size);
    store.set(part);
    const presized = new DenoBuffer(store.buffer);
    presized.truncate(part.length);
    this.msgBuf = presized;
  };

  if (!probeSplitMessage(true)) {
    proto.parse = stockParse;
    return false;
  }
  return true;
}

export function packSmallPublishes(): boolean {
  sendPathPatched ??= installSendPatch();
  return sendPathPatched;
}

function installSendPatch(): boolean {
  const Transport = isNode ? nodeTransportClass() : undefined;
  if (!Transport || !ProtocolHandler || !DataBuffer || !hasSendShape(Transport)) return false;

  const proto = ProtocolHandler.prototype as unknown as SendPathInternals;
  const transportProto = Transport.prototype;
  const stockSendCommand: SendPathInternals['sendCommand'] = Reflect.get(proto, 'sendCommand');
  const stockFlushPending: SendPathInternals['flushPending'] = Reflect.get(proto, 'flushPending');
  const stockSend: NodeTransportInternals['send'] = Reflect.get(transportProto, 'send');
  const utf8Write: ((this: Buffer, text: string, offset: number) => number) | undefined = Reflect.get(Buffer.prototype, 'utf8Write');

  proto.sendCommand = function (this: SendPathInternals, cmd: string | Uint8Array, ...payloads: Uint8Array[]): void {
    if (!(this.transport instanceof Transport)) {
      stockSendCommand.call(this, cmd, ...payloads);
      return;
    }
    const outbound = this.outbound;
    const queued = outbound.buffers.length;
    if (typeof cmd === 'string') {
      outbound.buffers.push(cmd);
      outbound.byteLength += Buffer.byteLength(cmd);
    } else if (cmd) {
      outbound.buffers.push(cmd);
      outbound.byteLength += cmd.length;
    }
    for (const payload of payloads) {
      if (payload?.length) {
        outbound.buffers.push(payload);
        outbound.byteLength += payload.length;
      }
    }
    if (queued === 0) {
      queueMicrotask(() => this.flushPending());
    } else if (outbound.byteLength >= this.pendingLimit) {
      this.flushPending();
    }
  };

  proto.flushPending = function (this: SendPathInternals): void {
    if (!(this.transport instanceof Transport)) {
      stockFlushPending.call(this);
      return;
    }
    const outbound = this.outbound;
    if (!this.infoReceived || !this.connected || outbound.byteLength === 0) return;
    const buffers = outbound.buffers;
    let frame: Uint8Array;
    const only = buffers.length === 1 ? buffers[0] : undefined;
    if (only !== undefined && typeof only !== 'string') {
      frame = only;
    } else {
      const packed = Buffer.allocUnsafe(outbound.byteLength);
      let offset = 0;
      for (const buffer of buffers) {
        if (typeof buffer === 'string') {
          offset += utf8Write ? utf8Write.call(packed, buffer, offset) : packed.write(buffer, offset);
        } else {
          packed.set(buffer, offset);
          offset += buffer.length;
        }
      }
      frame = packed;
    }
    buffers.length = 0;
    outbound.byteLength = 0;
    this.transport.send(frame);
  };

  transportProto.send = function (this: NodeTransportInternals, frame: Uint8Array): void {
    if (this.options?.debug) {
      stockSend.call(this, frame);
      return;
    }
    if (this.isClosed || this.socket === undefined) return;
    try {
      this.socket.write(frame);
    } catch {
      // the read side or the heartbeat notices a broken socket, as with the stock send
    }
  };

  if (!probeSendPath(proto, Transport)) {
    proto.sendCommand = stockSendCommand;
    proto.flushPending = stockFlushPending;
    transportProto.send = stockSend;
    return false;
  }
  return true;
}

function probeSendPath(proto: SendPathInternals, Transport: NodeTransportClass): boolean {
  if (!DataBuffer) return false;
  try {
    const written: Buffer[] = [];
    const transport = Object.assign(Object.create(Transport.prototype) as object, {
      done: false,
      options: {},
      socket: { write: (frame: Uint8Array) => written.push(Buffer.from(frame)) },
    });
    const protocol = Object.assign(Object.create(proto) as SendPathInternals, {
      outbound: new DataBuffer(),
      pendingLimit: 32 * 1024,
      infoReceived: true,
      connected: true,
      outBytes: 0,
      outMsgs: 0,
      options: {},
      transport,
    });
    protocol.publish('probe.ü', Uint8Array.of(1, 2, 3), { reply: 'probe.reply' });
    protocol.flushPending();
    const expected = Buffer.concat([Buffer.from('PUB probe.ü probe.reply 3\r\n'), Buffer.from([1, 2, 3]), Buffer.from('\r\n')]);
    return written.length === 1 && written[0].equals(expected) && protocol.outbound.byteLength === 0;
  } catch {
    return false;
  }
}

function hasSendShape(Transport: NodeTransportClass): boolean {
  const proto = ProtocolHandler?.prototype as Partial<Record<keyof SendPathInternals, unknown>> | undefined;
  const outbound = DataBuffer ? (new DataBuffer() as unknown as Partial<OutboundInternals>) : undefined;
  const isClosed = Object.getOwnPropertyDescriptor(Transport.prototype, 'isClosed');
  return (
    typeof proto?.sendCommand === 'function' &&
    typeof proto.flushPending === 'function' &&
    typeof proto.publish === 'function' &&
    Array.isArray(outbound?.buffers) &&
    typeof outbound.byteLength === 'number' &&
    typeof Transport.prototype.send === 'function' &&
    typeof isClosed?.get === 'function'
  );
}

export function shareReadBuffer(): boolean {
  readPathPatched ??= installReadPatch();
  return readPathPatched;
}

export function readsShared(socket: unknown): boolean {
  return typeof socket === 'object' && socket !== null && sharedReadSockets.has(socket);
}

function installReadPatch(): boolean {
  const Transport = isNode ? nodeTransportClass() : undefined;
  const net = isNode ? builtinModule<NetModule>('node:net') : undefined;
  if (!Transport || typeof net?.createConnection !== 'function' || !hasReadShape(Transport)) return false;

  const proto = Transport.prototype as unknown as ReadPathInternals;
  const stockConnect: ReadPathInternals['connect'] = Reflect.get(proto, 'connect');
  const stockDial: ReadPathInternals['dial'] = Reflect.get(proto, 'dial');
  const stockPeekInfo: ReadPathInternals['peekInfo'] = Reflect.get(proto, 'peekInfo');
  const stockSetupHandlers: ReadPathInternals['setupHandlers'] = Reflect.get(proto, 'setupHandlers');
  const stockStartTLS: ReadPathInternals['startTLS'] = Reflect.get(proto, 'startTLS');
  const shared = Buffer.allocUnsafeSlow(READ_BUFFER_SIZE);

  proto.dial = function (this: ReadPathInternals, hp: ServerAddress): Promise<ReadSocket> {
    if (this.options?.tls || tlsServers.has(serverKey(hp))) return stockDial.call(this, hp);
    return new Promise<ReadSocket>((resolve, reject) => {
      let dialError: Error | undefined;
      const socket = net.createConnection(
        {
          port: hp.port,
          host: hp.hostname,
          onread: {
            buffer: shared,
            callback: (bytes, buffer) => {
              // own memory per read like the stock path, a view of the shared buffer must not escape
              const frame = Buffer.allocUnsafeSlow(bytes);
              frame.set(buffer.subarray(0, bytes));
              deliverRead(socket, frame);
            },
          },
        },
        () => {
          socket.removeAllListeners();
          resolve(socket);
        },
      );
      sharedReadSockets.set(socket, []);
      socket.on('error', (error) => {
        dialError = error;
      });
      socket.on('close', () => {
        socket.removeAllListeners();
        reject(dialError);
      });
      socket.setNoDelay(true);
    });
  };

  proto.peekInfo = function (this: ReadPathInternals): Promise<unknown> {
    const info = stockPeekInfo.call(this);
    replayReads(this.socket);
    return info;
  };

  proto.setupHandlers = function (this: ReadPathInternals): void {
    stockSetupHandlers.call(this);
    replayReads(this.socket);
  };

  proto.startTLS = function (this: ReadPathInternals): Promise<ReadSocket> {
    if (readsShared(this.socket)) return Promise.reject(new TlsNeedsStockReads('TLS needs a socket without onread'));
    return stockStartTLS.call(this);
  };

  proto.connect = async function (this: ReadPathInternals, hp: ServerAddress, options: unknown): Promise<void> {
    try {
      await stockConnect.call(this, hp, options);
    } catch (error) {
      if (!(error instanceof TlsNeedsStockReads)) throw error;
      tlsServers.add(serverKey(hp));
      this.yields = [];
      await stockConnect.call(this, hp, options);
    }
  };
  return true;
}

function deliverRead(socket: ReadSocket, frame: Uint8Array): void {
  if (socket.listenerCount('data') === 0) {
    sharedReadSockets.get(socket)?.push(frame);
    return;
  }
  try {
    socket.emit('data', frame);
  } catch (error) {
    socket.destroy(error instanceof Error ? error : new Error(String(error)));
  }
}

function replayReads(socket: ReadSocket | undefined): void {
  const backlog = socket ? sharedReadSockets.get(socket) : undefined;
  while (backlog?.length && socket!.listenerCount('data') > 0) {
    deliverRead(socket!, backlog.shift()!);
  }
}

function serverKey(hp: ServerAddress): string {
  return `${hp.hostname}:${hp.port}`;
}

function hasReadShape(Transport: NodeTransportClass): boolean {
  const proto = Transport.prototype as unknown as Partial<Record<keyof ReadPathInternals, unknown>>;
  if (
    typeof proto.connect !== 'function' ||
    typeof proto.dial !== 'function' ||
    typeof proto.peekInfo !== 'function' ||
    typeof proto.setupHandlers !== 'function' ||
    typeof proto.startTLS !== 'function'
  ) {
    return false;
  }
  try {
    return Array.isArray(new (Transport as unknown as new () => { yields?: unknown })().yields);
  } catch {
    return false;
  }
}

function builtinModule<T>(id: string): T | undefined {
  const runtime = (globalThis as { process?: { getBuiltinModule?: (id: string) => unknown } }).process;
  return runtime?.getBuiltinModule?.(id) as T | undefined;
}

function nodeTransportClass(): NodeTransportClass | undefined {
  try {
    // a static import would pull node:net into browser bundles
    const nodeModule = builtinModule<{ createRequire?: (from: string) => (id: string) => unknown }>('node:module');
    const load = nodeModule?.createRequire?.(import.meta.url);
    const Transport = (load?.('@nats-io/transport-node/lib/node_transport.js') as { NodeTransport?: unknown } | undefined)?.NodeTransport;
    return typeof Transport === 'function' ? (Transport as NodeTransportClass) : undefined;
  } catch {
    return undefined;
  }
}

function hasParserShape(): boolean {
  const proto = (Parser as unknown as { prototype?: Record<string, unknown> } | undefined)?.prototype;
  const buffer = (DenoBuffer as unknown as { prototype?: Record<string, unknown> } | undefined)?.prototype;
  return (
    typeof proto?.parse === 'function' &&
    typeof State?.MSG_PAYLOAD === 'number' &&
    typeof buffer?.bytes === 'function' &&
    typeof buffer.truncate === 'function' &&
    typeof buffer.write === 'function' &&
    probeSplitMessage(false)
  );
}

function probeSplitMessage(presized: boolean): boolean {
  if (!Parser || !State || !DenoBuffer) return false;
  try {
    const events: ParserEvent[] = [];
    const parser = new Parser({ push: (event: ParserEvent) => events.push(event) } as unknown as ConstructorParameters<typeof Parser>[0]);
    parser.parse(encoder.encode('MSG probe 1 8\r\nabc'));
    const msgBuf = parser.msgBuf;
    if (parser.state !== State.MSG_PAYLOAD || !(msgBuf instanceof DenoBuffer) || parser.ma?.size !== 8 || msgBuf.length !== 3) return false;
    if (presized && msgBuf.capacity !== 8) return false;
    parser.parse(encoder.encode('defgh\r\n'));
    const data = events[0]?.data;
    return events.length === 1 && data !== undefined && new TextDecoder().decode(data) === 'abcdefgh';
  } catch {
    return false;
  }
}

function sendNow(protocol: ProtocolInternals, buffer: Uint8Array): void {
  // nats-core writes a queue of one buffer as it is and packs a longer queue into a copy
  protocol.sendCommand(buffer);
  protocol.flushPending();
}

function corkableSocket(socket: unknown): CorkableSocket | undefined {
  const s = socket as Partial<CorkableSocket> | null | undefined;
  return typeof s?.cork === 'function' && typeof s.uncork === 'function' ? (s as CorkableSocket) : undefined;
}

function hasProtocolShape(protocol: unknown): protocol is ProtocolInternals {
  const p = protocol as Partial<ProtocolInternals> | null | undefined;
  return typeof p?.sendCommand === 'function' && typeof p.flushPending === 'function' && typeof p.outBytes === 'number' && typeof p.outMsgs === 'number';
}
