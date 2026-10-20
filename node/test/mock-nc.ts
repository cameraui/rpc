import { expect } from 'vitest';

import { createChunks } from '../src/chunking.js';
import { RPCClient } from '../src/client.js';
import { encode } from '../src/codec.js';

export interface MockSub {
  pattern: string;
  callback: (err: Error | null, msg: any) => void;
  closed: boolean;
  ended?: string;
  unsubscribe(): void;
  drain(): Promise<void>;
  isClosed(): boolean;
}

export interface Published {
  subject: string;
  data: Uint8Array;
  opts?: { reply?: string; headers?: any };
}

export function createMockNc() {
  const subs: MockSub[] = [];
  const published: Published[] = [];
  const requests: Published[] = [];
  const calls: string[] = [];
  const statusFeeds = new Set<{ push: (status: { type: string } | null) => void }>();
  let isClosed = false;
  let settleClosed!: () => void;
  const closed = new Promise<void>((resolve) => {
    settleClosed = resolve;
    // status iterators end with the connection, as in nats.js
  }).then(() => {
    for (const feed of statusFeeds) feed.push(null);
  });
  const mock = {
    emitStatus(status: { type: string }): void {
      for (const feed of statusFeeds) feed.push(status);
    },
    status(): AsyncIterable<{ type: string }> {
      const queue: ({ type: string } | null)[] = [];
      let wake: (() => void) | undefined;
      const feed = {
        push: (status: { type: string } | null) => {
          queue.push(status);
          wake?.();
        },
      };
      statusFeeds.add(feed);
      return {
        async *[Symbol.asyncIterator]() {
          try {
            while (true) {
              if (queue.length === 0) {
                await new Promise<void>((resolve) => {
                  wake = resolve;
                });
                wake = undefined;
              }
              const status = queue.shift()!;
              if (status === null) return;
              yield status;
            }
          } finally {
            statusFeeds.delete(feed);
          }
        },
      };
    },
    subs,
    published,
    requests,
    calls,
    onFlush: (): Promise<void> => Promise.resolve(),
    flush(): Promise<void> {
      calls.push('flush');
      return mock.onFlush();
    },
    close(): Promise<void> {
      calls.push('close');
      isClosed = true;
      settleClosed();
      return Promise.resolve();
    },
    closed: (): Promise<void> => closed,
    lose(): void {
      isClosed = true;
      for (const sub of subs) sub.closed = true;
      settleClosed();
    },
    onRequest: (_subject: string, _data: Uint8Array): Promise<any> => new Promise(() => {}),
    request(subject: string, data: Uint8Array, opts?: { headers?: any }): Promise<any> {
      requests.push({ subject, data, opts });
      return mock.onRequest(subject, data);
    },
    onDrain: (_sub: MockSub): Promise<void> => Promise.resolve(),
    subscribe(pattern: string, opts: { callback: (err: Error | null, msg: any) => void }): MockSub {
      const sub: MockSub = {
        pattern,
        callback: opts.callback,
        closed: false,
        unsubscribe() {
          this.closed = true;
          this.ended ??= 'unsubscribe';
        },
        drain() {
          this.ended ??= 'drain';
          calls.push(`drain ${pattern}`);
          return mock.onDrain(sub).then(() => {
            sub.closed = true;
          });
        },
        isClosed() {
          return this.closed;
        },
      };
      subs.push(sub);
      return sub;
    },
    publish(subject: string, data: Uint8Array, opts?: { reply?: string; headers?: any }): void {
      published.push({ subject, data, opts });
    },
    isClosed: () => isClosed,
  };
  return mock;
}

export function createClient(options?: Partial<ConstructorParameters<typeof RPCClient>[0]>) {
  const client = new RPCClient({
    servers: ['nats://127.0.0.1:4222'],
    name: 'mux-test',
    // retries would re-publish and slow the 503 tests down
    noResponderRetry: { maxRetries: 0, delays: [1] },
    ...options,
  });
  const nc = createMockNc();
  (client as any).nc = nc;
  return { client, nc };
}

export function hostServices(nc: ReturnType<typeof createMockNc>): void {
  Object.assign(nc, { addCloseListener() {}, removeCloseListener() {} });
  nc.subscribe = ((pattern: string, opts: { callback: (err: Error | null, msg: any) => void; queue?: string }) => {
    const sub = {
      pattern,
      queue: opts?.queue,
      callback: opts?.callback,
      closed: false as boolean | Promise<void>,
      drain: () => Promise.resolve(),
      unsubscribe() {
        sub.gone = true;
      },
      gone: false,
      isClosed: () => sub.gone,
    };
    sub.closed = new Promise(() => {});
    nc.subs.push(sub as any);
    return sub as any;
  }) as any;
}

export function muxSub(client: RPCClient, nc: ReturnType<typeof createMockNc>): MockSub {
  const prefix = (client as any).replyPrefix as string;
  const matches = nc.subs.filter((s) => s.pattern === `rpc.reply.${prefix}.>`);
  expect(matches.length).toBe(1);
  return matches[0];
}

export function emptyHeaders(code = 0) {
  return { code, get: (_k: string) => undefined as string | undefined };
}

export function headersWith(entries: Record<string, string>) {
  return { code: 0, get: (k: string) => entries[k] };
}

export function chunkedMessages(subject: string, payload: unknown, transferId: string, chunkSize: number) {
  const encoded = encode(payload);
  const totalChunks = Math.ceil(encoded.length / chunkSize);
  const header = {
    subject,
    data: encode({ type: 'chunked', transferId, totalChunks, totalSize: encoded.length, chunkSize }),
    headers: headersWith({ 'x-chunked-transfer': 'header', 'x-chunk-id': transferId }),
  };
  const chunks = [...createChunks(encoded, transferId, chunkSize)].map((chunk) => ({
    subject,
    data: chunk.data,
    headers: headersWith({ 'x-chunked-transfer': 'chunk', 'x-chunk-id': transferId, 'x-chunk-index': chunk.chunkIndex.toString() }),
  }));
  return { header, chunks };
}

export async function drainMicrotasks(rounds = 50): Promise<void> {
  for (let i = 0; i < rounds; i++) await Promise.resolve();
}
