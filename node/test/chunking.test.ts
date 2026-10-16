import { afterEach, describe, expect, it, vi } from 'vitest';

import { ChunkAssembler, ChunkingManager, createChunks, parseChunkIndex, TransferBudget, type ChunkMessage } from '../src/chunking.js';
import { encode } from '../src/codec.js';

function collectChunks(encoded: Uint8Array, chunkId: string, maxChunkSize: number): ChunkMessage[] {
  return [...createChunks(encoded, chunkId, maxChunkSize)];
}

function reassemble<T>(encoded: Uint8Array, chunks: ChunkMessage[], chunkSize: number): T {
  const assembler = new ChunkAssembler(chunks[0].id, encoded.length, chunks.length, chunkSize);
  let complete = false;
  for (const chunk of chunks) {
    complete = assembler.addChunk(chunk);
  }
  expect(complete).toBe(true);
  return assembler.getData<T>();
}

describe('createChunks', () => {
  it('splits data into the expected number of chunks', () => {
    const encoded = new Uint8Array(250);
    const chunks = collectChunks(encoded, 'id-1', 100);
    expect(chunks.length).toBe(3);
    expect(chunks.map((c) => c.data.length)).toEqual([100, 100, 50]);
  });

  it('marks only the final chunk as last', () => {
    const chunks = collectChunks(new Uint8Array(250), 'id-1', 100);
    expect(chunks.map((c) => c.isLast)).toEqual([false, false, true]);
  });

  it('assigns sequential chunk indices', () => {
    const chunks = collectChunks(new Uint8Array(250), 'id-1', 100);
    expect(chunks.map((c) => c.chunkIndex)).toEqual([0, 1, 2]);
  });

  it('propagates the chunk id', () => {
    const chunks = collectChunks(new Uint8Array(50), 'transfer-abc', 100);
    expect(chunks.every((c) => c.id === 'transfer-abc')).toBe(true);
  });

  it('produces a single chunk when data fits exactly', () => {
    const chunks = collectChunks(new Uint8Array(100), 'id-1', 100);
    expect(chunks.length).toBe(1);
    expect(chunks[0].isLast).toBe(true);
  });

  it('produces a single chunk for data smaller than the chunk size', () => {
    const chunks = collectChunks(new Uint8Array(10), 'id-1', 100);
    expect(chunks.length).toBe(1);
  });

  it('produces no chunks for an empty payload', () => {
    const chunks = collectChunks(new Uint8Array(0), 'id-1', 100);
    expect(chunks.length).toBe(0);
  });

  it('splits one byte over a chunk boundary into two chunks', () => {
    const chunks = collectChunks(new Uint8Array(101), 'id-1', 100);
    expect(chunks.length).toBe(2);
    expect(chunks.map((c) => c.data.length)).toEqual([100, 1]);
  });
});

describe('ChunkAssembler', () => {
  it('reassembles a multi-chunk payload', () => {
    const payload = { items: Array.from({ length: 500 }, (_, i) => ({ id: i, value: `v${i}` })) };
    const encoded = encode(payload);
    const chunkSize = 64;
    const chunks = collectChunks(encoded, 'big', chunkSize);
    expect(chunks.length).toBeGreaterThan(1);
    expect(reassemble(encoded, chunks, chunkSize)).toEqual(payload);
  });

  it('reassembles a single-chunk payload', () => {
    const payload = { hello: 'world' };
    const encoded = encode(payload);
    const chunks = collectChunks(encoded, 'single', 1024);
    expect(chunks.length).toBe(1);
    expect(reassemble(encoded, chunks, 1024)).toEqual(payload);
  });

  it('reassembles correctly when chunks arrive out of order', () => {
    const payload = Array.from({ length: 200 }, (_, i) => i);
    const encoded = encode(payload);
    const chunkSize = 16;
    const chunks = collectChunks(encoded, 'ooo', chunkSize);
    const assembler = new ChunkAssembler('ooo', encoded.length, chunks.length, chunkSize);
    for (const chunk of [...chunks].reverse()) {
      assembler.addChunk(chunk);
    }
    expect(assembler.isComplete()).toBe(true);
    expect(assembler.getData()).toEqual(payload);
  });

  it('reports progress as chunks are added', () => {
    const chunks = collectChunks(new Uint8Array(300), 'p', 100);
    const assembler = new ChunkAssembler('p', 300, chunks.length, 100);
    expect(assembler.getProgress()).toEqual({ received: 0, total: 3, percentage: 0 });
    assembler.addChunk(chunks[0]);
    expect(assembler.getProgress().received).toBe(1);
    expect(assembler.getProgress().percentage).toBeCloseTo(33.333, 2);
  });

  it('throws when getData is called before completion', () => {
    const chunks = collectChunks(new Uint8Array(300), 'x', 100);
    const assembler = new ChunkAssembler('x', 300, chunks.length, 100);
    assembler.addChunk(chunks[0]);
    expect(() => assembler.getData()).toThrow('Not all chunks received');
  });

  it('throws on chunk id mismatch', () => {
    const assembler = new ChunkAssembler('expected', 10, 1, 10);
    expect(() => assembler.addChunk({ id: 'other', chunkIndex: 0, data: new Uint8Array(10), isLast: true })).toThrow('Chunk ID mismatch');
  });
});

describe('ChunkingManager', () => {
  it('invokes onComplete with the reassembled data', () => {
    const payload = { value: Array.from({ length: 100 }, (_, i) => i) };
    const encoded = encode(payload);
    const chunkSize = 32;
    const chunks = collectChunks(encoded, 't1', chunkSize);

    const manager = new ChunkingManager();
    let received: unknown;
    let errored: Error | undefined;
    manager.startReceiving(
      't1',
      chunks.length,
      (data) => {
        received = data;
      },
      (err) => {
        errored = err;
      },
      encoded.length,
      chunkSize,
    );

    for (const chunk of chunks) {
      manager.processChunk(chunk);
    }

    expect(errored).toBeUndefined();
    expect(received).toEqual(payload);
  });

  it('reports null progress for an unknown transfer', () => {
    const manager = new ChunkingManager();
    expect(manager.getProgress('missing')).toBeNull();
  });

  it('clears progress once a transfer completes', () => {
    const encoded = encode({ a: 1 });
    const chunks = collectChunks(encoded, 'done', 1024);
    const manager = new ChunkingManager();
    manager.startReceiving('done', chunks.length, () => {}, () => {}, encoded.length, 1024);
    for (const chunk of chunks) {
      manager.processChunk(chunk);
    }
    expect(manager.getProgress('done')).toBeNull();
  });

  it('invokes the error callback when a transfer is cancelled', () => {
    const manager = new ChunkingManager();
    let errored: Error | undefined;
    manager.startReceiving('c', 4, () => {}, (err) => {
      errored = err;
    }, 400, 100);
    manager.cancel('c');
    expect(errored).toBeInstanceOf(Error);
    expect(errored?.message).toBe('Transfer cancelled');
    expect(manager.getProgress('c')).toBeNull();
  });
});

describe('ChunkingManager header checks', () => {
  function start(manager: ChunkingManager, totalChunks: number, totalSize: number, chunkSize?: number) {
    const errors: string[] = [];
    manager.startReceiving(
      'h',
      totalChunks,
      () => {},
      (err) => errors.push(err.message),
      totalSize,
      chunkSize,
    );
    return errors;
  }

  it.each([
    ['negative totalSize', 1, -1, 100],
    ['zero totalSize', 0, 0, 100],
    ['fractional totalSize', 1, 10.5, 100],
    ['zero chunkSize', 1, 10, 0],
    ['chunkSize above 64 MiB', 1, 10, 64 * 1024 * 1024 + 1],
    ['missing chunkSize', 1, 10, undefined],
    ['totalChunks not matching', 2, 250, 100],
    ['totalChunks as string', '3' as unknown as number, 250, 100],
  ])('drops a header with %s without allocating', (_name, totalChunks, totalSize, chunkSize) => {
    const manager = new ChunkingManager();
    const errors = start(manager, totalChunks, totalSize, chunkSize);
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatch(/^Invalid chunk header/);
    expect(manager.size).toBe(0);
  });

  it('accepts an integral float and the default 1 GiB limit', () => {
    const manager = new ChunkingManager();
    expect(start(manager, 3.0, 250, 100.0)).toEqual([]);
    expect(manager.size).toBe(1);
    manager.dispose();
    expect(start(manager, 1, 1024 * 1024 * 1024 + 1, 64 * 1024 * 1024)).toHaveLength(1);
  });

  it('honours maxTransferSize', () => {
    const manager = new ChunkingManager(1000);
    expect(start(manager, 11, 1001, 100)).toHaveLength(1);
    expect(start(manager, 10, 1000, 100)).toEqual([]);
    expect(manager.size).toBe(1);
    manager.dispose();
  });

  it('treats maxTransferSize 0 as the default', () => {
    const manager = new ChunkingManager(0);
    expect(start(manager, 10, 2000, 200)).toEqual([]);
    manager.dispose();
  });

  it('drops headers beyond 32 concurrent transfers', () => {
    const manager = new ChunkingManager();
    const errors: string[] = [];
    for (let i = 0; i < 33; i++) {
      manager.startReceiving(
        `t${i}`,
        2,
        () => {},
        (err) => errors.push(err.message),
        200,
        100,
      );
    }
    expect(manager.size).toBe(32);
    expect(errors).toEqual(['Too many concurrent chunked transfers (32)']);
    manager.dispose();
  });
});

describe('ChunkingManager chunk checks', () => {
  function setup() {
    const payload = { value: Array.from({ length: 100 }, (_, i) => i) };
    const encoded = encode(payload);
    const chunks = collectChunks(encoded, 't', 64);
    const budget = new TransferBudget();
    const manager = new ChunkingManager(undefined, undefined, budget);
    const result: { data?: unknown; errors: string[]; completions: number } = { errors: [], completions: 0 };
    manager.startReceiving(
      't',
      chunks.length,
      (data) => {
        result.data = data;
        result.completions++;
      },
      (err) => result.errors.push(err.message),
      encoded.length,
      64,
    );
    return { payload, chunks, budget, manager, result };
  }

  // each case sends chunk 0 first, so the budget is reserved when the bad chunk arrives
  it.each([
    ['a NaN index', (chunks: ChunkMessage[]) => ({ ...chunks[1], chunkIndex: parseChunkIndex('x') }), /Invalid chunk index/],
    ['a signed index', (chunks: ChunkMessage[]) => ({ ...chunks[1], chunkIndex: parseChunkIndex('+1') }), /Invalid chunk index/],
    ['a negative index', (chunks: ChunkMessage[]) => ({ ...chunks[1], chunkIndex: -1 }), /Invalid chunk index/],
    ['an index of totalChunks', (chunks: ChunkMessage[]) => ({ ...chunks[1], chunkIndex: chunks.length }), /Invalid chunk index/],
    ['a fractional index', (chunks: ChunkMessage[]) => ({ ...chunks[1], chunkIndex: 0.5 }), /Invalid chunk index/],
    ['a short chunk', (chunks: ChunkMessage[]) => ({ ...chunks[1], data: chunks[1].data.subarray(1) }), /^Invalid length/],
    ['a long chunk', (chunks: ChunkMessage[]) => ({ ...chunks[1], data: new Uint8Array(chunks[1].data.length + 1) }), /^Invalid length/],
    ['a short last chunk', (chunks: ChunkMessage[]) => ({ ...chunks.at(-1)!, data: chunks.at(-1)!.data.subarray(1) }), /^Invalid length/],
    ['a long last chunk', (chunks: ChunkMessage[]) => ({ ...chunks.at(-1)!, data: new Uint8Array(chunks.at(-1)!.data.length + 1) }), /^Invalid length/],
    ['a duplicate with the wrong length', (chunks: ChunkMessage[]) => ({ ...chunks[0], data: chunks[0].data.subarray(1) }), /^Invalid length/],
  ])('drops the whole transfer on %s', (_name, bad, message) => {
    const { chunks, budget, manager, result } = setup();
    manager.processChunk(chunks[0]);
    expect(budget.inUse).toBeGreaterThan(0);

    manager.processChunk(bad(chunks));
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0]).toMatch(message);
    expect(manager.size).toBe(0);
    expect(budget.inUse).toBe(0);

    for (const chunk of chunks) manager.processChunk(chunk);
    expect(result.completions).toBe(0);
    expect(result.errors).toHaveLength(1);
    expect(manager.size).toBe(0);
    expect(budget.inUse).toBe(0);
  });

  it('keeps the first copy of a duplicate chunk with the right length', () => {
    const { payload, chunks, manager, result } = setup();
    manager.processChunk(chunks[0]);
    manager.processChunk({ ...chunks[0], data: new Uint8Array(chunks[0].data.length).fill(0xff) });
    for (const chunk of chunks.slice(1)) manager.processChunk(chunk);
    expect(result.errors).toEqual([]);
    expect(result.completions).toBe(1);
    expect(result.data).toEqual(payload);
  });

  it('ignores a duplicate chunk instead of counting it', () => {
    const { payload, chunks, manager, result } = setup();
    for (let i = 0; i < chunks.length - 1; i++) {
      manager.processChunk(chunks[i]);
      manager.processChunk(chunks[i]);
    }
    expect(result.completions).toBe(0);
    manager.processChunk(chunks[chunks.length - 1]);
    expect(result.completions).toBe(1);
    expect(result.data).toEqual(payload);
    expect(result.errors).toEqual([]);
  });

  it('ignores a chunk for an unknown transfer', () => {
    const manager = new ChunkingManager();
    expect(() => manager.processChunk({ id: 'nobody', chunkIndex: 0, data: new Uint8Array(1), isLast: true })).not.toThrow();
  });
});

describe('ChunkingManager transfer budget', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  function open(manager: ChunkingManager, id: string, totalSize: number, errors: string[] = [], done: unknown[] = []) {
    manager.startReceiving(
      id,
      Math.ceil(totalSize / 100),
      (data) => done.push(data),
      (err) => errors.push(err.message),
      totalSize,
      100,
    );
  }

  function chunk(id: string, index: number, length = 100): ChunkMessage {
    return { id, chunkIndex: index, data: new Uint8Array(length), isLast: false };
  }

  it('reserves the buffer with the first chunk and returns it when the transfer completes', () => {
    const budget = new TransferBudget(1000);
    const manager = new ChunkingManager(1000, undefined, budget);
    const payload = encode({ a: 'x'.repeat(150) });
    const done: unknown[] = [];
    open(manager, 't', payload.length, [], done);
    expect(budget.inUse).toBe(0);

    const chunks = collectChunks(payload, 't', 100);
    manager.processChunk(chunks[0]);
    expect(budget.inUse).toBe(payload.length);
    for (const c of chunks.slice(1)) manager.processChunk(c);

    expect(done).toEqual([{ a: 'x'.repeat(150) }]);
    expect(budget.inUse).toBe(0);
  });

  it('drops a transfer whose buffer does not fit the budget shared with another manager', () => {
    const budget = new TransferBudget(1000);
    const first = new ChunkingManager(1000, undefined, budget);
    const second = new ChunkingManager(1000, undefined, budget);
    const errors: string[] = [];
    open(first, 'a', 800);
    first.processChunk(chunk('a', 0));
    open(second, 'b', 300, errors);
    second.processChunk(chunk('b', 0));

    expect(errors).toEqual(['Transfers in progress would hold more than 1000 bytes']);
    expect(second.size).toBe(0);
    expect(budget.inUse).toBe(800);

    first.discard('a');
    open(second, 'c', 300, errors);
    second.processChunk(chunk('c', 0));
    expect(budget.inUse).toBe(300);
    second.dispose();
    expect(budget.inUse).toBe(0);
  });

  it('returns the reservation of a cancelled, replaced or stalled transfer', () => {
    vi.useFakeTimers();
    const budget = new TransferBudget(10_000);
    const manager = new ChunkingManager(10_000, undefined, budget);
    for (const id of ['cancelled', 'replaced', 'stalled']) {
      open(manager, id, 400);
      manager.processChunk(chunk(id, 0));
    }
    expect(budget.inUse).toBe(1200);

    manager.cancel('cancelled');
    expect(budget.inUse).toBe(800);
    open(manager, 'replaced', 400);
    expect(budget.inUse).toBe(400);
    vi.advanceTimersByTime(31_000);
    expect(manager.size).toBe(0);
    expect(budget.inUse).toBe(0);
  });

  it('gives a manager without a shared budget its own of maxTransferSize', () => {
    const manager = new ChunkingManager(250);
    const errors: string[] = [];
    open(manager, 'a', 200, errors);
    manager.processChunk(chunk('a', 0));
    open(manager, 'b', 200, errors);
    manager.processChunk(chunk('b', 0));

    expect(errors).toEqual(['Transfers in progress would hold more than 250 bytes']);
    manager.dispose();
  });
});

describe('ChunkingManager stale sweep', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('drops a stalled transfer by timer without further chunk traffic', () => {
    vi.useFakeTimers();
    const manager = new ChunkingManager();
    const errors: string[] = [];
    manager.startReceiving(
      's',
      4,
      () => {},
      (err) => errors.push(err.message),
      400,
      100,
    );
    manager.processChunk({ id: 's', chunkIndex: 0, data: new Uint8Array(100), isLast: false });

    vi.advanceTimersByTime(29_000);
    expect(manager.size).toBe(1);
    vi.advanceTimersByTime(2_000);
    expect(manager.size).toBe(0);
    expect(errors).toEqual(['Transfer timed out']);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('keeps a transfer alive while chunks arrive and drops it once they stop', () => {
    vi.useFakeTimers();
    const manager = new ChunkingManager();
    expect(vi.getTimerCount()).toBe(0);
    const errors: string[] = [];
    manager.startReceiving(
      'a',
      4,
      () => {},
      (err) => errors.push(err.message),
      400,
      100,
    );
    expect(vi.getTimerCount()).toBe(1);
    for (let i = 0; i < 3; i++) {
      vi.advanceTimersByTime(20_000);
      manager.processChunk({ id: 'a', chunkIndex: i, data: new Uint8Array(100), isLast: false });
    }
    expect(manager.size).toBe(1);
    expect(errors).toEqual([]);

    vi.advanceTimersByTime(31_000);
    expect(manager.size).toBe(0);
    expect(errors).toEqual(['Transfer timed out']);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('stops the timer on dispose', () => {
    vi.useFakeTimers();
    const manager = new ChunkingManager();
    manager.startReceiving(
      'd',
      4,
      () => {},
      () => {},
      400,
      100,
    );
    expect(vi.getTimerCount()).toBe(1);
    manager.dispose();
    expect(manager.size).toBe(0);
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe('parseChunkIndex', () => {
  it('accepts plain decimal integers only', () => {
    expect(parseChunkIndex('0')).toBe(0);
    expect(parseChunkIndex('7')).toBe(7);
    expect(parseChunkIndex('007')).toBe(7);
    expect(parseChunkIndex('42')).toBe(42);
    expect(parseChunkIndex('9'.repeat(15))).toBe(999999999999999);
    for (const raw of ['', undefined, null, 'x', '+1', '-1', '-0', '1.0', '1.5', '1e3', ' 1', '1 ', '0x10', 'abc', '1abc', '9'.repeat(16), '\u0661', '\uff11']) {
      expect(parseChunkIndex(raw)).toBeNaN();
    }
  });
});
