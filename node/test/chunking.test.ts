import { describe, expect, it } from 'vitest';

import { ChunkAssembler, ChunkingManager, createChunks, type ChunkMessage } from '../src/chunking.js';
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
