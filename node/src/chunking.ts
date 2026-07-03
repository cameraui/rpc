import { decodeMessage } from './codec.js';

/**
 * Message types for chunked transfers
 */
export interface ChunkMessage {
  id: string;
  chunkIndex: number;
  data: Uint8Array;
  isLast: boolean;
}

/**
 * Split data into chunks
 */
export function* createChunks(encoded: Uint8Array, chunkId: string, maxChunkSize: number): Generator<ChunkMessage> {
  const totalSize = encoded.length;
  const totalChunks = Math.ceil(totalSize / maxChunkSize);

  for (let i = 0; i < totalChunks; i++) {
    const start = i * maxChunkSize;
    const end = Math.min(start + maxChunkSize, totalSize);
    const chunk = encoded.subarray(start, end);

    yield {
      id: chunkId,
      chunkIndex: i,
      data: chunk,
      isLast: i === totalChunks - 1,
    };
  }
}

/**
 * Reassemble chunks back into original data using a pre-allocated buffer.
 * Chunks are written directly to the buffer - no intermediate Map storage.
 */
export class ChunkAssembler {
  private buffer!: Uint8Array;
  private receivedChunks = new Set<number>();
  private totalChunks = 0;
  private chunkSize = 0;

  constructor(
    public readonly id: string,
    totalSize: number,
    totalChunks: number,
    chunkSize?: number,
  ) {
    this.totalChunks = totalChunks;
    // Use provided chunkSize or calculate from totalSize/totalChunks
    this.chunkSize = chunkSize ?? Math.ceil(totalSize / totalChunks);
    this.buffer = new Uint8Array(totalSize);
  }

  /**
   * Add a chunk to the assembler.
   * Writes directly to the pre-allocated buffer.
   * @returns true if all chunks are received
   */
  public addChunk(chunk: ChunkMessage): boolean {
    if (chunk.id !== this.id) {
      throw new Error(`Chunk ID mismatch: expected ${this.id}, got ${chunk.id}`);
    }

    // Write chunk directly to pre-allocated buffer
    const offset = chunk.chunkIndex * this.chunkSize;
    this.buffer.set(chunk.data, offset);
    this.receivedChunks.add(chunk.chunkIndex);

    return this.isComplete();
  }

  /**
   * Check if all chunks have been received
   */
  public isComplete(): boolean {
    return this.receivedChunks.size === this.totalChunks;
  }

  /**
   * Get the reassembled data.
   * Buffer is already complete, just decode - no copy needed. Chunked
   * payloads are always full wire messages, so this goes through
   * decodeMessage: a reassembled CUIB frame yields zero-copy views into
   * the assembly buffer.
   */
  public getData<T = any>(): T {
    if (!this.isComplete()) {
      throw new Error('Not all chunks received');
    }
    return decodeMessage(this.buffer);
  }

  /**
   * Get progress information
   */
  public getProgress(): { received: number; total: number; percentage: number } {
    const received = this.receivedChunks.size;
    const total = this.totalChunks;
    const percentage = total > 0 ? (received / total) * 100 : 0;
    return { received, total, percentage };
  }
}

/**
 * Manager for handling multiple chunk transfers
 */
export class ChunkingManager {
  private static readonly STALE_TRANSFER_TTL_MS = 30_000;
  private static readonly SWEEP_INTERVAL_MS = 1_000;

  private assemblers = new Map<string, ChunkAssembler>();
  private completedCallbacks = new Map<string, (data: any) => void>();
  private errorCallbacks = new Map<string, (error: Error) => void>();
  private lastActivity = new Map<string, number>();
  private lastSweep = 0;

  /**
   * Start receiving chunks for a transfer.
   * Pre-allocates buffer for direct chunk writing.
   */
  public startReceiving(id: string, totalChunks: number, onComplete: (data: any) => void, onError: (error: Error) => void, totalSize: number, chunkSize?: number): void {
    this.sweepStale();
    const assembler = new ChunkAssembler(id, totalSize, totalChunks, chunkSize);
    this.assemblers.set(id, assembler);
    this.completedCallbacks.set(id, onComplete);
    this.errorCallbacks.set(id, onError);
    this.lastActivity.set(id, Date.now());
  }

  private sweepStale(): void {
    if (this.lastActivity.size === 0) return;
    // Time-gate: sweeping per chunk would iterate the activity map on every
    // message of a large transfer. Once per second is plenty for a 30s TTL.
    const now = Date.now();
    if (now - this.lastSweep < ChunkingManager.SWEEP_INTERVAL_MS) return;
    this.lastSweep = now;
    const cutoff = now - ChunkingManager.STALE_TRANSFER_TTL_MS;
    for (const [id, activity] of this.lastActivity) {
      if (activity < cutoff) {
        const errorCallback = this.errorCallbacks.get(id);
        this.assemblers.delete(id);
        this.completedCallbacks.delete(id);
        this.errorCallbacks.delete(id);
        this.lastActivity.delete(id);
        if (errorCallback) {
          errorCallback(new Error('Transfer timed out'));
        }
      }
    }
  }

  /**
   * Process an incoming chunk
   */
  public processChunk(chunk: ChunkMessage): void {
    this.sweepStale();
    const assembler = this.assemblers.get(chunk.id);
    if (!assembler) {
      console.warn(`Received chunk for unknown transfer: ${chunk.id}`);
      return;
    }
    this.lastActivity.set(chunk.id, Date.now());

    try {
      const isComplete = assembler.addChunk(chunk);
      if (isComplete) {
        const data = assembler.getData();
        const callback = this.completedCallbacks.get(chunk.id);

        // Cleanup
        this.assemblers.delete(chunk.id);
        this.completedCallbacks.delete(chunk.id);
        this.errorCallbacks.delete(chunk.id);
        this.lastActivity.delete(chunk.id);

        // Notify completion
        if (callback) {
          callback(data);
        }
      }
    } catch (error) {
      const errorCallback = this.errorCallbacks.get(chunk.id);

      // Cleanup
      this.assemblers.delete(chunk.id);
      this.completedCallbacks.delete(chunk.id);
      this.errorCallbacks.delete(chunk.id);
      this.lastActivity.delete(chunk.id);

      // Notify error
      if (errorCallback) {
        errorCallback(error as Error);
      }
    }
  }

  /**
   * Cancel a transfer
   */
  public cancel(id: string): void {
    const errorCallback = this.errorCallbacks.get(id);

    this.assemblers.delete(id);
    this.completedCallbacks.delete(id);
    this.errorCallbacks.delete(id);
    this.lastActivity.delete(id);

    if (errorCallback) {
      errorCallback(new Error('Transfer cancelled'));
    }
  }

  /**
   * Get progress for a transfer
   */
  public getProgress(id: string): { received: number; total: number; percentage: number } | null {
    const assembler = this.assemblers.get(id);
    return assembler ? assembler.getProgress() : null;
  }
}
