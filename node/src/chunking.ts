import { decode } from './codec.js';

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
   * Buffer is already complete, just decode - no copy needed.
   */
  public getData<T = any>(): T {
    if (!this.isComplete()) {
      throw new Error('Not all chunks received');
    }
    return decode(this.buffer);
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
  private assemblers = new Map<string, ChunkAssembler>();
  private completedCallbacks = new Map<string, (data: any) => void>();
  private errorCallbacks = new Map<string, (error: Error) => void>();

  /**
   * Start receiving chunks for a transfer.
   * Pre-allocates buffer for direct chunk writing.
   */
  public startReceiving(id: string, totalChunks: number, onComplete: (data: any) => void, onError: (error: Error) => void, totalSize: number, chunkSize?: number): void {
    const assembler = new ChunkAssembler(id, totalSize, totalChunks, chunkSize);
    this.assemblers.set(id, assembler);
    this.completedCallbacks.set(id, onComplete);
    this.errorCallbacks.set(id, onError);
  }

  /**
   * Process an incoming chunk
   */
  public processChunk(chunk: ChunkMessage): void {
    const assembler = this.assemblers.get(chunk.id);
    if (!assembler) {
      console.warn(`Received chunk for unknown transfer: ${chunk.id}`);
      return;
    }

    try {
      const isComplete = assembler.addChunk(chunk);
      if (isComplete) {
        const data = assembler.getData();
        const callback = this.completedCallbacks.get(chunk.id);

        // Cleanup
        this.assemblers.delete(chunk.id);
        this.completedCallbacks.delete(chunk.id);
        this.errorCallbacks.delete(chunk.id);

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
