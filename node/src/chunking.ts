import { decodeMessage } from './codec.js';

/** Default for RPCClientOptions.maxTransferSize: 1 GiB. */
export const DEFAULT_MAX_TRANSFER_SIZE = 1024 * 1024 * 1024;

const MAX_CHUNK_SIZE = 64 * 1024 * 1024;
const MAX_CONCURRENT_TRANSFERS = 32;
const STALE_TRANSFER_TTL_MS = 30_000;

interface Transfer {
  assembler: ChunkAssembler;
  onComplete: (data: any) => void;
  onError: (error: Error) => void;
  lastActivity: number;
}

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
 * Bytes the buffers of all open transfers may hold together. A client shares
 * one budget between its managers, so a manager per subscription does not
 * multiply what a sender can make it allocate.
 */
export class TransferBudget {
  public readonly limit: number;
  private used = 0;

  /**
   * @param limit - Bytes in use at most (0 or undefined: 1 GiB)
   */
  constructor(limit?: number) {
    this.limit = limit && limit > 0 ? limit : DEFAULT_MAX_TRANSFER_SIZE;
  }

  /**
   * Bytes reserved right now
   */
  public get inUse(): number {
    return this.used;
  }

  /**
   * Take `bytes`, false when that would exceed the limit
   */
  public reserve(bytes: number): boolean {
    if (this.used + bytes > this.limit) {
      return false;
    }
    this.used += bytes;
    return true;
  }

  /**
   * Return bytes taken with reserve()
   */
  public release(bytes: number): void {
    this.used = Math.max(0, this.used - bytes);
  }
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
 * Reassemble chunks back into original data using a buffer of the full size,
 * allocated with the first chunk. Chunks are written directly to the buffer -
 * no intermediate Map storage.
 */
export class ChunkAssembler {
  private buffer?: Uint8Array;
  private receivedChunks = new Set<number>();
  private totalChunks = 0;
  private chunkSize = 0;
  private reserved = 0;

  /**
   * @param budget - Reserves the buffer with the first chunk; without it nothing is accounted
   */
  constructor(
    public readonly id: string,
    private readonly totalSize: number,
    totalChunks: number,
    chunkSize?: number,
    private readonly budget?: TransferBudget,
  ) {
    this.totalChunks = totalChunks;
    // Use provided chunkSize or calculate from totalSize/totalChunks
    this.chunkSize = chunkSize ?? Math.ceil(totalSize / totalChunks);
  }

  /**
   * Add a chunk to the assembler.
   * Writes directly to the buffer. A duplicate index is ignored.
   * @returns true if all chunks are received
   * @throws when the chunk does not fit this transfer (see checkChunk), or
   * when the budget has no room for the buffer
   */
  public addChunk(chunk: ChunkMessage): boolean {
    const problem = this.checkChunk(chunk);
    if (problem) {
      throw new Error(problem);
    }
    if (this.receivedChunks.has(chunk.chunkIndex)) {
      return false;
    }

    this.buffer ??= this.allocate();
    this.buffer.set(chunk.data, chunk.chunkIndex * this.chunkSize);
    this.receivedChunks.add(chunk.chunkIndex);

    return this.isComplete();
  }

  /**
   * Why a chunk does not fit this transfer, or undefined when it does: the
   * index must lie in [0, totalChunks) and the length must be chunkSize, or
   * the remainder for the last chunk.
   */
  public checkChunk(chunk: ChunkMessage): string | undefined {
    if (chunk.id !== this.id) {
      return `Chunk ID mismatch: expected ${this.id}, got ${chunk.id}`;
    }
    const index = chunk.chunkIndex;
    if (!Number.isInteger(index) || index < 0 || index >= this.totalChunks) {
      return `Invalid chunk index ${index} for ${this.totalChunks} chunks`;
    }
    const expected = index === this.totalChunks - 1 ? this.totalSize - index * this.chunkSize : this.chunkSize;
    if (chunk.data.length !== expected) {
      return `Invalid length ${chunk.data.length} for chunk ${index}, expected ${expected}`;
    }
    return undefined;
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
    return decodeMessage(this.buffer ?? new Uint8Array(this.totalSize));
  }

  /**
   * Return the bytes reserved for the buffer to the budget
   */
  public release(): void {
    if (this.reserved > 0) {
      this.budget?.release(this.reserved);
      this.reserved = 0;
    }
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

  private allocate(): Uint8Array {
    if (this.budget) {
      if (!this.budget.reserve(this.totalSize)) {
        throw new Error(`Transfers in progress would hold more than ${this.budget.limit} bytes`);
      }
      this.reserved = this.totalSize;
    }
    return new Uint8Array(this.totalSize);
  }
}

/**
 * Manager for handling multiple chunk transfers.
 *
 * A header is checked before anything is allocated, at most `maxTransfers`
 * transfers run at once, and a transfer without a chunk for 30 s is dropped
 * by a timer that only runs while transfers exist. A transfer reserves its
 * buffer in the budget with its first chunk and is dropped when the budget
 * has no room left. A chunk that does not fit drops its whole transfer.
 */
export class ChunkingManager {
  private transfers = new Map<string, Transfer>();
  private sweepTimer?: ReturnType<typeof setTimeout>;
  private maxTransferSize: number;
  private budget: TransferBudget;

  /**
   * @param maxTransferSize - Largest accepted totalSize in bytes (0 or undefined: 1 GiB)
   * @param maxTransfers - Most transfers open at once, further headers are refused (default 32)
   * @param budget - Bytes the buffers of open transfers may hold, shared with other managers (default: an own budget of maxTransferSize)
   */
  constructor(
    maxTransferSize?: number,
    private readonly maxTransfers = MAX_CONCURRENT_TRANSFERS,
    budget?: TransferBudget,
  ) {
    this.maxTransferSize = maxTransferSize && maxTransferSize > 0 ? maxTransferSize : DEFAULT_MAX_TRANSFER_SIZE;
    this.budget = budget ?? new TransferBudget(this.maxTransferSize);
  }

  /**
   * Start receiving chunks for a transfer. The buffer is allocated with the
   * first chunk. A header that does not describe a valid transfer, or one
   * beyond the transfer cap, is reported through onError.
   */
  public startReceiving(id: string, totalChunks: number, onComplete: (data: any) => void, onError: (error: Error) => void, totalSize: number, chunkSize?: number): void {
    const problem = this.checkHeader(totalChunks, totalSize, chunkSize);
    if (problem) {
      onError(new Error(`Invalid chunk header: ${problem}`));
      return;
    }
    if (!this.transfers.has(id) && this.transfers.size >= this.maxTransfers) {
      onError(new Error(`Too many concurrent chunked transfers (${this.maxTransfers})`));
      return;
    }

    this.transfers.get(id)?.assembler.release();
    const assembler = new ChunkAssembler(id, totalSize, totalChunks, chunkSize, this.budget);
    this.transfers.set(id, { assembler, onComplete, onError, lastActivity: Date.now() });
    this.armSweep();
  }

  /**
   * Process an incoming chunk. A chunk for an unknown transfer is ignored, a
   * duplicate of the right length too (the first copy wins). A chunk that
   * does not fit its transfer drops the transfer and is reported through
   * onError once; later chunks of that id count as unknown.
   */
  public processChunk(chunk: ChunkMessage): void {
    const transfer = this.transfers.get(chunk.id);
    if (!transfer) {
      return;
    }

    const problem = transfer.assembler.checkChunk(chunk);
    if (problem) {
      this.remove(chunk.id);
      transfer.onError(new Error(problem));
      return;
    }
    transfer.lastActivity = Date.now();

    let data: any;
    try {
      if (!transfer.assembler.addChunk(chunk)) {
        return;
      }
      data = transfer.assembler.getData();
    } catch (error) {
      this.remove(chunk.id);
      transfer.onError(error as Error);
      return;
    }

    this.remove(chunk.id);
    transfer.onComplete(data);
  }

  /**
   * Cancel a transfer
   */
  public cancel(id: string): void {
    this.remove(id)?.onError(new Error('Transfer cancelled'));
  }

  /**
   * Drop a transfer without reporting it, e.g. when nobody waits for it anymore.
   */
  public discard(id: string): void {
    this.remove(id);
  }

  /**
   * Drop every transfer without reporting it and stop the sweep timer.
   */
  public dispose(): void {
    for (const transfer of this.transfers.values()) {
      transfer.assembler.release();
    }
    this.transfers.clear();
    if (this.sweepTimer !== undefined) {
      clearTimeout(this.sweepTimer);
      this.sweepTimer = undefined;
    }
  }

  /**
   * Number of open transfers
   */
  public get size(): number {
    return this.transfers.size;
  }

  /**
   * Get progress for a transfer
   */
  public getProgress(id: string): { received: number; total: number; percentage: number } | null {
    return this.transfers.get(id)?.assembler.getProgress() ?? null;
  }

  private checkHeader(totalChunks: number, totalSize: number, chunkSize: number | undefined): string | undefined {
    if (!Number.isSafeInteger(totalSize) || !Number.isSafeInteger(totalChunks) || !Number.isSafeInteger(chunkSize)) {
      return `totalSize ${totalSize}, totalChunks ${totalChunks} and chunkSize ${chunkSize} must be integers`;
    }
    if (chunkSize! <= 0 || chunkSize! > MAX_CHUNK_SIZE) {
      return `chunkSize ${chunkSize} out of range`;
    }
    if (totalSize <= 0 || totalSize > this.maxTransferSize) {
      return `totalSize ${totalSize} out of range (max ${this.maxTransferSize})`;
    }
    if (totalChunks !== Math.ceil(totalSize / chunkSize!)) {
      return `totalChunks ${totalChunks} does not match totalSize ${totalSize} / chunkSize ${chunkSize}`;
    }
    return undefined;
  }

  private remove(id: string): Transfer | undefined {
    const transfer = this.transfers.get(id);
    if (transfer) {
      this.transfers.delete(id);
      transfer.assembler.release();
    }
    return transfer;
  }

  private armSweep(): void {
    if (this.sweepTimer !== undefined || this.transfers.size === 0) {
      return;
    }
    let oldest = Infinity;
    for (const transfer of this.transfers.values()) {
      oldest = Math.min(oldest, transfer.lastActivity);
    }
    const timer = setTimeout(() => this.sweepStale(), Math.max(0, oldest + STALE_TRANSFER_TTL_MS - Date.now()) + 1);
    // browsers return a number here
    (timer as { unref?: () => void }).unref?.();
    this.sweepTimer = timer;
  }

  private sweepStale(): void {
    this.sweepTimer = undefined;
    const cutoff = Date.now() - STALE_TRANSFER_TTL_MS;
    for (const [id, transfer] of this.transfers) {
      if (transfer.lastActivity <= cutoff) {
        this.remove(id);
        transfer.onError(new Error('Transfer timed out'));
      }
    }
    this.armSweep();
  }
}

/**
 * Parse an `x-chunk-index` header. Anything but a plain decimal integer
 * yields NaN, which the assembler rejects.
 */
export function parseChunkIndex(raw: string | null | undefined): number {
  return raw && /^\d{1,15}$/.test(raw) ? Number(raw) : NaN;
}
