import { decodeMessage } from './codec.js';

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

export interface ChunkMessage {
  id: string;
  chunkIndex: number;
  data: Uint8Array;
  isLast: boolean;
}

export class TransferBudget {
  public readonly limit: number;
  private used = 0;

  constructor(limit?: number) {
    this.limit = limit && limit > 0 ? limit : DEFAULT_MAX_TRANSFER_SIZE;
  }

  public get inUse(): number {
    return this.used;
  }

  public reserve(bytes: number): boolean {
    if (this.used + bytes > this.limit) {
      return false;
    }
    this.used += bytes;
    return true;
  }

  public release(bytes: number): void {
    this.used = Math.max(0, this.used - bytes);
  }
}

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

export class ChunkAssembler {
  private buffer?: Uint8Array;
  private receivedChunks = new Set<number>();
  private totalChunks = 0;
  private chunkSize = 0;
  private reserved = 0;

  constructor(
    public readonly id: string,
    private readonly totalSize: number,
    totalChunks: number,
    chunkSize?: number,
    private readonly budget?: TransferBudget,
  ) {
    this.totalChunks = totalChunks;
    this.chunkSize = chunkSize ?? Math.ceil(totalSize / totalChunks);
  }

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

  public isComplete(): boolean {
    return this.receivedChunks.size === this.totalChunks;
  }

  public getData<T = any>(): T {
    if (!this.isComplete()) {
      throw new Error('Not all chunks received');
    }
    return decodeMessage(this.buffer ?? new Uint8Array(this.totalSize));
  }

  public release(): void {
    if (this.reserved > 0) {
      this.budget?.release(this.reserved);
      this.reserved = 0;
    }
  }

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

export class ChunkingManager {
  private transfers = new Map<string, Transfer>();
  private sweepTimer?: ReturnType<typeof setTimeout>;
  private maxTransferSize: number;
  private budget: TransferBudget;

  constructor(
    maxTransferSize?: number,
    private readonly maxTransfers = MAX_CONCURRENT_TRANSFERS,
    budget?: TransferBudget,
  ) {
    this.maxTransferSize = maxTransferSize && maxTransferSize > 0 ? maxTransferSize : DEFAULT_MAX_TRANSFER_SIZE;
    this.budget = budget ?? new TransferBudget(this.maxTransferSize);
  }

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

  public cancel(id: string): void {
    this.remove(id)?.onError(new Error('Transfer cancelled'));
  }

  public discard(id: string): void {
    this.remove(id);
  }

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

  public get size(): number {
    return this.transfers.size;
  }

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

export function parseChunkIndex(raw: string | null | undefined): number {
  return raw && /^\d{1,15}$/.test(raw) ? Number(raw) : NaN;
}
