import { Packr, Unpackr } from 'msgpackr';

const packr = new Packr({
  useRecords: false,
  bundleStrings: false, // a msgpackr extension the other languages cannot read
  int64AsType: 'number',
});

const unpackr = new Unpackr({
  useRecords: false,
  bundleStrings: false,
  int64AsType: 'number',
});

export function encode(data: any): Uint8Array {
  return packr.pack(data);
}

export function decode<T = any>(data: Uint8Array): T {
  return unpackr.unpack(data);
}

export const BINARY_EXTRACT_THRESHOLD = 16_384;

const MAGIC = [0x43, 0x55, 0x49, 0x42] as const; // "CUIB", no multi-byte msgpack value starts with 0x43
const HAS_BUFFER = typeof Buffer !== 'undefined';
const HEADER_SIZE = 8; // magic + u32 LE envLen
const PLACEHOLDER_KEY = '__cui_bin__';

function isPlainObject(value: any): value is Record<string, any> {
  if (value === null || typeof value !== 'object') return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function byteView(value: ArrayBufferView): Uint8Array {
  return value instanceof Uint8Array ? value : new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
}

function asExtractableSegment(value: any): Uint8Array | undefined {
  if (ArrayBuffer.isView(value)) {
    return value.byteLength >= BINARY_EXTRACT_THRESHOLD ? byteView(value) : undefined;
  }
  if (value instanceof ArrayBuffer) {
    return value.byteLength >= BINARY_EXTRACT_THRESHOLD ? new Uint8Array(value) : undefined;
  }
  return undefined;
}

function extractBinaries(value: object, segments: Uint8Array[]): any {
  if (Array.isArray(value)) {
    let copy: any[] | undefined;
    for (let i = 0; i < value.length; i++) {
      const item = value[i];
      if (typeof item !== 'object' || item === null) continue;
      const transformed = extractBinaries(item, segments);
      if (transformed !== item) {
        copy ??= value.slice();
        copy[i] = transformed;
      }
    }
    return copy ?? value;
  }

  const proto = Object.getPrototypeOf(value);
  if (proto === Object.prototype || proto === null) {
    const record = value as Record<string, any>;
    let copy: Record<string, any> | undefined;
    // hasOwn skips keys inherited from a polluted prototype, msgpackr does not write them either
    for (const key in record) {
      const item = record[key];
      if (typeof item !== 'object' || item === null || !Object.hasOwn(record, key)) continue;
      const transformed = extractBinaries(item, segments);
      if (transformed !== item) {
        copy ??= { ...record };
        copy[key] = transformed;
      }
    }
    return copy ?? value;
  }

  const segment = asExtractableSegment(value);
  if (segment) {
    const index = segments.length;
    segments.push(segment);
    return { [PLACEHOLDER_KEY]: index, l: segment.byteLength };
  }
  // msgpackr cuts other typed arrays short and sends a DataView empty, so they go out as their bytes
  if (ArrayBuffer.isView(value) && !(value instanceof Uint8Array)) {
    return byteView(value);
  }
  return value;
}

function isBinaryPlaceholder(value: any): value is { [PLACEHOLDER_KEY]: number; l: number } {
  if (!isPlainObject(value)) return false;
  const index = value[PLACEHOLDER_KEY];
  const length = value.l;
  if (!Number.isInteger(index) || index < 0) return false;
  if (!Number.isInteger(length) || length < 0) return false;
  return Object.keys(value).length === 2;
}

interface RestoreState {
  offset: number;
  next: number;
  outOfOrder: boolean;
}

function restoreSequential(value: any, data: Uint8Array, state: RestoreState): any {
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) {
      const item = value[i];
      if (typeof item === 'object' && item !== null) {
        value[i] = restoreSequential(item, data, state);
      }
    }
    return value;
  }
  if (value[PLACEHOLDER_KEY] !== undefined && isBinaryPlaceholder(value)) {
    if (state.outOfOrder || value[PLACEHOLDER_KEY] !== state.next) {
      state.outOfOrder = true;
      return value;
    }
    const length = value.l;
    const end = state.offset + length;
    if (end > data.byteLength) {
      throw new Error(`Invalid CUIB frame: segment ${state.next} exceeds payload size ${data.byteLength}`);
    }
    const view = data.subarray(state.offset, end);
    state.offset = end;
    state.next++;
    return view;
  }
  if (isPlainObject(value)) {
    for (const key in value) {
      const item = value[key];
      if (typeof item === 'object' && item !== null && Object.hasOwn(value, key)) {
        value[key] = restoreSequential(item, data, state);
      }
    }
  }
  return value;
}

function collectSegmentLengths(value: any, lengths: number[]): void {
  if (isBinaryPlaceholder(value)) {
    lengths[value[PLACEHOLDER_KEY]] = value.l;
    return;
  }
  if (Array.isArray(value)) {
    for (const item of value) collectSegmentLengths(item, lengths);
    return;
  }
  if (isPlainObject(value)) {
    for (const key of Object.keys(value)) collectSegmentLengths(value[key], lengths);
  }
}

function restoreByIndex(value: any, views: Uint8Array[]): any {
  if (isBinaryPlaceholder(value)) {
    return views[value[PLACEHOLDER_KEY]];
  }
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) {
      value[i] = restoreByIndex(value[i], views);
    }
    return value;
  }
  if (isPlainObject(value)) {
    for (const key of Object.keys(value)) {
      value[key] = restoreByIndex(value[key], views);
    }
    return value;
  }
  return value;
}

function restoreRemaining(envelope: any, data: Uint8Array, state: RestoreState): any {
  const lengths: number[] = [];
  collectSegmentLengths(envelope, lengths);

  const views: Uint8Array[] = new Array(lengths.length);
  let offset = state.offset;
  for (let i = state.next; i < lengths.length; i++) {
    const length = lengths[i];
    if (length === undefined) {
      throw new Error(`Invalid CUIB frame: missing placeholder for segment ${i}`);
    }
    if (offset + length > data.byteLength) {
      throw new Error(`Invalid CUIB frame: segment ${i} exceeds payload size ${data.byteLength}`);
    }
    views[i] = data.subarray(offset, offset + length);
    offset += length;
  }
  state.offset = offset;

  return restoreByIndex(envelope, views);
}

export interface FrameParts {
  envelope: Uint8Array;
  segments: Uint8Array[];
  size: number;
}

export function encodeMessage(data: any): Uint8Array {
  const parts = encodeMessageParts(data);
  return parts instanceof Uint8Array ? parts : assembleFrame(parts);
}

export function assembleFrame(parts: FrameParts): Uint8Array {
  // no zero-fill, writeFrame overwrites every byte
  const out = typeof Buffer !== 'undefined' ? Buffer.allocUnsafe(parts.size) : new Uint8Array(parts.size);
  writeFrame(parts, out, 0);
  return out;
}

export function encodeMessageParts(data: any): Uint8Array | FrameParts {
  if (typeof data !== 'object' || data === null) {
    return packr.pack(data);
  }

  const segments: Uint8Array[] = [];
  const transformed = extractBinaries(data, segments);

  // the same object as `data` unless a typed array became its bytes
  if (segments.length === 0) {
    return packr.pack(transformed);
  }

  const envelope = packr.pack(transformed);
  let size = HEADER_SIZE + envelope.byteLength;
  for (const segment of segments) {
    size += segment.byteLength;
  }
  return { envelope, segments, size };
}

export function writeFrame(parts: FrameParts, out: Uint8Array, offset: number): void {
  let position = writeFrameHead(parts, out, offset);
  for (const segment of parts.segments) {
    out.set(segment, position);
    position += segment.byteLength;
  }
}

export function frameHeadSize(parts: FrameParts): number {
  return HEADER_SIZE + parts.envelope.byteLength;
}

export function writeFrameHead(parts: FrameParts, out: Uint8Array, offset: number): number {
  const envLen = parts.envelope.byteLength;
  out[offset] = MAGIC[0];
  out[offset + 1] = MAGIC[1];
  out[offset + 2] = MAGIC[2];
  out[offset + 3] = MAGIC[3];
  out[offset + 4] = envLen & 0xff;
  out[offset + 5] = (envLen >>> 8) & 0xff;
  out[offset + 6] = (envLen >>> 16) & 0xff;
  out[offset + 7] = (envLen >>> 24) & 0xff;
  out.set(parts.envelope, offset + HEADER_SIZE);
  return offset + HEADER_SIZE + envLen;
}

export function decodeMessage<T = any>(data: Uint8Array): T {
  if (HAS_BUFFER && !Buffer.isBuffer(data)) {
    data = Buffer.from(data.buffer, data.byteOffset, data.byteLength);
  }
  if (data.byteLength < HEADER_SIZE || data[0] !== MAGIC[0] || data[1] !== MAGIC[1] || data[2] !== MAGIC[2] || data[3] !== MAGIC[3]) {
    return unpackr.unpack(data);
  }

  const envLen = (data[4] | (data[5] << 8) | (data[6] << 16)) + data[7] * 0x1000000;
  const segmentBase = HEADER_SIZE + envLen;
  if (segmentBase > data.byteLength) {
    throw new Error(`Invalid CUIB frame: envelope length ${envLen} exceeds payload size ${data.byteLength}`);
  }

  let envelope = unpackr.unpack(data.subarray(HEADER_SIZE, segmentBase));

  const state: RestoreState = { offset: segmentBase, next: 0, outOfOrder: false };
  if (typeof envelope === 'object' && envelope !== null) {
    envelope = restoreSequential(envelope, data, state);
  }
  if (state.outOfOrder) {
    envelope = restoreRemaining(envelope, data, state);
  }

  if (state.offset !== data.byteLength) {
    throw new Error(`Invalid CUIB frame: expected payload size ${state.offset}, got ${data.byteLength}`);
  }

  return envelope;
}
