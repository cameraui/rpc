import { Packr, Unpackr } from 'msgpackr';

// Configure msgpackr for better cross-language compatibility
// Disable bundleStrings to avoid proprietary extensions
const packr = new Packr({
  useRecords: false,
  bundleStrings: false, // Disable string bundling for compatibility
  // encodeUndefinedAsNil: true,
  int64AsType: 'number',
});

const unpackr = new Unpackr({
  useRecords: false,
  bundleStrings: false, // Disable string bundling for compatibility
  int64AsType: 'number',
});

/**
 * Encode data to MessagePack binary format
 * @param data - Any serializable data
 * @returns MessagePack encoded binary data
 */
export function encode(data: any): Uint8Array {
  return packr.pack(data);
}

/**
 * Decode MessagePack binary data
 * @param data - MessagePack binary data
 * @returns Decoded data
 */
export function decode<T = any>(data: Uint8Array): T {
  return unpackr.unpack(data);
}

// Messages without large binaries are plain msgpack — byte-identical to
// encode(). Messages containing Uint8Array/Buffer/ArrayBuffer values with
// byteLength >= BINARY_EXTRACT_THRESHOLD are framed as:
//
//   [4 bytes ASCII magic "CUIB"][u32 LE envLen][envelope: msgpack][bin0][bin1]...
//
// In the envelope every extracted binary is replaced by the placeholder map
// { "__cui_bin__": <index>, "l": <byteLength> } (index = position in the
// segment list, in traversal order; length redundant for validation).
// Segments are laid out back-to-back in index order directly after the
// envelope. The frame is self-describing, so it survives the chunking path
// (whose NATS headers are lost on reassembly).
//
// decodeMessage() replaces placeholders with zero-copy subarray views into
// the received buffer — consumers copy themselves if they need to retain the
// bytes beyond the buffer's lifetime (NATS delivers one buffer per message,
// so views are safe).
//
// msgpack envelopes start with 0x80-0x8f/0xde/0xdf (maps) or other type tags;
// no multi-byte msgpack encoding starts with 0x43 ('C'), so the magic cannot
// collide with plain-msgpack payloads.

/** Minimum byteLength for a binary value to be extracted out-of-band. */
export const BINARY_EXTRACT_THRESHOLD = 16_384;

const MAGIC = [0x43, 0x55, 0x49, 0x42] as const; // "CUIB"
const HEADER_SIZE = 8; // magic + u32 LE envLen
const PLACEHOLDER_KEY = '__cui_bin__';

function isPlainObject(value: any): value is Record<string, any> {
  if (value === null || typeof value !== 'object') return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

/** Returns the segment view for extractable binaries, undefined otherwise. */
function asExtractableSegment(value: any): Uint8Array | undefined {
  if (value instanceof Uint8Array) {
    return value.byteLength >= BINARY_EXTRACT_THRESHOLD ? value : undefined;
  }
  if (value instanceof ArrayBuffer) {
    return value.byteLength >= BINARY_EXTRACT_THRESHOLD ? new Uint8Array(value) : undefined;
  }
  return undefined;
}

/**
 * Depth-first copy-on-write transform: extracted binaries become placeholder
 * maps, untouched subtrees keep their identity (no copy for binary-free
 * messages). Only plain objects and arrays are traversed — class instances
 * (other than the binary types themselves) are left to msgpack.
 */
function extractBinaries(value: any, segments: Uint8Array[]): any {
  const segment = asExtractableSegment(value);
  if (segment) {
    const index = segments.length;
    segments.push(segment);
    return { [PLACEHOLDER_KEY]: index, l: segment.byteLength };
  }

  if (Array.isArray(value)) {
    let copy: any[] | undefined;
    for (let i = 0; i < value.length; i++) {
      const transformed = extractBinaries(value[i], segments);
      if (transformed !== value[i]) {
        copy ??= value.slice();
        copy[i] = transformed;
      }
    }
    return copy ?? value;
  }

  if (isPlainObject(value)) {
    let copy: Record<string, any> | undefined;
    for (const key of Object.keys(value)) {
      const transformed = extractBinaries(value[key], segments);
      if (transformed !== value[key]) {
        copy ??= { ...value };
        copy[key] = transformed;
      }
    }
    return copy ?? value;
  }

  return value;
}

/**
 * Strict placeholder check. A user map that happens to carry the
 * __cui_bin__ key but has extra keys, a missing "l" or non-integer values
 * is NOT treated as a placeholder and passes through untouched.
 */
function isBinaryPlaceholder(value: any): value is { [PLACEHOLDER_KEY]: number; l: number } {
  if (!isPlainObject(value)) return false;
  const index = value[PLACEHOLDER_KEY];
  const length = value.l;
  if (!Number.isInteger(index) || index < 0) return false;
  if (!Number.isInteger(length) || length < 0) return false;
  return Object.keys(value).length === 2;
}

interface RestoreState {
  /** Start offset of the next expected segment. */
  offset: number;
  /** Next expected segment index. */
  next: number;
  /** Set when a placeholder index arrives out of traversal order. */
  outOfOrder: boolean;
}

/**
 * Single-pass restore for the common case: placeholder indices appear in
 * increasing order during envelope traversal (an encoder that assigns
 * indices and serializes in one walk always produces this). Offsets are
 * then running prefix sums — no length-collection pass needed. Mutates the
 * freshly decoded envelope in place; on the first out-of-order index the
 * remaining placeholders are left untouched for the fallback pass.
 */
function restoreSequential(value: any, data: Uint8Array, state: RestoreState): any {
  if (isBinaryPlaceholder(value)) {
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
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) {
      value[i] = restoreSequential(value[i], data, state);
    }
    return value;
  }
  if (isPlainObject(value)) {
    for (const key of Object.keys(value)) {
      value[key] = restoreSequential(value[key], data, state);
    }
    return value;
  }
  return value;
}

/** Fallback pass 1: record remaining placeholders' lengths by segment index. */
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

/** Fallback pass 2: swap remaining placeholders for their segment views. */
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

/**
 * Out-of-order fallback: restoreSequential already consumed the in-order
 * prefix (segments 0..state.next-1); place the remaining segments via an
 * explicit index -> length table. Segments always lie back-to-back in index
 * order, whatever order their placeholders appear in.
 */
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

/**
 * Encode a message for the wire. Large binaries (>= BINARY_EXTRACT_THRESHOLD)
 * are extracted into out-of-band segments after the msgpack envelope;
 * binary-free messages stay byte-identical to encode().
 */
export function encodeMessage(data: any): Uint8Array {
  const segments: Uint8Array[] = [];
  const transformed = extractBinaries(data, segments);

  if (segments.length === 0) {
    return packr.pack(data);
  }

  const envelope = packr.pack(transformed);
  const envLen = envelope.byteLength;

  let totalSize = HEADER_SIZE + envLen;
  for (const segment of segments) {
    totalSize += segment.byteLength;
  }

  // Buffer.allocUnsafe skips zero-filling — the frame is fully overwritten
  // below. Fall back to Uint8Array outside Node.
  const out = typeof Buffer !== 'undefined' ? Buffer.allocUnsafe(totalSize) : new Uint8Array(totalSize);
  out[0] = MAGIC[0];
  out[1] = MAGIC[1];
  out[2] = MAGIC[2];
  out[3] = MAGIC[3];
  out[4] = envLen & 0xff;
  out[5] = (envLen >>> 8) & 0xff;
  out[6] = (envLen >>> 16) & 0xff;
  out[7] = (envLen >>> 24) & 0xff;
  out.set(envelope, HEADER_SIZE);

  let offset = HEADER_SIZE + envelope.byteLength;
  for (const segment of segments) {
    out.set(segment, offset);
    offset += segment.byteLength;
  }

  return out;
}

/**
 * Decode a wire message. Payloads without the CUIB magic are plain msgpack.
 * For framed payloads the placeholder maps are replaced by zero-copy
 * Uint8Array subarray views into `data`.
 */
export function decodeMessage<T = any>(data: Uint8Array): T {
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
  envelope = restoreSequential(envelope, data, state);
  if (state.outOfOrder) {
    envelope = restoreRemaining(envelope, data, state);
  }

  if (state.offset !== data.byteLength) {
    throw new Error(`Invalid CUIB frame: expected payload size ${state.offset}, got ${data.byteLength}`);
  }

  return envelope;
}
