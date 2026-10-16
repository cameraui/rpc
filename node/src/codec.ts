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
// encode(), except that a DataView or a typed array other than Uint8Array
// goes out as its bytes. Messages containing binary values (Uint8Array, Buffer,
// ArrayBuffer, any typed array or DataView) with
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
const HAS_BUFFER = typeof Buffer !== 'undefined';
const HEADER_SIZE = 8; // magic + u32 LE envLen
const PLACEHOLDER_KEY = '__cui_bin__';

function isPlainObject(value: any): value is Record<string, any> {
  if (value === null || typeof value !== 'object') return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

/** The bytes of a typed array or DataView, without a copy. */
function byteView(value: ArrayBufferView): Uint8Array {
  return value instanceof Uint8Array ? value : new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
}

/** Returns the segment view for extractable binaries, undefined otherwise. */
function asExtractableSegment(value: any): Uint8Array | undefined {
  if (ArrayBuffer.isView(value)) {
    return value.byteLength >= BINARY_EXTRACT_THRESHOLD ? byteView(value) : undefined;
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
 * (other than the binary types themselves) are left to msgpack. `value` must
 * be a non-null object; primitives are skipped by the caller.
 */
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
    // for-in visits own keys in Object.keys order; the hasOwn check skips keys
    // inherited from a polluted prototype, which msgpackr does not write either
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

/** Envelope and out-of-band segments of a CUIB frame, not yet assembled. */
export interface FrameParts {
  envelope: Uint8Array;
  segments: Uint8Array[];
  /** Byte size of the assembled frame */
  size: number;
}

/**
 * Encode a message for the wire. Large binaries (>= BINARY_EXTRACT_THRESHOLD)
 * are extracted into out-of-band segments after the msgpack envelope;
 * binary-free messages stay byte-identical to encode().
 */
export function encodeMessage(data: any): Uint8Array {
  const parts = encodeMessageParts(data);
  return parts instanceof Uint8Array ? parts : assembleFrame(parts);
}

/** Assemble the CUIB frame of `parts` into a new buffer. */
export function assembleFrame(parts: FrameParts): Uint8Array {
  // allocUnsafe skips zero-filling, writeFrame overwrites every byte; Uint8Array outside Node
  const out = typeof Buffer !== 'undefined' ? Buffer.allocUnsafe(parts.size) : new Uint8Array(parts.size);
  writeFrame(parts, out, 0);
  return out;
}

/**
 * encodeMessage without the final copy: plain msgpack bytes for a message
 * without large binaries, otherwise the parts of its CUIB frame.
 */
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

/** Write the CUIB frame of `parts` into `out` at `offset` (parts.size bytes). */
export function writeFrame(parts: FrameParts, out: Uint8Array, offset: number): void {
  let position = writeFrameHead(parts, out, offset);
  for (const segment of parts.segments) {
    out.set(segment, position);
    position += segment.byteLength;
  }
}

/** Byte size of the CUIB header and envelope of `parts`, the frame without its segments. */
export function frameHeadSize(parts: FrameParts): number {
  return HEADER_SIZE + parts.envelope.byteLength;
}

/**
 * Write the CUIB header and envelope of `parts` into `out` at `offset`
 * (frameHeadSize bytes). Returns the offset where the segments follow.
 */
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

/**
 * Decode a wire message. Payloads without the CUIB magic are plain msgpack.
 * For framed payloads the placeholder maps are replaced by zero-copy
 * Uint8Array subarray views into `data`. In Node every binary comes back as
 * a Buffer view, also when `data` is a plain Uint8Array (a message split
 * across socket reads, a reassembled chunked one).
 */
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
