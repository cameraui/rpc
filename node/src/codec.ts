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
