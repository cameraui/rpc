import { describe, expect, it } from 'vitest';

import { BINARY_EXTRACT_THRESHOLD, decode, decodeMessage, encode, encodeMessage } from '../src/codec.js';

function roundtrip<T>(value: T): T {
  return decode<T>(encode(value));
}

function messageRoundtrip<T>(value: T): T {
  return decodeMessage<T>(encodeMessage(value));
}

describe('codec', () => {
  it('encodes to a Uint8Array', () => {
    expect(encode({ a: 1 })).toBeInstanceOf(Uint8Array);
  });

  describe('numbers', () => {
    it('round-trips small positive integers', () => {
      expect(roundtrip(42)).toBe(42);
    });

    it('round-trips zero', () => {
      expect(roundtrip(0)).toBe(0);
    });

    it('round-trips negative integers', () => {
      expect(roundtrip(-12345)).toBe(-12345);
    });

    it('round-trips large integers', () => {
      expect(roundtrip(9007199254740991)).toBe(9007199254740991);
    });

    it('round-trips floats', () => {
      expect(roundtrip(3.14159)).toBeCloseTo(3.14159);
    });

    it('round-trips negative floats', () => {
      expect(roundtrip(-0.0001)).toBeCloseTo(-0.0001);
    });
  });

  describe('primitives', () => {
    it('round-trips true', () => {
      expect(roundtrip(true)).toBe(true);
    });

    it('round-trips false', () => {
      expect(roundtrip(false)).toBe(false);
    });

    it('round-trips null', () => {
      expect(roundtrip(null)).toBe(null);
    });
  });

  describe('strings', () => {
    it('round-trips an empty string', () => {
      expect(roundtrip('')).toBe('');
    });

    it('round-trips ascii strings', () => {
      expect(roundtrip('hello world')).toBe('hello world');
    });

    it('round-trips unicode strings', () => {
      expect(roundtrip('你好世界 🌍')).toBe('你好世界 🌍');
    });

    it('round-trips emoji strings', () => {
      expect(roundtrip('🎉🎊')).toBe('🎉🎊');
    });
  });

  describe('binary', () => {
    it('round-trips a Uint8Array', () => {
      const bytes = new Uint8Array([0, 1, 2, 254, 255]);
      const result = roundtrip(bytes);
      expect(Array.from(result)).toEqual([0, 1, 2, 254, 255]);
    });

    it('round-trips a Buffer as binary', () => {
      const buf = Buffer.from([10, 20, 30]);
      const result = roundtrip<Uint8Array>(buf);
      expect(Array.from(result)).toEqual([10, 20, 30]);
    });

    it('round-trips an empty binary buffer', () => {
      const result = roundtrip(new Uint8Array([]));
      expect(result.length).toBe(0);
    });
  });

  describe('collections', () => {
    it('round-trips an empty array', () => {
      expect(roundtrip([])).toEqual([]);
    });

    it('round-trips a flat array', () => {
      expect(roundtrip([1, 2, 3, 'a', true, null])).toEqual([1, 2, 3, 'a', true, null]);
    });

    it('round-trips an empty object', () => {
      expect(roundtrip({})).toEqual({});
    });

    it('round-trips a nested object', () => {
      const value = {
        name: 'service',
        meta: { version: 2, tags: ['x', 'y'], nested: { deep: true } },
        items: [{ id: 1 }, { id: 2 }],
      };
      expect(roundtrip(value)).toEqual(value);
    });

    it('encodes a Map as a plain key/value object for cross-language compatibility', () => {
      const map = new Map<string, number>([
        ['a', 1],
        ['b', 2],
      ]);
      const result = roundtrip(map) as unknown as Record<string, number>;
      expect(result).toEqual({ a: 1, b: 2 });
    });
  });

  it('preserves a complex mixed payload through a round-trip', () => {
    const payload = {
      id: '12345-abcde',
      method: 'rpc.app.doThing',
      params: [1, -2, 3.5, 'text', '你好', true, null, [4, 5], { k: 'v' }],
    };
    expect(roundtrip(payload)).toEqual(payload);
  });
});

describe('message codec (CUIB wire format)', () => {
  const bytes = (length: number, fill = 7): Uint8Array => new Uint8Array(length).fill(fill);
  // Extractable sizes are expressed relative to the threshold so the tests
  // survive threshold tuning.
  const T = BINARY_EXTRACT_THRESHOLD;

  describe('framing', () => {
    it('keeps a message without large binaries byte-identical to encode()', () => {
      const message = { id: 'abc', method: 'call', params: [1, 'x', { nested: true }, new Uint8Array([1, 2, 3])] };
      expect(Buffer.from(encodeMessage(message))).toEqual(Buffer.from(encode(message)));
    });

    it('frames extracted binaries as magic + u32 LE envLen + envelope + segments', () => {
      const bin = bytes(T + 1024, 9);
      const message = { id: 'abc', params: [bin] };
      const encoded = encodeMessage(message);

      // Magic "CUIB"
      expect(Array.from(encoded.subarray(0, 4))).toEqual([0x43, 0x55, 0x49, 0x42]);

      const envLen = new DataView(encoded.buffer, encoded.byteOffset).getUint32(4, true);
      const envelope = decode(encoded.subarray(8, 8 + envLen));
      expect(envelope).toEqual({ id: 'abc', params: [{ __cui_bin__: 0, l: T + 1024 }] });

      // Segment lies back-to-back after the envelope
      expect(encoded.byteLength).toBe(8 + envLen + T + 1024);
      expect(Buffer.from(encoded.subarray(8 + envLen))).toEqual(Buffer.from(bin));
    });

    it('lays out multiple segments in traversal order', () => {
      const a = bytes(T, 1);
      const b = bytes(T + 500, 2);
      const c = bytes(T + 4096, 3);
      const message = { first: a, nested: { deep: [b] }, last: c };
      const encoded = encodeMessage(message);

      const envLen = new DataView(encoded.buffer, encoded.byteOffset).getUint32(4, true);
      const envelope = decode<any>(encoded.subarray(8, 8 + envLen));
      expect(envelope.first).toEqual({ __cui_bin__: 0, l: T });
      expect(envelope.nested.deep[0]).toEqual({ __cui_bin__: 1, l: T + 500 });
      expect(envelope.last).toEqual({ __cui_bin__: 2, l: T + 4096 });

      const base = 8 + envLen;
      expect(encoded[base]).toBe(1);
      expect(encoded[base + T]).toBe(2);
      expect(encoded[base + T + (T + 500)]).toBe(3);
      expect(encoded.byteLength).toBe(base + T + (T + 500) + (T + 4096));
    });

    it('does not mutate the input message during extraction', () => {
      const bin = bytes(T);
      const message = { params: [bin, { inner: bin }] };
      encodeMessage(message);
      expect(message.params[0]).toBe(bin);
      expect((message.params[1] as any).inner).toBe(bin);
    });
  });

  describe('roundtrip', () => {
    it('round-trips a binary inside an args array', () => {
      const bin = bytes(T + 4096, 42);
      const message = { id: 'x1', method: 'call', params: ['snapshot', bin, { quality: 80 }] };
      const result = messageRoundtrip(message);
      expect(result.id).toBe('x1');
      expect(result.params[0]).toBe('snapshot');
      expect(Buffer.from(result.params[1] as Uint8Array)).toEqual(Buffer.from(bin));
      expect(result.params[2]).toEqual({ quality: 80 });
    });

    it('round-trips a binary inside a nested object', () => {
      const bin = bytes(T + 10_000, 5);
      const message = { result: { frame: { data: bin, pts: 1234 }, ok: true } };
      const result = messageRoundtrip(message);
      expect(result.result.frame.pts).toBe(1234);
      expect(result.result.ok).toBe(true);
      expect(Buffer.from(result.result.frame.data)).toEqual(Buffer.from(bin));
    });

    it('round-trips multiple binaries and keeps their contents distinct', () => {
      const a = bytes(T, 1);      const b = bytes(T + 2048, 2);
      const c = bytes(T + 1, 3);
      const message = { params: [a, { b }, [c]] };
      const result = messageRoundtrip(message);
      expect(Buffer.from(result.params[0] as Uint8Array)).toEqual(Buffer.from(a));
      expect(Buffer.from((result.params[1] as any).b)).toEqual(Buffer.from(b));
      expect(Buffer.from((result.params[2] as any)[0])).toEqual(Buffer.from(c));
    });

    it('keeps a 1023-byte binary inline (below threshold)', () => {
      const bin = bytes(BINARY_EXTRACT_THRESHOLD - 1);
      const message = { params: [bin] };
      const encoded = encodeMessage(message);
      expect(Buffer.from(encoded)).toEqual(Buffer.from(encode(message)));
      expect(Buffer.from(messageRoundtrip(message).params[0] as Uint8Array)).toEqual(Buffer.from(bin));
    });

    it('extracts a 1024-byte binary (at threshold)', () => {
      const bin = bytes(BINARY_EXTRACT_THRESHOLD);
      const message = { params: [bin] };
      const encoded = encodeMessage(message);
      expect(Array.from(encoded.subarray(0, 4))).toEqual([0x43, 0x55, 0x49, 0x42]);
      expect(Buffer.from(messageRoundtrip(message).params[0] as Uint8Array)).toEqual(Buffer.from(bin));
    });

    it('round-trips a Buffer as an extracted segment', () => {
      const buf = Buffer.alloc(T + 5000, 0xab);
      const result = messageRoundtrip({ params: [buf] });
      expect(Buffer.from(result.params[0] as Uint8Array)).toEqual(buf);
    });

    it('round-trips an ArrayBuffer as an extracted segment', () => {
      const ab = new ArrayBuffer(T + 2000);
      new Uint8Array(ab).fill(0xcd);
      const result = messageRoundtrip<any>({ params: [ab] });
      expect(Buffer.from(result.params[0] as Uint8Array)).toEqual(Buffer.from(new Uint8Array(ab)));
    });

    it('round-trips a root-level binary', () => {
      const bin = bytes(T + 3000, 6);
      const result = messageRoundtrip(bin);
      expect(Buffer.from(result)).toEqual(Buffer.from(bin));
    });
  });

  describe('zero-copy decode', () => {
    it('returns subarray views into the received buffer', () => {
      const bin = bytes(T + 2048, 0x11);
      const encoded = encodeMessage({ params: [bin] });
      const result = decodeMessage<any>(encoded);
      const view: Uint8Array = result.params[0];
      expect(view.buffer).toBe(encoded.buffer);
      // Mutating the receive buffer is visible through the view (no copy).
      encoded[encoded.byteLength - 1] = 0x99;
      expect(view[view.byteLength - 1]).toBe(0x99);
    });
  });

  describe('placeholder collision safety', () => {
    it('does not replace a user map with __cui_bin__ but without "l"', () => {
      const message = { params: [{ __cui_bin__: 0 }, bytes(T)] };
      const result = messageRoundtrip<any>(message);
      expect(result.params[0]).toEqual({ __cui_bin__: 0 });
    });

    it('does not replace a user map with a non-integer "l"', () => {
      const message = { params: [{ __cui_bin__: 0, l: "nope" }, bytes(T)] };
      const result = messageRoundtrip<any>(message);
      expect(result.params[0]).toEqual({ __cui_bin__: 0, l: 'nope' });
    });

    it('does not replace a user map with extra keys next to __cui_bin__ and "l"', () => {
      const message = { params: [{ __cui_bin__: 0, l: 1, extra: true }, bytes(T)] };
      const result = messageRoundtrip<any>(message);
      expect(result.params[0]).toEqual({ __cui_bin__: 0, l: 1, extra: true });
    });

    it('leaves placeholder-shaped user maps untouched in binary-free messages', () => {
      // Without extracted binaries there is no CUIB frame, hence no
      // placeholder substitution at all.
      const message = { params: [{ __cui_bin__: 0, l: 123 }] };
      const result = messageRoundtrip<any>(message);
      expect(result.params[0]).toEqual({ __cui_bin__: 0, l: 123 });
    });
  });

  describe('out-of-order placeholders', () => {
    it('restores segments whose placeholders appear out of traversal order', () => {
      // A port whose map serialization order differs from its extraction
      // order (e.g. Go map iteration) may emit placeholder 1 before 0 in
      // the envelope. Segments still lie back-to-back in index order.
      const seg0 = bytes(T, 0xaa);
      const seg1 = bytes(T + 2048, 0xbb);
      const envelope = encode({ b: { __cui_bin__: 1, l: T + 2048 }, a: { __cui_bin__: 0, l: T } });
      const frame = new Uint8Array(8 + envelope.byteLength + T + (T + 2048));
      frame.set([0x43, 0x55, 0x49, 0x42], 0);
      new DataView(frame.buffer).setUint32(4, envelope.byteLength, true);
      frame.set(envelope, 8);
      frame.set(seg0, 8 + envelope.byteLength);
      frame.set(seg1, 8 + envelope.byteLength + T);

      const result = decodeMessage<any>(frame);
      expect(Buffer.from(result.a)).toEqual(Buffer.from(seg0));
      expect(Buffer.from(result.b)).toEqual(Buffer.from(seg1));
    });
  });

  describe('frame validation', () => {
    it('rejects a frame whose envelope length exceeds the payload', () => {
      const encoded = encodeMessage({ params: [bytes(T)] });
      new DataView(encoded.buffer, encoded.byteOffset).setUint32(4, encoded.byteLength, true);
      expect(() => decodeMessage(encoded)).toThrow(/envelope length/);
    });

    it('rejects a truncated frame (segment exceeds payload)', () => {
      const encoded = encodeMessage({ params: [bytes(T)] });
      expect(() => decodeMessage(encoded.subarray(0, encoded.byteLength - 10))).toThrow(/Invalid CUIB frame/);
    });

    it('rejects trailing bytes after the last segment', () => {
      const encoded = encodeMessage({ params: [bytes(T)] });
      const padded = new Uint8Array(encoded.byteLength + 4);
      padded.set(encoded, 0);
      expect(() => decodeMessage(padded)).toThrow(/expected payload size/);
    });
  });
});
