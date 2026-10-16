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
      const a = bytes(T, 1);
      const b = bytes(T + 2048, 2);
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
      const message = { params: [{ __cui_bin__: 0, l: 'nope' }, bytes(T)] };
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

  describe('binary walk', () => {
    it('reads each property once during extraction', () => {
      let reads = 0;
      const message = {
        get value() {
          reads++;
          return 7;
        },
      };
      encodeMessage(message);
      // one read by the walk, one by msgpack
      expect(reads).toBe(2);
    });

    it('ignores enumerable keys inherited from a polluted prototype', () => {
      const proto = Object.prototype as Record<string, unknown>;
      proto.polluted = bytes(T);
      try {
        const message = { id: 'a', params: [{ x: 1 }] };
        expect(Buffer.from(encodeMessage(message))).toEqual(Buffer.from(encode(message)));
      } finally {
        delete proto.polluted;
      }
    });

    it('assigns placeholder indices in Object.keys order, integer-like keys first', () => {
      const message = { b: bytes(T, 1), 1: bytes(T, 2), a: { deep: [0, bytes(T, 3)] } };
      const encoded = encodeMessage(message);
      const envLen = new DataView(encoded.buffer, encoded.byteOffset).getUint32(4, true);
      const envelope = decode<any>(encoded.subarray(8, 8 + envLen));
      expect(envelope[1].__cui_bin__).toBe(0);
      expect(envelope.b.__cui_bin__).toBe(1);
      expect(envelope.a.deep[1].__cui_bin__).toBe(2);
      const result = decodeMessage<any>(encoded);
      expect([result[1][0], result.b[0], result.a.deep[1][0]]).toEqual([2, 1, 3]);
    });

    it('walks null-prototype objects and skips primitive roots', () => {
      const message = Object.assign(Object.create(null), { data: bytes(T, 9) });
      expect(decodeMessage<any>(encodeMessage(message)).data[0]).toBe(9);
      for (const value of [1, 'x', null, true]) {
        expect(Buffer.from(encodeMessage(value))).toEqual(Buffer.from(encode(value)));
      }
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

describe('typed arrays other than Uint8Array', () => {
  // the bytes a value holds, little-endian on every platform this runs on
  function bytesOf(value: ArrayBufferView): Buffer {
    return Buffer.from(value.buffer, value.byteOffset, value.byteLength);
  }

  function views(byteLength: number): [string, ArrayBufferView][] {
    const source = new Uint8Array(byteLength + 16);
    for (let i = 0; i < source.length; i++) source[i] = (i * 37 + 11) & 0xff;
    // offset 8 keeps every element type aligned and shows that only the view's own bytes go out
    const buffer = source.buffer;
    return [
      ['Float32Array', new Float32Array(buffer, 8, byteLength / 4)],
      ['Float64Array', new Float64Array(buffer, 8, byteLength / 8)],
      ['Int16Array', new Int16Array(buffer, 8, byteLength / 2)],
      ['Uint8ClampedArray', new Uint8ClampedArray(buffer, 8, byteLength)],
      ['DataView', new DataView(buffer, 8, byteLength)],
    ];
  }

  for (const size of [8, 1024, 16376, 16384, 32768]) {
    it(`arrives as its exact bytes at ${size} bytes`, () => {
      for (const [name, view] of views(size)) {
        // a message before may leave other bytes in msgpackr's shared buffer
        encodeMessage({ secret: 'MARKER'.repeat(4000) });
        const wire = encodeMessage({ name, view, nested: [{ view }] });
        const decoded = decodeMessage(wire);

        expect(decoded.view, name).toBeInstanceOf(Uint8Array);
        expect(bytesOf(decoded.view).equals(bytesOf(view)), name).toBe(true);
        expect(bytesOf(decoded.nested[0].view).equals(bytesOf(view)), name).toBe(true);
        expect(Buffer.from(wire).includes('MARKER'), name).toBe(false);
      }
    });
  }

  it('encodes a typed array on its own as its bytes', () => {
    const value = new Int16Array([1, -2, 3]);
    expect(bytesOf(decodeMessage(encodeMessage(value))).equals(bytesOf(value))).toBe(true);
  });

  it('keeps messages without such views byte-identical to encode()', () => {
    const message = { id: 'a', params: [1, 'x', { deep: [true, null] }], blob: new Uint8Array([1, 2, 3]) };
    expect(Buffer.from(encodeMessage(message)).equals(Buffer.from(encode(message)))).toBe(true);
  });
});

describe('binaries after decodeMessage', () => {
  it('are Buffer views whether the payload came as a Buffer or a plain Uint8Array', () => {
    const small = { blob: Buffer.from([1, 2, 3]), nested: [{ blob: new Uint8Array([4, 5]) }] };
    const framed = { frame: new Uint8Array(BINARY_EXTRACT_THRESHOLD).fill(7) };

    for (const message of [small, framed]) {
      const wire = encodeMessage(message);
      const plain = new Uint8Array(wire.byteLength + 4).subarray(2, 2 + wire.byteLength);
      plain.set(wire);
      for (const data of [wire, plain]) {
        const decoded = decodeMessage(data);
        const binaries = 'frame' in decoded ? [decoded.frame] : [decoded.blob, decoded.nested[0].blob];
        for (const binary of binaries) expect(Buffer.isBuffer(binary)).toBe(true);
      }
    }
  });

  it('views the payload instead of copying it', () => {
    const wire = encodeMessage({ frame: new Uint8Array(BINARY_EXTRACT_THRESHOLD).fill(1) });
    const plain = new Uint8Array(wire);
    const decoded = decodeMessage(plain);
    expect(decoded.frame.buffer).toBe(plain.buffer);
  });
});
