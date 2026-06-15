import { describe, expect, it } from 'vitest';

import { decode, encode } from '../src/codec.js';

function roundtrip<T>(value: T): T {
  return decode<T>(encode(value));
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
