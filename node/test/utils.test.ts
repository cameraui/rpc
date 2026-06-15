import { describe, expect, it } from 'vitest';

import { getCallbacksOneway, isPromise, isRpcCallbacks, rpcCallbacks, sleep } from '../src/utils.js';

describe('rpcCallbacks', () => {
  it('returns the same object reference', () => {
    const obj = { onItem: () => {} };
    expect(rpcCallbacks(obj)).toBe(obj);
  });

  it('marks the object so isRpcCallbacks detects it', () => {
    const cbs = rpcCallbacks({ onItem: () => {}, onEnd: () => {} });
    expect(isRpcCallbacks(cbs)).toBe(true);
  });

  it('keeps the marker non-enumerable', () => {
    const cbs = rpcCallbacks({ onItem: () => {} });
    expect(Object.keys(cbs)).toEqual(['onItem']);
  });

  it('defaults the oneway list to all method names', () => {
    const cbs = rpcCallbacks({ onItem: () => {}, onEnd: () => {} });
    expect(getCallbacksOneway(cbs).sort()).toEqual(['onEnd', 'onItem']);
  });

  it('honours an explicit oneway list', () => {
    const cbs = rpcCallbacks({ onItem: () => {}, onEnd: () => {} }, { oneway: ['onItem'] });
    expect(getCallbacksOneway(cbs)).toEqual(['onItem']);
  });
});

describe('isRpcCallbacks', () => {
  it('returns false for plain objects', () => {
    expect(isRpcCallbacks({ onItem: () => {} })).toBe(false);
  });

  it('returns false for non-object values', () => {
    expect(isRpcCallbacks(null)).toBe(false);
    expect(isRpcCallbacks(undefined)).toBe(false);
    expect(isRpcCallbacks(42)).toBe(false);
  });
});

describe('getCallbacksOneway', () => {
  it('returns an empty array for an unmarked value', () => {
    expect(getCallbacksOneway({})).toEqual([]);
    expect(getCallbacksOneway(null)).toEqual([]);
  });
});

describe('isPromise', () => {
  it('returns true for a native Promise', () => {
    expect(isPromise(Promise.resolve(1))).toBe(true);
  });

  it('returns true for a thenable with then and catch', () => {
    expect(isPromise({ then: () => {}, catch: () => {} })).toBe(true);
  });

  it('returns false for non-thenables', () => {
    expect(isPromise(null)).toBe(false);
    expect(isPromise(undefined)).toBe(false);
    expect(isPromise(42)).toBe(false);
    expect(isPromise({ then: 1 })).toBe(false);
    expect(isPromise(() => {})).toBe(false);
  });
});

describe('sleep', () => {
  it('resolves after at least the given delay', async () => {
    const start = Date.now();
    await sleep(20);
    expect(Date.now() - start).toBeGreaterThanOrEqual(15);
  });
});
