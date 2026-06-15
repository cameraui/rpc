import { describe, expect, it } from 'vitest';

import { generateId } from '../src/utils.js';

describe('generateId', () => {
  it('returns a non-empty string', () => {
    const id = generateId();
    expect(typeof id).toBe('string');
    expect(id.length).toBeGreaterThan(0);
  });

  it('matches the timestamp-random format', () => {
    expect(generateId()).toMatch(/^\d+-[a-z0-9]+$/);
  });

  it('produces unique ids across many calls', () => {
    const count = 10000;
    const ids = new Set<string>();
    for (let i = 0; i < count; i++) {
      ids.add(generateId());
    }
    expect(ids.size).toBe(count);
  });

  it('encodes a leading timestamp that is close to now', () => {
    const before = Date.now();
    const ts = Number(generateId().split('-')[0]);
    const after = Date.now();
    expect(ts).toBeGreaterThanOrEqual(before);
    expect(ts).toBeLessThanOrEqual(after);
  });
});
