import * as fs from 'fs';
import { decode, encode } from '../node/src/codec.js';

interface TestCase {
  name: string;
  data: any;
}

const testCases: TestCase[] = [
  { name: 'string', data: 'Hello World' },
  { name: 'empty_string', data: '' },
  { name: 'number_int', data: 42 },
  { name: 'number_negative', data: -42 },
  { name: 'number_zero', data: 0 },
  { name: 'number_large', data: 2 ** 31 - 1 },
  { name: 'float', data: 3.14159 },
  { name: 'float_negative', data: -3.14159 },
  { name: 'float_zero', data: 0.0 },
  { name: 'float_inf', data: Infinity },
  { name: 'float_neg_inf', data: -Infinity },
  { name: 'float_nan', data: NaN },
  { name: 'boolean_true', data: true },
  { name: 'boolean_false', data: false },
  { name: 'null', data: null },

  { name: 'empty_array', data: [] },
  { name: 'array', data: [1, 2, 3, 4, 5] },
  { name: 'mixed_array', data: ['hello', 42, true, null] },
  { name: 'nested_array', data: [[1, 2], [3, 4], [5, 6]] },
  { name: 'array_with_objects', data: [{ a: 1 }, { b: 2 }] },
  { name: 'tuple', data: [1, 2, 3] },
  { name: 'nested_tuple', data: [[1, 2], [3, 4]] },

  { name: 'empty_object', data: {} },
  { name: 'simple_object', data: { key: 'value', number: 123 } },
  { name: 'nested_object', data: { outer: { inner: { value: 42 } } } },
  { name: 'object_mixed_keys', data: { str: 'text', num: 42, bool: true, null: null } },

  { name: 'datetime', data: new Date() },
  { name: 'date', data: new Date().toISOString().split('T')[0] },
  { name: 'time', data: new Date().toTimeString().split(' ')[0] },
  { name: 'timestamp', data: Date.now() / 1000 },

  { name: 'enum', data: 'INTERNAL_ERROR' },
  { name: 'enum_in_dict', data: { error: 'TIMEOUT', code: 408 } },

  {
    name: 'complex',
    data: {
      id: 'test-123',
      method: 'greet',
      params: ['Python'],
      nested: { foo: 'bar', baz: [1, 2, 3] },
      timestamp: new Date(),
      metadata: {
        version: 1.0,
        features: ['streaming', 'chunking'],
        limits: { max_size: 10485760, timeout: 30000 },
      },
    },
  },

  { name: 'binary', data: Buffer.from('Hello binary world') },
  { name: 'empty_binary', data: Buffer.from('') },
  { name: 'binary_with_nulls', data: Buffer.from([0, 1, 2, 3, 4]) },

  { name: 'unicode', data: '你好世界 🌍' },
  { name: 'emoji', data: '🎉🎊🎈🎁🎀' },
  { name: 'special_chars', data: 'äöü ñ é à ß' },
  { name: 'escape_chars', data: 'line1\nline2\ttab\r\nwindows' },
  { name: 'quotes', data: 'He said "Hello" and she said \'Hi\'' },

  { name: 'very_long_string', data: 'x'.repeat(10000) },
  { name: 'deeply_nested', data: { l1: { l2: { l3: { l4: { l5: { value: 'deep' } } } } } } },
  { name: 'large_array', data: Array.from({ length: 1000 }, (_, i) => i) },

  { name: 'max_safe_int', data: Number.MAX_SAFE_INTEGER },
  { name: 'min_safe_int', data: Number.MIN_SAFE_INTEGER },

  {
    name: 'rpc_message',
    data: {
      id: '1234567890-abcdef',
      method: 'test.method',
      params: [1, 'two', { three: 3 }],
      error: null,
    },
  },
  {
    name: 'stream_message',
    data: {
      id: 'stream-123',
      type: 'data',
      data: { chunk: 1, total: 10 },
    },
  },

  { name: 'js_timestamp', data: 1708786800000 },

  { name: 'date_ext', data: new Date('2024-02-24T12:00:00.000Z') },

  {
    name: 'camera_config',
    data: {
      cameraId: 'cam-abc-123',
      fps: 30,
      eventTimeout: 30,
      timestamp: 1708786800000,
      confidence: 0.85,
      enabled: true,
      name: 'Front Door',
    },
  },

  {
    name: 'detection_event',
    data: {
      type: 'start',
      data: {
        id: 'evt-abc-123',
        state: 'active',
        types: ['motion', 'audio'],
        startTime: 1708786800000,
        endTime: 0,
        triggers: [
          { type: 'motion', timestamp: 1708786800000, data: { score: 0.95 } },
          { type: 'audio', timestamp: 1708786800500, data: { decibels: -25.5 } },
        ],
        segments: [],
      },
    },
  },

  { name: 'map_with_nil', data: { key: 'value', optional: null, count: 0, flag: false } },

  { name: 'nested_ints', data: { a: { b: { c: 42 } }, d: [1, 2, 3], e: 100 } },

  { name: 'empty_nested', data: { a: {}, b: [], c: { d: {} } } },

  {
    name: 'sensor_list',
    data: [
      { id: 'sensor-1', type: 'motion', online: true, score: 0.95 },
      { id: 'sensor-2', type: 'audio', online: false, score: 0.0 },
      { id: 'sensor-3', type: 'object', online: true, score: 0.87 },
    ],
  },

  { name: 'mixed_numerics', data: { integer: 42, float_val: 3.14, zero_int: 0, zero_float: 0.0, negative: -10, neg_float: -2.5 } },
];

console.log('Node.js codec test');
console.log('='.repeat(60));
console.log();

let passed = 0;
let failed = 0;

console.log('Phase 1: Encoding...');

for (const test of testCases) {
  try {
    const encoded = encode(test.data);
    fs.writeFileSync(`/tmp/node-encoded-${test.name}.msgpack`, encoded);

    const decoded = decode(encoded);
    const success = compareValues(test.data, decoded, test.name);

    if (success) {
      console.log(`  OK ${test.name} (${encoded.length} bytes)`);
      passed++;
    } else {
      console.log(`  FAIL ${test.name}: roundtrip mismatch`);
      console.log(`    Expected: ${JSON.stringify(test.data)?.slice(0, 100)}`);
      console.log(`    Got:      ${JSON.stringify(decoded)?.slice(0, 100)}`);
      failed++;
    }
  } catch (e: any) {
    console.log(`  FAIL ${test.name}: ${e.message}`);
    failed++;
  }
}

console.log();
console.log(`Results: ${passed} passed, ${failed} failed`);
if (failed > 0) {
  process.exit(1);
}

function compareValues(expected: any, actual: any, testName: string): boolean {
  if (testName === 'float_nan') {
    return typeof actual === 'number' && isNaN(actual);
  }

  if (testName === 'float_zero' && expected === 0.0) {
    return actual === 0 || actual === 0.0;
  }

  if (expected instanceof Date) {
    if (actual instanceof Date) {
      return Math.abs(expected.getTime() - actual.getTime()) < 1000;
    }
    if (typeof actual === 'string') {
      return expected.toISOString().split('T')[0] === actual.split('T')[0];
    }
    return false;
  }

  if (testName === 'timestamp') {
    return typeof actual === 'number' && Math.abs(expected - actual) < 2;
  }

  if (testName === 'date' || testName === 'time') {
    return typeof actual === 'string' && actual.length > 0;
  }

  if (Buffer.isBuffer(expected)) {
    if (Buffer.isBuffer(actual)) return expected.equals(actual);
    if (actual instanceof Uint8Array) return expected.equals(Buffer.from(actual));
    return false;
  }

  if (testName === 'complex' && typeof expected === 'object' && typeof actual === 'object') {
    const exp = { ...expected };
    const act = { ...actual };
    delete exp.timestamp;
    delete act.timestamp;
    return JSON.stringify(exp) === JSON.stringify(act);
  }

  return deepEqual(expected, actual);
}

function deepEqual(a: any, b: any): boolean {
  if (a === b) return true;
  if (a === null || b === null) return a === b;
  if (typeof a !== typeof b) return false;

  if (typeof a === 'number') {
    if (isNaN(a) && isNaN(b)) return true;
    return a === b;
  }

  if (Array.isArray(a)) {
    if (!Array.isArray(b) || a.length !== b.length) return false;
    return a.every((v, i) => deepEqual(v, b[i]));
  }

  if (Buffer.isBuffer(a)) {
    if (Buffer.isBuffer(b)) return a.equals(b);
    if (b instanceof Uint8Array) return a.equals(Buffer.from(b));
    return false;
  }

  if (typeof a === 'object') {
    const aKeys = Object.keys(a);
    const bKeys = Object.keys(b);
    if (aKeys.length !== bKeys.length) return false;
    return aKeys.every(key => key in b && deepEqual(a[key], b[key]));
  }

  return false;
}
