import * as fs from 'fs';
import { decode } from '../node/src/codec.js';

const expectedData: Record<string, any> = {
  string: 'Hello World',
  empty_string: '',
  number_int: 42,
  number_negative: -42,
  number_zero: 0,
  number_large: 2 ** 31 - 1,
  float: 3.14159,
  float_negative: -3.14159,
  float_zero: 0.0,
  float_inf: Infinity,
  float_neg_inf: -Infinity,
  float_nan: NaN,
  boolean_true: true,
  boolean_false: false,
  null: null,

  empty_array: [],
  array: [1, 2, 3, 4, 5],
  mixed_array: ['hello', 42, true, null],
  nested_array: [[1, 2], [3, 4], [5, 6]],
  array_with_objects: [{ a: 1 }, { b: 2 }],
  tuple: [1, 2, 3],
  nested_tuple: [[1, 2], [3, 4]],

  empty_object: {},
  simple_object: { key: 'value', number: 123 },
  nested_object: { outer: { inner: { value: 42 } } },
  object_mixed_keys: { str: 'text', num: 42, bool: true, null: null },

  datetime: '__type_check__',
  date: '__type_check__',
  time: '__type_check__',
  timestamp: '__type_check__',

  enum: 'INTERNAL_ERROR',
  enum_in_dict: { error: 'TIMEOUT', code: 408 },

  complex: '__type_check__',

  binary: Buffer.from('Hello binary world'),
  empty_binary: Buffer.from(''),
  binary_with_nulls: Buffer.from([0, 1, 2, 3, 4]),

  unicode: '你好世界 🌍',
  emoji: '🎉🎊🎈🎁🎀',
  special_chars: 'äöü ñ é à ß',
  escape_chars: 'line1\nline2\ttab\r\nwindows',
  quotes: 'He said "Hello" and she said \'Hi\'',

  very_long_string: 'x'.repeat(10000),
  deeply_nested: { l1: { l2: { l3: { l4: { l5: { value: 'deep' } } } } } },
  large_array: '__type_check__',

  max_safe_int: 9007199254740991,
  min_safe_int: -9007199254740991,

  rpc_message: {
    id: '1234567890-abcdef',
    method: 'test.method',
    params: [1, 'two', { three: 3 }],
    error: null,
  },
  stream_message: { id: 'stream-123', type: 'data', data: { chunk: 1, total: 10 } },

  js_timestamp: 1708786800000,
  date_ext: '__type_check__',
  camera_config: {
    cameraId: 'cam-abc-123',
    fps: 30,
    eventTimeout: 30,
    timestamp: 1708786800000,
    confidence: 0.85,
    enabled: true,
    name: 'Front Door',
  },
  detection_event: {
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
  map_with_nil: { key: 'value', optional: null, count: 0, flag: false },
  nested_ints: { a: { b: { c: 42 } }, d: [1, 2, 3], e: 100 },
  empty_nested: { a: {}, b: [], c: { d: {} } },
  sensor_list: [
    { id: 'sensor-1', type: 'motion', online: true, score: 0.95 },
    { id: 'sensor-2', type: 'audio', online: false, score: 0.0 },
    { id: 'sensor-3', type: 'object', online: true, score: 0.87 },
  ],
  mixed_numerics: '__type_check__',
};

function compareValues(expected: any, actual: any, testName: string): boolean {
  if (testName === 'float_nan') return typeof actual === 'number' && isNaN(actual);
  if (testName === 'float_zero') return actual === 0 || actual === 0.0;

  if (expected === '__type_check__') {
    return checkTypeOnly(testName, actual);
  }

  if (expected instanceof Date) {
    if (actual instanceof Date) return Math.abs(expected.getTime() - actual.getTime()) < 1000;
    if (typeof actual === 'string') return typeof actual === 'string' && actual.length > 0;
    return false;
  }

  if (Buffer.isBuffer(expected)) {
    if (Buffer.isBuffer(actual)) return expected.equals(actual);
    if (actual instanceof Uint8Array) return expected.equals(Buffer.from(actual));
    if (expected.length === 0 && actual === null) return true;
    return false;
  }

  // Default: deep comparison (JSON.stringify is key-order-sensitive, Go/Python may differ)
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

function checkTypeOnly(name: string, actual: any): boolean {
  switch (name) {
    case 'datetime':
      return (actual instanceof Date) || (typeof actual === 'string' && actual.length > 0);
    case 'date':
    case 'time':
      return typeof actual === 'string' && actual.length > 0;
    case 'timestamp':
      return typeof actual === 'number';
    case 'date_ext':
      return actual instanceof Date;
    case 'complex':
      if (typeof actual !== 'object' || actual === null) return false;
      const { timestamp: _, ...rest } = actual;
      return rest.id === 'test-123' && rest.method === 'greet';
    case 'large_array':
      return Array.isArray(actual) && actual.length === 1000;
    case 'mixed_numerics':
      return typeof actual === 'object' && actual.float_val === 3.14 && actual.integer === 42;
    default:
      return actual != null;
  }
}

function runCrossTest(sourceName: string, pathPattern: string): { passed: number; failed: number } {
  let passed = 0;
  let failed = 0;

  console.log(`Node.js cross-language decode: ${sourceName} data`);
  console.log('='.repeat(60));

  for (const [testName, expected] of Object.entries(expectedData)) {
    const filepath = pathPattern.replace('%s', testName);

    if (!fs.existsSync(filepath)) {
      console.log(`  SKIP ${testName}: file not found`);
      continue;
    }

    try {
      const encoded = fs.readFileSync(filepath);
      const decoded = decode(encoded);
      const success = compareValues(expected, decoded, testName);

      if (success) {
        console.log(`  OK ${testName}`);
        passed++;
      } else {
        console.log(`  FAIL ${testName}`);
        console.log(`    Expected: ${JSON.stringify(expected)?.slice(0, 80)}`);
        console.log(`    Got:      ${JSON.stringify(decoded)?.slice(0, 80)}`);
        failed++;
      }
    } catch (e: any) {
      console.log(`  FAIL ${testName}: ${e.message}`);
      failed++;
    }
  }

  return { passed, failed };
}

const results = [
  runCrossTest('Python', '/tmp/py-encoded-%s.msgpack'),
  runCrossTest('Go', '/tmp/go-encoded-%s.msgpack'),
];

console.log();
const totalPassed = results.reduce((sum, r) => sum + r.passed, 0);
const totalFailed = results.reduce((sum, r) => sum + r.failed, 0);
console.log(`Total: ${totalPassed} passed, ${totalFailed} failed`);

if (totalFailed > 0) {
  process.exit(1);
}
