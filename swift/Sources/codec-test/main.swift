import Foundation

import CameraUIRPC

struct TestCase {
  let name: String
  let data: Any?
}

let now = Date()
let isoDay = ISO8601DateFormatter().string(from: now).components(separatedBy: "T")[0]
let clock = DateFormatter()
clock.dateFormat = "HH:mm:ss"

let testCases: [TestCase] = [
  TestCase(name: "string", data: "Hello World"),
  TestCase(name: "empty_string", data: ""),
  TestCase(name: "number_int", data: 42),
  TestCase(name: "number_negative", data: -42),
  TestCase(name: "number_zero", data: 0),
  TestCase(name: "number_large", data: 2_147_483_647),
  TestCase(name: "float", data: 3.14159),
  TestCase(name: "float_negative", data: -3.14159),
  TestCase(name: "float_zero", data: 0.0),
  TestCase(name: "float_inf", data: Double.infinity),
  TestCase(name: "float_neg_inf", data: -Double.infinity),
  TestCase(name: "float_nan", data: Double.nan),
  TestCase(name: "boolean_true", data: true),
  TestCase(name: "boolean_false", data: false),
  TestCase(name: "null", data: nil),

  TestCase(name: "empty_array", data: [Any?]()),
  TestCase(name: "array", data: [1, 2, 3, 4, 5]),
  TestCase(name: "mixed_array", data: ["hello", 42, true, nil] as [Any?]),
  TestCase(name: "nested_array", data: [[1, 2], [3, 4], [5, 6]]),
  TestCase(name: "array_with_objects", data: [["a": 1], ["b": 2]]),
  TestCase(name: "tuple", data: [1, 2, 3]),
  TestCase(name: "nested_tuple", data: [[1, 2], [3, 4]]),

  TestCase(name: "empty_object", data: [String: Any?]()),
  TestCase(name: "simple_object", data: ["key": "value", "number": 123] as [String: Any?]),
  TestCase(name: "nested_object", data: ["outer": ["inner": ["value": 42]]]),
  TestCase(name: "object_mixed_keys", data: ["str": "text", "num": 42, "bool": true, "null": nil] as [String: Any?]),

  TestCase(name: "datetime", data: now),
  TestCase(name: "date", data: isoDay),
  TestCase(name: "time", data: clock.string(from: now)),
  TestCase(name: "timestamp", data: now.timeIntervalSince1970),

  TestCase(name: "enum", data: "INTERNAL_ERROR"),
  TestCase(name: "enum_in_dict", data: ["error": "TIMEOUT", "code": 408] as [String: Any?]),

  TestCase(
    name: "complex",
    data: [
      "id": "test-123",
      "method": "greet",
      "params": ["Python"],
      "nested": ["foo": "bar", "baz": [1, 2, 3]] as [String: Any?],
      "timestamp": now,
      "metadata": [
        "version": 1.0,
        "features": ["streaming", "chunking"],
        "limits": ["max_size": 10_485_760, "timeout": 30000] as [String: Any?],
      ] as [String: Any?],
    ] as [String: Any?]
  ),

  TestCase(name: "binary", data: Data("Hello binary world".utf8)),
  TestCase(name: "empty_binary", data: Data()),
  TestCase(name: "binary_with_nulls", data: Data([0, 1, 2, 3, 4])),

  TestCase(name: "unicode", data: "你好世界 🌍"),
  TestCase(name: "emoji", data: "🎉🎊🎈🎁🎀"),
  TestCase(name: "special_chars", data: "äöü ñ é à ß"),
  TestCase(name: "escape_chars", data: "line1\nline2\ttab\r\nwindows"),
  TestCase(name: "quotes", data: "He said \"Hello\" and she said 'Hi'"),

  TestCase(name: "very_long_string", data: String(repeating: "x", count: 10000)),
  TestCase(name: "deeply_nested", data: ["l1": ["l2": ["l3": ["l4": ["l5": ["value": "deep"]]]]]]),
  TestCase(name: "large_array", data: Array(0..<1000)),

  TestCase(name: "max_safe_int", data: 9_007_199_254_740_991),
  TestCase(name: "min_safe_int", data: -9_007_199_254_740_991),

  TestCase(
    name: "rpc_message",
    data: [
      "id": "1234567890-abcdef",
      "method": "test.method",
      "params": [1, "two", ["three": 3]] as [Any?],
      "error": nil,
    ] as [String: Any?]
  ),
  TestCase(
    name: "stream_message",
    data: ["id": "stream-123", "type": "data", "data": ["chunk": 1, "total": 10] as [String: Any?]] as [String: Any?]
  ),

  TestCase(name: "js_timestamp", data: 1_708_786_800_000),
  TestCase(name: "date_ext", data: Date(timeIntervalSince1970: 1_708_776_000)),

  TestCase(
    name: "camera_config",
    data: [
      "cameraId": "cam-abc-123",
      "fps": 30,
      "eventTimeout": 30,
      "timestamp": 1_708_786_800_000,
      "confidence": 0.85,
      "enabled": true,
      "name": "Front Door",
    ] as [String: Any?]
  ),
  TestCase(
    name: "detection_event",
    data: [
      "type": "start",
      "data": [
        "id": "evt-abc-123",
        "state": "active",
        "types": ["motion", "audio"],
        "startTime": 1_708_786_800_000,
        "endTime": 0,
        "triggers": [
          ["type": "motion", "timestamp": 1_708_786_800_000, "data": ["score": 0.95] as [String: Any?]] as [String: Any?],
          ["type": "audio", "timestamp": 1_708_786_800_500, "data": ["decibels": -25.5] as [String: Any?]] as [String: Any?],
        ] as [Any?],
        "segments": [Any?](),
      ] as [String: Any?],
    ] as [String: Any?]
  ),

  TestCase(name: "map_with_nil", data: ["key": "value", "optional": nil, "count": 0, "flag": false] as [String: Any?]),
  TestCase(name: "nested_ints", data: ["a": ["b": ["c": 42]], "d": [1, 2, 3], "e": 100] as [String: Any?]),
  TestCase(name: "empty_nested", data: ["a": [String: Any?](), "b": [Any?](), "c": ["d": [String: Any?]()] as [String: Any?]] as [String: Any?]),

  TestCase(
    name: "sensor_list",
    data: [
      ["id": "sensor-1", "type": "motion", "online": true, "score": 0.95] as [String: Any?],
      ["id": "sensor-2", "type": "audio", "online": false, "score": 0.0] as [String: Any?],
      ["id": "sensor-3", "type": "object", "online": true, "score": 0.87] as [String: Any?],
    ] as [Any?]
  ),
  TestCase(
    name: "mixed_numerics",
    data: ["integer": 42, "float_val": 3.14, "zero_int": 0, "zero_float": 0.0, "negative": -10, "neg_float": -2.5] as [String: Any?]
  ),
]

func numeric(_ value: Any?) -> Double? {
  if value is Bool { return nil }
  if let int = value as? Int { return Double(int) }
  if let uint = value as? UInt64 { return Double(uint) }
  if let double = value as? Double { return double }
  return nil
}

func deepEqual(_ a: Any?, _ b: Any?) -> Bool {
  switch (a, b) {
  case (nil, nil):
    return true
  case (let x as Bool, let y as Bool):
    return x == y
  case (let x as String, let y as String):
    return x == y
  case (let x as Data, let y as Data):
    return x == y
  case (let x as Date, let y as Date):
    return abs(x.timeIntervalSince(y)) < 1
  case (let x as [Any?], let y as [Any?]):
    return x.count == y.count && zip(x, y).allSatisfy { deepEqual($0, $1) }
  case (let x as [String: Any?], let y as [String: Any?]):
    guard x.count == y.count else { return false }
    return x.allSatisfy { key, value in
      guard let other = y[key] else { return false }
      return deepEqual(value, other)
    }
  default:
    if let x = numeric(a), let y = numeric(b) {
      if x.isNaN, y.isNaN { return true }
      return x == y
    }
    return false
  }
}

func typeCheck(_ name: String, _ actual: Any?) -> Bool {
  switch name {
  case "datetime":
    if actual is Date { return true }
    if let string = actual as? String { return !string.isEmpty }
    return false
  case "date", "time":
    if let string = actual as? String { return !string.isEmpty }
    return false
  case "timestamp":
    return numeric(actual) != nil
  case "date_ext":
    return actual is Date
  case "complex":
    guard let map = actual as? [String: Any?] else { return false }
    return map["id"] as? String == "test-123" && map["method"] as? String == "greet"
  case "large_array":
    return (actual as? [Any?])?.count == 1000
  case "mixed_numerics":
    guard let map = actual as? [String: Any?] else { return false }
    return numeric(map["float_val"] ?? nil) == 3.14 && numeric(map["integer"] ?? nil) == 42
  default:
    return actual != nil
  }
}

func compare(_ name: String, expected: Any?, actual: Any?, typeChecks: Set<String>) -> Bool {
  if name == "float_nan" {
    return numeric(actual)?.isNaN == true
  }
  if name == "float_zero" {
    return numeric(actual) == 0
  }
  if typeChecks.contains(name) {
    return typeCheck(name, actual)
  }
  if let date = expected as? Date {
    if let actualDate = actual as? Date { return abs(date.timeIntervalSince(actualDate)) < 1 }
    if let string = actual as? String { return !string.isEmpty }
    return false
  }
  if let binary = expected as? Data {
    if let actualBinary = actual as? Data { return binary == actualBinary }
    return binary.isEmpty && actual == nil
  }
  return deepEqual(expected, actual)
}

let selfTypeChecks: Set<String> = []
let crossTypeChecks: Set<String> = ["datetime", "date", "time", "timestamp", "date_ext", "complex", "large_array", "mixed_numerics"]

func runEncodePhase() -> Bool {
  print("Swift codec test")
  print(String(repeating: "=", count: 60))
  print("")
  print("Phase 1: Encoding...")

  var passed = 0
  var failed = 0

  for test in testCases {
    do {
      let encoded = try RPCCodec.encodeMessage(test.data)
      try encoded.write(to: URL(fileURLWithPath: "/tmp/swift-encoded-\(test.name).msgpack"))

      let decoded = try RPCCodec.decodeMessage(encoded)
      if compare(test.name, expected: test.data, actual: decoded, typeChecks: selfTypeChecks) {
        print("  OK \(test.name) (\(encoded.count) bytes)")
        passed += 1
      } else {
        print("  FAIL \(test.name): roundtrip mismatch")
        failed += 1
      }
    } catch {
      print("  FAIL \(test.name): \(error)")
      failed += 1
    }
  }

  print("")
  print("Results: \(passed) passed, \(failed) failed")
  return failed == 0
}

func runCrossPhase() -> Bool {
  var totalFailed = 0

  for (source, prefix) in [("Python", "py"), ("Node.js", "node"), ("Go", "go")] {
    print("Swift cross-language decode: \(source) data")
    print(String(repeating: "=", count: 60))

    var passed = 0
    var failed = 0

    for test in testCases {
      let path = "/tmp/\(prefix)-encoded-\(test.name).msgpack"
      guard FileManager.default.fileExists(atPath: path) else {
        print("  SKIP \(test.name): file not found")
        continue
      }
      do {
        let encoded = try Data(contentsOf: URL(fileURLWithPath: path))
        let decoded = try RPCCodec.decodeMessage(encoded)
        if compare(test.name, expected: test.data, actual: decoded, typeChecks: crossTypeChecks) {
          print("  OK \(test.name)")
          passed += 1
        } else {
          print("  FAIL \(test.name)")
          failed += 1
        }
      } catch {
        print("  FAIL \(test.name): \(error)")
        failed += 1
      }
    }

    print("Results: \(passed) passed, \(failed) failed")
    print("")
    totalFailed += failed
  }

  return totalFailed == 0
}

let mode = CommandLine.arguments.count > 1 ? CommandLine.arguments[1] : "encode"
let success: Bool
switch mode {
case "encode":
  success = runEncodePhase()
case "cross":
  success = runCrossPhase()
default:
  print("usage: codec-test [encode|cross]")
  success = false
}
exit(success ? 0 : 1)
