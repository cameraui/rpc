import Foundation

public enum RPCCodecError: Error, CustomStringConvertible {
  case invalidFrame(String)

  public var description: String {
    switch self {
    case .invalidFrame(let reason): return "Invalid CUIB frame: \(reason)"
    }
  }
}

public enum RPCCodec {
  public static let binaryExtractThreshold = 16_384

  static let magic: [UInt8] = [0x43, 0x55, 0x49, 0x42]
  static let headerSize = 8
  static let placeholderKey = "__cui_bin__"

  public static func encode(_ value: Any?) throws -> Data {
    try MessagePack.encode(value)
  }

  public static func decode(_ data: Data) throws -> Any? {
    try MessagePack.decode(data)
  }

  public static func encodeMessage(_ value: Any?) throws -> Data {
    var segments: [Data] = []
    let transformed = extractBinaries(value, into: &segments)

    if segments.isEmpty {
      return try MessagePack.encode(value)
    }

    let envelope = try MessagePack.encode(transformed)
    let envLen = envelope.count

    var out = Data(capacity: headerSize + envLen + segments.reduce(0) { $0 + $1.count })
    out.append(contentsOf: magic)
    out.append(UInt8(envLen & 0xff))
    out.append(UInt8((envLen >> 8) & 0xff))
    out.append(UInt8((envLen >> 16) & 0xff))
    out.append(UInt8((envLen >> 24) & 0xff))
    out.append(envelope)
    for segment in segments {
      out.append(segment)
    }
    return out
  }

  public static func decodeMessage(_ data: Data) throws -> Any? {
    let base = data.startIndex
    guard
      data.count >= headerSize,
      data[base] == magic[0], data[base + 1] == magic[1],
      data[base + 2] == magic[2], data[base + 3] == magic[3]
    else {
      return try MessagePack.decode(data)
    }

    let envLen = Int(data[base + 4]) | Int(data[base + 5]) << 8 | Int(data[base + 6]) << 16 | Int(data[base + 7]) << 24
    let segmentBase = headerSize + envLen
    guard segmentBase <= data.count else {
      throw RPCCodecError.invalidFrame("envelope length \(envLen) exceeds payload size \(data.count)")
    }

    let envelope = try MessagePack.decode(Data(data[(base + headerSize)..<(base + segmentBase)]))

    var lengths: [Int: Int] = [:]
    collectSegmentLengths(envelope, into: &lengths)
    let segmentCount = lengths.isEmpty ? 0 : (lengths.keys.max()! + 1)

    // segments follow in index order, whatever order the placeholders have in the envelope
    var segments: [Data] = []
    segments.reserveCapacity(segmentCount)
    var offset = base + segmentBase
    for index in 0..<segmentCount {
      guard let length = lengths[index] else {
        throw RPCCodecError.invalidFrame("missing placeholder for segment \(index)")
      }
      guard offset + length <= data.endIndex else {
        throw RPCCodecError.invalidFrame("segment \(index) exceeds payload size \(data.count)")
      }
      segments.append(Data(data[offset..<(offset + length)]))
      offset += length
    }

    guard offset == data.endIndex else {
      throw RPCCodecError.invalidFrame("expected payload size \(offset - base), got \(data.count)")
    }

    return restoreBinaries(envelope, segments: segments)
  }

  private static func extractBinaries(_ value: Any?, into segments: inout [Data]) -> Any? {
    if let binary = value as? Data, binary.count >= binaryExtractThreshold {
      let index = segments.count
      segments.append(binary)
      return [placeholderKey: index, "l": binary.count] as [String: Any?]
    }
    if let array = value as? [Any?] {
      return array.map { extractBinaries($0, into: &segments) }
    }
    if let map = value as? [String: Any?] {
      var copy: [String: Any?] = [:]
      copy.reserveCapacity(map.count)
      for (key, element) in map {
        copy.updateValue(extractBinaries(element, into: &segments), forKey: key)
      }
      return copy
    }
    return value
  }

  private static func placeholder(_ value: Any?) -> (index: Int, length: Int)? {
    // anything but exactly these two keys with non-negative ints is user data
    guard
      let map = value as? [String: Any?],
      map.count == 2,
      let index = map[placeholderKey] as? Int, index >= 0,
      let length = map["l"] as? Int, length >= 0
    else { return nil }
    return (index, length)
  }

  private static func collectSegmentLengths(_ value: Any?, into lengths: inout [Int: Int]) {
    if let found = placeholder(value) {
      lengths[found.index] = found.length
      return
    }
    if let array = value as? [Any?] {
      for element in array {
        collectSegmentLengths(element, into: &lengths)
      }
      return
    }
    if let map = value as? [String: Any?] {
      for element in map.values {
        collectSegmentLengths(element, into: &lengths)
      }
    }
  }

  private static func restoreBinaries(_ value: Any?, segments: [Data]) -> Any? {
    if let found = placeholder(value) {
      return segments[found.index]
    }
    if let array = value as? [Any?] {
      return array.map { restoreBinaries($0, segments: segments) }
    }
    if let map = value as? [String: Any?] {
      var copy: [String: Any?] = [:]
      copy.reserveCapacity(map.count)
      for (key, element) in map {
        copy.updateValue(restoreBinaries(element, segments: segments), forKey: key)
      }
      return copy
    }
    return value
  }
}
