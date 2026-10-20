import Foundation

public enum MessagePackError: Error, CustomStringConvertible {
  case unsupportedType(String)
  case truncated
  case malformed(String)

  public var description: String {
    switch self {
    case .unsupportedType(let type): return "unsupported type: \(type)"
    case .truncated: return "truncated messagepack data"
    case .malformed(let reason): return "malformed messagepack data: \(reason)"
    }
  }
}

public struct MessagePackExtension: Equatable, Sendable {
  public let type: Int8
  public let data: Data

  public init(type: Int8, data: Data) {
    self.type = type
    self.data = data
  }
}

public enum MessagePack {
  public static func encode(_ value: Any?) throws -> Data {
    var writer = Writer()
    try writer.write(value)
    return writer.data
  }

  public static func decode(_ data: Data) throws -> Any? {
    var reader = Reader(data)
    let value = try reader.read()
    guard reader.isAtEnd else { throw MessagePackError.malformed("trailing bytes") }
    return value
  }

  struct Writer {
    var data = Data()

    mutating func write(_ value: Any?) throws {
      guard let value, !(value is NSNull) else {
        data.append(0xc0)
        return
      }

      switch value {
      case let bool as Bool:
        data.append(bool ? 0xc3 : 0xc2)
      case let int as Int:
        writeInt(Int64(int))
      case let int as Int64:
        writeInt(int)
      case let int as Int32:
        writeInt(Int64(int))
      case let int as Int16:
        writeInt(Int64(int))
      case let int as Int8:
        writeInt(Int64(int))
      case let uint as UInt64:
        writeUInt(uint)
      case let uint as UInt:
        writeUInt(UInt64(uint))
      case let uint as UInt32:
        writeUInt(UInt64(uint))
      case let uint as UInt16:
        writeUInt(UInt64(uint))
      case let uint as UInt8:
        writeUInt(UInt64(uint))
      case let double as Double:
        writeFloat64(double)
      case let float as Float:
        writeFloat64(Double(float))
      case let string as String:
        try writeString(string)
      case let binary as Data:
        try writeBinary(binary)
      case let date as Date:
        writeTimestamp(date)
      case let ext as MessagePackExtension:
        try writeExtension(ext)
      case let array as [Any?]:
        try writeArray(array)
      case let dictionary as [String: Any?]:
        try writeMap(dictionary)
      default:
        throw MessagePackError.unsupportedType(String(describing: Swift.type(of: value)))
      }
    }

    private mutating func writeInt(_ value: Int64) {
      if value >= 0 {
        writeUInt(UInt64(value))
        return
      }
      if value >= -32 {
        data.append(UInt8(truncatingIfNeeded: value))
      } else if value >= Int64(Int8.min) {
        data.append(0xd0)
        data.append(UInt8(bitPattern: Int8(value)))
      } else if value >= Int64(Int16.min) {
        data.append(0xd1)
        appendBigEndian(UInt16(bitPattern: Int16(value)))
      } else if value >= Int64(Int32.min) {
        data.append(0xd2)
        appendBigEndian(UInt32(bitPattern: Int32(value)))
      } else {
        data.append(0xd3)
        appendBigEndian(UInt64(bitPattern: value))
      }
    }

    private mutating func writeUInt(_ value: UInt64) {
      if value <= 0x7f {
        data.append(UInt8(value))
      } else if value <= UInt64(UInt8.max) {
        data.append(0xcc)
        data.append(UInt8(value))
      } else if value <= UInt64(UInt16.max) {
        data.append(0xcd)
        appendBigEndian(UInt16(value))
      } else if value <= UInt64(UInt32.max) {
        data.append(0xce)
        appendBigEndian(UInt32(value))
      } else {
        data.append(0xcf)
        appendBigEndian(value)
      }
    }

    private mutating func writeFloat64(_ value: Double) {
      data.append(0xcb)
      appendBigEndian(value.bitPattern)
    }

    private mutating func writeString(_ value: String) throws {
      let utf8 = Data(value.utf8)
      switch utf8.count {
      case 0...31:
        data.append(0xa0 | UInt8(utf8.count))
      case 32...Int(UInt8.max):
        data.append(0xd9)
        data.append(UInt8(utf8.count))
      case (Int(UInt8.max) + 1)...Int(UInt16.max):
        data.append(0xda)
        appendBigEndian(UInt16(utf8.count))
      default:
        guard utf8.count <= UInt32.max else { throw MessagePackError.unsupportedType("oversized string") }
        data.append(0xdb)
        appendBigEndian(UInt32(utf8.count))
      }
      data.append(utf8)
    }

    private mutating func writeBinary(_ value: Data) throws {
      switch value.count {
      case 0...Int(UInt8.max):
        data.append(0xc4)
        data.append(UInt8(value.count))
      case (Int(UInt8.max) + 1)...Int(UInt16.max):
        data.append(0xc5)
        appendBigEndian(UInt16(value.count))
      default:
        guard value.count <= UInt32.max else { throw MessagePackError.unsupportedType("oversized binary") }
        data.append(0xc6)
        appendBigEndian(UInt32(value.count))
      }
      data.append(value)
    }

    private mutating func writeTimestamp(_ date: Date) {
      let interval = date.timeIntervalSince1970
      let seconds = Int64(interval.rounded(.down))
      let nanoseconds = UInt32(((interval - Double(seconds)) * 1_000_000_000).rounded())

      if nanoseconds == 0, seconds >= 0, seconds <= UInt32.max {
        data.append(contentsOf: [0xd6, 0xff])
        appendBigEndian(UInt32(seconds))
      } else if seconds >= 0, seconds < (1 << 34) {
        let packed = (UInt64(nanoseconds) << 34) | UInt64(seconds)
        data.append(contentsOf: [0xd7, 0xff])
        appendBigEndian(packed)
      } else {
        data.append(contentsOf: [0xc7, 12, 0xff])
        appendBigEndian(nanoseconds)
        appendBigEndian(UInt64(bitPattern: seconds))
      }
    }

    private mutating func writeExtension(_ ext: MessagePackExtension) throws {
      switch ext.data.count {
      case 1: data.append(0xd4)
      case 2: data.append(0xd5)
      case 4: data.append(0xd6)
      case 8: data.append(0xd7)
      case 16: data.append(0xd8)
      case 0...Int(UInt8.max):
        data.append(0xc7)
        data.append(UInt8(ext.data.count))
      case (Int(UInt8.max) + 1)...Int(UInt16.max):
        data.append(0xc8)
        appendBigEndian(UInt16(ext.data.count))
      default:
        guard ext.data.count <= UInt32.max else { throw MessagePackError.unsupportedType("oversized extension") }
        data.append(0xc9)
        appendBigEndian(UInt32(ext.data.count))
      }
      data.append(UInt8(bitPattern: ext.type))
      data.append(ext.data)
    }

    private mutating func writeArray(_ array: [Any?]) throws {
      switch array.count {
      case 0...15:
        data.append(0x90 | UInt8(array.count))
      case 16...Int(UInt16.max):
        data.append(0xdc)
        appendBigEndian(UInt16(array.count))
      default:
        guard array.count <= UInt32.max else { throw MessagePackError.unsupportedType("oversized array") }
        data.append(0xdd)
        appendBigEndian(UInt32(array.count))
      }
      for element in array {
        try write(element)
      }
    }

    private mutating func writeMap(_ dictionary: [String: Any?]) throws {
      switch dictionary.count {
      case 0...15:
        data.append(0x80 | UInt8(dictionary.count))
      case 16...Int(UInt16.max):
        data.append(0xde)
        appendBigEndian(UInt16(dictionary.count))
      default:
        guard dictionary.count <= UInt32.max else { throw MessagePackError.unsupportedType("oversized map") }
        data.append(0xdf)
        appendBigEndian(UInt32(dictionary.count))
      }
      for (key, element) in dictionary {
        try writeString(key)
        try write(element)
      }
    }

    private mutating func appendBigEndian<T: FixedWidthInteger>(_ value: T) {
      withUnsafeBytes(of: value.bigEndian) { data.append(contentsOf: $0) }
    }
  }

  struct Reader {
    private let data: Data
    private var offset: Int

    init(_ data: Data) {
      self.data = data
      self.offset = data.startIndex
    }

    var isAtEnd: Bool { offset == data.endIndex }

    mutating func read() throws -> Any? {
      let marker = try readByte()

      switch marker {
      case 0x00...0x7f:
        return Int(marker)
      case 0x80...0x8f:
        return try readMap(count: Int(marker & 0x0f))
      case 0x90...0x9f:
        return try readArray(count: Int(marker & 0x0f))
      case 0xa0...0xbf:
        return try readString(length: Int(marker & 0x1f))
      case 0xc0:
        return nil
      case 0xc1:
        throw MessagePackError.malformed("reserved marker 0xc1")
      case 0xc2:
        return false
      case 0xc3:
        return true
      case 0xc4:
        return try readData(length: Int(try readByte()))
      case 0xc5:
        return try readData(length: Int(try readBigEndian(UInt16.self)))
      case 0xc6:
        return try readData(length: Int(try readBigEndian(UInt32.self)))
      case 0xc7:
        let length = Int(try readByte())
        return try readExtension(length: length)
      case 0xc8:
        let length = Int(try readBigEndian(UInt16.self))
        return try readExtension(length: length)
      case 0xc9:
        let length = Int(try readBigEndian(UInt32.self))
        return try readExtension(length: length)
      case 0xca:
        return Double(Float(bitPattern: try readBigEndian(UInt32.self)))
      case 0xcb:
        return Double(bitPattern: try readBigEndian(UInt64.self))
      case 0xcc:
        return Int(try readByte())
      case 0xcd:
        return Int(try readBigEndian(UInt16.self))
      case 0xce:
        return Int(try readBigEndian(UInt32.self))
      case 0xcf:
        let value = try readBigEndian(UInt64.self)
        return value <= UInt64(Int64.max) ? Int(value) : value
      case 0xd0:
        return Int(Int8(bitPattern: try readByte()))
      case 0xd1:
        return Int(Int16(bitPattern: try readBigEndian(UInt16.self)))
      case 0xd2:
        return Int(Int32(bitPattern: try readBigEndian(UInt32.self)))
      case 0xd3:
        return Int(Int64(bitPattern: try readBigEndian(UInt64.self)))
      case 0xd4:
        return try readExtension(length: 1)
      case 0xd5:
        return try readExtension(length: 2)
      case 0xd6:
        return try readExtension(length: 4)
      case 0xd7:
        return try readExtension(length: 8)
      case 0xd8:
        return try readExtension(length: 16)
      case 0xd9:
        return try readString(length: Int(try readByte()))
      case 0xda:
        return try readString(length: Int(try readBigEndian(UInt16.self)))
      case 0xdb:
        return try readString(length: Int(try readBigEndian(UInt32.self)))
      case 0xdc:
        return try readArray(count: Int(try readBigEndian(UInt16.self)))
      case 0xdd:
        return try readArray(count: Int(try readBigEndian(UInt32.self)))
      case 0xde:
        return try readMap(count: Int(try readBigEndian(UInt16.self)))
      case 0xdf:
        return try readMap(count: Int(try readBigEndian(UInt32.self)))
      case 0xe0...0xff:
        return Int(Int8(bitPattern: marker))
      default:
        throw MessagePackError.malformed("unknown marker 0x\(String(marker, radix: 16))")
      }
    }

    private mutating func readByte() throws -> UInt8 {
      guard offset < data.endIndex else { throw MessagePackError.truncated }
      defer { offset += 1 }
      return data[offset]
    }

    private mutating func readBigEndian<T: FixedWidthInteger>(_ type: T.Type) throws -> T {
      let size = MemoryLayout<T>.size
      guard offset + size <= data.endIndex else { throw MessagePackError.truncated }
      var value: T = 0
      for _ in 0..<size {
        value = value << 8 | T(data[offset])
        offset += 1
      }
      return value
    }

    private mutating func readData(length: Int) throws -> Data {
      guard offset + length <= data.endIndex else { throw MessagePackError.truncated }
      defer { offset += length }
      return Data(data[offset..<(offset + length)])
    }

    private mutating func readString(length: Int) throws -> String {
      let bytes = try readData(length: length)
      guard let string = String(data: bytes, encoding: .utf8) else {
        throw MessagePackError.malformed("invalid utf8 string")
      }
      return string
    }

    private mutating func readArray(count: Int) throws -> [Any?] {
      var array: [Any?] = []
      array.reserveCapacity(count)
      for _ in 0..<count {
        array.append(try read())
      }
      return array
    }

    private mutating func readMap(count: Int) throws -> [String: Any?] {
      var map: [String: Any?] = [:]
      map.reserveCapacity(count)
      for _ in 0..<count {
        let key = try read()
        let keyString: String
        switch key {
        case let string as String: keyString = string
        case let int as Int: keyString = String(int)
        default: throw MessagePackError.malformed("unsupported map key")
        }
        map.updateValue(try read(), forKey: keyString)
      }
      return map
    }

    private mutating func readExtension(length: Int) throws -> Any? {
      let type = Int8(bitPattern: try readByte())
      let payload = try readData(length: length)
      switch type {
      case -1:
        return try decodeTimestamp(payload)
      case 0:
        // msgpackr encodes JS undefined as ext 0
        return nil
      default:
        return MessagePackExtension(type: type, data: payload)
      }
    }

    private func decodeTimestamp(_ payload: Data) throws -> Date {
      var reader = Reader(payload)
      switch payload.count {
      case 4:
        let seconds = try reader.readBigEndian(UInt32.self)
        return Date(timeIntervalSince1970: Double(seconds))
      case 8:
        let packed = try reader.readBigEndian(UInt64.self)
        let nanoseconds = packed >> 34
        let seconds = packed & 0x3_ffff_ffff
        return Date(timeIntervalSince1970: Double(seconds) + Double(nanoseconds) / 1_000_000_000)
      case 12:
        let nanoseconds = try reader.readBigEndian(UInt32.self)
        let seconds = Int64(bitPattern: try reader.readBigEndian(UInt64.self))
        return Date(timeIntervalSince1970: Double(seconds) + Double(nanoseconds) / 1_000_000_000)
      default:
        throw MessagePackError.malformed("invalid timestamp length \(payload.count)")
      }
    }
  }
}
