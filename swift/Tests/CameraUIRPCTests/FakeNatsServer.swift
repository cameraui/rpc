import Foundation

@testable import CameraUIRPC

#if canImport(Glibc)
  import Glibc
#elseif canImport(Darwin)
  import Darwin
#endif

struct FakeMessage {
  let subject: String
  let reply: String?
  let headers: [String: String]
  let payload: Data
}

final class FakeNatsServer: @unchecked Sendable {
  private let lock = NSLock()
  private var listenFD: Int32 = -1
  private var connections: [FakeNatsConnection] = []
  private var handlers: [(pattern: String, handle: (FakeMessage) -> Void)] = []
  private var published: [FakeMessage] = []
  private var refusedPublishes: [String] = []
  private var refusing = false
  private var readsPaused = false
  private var stopped = false
  private(set) var port: UInt16 = 0

  var url: URL { URL(string: "nats://127.0.0.1:\(port)")! }

  var openConnections: Int {
    lock.lock()
    defer { lock.unlock() }
    return connections.filter { !$0.isClosed }.count
  }

  init() throws {
    listenFD = socket(AF_INET, Self.streamType, 0)
    guard listenFD >= 0 else { throw FakeServerError.socket("socket") }
    var one: Int32 = 1
    setsockopt(listenFD, SOL_SOCKET, SO_REUSEADDR, &one, socklen_t(MemoryLayout<Int32>.size))

    var address = sockaddr_in()
    #if canImport(Darwin)
      address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
    #endif
    address.sin_family = sa_family_t(AF_INET)
    address.sin_port = 0
    address.sin_addr.s_addr = inet_addr("127.0.0.1")
    var length = socklen_t(MemoryLayout<sockaddr_in>.size)
    let bound = withUnsafePointer(to: &address) {
      $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { bind(listenFD, $0, length) }
    }
    guard bound == 0, listen(listenFD, 16) == 0 else { throw FakeServerError.socket("bind") }
    _ = withUnsafeMutablePointer(to: &address) {
      $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { getsockname(listenFD, $0, &length) }
    }
    port = UInt16(bigEndian: address.sin_port)

    let thread = Thread { [weak self] in self?.acceptLoop() }
    thread.start()
  }

  func stop() {
    lock.lock()
    stopped = true
    let open = connections
    lock.unlock()
    for connection in open {
      connection.close()
    }
  }

  func dropConnections() {
    lock.lock()
    let open = connections
    lock.unlock()
    for connection in open {
      connection.close()
    }
  }

  func silenceConnections() {
    lock.lock()
    let open = connections
    lock.unlock()
    for connection in open {
      connection.silence()
    }
  }

  func setRefusing(_ refusing: Bool) {
    lock.lock()
    self.refusing = refusing
    lock.unlock()
  }

  func refusePublishes(to pattern: String) {
    lock.lock()
    refusedPublishes.append(pattern)
    lock.unlock()
  }

  func pauseReads() {
    lock.lock()
    readsPaused = true
    lock.unlock()
  }

  func resumeReads() {
    lock.lock()
    readsPaused = false
    lock.unlock()
  }

  var readsArePaused: Bool {
    lock.lock()
    defer { lock.unlock() }
    return readsPaused
  }

  func onPublish(_ pattern: String, _ handle: @escaping (FakeMessage) -> Void) {
    lock.lock()
    handlers.append((pattern, handle))
    lock.unlock()
  }

  func messages(on pattern: String) -> [FakeMessage] {
    lock.lock()
    defer { lock.unlock() }
    return published.filter { Self.matches(pattern, $0.subject) }
  }

  func publish(_ subject: String, _ payload: Data, headers: [String: String] = [:]) {
    lock.lock()
    let open = connections
    lock.unlock()
    for connection in open {
      connection.deliver(subject: subject, reply: nil, headers: headers, payload: payload)
    }
  }

  func reply(to message: FakeMessage, _ value: Any?) {
    guard let reply = message.reply, let data = try? RPCCodec.encodeMessage(value) else { return }
    publish(reply, data)
  }

  static func decode(_ message: FakeMessage) -> [String: Any?] {
    (try? RPCCodec.decodeMessage(message.payload)) as? [String: Any?] ?? [:]
  }

  static func matches(_ pattern: String, _ subject: String) -> Bool {
    let wanted = pattern.split(separator: ".", omittingEmptySubsequences: false)
    let actual = subject.split(separator: ".", omittingEmptySubsequences: false)
    for (index, token) in wanted.enumerated() {
      if token == ">" { return actual.count > index }
      guard index < actual.count, token == "*" || token == actual[index] else { return false }
    }
    return wanted.count == actual.count
  }

  fileprivate func received(_ message: FakeMessage) -> Bool {
    lock.lock()
    published.append(message)
    if refusedPublishes.contains(where: { Self.matches($0, message.subject) }) {
      lock.unlock()
      return false
    }
    let open = connections
    let matching = handlers.filter { Self.matches($0.pattern, message.subject) }
    lock.unlock()
    for connection in open {
      connection.deliver(
        subject: message.subject, reply: message.reply, headers: message.headers,
        payload: message.payload)
    }
    for handler in matching {
      handler.handle(message)
    }
    return true
  }

  private func acceptLoop() {
    while true {
      lock.lock()
      let done = stopped
      lock.unlock()
      if done { break }

      var poller = pollfd(fd: listenFD, events: Int16(POLLIN), revents: 0)
      guard poll(&poller, 1, 50) > 0 else { continue }
      let fd = accept(listenFD, nil, nil)
      guard fd >= 0 else { continue }

      lock.lock()
      let refuse = refusing || stopped
      lock.unlock()
      if refuse {
        closeSocket(fd)
        continue
      }
      let connection = FakeNatsConnection(fd: fd, server: self, port: port)
      lock.lock()
      connections.append(connection)
      lock.unlock()
      connection.start()
    }
    closeSocket(listenFD)
  }

  #if canImport(Glibc)
    private static let streamType = Int32(SOCK_STREAM.rawValue)
  #else
    private static let streamType = SOCK_STREAM
  #endif
}

enum FakeServerError: Error {
  case socket(String)
}

final class FakeNatsConnection: @unchecked Sendable {
  private let fd: Int32
  private let server: FakeNatsServer
  private let port: UInt16
  private let lock = NSLock()
  private let writeLock = NSLock()
  private var subscriptions: [String: String] = [:]
  private var closed = false
  private var silent = false

  init(fd: Int32, server: FakeNatsServer, port: UInt16) {
    self.fd = fd
    self.server = server
    self.port = port
    #if canImport(Darwin)
      var one: Int32 = 1
      setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, socklen_t(MemoryLayout<Int32>.size))
    #endif
  }

  var isClosed: Bool {
    lock.lock()
    defer { lock.unlock() }
    return closed
  }

  func start() {
    let info =
      "INFO {\"server_id\":\"FAKE\",\"server_name\":\"fake\",\"version\":\"2.10.0\","
      + "\"proto\":1,\"go\":\"go1.22\",\"host\":\"127.0.0.1\",\"port\":\(port),"
      + "\"headers\":true,\"max_payload\":1048576,\"client_ip\":\"127.0.0.1\"}\r\n"
    write(Data(info.utf8))
    let thread = Thread { [self] in readLoop() }
    thread.start()
  }

  func silence() {
    lock.lock()
    silent = true
    lock.unlock()
  }

  func close() {
    lock.lock()
    let wasClosed = closed
    closed = true
    lock.unlock()
    guard !wasClosed else { return }
    shutdown(fd, Int32(SHUT_RDWR))
  }

  func deliver(subject: String, reply: String?, headers: [String: String], payload: Data) {
    lock.lock()
    let sids = subscriptions.filter { FakeNatsServer.matches($0.value, subject) }.map(\.key)
    lock.unlock()
    let replyPart = reply.map { " \($0)" } ?? ""
    for sid in sids {
      var frame = Data()
      if headers.isEmpty {
        frame.append(Data("MSG \(subject) \(sid)\(replyPart) \(payload.count)\r\n".utf8))
      } else {
        var block = "NATS/1.0\r\n"
        for (key, value) in headers {
          block += "\(key): \(value)\r\n"
        }
        block += "\r\n"
        let headerData = Data(block.utf8)
        frame.append(
          Data(
            "HMSG \(subject) \(sid)\(replyPart) \(headerData.count) \(headerData.count + payload.count)\r\n"
              .utf8))
        frame.append(headerData)
      }
      frame.append(payload)
      frame.append(Data("\r\n".utf8))
      write(frame)
    }
  }

  private func write(_ data: Data) {
    writeLock.lock()
    defer { writeLock.unlock() }
    lock.lock()
    let blocked = closed || silent
    lock.unlock()
    guard !blocked else { return }
    data.withUnsafeBytes { raw in
      var offset = 0
      while offset < raw.count {
        #if canImport(Glibc)
          let sent = send(fd, raw.baseAddress! + offset, raw.count - offset, Int32(MSG_NOSIGNAL))
        #else
          let sent = send(fd, raw.baseAddress! + offset, raw.count - offset, 0)
        #endif
        guard sent > 0 else { return }
        offset += sent
      }
    }
  }

  private func readLoop() {
    var buffer = Data()
    var chunk = [UInt8](repeating: 0, count: 64 * 1024)
    reading: while true {
      while server.readsArePaused && !isClosed {
        usleep(5_000)
      }
      let count = recv(fd, &chunk, chunk.count, 0)
      guard count > 0 else { break }
      buffer.append(contentsOf: chunk[0..<count])
      while let consumed = parse(buffer) {
        guard consumed > 0 else { break }
        buffer.removeSubrange(buffer.startIndex..<(buffer.startIndex + consumed))
        if isClosed { break reading }
      }
    }
    close()
    closeSocket(fd)
  }

  private func parse(_ buffer: Data) -> Int? {
    guard let lineEnd = buffer.range(of: Data("\r\n".utf8)) else { return nil }
    let line = String(decoding: buffer[buffer.startIndex..<lineEnd.lowerBound], as: UTF8.self)
    let headerLength = lineEnd.upperBound - buffer.startIndex
    let parts = line.split(separator: " ").map(String.init)
    guard let op = parts.first?.uppercased() else { return headerLength }

    switch op {
    case "PING":
      write(Data("PONG\r\n".utf8))
    case "SUB" where parts.count >= 3:
      lock.lock()
      subscriptions[parts[parts.count - 1]] = parts[1]
      lock.unlock()
    case "UNSUB" where parts.count >= 2:
      lock.lock()
      subscriptions.removeValue(forKey: parts[1])
      lock.unlock()
    case "PUB", "HPUB":
      let hasHeaders = op == "HPUB"
      guard let total = parts.last.flatMap(Int.init) else { return headerLength }
      let headerBytes = hasHeaders ? Int(parts[parts.count - 2]) ?? 0 : 0
      let argCount = parts.count - (hasHeaders ? 3 : 2)
      let reply = argCount == 2 ? parts[2] : nil
      guard buffer.count >= headerLength + total + 2 else { return nil }
      let body = buffer[(buffer.startIndex + headerLength)..<(buffer.startIndex + headerLength + total)]
      let headers = hasHeaders ? Self.parseHeaders(Data(body.prefix(headerBytes))) : [:]
      let payload = Data(body.dropFirst(headerBytes))
      let message = FakeMessage(subject: parts[1], reply: reply, headers: headers, payload: payload)
      if !server.received(message) {
        write(Data("-ERR 'Permissions Violation for Publish to \"\(message.subject)\"'\r\n".utf8))
      }
      return headerLength + total + 2
    default:
      break
    }
    return headerLength
  }

  private static func parseHeaders(_ data: Data) -> [String: String] {
    var headers: [String: String] = [:]
    for line in String(decoding: data, as: UTF8.self).components(separatedBy: "\r\n").dropFirst() {
      guard let colon = line.firstIndex(of: ":") else { continue }
      headers[String(line[..<colon])] = line[line.index(after: colon)...].trimmingCharacters(in: .whitespaces)
    }
    return headers
  }
}

private func closeSocket(_ fd: Int32) {
  #if canImport(Glibc)
    _ = Glibc.close(fd)
  #else
    _ = Darwin.close(fd)
  #endif
}

func eventually(timeout: TimeInterval, _ condition: () async -> Bool) async -> Bool {
  let deadline = Date().addingTimeInterval(timeout)
  while Date() < deadline {
    if await condition() { return true }
    try? await Task.sleep(nanoseconds: 20_000_000)
  }
  return await condition()
}
