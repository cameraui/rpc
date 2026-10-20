import Foundation
import NIOCore
import NIOHTTP1
import NIOPosix
import NIOWebSocket

final class FakeWebSocketNats: @unchecked Sendable {
  private let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
  private let lock = NSLock()
  private var serverChannel: Channel?
  private var refusal: UInt16?
  private var links: [Channel] = []
  private var upgrades = 0

  var url: URL {
    URL(string: "ws://127.0.0.1:\(serverChannel?.localAddress?.port ?? 0)")!
  }

  var accepted: Int {
    lock.lock()
    defer { lock.unlock() }
    return upgrades
  }

  var openLinks: Int {
    lock.lock()
    defer { lock.unlock() }
    return links.filter(\.isActive).count
  }

  func start() throws {
    serverChannel = try ServerBootstrap(group: group)
      .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
      .childChannelInitializer { channel in self.accept(channel) }
      .bind(host: "127.0.0.1", port: 0)
      .wait()
  }

  func stop() {
    lock.lock()
    let open = links
    lock.unlock()
    for link in open {
      link.close(promise: nil)
    }
    try? serverChannel?.close().wait()
    try? group.syncShutdownGracefully()
  }

  func refuse(with code: UInt16?) {
    lock.lock()
    refusal = code
    lock.unlock()
  }

  func closeLinks(with code: UInt16) {
    lock.lock()
    let open = links
    lock.unlock()
    for link in open {
      link.writeAndFlush(Self.closeFrame(code, allocator: link.allocator), promise: nil)
    }
  }

  func dropLinks() {
    lock.lock()
    let open = links
    lock.unlock()
    for link in open {
      link.close(promise: nil)
    }
  }

  static func closeFrame(_ code: UInt16, allocator: ByteBufferAllocator) -> WebSocketFrame {
    var data = allocator.buffer(capacity: 16)
    data.writeInteger(code)
    data.writeString("refused")
    return WebSocketFrame(fin: true, opcode: .connectionClose, data: data)
  }

  private func accept(_ channel: Channel) -> EventLoopFuture<Void> {
    let upgrader = NIOWebSocketServerUpgrader(
      shouldUpgrade: { channel, _ in channel.eventLoop.makeSucceededFuture(HTTPHeaders()) },
      upgradePipelineHandler: { channel, _ in
        self.lock.lock()
        self.upgrades += 1
        self.links.append(channel)
        let refusal = self.refusal
        self.lock.unlock()
        let handler: ChannelHandler =
          refusal.map { FakeRefusal(code: $0) } ?? FakeWebSocketNatsHandler()
        return channel.pipeline.addHandler(handler)
      }
    )
    return channel.pipeline.configureHTTPServerPipeline(
      withServerUpgrade: (upgraders: [upgrader], completionHandler: { _ in }))
  }
}

private final class FakeRefusal: ChannelInboundHandler {
  typealias InboundIn = WebSocketFrame
  typealias OutboundOut = WebSocketFrame

  private let code: UInt16

  init(code: UInt16) {
    self.code = code
  }

  func handlerAdded(context: ChannelHandlerContext) {
    let frame = FakeWebSocketNats.closeFrame(code, allocator: context.channel.allocator)
    context.writeAndFlush(wrapOutboundOut(frame), promise: nil)
  }

  func channelRead(context: ChannelHandlerContext, data: NIOAny) {
    if unwrapInboundIn(data).opcode == .connectionClose {
      context.close(promise: nil)
    }
  }
}

private final class FakeWebSocketNatsHandler: ChannelInboundHandler {
  typealias InboundIn = WebSocketFrame
  typealias OutboundOut = WebSocketFrame

  private var buffer = ByteBuffer()
  private var payloadToSkip = 0

  func handlerAdded(context: ChannelHandlerContext) {
    let port = context.channel.localAddress?.port ?? 0
    send(
      "INFO {\"server_id\":\"FAKE\",\"server_name\":\"fake\",\"version\":\"2.10.0\","
        + "\"proto\":1,\"go\":\"go1.22\",\"host\":\"127.0.0.1\",\"port\":\(port),"
        + "\"headers\":true,\"max_payload\":1048576,\"client_ip\":\"127.0.0.1\"}\r\n",
      context: context)
  }

  func channelRead(context: ChannelHandlerContext, data: NIOAny) {
    let frame = unwrapInboundIn(data)
    switch frame.opcode {
    case .binary, .text, .continuation:
      var incoming = frame.unmaskedData
      buffer.writeBuffer(&incoming)
      process(context: context)
    case .connectionClose:
      context.close(promise: nil)
    default:
      break
    }
  }

  private func process(context: ChannelHandlerContext) {
    while buffer.readableBytes > 0 {
      if payloadToSkip > 0 {
        let skip = min(payloadToSkip, buffer.readableBytes)
        buffer.moveReaderIndex(forwardBy: skip)
        payloadToSkip -= skip
        continue
      }
      let view = buffer.readableBytesView
      guard let newline = view.firstIndex(of: UInt8(ascii: "\n")) else { break }
      let line = buffer.readString(length: view.distance(from: view.startIndex, to: newline) + 1) ?? ""
      let parts = line.trimmingCharacters(in: .whitespacesAndNewlines).split(separator: " ")
      switch parts.first?.uppercased() {
      case "PING":
        send("PONG\r\n", context: context)
      case "PUB", "HPUB":
        payloadToSkip = (parts.last.flatMap { Int($0) } ?? 0) + 2
      default:
        break
      }
    }
    buffer.discardReadBytes()
  }

  private func send(_ text: String, context: ChannelHandlerContext) {
    let frame = WebSocketFrame(fin: true, opcode: .binary, data: context.channel.allocator.buffer(string: text))
    context.writeAndFlush(wrapOutboundOut(frame), promise: nil)
  }
}
