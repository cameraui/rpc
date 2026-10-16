import Foundation
import XCTest

@testable import CameraUIRPC

final class PullCallbackDeliveryTests: XCTestCase {
  private var server: FakeNatsServer!
  private var clients: [RPCClient] = []

  override func setUp() async throws {
    try await super.setUp()
    server = try FakeNatsServer()
  }

  override func tearDown() async throws {
    for client in clients {
      await client.close()
    }
    clients = []
    server.stop()
    try await super.tearDown()
  }

  private final class Timeline: @unchecked Sendable {
    private let lock = NSLock()
    private var handled: [(index: Int, at: Date)] = []
    private var boundaries: [Date] = []

    func handle(_ index: Int) {
      lock.lock()
      handled.append((index, Date()))
      lock.unlock()
    }

    func boundary() {
      lock.lock()
      boundaries.append(Date())
      lock.unlock()
    }

    var order: [Int] {
      lock.lock()
      defer { lock.unlock() }
      return handled.map(\.index)
    }

    var firstHandled: Date? {
      lock.lock()
      defer { lock.unlock() }
      return handled.first?.at
    }

    var firstBoundary: Date? {
      lock.lock()
      defer { lock.unlock() }
      return boundaries.first
    }
  }

  func testFramesReachTheHandlerWhileTheBatchIsStillArriving() async throws {
    let timeline = Timeline()
    servePacedPlay(frames: 20, every: 0.02, timeline: timeline)
    let client = try await makeClient()
    let iterator = try await client.callPullIteratorWithCallback(
      "test", "play",
      callbacks: ["onVideo": { args in timeline.handle(Self.index(args)) }],
      onewayMethods: ["onVideo"]
    )

    let next = iterator.makeAsyncIterator()
    _ = try await next.next()

    XCTAssertEqual(timeline.order, Array(0..<20), "the boundary returned before its batch was handled, or out of order")
    let first = try XCTUnwrap(timeline.firstHandled)
    let boundary = try XCTUnwrap(timeline.firstBoundary)
    XCTAssertLessThan(first.timeIntervalSince(boundary), -0.2, "the first frame waited for the batch boundary")
    await iterator.cancel()
  }

  func testSlowHandlerHoldsTheNextRequest() async throws {
    let timeline = Timeline()
    servePacedPlay(frames: 5, every: 0, timeline: timeline)
    let client = try await makeClient()
    let requestsSeen = Counter()
    let iterator = try await client.callPullIteratorWithCallback(
      "test", "play",
      callbacks: ["onVideo": { [server] args in
        try? await Task.sleep(nanoseconds: 60_000_000)
        requestsSeen.record(Self.nextRequests(server!))
        timeline.handle(Self.index(args))
      }],
      onewayMethods: ["onVideo"]
    )

    let next = iterator.makeAsyncIterator()
    _ = try await next.next()

    XCTAssertEqual(timeline.order, Array(0..<5))
    XCTAssertEqual(requestsSeen.maximum, 1, "a next request went out while the batch was still being handled")
    await iterator.cancel()
  }

  func testCancelStopsTheCallbacks() async throws {
    let timeline = Timeline()
    servePacedPlay(frames: 60, every: 0.02, timeline: timeline)
    let client = try await makeClient()
    let iterator = try await client.callPullIteratorWithCallback(
      "test", "play",
      callbacks: ["onVideo": { args in timeline.handle(Self.index(args)) }],
      onewayMethods: ["onVideo"]
    )
    let consumer = Task {
      let next = iterator.makeAsyncIterator()
      _ = try? await next.next()
    }
    let started = await eventually(timeout: 3) { timeline.order.count >= 5 }
    XCTAssertTrue(started)

    await iterator.cancel()
    let atCancel = timeline.order.count
    try await Task.sleep(nanoseconds: 300_000_000)
    XCTAssertLessThanOrEqual(timeline.order.count, atCancel + 1)
    let ended: Void? = await result(of: consumer, within: 2)
    XCTAssertNotNil(ended, "next() hung after cancel")
  }

  // serves test.play: every next request gets `frames` callback frames, one
  // per `every` seconds, then the batch boundary
  private func servePacedPlay(frames: Int, every interval: TimeInterval, timeline: Timeline) {
    let callbackSubjects = Subjects()
    server.onPublish("rpc.test.play") { [server] message in
      let envelope = FakeNatsServer.decode(message)
      let params = (envelope["params"] as? [Any?])?.first as? [String: Any?]
      guard let iteratorId = params?["__iteratorId"] as? String else { return }
      callbackSubjects.set(iteratorId, params?["__callbackSubject"] as? String)
      server!.reply(to: message, ["id": envelope["id"] ?? nil, "result": ["iteratorId": iteratorId]] as [String: Any?])
    }
    server.onPublish("_rpc.iterator.>") { [server] message in
      guard message.subject.hasSuffix(".request"), FakeNatsServer.decode(message)["type"] as? String == "next" else { return }
      let iteratorId = String(message.subject.dropFirst("_rpc.iterator.".count).dropLast(".request".count))
      guard let callbackSubject = callbackSubjects.get(iteratorId) else { return }
      Thread {
        for index in 0..<frames {
          let frame: [String: Any?] = ["method": "onVideo", "args": [["index": index]]]
          if let data = try? RPCCodec.encodeMessage(frame) {
            server!.publish(callbackSubject, data)
          }
          if interval > 0 {
            Thread.sleep(forTimeInterval: interval)
          }
        }
        timeline.boundary()
        let boundary: [String: Any?] = ["id": iteratorId, "type": "value", "value": nil]
        if let data = try? RPCCodec.encodeMessage(boundary) {
          server!.publish("_rpc.iterator.\(iteratorId).response", data)
        }
      }.start()
    }
  }

  private func makeClient() async throws -> RPCClient {
    let client = RPCClient(options: RPCClientOptions(url: server.url, connId: "conn\(UInt32.random(in: 1000...9999))"))
    try await client.connect()
    clients.append(client)
    return client
  }

  private static func index(_ args: [Any?]) -> Int {
    ((args.first ?? nil) as? [String: Any?])?["index"] as? Int ?? -1
  }

  private static func nextRequests(_ server: FakeNatsServer) -> Int {
    server.messages(on: "_rpc.iterator.>").filter {
      $0.subject.hasSuffix(".request") && FakeNatsServer.decode($0)["type"] as? String == "next"
    }.count
  }

  private final class Subjects: @unchecked Sendable {
    private let lock = NSLock()
    private var byIterator: [String: String] = [:]

    func set(_ iteratorId: String, _ subject: String?) {
      lock.lock()
      byIterator[iteratorId] = subject
      lock.unlock()
    }

    func get(_ iteratorId: String) -> String? {
      lock.lock()
      defer { lock.unlock() }
      return byIterator[iteratorId]
    }
  }

  private final class Counter: @unchecked Sendable {
    private let lock = NSLock()
    private var highest = 0

    func record(_ value: Int) {
      lock.lock()
      highest = max(highest, value)
      lock.unlock()
    }

    var maximum: Int {
      lock.lock()
      defer { lock.unlock() }
      return highest
    }
  }
}
