import Foundation
import XCTest

@testable import CameraUIRPC

final class RPCClientTransportTests: XCTestCase {
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

  func testParkedIteratorEndsWithNotConnectedWhenTheLinkDrops() async throws {
    servePullInit("pull")
    let client = try await makeClient()
    let iterator = try await client.callPullIterator("test", "pull")
    let outcome = Task { () -> Error? in
      let next = iterator.makeAsyncIterator()
      do {
        _ = try await next.next()
        return nil
      } catch {
        return error
      }
    }
    let requested = await eventually(timeout: 2) { !self.iteratorRequests("next").isEmpty }
    XCTAssertTrue(requested)

    server.setRefusing(true)
    server.dropConnections()

    let ended = await result(of: outcome, within: 3)
    outcome.cancel()
    guard case .some(.some(RPCClientError.notConnected)) = ended else {
      return XCTFail("next() did not end with notConnected: \(String(describing: ended))")
    }

    server.setRefusing(false)
    let cancelled = await eventually(timeout: 5) { self.iteratorRequests("cancel").count == 1 }
    XCTAssertTrue(cancelled, "the producer of the dropped iterator was never cancelled")
  }

  func testCallDuringAnOutageIsSentOnceTheLinkIsBack() async throws {
    serveEcho()
    let client = try await makeClient(requestTimeout: 8)
    let events = await client.statusEvents()

    server.setRefusing(true)
    server.dropConnections()
    let dropped = await firstEvent(.disconnected, in: events, within: 3)
    XCTAssertTrue(dropped)

    let started = Date()
    let call = Task { try await client.call("test", "echo", args: ["hello"]) }
    try await Task.sleep(nanoseconds: 300_000_000)
    server.setRefusing(false)

    let value = try await call.value
    XCTAssertEqual(value as? String, "hello")
    XCTAssertLessThan(Date().timeIntervalSince(started), 5)
  }

  func testCallbackEndedDuringAnOutageIsCancelledAfterTheReconnect() async throws {
    serveEmptyReply("watch")
    let client = try await makeClient()
    let callbacks = try await client.callWithCallback("test", "watch")
    let consumer = Task {
      for try await _ in callbacks {}
    }
    let events = await client.statusEvents()
    server.setRefusing(true)
    server.dropConnections()
    let dropped = await firstEvent(.disconnected, in: events, within: 3)
    XCTAssertTrue(dropped)

    consumer.cancel()
    try await Task.sleep(nanoseconds: 100_000_000)
    server.setRefusing(false)

    let cancelled = await eventually(timeout: 5) {
      self.server.messages(on: "rpc.cb.>").contains { $0.subject.hasSuffix(".cancel") }
    }
    XCTAssertTrue(cancelled, "the cancel made during the outage never reached the server")
  }

  func testCloseCancelsOpenIteratorsAndCallbacks() async throws {
    servePullInit("pull")
    serveEmptyReply("watch")
    let client = try await makeClient()
    let iterator = try await client.callPullIterator("test", "pull")
    let callbacks = try await client.callWithCallback("test", "watch")

    await client.close()

    let iteratorCancelled = await eventually(timeout: 1) { self.iteratorRequests("cancel").count == 1 }
    XCTAssertTrue(iteratorCancelled, "close() left the iterator parked on the server")
    let callbackCancelled = server.messages(on: "rpc.cb.>").contains { $0.subject.hasSuffix(".cancel") }
    XCTAssertTrue(callbackCancelled, "close() left the callback registered on the server")
    withExtendedLifetime((iterator, callbacks)) {}
  }

  func testCloseWithTheLinkDownReturnsPromptly() async throws {
    servePullInit("pull")
    let client = try await makeClient()
    let iterator = try await client.callPullIterator("test", "pull")
    let events = await client.statusEvents()
    server.setRefusing(true)
    server.dropConnections()
    let dropped = await firstEvent(.disconnected, in: events, within: 3)
    XCTAssertTrue(dropped)

    let started = Date()
    await client.close()
    XCTAssertLessThan(Date().timeIntervalSince(started), 1)
    withExtendedLifetime(iterator) {}
  }

  func testChunkedCallbackInvocationReachesItsHandler() async throws {
    servePullInit("frames")
    let frame = Data((0..<40_000).map { UInt8(truncatingIfNeeded: $0) })
    server.onPublish("_rpc.iterator.>") { [server] message in
      guard message.subject.hasSuffix(".request") else { return }
      let request = FakeNatsServer.decode(message)
      guard request["type"] as? String == "next", let id = request["id"] as? String else { return }
      let invocation: [String: Any?] = ["method": "onVideo", "args": [frame]]
      server!.publishChunked("_rpc.cb.\(id)", try! RPCCodec.encodeMessage(invocation), chunkSize: 16_000)
      let value: [String: Any?] = ["id": id, "type": "value", "value": nil]
      server!.publish("_rpc.iterator.\(id).response", try! RPCCodec.encodeMessage(value))
    }
    let client = try await makeClient()
    let received = Recorder<Data>()
    let iterator = try await client.callPullIteratorWithCallback(
      "test", "frames",
      callbacks: ["onVideo": { args in received.append(args.first as? Data ?? Data()) }],
      onewayMethods: ["onVideo"]
    )

    let pulled = Task { () -> Bool in
      let next = iterator.makeAsyncIterator()
      return (try? await next.next()) != nil
    }
    let answered = await result(of: pulled, within: 3)
    XCTAssertEqual(answered, true)
    XCTAssertEqual(received.values, [frame])
  }

  func testChunkedReplyIsReassembled() async throws {
    let payload = Data((0..<50_000).map { UInt8(truncatingIfNeeded: $0 * 7) })
    server.onPublish("rpc.test.big") { [server] message in
      let envelope = FakeNatsServer.decode(message)
      let reply: [String: Any?] = ["id": envelope["id"] ?? nil, "result": payload]
      server!.publishChunked(message.reply!, try! RPCCodec.encodeMessage(reply), chunkSize: 20_000)
    }
    let client = try await makeClient()
    let value = try await client.call("test", "big")
    XCTAssertEqual(value as? Data, payload)
  }

  func testTimedOutCallDropsItsHalfReceivedReply() async throws {
    server.onPublish("rpc.test.slow") { [server] message in
      let transferId = "t1"
      let header: [String: Any?] = [
        "type": "chunked", "transferId": transferId, "totalChunks": 2, "totalSize": 100, "chunkSize": 60,
      ]
      server!.publish(
        message.reply!, try! RPCCodec.encode(header),
        headers: ["x-chunked-transfer": "header", "x-chunk-id": transferId])
      server!.publish(
        message.reply!, Data(count: 60),
        headers: ["x-chunked-transfer": "chunk", "x-chunk-id": transferId, "x-chunk-index": "0"])
    }
    let client = try await makeClient()

    do {
      _ = try await client.call("test", "slow", timeout: 0.5)
      XCTFail("the call finished without its second chunk")
    } catch RPCClientError.timeout {
      let replies = await client.replyChunks
      let budget = await client.budget
      XCTAssertEqual(replies.count, 0)
      XCTAssertNotNil(budget.reserve(budget.limit), "the timed-out transfer still holds budget")
    }
  }

  func testBrokenChunkedReplyFailsTheCallRightAway() async throws {
    server.onPublish("rpc.test.broken") { [server] message in
      let transferId = "t1"
      let header: [String: Any?] = [
        "type": "chunked", "transferId": transferId, "totalChunks": 2, "totalSize": 100, "chunkSize": 60,
      ]
      server!.publish(
        message.reply!, try! RPCCodec.encode(header),
        headers: ["x-chunked-transfer": "header", "x-chunk-id": transferId])
      server!.publish(
        message.reply!, Data(count: 10),
        headers: ["x-chunked-transfer": "chunk", "x-chunk-id": transferId, "x-chunk-index": "0"])
    }
    let client = try await makeClient(requestTimeout: 5)
    let started = Date()
    do {
      _ = try await client.call("test", "broken")
      XCTFail("a broken chunked reply was accepted")
    } catch RPCClientError.invalidResponse {
      XCTAssertLessThan(Date().timeIntervalSince(started), 2)
    } catch {
      XCTFail("unexpected error \(error)")
    }
  }

  func testChunkedReplyCutByADisconnectFailsTheCallRightAway() async throws {
    server.onPublish("rpc.test.cut") { [server] message in
      let transferId = "t1"
      let header: [String: Any?] = [
        "type": "chunked", "transferId": transferId, "totalChunks": 2, "totalSize": 100, "chunkSize": 60,
      ]
      server!.publish(
        message.reply!, try! RPCCodec.encode(header),
        headers: ["x-chunked-transfer": "header", "x-chunk-id": transferId])
    }
    let client = try await makeClient(requestTimeout: 8)
    let call = Task { () -> Error? in
      do {
        _ = try await client.call("test", "cut")
        return nil
      } catch {
        return error
      }
    }
    let started = await eventually(timeout: 2) { await client.replyChunks.count == 1 }
    XCTAssertTrue(started)

    server.dropConnections()

    let ended = await result(of: call, within: 3)
    call.cancel()
    guard case .some(.some(RPCClientError.notConnected)) = ended else {
      return XCTFail("the cut reply did not fail its call: \(String(describing: ended))")
    }
  }

  func testFlushReportsNotConnectedWhenTheLinkDropsDuringTheProbe() async throws {
    let client = try await makeClient()
    server.silenceConnections()
    let probe = Task { () -> Error? in
      do {
        try await client.flush(timeout: 4)
        return nil
      } catch {
        return error
      }
    }
    try await Task.sleep(nanoseconds: 200_000_000)

    server.dropConnections()

    let ended = await result(of: probe, within: 5)
    guard case .some(.some(RPCClientError.notConnected)) = ended else {
      return XCTFail("flush ended with \(String(describing: ended))")
    }
  }

  func testSilentLinkIsDroppedAfterThreeMissedPings() async throws {
    let client = try await makeClient(pingInterval: 0.2)
    let events = await client.statusEvents()

    server.silenceConnections()

    let dropped = await firstEvent(.disconnected, in: events, within: 3)
    XCTAssertTrue(dropped)
  }

  func testFlushTimesOutOnASilentLink() async throws {
    let client = try await makeClient()
    try await client.flush(timeout: 1)

    server.silenceConnections()

    let started = Date()
    do {
      try await client.flush(timeout: 0.3)
      XCTFail("flush succeeded on a silent link")
    } catch RPCClientError.timeout {
      XCTAssertLessThan(Date().timeIntervalSince(started), 1)
    }
  }

  func testCameraUIOptionsPingEvery15Seconds() {
    let options = RPCClientOptions.cameraUI(serverURL: URL(string: "https://nvr.local")!, token: "cui_x")
    XCTAssertEqual(options.pingInterval, 15)
  }

  func testNextOnACancelledTaskSendsOnlyTheCancel() async throws {
    servePullInit("pull")
    let client = try await makeClient()
    let iterator = try await client.callPullIterator("test", "pull")

    let outcome = Task { () -> String in
      withUnsafeCurrentTask { $0?.cancel() }
      do {
        return try await iterator.makeAsyncIterator().next() == nil ? "ended" : "value"
      } catch {
        return "\(error)"
      }
    }

    let ended = await result(of: outcome, within: 3)
    XCTAssertEqual(ended, "ended")
    let cancelled = await eventually(timeout: 2) { self.iteratorRequests("cancel").count == 1 }
    XCTAssertTrue(cancelled, "the producer was never cancelled")
    XCTAssertTrue(iteratorRequests("next").isEmpty, "a cancelled consumer still asked for a batch")
  }

  func testPullIteratorWhoseInitTimesOutCancelsItsSession() async throws {
    let client = try await makeClient(requestTimeout: 0.5)

    do {
      _ = try await client.callPullIterator("test", "unanswered")
      XCTFail("an unanswered iterator started")
    } catch RPCClientError.timeout {}
    let cancelled = await eventually(timeout: 2) { self.iteratorRequests("cancel").count == 1 }
    XCTAssertTrue(cancelled, "a session the server may still open was never cancelled")
  }

  func testNoCallbackRunsOnceTheIteratorIsCancelled() async throws {
    servePullInit("frames")
    server.onPublish("_rpc.iterator.>") { [server] message in
      guard message.subject.hasSuffix(".request") else { return }
      let request = FakeNatsServer.decode(message)
      guard request["type"] as? String == "next", let id = request["id"] as? String else { return }
      for index in 0..<5 {
        let invocation: [String: Any?] = ["method": "onTick", "args": [index]]
        server!.publish("_rpc.cb.\(id)", try! RPCCodec.encodeMessage(invocation))
      }
      let value: [String: Any?] = ["id": id, "type": "value", "value": nil]
      server!.publish("_rpc.iterator.\(id).response", try! RPCCodec.encodeMessage(value))
    }
    let client = try await makeClient()
    let ticks = Recorder<Int>()
    let (reached, reachedSink) = AsyncStream.makeStream(of: Void.self)
    let (release, releaseSink) = AsyncStream.makeStream(of: Void.self)
    let iterator = try await client.callPullIteratorWithCallback(
      "test", "frames",
      callbacks: [
        "onTick": { args in
          ticks.append(args.first as? Int ?? -1)
          guard ticks.values.count == 1 else { return }
          reachedSink.yield()
          for await _ in release { break }
        }
      ],
      onewayMethods: ["onTick"]
    )
    let pulled = Task { _ = try? await iterator.makeAsyncIterator().next() }
    let firstTick = Task { () -> Bool in
      for await _ in reached {
        return true
      }
      return false
    }
    let first = await result(of: firstTick, within: 3)
    XCTAssertEqual(first, true)

    // a backed-up link keeps the cancel from closing the callback subscription
    server.pauseReads()
    let flooded = Recorder<Int>()
    let flood = Task {
      let chunk = Data(count: 256 * 1024)
      for index in 0..<64 {
        try? await client.publish("flood", chunk)
        flooded.append(index)
      }
    }
    try await Task.sleep(nanoseconds: 500_000_000)
    XCTAssertLessThan(flooded.values.count, 64, "the link never backed up")

    let cancelling = Task { await iterator.cancel() }
    try await Task.sleep(nanoseconds: 200_000_000)
    releaseSink.yield()
    _ = await result(of: pulled, within: 3)
    XCTAssertEqual(ticks.values, [0], "callbacks ran after the iterator was cancelled")

    server.resumeReads()
    _ = await result(of: cancelling, within: 5)
    let cancelled = await eventually(timeout: 5) { self.iteratorRequests("cancel").count == 1 }
    XCTAssertTrue(cancelled, "the producer was never cancelled")
    _ = await result(of: flood, within: 5)
  }

  func testCallToARefusedSubjectFailsWithForbiddenAtOnce() async throws {
    server.refusePublishes(to: "rpc.test.secret")
    serveEcho()
    let client = try await makeClient(requestTimeout: 3)

    let started = Date()
    do {
      _ = try await client.call("test", "secret")
      XCTFail("a refused call succeeded")
    } catch let error as RPCRemoteError {
      XCTAssertEqual(error.code, "FORBIDDEN")
      XCTAssertEqual((error.data as? [String: Any?])?["subject"] as? String, "rpc.test.secret")
      XCTAssertLessThan(Date().timeIntervalSince(started), 1)
    }
    XCTAssertEqual(server.messages(on: "rpc.test.secret").count, 1, "the refused call was retried")

    let echoed = try await client.call("test", "echo", args: ["still up"])
    XCTAssertEqual(echoed as? String, "still up")
  }

  func testStreamToARefusedSubjectFailsWithForbiddenAtOnce() async throws {
    server.refusePublishes(to: "rpc.test.secretFeed")
    let client = try await makeClient(requestTimeout: 3)

    let started = Date()
    let outcome = Task { () -> Error? in
      do {
        for try await _ in await client.callStream("test", "secretFeed") {}
        return nil
      } catch {
        return error
      }
    }
    let failure = await result(of: outcome, within: 3)
    outcome.cancel()
    guard case .some(.some(let error as RPCRemoteError)) = failure else {
      return XCTFail("the stream did not fail as forbidden: \(String(describing: failure))")
    }
    XCTAssertEqual(error.code, "FORBIDDEN")
    XCTAssertLessThan(Date().timeIntervalSince(started), 1)
    XCTAssertEqual(server.messages(on: "rpc.test.secretFeed").count, 1, "the refused stream was retried")
    XCTAssertTrue(server.messages(on: "stream.>").isEmpty, "a refused stream start was cancelled")
  }

  func testPullIteratorOnARefusedSubjectFailsWithForbiddenAtOnce() async throws {
    server.refusePublishes(to: "rpc.test.secretPull")
    let client = try await makeClient(requestTimeout: 3)

    let started = Date()
    do {
      _ = try await client.callPullIteratorWithCallback(
        "test", "secretPull", callbacks: ["onTick": { _ in }], onewayMethods: ["onTick"])
      XCTFail("a refused iterator started")
    } catch let error as RPCRemoteError {
      XCTAssertEqual(error.code, "FORBIDDEN")
      XCTAssertLessThan(Date().timeIntervalSince(started), 1)
    }
    XCTAssertEqual(server.messages(on: "rpc.test.secretPull").count, 1, "the refused init was retried")
  }

  private func makeClient(
    requestTimeout: TimeInterval = 5, pingInterval: TimeInterval? = nil
  ) async throws
    -> RPCClient
  {
    let options = RPCClientOptions(
      url: server.url,
      connId: "conn\(UInt32.random(in: 1000...9999))",
      requestTimeout: requestTimeout,
      pingInterval: pingInterval
    )
    let client = RPCClient(options: options)
    try await client.connect()
    clients.append(client)
    return client
  }

  private func iteratorRequests(_ type: String) -> [FakeMessage] {
    server.messages(on: "_rpc.iterator.>").filter {
      $0.subject.hasSuffix(".request") && FakeNatsServer.decode($0)["type"] as? String == type
    }
  }

  private func serveEcho() {
    server.onPublish("rpc.test.echo") { [server] message in
      let envelope = FakeNatsServer.decode(message)
      let args = envelope["params"] as? [Any?] ?? []
      server!.reply(to: message, ["id": envelope["id"] ?? nil, "result": args.first ?? nil] as [String: Any?])
    }
  }

  private func serveEmptyReply(_ method: String) {
    server.onPublish("rpc.test.\(method)") { [server] message in
      let envelope = FakeNatsServer.decode(message)
      server!.reply(to: message, ["id": envelope["id"] ?? nil, "result": nil] as [String: Any?])
    }
  }

  // next requests stay unanswered, like a producer waiting at the live edge
  private func servePullInit(_ method: String) {
    server.onPublish("rpc.test.\(method)") { [server] message in
      let envelope = FakeNatsServer.decode(message)
      let params = (envelope["params"] as? [Any?])?.first as? [String: Any?]
      let iteratorId = params?["__iteratorId"] as? String
      let result: [String: Any?] = ["iteratorId": iteratorId]
      server!.reply(to: message, ["id": envelope["id"] ?? nil, "result": result] as [String: Any?])
    }
  }

  private func firstEvent(
    _ wanted: RPCConnectionEvent, in events: AsyncStream<RPCConnectionEvent>, within seconds: TimeInterval
  ) async -> Bool {
    let watcher = Task { () -> Bool in
      for await event in events where event == wanted {
        return true
      }
      return false
    }
    let seen = await result(of: watcher, within: seconds)
    watcher.cancel()
    return seen ?? false
  }
}

final class Recorder<Value>: @unchecked Sendable {
  private let lock = NSLock()
  private var stored: [Value] = []

  var values: [Value] {
    lock.lock()
    defer { lock.unlock() }
    return stored
  }

  func append(_ value: Value) {
    lock.lock()
    stored.append(value)
    lock.unlock()
  }
}

final class OnceFlag: @unchecked Sendable {
  private let lock = NSLock()
  private var set = false

  func trySet() -> Bool {
    lock.lock()
    defer { lock.unlock() }
    guard !set else { return false }
    set = true
    return true
  }
}

// the value of `task`, or nil when it is still running after `seconds`
func result<T: Sendable>(of task: Task<T, Never>, within seconds: TimeInterval) async -> T? {
  let settled = OnceFlag()
  return await withCheckedContinuation { (continuation: CheckedContinuation<T?, Never>) in
    Task {
      let value = await task.value
      if settled.trySet() { continuation.resume(returning: value) }
    }
    Task {
      try? await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
      if settled.trySet() { continuation.resume(returning: nil) }
    }
  }
}

extension FakeNatsServer {
  func publishChunked(_ subject: String, _ data: Data, chunkSize: Int) {
    let transferId = UUID().uuidString
    let totalChunks = (data.count + chunkSize - 1) / chunkSize
    let header: [String: Any?] = [
      "type": "chunked", "transferId": transferId, "totalChunks": totalChunks,
      "totalSize": data.count, "chunkSize": chunkSize,
    ]
    publish(
      subject, try! RPCCodec.encode(header),
      headers: ["x-chunked-transfer": "header", "x-chunk-id": transferId])
    for index in 0..<totalChunks {
      let start = data.startIndex + index * chunkSize
      let chunk = data[start..<min(start + chunkSize, data.endIndex)]
      publish(
        subject, Data(chunk),
        headers: ["x-chunked-transfer": "chunk", "x-chunk-id": transferId, "x-chunk-index": String(index)])
    }
  }
}
