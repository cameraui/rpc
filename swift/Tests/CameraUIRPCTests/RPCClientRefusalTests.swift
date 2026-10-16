import Foundation
import XCTest

@testable import CameraUIRPC

final class RPCClientRefusalTests: XCTestCase {
  private var server: FakeWebSocketNats!
  private var clients: [RPCClient] = []
  private var watchers: [Task<Void, Never>] = []

  override func setUp() async throws {
    try await super.setUp()
    server = FakeWebSocketNats()
    try server.start()
  }

  override func tearDown() async throws {
    for client in clients {
      await client.close()
    }
    clients = []
    for watcher in watchers {
      watcher.cancel()
    }
    watchers = []
    server.stop()
    try await super.tearDown()
  }

  func testRevokedTokenFailsTheConnectWithoutRedialing() async throws {
    server.refuse(with: 4401)
    let client = makeClient()
    let events = await record(client)

    let connect = Task { () -> Error? in
      do {
        try await client.connect()
        return nil
      } catch {
        return error
      }
    }
    let failure = await result(of: connect, within: 3)
    guard case .some(.some(RPCClientError.unauthorized(let code, _))) = failure else {
      return XCTFail("connect did not fail as unauthorized: \(String(describing: failure))")
    }
    XCTAssertEqual(code, 4401)
    XCTAssertEqual(events.values, [.unauthorized])
    try await Task.sleep(nanoseconds: 1_500_000_000)
    XCTAssertEqual(server.accepted, 1)
  }

  func testTokenRefusedOnReconnectClosesTheClient() async throws {
    let client = try await connectedClient()
    let events = await record(client)

    server.refuse(with: 4401)
    server.dropLinks()

    let closed = await eventually(timeout: 3) { events.values.last == .closed }
    XCTAssertTrue(closed, "events: \(events.values)")
    XCTAssertEqual(events.values.filter { $0 == .unauthorized }.count, 1)
    try await Task.sleep(nanoseconds: 2_500_000_000)
    XCTAssertEqual(server.accepted, 2, "the client kept redialing a refused token")
    await assertNotConnected(client)
  }

  func testLinkClosedAsForbiddenClosesTheClient() async throws {
    let client = try await connectedClient()
    let events = await record(client)

    server.closeLinks(with: 4403)

    let closed = await eventually(timeout: 3) { events.values.last == .closed }
    XCTAssertTrue(closed, "events: \(events.values)")
    XCTAssertTrue(events.values.contains(.unauthorized))
    let released = await eventually(timeout: 2) { self.server.openLinks == 0 }
    XCTAssertTrue(released)
    let accepted = server.accepted
    try await Task.sleep(nanoseconds: 2_000_000_000)
    XCTAssertEqual(server.accepted, accepted)
    await assertNotConnected(client)
  }

  func testUpstreamLossKeepsReconnecting() async throws {
    let client = try await connectedClient()
    let events = await record(client)

    server.closeLinks(with: 4500)

    let reconnected = await eventually(timeout: 4) { events.values.contains(.connected) }
    XCTAssertTrue(reconnected, "events: \(events.values)")
    XCTAssertFalse(events.values.contains(.unauthorized))
  }

  private func makeClient() -> RPCClient {
    let options = RPCClientOptions(
      url: server.url,
      connId: "conn\(UInt32.random(in: 1000...9999))",
      requestTimeout: 2
    )
    let client = RPCClient(options: options)
    clients.append(client)
    return client
  }

  private func connectedClient() async throws -> RPCClient {
    let client = makeClient()
    try await client.connect()
    return client
  }

  private func record(_ client: RPCClient) async -> Recorder<RPCConnectionEvent> {
    let recorder = Recorder<RPCConnectionEvent>()
    let events = await client.statusEvents()
    watchers.append(
      Task {
        for await event in events {
          recorder.append(event)
        }
      })
    return recorder
  }

  private func assertNotConnected(_ client: RPCClient) async {
    do {
      _ = try await client.call("test", "echo", timeout: 1)
      XCTFail("a call on a refused client went through")
    } catch RPCClientError.notConnected {
    } catch {
      XCTFail("a call on a refused client failed with \(error)")
    }
  }
}
