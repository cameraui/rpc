import Foundation

import Nats

public typealias PullCallback = @Sendable ([Any?]) async -> Void

public final class PullIterator: AsyncSequence, Sendable {
  public typealias Element = Any?

  let session: PullIteratorSession

  init(session: PullIteratorSession) {
    self.session = session
  }

  deinit {
    let session = self.session
    Task { await session.cancel() }
  }

  public func makeAsyncIterator() -> Iterator {
    Iterator(session: session)
  }

  public func cancel() async {
    await session.cancel()
  }

  public struct Iterator: AsyncIteratorProtocol {
    let session: PullIteratorSession

    public func next() async throws -> Any?? {
      try await session.next()
    }
  }
}

actor PullIteratorSession {
  private struct Boundary {
    let response: [String: Any?]
    let callbacks: UInt64
  }

  private let client: RPCClient
  private let iteratorId: String
  private let requestSubject: String
  private let statusInbox: String
  private let probeInterval: TimeInterval
  private let callbacks: [String: PullCallback]
  private let prefetch: Bool
  private let callbackSub: NatsSubscription?
  private let callbackChunks: ChunkReceiver
  private var responseTask: Task<Void, Never>?
  private var callbackTask: Task<Void, Never>?
  private var probeTask: Task<Void, Never>?
  private var steps = 0
  // the step this session waits for, 0 between steps
  private var waiting = 0
  private var waitingAtLastProbe = 0
  private var queue: [Boundary] = []
  private var waiter: CheckedContinuation<Boundary, Never>?
  private var handled: UInt64 = 0
  private var pumpEnded = false
  private var callbackWaiters: [(through: UInt64, continuation: CheckedContinuation<Void, Never>)] = []
  private var ended = false
  private var endedError: Error?
  private var prefetched = false
  private var cleanedUp = false

  init(
    client: RPCClient,
    iteratorId: String,
    requestSubject: String,
    statusInbox: String,
    probeInterval: TimeInterval,
    callbacks: [String: PullCallback],
    prefetch: Bool,
    callbackSub: NatsSubscription?,
    callbackChunks: ChunkReceiver
  ) {
    self.client = client
    self.iteratorId = iteratorId
    self.requestSubject = requestSubject
    self.statusInbox = statusInbox
    self.probeInterval = probeInterval
    self.callbacks = callbacks
    self.prefetch = prefetch
    self.callbackSub = callbackSub
    self.callbackChunks = callbackChunks
  }

  func start(responses: AsyncThrowingStream<Any?, Error>) {
    startCallbackPump()
    responseTask = Task { [weak self] in
      do {
        for try await message in responses {
          guard let map = message as? [String: Any?] else { continue }
          await self?.onResponse(map)
        }
      } catch {
        await self?.onResponse([
          "type": "error",
          "error": ["code": "SUBSCRIPTION", "message": "\(error)"] as [String: Any?],
        ])
      }
    }
    probeTask = Task { [weak self, probeInterval] in
      while !Task.isCancelled {
        try? await Task.sleep(nanoseconds: UInt64(probeInterval * 1_000_000_000))
        guard let self, !Task.isCancelled else { return }
        await self.probe()
      }
    }
  }

  func next() async throws -> Any?? {
    if cleanedUp { return nil }
    if ended {
      await cleanup(sendCancel: false)
      if let error = endedError {
        endedError = nil
        throw error
      }
      return nil
    }
    if Task.isCancelled {
      await cancel()
      return nil
    }

    if prefetched {
      prefetched = false
    } else {
      do {
        try await sendNextRequest()
      } catch {
        await cleanup(sendCancel: true)
        throw error
      }
    }

    let boundary = await withTaskCancellationHandler {
      await awaitResponse()
    } onCancel: {
      Task { await self.cancel() }
    }
    let response = boundary.response

    switch response["type"] as? String {
    case "value":
      // opt-in prefetch: the server produces the next batch while this one drains
      if prefetch && !ended && !cleanedUp {
        do {
          try await sendNextRequest()
          prefetched = true
        } catch {
          prefetched = false
        }
      }
      await callbacksHandled(through: boundary.callbacks)
      return .some(response["value"] ?? nil)
    case "done":
      await callbacksHandled(through: boundary.callbacks)
      await cleanup(sendCancel: false)
      return nil
    case "cancelled":
      return nil
    case "lost":
      await cleanup(sendCancel: true)
      throw RPCRemoteError(
        code: "STREAM_ERROR",
        message: "pull session lost a message, the handler waits for a step this client already asked for")
    default:
      await cleanup(sendCancel: false)
      if let error = endedError {
        endedError = nil
        throw error
      }
      throw RPCRemoteError(fromEnvelope: response["error"] as? [String: Any?])
    }
  }

  func cancel() async {
    if cleanedUp { return }
    if let waiter {
      self.waiter = nil
      waiter.resume(returning: Boundary(response: ["type": "cancelled"], callbacks: 0))
    }
    await cleanup(sendCancel: true)
  }

  func handleNoResponders(subject: String) {
    onResponse([
      "type": "error",
      "error": ["code": "503", "message": "No responders for \(subject)"] as [String: Any?],
    ])
  }

  func handleDisconnect() {
    waiting = 0
    if !ended {
      ended = true
      endedError = RPCClientError.notConnected
    }
    if let waiter {
      self.waiter = nil
      waiter.resume(returning: Boundary(response: ["type": "disconnected"], callbacks: 0))
    }
  }

  private func onResponse(_ map: [String: Any?]) {
    var response = map
    switch map["type"] as? String {
    case "idle":
      // the answer to this step would have arrived ahead of the idle,
      // one for an earlier step crossed its answer on the way
      guard waiting > 0, map["step"] as? Int == waiting else { return }
      response = ["type": "lost"]
    case "error":
      ended = true
      endedError = RPCRemoteError(fromEnvelope: map["error"] as? [String: Any?])
    case "done":
      ended = true
    default:
      break
    }
    waiting = 0
    let boundary = Boundary(response: response, callbacks: callbackSub?.received ?? 0)
    if let waiter {
      self.waiter = nil
      waiter.resume(returning: boundary)
    } else {
      queue.append(boundary)
    }
  }

  private func sendNextRequest() async throws {
    steps += 1
    waiting = steps
    let request: [String: Any?] = ["id": iteratorId, "type": "next"]
    do {
      try await client.publishLinked(requestSubject, request, reply: statusInbox)
    } catch {
      waiting = 0
      throw error
    }
  }

  private func probe() async {
    // only a step that waited at the probe before has waited a whole interval
    let step = waiting
    let waited = step > 0 && step == waitingAtLastProbe
    waitingAtLastProbe = step
    guard waited else { return }
    // a handler with nothing open answers idle, a session that is gone shows as a 503 on the status inbox
    let request: [String: Any?] = ["id": iteratorId, "type": "probe", "step": step]
    try? await client.publishLinked(requestSubject, request, reply: statusInbox)
  }

  private func awaitResponse() async -> Boundary {
    if !queue.isEmpty { return queue.removeFirst() }
    if ended { return Boundary(response: ["type": endedError == nil ? "done" : "error"], callbacks: 0) }
    return await withCheckedContinuation { waiter = $0 }
  }

  private func startCallbackPump() {
    guard let callbackSub else { return }
    let callbacks = self.callbacks
    let chunks = callbackChunks
    callbackTask = Task { [weak self] in
      do {
        for try await message in callbackSub {
          if let payload = chunks.receive(message),
            let invocation = (try? RPCCodec.decodeMessage(payload)) as? [String: Any?],
            let method = invocation["method"] as? String,
            let handler = callbacks[method]
          {
            // cancel() can run during a handler, before the subscription closes
            guard !Task.isCancelled else { break }
            await handler(invocation["args"] as? [Any?] ?? [])
          }
          await self?.markHandled()
        }
      } catch {}
      await self?.endPump()
    }
  }

  private func callbacksHandled(through mark: UInt64) async {
    guard !pumpEnded, !cleanedUp, handled < mark else { return }
    await withCheckedContinuation { callbackWaiters.append((mark, $0)) }
  }

  private func markHandled() {
    handled += 1
    releaseCallbackWaiters()
  }

  private func endPump() {
    pumpEnded = true
    releaseCallbackWaiters()
  }

  private func releaseCallbackWaiters() {
    let open = pumpEnded || cleanedUp
    let ready = callbackWaiters.filter { open || $0.through <= handled }
    callbackWaiters.removeAll { open || $0.through <= handled }
    for waiter in ready {
      waiter.continuation.resume()
    }
  }

  private func cleanup(sendCancel: Bool) async {
    if cleanedUp { return }
    cleanedUp = true
    responseTask?.cancel()
    responseTask = nil
    callbackTask?.cancel()
    callbackTask = nil
    probeTask?.cancel()
    probeTask = nil
    releaseCallbackWaiters()
    callbackChunks.dispose()
    await client.removeStatusHandler(iteratorId)
    await client.removeIteratorSettle(iteratorId)
    try? await callbackSub?.unsubscribe()
    if sendCancel && !ended {
      ended = true
      await client.cancelSession(iteratorId)
    } else {
      await client.endSession(iteratorId)
    }
    ended = true
  }
}
