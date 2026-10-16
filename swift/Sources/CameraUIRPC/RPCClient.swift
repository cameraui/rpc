import Foundation

import Nats

public struct RPCClientOptions: Sendable {
  public var url: URL
  public var connId: String
  public var username: String?
  public var password: String?
  public var pinnedCertificate: URL?
  public var skipHostnameVerification: Bool
  public var skipCertificateVerification: Bool
  /// DER certificates trusted when the system roots reject the server. The
  /// chain must end at one of them, and the host name is not checked
  /// against them. Empty trusts the system roots only.
  public var additionalTrustRoots: [Data]
  public var requestTimeout: TimeInterval
  public var noResponderRetries: Int
  /// Seconds between keepalive PINGs. After two unanswered ones the next
  /// tick drops the link. Nil keeps the nats.swift default of 60 s.
  public var pingInterval: TimeInterval?
  /// Seconds one dial may take, TLS and WebSocket upgrade included. Nil
  /// keeps the nats.swift default of 10 s.
  public var connectTimeout: TimeInterval?
  /// Largest chunked transfer accepted, and the bytes all transfers in
  /// progress may hold together.
  public var maxTransferSize: Int

  public init(
    url: URL,
    connId: String = UUID().uuidString,
    username: String? = nil,
    password: String? = nil,
    pinnedCertificate: URL? = nil,
    skipHostnameVerification: Bool = false,
    skipCertificateVerification: Bool = false,
    additionalTrustRoots: [Data] = [],
    requestTimeout: TimeInterval = 15,
    noResponderRetries: Int = 3,
    pingInterval: TimeInterval? = nil,
    connectTimeout: TimeInterval? = nil,
    maxTransferSize: Int = 1 << 30
  ) {
    self.url = url
    self.connId = connId
    self.username = username
    self.password = password
    self.pinnedCertificate = pinnedCertificate
    self.skipHostnameVerification = skipHostnameVerification
    self.skipCertificateVerification = skipCertificateVerification
    self.additionalTrustRoots = additionalTrustRoots
    self.requestTimeout = requestTimeout
    self.noResponderRetries = noResponderRetries
    self.pingInterval = pingInterval
    self.connectTimeout = connectTimeout
    self.maxTransferSize = maxTransferSize
  }
}

/// Connection lifecycle events, mirroring the node client's `status()`
/// iterator and the python client's `reconnected_cb`. A `connected` following
/// a `disconnected` is a completed reconnect; nats.swift restores all
/// subscriptions itself before emitting it.
public enum RPCConnectionEvent: String, Sendable {
  case connected
  case disconnected
  case suspended
  /// The server turned the token or the connection down (WebSocket close
  /// 4400, 4401 or 4403). On a live link the client closes without
  /// redialing and a final `closed` follows. A refused `connect()` throws
  /// `RPCClientError.unauthorized` after this event and the streams stay open.
  case unauthorized
  case closed
}

extension RPCConnectionEvent {
  fileprivate init?(_ event: NatsEvent) {
    switch event {
    case .connected: self = .connected
    case .disconnected: self = .disconnected
    case .suspended: self = .suspended
    case .closed: self = .closed
    case .error(let error):
      guard let close = error as? NatsError.WebSocketClosed, close.refuses else { return nil }
      self = .unauthorized
    default: return nil
    }
  }
}

extension NatsError.WebSocketClosed {
  fileprivate var refuses: Bool {
    guard let code else { return false }
    return code == 4400 || code == 4401 || code == 4403
  }
}

private enum LinkSignal: Sendable {
  case event(RPCConnectionEvent)
  case refusedPublish(String)

  init?(_ event: NatsEvent) {
    if case .error(let error) = event,
      case .permissionsViolation(.publish, let subject, _)? = error as? NatsError.ServerError
    {
      self = .refusedPublish(subject)
    } else if let mapped = RPCConnectionEvent(event) {
      self = .event(mapped)
    } else {
      return nil
    }
  }
}

public actor RPCClient {
  let budget: TransferBudget
  let replyChunks: ChunkReceiver

  private static let closeFlushTimeout: TimeInterval = 0.5
  private let options: RPCClientOptions
  private var nats: NatsClient?
  private var replyTask: Task<Void, Never>?
  private var linkTask: Task<Void, Never>?
  private var linkEvents: AsyncStream<LinkSignal>.Continuation?
  private var linkUp = false
  private var linkEpoch = 0
  private var linkWaiters: [UUID: LinkWaiter] = [:]
  private var pending: [String: PendingRequest] = [:]
  private var statusHandlers: [String: @Sendable () -> Void] = [:]
  private var streamRefusals: [String: StreamRefusal] = [:]
  private var iteratorSettles: [String: @Sendable () -> Void] = [:]
  private var openSessions: [String: SessionCancel] = [:]
  private var owedCancels: [String: SessionCancel] = [:]
  private var eventContinuations: [UUID: AsyncStream<RPCConnectionEvent>.Continuation] = [:]
  private var counter: UInt64 = 0

  public init(options: RPCClientOptions) {
    self.options = options
    budget = TransferBudget(limit: options.maxTransferSize)
    // one transfer per pending call, so the calls bound it
    replyChunks = ChunkReceiver(budget: budget, maxTransfers: nil)
  }

  public func connect() async throws {
    guard nats == nil else { return }

    // no maxReconnects: nil means infinite auto-reconnect in nats.swift
    // (-1 would disable reconnecting entirely, the loop checks attempts < max)
    var natsOptions = NatsClientOptions()
      .url(options.url)
      .reconnectWait(1)
    if let pingInterval = options.pingInterval {
      natsOptions = natsOptions.pingInterval(pingInterval)
    }
    if let connectTimeout = options.connectTimeout {
      natsOptions = natsOptions.connectTimeout(connectTimeout)
    }
    if let username = options.username, let password = options.password {
      natsOptions = natsOptions.usernameAndPassword(username, password)
    }
    if let pinnedCertificate = options.pinnedCertificate {
      natsOptions = natsOptions.pinnedCertificate(pinnedCertificate)
    }
    if !options.additionalTrustRoots.isEmpty {
      natsOptions = natsOptions.additionalTrustRoots(options.additionalTrustRoots)
    }
    if options.skipHostnameVerification {
      natsOptions = natsOptions.noHostnameVerification()
    }
    if options.skipCertificateVerification {
      natsOptions = natsOptions.noCertificateVerification()
    }

    let client = natsOptions.build()
    do {
      try await client.connect()
    } catch let close as NatsError.WebSocketClosed where close.refuses {
      try? await client.close()
      broadcast(.unauthorized)
      throw RPCClientError.unauthorized(code: close.code ?? 0, reason: close.reason)
    }
    nats = client
    linkUp = true

    // a single consumer keeps the events in the order nats.swift fires them
    let (signals, sink) = AsyncStream.makeStream(of: LinkSignal.self)
    linkEvents = sink
    client.on([.connected, .disconnected, .suspended, .closed, .error]) { event in
      guard let signal = LinkSignal(event) else { return }
      sink.yield(signal)
    }
    linkTask = Task { [weak self] in
      for await signal in signals {
        switch signal {
        case .event(let event):
          await self?.handleLinkEvent(event)
        case .refusedPublish(let subject):
          await self?.failRefusedPublish(subject)
        }
      }
    }

    // one muxed inbox for all replies, routed by envelope id
    let replies = try await client.subscribe(subject: "rpc.reply.\(options.connId).>")
    replyTask = Task { [weak self] in
      do {
        for try await message in replies {
          await self?.handleReply(message)
        }
      } catch {
        await self?.failAllPending(error)
      }
    }
  }

  /// Ends every open pull iterator, callback and stream on the server before
  /// the connection closes, waiting at most half a second for the server to
  /// take the cancels.
  public func close() async {
    await shutdown(refused: false)
  }

  // a refused client is no longer heard by the server, nothing to announce
  private func shutdown(refused: Bool) async {
    let client = nats
    nats = nil
    linkEvents?.finish()
    linkEvents = nil
    linkTask?.cancel()
    linkTask = nil
    if let client, linkUp, !refused, !openSessions.isEmpty {
      await announceClose(client)
    }
    linkUp = false
    openSessions = [:]
    owedCancels = [:]
    replyTask?.cancel()
    replyTask = nil
    try? await client?.close()
    releaseLinkWaiters()
    failAllPending(RPCClientError.notConnected)
    replyChunks.dispose()
    budget.reset()
    let continuations = eventContinuations
    eventContinuations = [:]
    for continuation in continuations.values {
      if refused {
        continuation.yield(.unauthorized)
      }
      continuation.yield(.closed)
      continuation.finish()
    }
  }

  /// Streams connection lifecycle events until the client closes. Multiple
  /// streams can be open at once; each terminates with a final `closed`.
  public func statusEvents() -> AsyncStream<RPCConnectionEvent> {
    let id = UUID()
    return AsyncStream { continuation in
      continuation.onTermination = { [weak self] _ in
        Task { await self?.removeEventContinuation(id) }
      }
      eventContinuations[id] = continuation
    }
  }

  private func broadcast(_ event: RPCConnectionEvent) {
    for continuation in eventContinuations.values {
      continuation.yield(event)
    }
  }

  private func removeEventContinuation(_ id: UUID) {
    eventContinuations.removeValue(forKey: id)
  }

  /// One PING/PONG round trip: returns once the server answered, throws
  /// `timeout` when it did not within `timeout` seconds and `notConnected`
  /// when the link is down or drops meanwhile. A link that silently died
  /// answers nothing, so this tells it apart from a quiet one.
  public func flush(timeout: TimeInterval = 5) async throws {
    guard let nats, linkUp else { throw RPCClientError.notConnected }
    let answered: Bool
    do {
      answered = try await Self.within(timeout) { _ = try await nats.rtt() }
    } catch {
      // the PING went down with the link
      throw RPCClientError.notConnected
    }
    guard answered else { throw RPCClientError.timeout("flush") }
  }

  public func call(
    _ namespace: String,
    _ method: String,
    args: [Any?] = [],
    timeout: TimeInterval? = nil
  ) async throws -> Any? {
    try await callSubject("rpc.\(namespace).\(method)", args: args, timeout: timeout)
  }

  /// While the link is down the publish waits for the reconnect, at most
  /// `requestTimeout` seconds.
  public func publish(_ subject: String, _ value: Any? = nil, reply: String? = nil) async throws {
    guard nats != nil else { throw RPCClientError.notConnected }
    try await publishWhenLinked(
      RPCCodec.encodeMessage(value), subject: subject, reply: reply, timeout: options.requestTimeout)
  }

  public func subscribe(_ subject: String) async throws -> AsyncThrowingStream<Any?, Error> {
    guard let nats else { throw RPCClientError.notConnected }
    let subscription = try await nats.subscribe(subject: subject)
    let chunks = ChunkReceiver(budget: budget)

    return AsyncThrowingStream { continuation in
      let task = Task {
        defer { chunks.dispose() }
        do {
          for try await message in subscription {
            guard let payload = chunks.receive(message) else { continue }
            continuation.yield(try RPCCodec.decodeMessage(payload))
          }
          continuation.finish()
        } catch {
          continuation.finish(throwing: error)
        }
      }
      continuation.onTermination = { _ in
        task.cancel()
        Task { try? await subscription.unsubscribe() }
      }
    }
  }

  /// Callback subscription (`rpc.cb.<id>`): registers with the remote method
  /// and yields every pushed value. Terminating the stream publishes the
  /// cancel frame so the server drops the callback.
  public func callWithCallback(
    _ namespace: String,
    _ method: String,
    args: [Any?] = [],
    timeout: TimeInterval? = nil
  ) async throws -> AsyncThrowingStream<Any?, Error> {
    let id = nextId()
    let callbackSubject = "rpc.cb.\(id)"
    let messages = try await subscribe(callbackSubject)
    openSessions[id] = SessionCancel(subject: "\(callbackSubject).cancel", frame: ["id": id])

    var forward: Task<Void, Never>?
    let stream = AsyncThrowingStream<Any?, Error> { continuation in
      let task = Task {
        do {
          for try await message in messages {
            guard let map = message as? [String: Any?] else { continue }
            // error frames keep the stream alive, matching the wire contract
            if map["type"] as? String == "data" {
              continuation.yield(map["data"] ?? nil)
            }
          }
          continuation.finish()
        } catch {
          continuation.finish(throwing: error)
        }
      }
      forward = task
      continuation.onTermination = { [weak self] _ in
        task.cancel()
        Task { await self?.cancelSession(id) }
      }
    }

    let params: [String: Any?] = [
      "__callback": true,
      "__callbackSubject": callbackSubject,
      "args": args,
    ]
    do {
      _ = try await call(namespace, method, args: [params], timeout: timeout)
    } catch {
      forward?.cancel()
      throw error
    }
    return stream
  }

  public func callStream(
    _ namespace: String,
    _ method: String,
    args: [Any?] = []
  ) -> AsyncThrowingStream<Any?, Error> {
    let subject = "rpc.\(namespace).\(method)"
    let delays: [UInt64] = [500_000_000, 1_000_000_000, 2_000_000_000]
    let maxRetries = options.noResponderRetries

    return AsyncThrowingStream { continuation in
      let task = Task {
        var attempt = 0
        while true {
          let id = self.nextId()
          let streamSubject = "stream.\(subject).\(id)"
          let noResponders = SignalFlag()
          let refused = SignalFlag()

          do {
            guard let nats = self.natsClient() else { throw RPCClientError.notConnected }
            let subscription = try await nats.subscribe(subject: streamSubject)
            // the reply inbox only ever carries a 503 for stream requests:
            // wake the parked loop by completing the subscription
            self.registerStatusHandler(id) {
              noResponders.set()
              Task { try? await subscription.unsubscribe() }
            }
            self.streamRefusals[id] = StreamRefusal(subject: subject) {
              refused.set()
              Task { try? await subscription.unsubscribe() }
            }

            let params: [String: Any?] = [
              "__stream": true,
              "__streamSubject": streamSubject,
              "args": args,
            ]
            let envelope: [String: Any?] = ["id": id, "method": "stream", "params": params]
            try await self.publish(subject, envelope, reply: "rpc.reply.\(id)")
            self.openSessions[id] = SessionCancel(subject: "\(streamSubject).cancel", frame: ["id": id])

            var yielded = false
            var ended = false
            var streamError: Error?
            let chunks = ChunkReceiver(budget: self.budget)
            defer { chunks.dispose() }

            for try await message in subscription {
              guard let payloadData = chunks.receive(message) else { continue }

              guard
                let map = (try? RPCCodec.decodeMessage(payloadData)) as? [String: Any?],
                map["id"] as? String == id
              else { continue }

              switch map["type"] as? String {
              case "data":
                yielded = true
                continuation.yield(map["data"] ?? nil)
              case "end":
                ended = true
              case "error":
                streamError = RPCRemoteError(fromEnvelope: map["error"] as? [String: Any?])
                ended = true
              default:
                break
              }
              if ended { break }
            }

            self.removeStatusHandler(id)
            self.streamRefusals.removeValue(forKey: id)
            try? await subscription.unsubscribe()

            if refused.isSet && !ended {
              self.openSessions.removeValue(forKey: id)
              continuation.finish(throwing: RPCRemoteError.forbidden(subject))
              return
            }
            if ended {
              self.openSessions.removeValue(forKey: id)
            }
            if noResponders.isSet && !yielded {
              self.openSessions.removeValue(forKey: id)
              attempt += 1
              if attempt >= maxRetries {
                continuation.finish(throwing: RPCClientError.noResponders(subject))
                return
              }
              try await Task.sleep(nanoseconds: delays[min(attempt - 1, delays.count - 1)])
              continue
            }
            if let streamError {
              continuation.finish(throwing: streamError)
              return
            }
            if !ended {
              // consumer stopped or the subscription died mid-stream
              await self.cancelSession(id)
            }
            continuation.finish()
            return
          } catch {
            self.removeStatusHandler(id)
            self.streamRefusals.removeValue(forKey: id)
            await self.cancelSession(id)
            continuation.finish(throwing: error)
            return
          }
        }
      }
      continuation.onTermination = { _ in
        task.cancel()
      }
    }
  }

  public func callPullIterator(
    _ namespace: String,
    _ method: String,
    args: [Any?] = []
  ) async throws -> PullIterator {
    try await makePullIterator(
      subject: "rpc.\(namespace).\(method)",
      callbacks: [:],
      onewayMethods: nil,
      args: args,
      prefetch: false
    )
  }

  public func callPullIteratorWithCallback(
    _ namespace: String,
    _ method: String,
    callbacks: [String: PullCallback],
    onewayMethods: [String] = [],
    args: [Any?] = [],
    prefetch: Bool = false
  ) async throws -> PullIterator {
    try await makePullIterator(
      subject: "rpc.\(namespace).\(method)",
      callbacks: callbacks,
      onewayMethods: onewayMethods,
      args: args,
      prefetch: prefetch
    )
  }

  func callSubject(_ subject: String, args: [Any?], timeout: TimeInterval? = nil) async throws -> Any? {
    var attempt = 0
    var delay: UInt64 = 300_000_000

    while true {
      do {
        return try await callOnce(subject: subject, args: args, timeout: timeout ?? options.requestTimeout)
      } catch RPCClientError.noResponders {
        attempt += 1
        guard attempt < options.noResponderRetries else {
          throw RPCClientError.noResponders(subject)
        }
        try await Task.sleep(nanoseconds: delay)
        delay *= 2
      }
    }
  }

  func natsClient() -> NatsClient? {
    nats
  }

  func registerStatusHandler(_ id: String, _ handler: @escaping @Sendable () -> Void) {
    statusHandlers[id] = handler
  }

  func removeStatusHandler(_ id: String) {
    statusHandlers.removeValue(forKey: id)
  }

  func registerIteratorSettle(_ id: String, _ settle: @escaping @Sendable () -> Void) {
    iteratorSettles[id] = settle
  }

  func removeIteratorSettle(_ id: String) {
    iteratorSettles.removeValue(forKey: id)
  }

  // the server-side session ended on its own, nothing to cancel
  func endSession(_ id: String) {
    openSessions.removeValue(forKey: id)
  }

  func cancelSession(_ id: String) async {
    guard let session = openSessions.removeValue(forKey: id) else { return }
    await sendCancel(id, session)
  }

  // pull requests do not wait for a reconnect, the iterator ends with the link
  func publishLinked(_ subject: String, _ value: Any?, reply: String?) async throws {
    guard let nats, linkUp else { throw RPCClientError.notConnected }
    do {
      try await nats.publish(RPCCodec.encodeMessage(value), subject: subject, reply: reply)
    } catch NatsError.ClientError.invalidConnection {
      throw RPCClientError.notConnected
    }
  }

  // ids double as reply-subject suffixes: rpc.reply.<connId>.<unique>
  func nextId() -> String {
    counter += 1
    let millis = UInt64(Date().timeIntervalSince1970 * 1000)
    let random = UInt32.random(in: 0x1000...0xffff)
    return "\(options.connId).\(millis)-\(String(random, radix: 36))\(String(counter, radix: 36))"
  }

  private func makePullIterator(
    subject: String,
    callbacks: [String: PullCallback],
    onewayMethods: [String]?,
    args: [Any?],
    prefetch: Bool
  ) async throws -> PullIterator {
    guard let nats else { throw RPCClientError.notConnected }

    let iteratorId = nextId()
    let requestSubject = "_rpc.iterator.\(iteratorId).request"
    let responseSubject = "_rpc.iterator.\(iteratorId).response"
    // 503 status inbox for `next` requests, served by the muxed reply inbox:
    // iteratorId starts with connId, so this subject falls under the mux
    // wildcard. Real responses keep arriving on responseSubject.
    let statusInbox = "rpc.reply.\(iteratorId)"

    var callbackSub: NatsSubscription?
    let params: [String: Any?]
    if let onewayMethods {
      let callbackSubject = "_rpc.cb.\(iteratorId)"
      callbackSub = try await nats.subscribe(subject: callbackSubject)
      params = [
        "__pullCallback": true,
        "__iteratorId": iteratorId,
        "__callbackSubject": callbackSubject,
        "__callbackMethods": Array(callbacks.keys),
        "__onewayMethods": onewayMethods,
        "args": args,
      ]
    } else {
      params = ["__pullIterator": true, "__iteratorId": iteratorId, "args": args]
    }

    let cancel = SessionCancel(subject: requestSubject, frame: ["id": iteratorId, "type": "cancel"])
    let initResult: Any?
    do {
      initResult = try await callSubject(subject, args: [params])
    } catch {
      try? await callbackSub?.unsubscribe()
      // a late init reply leaves a session behind
      if case RPCClientError.timeout = error {
        await sendCancel(iteratorId, cancel)
      }
      throw error
    }
    guard
      let initMap = initResult as? [String: Any?],
      initMap["iteratorId"] as? String == iteratorId
    else {
      try? await callbackSub?.unsubscribe()
      throw RPCClientError.invalidResponse("failed to initialize pull iterator")
    }

    let responses: AsyncThrowingStream<Any?, Error>
    do {
      responses = try await subscribe(responseSubject)
    } catch {
      try? await callbackSub?.unsubscribe()
      await sendCancel(iteratorId, cancel)
      throw error
    }
    openSessions[iteratorId] = cancel
    let session = PullIteratorSession(
      client: self,
      iteratorId: iteratorId,
      requestSubject: requestSubject,
      statusInbox: statusInbox,
      callbacks: callbacks,
      prefetch: prefetch,
      callbackSub: callbackSub,
      callbackChunks: ChunkReceiver(budget: budget)
    )
    await session.start(responses: responses)
    registerStatusHandler(iteratorId) { [weak session] in
      Task { await session?.handleNoResponders(subject: subject) }
    }
    registerIteratorSettle(iteratorId) { [weak session] in
      Task { await session?.handleDisconnect() }
    }
    return PullIterator(session: session)
  }

  private func callOnce(subject: String, args: [Any?], timeout: TimeInterval) async throws -> Any? {
    guard nats != nil else { throw RPCClientError.notConnected }

    let id = nextId()
    let replySubject = "rpc.reply.\(id)"
    let envelope: [String: Any?] = ["id": id, "method": "call", "params": args]
    let payload = try RPCCodec.encodeMessage(envelope)

    return try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Any?, Error>) in
      let timeoutTask = Task {
        try? await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
        guard !Task.isCancelled else { return }
        self.settle(id: id, with: .failure(RPCClientError.timeout(subject)))
      }
      pending[id] = PendingRequest(subject: subject, continuation: continuation, timeout: timeoutTask)

      Task {
        do {
          try await self.publishWhenLinked(
            payload, subject: subject, reply: replySubject, timeout: timeout, callId: id)
        } catch {
          self.settle(id: id, with: .failure(error))
        }
      }
    }
  }

  // nats-server refused a publish of this connection: the calls and stream
  // starts waiting on that subject would only time out, nothing retries them
  private func failRefusedPublish(_ subject: String) {
    for (id, entry) in pending where entry.subject == subject {
      settle(id: id, with: .failure(RPCRemoteError.forbidden(subject)))
    }
    for refusal in streamRefusals.values where refusal.subject == subject {
      refusal.refuse()
    }
  }

  private func settle(id: String, with result: Result<Any?, Error>) {
    guard let entry = pending.removeValue(forKey: id) else { return }
    entry.timeout.cancel()
    if let transferKey = entry.transferKey {
      replyChunks.discard(transferKey)
    }
    entry.continuation.resume(with: result)
  }

  private func handleLinkEvent(_ event: RPCConnectionEvent) async {
    switch event {
    case .connected:
      linkUp = true
      linkEpoch += 1
      releaseLinkWaiters()
      await sendOwedCancels()
    case .disconnected, .suspended:
      linkUp = false
      linkLost()
    case .closed:
      linkUp = false
    case .unauthorized:
      // redialing with this token only meets the same refusal
      await shutdown(refused: true)
      return
    }
    broadcast(event)
  }

  // nothing in flight on the dropped link arrives anymore: chunked transfers
  // are dropped and fail their calls, and parked pull iterators end with a
  // retryable notConnected while their producers get a cancel once the link is back
  private func linkLost() {
    budget.reset()
    for (id, entry) in pending where entry.transferKey != nil {
      settle(id: id, with: .failure(RPCClientError.notConnected))
    }
    replyChunks.dispose()
    let settles = iteratorSettles
    iteratorSettles = [:]
    for (id, settle) in settles {
      statusHandlers.removeValue(forKey: id)
      if let session = openSessions.removeValue(forKey: id) {
        owedCancels[id] = session
      }
      settle()
    }
  }

  // nats.swift refuses writes while the link is down, so publishes wait for
  // the reconnect, bounded by `timeout`
  private func publishWhenLinked(
    _ payload: Data, subject: String, reply: String?, timeout: TimeInterval, callId: String? = nil
  ) async throws {
    let deadline = ProcessInfo.processInfo.systemUptime + timeout
    while true {
      guard let nats else { throw RPCClientError.notConnected }
      if let callId, pending[callId] == nil { return }
      let remaining = deadline - ProcessInfo.processInfo.systemUptime
      guard linkUp else {
        guard remaining > 0 else { throw RPCClientError.timeout(subject) }
        await waitForLink(timeout: remaining)
        continue
      }
      let epoch = linkEpoch
      do {
        try await nats.publish(payload, subject: subject, reply: reply)
        return
      } catch NatsError.ClientError.invalidConnection {
        // refused between the drop and its disconnected event
        guard remaining > 0 else { throw RPCClientError.timeout(subject) }
        if epoch == linkEpoch, nats === self.nats {
          linkUp = false
        }
      }
    }
  }

  // returns once the link is back, the client closed or `timeout` passed
  private func waitForLink(timeout: TimeInterval) async {
    guard !linkUp, nats != nil else { return }
    let id = UUID()
    await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
      let timer = Task { [weak self] in
        try? await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
        await self?.releaseLinkWaiter(id)
      }
      linkWaiters[id] = LinkWaiter(continuation: continuation, timer: timer)
    }
  }

  private func releaseLinkWaiter(_ id: UUID) {
    linkWaiters.removeValue(forKey: id)?.continuation.resume()
  }

  private func releaseLinkWaiters() {
    let waiters = linkWaiters
    linkWaiters = [:]
    for waiter in waiters.values {
      waiter.timer.cancel()
      waiter.continuation.resume()
    }
  }

  // a cancel made while the link is down goes out after the reconnect
  private func sendCancel(_ id: String, _ session: SessionCancel) async {
    guard let frame = try? RPCCodec.encodeMessage(session.frame) else { return }
    while linkUp, let nats {
      let epoch = linkEpoch
      do {
        try await nats.publish(frame, subject: session.subject)
        return
      } catch NatsError.ClientError.invalidConnection {
        if epoch == linkEpoch, nats === self.nats {
          linkUp = false
        }
      } catch {
        return
      }
    }
    if nats != nil {
      owedCancels[id] = session
    }
  }

  private func sendOwedCancels() async {
    let owed = owedCancels
    owedCancels = [:]
    for (id, session) in owed {
      await sendCancel(id, session)
    }
  }

  // once the server answered the PING it has every cancel published before it
  private func announceClose(_ client: NatsClient) async {
    for session in openSessions.values {
      guard let frame = try? RPCCodec.encodeMessage(session.frame) else { continue }
      try? await client.publish(frame, subject: session.subject)
    }
    _ = try? await Self.within(Self.closeFlushTimeout) { _ = try await client.rtt() }
  }

  // true when `operation` finished in time; the loser is not awaited, a PING
  // on a dead link only completes when the connection closes
  private static func within(
    _ seconds: TimeInterval, _ operation: @escaping @Sendable () async throws -> Void
  ) async throws -> Bool {
    let settled = SignalFlag()
    return try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Bool, Error>) in
      let timer = Task {
        try? await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
        if settled.trySet() { continuation.resume(returning: false) }
      }
      Task {
        do {
          try await operation()
          if settled.trySet() { continuation.resume(returning: true) }
        } catch {
          if settled.trySet() { continuation.resume(throwing: error) }
        }
        timer.cancel()
      }
    }
  }

  private func failAllPending(_ error: Error) {
    let waiting = pending
    pending = [:]
    for entry in waiting.values {
      entry.timeout.cancel()
      entry.continuation.resume(throwing: error)
    }

    let settles = iteratorSettles
    iteratorSettles = [:]
    statusHandlers = [:]
    for settle in settles.values {
      settle()
    }
  }

  private func handleReply(_ message: NatsMessage) {
    let id = String(message.subject.dropFirst("rpc.reply.".count))

    // no-responder statuses carry no payload, route by subject suffix
    if message.status == .noResponders {
      if pending[id] != nil {
        settle(id: id, with: .failure(RPCClientError.noResponders(message.subject)))
      } else if let handler = statusHandlers[id] {
        handler()
      }
      return
    }

    guard let payload = message.payload else { return }

    let chunkType = ChunkReceiver.header(message, ChunkReceiver.transferHeader)
    guard chunkType == "header" || chunkType == "chunk" else {
      route(payloadData: payload)
      return
    }
    // a chunked reply nobody waits for is dropped, one that cannot be
    // assembled fails its call at once instead of at the timeout
    guard var entry = pending[id], let chunkId = ChunkReceiver.header(message, ChunkReceiver.idHeader) else { return }
    let transferKey = "\(id)|\(chunkId)"
    do {
      if chunkType == "header" {
        if let previous = entry.transferKey, previous != transferKey {
          replyChunks.discard(previous)
        }
        entry.transferKey = transferKey
        pending[id] = entry
        try replyChunks.start(key: transferKey, transferId: chunkId, header: payload)
      } else if let assembled = try replyChunks.add(
        key: transferKey, index: ChunkReceiver.header(message, ChunkReceiver.indexHeader), data: payload)
      {
        route(payloadData: assembled, callId: id)
      }
    } catch {
      let reason = "chunked reply to \(entry.subject) failed: \(error)"
      settle(id: id, with: .failure(RPCClientError.invalidResponse(reason)))
    }
  }

  private func route(payloadData: Data, callId: String? = nil) {
    guard
      let envelope = (try? RPCCodec.decodeMessage(payloadData)) as? [String: Any?],
      let id = envelope["id"] as? String
    else {
      if let callId, let entry = pending[callId] {
        settle(id: callId, with: .failure(RPCClientError.invalidResponse("undecodable reply to \(entry.subject)")))
      }
      return
    }

    if let errorMap = envelope["error"] as? [String: Any?] {
      settle(id: id, with: .failure(RPCRemoteError(fromEnvelope: errorMap)))
      return
    }
    settle(id: id, with: .success(envelope["result"] ?? nil))
  }
}

private struct PendingRequest {
  let subject: String
  let continuation: CheckedContinuation<Any?, Error>
  let timeout: Task<Void, Never>
  var transferKey: String?
}

private struct LinkWaiter {
  let continuation: CheckedContinuation<Void, Never>
  let timer: Task<Void, Never>
}

private struct SessionCancel {
  let subject: String
  let frame: [String: Any?]
}

private struct StreamRefusal {
  let subject: String
  let refuse: @Sendable () -> Void
}

final class SignalFlag: @unchecked Sendable {
  private let lock = NSLock()
  private var value = false

  var isSet: Bool {
    lock.lock()
    defer { lock.unlock() }
    return value
  }

  func set() {
    lock.lock()
    defer { lock.unlock() }
    value = true
  }

  // false when it was set already
  func trySet() -> Bool {
    lock.lock()
    defer { lock.unlock() }
    guard !value else { return false }
    value = true
    return true
  }
}
