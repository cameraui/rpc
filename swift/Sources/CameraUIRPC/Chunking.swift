import Foundation

import Nats

struct ChunkError: Error, CustomStringConvertible {
  let reason: String

  var description: String { reason }
}

// one per client, so a receiver per subscription does not multiply what a
// sender can make it allocate
final class TransferBudget: @unchecked Sendable {
  let limit: Int

  private let lock = NSLock()
  private var used = 0
  private var epoch = 0

  init(limit: Int) {
    self.limit = limit > 0 ? limit : ChunkReceiver.defaultMaxTransferSize
  }

  var currentEpoch: Int {
    lock.lock()
    defer { lock.unlock() }
    return epoch
  }

  // the epoch the bytes count in, nil when they do not fit
  func reserve(_ bytes: Int) -> Int? {
    lock.lock()
    defer { lock.unlock() }
    guard used + bytes <= limit else { return nil }
    used += bytes
    return epoch
  }

  func release(_ bytes: Int, epoch: Int) {
    lock.lock()
    defer { lock.unlock() }
    guard epoch == self.epoch else { return }
    used = max(0, used - bytes)
  }

  // a lost link breaks every transfer in flight: their bytes stop counting at
  // once, each receiver drops them with its next message or sweep
  func reset() {
    lock.lock()
    defer { lock.unlock() }
    epoch += 1
    used = 0
  }
}

final class ChunkAssembler {
  let id: String
  let totalSize: Int
  let totalChunks: Int
  let chunkSize: Int

  private var buffer: Data?
  private var received: Set<Int> = []

  init(id: String, totalSize: Int, totalChunks: Int, chunkSize: Int) {
    self.id = id
    self.totalSize = totalSize
    self.totalChunks = totalChunks
    self.chunkSize = chunkSize
  }

  var isAllocated: Bool { buffer != nil }

  func problem(index: Int, length: Int) -> String? {
    guard index >= 0, index < totalChunks else {
      return "chunk index \(index) out of range for \(totalChunks) chunks"
    }
    let expected = index == totalChunks - 1 ? totalSize - index * chunkSize : chunkSize
    guard length == expected else {
      return "chunk \(index) has \(length) bytes, expected \(expected)"
    }
    return nil
  }

  // the caller checks problem() first, a repeated index is ignored
  func add(index: Int, data: Data) -> Data? {
    guard !received.contains(index) else { return nil }
    if buffer == nil {
      buffer = Data(count: totalSize)
    }
    let offset = index * chunkSize
    buffer!.replaceSubrange(offset..<(offset + data.count), with: data)
    received.insert(index)
    return received.count == totalChunks ? buffer : nil
  }
}

// a message above the NATS max payload arrives as a msgpack header
// {type:'chunked',transferId,totalChunks,totalSize,chunkSize} followed by raw
// chunks, correlated via the x-chunked-transfer / x-chunk-id / x-chunk-index
// headers
final class ChunkReceiver: @unchecked Sendable {
  static let defaultMaxTransferSize = 1 << 30
  static let maxChunkSize = 64 << 20
  static let transferHeader = try! NatsHeaderName("x-chunked-transfer")
  static let idHeader = try! NatsHeaderName("x-chunk-id")
  static let indexHeader = try! NatsHeaderName("x-chunk-index")

  private struct Transfer {
    let assembler: ChunkAssembler
    let epoch: Int
    var reservedEpoch: Int?
    var lastActivity: TimeInterval
  }

  private let budget: TransferBudget
  private let maxTransfers: Int?
  private let staleAfter: TimeInterval
  private let lock = NSLock()
  private var transfers: [String: Transfer] = [:]
  private var sweepTask: Task<Void, Never>?

  init(budget: TransferBudget, maxTransfers: Int? = 32, staleAfter: TimeInterval = 30) {
    self.budget = budget
    self.maxTransfers = maxTransfers
    self.staleAfter = staleAfter
  }

  deinit {
    sweepTask?.cancel()
  }

  var count: Int {
    lock.lock()
    defer { lock.unlock() }
    return transfers.count
  }

  // nil while a transfer is incomplete and for refused parts
  func receive(_ message: NatsMessage) -> Data? {
    guard let payload = message.payload else { return nil }
    switch Self.header(message, Self.transferHeader) {
    case "header":
      guard let id = Self.header(message, Self.idHeader) else { return nil }
      try? start(key: id, transferId: id, header: payload)
      return nil
    case "chunk":
      guard let id = Self.header(message, Self.idHeader) else { return nil }
      return try? add(key: id, index: Self.header(message, Self.indexHeader), data: payload)
    default:
      return payload
    }
  }

  func start(key: String, transferId: String?, header: Data) throws {
    guard
      let fields = (try? RPCCodec.decode(header)) as? [String: Any?],
      let id = fields["transferId"] as? String, id == transferId
    else { throw ChunkError(reason: "invalid chunk header") }
    let totalSize = fields["totalSize"] as? Int ?? -1
    let totalChunks = fields["totalChunks"] as? Int ?? -1
    let chunkSize =
      fields["chunkSize"] as? Int
      ?? (totalChunks > 0 && totalSize > 0 ? (totalSize + totalChunks - 1) / totalChunks : -1)

    guard chunkSize > 0, chunkSize <= Self.maxChunkSize else {
      throw ChunkError(reason: "chunk size \(chunkSize) out of range")
    }
    guard totalSize > 0, totalSize <= budget.limit else {
      throw ChunkError(reason: "transfer size \(totalSize) out of range (max \(budget.limit))")
    }
    guard totalChunks == (totalSize - 1) / chunkSize + 1 else {
      throw ChunkError(reason: "\(totalChunks) chunks do not match \(totalSize) bytes in chunks of \(chunkSize)")
    }

    let epoch = budget.currentEpoch
    lock.lock()
    defer { lock.unlock() }
    dropLostLocked(epoch: epoch)
    if transfers[key] == nil, let maxTransfers, transfers.count >= maxTransfers {
      throw ChunkError(reason: "too many concurrent chunked transfers (\(maxTransfers))")
    }
    removeLocked(key)
    transfers[key] = Transfer(
      assembler: ChunkAssembler(id: id, totalSize: totalSize, totalChunks: totalChunks, chunkSize: chunkSize),
      epoch: epoch,
      lastActivity: Self.now()
    )
    armSweepLocked()
  }

  // a chunk that does not fit throws and drops its transfer
  func add(key: String, index rawIndex: String?, data: Data) throws -> Data? {
    let epoch = budget.currentEpoch
    lock.lock()
    defer { lock.unlock() }
    dropLostLocked(epoch: epoch)
    guard var transfer = transfers[key] else { return nil }

    guard let index = Self.parseIndex(rawIndex) else {
      removeLocked(key)
      throw ChunkError(reason: "invalid chunk index \(rawIndex ?? "nil")")
    }
    if let problem = transfer.assembler.problem(index: index, length: data.count) {
      removeLocked(key)
      throw ChunkError(reason: problem)
    }
    if !transfer.assembler.isAllocated {
      guard let reserved = budget.reserve(transfer.assembler.totalSize) else {
        removeLocked(key)
        throw ChunkError(reason: "transfers in progress would hold more than \(budget.limit) bytes")
      }
      transfer.reservedEpoch = reserved
    }
    transfer.lastActivity = Self.now()
    transfers[key] = transfer

    guard let complete = transfer.assembler.add(index: index, data: data) else { return nil }
    removeLocked(key)
    return complete
  }

  func discard(_ key: String) {
    lock.lock()
    defer { lock.unlock() }
    removeLocked(key)
  }

  func dispose() {
    lock.lock()
    defer { lock.unlock() }
    for key in Array(transfers.keys) {
      removeLocked(key)
    }
  }

  // senders write the index with toString, str or strconv.Itoa: a sign, a
  // blank or more than 15 digits is never a real index
  static func parseIndex(_ raw: String?) -> Int? {
    guard let raw, !raw.isEmpty, raw.utf8.count <= 15, raw.utf8.allSatisfy({ $0 >= 48 && $0 <= 57 }) else {
      return nil
    }
    return Int(raw)
  }

  static func header(_ message: NatsMessage, _ name: NatsHeaderName) -> String? {
    message.headers?.get(name)?.description
  }

  private func removeLocked(_ key: String) {
    guard let transfer = transfers.removeValue(forKey: key) else { return }
    if let reservedEpoch = transfer.reservedEpoch {
      budget.release(transfer.assembler.totalSize, epoch: reservedEpoch)
    }
    if transfers.isEmpty {
      sweepTask?.cancel()
      sweepTask = nil
    }
  }

  private func dropLostLocked(epoch: Int) {
    for (key, transfer) in transfers where transfer.epoch != epoch {
      removeLocked(key)
    }
  }

  private func armSweepLocked() {
    guard sweepTask == nil, let oldest = transfers.values.map(\.lastActivity).min() else { return }
    let delay = max(0, oldest + staleAfter - Self.now()) + 0.001
    sweepTask = Task { [weak self] in
      try? await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
      guard !Task.isCancelled else { return }
      self?.sweep()
    }
  }

  private func sweep() {
    let epoch = budget.currentEpoch
    lock.lock()
    defer { lock.unlock() }
    sweepTask = nil
    dropLostLocked(epoch: epoch)
    let cutoff = Self.now() - staleAfter
    for (key, transfer) in transfers where transfer.lastActivity <= cutoff {
      removeLocked(key)
    }
    armSweepLocked()
  }

  private static func now() -> TimeInterval {
    ProcessInfo.processInfo.systemUptime
  }
}
