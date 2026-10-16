import Foundation
import XCTest

@testable import CameraUIRPC

final class ChunkingTests: XCTestCase {
  func testTransferIsReassembledInAnyOrder() throws {
    let receiver = ChunkReceiver(budget: TransferBudget(limit: 1 << 20))
    try receiver.start(key: "t", transferId: "t", header: header("t", totalSize: 10, chunkSize: 4))

    XCTAssertNil(try receiver.add(key: "t", index: "2", data: Data([9, 10])))
    XCTAssertNil(try receiver.add(key: "t", index: "0", data: Data([1, 2, 3, 4])))
    let whole = try receiver.add(key: "t", index: "1", data: Data([5, 6, 7, 8]))

    XCTAssertEqual(whole, Data(1...10))
    XCTAssertEqual(receiver.count, 0)
  }

  func testHeaderOutsideTheLimitsIsRefusedWithoutAllocating() {
    let budget = TransferBudget(limit: 1000)
    let receiver = ChunkReceiver(budget: budget)

    XCTAssertThrowsError(
      try receiver.start(key: "a", transferId: "a", header: header("a", totalSize: 3_000_000_000, chunkSize: 1)))
    XCTAssertThrowsError(
      try receiver.start(key: "b", transferId: "b", header: header("b", totalSize: 10, chunkSize: 0)))
    XCTAssertThrowsError(
      try receiver.start(key: "c", transferId: "c", header: header("c", totalSize: 10, chunkSize: 4, totalChunks: 2)))
    XCTAssertThrowsError(
      try receiver.start(key: "d", transferId: "other", header: header("d", totalSize: 10, chunkSize: 4)))
    XCTAssertThrowsError(
      try receiver.start(key: "e", transferId: "e", header: header("e", totalSize: 10, chunkSize: 128 << 20)))
    XCTAssertEqual(receiver.count, 0)
    XCTAssertNotNil(budget.reserve(1000), "a refused header must not hold budget")
  }

  func testChunkThatDoesNotFitDropsItsTransfer() throws {
    let receiver = ChunkReceiver(budget: TransferBudget(limit: 1 << 20))
    let misfits = [("-1", 4), ("3", 2), ("+1", 4), ("0", 3), ("2", 4), ("", 4), (String(repeating: "9", count: 16), 4)]
    for (index, length) in misfits {
      try receiver.start(key: "t", transferId: "t", header: header("t", totalSize: 10, chunkSize: 4))
      XCTAssertThrowsError(try receiver.add(key: "t", index: index, data: Data(count: length)), "index \(index)")
      XCTAssertEqual(receiver.count, 0, "index \(index) length \(length) kept the transfer")
    }
  }

  func testDuplicateChunkIsIgnored() throws {
    let receiver = ChunkReceiver(budget: TransferBudget(limit: 1 << 20))
    try receiver.start(key: "t", transferId: "t", header: header("t", totalSize: 8, chunkSize: 4))

    XCTAssertNil(try receiver.add(key: "t", index: "0", data: Data([1, 2, 3, 4])))
    XCTAssertNil(try receiver.add(key: "t", index: "0", data: Data([0, 0, 0, 0])))
    let whole = try receiver.add(key: "t", index: "1", data: Data([5, 6, 7, 8]))

    XCTAssertEqual(whole, Data(1...8))
  }

  func testChunkOfUnknownTransferIsIgnored() throws {
    let receiver = ChunkReceiver(budget: TransferBudget(limit: 1 << 20))
    XCTAssertNil(try receiver.add(key: "nobody", index: "0", data: Data(count: 4)))
  }

  func testBudgetIsSharedAndReleased() throws {
    let budget = TransferBudget(limit: 100)
    let first = ChunkReceiver(budget: budget)
    let second = ChunkReceiver(budget: budget)
    try first.start(key: "a", transferId: "a", header: header("a", totalSize: 80, chunkSize: 40))
    try second.start(key: "b", transferId: "b", header: header("b", totalSize: 80, chunkSize: 40))

    XCTAssertNil(try first.add(key: "a", index: "0", data: Data(count: 40)))
    XCTAssertThrowsError(try second.add(key: "b", index: "0", data: Data(count: 40)))

    first.discard("a")
    try second.start(key: "b", transferId: "b", header: header("b", totalSize: 80, chunkSize: 40))
    XCTAssertNil(try second.add(key: "b", index: "0", data: Data(count: 40)))
  }

  func testOpenTransfersAreCapped() throws {
    let receiver = ChunkReceiver(budget: TransferBudget(limit: 1 << 20), maxTransfers: 2)
    try receiver.start(key: "a", transferId: "a", header: header("a", totalSize: 8, chunkSize: 4))
    try receiver.start(key: "b", transferId: "b", header: header("b", totalSize: 8, chunkSize: 4))

    XCTAssertThrowsError(try receiver.start(key: "c", transferId: "c", header: header("c", totalSize: 8, chunkSize: 4)))
    XCTAssertNoThrow(try receiver.start(key: "a", transferId: "a", header: header("a", totalSize: 8, chunkSize: 4)))
  }

  func testStaleTransferIsSweptAndFreesItsBudget() async throws {
    let budget = TransferBudget(limit: 100)
    let receiver = ChunkReceiver(budget: budget, staleAfter: 0.2)
    try receiver.start(key: "t", transferId: "t", header: header("t", totalSize: 80, chunkSize: 40))
    XCTAssertNil(try receiver.add(key: "t", index: "0", data: Data(count: 40)))

    let swept = await eventually(timeout: 2) { receiver.count == 0 }
    XCTAssertTrue(swept)
    XCTAssertNotNil(budget.reserve(100))
  }

  func testLostLinkDropsTransfersInFlight() throws {
    let budget = TransferBudget(limit: 100)
    let receiver = ChunkReceiver(budget: budget)
    try receiver.start(key: "t", transferId: "t", header: header("t", totalSize: 80, chunkSize: 40))
    XCTAssertNil(try receiver.add(key: "t", index: "0", data: Data(count: 40)))

    budget.reset()

    XCTAssertNotNil(budget.reserve(100))
    XCTAssertNil(try receiver.add(key: "t", index: "1", data: Data(count: 40)))
    XCTAssertEqual(receiver.count, 0)
  }

  private func header(_ id: String, totalSize: Int, chunkSize: Int, totalChunks: Int? = nil) -> Data {
    let fields: [String: Any?] = [
      "type": "chunked",
      "transferId": id,
      "totalChunks": totalChunks ?? (chunkSize > 0 ? (totalSize + chunkSize - 1) / chunkSize : 1),
      "totalSize": totalSize,
      "chunkSize": chunkSize,
    ]
    return try! RPCCodec.encode(fields)
  }
}
