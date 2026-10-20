import Foundation

public struct RPCRemoteError: Error, CustomStringConvertible {
  public let code: String
  public let message: String
  public let data: Any?

  public init(code: String, message: String, data: Any? = nil) {
    self.code = code
    self.message = message
    self.data = data
  }

  init(fromEnvelope envelope: [String: Any?]?) {
    self.init(
      code: envelope?["code"] as? String ?? "UNKNOWN",
      message: envelope?["message"] as? String ?? "unknown error",
      data: envelope?["data"] ?? nil
    )
  }

  public var description: String { "\(code): \(message)" }

  static func forbidden(_ subject: String) -> RPCRemoteError {
    RPCRemoteError(
      code: RPCErrorCode.forbidden, message: "Permission denied for \"\(subject)\"",
      data: ["subject": subject] as [String: Any?])
  }
}

public enum RPCErrorCode {
  public static let forbidden = "FORBIDDEN"
}

public enum RPCClientError: Error, CustomStringConvertible {
  case notConnected
  case timeout(String)
  case noResponders(String)
  case invalidResponse(String)
  case unauthorized(code: UInt16, reason: String)

  public var description: String {
    switch self {
    case .notConnected: return "client is not connected"
    case .timeout(let subject): return "request timed out: \(subject)"
    case .noResponders(let subject): return "no responders: \(subject)"
    case .invalidResponse(let reason): return "invalid response: \(reason)"
    case .unauthorized(let code, let reason): return "connection refused: \(code) \(reason)"
    }
  }
}
