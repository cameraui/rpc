import Foundation

extension RPCClientOptions {
  public static func cameraUI(
    serverURL: URL,
    token: String,
    connId: String = UUID().uuidString,
    pinnedCertificate: URL? = nil,
    skipHostnameVerification: Bool = false,
    skipCertificateVerification: Bool = false,
    additionalTrustRoots: [Data] = [],
    pingInterval: TimeInterval = 15  // a silently dead link shows within 45 s
  ) -> RPCClientOptions {
    var components = URLComponents(url: serverURL, resolvingAgainstBaseURL: false)!
    let insecure = components.scheme == "http" || components.scheme == "ws"
    components.scheme = insecure ? "ws" : "wss"
    if components.port == nil {
      components.port = insecure ? 80 : 443
    }
    let prefix = components.path.hasSuffix("/") ? String(components.path.dropLast()) : components.path
    components.path = prefix + "/api/proxy"
    components.queryItems = [
      URLQueryItem(name: "token", value: token),
      URLQueryItem(name: "connId", value: connId),
    ]
    return RPCClientOptions(
      url: components.url!,
      connId: connId,
      username: "secret",
      password: "secret",
      pinnedCertificate: pinnedCertificate,
      skipHostnameVerification: skipHostnameVerification,
      skipCertificateVerification: skipCertificateVerification,
      additionalTrustRoots: additionalTrustRoots,
      pingInterval: pingInterval
    )
  }

  public static func cameraUI(
    host: String,
    port: Int = 3443,
    token: String,
    connId: String = UUID().uuidString,
    pinnedCertificate: URL? = nil,
    skipHostnameVerification: Bool = false,
    skipCertificateVerification: Bool = false,
    additionalTrustRoots: [Data] = [],
    pingInterval: TimeInterval = 15
  ) -> RPCClientOptions {
    cameraUI(
      serverURL: URL(string: "https://\(host):\(port)")!,
      token: token,
      connId: connId,
      pinnedCertificate: pinnedCertificate,
      skipHostnameVerification: skipHostnameVerification,
      skipCertificateVerification: skipCertificateVerification,
      additionalTrustRoots: additionalTrustRoots,
      pingInterval: pingInterval
    )
  }
}
