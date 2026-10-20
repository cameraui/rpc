import Foundation

import CameraUIRPC

let env = ProcessInfo.processInfo.environment
guard let host = env["CUI_HOST"], let token = env["CUI_TOKEN"] else {
  print("usage: CUI_HOST=… CUI_TOKEN=cui_… [CUI_PORT=3443] [CUI_CERT=server.pem] smoke-test")
  exit(2)
}
let port = env["CUI_PORT"].flatMap(Int.init) ?? 3443

let options = RPCClientOptions.cameraUI(
  host: host,
  port: port,
  token: token,
  pinnedCertificate: env["CUI_CERT"].map { URL(fileURLWithPath: $0) },
  skipHostnameVerification: true
)

let client = RPCClient(options: options)

func run() async throws {
  print("connecting to wss://\(host):\(port)/api/proxy …")
  try await client.connect()
  print("connected")

  var plugin = try await client.call("coreManager.rpc", "getPlugin", args: ["camera-ui-nvr"])
  if plugin == nil {
    plugin = try await client.call("coreManager.rpc", "getPlugin", args: ["@camera.ui/camera-ui-nvr"])
  }
  if plugin == nil, let all = try await client.call("coreManager.rpc", "getPlugins") as? [Any?] {
    let names = all.compactMap { ($0 as? [String: Any?])?["name"] as? String }
    print("installed plugins: \(names.joined(separator: ", "))")
    plugin = all.first { name in
      ((name as? [String: Any?])?["name"] as? String)?.contains("nvr") == true
    } ?? nil
  }
  guard let pluginMap = plugin as? [String: Any?], let pluginId = pluginMap["id"] as? String else {
    print("getPlugin returned: \(String(describing: plugin))")
    print("NVR plugin not found, skipping event calls")
    return
  }
  print("nvr plugin id: \(pluginId)")

  let events = try await client.call(
    "plugin.\(pluginId).child.rpc",
    "getEvents",
    args: [["limit": 5] as [String: Any?]]
  )
  if let map = events as? [String: Any?], let list = map["events"] as? [Any?] {
    print("getEvents: \(list.count) events")
    for entry in list.prefix(5) {
      guard let event = entry as? [String: Any?] else { continue }
      let camera = event["cameraId"] as? String ?? "?"
      let types = (event["types"] as? [Any?])?.compactMap { $0 as? String } ?? []
      let start = event["startTime"] as? Int ?? 0
      print("  \(camera) \(types.joined(separator: ",")) @\(start)")
    }
  } else {
    print("getEvents returned: \(String(describing: events).prefix(300))")
  }

  let stats = try await client.call("plugin.\(pluginId).child.rpc", "getStorageStats")
  print("getStorageStats: \(String(describing: stats).prefix(200))")

  let cameras = try await client.call("plugin.\(pluginId).child.rpc", "getManagedCameraIds")
  let cameraIds = (cameras as? [Any?])?.compactMap { $0 as? String } ?? []
  var playbackTarget: (cameraId: String, startMs: Double)?
  let nowMs = Date().timeIntervalSince1970 * 1000
  for cameraId in cameraIds {
    let segments = try await client.call(
      "plugin.\(pluginId).child.rpc",
      "getRecordingSegments",
      args: [cameraId, nowMs - 24 * 3600 * 1000, nowMs]
    )
    if let segment = (segments as? [Any?])?.last as? [String: Any?],
      let startMs = asDouble(segment["startTime"] ?? nil)
    {
      playbackTarget = (cameraId, startMs)
      break
    }
  }
  if let (cameraId, startMs) = playbackTarget {
    let tsUs = (startMs + 5000) * 1000
    print("nvrPlayback: camera \(cameraId) @\(Int(startMs) + 5000)ms …")
    let counters = PlaybackCounters()
    let iterator = try await client.callPullIteratorWithCallback(
      "plugin.\(pluginId).child.rpc",
      "nvrPlayback",
      callbacks: [
        "onReady": { args in await counters.ready(args.first ?? nil) },
        "onVideo": { _ in await counters.video() },
        "onBatch": { args in await counters.batch(args.first ?? nil) },
        "onAudio": { _ in await counters.audio() },
        "onNoData": { _ in print("  no recording data at ts") },
      ],
      onewayMethods: ["onReady", "onVideo", "onBatch", "onAudio", "onNoData"],
      args: [cameraId, tsUs, true, ""]
    )
    var batches = 0
    for try await _ in iterator {
      batches += 1
      if batches >= 5 { break }
    }
    await iterator.cancel()
    await counters.report(batches: batches)
  } else {
    print("nvrPlayback: no recording segments in the last 24h, skipping")
  }

  print("registering onSystemEvent callback for 5s …")
  let callbackStream = try await client.callWithCallback("plugin.\(pluginId).child.rpc", "onSystemEvent")
  let callbackTask = Task {
    var count = 0
    do {
      for try await value in callbackStream {
        print("  system event: \(String(describing: value).prefix(120))")
        count += 1
      }
    } catch {}
    return count
  }
  try? await Task.sleep(nanoseconds: 5_000_000_000)
  callbackTask.cancel()
  print("onSystemEvent: registered + cancelled, \(await callbackTask.value) pushes (zero is fine)")

  print("subscribing to camera.*.events.subject for 10s …")
  let stream = try await client.subscribe("camera.*.events.subject")
  let eventsTask = Task {
    var count = 0
    do {
      for try await message in stream {
        guard let map = message as? [String: Any?] else { continue }
        let type = map["type"] as? String ?? "?"
        let camera = (map["data"] as? [String: Any?])?["cameraId"] as? String ?? "?"
        print("  live event: \(type) \(camera)")
        count += 1
      }
    } catch {}
    return count
  }
  try? await Task.sleep(nanoseconds: 10_000_000_000)
  eventsTask.cancel()
  print("live events received: \(await eventsTask.value) (zero is fine on a quiet system)")

  await client.close()
  print("smoke OK")
}

func asDouble(_ value: Any?) -> Double? {
  switch value {
  case let number as Double: return number
  case let number as Int: return Double(number)
  case let number as UInt64: return Double(number)
  default: return nil
  }
}

actor PlaybackCounters {
  private var videoFrames = 0
  private var audioFrames = 0
  private var batchItems = 0

  func ready(_ payload: Any?) {
    guard let map = payload as? [String: Any?] else { return }
    let codec = map["codecString"] as? String ?? "?"
    let width = map["width"] as? Int ?? 0
    let height = map["height"] as? Int ?? 0
    print("  ready: \(codec) \(width)x\(height)")
  }

  func video() {
    videoFrames += 1
  }

  func audio() {
    audioFrames += 1
  }

  func batch(_ payload: Any?) {
    guard let map = payload as? [String: Any?], let items = map["items"] as? [Any?] else { return }
    batchItems += items.count
  }

  func report(batches: Int) {
    print("  \(batches) batch boundaries, \(batchItems) batched frames, \(videoFrames) video / \(audioFrames) audio single frames")
  }
}

let semaphore = DispatchSemaphore(value: 0)
Task {
  do {
    try await run()
    semaphore.signal()
    exit(0)
  } catch {
    print("smoke FAILED: \(error)")
    semaphore.signal()
    exit(1)
  }
}
semaphore.wait()
