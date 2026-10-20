# camera.ui - RPC

[![npm](https://img.shields.io/npm/v/@camera.ui/rpc?label=npm&logo=npm)](https://www.npmjs.com/package/@camera.ui/rpc)
[![PyPI](https://img.shields.io/pypi/v/camera-ui-rpc?label=pypi&logo=pypi&logoColor=white)](https://pypi.org/project/camera-ui-rpc/)
[![Go](https://img.shields.io/github/v/tag/cameraui/rpc?filter=go/*&label=go&logo=go&logoColor=white)](https://pkg.go.dev/github.com/cameraui/rpc/go)

A lightweight, NATS-based RPC framework powering communication across the camera.ui ecosystem. Supports request/response, streaming, bidirectional channels and callbacks.

Available for three runtimes:

| Runtime | Package                               |
| ------- | ------------------------------------- |
| Node    | `@camera.ui/rpc` (`./node`)           |
| Go      | `github.com/cameraui/rpc/go` (`./go`) |
| Python  | `camera-ui-rpc` (`./python`)          |

## Binary frames

A message without large binaries is plain msgpack. A message that holds a binary value (Uint8Array, Buffer, ArrayBuffer, any typed array or DataView) of 16 KiB or more goes out as a CUIB frame:

```
[4 bytes ASCII "CUIB"][u32 LE envelope length][envelope: msgpack][bin0][bin1]...
```

- In the envelope every extracted binary is replaced by the map `{ "__cui_bin__": <index>, "l": <byteLength> }`. The index is the position in traversal order, the length is there for validation.
- The segments follow the envelope back to back in index order. The frame describes itself, so it survives chunking, which loses the NATS headers on reassembly.
- Decoders return views into the received buffer, not copies. A consumer that keeps the bytes beyond the message copies them itself.
- No multi-byte msgpack encoding starts with `0x43` (`C`), so the magic never collides with a plain msgpack payload.
- Node, Python, Go and Swift extract at the same 16 KiB threshold.

---

_Part of the camera.ui ecosystem - A comprehensive camera management solution._
