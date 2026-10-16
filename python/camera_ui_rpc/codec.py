"""MessagePack encoding/decoding using ormsgpack for better performance."""

import struct
from datetime import date, datetime, time
from enum import Enum
from typing import Any

import ormsgpack

# Precomputed option masks — avoids re-evaluating the flag OR on every call.
# OPT_DATETIME_AS_TIMESTAMP_EXT: (De)serialize datetime as MessagePack timestamp extension
# OPT_NON_STR_KEYS: Allow non-string dict keys
# OPT_PASSTHROUGH_ENUM: Pass enums to default handler
_PACK_OPTS: int = (
    ormsgpack.OPT_NON_STR_KEYS | ormsgpack.OPT_DATETIME_AS_TIMESTAMP_EXT | ormsgpack.OPT_PASSTHROUGH_ENUM
)
_UNPACK_OPTS: int = ormsgpack.OPT_DATETIME_AS_TIMESTAMP_EXT | ormsgpack.OPT_NON_STR_KEYS


def _default_handler(obj: Any) -> Any:
    """Default handler for custom types."""
    if isinstance(obj, Enum):
        return obj.value
    # date and time objects need special handling
    if isinstance(obj, date) and not isinstance(obj, datetime):
        # Convert date to datetime at midnight for timestamp encoding
        return datetime.combine(obj, time.min)
    if isinstance(obj, time):
        # For time objects, return as string
        return obj.isoformat()
    # Let ormsgpack handle other types
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def encode(data: Any) -> bytes:
    """
    Encode data to MessagePack binary format using ormsgpack.

    Args:
        data: Any serializable data

    Returns:
        MessagePack encoded binary data
    """
    # Use ormsgpack with proper options for cross-language compatibility
    return ormsgpack.packb(data, default=_default_handler, option=_PACK_OPTS)


def _ext_hook(_code: int, _data: bytes) -> Any:
    """Handle MessagePack extension types from other languages."""
    # Handle undefined extension type (from msgpackr when encodeUndefinedAsNil is disabled)
    # Extension type codes vary by implementation, return None for unknown types
    return None


def decode(data: bytes | bytearray | memoryview) -> Any:
    """
    Decode MessagePack binary data using ormsgpack.

    Args:
        data: MessagePack binary data

    Returns:
        Decoded data
    """
    # ext_hook: Handle extension types from Node.js msgpackr
    return ormsgpack.unpackb(data, option=_UNPACK_OPTS, ext_hook=_ext_hook)


# see node/src/codec.ts

BINARY_EXTRACT_THRESHOLD = 16384
"""Minimum byte length for a binary value to be extracted out-of-band."""

_MAGIC = b"CUIB"
_HEADER_SIZE = 8  # magic + u32 LE envLen
_PLACEHOLDER_KEY = "__cui_bin__"

_ENV_LEN_STRUCT = struct.Struct("<I")

_SCALARS = frozenset((str, int, float, bool, type(None)))
_ENVELOPE_KEYS = ("params", "result", "data", "args", "value")


def _as_extractable_segment(value: Any) -> bytes | bytearray | memoryview | None:
    """Return the segment buffer for extractable binaries, None otherwise."""
    if isinstance(value, (bytes, bytearray)):
        return value if len(value) >= BINARY_EXTRACT_THRESHOLD else None
    if isinstance(value, memoryview):
        if value.nbytes < BINARY_EXTRACT_THRESHOLD:
            return None
        # join()/len math below need a flat byte layout.
        return value if value.c_contiguous else memoryview(value.tobytes())
    return None


def _extract_binaries(value: Any, segments: list[bytes | bytearray | memoryview]) -> Any:
    """Depth-first copy-on-write transform: extracted binaries become
    placeholder maps, untouched subtrees keep their identity (no copy for
    binary-free messages). Only dicts, lists and tuples are traversed —
    other class instances are left to msgpack.
    """
    cls = value.__class__
    if cls is dict:
        return _extract_from_dict(value, segments)
    if cls is list or cls is tuple:
        return _extract_from_sequence(value, segments)
    if cls in _SCALARS:
        return value

    segment = _as_extractable_segment(value)
    if segment is not None:
        index = len(segments)
        segments.append(segment)
        return {
            _PLACEHOLDER_KEY: index,
            "l": segment.nbytes if isinstance(segment, memoryview) else len(segment),
        }

    if isinstance(value, dict):
        return _extract_from_dict(value, segments)  # pyright: ignore[reportUnknownArgumentType]
    if isinstance(value, (list, tuple)):
        return _extract_from_sequence(value, segments)  # pyright: ignore[reportUnknownArgumentType]
    return value


def _extract_from_dict(value: dict[Any, Any], segments: list[bytes | bytearray | memoryview]) -> Any:
    dict_copy: dict[Any, Any] | None = None
    for key, item in value.items():
        if item.__class__ in _SCALARS:
            continue
        transformed = _extract_binaries(item, segments)
        if transformed is not item:
            if dict_copy is None:
                dict_copy = dict(value)
            dict_copy[key] = transformed
    return dict_copy if dict_copy is not None else value


def _extract_from_sequence(
    value: list[Any] | tuple[Any, ...], segments: list[bytes | bytearray | memoryview]
) -> Any:
    # the scalar check runs in C, long number lists skip the per-item loop
    if len(value) > 8 and _SCALARS.issuperset(map(type, value)):
        return value
    list_copy: list[Any] | None = None
    for i, item in enumerate(value):
        if item.__class__ in _SCALARS:
            continue
        transformed = _extract_binaries(item, segments)
        if transformed is not item:
            if list_copy is None:
                list_copy = list(value)
            list_copy[i] = transformed
    return list_copy if list_copy is not None else value


def _is_large_binary(value: Any) -> bool:
    cls = value.__class__
    if cls is bytes or cls is bytearray:
        return len(value) >= BINARY_EXTRACT_THRESHOLD
    if cls is memoryview:
        return bool(value.nbytes >= BINARY_EXTRACT_THRESHOLD)
    return False


def _dict_has_large_binary(value: dict[Any, Any]) -> bool:
    return any(map(_is_large_binary, value.values()))


def _has_large_binary_near(value: Any) -> bool:
    cls = value.__class__
    if cls is dict:
        return _dict_has_large_binary(value)
    if (cls is list or cls is tuple) and value:
        first = value[0]
        first_cls = first.__class__
        if first_cls is dict:
            return _dict_has_large_binary(first)
        if (first_cls is list or first_cls is tuple) and first and first[0].__class__ is dict:
            return _dict_has_large_binary(first[0])
        return _is_large_binary(first)
    return _is_large_binary(value)


def _hint_large_binary(data: Any) -> bool:
    """Look for an extractable binary where the library's envelopes carry one.

    Covers call params, results, stream and pull-iterator items and callback
    args without walking the message. A miss only costs encode_message one
    extra pack before it walks.
    """
    if data.__class__ is not dict:
        return _has_large_binary_near(data)
    for key in _ENVELOPE_KEYS:
        value = data.get(key)
        if value is not None and _has_large_binary_near(value):
            return True
    return False


def _is_binary_placeholder(value: Any) -> bool:
    """Strict placeholder check. A user map that happens to carry the
    __cui_bin__ key but has extra keys, a missing "l" or non-integer values
    is NOT treated as a placeholder and passes through untouched.
    """
    if type(value) is not dict or len(value) != 2:  # pyright: ignore[reportUnknownArgumentType]
        return False
    index = value.get(_PLACEHOLDER_KEY)
    length = value.get("l")
    # type() is int: bools are ints in Python but not valid placeholders.
    return type(index) is int and index >= 0 and type(length) is int and length >= 0


class _RestoreState:
    """Mutable cursor for the sequential restore pass."""

    __slots__ = ("next", "offset", "out_of_order")

    def __init__(self, offset: int) -> None:
        self.offset = offset  # start offset of the next expected segment
        self.next = 0  # next expected segment index
        self.out_of_order = False  # placeholder index arrived out of order


def _restore_sequential(value: Any, data: memoryview, state: _RestoreState) -> Any:
    """Single-pass restore for the common case: placeholder indices appear in
    increasing order during envelope traversal (an encoder that assigns
    indices and serializes in one walk always produces this). Offsets are
    then running prefix sums — no length-collection pass needed. Mutates the
    freshly decoded envelope in place; on the first out-of-order index the
    remaining placeholders are left untouched for the fallback pass.

    Decoded envelopes only ever contain plain dicts/lists, so exact type
    checks are safe; the placeholder check is _is_binary_placeholder inlined
    for the hot path.
    """
    if type(value) is dict:
        if len(value) == 2:
            index = value.get(_PLACEHOLDER_KEY)
            length = value.get("l")
            # type() is int: bools are ints in Python but not valid placeholders.
            if type(index) is int and index >= 0 and type(length) is int and length >= 0:
                if state.out_of_order or index != state.next:
                    state.out_of_order = True
                    return value
                end = state.offset + length
                if end > data.nbytes:
                    raise ValueError(
                        f"Invalid CUIB frame: segment {state.next} exceeds payload size {data.nbytes}"
                    )
                view = data[state.offset : end]
                state.offset = end
                state.next += 1
                return view
        for key, item in value.items():
            if type(item) is dict or type(item) is list:
                value[key] = _restore_sequential(item, data, state)
        return value
    if type(value) is list:
        for i, item in enumerate(value):
            if type(item) is dict or type(item) is list:
                value[i] = _restore_sequential(item, data, state)
        return value
    return value


def _collect_segment_lengths(value: Any, lengths: dict[int, int]) -> None:
    """Fallback pass 1: record remaining placeholders' lengths by index."""
    if _is_binary_placeholder(value):
        lengths[value[_PLACEHOLDER_KEY]] = value["l"]
        return
    if isinstance(value, list):
        for item in value:  # pyright: ignore[reportUnknownVariableType]
            _collect_segment_lengths(item, lengths)
        return
    if isinstance(value, dict):
        for item in value.values():  # pyright: ignore[reportUnknownVariableType]
            _collect_segment_lengths(item, lengths)


def _restore_by_index(value: Any, views: dict[int, memoryview]) -> Any:
    """Fallback pass 2: swap remaining placeholders for their segment views."""
    if _is_binary_placeholder(value):
        return views[value[_PLACEHOLDER_KEY]]
    if isinstance(value, list):
        for i, item in enumerate(value):  # pyright: ignore[reportUnknownVariableType,reportUnknownArgumentType]
            value[i] = _restore_by_index(item, views)
        return value
    if isinstance(value, dict):
        for key, item in value.items():  # pyright: ignore[reportUnknownVariableType]
            value[key] = _restore_by_index(item, views)
        return value
    return value


def _restore_remaining(envelope: Any, data: memoryview, state: _RestoreState) -> Any:
    """Out-of-order fallback: _restore_sequential already consumed the
    in-order prefix (segments 0..state.next-1); place the remaining segments
    via an explicit index -> length table. Segments always lie back-to-back
    in index order, whatever order their placeholders appear in.
    """
    lengths: dict[int, int] = {}
    _collect_segment_lengths(envelope, lengths)

    views: dict[int, memoryview] = {}
    offset = state.offset
    for i in range(state.next, max(lengths) + 1 if lengths else state.next):
        length = lengths.get(i)
        if length is None:
            raise ValueError(f"Invalid CUIB frame: missing placeholder for segment {i}")
        end = offset + length
        if end > data.nbytes:
            raise ValueError(f"Invalid CUIB frame: segment {i} exceeds payload size {data.nbytes}")
        views[i] = data[offset:end]
        offset = end
    state.offset = offset

    return _restore_by_index(envelope, views)


def encode_message(data: Any) -> bytes:
    """
    Encode a message for the wire.

    Large binaries (bytes/bytearray/memoryview with byte length >=
    BINARY_EXTRACT_THRESHOLD) are extracted into out-of-band segments after
    the msgpack envelope; binary-free messages stay byte-identical to
    encode(). The input structure is never mutated (copy-on-write).

    Args:
        data: Any serializable data

    Returns:
        Wire-encoded binary data
    """
    packed: bytes | None = None
    if not _hint_large_binary(data):
        try:
            packed = encode(data)
        except TypeError:
            # e.g. a non-contiguous memoryview or a bytes subclass, only the walk below handles those
            packed = None
        # an extractable binary would add at least BINARY_EXTRACT_THRESHOLD bytes of bin payload
        if packed is not None and len(packed) < BINARY_EXTRACT_THRESHOLD:
            return packed

    segments: list[bytes | bytearray | memoryview] = []
    transformed = _extract_binaries(data, segments)

    if not segments:
        return packed if packed is not None else encode(data)

    envelope = encode(transformed)
    # One join copy assembles the full frame — acceptable even for large
    # frames, and cheaper than incremental writes from Python.
    return b"".join((_MAGIC, _ENV_LEN_STRUCT.pack(len(envelope)), envelope, *segments))


def decode_message(data: bytes | bytearray | memoryview) -> Any:
    """
    Decode a wire message.

    Payloads without the CUIB magic are plain msgpack. For framed payloads
    the placeholder maps are replaced by zero-copy memoryview slices into
    ``data`` — binary values >= BINARY_EXTRACT_THRESHOLD (1 KiB) therefore
    arrive as ``memoryview``, not ``bytes``. Consumers speaking the buffer
    protocol (numpy, Rust bindings, ``len()``/slicing) handle views
    directly; call ``bytes(view)`` where a real bytes object is required or
    the data must outlive the receive buffer.

    Args:
        data: Wire-encoded binary data (one complete message)

    Returns:
        Decoded data
    """
    # Fast path for plain msgpack payloads: bytes/bytearray get a cheap
    # startswith check without allocating a memoryview.
    if isinstance(data, memoryview):
        if data.nbytes < _HEADER_SIZE or data[:4] != _MAGIC:
            return decode(data)
        view = data
    else:
        if len(data) < _HEADER_SIZE or not data.startswith(_MAGIC):
            return decode(data)
        view = memoryview(data)
    total = view.nbytes

    env_len: int = _ENV_LEN_STRUCT.unpack_from(view, 4)[0]
    segment_base = _HEADER_SIZE + env_len
    if segment_base > total:
        raise ValueError(f"Invalid CUIB frame: envelope length {env_len} exceeds payload size {total}")

    envelope = decode(view[_HEADER_SIZE:segment_base])

    state = _RestoreState(segment_base)
    envelope = _restore_sequential(envelope, view, state)
    if state.out_of_order:
        envelope = _restore_remaining(envelope, view, state)

    if state.offset != total:
        raise ValueError(f"Invalid CUIB frame: expected payload size {state.offset}, got {total}")

    return envelope
