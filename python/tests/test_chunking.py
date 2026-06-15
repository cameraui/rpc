"""Tests for chunk splitting and reassembly."""

import pytest

from camera_ui_rpc.chunking import ChunkAssembler, ChunkingManager, create_chunks
from camera_ui_rpc.codec import encode


def split(encoded: bytes, chunk_id: str, max_chunk_size: int) -> list[dict[str, object]]:
    return list(create_chunks(encoded, chunk_id, max_chunk_size))


def test_empty_payload_yields_no_chunks() -> None:
    assert split(b"", "t1", 10) == []


def test_single_chunk_when_payload_smaller_than_max() -> None:
    chunks = split(b"hello", "t1", 100)
    assert len(chunks) == 1
    assert chunks[0]["transferId"] == "t1"
    assert chunks[0]["index"] == 0
    assert chunks[0]["data"] == b"hello"


def test_exact_boundary_size_single_chunk() -> None:
    data = b"a" * 10
    chunks = split(data, "t1", 10)
    assert len(chunks) == 1
    assert chunks[0]["data"] == data


def test_one_byte_over_boundary_splits_into_two() -> None:
    data = b"a" * 11
    chunks = split(data, "t1", 10)
    assert len(chunks) == 2
    assert chunks[0]["data"] == b"a" * 10
    assert chunks[1]["data"] == b"a"


def test_multiple_chunks_have_sequential_indices() -> None:
    data = b"x" * 25
    chunks = split(data, "t1", 10)
    assert [c["index"] for c in chunks] == [0, 1, 2]


def test_chunks_reconstruct_original_bytes() -> None:
    data = bytes(range(256)) * 4
    chunks = split(data, "t1", 50)
    reassembled = b"".join(c["data"] for c in chunks)
    assert reassembled == data


def test_assembler_reassembles_single_chunk() -> None:
    payload = encode({"hello": "world"})
    chunks = split(payload, "t1", 1000)

    assembler = ChunkAssembler("t1")
    assembler.set_expected_chunks(len(chunks), total_size=len(payload))
    for chunk in chunks:
        assembler.add_chunk(chunk)

    assert assembler.is_complete()
    assert assembler.get_data() == {"hello": "world"}


def test_assembler_reassembles_multiple_chunks() -> None:
    value = {"items": list(range(500)), "text": "你好世界 🌍"}
    payload = encode(value)
    chunks = split(payload, "t1", 32)

    assembler = ChunkAssembler("t1")
    assembler.set_expected_chunks(len(chunks), total_size=len(payload), chunk_size=32)
    for chunk in chunks:
        complete = assembler.add_chunk(chunk)

    assert complete is True
    assert assembler.get_data() == value


def test_assembler_fallback_path_without_total_size() -> None:
    value = {"a": list(range(200))}
    payload = encode(value)
    chunks = split(payload, "t1", 16)

    assembler = ChunkAssembler("t1")
    assembler.set_expected_chunks(len(chunks))
    assert assembler.buffer is None
    for chunk in chunks:
        assembler.add_chunk(chunk)

    assert assembler.is_complete()
    assert assembler.get_data() == value


def test_assembler_not_complete_until_all_chunks_received() -> None:
    payload = encode([1, 2, 3, 4, 5, 6, 7, 8])
    chunks = split(payload, "t1", 4)
    assert len(chunks) > 1

    assembler = ChunkAssembler("t1")
    assembler.set_expected_chunks(len(chunks), total_size=len(payload), chunk_size=4)
    assembler.add_chunk(chunks[0])
    assert assembler.is_complete() is False


def test_assembler_get_data_before_complete_raises() -> None:
    assembler = ChunkAssembler("t1")
    assembler.set_expected_chunks(2)
    with pytest.raises(RuntimeError):
        assembler.get_data()


def test_assembler_rejects_mismatched_transfer_id() -> None:
    assembler = ChunkAssembler("t1")
    assembler.set_expected_chunks(1, total_size=5)
    with pytest.raises(ValueError):
        assembler.add_chunk({"transferId": "other", "index": 0, "data": b"hello"})


def test_assembler_islast_flag_sets_total_chunks() -> None:
    payload = encode("hi")
    chunks = split(payload, "t1", 1)

    assembler = ChunkAssembler("t1")
    for i, chunk in enumerate(chunks):
        if i == len(chunks) - 1:
            chunk["isLast"] = True
        complete = assembler.add_chunk(chunk)

    assert complete is True
    assert assembler.get_data() == "hi"


def test_progress_reporting() -> None:
    payload = encode(list(range(100)))
    chunks = split(payload, "t1", 8)

    assembler = ChunkAssembler("t1")
    assembler.set_expected_chunks(len(chunks), total_size=len(payload), chunk_size=8)
    assembler.add_chunk(chunks[0])

    progress = assembler.get_progress()
    assert progress["received"] == 1
    assert progress["total"] == len(chunks)
    assert 0 < progress["percentage"] <= 100


def test_manager_invokes_complete_callback() -> None:
    value = {"message": "done", "ids": list(range(50))}
    payload = encode(value)
    chunks = split(payload, "t1", 16)

    received: list[object] = []
    errors: list[Exception] = []
    manager = ChunkingManager()
    manager.start_receiving(
        "t1",
        len(chunks),
        on_complete=received.append,
        on_error=errors.append,
        total_size=len(payload),
        chunk_size=16,
    )
    for chunk in chunks:
        manager.process_chunk(chunk)

    assert errors == []
    assert received == [value]


def test_manager_ignores_unknown_transfer() -> None:
    manager = ChunkingManager()
    manager.process_chunk({"transferId": "unknown", "index": 0, "data": b"x"})
    assert manager.get_progress("unknown") is None


def test_manager_normalizes_chunk_index_key() -> None:
    value = [1, 2, 3]
    payload = encode(value)
    chunks = split(payload, "t1", 1000)

    received: list[object] = []
    manager = ChunkingManager()
    manager.start_receiving(
        "t1",
        len(chunks),
        on_complete=received.append,
        on_error=lambda e: None,
        total_size=len(payload),
    )
    chunk = chunks[0]
    manager.process_chunk(
        {"transferId": "t1", "chunkIndex": chunk["index"], "data": chunk["data"]}
    )
    assert received == [value]


def test_manager_cancel_invokes_error_callback() -> None:
    errors: list[Exception] = []
    manager = ChunkingManager()
    manager.start_receiving(
        "t1", 5, on_complete=lambda d: None, on_error=errors.append, total_size=100
    )
    manager.cancel("t1")
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert manager.get_progress("t1") is None
