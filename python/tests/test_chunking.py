import asyncio
import math

import pytest

from camera_ui_rpc.chunking import (
    DEFAULT_MAX_TRANSFER_SIZE,
    MAX_CHUNK_SIZE,
    MAX_CONCURRENT_TRANSFERS,
    ChunkAssembler,
    ChunkingManager,
    TransferBudget,
    create_chunks,
    parse_chunk_index,
)
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
        chunk_size=1000,
    )
    chunk = chunks[0]
    manager.process_chunk({"transferId": "t1", "chunkIndex": chunk["index"], "data": chunk["data"]})
    assert received == [value]


def test_manager_cancel_invokes_error_callback() -> None:
    errors: list[Exception] = []
    manager = ChunkingManager()
    manager.start_receiving(
        "t1", 5, on_complete=lambda d: None, on_error=errors.append, total_size=100, chunk_size=20
    )
    manager.cancel("t1")
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert manager.get_progress("t1") is None


def start(
    manager: ChunkingManager, transfer_id: str, header: dict[str, object]
) -> tuple[list[object], list[Exception]]:
    received: list[object] = []
    errors: list[Exception] = []
    manager.start_receiving(
        transfer_id,
        header.get("totalChunks"),  # type: ignore[arg-type]
        on_complete=received.append,
        on_error=errors.append,
        total_size=header.get("totalSize"),  # type: ignore[arg-type]
        chunk_size=header.get("chunkSize"),  # type: ignore[arg-type]
    )
    return received, errors


@pytest.mark.parametrize(
    "header",
    [
        {"totalChunks": 1, "totalSize": -1, "chunkSize": 10},
        {"totalChunks": 0, "totalSize": 0, "chunkSize": 10},
        {"totalChunks": 17, "totalSize": 1024**3 + 1, "chunkSize": 64 * 1024**2},
        {"totalChunks": 2, "totalSize": 15.5, "chunkSize": 10},
        {"totalChunks": 1, "totalSize": True, "chunkSize": 10},
        {"totalChunks": 2, "totalSize": "15", "chunkSize": 10},
        {"totalChunks": 2, "totalSize": 15, "chunkSize": 0},
        {"totalChunks": 2, "totalSize": 15, "chunkSize": None},
        {"totalChunks": 1, "totalSize": 65 * 1024 * 1024, "chunkSize": 65 * 1024 * 1024},
        {"totalChunks": 3, "totalSize": 15, "chunkSize": 10},
        {"totalChunks": None, "totalSize": 15, "chunkSize": 10},
    ],
)
def test_manager_rejects_invalid_header_without_allocating(header: dict[str, object]) -> None:
    manager = ChunkingManager()
    received, errors = start(manager, "t1", header)

    assert received == []
    assert len(errors) == 1 and isinstance(errors[0], ValueError)
    assert manager.assemblers == {}
    manager.process_chunk({"id": "t1", "chunkIndex": 0, "data": b"x" * 10})
    assert received == [] and len(errors) == 1


def test_manager_rejects_header_above_max_transfer_size() -> None:
    manager = ChunkingManager(max_transfer_size=100)
    _, errors = start(manager, "t1", {"totalChunks": 11, "totalSize": 101, "chunkSize": 10})
    assert len(errors) == 1
    _, errors = start(manager, "t2", {"totalChunks": 10, "totalSize": 100, "chunkSize": 10})
    assert errors == []
    assert list(manager.assemblers) == ["t2"]


def test_limits_match_the_spec() -> None:
    assert DEFAULT_MAX_TRANSFER_SIZE == 1024**3
    assert MAX_CHUNK_SIZE == 64 * 1024**2
    assert MAX_CONCURRENT_TRANSFERS == 32


def test_manager_max_transfer_size_zero_means_default() -> None:
    assert ChunkingManager(0).max_transfer_size == DEFAULT_MAX_TRANSFER_SIZE
    assert ChunkingManager().max_transfer_size == DEFAULT_MAX_TRANSFER_SIZE


def test_manager_accepts_integral_float_header_values() -> None:
    payload = encode({"big": "x" * 50})
    chunks = split(payload, "t1", 16)
    manager = ChunkingManager()
    received, errors = start(
        manager,
        "t1",
        {"totalChunks": float(len(chunks)), "totalSize": float(len(payload)), "chunkSize": 16.0},
    )
    for chunk in chunks:
        manager.process_chunk(chunk)
    assert errors == []
    assert received == [{"big": "x" * 50}]


def test_manager_header_alone_allocates_nothing() -> None:
    manager = ChunkingManager()
    start(manager, "t1", {"totalChunks": 64, "totalSize": 64 * 1024 * 1024, "chunkSize": 1024 * 1024})
    assert manager.assemblers["t1"].buffer is None
    assert manager.budget.used == 0


def test_manager_caps_concurrent_transfers() -> None:
    manager = ChunkingManager()
    header = {"totalChunks": 2, "totalSize": 20, "chunkSize": 10}
    for i in range(MAX_CONCURRENT_TRANSFERS):
        _, errors = start(manager, f"t{i}", header)
        assert errors == []
    _, errors = start(manager, "one-too-many", header)
    assert len(errors) == 1
    assert len(manager.assemblers) == MAX_CONCURRENT_TRANSFERS
    manager.cancel("t0")
    _, errors = start(manager, "after-cancel", header)
    assert errors == []


def test_manager_takes_its_own_transfer_limit() -> None:
    header = {"totalChunks": 2, "totalSize": 20, "chunkSize": 10}
    small = ChunkingManager(None, 2)
    assert [start(small, f"t{i}", header)[1] != [] for i in range(3)] == [False, False, True]

    unlimited = ChunkingManager(None, math.inf)
    for i in range(MAX_CONCURRENT_TRANSFERS + 8):
        _, errors = start(unlimited, f"t{i}", header)
        assert errors == []
    assert len(unlimited.assemblers) == MAX_CONCURRENT_TRANSFERS + 8


def test_manager_discard_drops_without_callbacks() -> None:
    manager = ChunkingManager()
    done, errors = start(manager, "t1", {"totalChunks": 2, "totalSize": 20, "chunkSize": 10})

    manager.discard("t1")
    manager.process_chunk({"id": "t1", "chunkIndex": 0, "data": b"\x00" * 10})

    assert manager.assemblers == {}
    assert done == []
    assert errors == []
    assert manager._timer is None  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("index", [-1, 3, 7, None, "1", 1.0])
def test_manager_fails_transfer_on_chunk_index_out_of_range(index: object) -> None:
    payload = encode(list(range(20)))
    chunks = split(payload, "t1", 10)
    assert len(chunks) == 3
    manager = ChunkingManager()
    received, errors = start(manager, "t1", {"totalChunks": 3, "totalSize": len(payload), "chunkSize": 10})

    manager.process_chunk({"id": "t1", "chunkIndex": index, "data": b"\x00" * 10})

    assert received == []
    assert len(errors) == 1 and isinstance(errors[0], ValueError)
    assert manager.assemblers == {}


@pytest.mark.parametrize(("index", "size"), [(0, 9), (0, 11), (2, 10), (2, 1)])
def test_manager_fails_transfer_on_wrong_chunk_length(index: int, size: int) -> None:
    manager = ChunkingManager()
    received, errors = start(manager, "t1", {"totalChunks": 3, "totalSize": 25, "chunkSize": 10})

    manager.process_chunk({"id": "t1", "chunkIndex": index, "data": b"\x00" * size})

    assert received == []
    assert len(errors) == 1
    assert manager.assemblers == {}


@pytest.mark.parametrize("size", [9, 11])
def test_manager_drops_the_transfer_on_a_repeated_index_with_the_wrong_length(size: int) -> None:
    payload = encode(list(range(20)))
    chunks = split(payload, "t1", 10)
    manager = ChunkingManager()
    received, errors = start(manager, "t1", {"totalChunks": 3, "totalSize": len(payload), "chunkSize": 10})

    manager.process_chunk(dict(chunks[0]))
    manager.process_chunk({"transferId": "t1", "index": 0, "data": b"\x00" * size})

    assert len(errors) == 1 and isinstance(errors[0], ValueError)
    assert manager.assemblers == {}
    assert manager.budget.used == 0
    for chunk in chunks:
        manager.process_chunk(dict(chunk))
    assert received == []
    assert len(errors) == 1


def test_manager_keeps_the_open_transfer_when_a_repeated_header_is_refused() -> None:
    payload = encode(list(range(20)))
    chunks = split(payload, "t1", 10)
    manager = ChunkingManager()
    received, errors = start(
        manager, "t1", {"totalChunks": len(chunks), "totalSize": len(payload), "chunkSize": 10}
    )
    manager.process_chunk(dict(chunks[0]))

    _, refused = start(manager, "t1", {"totalChunks": 1, "totalSize": 10, "chunkSize": 0})

    assert len(refused) == 1 and isinstance(refused[0], ValueError)
    for chunk in chunks[1:]:
        manager.process_chunk(dict(chunk))
    assert received == [list(range(20))]
    assert errors == []


def test_manager_ignores_duplicate_chunks() -> None:
    value = {"items": list(range(100))}
    payload = encode(value)
    chunks = split(payload, "t1", 32)
    manager = ChunkingManager()
    received, errors = start(
        manager, "t1", {"totalChunks": len(chunks), "totalSize": len(payload), "chunkSize": 32}
    )

    manager.process_chunk(dict(chunks[0]))
    manager.process_chunk({"transferId": "t1", "index": 0, "data": b"\xff" * 32})
    for chunk in chunks[1:]:
        manager.process_chunk(chunk)

    assert errors == []
    assert received == [value]


@pytest.mark.parametrize("total", [30, 31, 39])
def test_manager_accepts_every_sender_split(total: int) -> None:
    payload = bytes(range(256)) * 2
    value = payload[:total]
    encoded = encode(value)
    step = 10
    chunks = split(encoded, "t1", step)
    manager = ChunkingManager()
    received, errors = start(
        manager, "t1", {"totalChunks": len(chunks), "totalSize": len(encoded), "chunkSize": step}
    )
    for chunk in chunks:
        manager.process_chunk(chunk)
    assert errors == []
    assert received == [value]


def test_assembler_does_not_keep_chunk_size_of_previous_transfer() -> None:
    assembler = ChunkAssembler("t1")
    assembler.set_expected_chunks(4, total_size=150, chunk_size=40)
    assembler.set_expected_chunks(6, total_size=150)
    assert assembler.chunk_size is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("0", 0),
        ("7", 7),
        ("007", 7),
        ("12", 12),
        ("9" * 15, int("9" * 15)),
        (None, None),
        ("", None),
        ("-1", None),
        ("-0", None),
        ("+1", None),
        ("1.0", None),
        (" 1", None),
        ("1 ", None),
        ("x", None),
        ("1" * 16, None),
        ("99999999999999999999", None),
    ],
)
def test_parse_chunk_index(value: str | None, expected: int | None) -> None:
    assert parse_chunk_index(value) == expected


def test_parse_chunk_index_rejects_non_ascii_digits() -> None:
    assert parse_chunk_index("٣") is None


def test_stale_transfer_is_dropped_by_timer_without_traffic() -> None:
    async def run() -> None:
        manager = ChunkingManager()
        manager._STALE_TRANSFER_TTL = 0.05  # pyright: ignore[reportPrivateUsage]
        _, errors = start(manager, "t1", {"totalChunks": 2, "totalSize": 20, "chunkSize": 10})
        manager.process_chunk({"id": "t1", "chunkIndex": 0, "data": b"\x00" * 10})

        await asyncio.sleep(0.4)

        assert manager.assemblers == {}
        assert len(errors) == 1 and "timed out" in str(errors[0])
        assert manager._timer is None  # pyright: ignore[reportPrivateUsage]
        assert manager.budget.used == 0

    asyncio.run(run())


def test_timer_stops_once_no_transfer_is_open() -> None:
    async def run() -> None:
        payload = encode("done")
        chunks = split(payload, "t1", 2)
        manager = ChunkingManager()
        received, _ = start(
            manager, "t1", {"totalChunks": len(chunks), "totalSize": len(payload), "chunkSize": 2}
        )
        assert manager._timer is not None  # pyright: ignore[reportPrivateUsage]
        for chunk in chunks:
            manager.process_chunk(chunk)
        assert received == ["done"]
        assert manager._timer is None  # pyright: ignore[reportPrivateUsage]

    asyncio.run(run())


def test_close_cancels_the_timer_and_drops_open_transfers() -> None:
    async def run() -> None:
        manager = ChunkingManager()
        received, errors = start(manager, "t1", {"totalChunks": 2, "totalSize": 20, "chunkSize": 10})
        manager.process_chunk({"id": "t1", "chunkIndex": 0, "data": b"\x00" * 10})
        timer = manager._timer  # pyright: ignore[reportPrivateUsage]
        assert timer is not None
        manager.close()
        assert timer.cancelled()
        assert manager.assemblers == {}
        assert manager.budget.used == 0
        assert received == [] and errors == []

    asyncio.run(run())


def test_budget_defaults_to_the_max_transfer_size() -> None:
    assert ChunkingManager().budget.limit == DEFAULT_MAX_TRANSFER_SIZE
    assert ChunkingManager(1000).budget.limit == 1000
    assert TransferBudget(0).limit == DEFAULT_MAX_TRANSFER_SIZE


def test_budget_counts_transfers_from_their_first_chunk() -> None:
    payload = encode("x" * 58)
    chunks = split(payload, "t1", 10)
    header = {"totalChunks": len(chunks), "totalSize": len(payload), "chunkSize": 10}
    manager = ChunkingManager(100)
    received, _ = start(manager, "t1", header)
    _, refused = start(manager, "t2", header)
    assert manager.budget.used == 0

    manager.process_chunk(chunks[0])
    assert manager.budget.used == len(payload)
    manager.process_chunk({"id": "t2", "chunkIndex": 0, "data": chunks[0]["data"]})

    assert len(refused) == 1 and "more than 100 bytes" in str(refused[0])
    assert list(manager.assemblers) == ["t1"]
    assert manager.budget.used == len(payload)
    for chunk in chunks[1:]:
        manager.process_chunk(chunk)
    assert received == ["x" * 58]
    assert manager.budget.used == 0


def test_managers_on_one_budget_stay_within_it_together() -> None:
    budget = TransferBudget(100)
    first = ChunkingManager(100, budget=budget)
    second = ChunkingManager(100, budget=budget)
    header = {"totalChunks": 6, "totalSize": 60, "chunkSize": 10}
    start(first, "t1", header)
    first.process_chunk({"id": "t1", "chunkIndex": 0, "data": b"\x00" * 10})

    _, refused = start(second, "t1", header)
    second.process_chunk({"id": "t1", "chunkIndex": 0, "data": b"\x00" * 10})
    assert len(refused) == 1
    assert budget.used == 60

    first.close()
    assert budget.used == 0
    _, errors = start(second, "t1", header)
    second.process_chunk({"id": "t1", "chunkIndex": 0, "data": b"\x00" * 10})
    assert errors == []
    assert budget.used == 60


@pytest.mark.parametrize("end", ["cancel", "discard", "close", "bad chunk", "same id again"])
def test_every_dropped_transfer_returns_its_bytes(end: str) -> None:
    manager = ChunkingManager()
    header = {"totalChunks": 2, "totalSize": 20, "chunkSize": 10}
    start(manager, "t1", header)
    manager.process_chunk({"id": "t1", "chunkIndex": 0, "data": b"\x00" * 10})
    assert manager.budget.used == 20

    if end == "cancel":
        manager.cancel("t1")
    elif end == "discard":
        manager.discard("t1")
    elif end == "close":
        manager.close()
    elif end == "bad chunk":
        manager.process_chunk({"id": "t1", "chunkIndex": 1, "data": b"\x00" * 3})
    else:
        start(manager, "t1", header)

    assert manager.budget.used == 0


def test_headers_without_chunks_hold_no_budget_but_count_against_the_cap() -> None:
    manager = ChunkingManager()
    header = {"totalChunks": 1, "totalSize": MAX_CHUNK_SIZE, "chunkSize": MAX_CHUNK_SIZE}
    refused = sum(len(start(manager, f"t{i}", header)[1]) for i in range(40))

    assert refused == 40 - MAX_CONCURRENT_TRANSFERS
    assert manager.budget.used == 0
