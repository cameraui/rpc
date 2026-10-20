import asyncio
import time
from collections.abc import Callable, Generator
from typing import Any, cast

from .codec import decode_message
from .types import ChunkData

DEFAULT_MAX_TRANSFER_SIZE = 1024 * 1024 * 1024
MAX_CHUNK_SIZE = 64 * 1024 * 1024  # the NATS max_payload ceiling
MAX_CONCURRENT_TRANSFERS = 32
CHUNK_RESERVE = 8192  # kept free in every message for headers and the envelope
MIN_PAYLOAD_LIMIT = 16384  # leaves room for a chunk next to CHUNK_RESERVE
_MAX_INDEX_DIGITS = 15


def parse_chunk_index(value: str | None) -> int | None:
    if not value or len(value) > _MAX_INDEX_DIGITS or not value.isascii() or not value.isdigit():
        return None
    return int(value)


def _as_int(value: Any) -> int | None:
    # msgpackr encodes large numbers as float64, an integral float is still a valid size
    if type(value) is int:
        return value
    if type(value) is float and value.is_integer():
        return int(value)
    return None


def _header_problem(
    total_chunks: Any, total_size: Any, chunk_size: Any, max_transfer_size: int
) -> str | None:
    chunks = _as_int(total_chunks)
    size = _as_int(total_size)
    step = _as_int(chunk_size)
    if step is None or not 0 < step <= MAX_CHUNK_SIZE:
        return f"invalid chunkSize {chunk_size!r}"
    if size is None or not 0 < size <= max_transfer_size:
        return f"invalid totalSize {total_size!r} (limit {max_transfer_size})"
    if chunks is None or chunks != -(-size // step):
        return f"totalChunks {total_chunks!r} does not match totalSize {size} and chunkSize {step}"
    return None


class TransferBudget:
    def __init__(self, limit: int | None = None) -> None:
        self.limit: int = limit or DEFAULT_MAX_TRANSFER_SIZE
        self.used: int = 0

    def reserve(self, size: int) -> bool:
        if self.used + size > self.limit:
            return False
        self.used += size
        return True

    def release(self, size: int) -> None:
        self.used = max(0, self.used - size)


def create_chunks(encoded: bytes, chunk_id: str, max_chunk_size: int) -> Generator[ChunkData, None, None]:
    total_size = len(encoded)
    if total_size == 0:
        return

    total_chunks = (total_size + max_chunk_size - 1) // max_chunk_size

    mv = memoryview(encoded)

    for i in range(total_chunks):
        start = i * max_chunk_size
        end = min(start + max_chunk_size, total_size)

        chunk_view = mv[start:end]

        # the only copy, nats-py needs bytes
        yield {"transferId": chunk_id, "index": i, "data": bytes(chunk_view)}


class ChunkAssembler:
    def __init__(self, chunk_id: str):
        self.id: str = chunk_id
        self.chunks_received: set[int] = set()
        self.total_chunks: int | None = None
        self.total_size: int = 0
        self.buffer: bytearray | None = None
        self.temp_chunks: dict[int, bytes] = {}
        self.chunk_size: int | None = None
        # set by ChunkingManager, the buffer is only allocated within it
        self.budget: TransferBudget | None = None
        self.reserved: int = 0
        self._preallocate = False

    def set_expected_chunks(
        self, count: int, total_size: int | None = None, chunk_size: int | None = None
    ) -> None:
        self.total_chunks = count
        self.chunk_size = chunk_size
        if total_size is not None and total_size > 0:
            self.total_size = total_size
            self._preallocate = True

    def add_chunk(self, chunk: dict[str, Any]) -> bool:
        if chunk["transferId"] != self.id:
            raise ValueError(f"Chunk ID mismatch: expected {self.id}, got {chunk['transferId']}")

        chunk_index = chunk["index"]
        chunk_data = chunk["data"]
        chunk_size = len(chunk_data)

        if (
            type(chunk_index) is not int
            or chunk_index < 0
            or (self.total_chunks is not None and chunk_index >= self.total_chunks)
        ):
            raise ValueError(f"Invalid chunk index {chunk_index!r} for {self.total_chunks} chunks")

        if self.chunk_size is not None and self.total_chunks is not None and self.total_size > 0:
            last = self.total_chunks - 1
            expected = self.chunk_size if chunk_index < last else self.total_size - last * self.chunk_size
            if chunk_size != expected:
                raise ValueError(f"Chunk {chunk_index} has {chunk_size} bytes, expected {expected}")

        if chunk_index in self.chunks_received:
            return False

        # chunk 0 sets the chunk size, senders differ in it
        if self.chunk_size is None and chunk_index == 0 and self.total_chunks and self.total_chunks > 1:
            self.chunk_size = chunk_size

        effective_chunk_size = self.chunk_size if self.chunk_size else chunk_size

        if self.buffer is None and self._preallocate:
            # not on the header: bytearray zero-fills, a bare header would commit totalSize
            if self.budget is not None:
                if not self.budget.reserve(self.total_size):
                    raise ValueError(f"Transfers in progress would hold more than {self.budget.limit} bytes")
                self.reserved = self.total_size
            self._preallocate = False
            try:
                self.buffer = bytearray(self.total_size)
            except MemoryError:
                print(f"MemoryError: Failed to allocate {self.total_size} bytes for chunk assembly.")

        if self.buffer is not None:
            offset = chunk_index * effective_chunk_size
            if offset + chunk_size <= self.total_size:
                self.buffer[offset : offset + chunk_size] = chunk_data
            else:
                # only the last chunk may be smaller
                remaining = self.total_size - offset
                if remaining > 0:
                    self.buffer[offset:] = chunk_data[:remaining]
        else:
            self.temp_chunks[chunk_index] = chunk_data

        self.chunks_received.add(chunk_index)

        if chunk.get("isLast", False):
            self.total_chunks = chunk_index + 1

        return self.is_complete()

    def is_complete(self) -> bool:
        if self.total_chunks is None:
            return False
        return len(self.chunks_received) == self.total_chunks

    def get_data(self) -> Any:
        if not self.is_complete():
            raise RuntimeError("Cannot get data: Not all chunks have been received.")

        if self.buffer is not None:
            # chunked payloads are full wire messages, a CUIB frame yields memoryview slices that keep the buffer alive
            data_view = memoryview(self.buffer)
            return decode_message(data_view)
        else:
            total_size = sum(len(self.temp_chunks[i]) for i in range(self.total_chunks or 0))
            combined = bytearray(total_size)
            offset = 0
            for i in range(self.total_chunks or 0):
                chunk = self.temp_chunks[i]
                chunk_len = len(chunk)
                combined[offset : offset + chunk_len] = chunk
                offset += chunk_len
            return decode_message(combined)

    def release(self) -> None:
        if self.budget is not None and self.reserved:
            self.budget.release(self.reserved)
        self.reserved = 0

    def get_progress(self) -> dict[str, int | float]:
        received = len(self.chunks_received)
        total = self.total_chunks or 0
        percentage = (received / total * 100) if total > 0 else 0
        return {"received": received, "total": total, "percentage": percentage}


class ChunkingManager:
    # an incomplete transfer holds its full buffer until this long after its last chunk
    _STALE_TRANSFER_TTL: float = 30.0
    # sweeping on every chunk of a large transfer is pure overhead
    _SWEEP_INTERVAL: float = 1.0

    def __init__(
        self,
        max_transfer_size: int | None = None,
        max_transfers: float = MAX_CONCURRENT_TRANSFERS,
        budget: TransferBudget | None = None,
    ) -> None:
        self.assemblers: dict[str, ChunkAssembler] = {}
        self.completed_callbacks: dict[str, Callable[[Any], None]] = {}
        self.error_callbacks: dict[str, Callable[[Exception], None]] = {}
        self.max_transfer_size: int = max_transfer_size or DEFAULT_MAX_TRANSFER_SIZE
        self.max_transfers: float = max_transfers
        self.budget: TransferBudget = budget if budget is not None else TransferBudget(self.max_transfer_size)
        self._last_activity: dict[str, float] = {}
        self._last_sweep: float = 0.0
        self._timer: asyncio.TimerHandle | None = None

    def start_receiving(
        self,
        transfer_id: str,
        total_chunks: Any,
        on_complete: Callable[[Any], None],
        on_error: Callable[[Exception], None],
        total_size: Any = None,
        chunk_size: Any = None,
    ) -> None:
        self._sweep_stale()
        problem = _header_problem(total_chunks, total_size, chunk_size, self.max_transfer_size)
        if problem is None and transfer_id in self.assemblers:
            self._drop(transfer_id)
        if problem is None and len(self.assemblers) >= self.max_transfers:
            problem = f"{self.max_transfers} transfers already open"
        if problem is not None:
            on_error(ValueError(f"Rejected chunked transfer {transfer_id}: {problem}"))
            return

        assembler = ChunkAssembler(transfer_id)
        assembler.set_expected_chunks(
            cast(int, _as_int(total_chunks)), _as_int(total_size), _as_int(chunk_size)
        )
        assembler.budget = self.budget
        self.assemblers[transfer_id] = assembler
        self.completed_callbacks[transfer_id] = on_complete
        self.error_callbacks[transfer_id] = on_error
        self._last_activity[transfer_id] = time.monotonic()
        self._arm()

    def process_chunk(self, chunk: dict[str, Any]) -> None:
        self._sweep_stale()

        transfer_id = chunk.get("id") or chunk.get("transferId")
        if not transfer_id:
            return

        assembler = self.assemblers.get(transfer_id)
        if not assembler:
            return

        self._last_activity[transfer_id] = time.monotonic()

        try:
            # senders name the keys differently, normalized in place
            if "transferId" not in chunk:
                chunk["transferId"] = transfer_id
            if "index" not in chunk and "chunkIndex" in chunk:
                chunk["index"] = chunk["chunkIndex"]

            if assembler.add_chunk(chunk):
                data = assembler.get_data()
                callback = self.completed_callbacks.get(transfer_id)
                self._drop(transfer_id)

                if callback:
                    callback(data)

        except Exception as e:
            self._handle_error(transfer_id, e)

    def cancel(self, transfer_id: str) -> None:
        self._handle_error(transfer_id, RuntimeError("Transfer cancelled"))

    def discard(self, transfer_id: str) -> None:
        self._drop(transfer_id)

    def get_progress(self, transfer_id: str) -> dict[str, int | float] | None:
        assembler = self.assemblers.get(transfer_id)
        return assembler.get_progress() if assembler else None

    def close(self) -> None:
        for assembler in self.assemblers.values():
            assembler.release()
        self.assemblers.clear()
        self.completed_callbacks.clear()
        self.error_callbacks.clear()
        self._last_activity.clear()
        self._cancel_timer()

    def _cancel_timer(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _arm(self) -> None:
        if self._timer is not None or not self._last_activity:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        due = min(self._last_activity.values()) + self._STALE_TRANSFER_TTL - time.monotonic()
        self._timer = loop.call_later(max(0.0, due) + 0.1, self._on_timer)

    def _on_timer(self) -> None:
        self._timer = None
        self._last_sweep = 0.0
        self._sweep_stale()
        self._arm()

    def _sweep_stale(self) -> None:
        if not self._last_activity:
            return
        now = time.monotonic()
        if now - self._last_sweep < self._SWEEP_INTERVAL:
            return
        self._last_sweep = now
        cutoff = now - self._STALE_TRANSFER_TTL
        stale = [tid for tid, ts in self._last_activity.items() if ts < cutoff]
        for transfer_id in stale:
            self._handle_error(transfer_id, RuntimeError("Transfer timed out"))

    def _handle_error(self, transfer_id: str, error: Exception) -> None:
        error_callback = self.error_callbacks.get(transfer_id)
        self._drop(transfer_id)

        if error_callback:
            error_callback(error)

    def _drop(self, transfer_id: str) -> None:
        assembler = self.assemblers.pop(transfer_id, None)
        if assembler is not None:
            assembler.release()
        self.completed_callbacks.pop(transfer_id, None)
        self.error_callbacks.pop(transfer_id, None)
        self._last_activity.pop(transfer_id, None)
        if not self._last_activity:
            self._cancel_timer()
