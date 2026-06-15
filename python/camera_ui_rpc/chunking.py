"""Optimized chunking system for large message handling."""

from collections.abc import Callable, Generator
from typing import Any

from .codec import decode
from .types import ChunkData


def create_chunks(encoded: bytes, chunk_id: str, max_chunk_size: int) -> Generator[ChunkData, None, None]:
    """
    Split data into chunks using memory views to avoid copies.

    Args:
        encoded: Encoded data to be chunked.
        chunk_id: A unique identifier for this chunk transfer.
        max_chunk_size: The maximum size for each chunk.

    Yields:
        ChunkData messages for each chunk.
    """
    total_size = len(encoded)
    if total_size == 0:
        return

    total_chunks = (total_size + max_chunk_size - 1) // max_chunk_size  # Ceiling division

    # Use a memoryview to avoid copying data when slicing
    mv = memoryview(encoded)

    for i in range(total_chunks):
        start = i * max_chunk_size
        end = min(start + max_chunk_size, total_size)

        # Create a view of the chunk instead of copying the data
        chunk_view = mv[start:end]

        # The `bytes()` conversion here is the only copy, which is necessary
        # for the NATS client to send the data.
        yield {"transferId": chunk_id, "index": i, "data": bytes(chunk_view)}


class ChunkAssembler:
    """
    Reassembles chunks into the original data.
    This implementation uses a pre-allocated buffer for high efficiency,
    minimizing memory copies and allocations (zero-copy assembly).
    """

    def __init__(self, chunk_id: str):
        self.id: str = chunk_id
        self.chunks_received: set[int] = set()
        self.total_chunks: int | None = None
        self.total_size: int = 0
        # The pre-allocated buffer for direct writing
        self.buffer: bytearray | None = None
        # Fallback storage if pre-allocation is not possible
        self.temp_chunks: dict[int, bytes] = {}
        # Dynamically determined chunk size from first chunk
        self.chunk_size: int | None = None

    def set_expected_chunks(
        self, count: int, total_size: int | None = None, chunk_size: int | None = None
    ) -> None:
        """
        Set the expected number of chunks and pre-allocate the buffer if total_size is known.

        Args:
            count: The total number of chunks expected.
            total_size: The total size of the final reassembled data.
            chunk_size: The size of each chunk (except possibly the last one).
        """
        self.total_chunks = count
        if chunk_size is not None:
            self.chunk_size = chunk_size
        if total_size is not None and total_size > 0:
            self.total_size = total_size
            try:
                # Pre-allocate the entire buffer to write chunks into directly.
                # This is the core of the zero-copy assembly strategy.
                self.buffer = bytearray(total_size)
            except MemoryError:
                print(f"MemoryError: Failed to allocate {total_size} bytes for chunk assembly.")
                self.buffer = None  # Fallback to temp_chunks

    def add_chunk(self, chunk: dict[str, Any]) -> bool:
        """
        Add a chunk to the assembler. Writes directly into the pre-allocated
        buffer if available, otherwise stores it temporarily.

        Args:
            chunk: A dictionary containing chunk metadata and data.
                   Expected keys: 'transferId', 'index', 'data'.
                   Optional key: 'isLast'.

        Returns:
            True if the transfer is complete, False otherwise.
        """
        if chunk["transferId"] != self.id:
            raise ValueError(f"Chunk ID mismatch: expected {self.id}, got {chunk['transferId']}")

        chunk_index = chunk["index"]
        chunk_data = chunk["data"]
        chunk_size = len(chunk_data)

        # Determine chunk size from first non-last chunk (chunk 0)
        # This handles dynamic chunk sizes from different clients
        if self.chunk_size is None and chunk_index == 0 and self.total_chunks and self.total_chunks > 1:
            # First chunk of a multi-chunk transfer - use its size as the standard
            self.chunk_size = chunk_size

        # Use dynamically determined chunk size, or fall back to received chunk size for single chunks
        effective_chunk_size = self.chunk_size if self.chunk_size else chunk_size

        if self.buffer is not None:
            # Optimal path: Direct write to the pre-allocated buffer.
            offset = chunk_index * effective_chunk_size
            if offset + chunk_size <= self.total_size:
                self.buffer[offset : offset + chunk_size] = chunk_data
            else:
                # This should only happen for the last chunk, which might be smaller.
                remaining = self.total_size - offset
                if remaining > 0:
                    self.buffer[offset:] = chunk_data[:remaining]
        else:
            # Fallback path: If pre-allocation failed or was not possible,
            # store chunks in a dictionary. This is less memory-efficient.
            self.temp_chunks[chunk_index] = chunk_data

        self.chunks_received.add(chunk_index)

        # If the 'isLast' flag is present, we can determine the total number of chunks.
        if chunk.get("isLast", False):
            self.total_chunks = chunk_index + 1

        return self.is_complete()

    def is_complete(self) -> bool:
        """Check if all expected chunks have been received."""
        if self.total_chunks is None:
            return False
        return len(self.chunks_received) == self.total_chunks

    def get_data(self) -> Any:
        """
        Reassemble and decode the final data with minimal copying.

        Returns:
            The decoded data.
        """
        if not self.is_complete():
            raise RuntimeError("Cannot get data: Not all chunks have been received.")

        if self.buffer is not None:
            # Optimal path: The buffer is already fully assembled.
            # We create a memoryview to avoid copying the buffer before decoding.
            data_view = memoryview(self.buffer)
            return decode(data_view)
        else:
            # Fallback path: Manually combine chunks from temporary storage.
            # This path involves a final copy operation.
            total_size = sum(len(self.temp_chunks[i]) for i in range(self.total_chunks or 0))
            combined = bytearray(total_size)
            offset = 0
            for i in range(self.total_chunks or 0):
                chunk = self.temp_chunks[i]
                chunk_len = len(chunk)
                combined[offset : offset + chunk_len] = chunk
                offset += chunk_len
            return decode(combined)

    def get_progress(self) -> dict[str, int | float]:
        """Get the current progress of the chunk transfer."""
        received = len(self.chunks_received)
        total = self.total_chunks or 0
        percentage = (received / total * 100) if total > 0 else 0
        return {"received": received, "total": total, "percentage": percentage}


class ChunkingManager:
    """Manages multiple concurrent chunk transfers."""

    def __init__(self) -> None:
        self.assemblers: dict[str, ChunkAssembler] = {}
        self.completed_callbacks: dict[str, Callable[[Any], None]] = {}
        self.error_callbacks: dict[str, Callable[[Exception], None]] = {}
        self._assembler_pool: list[ChunkAssembler] = []

    def _get_assembler(self, transfer_id: str) -> ChunkAssembler:
        """Get a reusable assembler from a pool or create a new one."""
        if self._assembler_pool:
            assembler = self._assembler_pool.pop()
            # Reset the state of the reused assembler
            assembler.id = transfer_id
            assembler.chunks_received.clear()
            assembler.total_chunks = None
            assembler.total_size = 0
            assembler.buffer = None
            assembler.temp_chunks.clear()
            return assembler
        return ChunkAssembler(transfer_id)

    def _return_assembler(self, assembler: ChunkAssembler) -> None:
        """Return an assembler to the pool for reuse."""
        # Clear large memory structures to free up memory before pooling.
        assembler.buffer = None
        assembler.temp_chunks.clear()
        if len(self._assembler_pool) < 10:  # Limit pool size
            self._assembler_pool.append(assembler)

    def start_receiving(
        self,
        transfer_id: str,
        total_chunks: int,
        on_complete: Callable[[Any], None],
        on_error: Callable[[Exception], None],
        total_size: int | None = None,
        chunk_size: int | None = None,
    ) -> None:
        """
        Start a new chunk transfer, preparing an assembler for it.

        Args:
            transfer_id: The unique ID for the transfer.
            total_chunks: The total number of chunks to expect.
            on_complete: Callback to execute when the transfer is complete.
            chunk_size: The size of each chunk (from sender).
            on_error: Callback to execute if an error occurs.
            total_size: The total size of the final data for pre-allocation.
        """
        assembler = self._get_assembler(transfer_id)
        assembler.set_expected_chunks(total_chunks, total_size, chunk_size)
        self.assemblers[transfer_id] = assembler
        self.completed_callbacks[transfer_id] = on_complete
        self.error_callbacks[transfer_id] = on_error

    def process_chunk(self, chunk: dict[str, Any]) -> None:
        """Process an incoming chunk for a specific transfer."""
        transfer_id = chunk.get("id") or chunk.get("transferId")
        if not transfer_id:
            return

        assembler = self.assemblers.get(transfer_id)
        if not assembler:
            # Received a chunk for an unknown or completed transfer.
            return

        try:
            # Normalize keys in-place to avoid allocating a new dict.
            # Handles minor key differences between JS/Go/Python senders.
            if "transferId" not in chunk:
                chunk["transferId"] = transfer_id
            if "index" not in chunk and "chunkIndex" in chunk:
                chunk["index"] = chunk["chunkIndex"]

            if assembler.add_chunk(chunk):
                # The transfer is now complete
                data = assembler.get_data()
                callback = self.completed_callbacks.pop(transfer_id, None)

                # Cleanup and return assembler to the pool
                del self.assemblers[transfer_id]
                self.error_callbacks.pop(transfer_id, None)
                self._return_assembler(assembler)

                if callback:
                    callback(data)

        except Exception as e:
            self._handle_error(transfer_id, e)

    def _handle_error(self, transfer_id: str, error: Exception) -> None:
        """Handle a transfer error and perform cleanup."""
        error_callback = self.error_callbacks.pop(transfer_id, None)
        assembler = self.assemblers.pop(transfer_id, None)
        if assembler:
            self._return_assembler(assembler)
        self.completed_callbacks.pop(transfer_id, None)

        if error_callback:
            error_callback(error)

    def cancel(self, transfer_id: str) -> None:
        """Cancel an ongoing transfer."""
        self._handle_error(transfer_id, RuntimeError("Transfer cancelled"))

    def get_progress(self, transfer_id: str) -> dict[str, int | float] | None:
        """Get the progress of a specific transfer."""
        assembler = self.assemblers.get(transfer_id)
        return assembler.get_progress() if assembler else None
