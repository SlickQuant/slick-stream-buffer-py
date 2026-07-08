"""
slick-stream-buffer-py: A lock-free single-producer multi-consumer byte stream
buffer with shared memory support, binary-compatible with the C++
slick::SlickStreamBuffer (slick/stream_buffer.hpp v1.x, header magic 'SSB1').

A Python producer/consumer and a C++ producer/consumer can communicate
seamlessly through the same shared memory segment.

The producer side exposes a beast::flat_buffer-like interface
(prepare/commit/consume/data/size) so that network bytes can be written
directly into the ring. consume(n) does not discard bytes - it PUBLISHES them
to consumers as one discrete message record. Each consumer owns a monotonic
cursor and reads whole messages as (bytes, length) pairs.

Shared memory layout (matches stream_buffer.hpp:136-167 byte-for-byte):

    [HEADER: 64 bytes]
      Offset 0-7   (8 bytes):  std::atomic<uint64_t> committed_   - monotonic end of committed bytes
      Offset 8-15  (8 bytes):  std::atomic<uint64_t> consumed_    - monotonic publish boundary
      Offset 16-23 (8 bytes):  std::atomic<uint64_t> next_seq_    - next record sequence number
      Offset 24-31 (8 bytes):  std::atomic<uint64_t> reserve_end_ - prepared-region high-water mark
      Offset 32-39 (8 bytes):  capacity_ - data ring size in bytes (uint64_t)
      Offset 40-43 (4 bytes):  control_size_ - control ring record count (uint32_t)
      Offset 44-47 (4 bytes):  header_magic - layout/version marker ('SSB1')
      Offset 48-51 (4 bytes):  init_state - atomic init state (0=uninit,2=init,3=ready)
      Offset 52-63 (12 bytes): PADDING - reserved for future use

    [CONTROL RING: 32 * control_size_ bytes]
      record: seq (atomic uint64) @+0, offset (uint64) @+8, length (uint32) @+16, pad to 32

    [DATA RING: capacity_ bytes]

Caveats (same as C++):
- Producer methods (prepare/commit/consume/discard/data/size/reset) must be
  called from a single thread.
- The buffer is lossy: if the producer outruns a consumer by more than the
  control ring or data ring size, the consumer skips ahead (see loss_count()).
- prepare() may relocate the committed-but-unconsumed region; memoryviews
  previously returned by data() or prepare() are invalidated.
- A single message (one consume() call) is limited to < 4 GiB.
"""
from __future__ import annotations

__version__ = '0.1.0'

import sys
import time
import struct
import ctypes
import inspect
from multiprocessing.shared_memory import SharedMemory
from typing import Optional, Tuple, Union

from ssb_atomic_ops import AtomicUInt64, AtomicUInt32

# ---------------------------------------------------------------------------
# Layout constants (must match stream_buffer.hpp:155-167 exactly)
# ---------------------------------------------------------------------------
HEADER_SIZE = 64
COMMITTED_OFFSET = 0
CONSUMED_OFFSET = 8
NEXT_SEQ_OFFSET = 16
RESERVE_END_OFFSET = 24
CAPACITY_OFFSET = 32
CONTROL_SIZE_OFFSET = 40
HEADER_MAGIC_OFFSET = 44
INIT_STATE_OFFSET = 48
HEADER_MAGIC = 0x53534231  # 'SSB1'
INIT_STATE_UNINITIALIZED = 0
INIT_STATE_INITIALIZING = 2
INIT_STATE_READY = 3

# record: seq (u64) @+0, offset (u64) @+8, length (u32) @+16, pad0 (u32), pad1 (u64)
RECORD_SIZE = 32
CONTROL_OFFSET = HEADER_SIZE
RECORD_BODY_FMT = "<Q I"  # offset @ slot+8, length @ slot+16

K_INVALID_SEQ = 0xFFFFFFFFFFFFFFFF
U64_MASK = 0xFFFFFFFFFFFFFFFF
U32_MAX = 0xFFFFFFFF

# Python >= 3.13 supports SharedMemory(track=False) to opt out of the
# resource tracker's exit-time auto-unlink when merely attaching on POSIX.
_SHM_SUPPORTS_TRACK = 'track' in inspect.signature(SharedMemory.__init__).parameters


def _is_power_of_two(value: int) -> bool:
    """Matches C++ is_power_of_two (stream_buffer.hpp:169-171)."""
    return value != 0 and (value & (value - 1)) == 0


def _buffer_base_addr(buffer: memoryview) -> int:
    """Raw memory address of buffer[0]."""
    buf_array = (ctypes.c_char * len(buffer)).from_buffer(buffer)
    return ctypes.addressof(buf_array)


class PublishedRecord:
    """The published message record as consumers will see it, returned by
    consume(). Mirrors C++ published_record (stream_buffer.hpp:180-185).

    Evaluates to False when nothing was published (zero-length consume).
    Unlike C++, `data` is a bytes copy, not a pointer into the ring.
    """
    __slots__ = ('sequence', 'data', 'length')

    def __init__(self, sequence: int = K_INVALID_SEQ,
                 data: Optional[bytes] = None, length: int = 0):
        self.sequence = sequence
        self.data = data
        self.length = length

    def __bool__(self) -> bool:
        return self.data is not None

    def __repr__(self) -> str:
        return f"PublishedRecord(sequence={self.sequence}, length={self.length})"


class SlickStreamBuffer:
    """A lock-free SPMC byte stream buffer compatible with C++ SlickStreamBuffer.

    Supports three construction modes (mirroring the two C++ constructors):

    - **Local memory mode**: ``SlickStreamBuffer(capacity=1024, control_size=16)``
      Single-process buffer backed by a local bytearray.
    - **Shared memory create-or-open**: ``SlickStreamBuffer(capacity=1<<20, control_size=1<<10, name="md")``
      Creates the segment if it does not exist, otherwise opens it and
      validates that the geometry matches.
    - **Shared memory open existing**: ``SlickStreamBuffer(name="md")``
      Opens an existing segment and adopts its geometry from the header.

    Args:
        capacity: Data ring size in bytes, must be a power of 2.
        control_size: Number of message records in the control ring, must be a power of 2.
        name: Shared memory segment name. If None, uses local memory mode.

    Raises:
        ValueError: If capacity or control_size is not a power of 2.
        RuntimeError: If shared memory mapping/validation fails.
        FileNotFoundError: If opening a non-existent segment (name-only form).
    """

    def __init__(self, *, capacity: Optional[int] = None,
                 control_size: Optional[int] = None,
                 name: Optional[str] = None):
        # Store the original user-provided name (without / prefix).
        # Python's SharedMemory adds the / prefix on POSIX automatically;
        # strip a user-provided leading / to avoid double-prefixing.
        self.name = name
        if self.name is not None and self.name.startswith('/'):
            self.name = self.name[1:]

        # macOS has a 31-character limit for POSIX shared memory names
        # (including the leading /).
        if self.name is not None and sys.platform == 'darwin':
            final_name = '/' + self.name
            if len(final_name) > 31:
                raise ValueError(f"Shared memory name '{final_name}' is {len(final_name)} characters, "
                                 f"but macOS has a 31-character limit. Please use a shorter name.")

        self.use_shm = name is not None
        self._shm: Optional[SharedMemory] = None
        self._local_buf: Optional[bytearray] = None
        self._buf: Optional[memoryview] = None
        self._own = False

        self._capacity = 0
        self._mask = 0
        self._control_size = 0
        self._control_mask = 0
        self._data_offset = 0

        # producer-private state (single thread only)
        self._prepared_size = 0
        self._committed_cursor = 0
        self._consumed_cursor = 0

        # per-instance loss counter (mirrors C++ loss_count_; always enabled in Python)
        self._loss_count = 0

        # atomic wrappers, built once the buffer exists
        self._a_committed: Optional[AtomicUInt64] = None
        self._a_consumed: Optional[AtomicUInt64] = None
        self._a_next_seq: Optional[AtomicUInt64] = None
        self._a_reserve_end: Optional[AtomicUInt64] = None
        self._a_init: Optional[AtomicUInt32] = None
        self._a_slot_seq: list = []

        if capacity is not None and not _is_power_of_two(int(capacity)):
            raise ValueError("capacity must be power of 2")
        if control_size is not None and not _is_power_of_two(int(control_size)):
            raise ValueError("control_size must be power of 2")

        if self.use_shm:
            if capacity is not None or control_size is not None:
                # Creator constructor (C++ stream_buffer.hpp:197-221 with shm_name)
                if capacity is None or control_size is None:
                    raise ValueError("capacity and control_size must both be provided when creating")
                self._allocate_shm(int(capacity), int(control_size), open_only=False)
            else:
                # Opener constructor (C++ stream_buffer.hpp:230-235)
                self._allocate_shm(None, None, open_only=True)
        else:
            # Local memory mode (C++ stream_buffer.hpp:213-220)
            if capacity is None or control_size is None:
                raise ValueError("capacity and control_size required for local memory mode")
            self._init_local(int(capacity), int(control_size))

    # ------------------------------------------------------------------
    # Initialization helpers
    # ------------------------------------------------------------------

    def _set_geometry(self, capacity: int, control_size: int) -> None:
        self._capacity = capacity
        self._mask = capacity - 1
        self._control_size = control_size
        self._control_mask = control_size - 1
        self._data_offset = CONTROL_OFFSET + RECORD_SIZE * control_size

    def _build_atomics(self) -> None:
        buf = self._buf
        self._a_committed = AtomicUInt64(buf, COMMITTED_OFFSET)
        self._a_consumed = AtomicUInt64(buf, CONSUMED_OFFSET)
        self._a_next_seq = AtomicUInt64(buf, NEXT_SEQ_OFFSET)
        self._a_reserve_end = AtomicUInt64(buf, RESERVE_END_OFFSET)
        self._a_init = AtomicUInt32(buf, INIT_STATE_OFFSET)
        self._a_slot_seq = [
            AtomicUInt64(buf, CONTROL_OFFSET + i * RECORD_SIZE)
            for i in range(self._control_size)
        ]

    def _write_header_and_control(self) -> None:
        """Initialize header fields and control ring as the creator
        (matches C++ stream_buffer.hpp:671-681)."""
        buf = self._buf
        # zero the four cursors
        struct.pack_into("<QQQQ", buf, COMMITTED_OFFSET, 0, 0, 0, 0)
        struct.pack_into("<Q", buf, CAPACITY_OFFSET, self._capacity)
        struct.pack_into("<I", buf, CONTROL_SIZE_OFFSET, self._control_size)
        struct.pack_into("<I", buf, HEADER_MAGIC_OFFSET, HEADER_MAGIC)
        # control ring: seq = kInvalidSeq, offset/length/pads = 0
        for i in range(self._control_size):
            off = CONTROL_OFFSET + i * RECORD_SIZE
            struct.pack_into("<QQIIQ", buf, off, K_INVALID_SEQ, 0, 0, 0, 0)

    def _init_local(self, capacity: int, control_size: int) -> None:
        """Local memory mode: same unified layout in a process-local bytearray."""
        self._own = True
        self._set_geometry(capacity, control_size)
        total = HEADER_SIZE + RECORD_SIZE * control_size + capacity
        self._local_buf = bytearray(total)
        self._buf = memoryview(self._local_buf)

        # The atomic extension requires 8-byte aligned addresses. CPython
        # bytearray payloads are at least 8-byte aligned in practice; fail
        # loudly if not.
        if _buffer_base_addr(self._buf) % 8 != 0:
            raise RuntimeError("local buffer is not 8-byte aligned")

        self._write_header_and_control()
        struct.pack_into("<I", self._buf, INIT_STATE_OFFSET, INIT_STATE_READY)
        self._build_atomics()

    @staticmethod
    def _attach_shm(name: str) -> SharedMemory:
        """Attach to an existing segment without registering it with the
        POSIX resource tracker (Python >= 3.13), so an attaching process
        exiting does not unlink a segment it does not own."""
        if _SHM_SUPPORTS_TRACK and sys.platform != 'win32':
            return SharedMemory(name=name, create=False, track=False)
        return SharedMemory(name=name, create=False)

    def _allocate_shm(self, capacity: Optional[int], control_size: Optional[int],
                      open_only: bool) -> None:
        """Maps C++ allocate_shm_data (stream_buffer.hpp:612-696)."""
        if open_only:
            # Opener constructor - open existing only
            self._shm = self._attach_shm(self.name)
            self._buf = self._shm.buf
            self._a_init = AtomicUInt32(self._buf, INIT_STATE_OFFSET)
            if not self._wait_ready():
                self._release_shm_on_error()
                raise RuntimeError("Timed out waiting for shared memory initialization")
            self._map_existing(expected_capacity=None, expected_control_size=None)
            return

        # Creator constructor - create or open (C++ open_or_create)
        total = HEADER_SIZE + RECORD_SIZE * control_size + capacity
        try:
            self._shm = SharedMemory(name=self.name, create=True, size=total)
        except FileExistsError:
            self._shm = self._attach_shm(self.name)
        self._buf = self._shm.buf
        self._a_init = AtomicUInt32(self._buf, INIT_STATE_OFFSET)

        # Atomically claim ownership: CAS init_state UNINITIALIZED -> INITIALIZING
        # (matches C++ stream_buffer.hpp:663-665)
        won, _ = self._a_init.compare_exchange(INIT_STATE_UNINITIALIZED, INIT_STATE_INITIALIZING)

        if won:
            # We are the creator - initialize (stream_buffer.hpp:667-683)
            self._own = True
            self._set_geometry(capacity, control_size)
            self._write_header_and_control()
            self._build_atomics()
            self._a_init.store_release(INIT_STATE_READY)
            self._committed_cursor = 0
            self._consumed_cursor = 0
        else:
            # Segment already existed - wait and validate geometry
            # (stream_buffer.hpp:685-694)
            self._own = False
            if not self._wait_ready():
                self._release_shm_on_error()
                raise RuntimeError("Timed out waiting for shared memory initialization")
            self._map_existing(expected_capacity=capacity, expected_control_size=control_size)

    def _wait_ready(self) -> bool:
        """Wait up to 2000ms for init_state == READY
        (matches C++ wait_for_shared_memory_ready, stream_buffer.hpp:568-577)."""
        for _ in range(2000):
            if self._a_init.load_acquire() == INIT_STATE_READY:
                return True
            time.sleep(0.001)
        return False

    def _map_existing(self, expected_capacity: Optional[int],
                      expected_control_size: Optional[int]) -> None:
        """Adopt geometry from the segment header
        (matches C++ map_existing, stream_buffer.hpp:579-610).

        Geometry always comes from the header, never from the mapped size:
        Windows rounds mappings up to allocation granularity.
        """
        buf = self._buf
        magic = struct.unpack_from("<I", buf, HEADER_MAGIC_OFFSET)[0]
        if magic != HEADER_MAGIC:
            self._release_shm_on_error()
            raise RuntimeError("Shared memory header magic mismatch - not a SlickStreamBuffer segment")

        capacity = struct.unpack_from("<Q", buf, CAPACITY_OFFSET)[0]
        control_size = struct.unpack_from("<I", buf, CONTROL_SIZE_OFFSET)[0]
        if not _is_power_of_two(capacity) or not _is_power_of_two(control_size):
            self._release_shm_on_error()
            raise RuntimeError("Shared memory geometry is invalid")

        if expected_capacity is not None and (
                capacity != expected_capacity or control_size != expected_control_size):
            self._release_shm_on_error()
            raise RuntimeError(
                f"Shared memory geometry mismatch. Expected capacity {expected_capacity}"
                f"/control_size {expected_control_size} but got {capacity}/{control_size}")

        self._set_geometry(capacity, control_size)
        self._build_atomics()
        # C++ loads these relaxed; acquire is stronger and equally correct
        self._committed_cursor = self._a_committed.load_acquire()
        self._consumed_cursor = self._a_consumed.load_acquire()

    def _release_shm_on_error(self) -> None:
        """Best-effort cleanup when construction fails after mapping."""
        if self._a_init is not None:
            self._a_init.release()
            self._a_init = None
        self._buf = None
        if self._shm is not None:
            try:
                self._shm._mmap = None
                self._shm.close()
            except Exception:
                pass
            self._shm = None

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    @property
    def capacity(self) -> int:
        """Data ring capacity in bytes."""
        return self._capacity

    @property
    def control_size(self) -> int:
        """Number of records in the control ring."""
        return self._control_size

    @property
    def own_buffer(self) -> bool:
        """True if this instance created (owns) the memory."""
        return self._own

    def loss_count(self) -> int:
        """Count of records skipped by this instance due to overwrite.

        Note: always counted in Python; the C++ side counts only when
        SLICK_STREAM_BUFFER_ENABLE_LOSS_DETECTION is enabled (debug default).
        """
        return self._loss_count

    def initial_reading_index(self) -> int:
        """Get the initial reading cursor for a late-joining consumer.

        A consumer starting at this cursor only sees records published after
        the call. (Matches C++ stream_buffer.hpp:296-298.)
        """
        return self._a_next_seq.load_acquire()

    def get_shm_name(self) -> Optional[str]:
        """Get the actual shared memory name for C++ interop.

        On POSIX this includes the leading / that C++ shm_open() requires;
        on Windows the name is returned unmodified.
        """
        if self._shm is not None:
            return self._shm._name
        elif self.name is not None:
            if sys.platform != 'win32':
                return '/' + self.name
            return self.name
        return None

    # ------------------------------------------------------------------
    # Producer side (single thread only)
    # ------------------------------------------------------------------

    def prepare(self, n: int) -> memoryview:
        """Get a contiguous writable region of n bytes as a memoryview.

        May relocate the committed-but-unconsumed region to keep it contiguous
        when the ring wraps; memoryviews previously returned by data() or
        prepare() are invalidated. (Matches C++ stream_buffer.hpp:313-348.)

        The returned view is zero-copy: write into it (e.g. sock.recv_into)
        then call commit().

        Raises:
            ValueError: if size() + n exceeds capacity().
        """
        do = self._data_offset
        if n == 0:
            pos = self._committed_cursor & self._mask
            return self._buf[do + pos: do + pos]
        unconsumed = self._committed_cursor - self._consumed_cursor
        if unconsumed + n > self._capacity:
            raise ValueError("SlickStreamBuffer.prepare exceeds capacity")

        pos = self._committed_cursor & self._mask
        if pos + n > self._capacity:
            # Not enough contiguous space at the end of the ring: relocate the
            # unconsumed region to the ring start and jump the monotonic
            # cursors to the next capacity boundary. The relocated bytes are
            # unpublished, so no consumer references them.
            base = self._committed_cursor + (self._capacity - pos)
            # Mark the bytes about to be clobbered as reserved BEFORE writing,
            # so consumers can detect the data lap.
            self._bump_reserve_end(base + unconsumed + n)
            if unconsumed != 0:
                src = self._consumed_cursor & self._mask
                if src != 0:
                    # memoryview slice assignment does not guarantee memmove
                    # semantics for overlapping ranges - copy through a
                    # temporary bytes object.
                    tmp = bytes(self._buf[do + src: do + src + unconsumed])
                    self._buf[do: do + unconsumed] = tmp
            self._consumed_cursor = base
            self._a_consumed.store_release(self._consumed_cursor)
            self._committed_cursor = base + unconsumed
            self._a_committed.store_release(self._committed_cursor)
            pos = unconsumed
        else:
            # bytes are written into the region before commit(), reserve now
            self._bump_reserve_end(self._committed_cursor + n)
        self._prepared_size = n
        return self._buf[do + pos: do + pos + n]

    def commit(self, n: int) -> None:
        """Move n bytes from the prepared region into the committed (readable)
        region. n is clamped to the size of the prepared region; any remainder
        stays prepared. (Matches C++ stream_buffer.hpp:355-363.)"""
        if n > self._prepared_size:
            n = self._prepared_size
        self._committed_cursor += n
        self._a_committed.store_release(self._committed_cursor)
        self._prepared_size -= n

    def consume(self, n: int) -> PublishedRecord:
        """Publish the first n committed bytes to consumers as ONE message
        record. n is clamped to size(); a zero-length consume publishes
        nothing. (Matches C++ stream_buffer.hpp:373-401.)

        Returns:
            The record as consumers will see it (sequence, data copy, length),
            or a falsy record (data is None) if nothing was published.
        """
        avail = self._committed_cursor - self._consumed_cursor
        if n > avail:  # clamp like flat_buffer
            n = avail
        if n == 0:
            return PublishedRecord()
        if n > U32_MAX:
            raise ValueError("a single message must be < 4 GiB")

        # Advance next_seq_ BEFORE publishing the record so that every live
        # record always has seq < next_seq_ (consumers use seq >= next_seq_
        # to detect reset()).
        seq = self._a_next_seq.load_acquire()
        self._a_next_seq.store_release((seq + 1) & U64_MASK)

        i = seq & self._control_mask
        slot = CONTROL_OFFSET + i * RECORD_SIZE
        # Best-effort: invalidate the slot before rewriting its fields so most
        # concurrent readers of a lapped slot observe "not ready" instead of
        # torn fields.
        self._a_slot_seq[i].store_release(K_INVALID_SEQ)
        struct.pack_into(RECORD_BODY_FMT, self._buf, slot + 8,
                         self._consumed_cursor & U64_MASK, n)
        self._a_slot_seq[i].store_release(seq)  # publication edge

        consumed = self._consumed_cursor
        self._consumed_cursor += n
        self._a_consumed.store_release(self._consumed_cursor)

        off = self._data_offset + (consumed & self._mask)
        return PublishedRecord(seq, bytes(self._buf[off: off + n]), n)

    def discard(self) -> None:
        """Discard the committed-but-unconsumed bytes and any prepared region
        without publishing them.

        Use this when the byte stream is interrupted mid-message (e.g. a
        websocket disconnect after a partial read). (Matches C++
        stream_buffer.hpp:417-422.)
        """
        self._committed_cursor = self._consumed_cursor
        self._a_committed.store_release(self._committed_cursor)
        self._prepared_size = 0

    def data(self) -> memoryview:
        """Read-only view of the committed-but-unconsumed region (always
        contiguous). Invalidated by the next prepare(). (Matches C++
        stream_buffer.hpp:427-429.)"""
        do = self._data_offset
        pos = self._consumed_cursor & self._mask
        return self._buf[do + pos: do + pos + self.size()].toreadonly()

    def size(self) -> int:
        """Number of committed-but-unconsumed bytes."""
        return self._committed_cursor - self._consumed_cursor

    # ------------------------------------------------------------------
    # Consumer side (lock-free)
    # ------------------------------------------------------------------

    def read(self, cursor: int) -> Tuple[Optional[bytes], int, int]:
        """Read the next published message.

        Python has no by-reference ints, so unlike C++ the updated cursor is
        returned: ``data, length, cursor = buf.read(cursor)``.

        Returns:
            (data, length, new_cursor) - data is None when no message is
            available. Lost (overwritten) records are skipped automatically
            and counted in loss_count().

        (Matches C++ stream_buffer.hpp:448-492.)
        """
        while True:
            i = cursor & self._control_mask
            slot = CONTROL_OFFSET + i * RECORD_SIZE
            seq = self._a_slot_seq[i].load_acquire()

            if seq != K_INVALID_SEQ and seq >= self._a_next_seq.load_acquire():
                # the buffer has been reset
                cursor = 0
                continue

            if seq == K_INVALID_SEQ or seq < cursor:
                # record not published yet
                return None, 0, cursor

            if seq > cursor:
                # producer lapped the control ring; skip to the record in this slot
                self._loss_count += seq - cursor
                cursor = seq

            # fields are ordered by the acquire load of seq
            offset, length = struct.unpack_from(RECORD_BODY_FMT, self._buf, slot + 8)

            # The control ring and data ring lap independently: a slot can
            # hold a valid seq while its bytes were already overwritten by
            # prepare(). Detect via the prepared high-water mark (best-effort).
            # NOTE: the subtraction must use C++ uint64 wraparound semantics -
            # a plain Python subtraction can go negative and silently invert
            # the verdict.
            if ((self._a_reserve_end.load_acquire() - offset) & U64_MASK) > self._capacity:
                self._loss_count += 1
                cursor = (cursor + 1) & U64_MASK
                continue

            cursor = (seq + 1) & U64_MASK
            off = self._data_offset + (offset & self._mask)
            return bytes(self._buf[off: off + length]), length, cursor

    def read_last(self) -> Tuple[Optional[bytes], int]:
        """Read the most recently published message without a cursor.

        Returns:
            (data, length) or (None, 0) if none is available. May transiently
            return (None, 0) while the newest record is being published or
            after it has been lapped.

        (Matches C++ stream_buffer.hpp:501-517.)
        """
        next_seq = self._a_next_seq.load_acquire()
        if next_seq == 0:
            return None, 0
        seq = next_seq - 1
        i = seq & self._control_mask
        slot = CONTROL_OFFSET + i * RECORD_SIZE
        if self._a_slot_seq[i].load_acquire() != seq:
            return None, 0
        offset, length = struct.unpack_from(RECORD_BODY_FMT, self._buf, slot + 8)
        if ((self._a_reserve_end.load_acquire() - offset) & U64_MASK) > self._capacity:
            return None, 0
        off = self._data_offset + (offset & self._mask)
        return bytes(self._buf[off: off + length]), length

    # ------------------------------------------------------------------
    # Reset / lifecycle
    # ------------------------------------------------------------------

    def reset(self) -> None:
        """Reset the buffer, invalidating all existing data.

        Not thread-safe: call only when no other threads or processes are
        accessing the buffer. (Matches C++ stream_buffer.hpp:525-542; same
        store order, with next_seq_ zeroed last.)
        """
        for i in range(self._control_size):
            off = CONTROL_OFFSET + i * RECORD_SIZE
            struct.pack_into("<QI", self._buf, off + 8, 0, 0)  # offset/length
            self._a_slot_seq[i].store_release(K_INVALID_SEQ)
        self._a_committed.store_release(0)
        self._a_consumed.store_release(0)
        self._a_reserve_end.store_release(0)
        self._a_next_seq.store_release(0)
        self._committed_cursor = 0
        self._consumed_cursor = 0
        self._prepared_size = 0
        self._loss_count = 0

    def close(self) -> None:
        """Close the buffer connection.

        For shared memory mode: releases all references to avoid 'exported
        pointers exist' errors. For local memory mode: releases the local
        buffer. Does not unlink the segment (see unlink()).
        """
        try:
            for attr in ('_a_committed', '_a_consumed', '_a_next_seq',
                         '_a_reserve_end', '_a_init'):
                wrapper = getattr(self, attr, None)
                if wrapper is not None:
                    wrapper.release()
                setattr(self, attr, None)

            if self._a_slot_seq:
                for slot in self._a_slot_seq:
                    slot.release()
            self._a_slot_seq = []

            self._buf = None

            if self.use_shm and self._shm:
                try:
                    # prevent "Exception ignored in: SharedMemory.__del__"
                    # when exported memoryviews still exist
                    self._shm._mmap = None
                    self._shm.close()
                    self._shm = None
                except Exception:
                    pass

            if not self.use_shm and self._local_buf:
                self._local_buf = None
        except Exception as e:
            print(e)

    def unlink(self) -> None:
        """Unlink (delete) the shared memory segment.

        Only applicable for shared memory mode; no-op for local memory mode.
        """
        if not self.use_shm:
            return
        try:
            if self._shm:
                self._shm.unlink()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # noqa: U100
        self.close()
        return False

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _bump_reserve_end(self, end: int) -> None:
        """Monotonic max - a re-prepare with a smaller n must not move the
        high-water back. (Matches C++ stream_buffer.hpp:545-550; single
        producer, so the non-atomic read-modify-write is safe.)"""
        if self._a_reserve_end.load_acquire() < end:
            self._a_reserve_end.store_release(end & U64_MASK)


__all__ = ["SlickStreamBuffer", "PublishedRecord", "__version__",
           "HEADER_MAGIC", "K_INVALID_SEQ"]
