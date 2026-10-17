"""
slick-stream-buffer-py: A lock-free single-producer multi-consumer byte stream
buffer with shared memory support, binary-compatible with the C++
slick::SlickStreamBuffer (slick/stream_buffer.hpp v2.x, header magic 'SSB1').

A Python producer/consumer and a C++ producer/consumer can communicate
seamlessly through the same shared memory segment.

The producer side exposes a beast::flat_buffer-like interface
(prepare/commit/consume/data/size) so that network bytes can be written
directly into the ring. consume(n) does not discard bytes - it PUBLISHES them
to consumers as one discrete message record. Each consumer owns a monotonic
cursor and reads whole messages as (bytes, length) pairs.

Shared memory layout (matches the layout comment in stream_buffer.hpp
byte-for-byte):

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
- A single message (one consume() call) is limited to < 4 GiB; consume()
  raises ValueError rather than publishing a truncated record length.
- read() takes a per-call configuration (see read_traits): reset detection is
  on by default, loss counting is off, so loss_count() stays 0 unless asked
  for. Records are still skipped correctly either way.
- Only the process that actually creates a segment initializes it. Attaching
  to a name another application owns never writes a byte; a segment left
  wedged by a creator that died mid-initialization is cleared explicitly with
  SlickStreamBuffer.remove(name).
"""
from __future__ import annotations

__version__ = '2.1.0'

import os
import sys
import time
import mmap
import struct
import ctypes
import inspect
from multiprocessing.shared_memory import SharedMemory
from typing import Optional, Tuple, Union

from ssb_atomic_ops import AtomicUInt64, AtomicUInt32

# ---------------------------------------------------------------------------
# Layout constants (must match the C++ header's layout constants exactly)
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

# Python >= 3.13 supports SharedMemory(track=False), which keeps a segment out of
# multiprocessing's resource tracker.
_SHM_SUPPORTS_TRACK = 'track' in inspect.signature(SharedMemory.__init__).parameters

if not _SHM_SUPPORTS_TRACK and sys.platform != 'win32':
    import _posixshmem

    class _UntrackedSharedMemory(SharedMemory):
        """``SharedMemory(track=False)`` for POSIX Python < 3.13, where that
        argument does not exist.

        The base class touches the resource tracker in exactly two places, and
        only those are replaced: ``__init__``, which registers the name, and
        ``unlink()``, which unregisters it. This ``__init__`` sets the same
        attributes the base one does, which ``close()``, ``buf``, ``name``,
        ``size``, ``__del__`` and ``__reduce__`` rely on.

        Sending the tracker nothing at all is what makes this safe. Registering
        and then retracting at once needed a lock against other threads, which a
        fork could leave held in the child, and still raced against other
        processes sharing the tracker - multiprocessing children share their
        parent's - which answers an UNREGISTER for a name it no longer holds by
        printing a KeyError traceback.
        """

        def __init__(self, name: str, create: bool = False, size: int = 0):
            flags = os.O_RDWR
            if create:
                if size <= 0:
                    raise ValueError("'size' must be a positive number different from zero")
                flags |= os.O_CREAT | os.O_EXCL
            self._name = '/' + name
            self._fd = _posixshmem.shm_open(self._name, flags, mode=self._mode)
            try:
                if create:
                    os.ftruncate(self._fd, size)
                size = os.fstat(self._fd).st_size
                self._mmap = mmap.mmap(self._fd, size)
            except OSError:
                os.close(self._fd)
                self._fd = -1
                if create:  # only a segment this call made is ours to delete
                    try:
                        _posixshmem.shm_unlink(self._name)
                    except OSError:
                        pass
                raise
            self._size = size
            self._buf = memoryview(self._mmap)

        def unlink(self) -> None:
            if self._name:
                _posixshmem.shm_unlink(self._name)


def _open_shm(name: str, create: bool, size: int = 0) -> SharedMemory:
    """Create or attach to a segment without registering it with
    multiprocessing's resource tracker.

    A registered name is unlinked by the tracker when the process exits, which on
    POSIX is the lifetime C++ slick-stream-buffer v2.1.0 dropped: a peer still
    attached keeps reading the orphaned mapping while a restarted producer
    silently creates a fresh segment under the same name, so the two never meet
    again. Python before 3.13 registers on attach as well as on create, so any
    peer's exit removed the segment. The name now outlives every process until
    remove_shm(), unlink() or remove() is called. Windows has no tracker; a
    section there dies with its last handle.
    """
    if _SHM_SUPPORTS_TRACK:
        return SharedMemory(name=name, create=create, size=size, track=False)
    if sys.platform == 'win32':
        return SharedMemory(name=name, create=create, size=size)
    return _UntrackedSharedMemory(name, create, size)


def _is_power_of_two(value: int) -> bool:
    """Matches C++ is_power_of_two."""
    return value != 0 and (value & (value - 1)) == 0


def _total_bytes(capacity: int, control_size: int) -> int:
    """Bytes a segment of this geometry occupies (matches C++ total_bytes)."""
    return HEADER_SIZE + RECORD_SIZE * control_size + capacity


def _buffer_base_addr(buffer: memoryview) -> int:
    """Raw memory address of buffer[0]."""
    buf_array = (ctypes.c_char * len(buffer)).from_buffer(buffer)
    return ctypes.addressof(buf_array)


class read_traits:
    """Per-call configuration for :meth:`SlickStreamBuffer.read`, mirroring
    C++ ``slick::read_traits``.

    Ordinary callers write ``buf.read(cursor)`` and never name a traits class.
    Subclass to change a flag::

        class counting(read_traits):
            count_loss = True

        data, length, cursor = buf.read(cursor, counting)

    The configuration rides on ``read()`` rather than on the buffer, so every
    configuration is still the same ``SlickStreamBuffer``.
    """

    #: Resynchronize a consumer whose cursor outlived a ``reset()`` that
    #: restarted the numbering. On by default, as in C++.
    detect_reset = True

    #: Accumulate :meth:`SlickStreamBuffer.loss_count` as records are skipped.
    #: Off by default, as in C++: records are still skipped correctly, only the
    #: counter is silent. Turn it on when diagnosing loss.
    count_loss = False


class counting_read_traits(read_traits):
    """``read_traits`` with loss counting on - the diagnostic configuration."""
    count_loss = True


#: PEP 8 spellings of the traits classes; the snake_case names match C++.
ReadTraits = read_traits
CountingReadTraits = counting_read_traits


class PublishedRecord:
    """The published message record as consumers will see it, returned by
    consume(). Mirrors C++ published_record.

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
        # Tracker-form name of the mapping. Outlives close(), which drops
        # _shm, so unlink() still knows what to unlink - see unlink().
        self._shm_name: Optional[str] = None
        self._local_buf: Optional[bytearray] = None
        self._buf: Optional[memoryview] = None
        self._own = False

        self._capacity = 0
        self._mask = 0
        self._control_size = 0
        self._control_mask = 0
        self._data_offset = 0

        # producer-private state (single thread only). The cursors shadow the
        # shared atomics so publishing never re-loads a counter only the
        # producer writes (matches the C++ *_cursor_ members).
        self._prepared_size = 0
        self._committed_cursor = 0
        self._consumed_cursor = 0
        self._next_seq_cursor = 0
        self._reserve_end_cursor = 0

        # per-instance loss counter (mirrors C++ loss_count_); only accumulated
        # for reads made with a traits whose count_loss is true
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
        if capacity is not None and control_size is not None:
            # A geometry whose total cannot be addressed must be rejected, not
            # truncated into a tiny mapping that would then pass every later
            # size check against its own wrapped value (C++ total_bytes()).
            if _total_bytes(int(capacity), int(control_size)) > sys.maxsize:
                raise ValueError(
                    "capacity plus control ring exceeds the address space of this build")

        if self.use_shm:
            if capacity is not None or control_size is not None:
                # Creator constructor (C++ SlickStreamBuffer(capacity, control_size, name))
                if capacity is None or control_size is None:
                    raise ValueError("capacity and control_size must both be provided when creating")
                self._allocate_shm(int(capacity), int(control_size), open_only=False)
            else:
                # Opener constructor (C++ SlickStreamBuffer(name))
                self._allocate_shm(None, None, open_only=True)
        else:
            # Local memory mode
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
        """Initialize header fields and control ring as the creator.

        Only ever called by the process that actually created the segment."""
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

    def _adopt_shm(self, shm: SharedMemory) -> None:
        """Take over a mapping: hold the object, map its bytes, and remember
        its name. The name is kept separately because close() drops the object
        while the segment itself lives on until something unlinks it."""
        self._shm = shm
        self._shm_name = shm._name
        self._buf = shm.buf

    def _allocate_shm(self, capacity: Optional[int], control_size: Optional[int],
                      open_only: bool) -> None:
        """Maps C++ allocate_shm_data.

        Initialization authority is the OS-backed answer to "did this process
        create the segment?", not a CAS on the in-band init-state slot: a
        foreign segment that happens to share our name is zero-filled too, so a
        CAS on it would win and overwrite bytes we do not own before the magic
        is ever checked. ``SharedMemory(create=True)`` is exactly that answer -
        ``O_CREAT|O_EXCL`` on POSIX, ``ERROR_ALREADY_EXISTS`` on Windows.
        """
        if open_only:
            # Opener constructor - open existing only
            self._adopt_shm(_open_shm(self.name, create=False))
            self._a_init = AtomicUInt32(self._buf, INIT_STATE_OFFSET)
            self._require_mapped_size(HEADER_SIZE)
            self._wait_for_ready()
            self._map_existing(expected_capacity=None, expected_control_size=None)
            return

        # Creator constructor - create or open (C++ open_or_create)
        total = _total_bytes(capacity, control_size)
        try:
            shm = _open_shm(self.name, create=True, size=total)
            created = True
        except FileExistsError:
            shm = _open_shm(self.name, create=False)
            created = False
        self._adopt_shm(shm)
        self._a_init = AtomicUInt32(self._buf, INIT_STATE_OFFSET)

        if not created:
            # The segment pre-existed us - attach to it without touching a byte
            # until _map_existing() has validated its magic, geometry and size.
            self._own = False
            self._require_mapped_size(HEADER_SIZE)
            self._wait_for_ready()
            self._map_existing(expected_capacity=capacity, expected_control_size=control_size)
            return

        # Initialize as creator
        self._own = True
        self._require_mapped_size(total)
        won, _ = self._a_init.compare_exchange(INIT_STATE_UNINITIALIZED, INIT_STATE_INITIALIZING)
        if not won:
            # A freshly created segment is zero-filled by the OS, so this cannot
            # happen unless the mapping is not what the platform reported.
            self._fail("Newly created shared memory segment is already initialized")
        self._set_geometry(capacity, control_size)
        self._write_header_and_control()
        self._build_atomics()
        self._a_init.store_release(INIT_STATE_READY)

    def _fail(self, message: str) -> None:
        """Release the mapping and raise - construction failed after mapping."""
        self._release_mappings()
        raise RuntimeError(message)

    def _require_mapped_size(self, required: int) -> None:
        """Raise unless the mapping holds at least `required` bytes.

        A segment this process did not create can be any size, so every header
        read must be size-checked before it happens (C++ require_mapped_size).
        """
        mapped = len(self._buf)
        if mapped < required:
            self._fail(f"Shared memory segment is too small - need {required} bytes "
                       f"but mapped {mapped}")

    def _stale_segment_message(self, state: int) -> str:
        """Explain a segment that never reached READY and name the recovery step.

        Nothing portable tells a creator that is still working from one that
        died mid-initialization - a stamped PID is meaningless across PID
        namespaces and unreliable under PID reuse - so recovery is deliberately
        an explicit call to remove() (C++ stale_segment_message).
        """
        recovery = (" Once no process is using it, clear it with "
                    f"SlickStreamBuffer.remove(\"{self.name}\") and construct again.")
        if state == INIT_STATE_INITIALIZING:
            return (f"Shared memory segment '{self.name}' is stuck in the INITIALIZING "
                    "state - a creator died part-way through initializing it." + recovery)
        return (f"Timed out waiting for shared memory '{self.name}' to be initialized - "
                "the segment exists but no SlickStreamBuffer creator ever initialized it, "
                "so it may belong to another application." + recovery)

    def _wait_for_ready(self) -> None:
        """Wait for the creator to publish INIT_STATE_READY, raising if it never
        arrives (matches C++ wait_for_shared_memory_ready).

        Fails fast when the magic slot already holds a foreign value: a
        SlickStreamBuffer creator only ever writes HEADER_MAGIC there, so
        anything else non-zero means the name collides with another
        application's segment. The caller must have size-checked the header.

        The 2-second limit is a real wall-clock deadline: counting 1 ms sleeps
        would stretch it by an order of magnitude wherever the OS timer
        granularity rounds them up (~15.6 ms on Windows).
        """
        deadline = time.monotonic() + 2.0
        while True:
            if self._a_init.load_acquire() == INIT_STATE_READY:
                return
            observed = struct.unpack_from("<I", self._buf, HEADER_MAGIC_OFFSET)[0]
            if observed != 0 and observed != HEADER_MAGIC:
                self._fail("Shared memory header magic mismatch - not a SlickStreamBuffer segment")
            if time.monotonic() >= deadline:
                self._fail(self._stale_segment_message(self._a_init.load_acquire()))
            time.sleep(0.001)

    def _map_existing(self, expected_capacity: Optional[int],
                      expected_control_size: Optional[int]) -> None:
        """Adopt geometry from the segment header (matches C++ map_existing).

        Geometry always comes from the header, never from the mapped size:
        Windows rounds mappings up to allocation granularity. The mapping must
        still be large enough to hold what the header declares, or the control
        and data slices would run off the end of it.
        """
        buf = self._buf
        self._require_mapped_size(HEADER_SIZE)

        magic = struct.unpack_from("<I", buf, HEADER_MAGIC_OFFSET)[0]
        if magic != HEADER_MAGIC:
            self._fail("Shared memory header magic mismatch - not a SlickStreamBuffer segment")

        capacity = struct.unpack_from("<Q", buf, CAPACITY_OFFSET)[0]
        control_size = struct.unpack_from("<I", buf, CONTROL_SIZE_OFFSET)[0]
        if not _is_power_of_two(capacity) or not _is_power_of_two(control_size):
            self._fail("Shared memory geometry is invalid")

        # The power-of-two checks above bound both values, so a bogus header
        # geometry cannot wrap the sum past the mapped size.
        if _total_bytes(capacity, control_size) > len(buf):
            self._fail("Shared memory segment is smaller than the geometry its header declares")

        if expected_capacity is not None and (
                capacity != expected_capacity or control_size != expected_control_size):
            self._fail(
                f"Shared memory geometry mismatch. Expected capacity {expected_capacity}"
                f"/control_size {expected_control_size} but got {capacity}/{control_size}")

        self._set_geometry(capacity, control_size)
        self._build_atomics()
        # C++ loads these relaxed; acquire is stronger and equally correct
        self._committed_cursor = self._a_committed.load_acquire()
        self._consumed_cursor = self._a_consumed.load_acquire()
        self._next_seq_cursor = self._a_next_seq.load_acquire()
        self._reserve_end_cursor = self._a_reserve_end.load_acquire()

    def _release_mappings(self) -> None:
        """Drop every reference into the buffer and close the mapping.

        Shared by close() and the construction-failure path: a SharedMemory
        cannot be closed while exported memoryviews still reference it.
        """
        for attr in ('_a_committed', '_a_consumed', '_a_next_seq',
                     '_a_reserve_end', '_a_init'):
            wrapper = getattr(self, attr, None)
            if wrapper is not None:
                try:
                    wrapper.release()
                except Exception:
                    pass
            setattr(self, attr, None)
        for slot in self._a_slot_seq:
            try:
                slot.release()
            except Exception:
                pass
        self._a_slot_seq = []
        self._buf = None
        if self._shm is not None:
            try:
                self._shm._mmap = None
                self._shm.close()
            except Exception:
                pass
            self._shm = None

    @staticmethod
    def remove(name: str) -> bool:
        """Unlink a shared memory segment by name - the stale-segment recovery
        step, mirroring C++ ``stream_buffer::remove()``.

        A creator that dies part-way through initializing a segment leaves it
        stuck in the INITIALIZING state, and on POSIX the name outlives the
        process, so every later creator and opener fails construction with a
        message naming this call. The same call clears a stale but fully
        initialized segment left behind by a crashed producer, which would
        otherwise be attached with its dead cursors intact. Typically called
        once at startup, before creating::

            SlickStreamBuffer.remove("my_stream")
            buf = SlickStreamBuffer(capacity=1 << 20, control_size=4096,
                                    name="my_stream")

        Returns:
            True if the segment was removed (always True on Windows).

        Warning:
            Only call this when no process is using the segment. On POSIX the
            name is unlinked immediately, so the next creator gets a fresh
            segment while any process still mapped to the old one keeps reading
            the orphaned copy. On Windows this is a no-op returning True - a
            section there disappears once the last handle closes.
        """
        if name.startswith('/'):
            name = name[1:]
        if sys.platform == 'win32':
            return True
        # Straight to shm_unlink rather than SharedMemory.unlink(), which before
        # Python 3.13 also retracts a tracker registration this module never keeps -
        # see _open_shm().
        try:
            from _posixshmem import shm_unlink
        except ImportError:  # pragma: no cover - POSIX always has it
            return False
        try:
            shm_unlink('/' + name)
        except OSError:  # not there, or not ours to unlink
            return False
        return True

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

        Counts only reads made with a traits whose ``count_loss`` is true - so
        0 under the default :class:`read_traits`, exactly as in C++. Records are
        still skipped correctly either way; only the counter is silent. Pass
        :class:`counting_read_traits` to ``read()`` when diagnosing loss.
        """
        return self._loss_count

    def initial_reading_index(self) -> int:
        """Get the initial reading cursor for a late-joining consumer.

        A consumer starting at this cursor only sees records published after
        the call.
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
        prepare() are invalidated.

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
        stays prepared."""
        if n > self._prepared_size:
            n = self._prepared_size
        self._committed_cursor += n
        self._a_committed.store_release(self._committed_cursor)
        self._prepared_size -= n

    def consume(self, n: int) -> PublishedRecord:
        """Publish the first n committed bytes to consumers as ONE message
        record. n is clamped to size(); a zero-length consume publishes
        nothing.

        Returns:
            The record as consumers will see it (sequence, data copy, length),
            or a falsy record (data is None) if nothing was published.

        Raises:
            ValueError: if the clamped length does not fit a record's 32-bit
                length field (a single message must be < 4 GiB). The check runs
                before any state changes, so nothing is published and the bytes
                stay readable - publish them as several smaller records instead.
        """
        avail = self._committed_cursor - self._consumed_cursor
        if n > avail:  # clamp like flat_buffer
            n = avail
        if n == 0:
            return PublishedRecord()
        if n > U32_MAX:
            # A record's length field is 32 bits. Truncating here would publish
            # a wrong length and hand consumers a short read of a long message.
            raise ValueError("SlickStreamBuffer.consume exceeds the 4 GiB record limit")

        # Advance next_seq_ BEFORE publishing the record so that every live
        # record always has seq < next_seq_ (consumers use seq >= next_seq_
        # to detect reset()). The value comes from the producer-private shadow:
        # only the producer writes it, so re-loading the shared atomic would
        # just risk a miss on a line consumers keep pulling.
        seq = self._next_seq_cursor
        self._next_seq_cursor = (seq + 1) & U64_MASK
        self._a_next_seq.store_release(self._next_seq_cursor)

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
        websocket disconnect after a partial read).
        """
        self._committed_cursor = self._consumed_cursor
        self._a_committed.store_release(self._committed_cursor)
        self._prepared_size = 0

    def data(self) -> memoryview:
        """Read-only view of the committed-but-unconsumed region (always
        contiguous). Invalidated by the next prepare()."""
        do = self._data_offset
        pos = self._consumed_cursor & self._mask
        return self._buf[do + pos: do + pos + self.size()].toreadonly()

    def size(self) -> int:
        """Number of committed-but-unconsumed bytes."""
        return self._committed_cursor - self._consumed_cursor

    # ------------------------------------------------------------------
    # Consumer side (lock-free)
    # ------------------------------------------------------------------

    def read(self, cursor: int,
             traits: type = read_traits) -> Tuple[Optional[bytes], int, int]:
        """Read the next published message.

        Python has no by-reference ints, so unlike C++ the updated cursor is
        returned: ``data, length, cursor = buf.read(cursor)``.

        Args:
            cursor: this consumer's reading cursor (record sequence number).
            traits: per-call configuration, see :class:`read_traits`. Its
                defaults resynchronize after a ``reset()`` but do not
                accumulate ``loss_count()``; subclass to change that.

        Returns:
            (data, length, new_cursor) - data is None when no message is
            available. Lost (overwritten) records are always skipped; they are
            added to loss_count() only when ``traits.count_loss`` is true.
        """
        detect_reset = traits.detect_reset
        count_loss = traits.count_loss
        while True:
            i = cursor & self._control_mask
            slot = CONTROL_OFFSET + i * RECORD_SIZE
            seq = self._a_slot_seq[i].load_acquire()

            if detect_reset and seq != K_INVALID_SEQ and \
                    seq >= self._a_next_seq.load_acquire():
                # A published seq is always < next_seq_, so seq >= next_seq_ can
                # only mean reset() restarted the numbering under a consumer
                # that was idle at the time.
                cursor = 0
                continue

            if seq == K_INVALID_SEQ or seq < cursor:
                # record not published yet
                return None, 0, cursor

            if seq > cursor:
                # producer lapped the control ring; skip to the record in this slot
                if count_loss:
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
                if count_loss:
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
        accessing the buffer. (Same store order as C++, with next_seq_ zeroed
        last.)
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
        self._next_seq_cursor = 0
        self._reserve_end_cursor = 0
        self._prepared_size = 0
        self._loss_count = 0

    def close(self) -> None:
        """Close the buffer connection.

        For shared memory mode: releases all references to avoid 'exported
        pointers exist' errors. For local memory mode: releases the local
        buffer. Does not delete the segment (see unlink() and remove()).
        """
        self._release_mappings()
        self._local_buf = None

    def unlink(self) -> None:
        """Unlink (delete) the shared memory segment this instance is attached to.

        Only applicable for shared memory mode; no-op for local memory mode.
        Use :meth:`remove` instead to clear a segment by name without attaching
        to it - the stale-segment recovery step.

        No buffer ever does this on its own - not close(), not the context
        manager and not process exit - so call it from the process that
        coordinates the buffer's lifetime, once no peer will attach again.

        Valid after :meth:`close`, which is the documented ``shared_memory``
        idiom - close every handle, then unlink the name once. Deleting a name
        never touches mappings that already exist, open or closed, so this is
        the same by-name operation ``remove()`` performs and carries the same
        caveat: it deletes whatever holds the name now. Unlinking twice is a
        no-op, as is unlinking a segment never attached, so a repeat cannot
        delete a segment a later run has since created under the same name;
        :meth:`remove_shm` is the C++-style call that unlinks on every call.
        """
        if not self.use_shm:
            return
        prefixed = self._shm_name
        if prefixed is None:
            return
        self._shm_name = None  # unlink once, however many times this is called
        try:
            self.remove(prefixed)
        except Exception:
            pass

    def remove_shm(self) -> bool:
        """Unlink this buffer's segment name so the next creator starts fresh.
        Matches C++ ``remove_shm()``.

        Mappings that are already open, including this buffer's own, stay valid
        until they are closed. Unlike :meth:`unlink` it unlinks by name on every
        call, so it deletes whatever holds the name at the time.

        After this call :meth:`unlink` is a no-op, whatever the result: a name
        removed here, or one already gone, can be taken by a replacement segment,
        which the pending unlink() would otherwise delete.

        Returns:
            True if the name was removed; False for a local-memory buffer or if
            removal failed (on POSIX, typically because the name is already
            gone). Always True for a shared buffer on Windows, where the segment
            is freed when its last handle closes.
        """
        if not self.use_shm:
            return False
        self._shm_name = None  # unlink() is done with this name - see above
        return self.remove(self.name)

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
        high-water back. Compared against the producer-private shadow, so the
        common no-op case costs no shared load at all."""
        if self._reserve_end_cursor < end:
            self._reserve_end_cursor = end & U64_MASK
            self._a_reserve_end.store_release(self._reserve_end_cursor)


__all__ = ["SlickStreamBuffer", "PublishedRecord",
           "read_traits", "counting_read_traits",
           "ReadTraits", "CountingReadTraits",
           "__version__", "HEADER_MAGIC", "K_INVALID_SEQ"]
