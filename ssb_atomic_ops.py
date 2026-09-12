"""
Platform-specific atomic operations for the lock-free SPMC stream buffer.

This module provides atomic load/store/compare-and-swap operations that match
the C++ std::atomic semantics used in slick::SlickStreamBuffer
(slick/stream_buffer.hpp), so Python and C++ processes sharing a memory
segment synchronize through the same lock-free primitives.

Supported platforms:
- Windows x86-64: Uses C++ std::atomic via the ssb_atomic_ops_ext extension (required)
- Linux x86-64/ARM64: Uses the extension if available, else libc __sync_* / libatomic
- macOS x86-64/ARM64: Uses the extension if available, else libc __sync_* / libatomic

Memory ordering semantics:
- RELAXED: No synchronization or ordering constraints
- ACQUIRE: Subsequent loads/stores cannot be reordered before this operation
- RELEASE: Prior loads/stores cannot be reordered after this operation
- SEQ_CST: Sequential consistency (strongest guarantee)

Note: the Python wrappers always use acquire loads and release stores, even
where the C++ side uses relaxed ordering. Stronger ordering preserves
correctness; the (theoretical) performance difference is irrelevant in Python.
"""
from __future__ import annotations

__version__ = '2.0.0'

import sys
import struct
import ctypes
from typing import Tuple, Union


class MemoryOrder:
    """Memory ordering constants matching C++ std::memory_order."""
    RELAXED = 0  # memory_order_relaxed
    ACQUIRE = 2  # memory_order_acquire
    RELEASE = 3  # memory_order_release
    SEQ_CST = 5  # memory_order_seq_cst


# Platform detection
IS_WINDOWS = sys.platform == 'win32'
IS_LINUX = sys.platform.startswith('linux')
IS_MACOS = sys.platform == 'darwin'
IS_64BIT = sys.maxsize > 2**32


def check_platform_support() -> Tuple[bool, str]:
    """
    Check if current platform supports required atomic operations.

    Returns:
        Tuple of (supported: bool, message: str)
    """
    if not IS_64BIT:
        return False, "64-bit platform required for atomic operations"

    if IS_WINDOWS:
        if _USE_EXTENSION:
            return True, "Windows x86-64 with C++ std::atomic extension"
        else:
            return False, "Windows requires the ssb_atomic_ops_ext C++ extension for cross-process synchronization"
    elif IS_LINUX or IS_MACOS:
        import platform
        machine = platform.machine().lower()
        if machine in ('x86_64', 'amd64'):
            return True, f"{sys.platform} x86-64 with 64-bit atomic operations"
        elif machine in ('aarch64', 'arm64'):
            return True, f"{sys.platform} ARM64 with 64-bit atomic operations"
        else:
            return False, f"Unsupported architecture: {machine}"
    else:
        return False, f"Unsupported platform: {sys.platform}"


def _import_extension():
    """Import ssb_atomic_ops_ext, retrying with this file's directory on sys.path.

    The retry helps with multiprocessing on Windows where child processes may
    not inherit the correct sys.path.
    """
    try:
        import ssb_atomic_ops_ext
        return ssb_atomic_ops_ext
    except ImportError:
        import os
        current_dir = os.path.dirname(os.path.abspath(__file__))
        if current_dir not in sys.path:
            sys.path.insert(0, current_dir)
            try:
                import ssb_atomic_ops_ext
                return ssb_atomic_ops_ext
            except ImportError:
                return None
        return None


def _buffer_addr(buffer: memoryview, offset: int) -> int:
    """Get the raw memory address of buffer[offset]."""
    buf_array = (ctypes.c_char * len(buffer)).from_buffer(buffer)
    return ctypes.addressof(buf_array) + offset


# Initialize platform-specific atomic functions
if IS_WINDOWS:
    # ===== Windows Implementation =====
    # The C++ extension is required: plain Python reads/writes are not
    # guaranteed atomic and MSVC provides no libc __sync_* fallback.
    _atomic_ops_ext = _import_extension()
    _USE_EXTENSION = _atomic_ops_ext is not None

    _EXT_REQUIRED_MSG = (
        "ssb_atomic_ops_ext C++ extension is required for cross-process atomic operations. "
        "Build it with: python setup.py build_ext --inplace"
    )

    def _platform_cas_64(buffer: memoryview, offset: int,
                         expected: int, desired: int) -> Tuple[bool, int]:
        """Windows-specific 64-bit CAS using C++ std::atomic<uint64_t>."""
        if not _USE_EXTENSION:
            raise RuntimeError(_EXT_REQUIRED_MSG)
        addr = _buffer_addr(buffer, offset)
        success, actual = _atomic_ops_ext.atomic_compare_exchange_64(addr, expected, desired)
        return bool(success), actual

    def _atomic_store_64(buffer: memoryview, offset: int, value: int) -> None:
        """Atomic 64-bit store with release semantics."""
        if not _USE_EXTENSION:
            raise RuntimeError(_EXT_REQUIRED_MSG)
        addr = _buffer_addr(buffer, offset)
        _atomic_ops_ext.atomic_store_64(addr, value)

    def _atomic_load_64(buffer: memoryview, offset: int) -> int:
        """Atomic 64-bit load with acquire semantics."""
        if not _USE_EXTENSION:
            raise RuntimeError(_EXT_REQUIRED_MSG)
        addr = _buffer_addr(buffer, offset)
        return _atomic_ops_ext.atomic_load_64(addr)

    def _platform_cas_32(buffer: memoryview, offset: int,
                         expected: int, desired: int) -> Tuple[bool, int]:
        """Windows-specific 32-bit CAS using C++ std::atomic<uint32_t>."""
        if not _USE_EXTENSION:
            raise RuntimeError(_EXT_REQUIRED_MSG)
        addr = _buffer_addr(buffer, offset)
        success, actual = _atomic_ops_ext.atomic_compare_exchange_32(addr, expected, desired)
        return bool(success), actual

    def _atomic_store_32(buffer: memoryview, offset: int, value: int) -> None:
        """Atomic 32-bit store with release semantics."""
        if not _USE_EXTENSION:
            raise RuntimeError(_EXT_REQUIRED_MSG)
        addr = _buffer_addr(buffer, offset)
        _atomic_ops_ext.atomic_store_32(addr, value)

    def _atomic_load_32(buffer: memoryview, offset: int) -> int:
        """Atomic 32-bit load with acquire semantics."""
        if not _USE_EXTENSION:
            raise RuntimeError(_EXT_REQUIRED_MSG)
        addr = _buffer_addr(buffer, offset)
        return _atomic_ops_ext.atomic_load_32(addr)

elif IS_LINUX or IS_MACOS:
    # ===== Linux/macOS Implementation =====
    import ctypes.util

    # Try to use the C++ extension first (most reliable), else native fallbacks.
    _atomic_ops_ext = _import_extension()
    _USE_EXTENSION = _atomic_ops_ext is not None

    # Try to load libatomic
    libatomic_path = ctypes.util.find_library('atomic')
    _HAS_LIBATOMIC = False
    _HAS_LIBATOMIC_32 = False
    if libatomic_path:
        try:
            libatomic = ctypes.CDLL(libatomic_path)
            try:
                libatomic.__atomic_compare_exchange_8.argtypes = [
                    ctypes.POINTER(ctypes.c_uint64),  # ptr
                    ctypes.POINTER(ctypes.c_uint64),  # expected
                    ctypes.c_uint64,                  # desired
                    ctypes.c_int,                     # success_memorder
                    ctypes.c_int                      # failure_memorder
                ]
                libatomic.__atomic_compare_exchange_8.restype = ctypes.c_bool
                _HAS_LIBATOMIC = True
            except AttributeError:
                pass
            try:
                libatomic.__atomic_compare_exchange_4.argtypes = [
                    ctypes.POINTER(ctypes.c_uint32),
                    ctypes.POINTER(ctypes.c_uint32),
                    ctypes.c_uint32,
                    ctypes.c_int,
                    ctypes.c_int
                ]
                libatomic.__atomic_compare_exchange_4.restype = ctypes.c_bool
                _HAS_LIBATOMIC_32 = True
            except AttributeError:
                pass
        except OSError:
            pass

    # Try to load libc for __sync_* builtins
    libc_name = 'c.so.6' if IS_LINUX else 'c'
    _sync_val_cas_8 = None
    _sync_val_cas_4 = None
    _sync_synchronize = None
    try:
        libc = ctypes.CDLL(ctypes.util.find_library(libc_name) or libc_name)

        try:
            _sync_val_cas_8 = libc.__sync_val_compare_and_swap_8
            _sync_val_cas_8.argtypes = [
                ctypes.POINTER(ctypes.c_uint64),
                ctypes.c_uint64,
                ctypes.c_uint64
            ]
            _sync_val_cas_8.restype = ctypes.c_uint64
        except AttributeError:
            _sync_val_cas_8 = None

        try:
            _sync_val_cas_4 = libc.__sync_val_compare_and_swap_4
            _sync_val_cas_4.argtypes = [
                ctypes.POINTER(ctypes.c_uint32),
                ctypes.c_uint32,
                ctypes.c_uint32
            ]
            _sync_val_cas_4.restype = ctypes.c_uint32
        except AttributeError:
            _sync_val_cas_4 = None

        try:
            _sync_synchronize = libc.__sync_synchronize
            _sync_synchronize.restype = None
        except AttributeError:
            _sync_synchronize = None
    except OSError:
        pass

    def _memory_fence_acquire():
        """Acquire memory fence."""
        if _sync_synchronize:
            _sync_synchronize()

    def _memory_fence_release():
        """Release memory fence."""
        if _sync_synchronize:
            _sync_synchronize()

    def _platform_cas_64(buffer: memoryview, offset: int,
                         expected: int, desired: int) -> Tuple[bool, int]:
        """Linux/macOS-specific 64-bit CAS."""
        addr = _buffer_addr(buffer, offset)

        if _USE_EXTENSION:
            success, actual = _atomic_ops_ext.atomic_cas_64(addr, expected, desired)
            return bool(success), actual

        ptr = ctypes.cast(addr, ctypes.POINTER(ctypes.c_uint64))

        if _sync_val_cas_8 is not None:
            actual = _sync_val_cas_8(ptr, expected, desired)
            return actual == expected, actual

        if _HAS_LIBATOMIC:
            expected_ref = ctypes.c_uint64(expected)
            success = libatomic.__atomic_compare_exchange_8(
                ptr,
                ctypes.byref(expected_ref),
                ctypes.c_uint64(desired),
                ctypes.c_int(MemoryOrder.RELEASE),
                ctypes.c_int(MemoryOrder.RELAXED)
            )
            return bool(success), expected_ref.value

        raise RuntimeError("64-bit atomic CAS not available (neither __sync_val_compare_and_swap_8 nor libatomic found)")

    def _atomic_store_64(buffer: memoryview, offset: int, value: int) -> None:
        """Atomic 64-bit store with release semantics."""
        if _USE_EXTENSION:
            addr = _buffer_addr(buffer, offset)
            _atomic_ops_ext.atomic_store_64(addr, value)
            return

        addr = _buffer_addr(buffer, offset)
        ptr = ctypes.cast(addr, ctypes.POINTER(ctypes.c_uint64))

        if _sync_val_cas_8 is not None:
            # Use CAS loop as an atomic store
            while True:
                current = ptr.contents.value
                if _sync_val_cas_8(ptr, current, value) == current:
                    break
        else:
            _memory_fence_release()
            struct.pack_into("<Q", buffer, offset, value)

    def _atomic_load_64(buffer: memoryview, offset: int) -> int:
        """Atomic 64-bit load with acquire semantics."""
        if _USE_EXTENSION:
            addr = _buffer_addr(buffer, offset)
            return _atomic_ops_ext.atomic_load_64(addr)

        # On x86-64/ARM64, aligned 64-bit reads are atomic
        value = struct.unpack_from("<Q", buffer, offset)[0]
        _memory_fence_acquire()
        return value

    def _platform_cas_32(buffer: memoryview, offset: int,
                         expected: int, desired: int) -> Tuple[bool, int]:
        """Linux/macOS-specific 32-bit CAS."""
        addr = _buffer_addr(buffer, offset)

        if _USE_EXTENSION:
            success, actual = _atomic_ops_ext.atomic_cas_32(addr, expected, desired)
            return bool(success), actual

        ptr = ctypes.cast(addr, ctypes.POINTER(ctypes.c_uint32))

        if _sync_val_cas_4 is not None:
            actual = _sync_val_cas_4(ptr, expected, desired)
            return actual == expected, actual

        if _HAS_LIBATOMIC_32:
            expected_ref = ctypes.c_uint32(expected)
            success = libatomic.__atomic_compare_exchange_4(
                ptr,
                ctypes.byref(expected_ref),
                ctypes.c_uint32(desired),
                ctypes.c_int(MemoryOrder.RELEASE),
                ctypes.c_int(MemoryOrder.RELAXED)
            )
            return bool(success), expected_ref.value

        raise RuntimeError("32-bit atomic CAS not available (neither __sync_val_compare_and_swap_4 nor libatomic found)")

    def _atomic_store_32(buffer: memoryview, offset: int, value: int) -> None:
        """Atomic 32-bit store with release semantics."""
        if _USE_EXTENSION:
            addr = _buffer_addr(buffer, offset)
            _atomic_ops_ext.atomic_store_32(addr, value)
            return

        addr = _buffer_addr(buffer, offset)
        ptr = ctypes.cast(addr, ctypes.POINTER(ctypes.c_uint32))

        if _sync_val_cas_4 is not None:
            while True:
                current = ptr.contents.value
                if _sync_val_cas_4(ptr, current, value) == current:
                    break
        else:
            _memory_fence_release()
            struct.pack_into("<I", buffer, offset, value)

    def _atomic_load_32(buffer: memoryview, offset: int) -> int:
        """Atomic 32-bit load with acquire semantics."""
        if _USE_EXTENSION:
            addr = _buffer_addr(buffer, offset)
            return _atomic_ops_ext.atomic_load_32(addr)

        # On x86-64/ARM64, aligned 32-bit reads are atomic
        value = struct.unpack_from("<I", buffer, offset)[0]
        _memory_fence_acquire()
        return value

else:
    # Unsupported platform
    _USE_EXTENSION = False

    def _unsupported(*args, **kwargs):
        raise RuntimeError(f"Atomic operations not supported on {sys.platform}")

    _platform_cas_64 = _unsupported
    _atomic_store_64 = _unsupported
    _atomic_load_64 = _unsupported
    _platform_cas_32 = _unsupported
    _atomic_store_32 = _unsupported
    _atomic_load_32 = _unsupported


class AtomicUInt64:
    """
    Atomic operations on an 8-byte uint64_t value inside a buffer.

    This matches the C++ std::atomic<uint64_t> used for the header cursors
    (committed_/consumed_/next_seq_/reserve_end_) and record.seq in
    slick/stream_buffer.hpp.
    """

    def __init__(self, buffer: Union[memoryview, bytearray], offset: int):
        """
        Initialize atomic uint64_t wrapper.

        Args:
            buffer: Memory buffer (typically SharedMemory.buf or a bytearray)
            offset: Byte offset in buffer (must be 8-byte aligned in memory)
        """
        if isinstance(buffer, bytearray):
            buffer = memoryview(buffer)
        self.buffer = buffer
        self.offset = offset

    def release(self):
        """Release buffer reference to allow proper cleanup."""
        self.buffer = None

    def load_acquire(self) -> int:
        """Load with memory_order_acquire.

        Acquire semantics ensure that subsequent loads/stores cannot be
        reordered before this load. Also used where the C++ side loads
        relaxed (stronger ordering is always correct).
        """
        return _atomic_load_64(self.buffer, self.offset)

    def store_release(self, value: int) -> None:
        """Store with memory_order_release.

        Release semantics ensure that prior loads/stores cannot be
        reordered after this store. Also used where the C++ side stores
        relaxed (stronger ordering is always correct).
        """
        _atomic_store_64(self.buffer, self.offset, value)

    def compare_exchange_weak(self, expected: int, desired: int) -> Tuple[bool, int]:
        """
        Atomic compare-and-swap with memory_order_release on success,
        memory_order_relaxed on failure.

        Returns:
            Tuple of (success: bool, actual: int)
            If success is False, actual contains the current value.
        """
        return _platform_cas_64(self.buffer, self.offset, expected, desired)


class AtomicUInt32:
    """
    Atomic operations on a 4-byte uint32_t value inside a buffer.

    This matches the C++ std::atomic<uint32_t> used for the init_state
    handshake in slick/stream_buffer.hpp (offset 48). Using a real 32-bit
    CAS keeps the operation width identical to the C++ side.
    """

    def __init__(self, buffer: Union[memoryview, bytearray], offset: int):
        """
        Initialize atomic uint32_t wrapper.

        Args:
            buffer: Memory buffer (typically SharedMemory.buf or a bytearray)
            offset: Byte offset in buffer (must be 4-byte aligned in memory)
        """
        if isinstance(buffer, bytearray):
            buffer = memoryview(buffer)
        self.buffer = buffer
        self.offset = offset

    def release(self):
        """Release buffer reference to allow proper cleanup."""
        self.buffer = None

    def load_acquire(self) -> int:
        """Load with memory_order_acquire."""
        return _atomic_load_32(self.buffer, self.offset)

    def store_release(self, value: int) -> None:
        """Store with memory_order_release."""
        _atomic_store_32(self.buffer, self.offset, value)

    def compare_exchange(self, expected: int, desired: int) -> Tuple[bool, int]:
        """
        Atomic compare-and-swap (strong, acq_rel on success) matching the C++
        init_state handshake.

        Returns:
            Tuple of (success: bool, actual: int)
            If success is False, actual contains the current value.
        """
        return _platform_cas_32(self.buffer, self.offset, expected, desired)


__all__ = [
    'AtomicUInt64',
    'AtomicUInt32',
    'MemoryOrder',
    'check_platform_support',
]
