"""
Unit tests for atomic operations (ssb_atomic_ops).

Tests platform-specific 64-bit and 32-bit atomic load/store/CAS operations
and memory ordering semantics used by slick-stream-buffer-py.
"""
import struct
import sys
import multiprocessing
from multiprocessing import shared_memory, Process
from pathlib import Path

if __name__ == '__main__':
    try:
        multiprocessing.set_start_method('spawn', force=False)
    except RuntimeError:
        pass

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ssb_atomic_ops import (
    AtomicUInt64,
    AtomicUInt32,
    check_platform_support
)


def test_platform_support():
    """Test that platform detection works."""
    supported, msg = check_platform_support()
    print(f"Platform support: {supported}")
    print(f"Message: {msg}")
    assert isinstance(supported, bool)
    assert isinstance(msg, str)
    assert supported, f"Platform not supported: {msg}"


def test_atomic_uint64_load_store():
    """Test 64-bit atomic load/store roundtrip."""
    shm = shared_memory.SharedMemory(create=True, size=64)
    try:
        atomic = AtomicUInt64(shm.buf, 16)

        atomic.store_release(0)
        assert atomic.load_acquire() == 0

        atomic.store_release(0xDEADBEEFCAFEBABE)
        assert atomic.load_acquire() == 0xDEADBEEFCAFEBABE

        # max uint64 (kInvalidSeq)
        atomic.store_release(0xFFFFFFFFFFFFFFFF)
        assert atomic.load_acquire() == 0xFFFFFFFFFFFFFFFF

        # Verify raw bytes are little-endian as C++ would see them
        raw = struct.unpack_from("<Q", shm.buf, 16)[0]
        assert raw == 0xFFFFFFFFFFFFFFFF
        atomic.release()
    finally:
        shm.close()
        shm.unlink()


def test_atomic_uint64_cas_success():
    """Test successful 64-bit CAS."""
    shm = shared_memory.SharedMemory(create=True, size=64)
    try:
        atomic = AtomicUInt64(shm.buf, 0)
        atomic.store_release(100)

        success, actual = atomic.compare_exchange_weak(100, 200)
        assert success, "CAS should succeed"
        assert actual == 100, f"Actual should be old value on success, got {actual}"
        assert atomic.load_acquire() == 200
        atomic.release()
    finally:
        shm.close()
        shm.unlink()


def test_atomic_uint64_cas_failure():
    """Test failed 64-bit CAS returns current value."""
    shm = shared_memory.SharedMemory(create=True, size=64)
    try:
        atomic = AtomicUInt64(shm.buf, 0)
        atomic.store_release(42)

        success, actual = atomic.compare_exchange_weak(100, 200)
        assert not success, "CAS should fail"
        assert actual == 42, f"Actual should be current value on failure, got {actual}"
        assert atomic.load_acquire() == 42, "Value should be unchanged"
        atomic.release()
    finally:
        shm.close()
        shm.unlink()


def test_atomic_uint64_adjacent_untouched():
    """64-bit store must not touch adjacent memory."""
    shm = shared_memory.SharedMemory(create=True, size=64)
    try:
        # Fill with a known pattern
        shm.buf[:32] = b'\xAA' * 32
        atomic = AtomicUInt64(shm.buf, 8)
        atomic.store_release(0)

        assert bytes(shm.buf[0:8]) == b'\xAA' * 8, "bytes before must be untouched"
        assert bytes(shm.buf[8:16]) == b'\x00' * 8
        assert bytes(shm.buf[16:32]) == b'\xAA' * 16, "bytes after must be untouched"
        atomic.release()
    finally:
        shm.close()
        shm.unlink()


def test_atomic_uint32_load_store():
    """Test 32-bit atomic load/store roundtrip."""
    shm = shared_memory.SharedMemory(create=True, size=64)
    try:
        atomic = AtomicUInt32(shm.buf, 48)

        atomic.store_release(0)
        assert atomic.load_acquire() == 0

        atomic.store_release(3)  # INIT_STATE_READY
        assert atomic.load_acquire() == 3

        atomic.store_release(0xFFFFFFFF)
        assert atomic.load_acquire() == 0xFFFFFFFF

        raw = struct.unpack_from("<I", shm.buf, 48)[0]
        assert raw == 0xFFFFFFFF
        atomic.release()
    finally:
        shm.close()
        shm.unlink()


def test_atomic_uint32_cas():
    """Test 32-bit CAS success and failure (init_state handshake pattern)."""
    shm = shared_memory.SharedMemory(create=True, size=64)
    try:
        atomic = AtomicUInt32(shm.buf, 48)
        # segment is zero-filled: init_state == UNINITIALIZED (0)

        # CAS 0 -> 2 succeeds (claim ownership)
        success, actual = atomic.compare_exchange(0, 2)
        assert success, "first CAS should succeed"
        assert actual == 0

        # second CAS 0 -> 2 fails (already claimed)
        success, actual = atomic.compare_exchange(0, 2)
        assert not success, "second CAS should fail"
        assert actual == 2, f"actual should be current value 2, got {actual}"

        atomic.store_release(3)
        assert atomic.load_acquire() == 3
        atomic.release()
    finally:
        shm.close()
        shm.unlink()


def test_atomic_uint32_adjacent_untouched():
    """32-bit CAS at offset 48 must not touch bytes 44-47 (header_magic) or 52+."""
    shm = shared_memory.SharedMemory(create=True, size=64)
    try:
        struct.pack_into("<I", shm.buf, 44, 0x53534231)  # 'SSB1' magic
        shm.buf[52:64] = b'\xBB' * 12

        atomic = AtomicUInt32(shm.buf, 48)
        success, _ = atomic.compare_exchange(0, 2)
        assert success

        magic = struct.unpack_from("<I", shm.buf, 44)[0]
        assert magic == 0x53534231, "header_magic must be untouched by init_state CAS"
        assert bytes(shm.buf[52:64]) == b'\xBB' * 12, "padding must be untouched"
        assert struct.unpack_from("<I", shm.buf, 48)[0] == 2
        atomic.release()
    finally:
        shm.close()
        shm.unlink()


def test_local_bytearray_mode():
    """Atomic ops must also work on a local bytearray (local memory mode)."""
    buf = bytearray(64)
    atomic64 = AtomicUInt64(buf, 0)
    atomic64.store_release(12345678901234567)
    assert atomic64.load_acquire() == 12345678901234567

    success, _ = atomic64.compare_exchange_weak(12345678901234567, 42)
    assert success
    assert atomic64.load_acquire() == 42

    atomic32 = AtomicUInt32(buf, 48)
    atomic32.store_release(3)
    assert atomic32.load_acquire() == 3
    atomic64.release()
    atomic32.release()


def _cas_contender(shm_name: str, iterations: int, increment: int):
    """Child process: atomically add `increment` to counter `iterations` times via CAS."""
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).parent.parent))
    from ssb_atomic_ops import AtomicUInt64 as _A64
    from multiprocessing import shared_memory as _shm_mod

    shm = _shm_mod.SharedMemory(name=shm_name, create=False)
    try:
        atomic = _A64(shm.buf, 0)
        for _ in range(iterations):
            while True:
                current = atomic.load_acquire()
                success, _ = atomic.compare_exchange_weak(current, current + increment)
                if success:
                    break
        atomic.release()
    finally:
        try:
            shm._mmap = None
            shm.close()
        except Exception:
            pass


def test_concurrent_cas():
    """Multiple processes CAS-increment a shared counter; total must be exact."""
    shm = shared_memory.SharedMemory(create=True, size=64)
    try:
        atomic = AtomicUInt64(shm.buf, 0)
        atomic.store_release(0)
        atomic.release()

        num_procs = 4
        iterations = 500
        procs = [Process(target=_cas_contender, args=(shm.name, iterations, 1))
                 for _ in range(num_procs)]
        for p in procs:
            p.start()
        for p in procs:
            p.join(timeout=60)
            assert p.exitcode == 0, f"child exited with {p.exitcode}"

        atomic = AtomicUInt64(shm.buf, 0)
        final = atomic.load_acquire()
        atomic.release()
        expected = num_procs * iterations
        print(f"Concurrent CAS: expected={expected}, actual={final}")
        assert final == expected, f"Expected {expected}, got {final}"
    finally:
        shm.close()
        shm.unlink()


def run_all_tests():
    """Run all tests."""
    tests = [
        ("Platform Support", test_platform_support),
        ("AtomicUInt64 Load/Store", test_atomic_uint64_load_store),
        ("AtomicUInt64 CAS Success", test_atomic_uint64_cas_success),
        ("AtomicUInt64 CAS Failure", test_atomic_uint64_cas_failure),
        ("AtomicUInt64 Adjacent Untouched", test_atomic_uint64_adjacent_untouched),
        ("AtomicUInt32 Load/Store", test_atomic_uint32_load_store),
        ("AtomicUInt32 CAS", test_atomic_uint32_cas),
        ("AtomicUInt32 Adjacent Untouched", test_atomic_uint32_adjacent_untouched),
        ("Local bytearray mode", test_local_bytearray_mode),
        ("Concurrent CAS", test_concurrent_cas),
    ]

    print("=" * 70)
    print("Running Atomic Operations Tests")
    print("=" * 70)

    passed = 0
    failed = 0

    for name, test_func in tests:
        try:
            print(f"\n[TEST] {name}...", end=" ")
            test_func()
            print("[PASSED]")
            passed += 1
        except Exception as e:
            print("[FAILED]")
            print(f"  Error: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print("\n" + "=" * 70)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 70)

    return failed == 0


if __name__ == '__main__':
    multiprocessing.freeze_support()
    success = run_all_tests()
    sys.exit(0 if success else 1)
