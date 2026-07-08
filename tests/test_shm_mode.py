"""
Shared memory mode tests for SlickStreamBuffer.

Mirrors the C++ test suite in slick-stream-buffer/tests/stream_buffer_shm_tests.cpp,
plus Python-specific regressions (geometry from header despite Windows
mapping-size rounding, cross-process broadcast via multiprocessing).
"""
import struct
import sys
import multiprocessing
from multiprocessing import shared_memory, Process, Queue
from pathlib import Path

if __name__ == '__main__':
    try:
        multiprocessing.set_start_method('spawn', force=False)
    except RuntimeError:
        pass

sys.path.insert(0, str(Path(__file__).parent.parent))

from slick_stream_buffer_py import (
    SlickStreamBuffer,
    HEADER_SIZE,
    RECORD_SIZE,
)


def publish_message(buf: SlickStreamBuffer, data: bytes) -> None:
    mv = buf.prepare(len(data))
    mv[:] = data
    buf.commit(len(data))
    buf.consume(len(data))


def _cleanup(*bufs):
    """Close all instances and unlink via the first one that can."""
    for b in bufs:
        try:
            b.close()
        except Exception:
            pass
    for b in bufs:
        try:
            b.unlink()
            break
        except Exception:
            pass


def _unlink_raw(name: str):
    try:
        shm = shared_memory.SharedMemory(name=name, create=False)
        try:
            shm._mmap = None
            shm.close()
        except Exception:
            pass
        shm.unlink()
    except FileNotFoundError:
        pass


def test_creator_opener_roundtrip():
    server = SlickStreamBuffer(capacity=1024, control_size=16, name="ssb_py_roundtrip")
    client = SlickStreamBuffer(name="ssb_py_roundtrip")
    try:
        assert server.own_buffer
        assert not client.own_buffer
        assert client.use_shm

        publish_message(server, b"hello shm")

        data, length, cursor = client.read(0)
        assert data == b"hello shm"
        assert length == 9
        assert cursor == 1

        data2, _, cursor = client.read(cursor)
        assert data2 is None

        publish_message(server, b"more")
        data3, length3, cursor = client.read(cursor)
        assert data3 == b"more"
        assert length3 == 4
    finally:
        _cleanup(client, server)


def test_opener_reads_geometry():
    server = SlickStreamBuffer(capacity=2048, control_size=32, name="ssb_py_geometry")
    client = SlickStreamBuffer(name="ssb_py_geometry")
    try:
        assert client.capacity == 2048
        assert client.control_size == 32
    finally:
        _cleanup(client, server)


def test_geometry_from_header_not_mapped_size():
    """Windows rounds mappings up to allocation granularity; geometry must
    come from the header, never from the mapped size."""
    server = SlickStreamBuffer(capacity=1024, control_size=16, name="ssb_py_pagesize")
    client = SlickStreamBuffer(name="ssb_py_pagesize")
    try:
        total = HEADER_SIZE + RECORD_SIZE * 16 + 1024
        # the mapped size may legitimately exceed the logical size
        assert client._shm.size >= total
        assert client.capacity == 1024
        assert client.control_size == 16

        # the buffer stays fully functional across the whole logical range
        publish_message(server, b"x" * 1000)
        data, length, _ = client.read(0)
        assert data == b"x" * 1000
        assert length == 1000
    finally:
        _cleanup(client, server)


def test_geometry_mismatch_raises():
    server = SlickStreamBuffer(capacity=1024, control_size=16, name="ssb_py_geo_mismatch")
    try:
        for kwargs in (dict(capacity=2048, control_size=16),
                       dict(capacity=1024, control_size=32)):
            try:
                SlickStreamBuffer(name="ssb_py_geo_mismatch", **kwargs)
                assert False, f"expected RuntimeError for {kwargs}"
            except RuntimeError as e:
                assert "geometry mismatch" in str(e), str(e)
    finally:
        _cleanup(server)


def test_magic_mismatch_raises():
    """A segment that claims to be ready but has the wrong magic must be rejected."""
    _unlink_raw("ssb_py_bad_magic")
    raw = shared_memory.SharedMemory(name="ssb_py_bad_magic", create=True, size=4096)
    try:
        struct.pack_into("<I", raw.buf, 44, 0xDEADBEEF)  # header magic slot
        struct.pack_into("<I", raw.buf, 48, 3)           # INIT_STATE_READY
        try:
            SlickStreamBuffer(name="ssb_py_bad_magic")
            assert False, "expected RuntimeError"
        except RuntimeError as e:
            assert "magic mismatch" in str(e), str(e)
    finally:
        try:
            raw._mmap = None
            raw.close()
        except Exception:
            pass
        try:
            raw.unlink()
        except Exception:
            pass


def test_open_nonexistent_raises():
    try:
        SlickStreamBuffer(name="ssb_py_does_not_exist_xyz")
        assert False, "expected FileNotFoundError"
    except FileNotFoundError:
        pass


def test_late_joiner_via_shm():
    server = SlickStreamBuffer(capacity=1024, control_size=16, name="ssb_py_late")
    try:
        publish_message(server, b"old1")
        publish_message(server, b"old2")

        client = SlickStreamBuffer(name="ssb_py_late")
        try:
            cursor = client.initial_reading_index()
            assert cursor == 2
            data, _, cursor = client.read(cursor)
            assert data is None

            publish_message(server, b"fresh")
            data, length, cursor = client.read(cursor)
            assert data == b"fresh"
            assert length == 5
        finally:
            client.close()
    finally:
        _cleanup(server)


def test_lossy_overwrite_skips_old_data():
    server = SlickStreamBuffer(capacity=1024, control_size=4, name="ssb_py_lossy")
    client = SlickStreamBuffer(name="ssb_py_lossy")
    try:
        for i in range(8):
            publish_message(server, bytes([i]))

        data, length, cursor = client.read(0)
        assert data is not None
        assert length == 1
        assert data[0] == 4
        assert client.loss_count() == 4
    finally:
        _cleanup(client, server)


def test_read_last_via_shm():
    server = SlickStreamBuffer(capacity=1024, control_size=16, name="ssb_py_read_last")
    client = SlickStreamBuffer(name="ssb_py_read_last")
    try:
        publish_message(server, b"first")
        publish_message(server, b"second")

        data, length = client.read_last()
        assert data == b"second"
        assert length == 6
    finally:
        _cleanup(client, server)


def _broadcast_consumer(name: str, n_messages: int, result_q):
    """Child process: attach and read n_messages sequential uint64 values."""
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).parent.parent))
    from slick_stream_buffer_py import SlickStreamBuffer as _SSB
    import time as _time

    client = _SSB(name=name)
    try:
        cursor = 0
        expected = 0
        deadline = _time.time() + 60
        while expected < n_messages:
            data, length, cursor = client.read(cursor)
            if data is None:
                if _time.time() > deadline:
                    result_q.put(('timeout', expected))
                    return
                _time.sleep(0)
                continue
            value = int.from_bytes(data, 'little')
            if length != 8 or value != expected:
                result_q.put(('mismatch', expected))
                return
            expected += 1
        result_q.put(('ok', expected))
    finally:
        client.close()


def test_broadcast_two_process_consumers():
    k_messages = 200
    server = SlickStreamBuffer(capacity=1 << 16, control_size=1024, name="ssb_py_broadcast")
    try:
        result_q = Queue()
        consumers = [
            Process(target=_broadcast_consumer, args=("ssb_py_broadcast", k_messages, result_q))
            for _ in range(2)
        ]
        for p in consumers:
            p.start()

        for i in range(k_messages):
            mv = server.prepare(8)
            mv[:] = i.to_bytes(8, 'little')
            server.commit(8)
            server.consume(8)

        results = [result_q.get(timeout=90) for _ in consumers]
        for p in consumers:
            p.join(timeout=30)

        for status, count in results:
            assert status == 'ok', f"consumer failed: {status} after {count} messages"
            assert count == k_messages
    finally:
        _cleanup(server)


def test_cross_process_producer():
    """Python consumer in this process; Python producer in a child process."""
    server = SlickStreamBuffer(capacity=1 << 12, control_size=64, name="ssb_py_xproc")
    try:
        n = 50
        p = Process(target=_child_producer, args=("ssb_py_xproc", n))
        p.start()

        cursor = 0
        received = []
        import time as _time
        deadline = _time.time() + 60
        while len(received) < n and _time.time() < deadline:
            data, length, cursor = server.read(cursor)
            if data is None:
                _time.sleep(0)
                continue
            received.append(data)
        p.join(timeout=30)
        assert p.exitcode == 0

        assert len(received) == n
        for i, data in enumerate(received):
            assert data == f"msg-{i:04d}".encode(), f"message {i} corrupted: {data!r}"
    finally:
        _cleanup(server)


def _child_producer(name: str, n_messages: int):
    """Child process: attach as opener and publish n_messages."""
    import sys as _sys
    from pathlib import Path as _Path
    _sys.path.insert(0, str(_Path(__file__).parent.parent))
    from slick_stream_buffer_py import SlickStreamBuffer as _SSB

    buf = _SSB(name=name)
    try:
        for i in range(n_messages):
            data = f"msg-{i:04d}".encode()
            mv = buf.prepare(len(data))
            mv[:] = data
            buf.commit(len(data))
            buf.consume(len(data))
    finally:
        buf.close()


def run_all_tests():
    tests = [
        ("CreatorOpenerRoundtrip", test_creator_opener_roundtrip),
        ("OpenerReadsGeometry", test_opener_reads_geometry),
        ("GeometryFromHeaderNotMappedSize", test_geometry_from_header_not_mapped_size),
        ("GeometryMismatchRaises", test_geometry_mismatch_raises),
        ("MagicMismatchRaises", test_magic_mismatch_raises),
        ("OpenNonexistentRaises", test_open_nonexistent_raises),
        ("LateJoinerViaShm", test_late_joiner_via_shm),
        ("LossyOverwriteSkipsOldData", test_lossy_overwrite_skips_old_data),
        ("ReadLastViaShm", test_read_last_via_shm),
        ("BroadcastTwoProcessConsumers", test_broadcast_two_process_consumers),
        ("CrossProcessProducer", test_cross_process_producer),
    ]

    print("=" * 70)
    print("Running Shared Memory Mode Tests")
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
