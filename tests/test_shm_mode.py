"""
Shared memory mode tests for SlickStreamBuffer.

Mirrors the C++ test suite in slick-stream-buffer/tests/stream_buffer_shm_tests.cpp,
plus Python-specific regressions (geometry from header despite Windows
mapping-size rounding, cross-process broadcast via multiprocessing).
"""
import struct
import sys
import time
import multiprocessing
from multiprocessing import shared_memory, resource_tracker, Process, Queue
from pathlib import Path

if __name__ == '__main__':
    try:
        multiprocessing.set_start_method('spawn', force=False)
    except RuntimeError:
        pass

sys.path.insert(0, str(Path(__file__).parent.parent))

import slick_stream_buffer_py as ssb
from slick_stream_buffer_py import (
    SlickStreamBuffer,
    HEADER_SIZE,
    RECORD_SIZE,
    HEADER_MAGIC,
    counting_read_traits as dbg,
)

# Loss counting is opt-in, and these tests assert on loss_count(), so they read
# through traits that turn it on - mirroring the `dbg` traits of the C++ suites.
# Reset resynchronization is on by default; test_default_read_traits_are_silent
# covers what the defaults do and do not do.


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


INIT_STATE_INITIALIZING = 2
INIT_STATE_READY = 3
CAPACITY_OFFSET = 32
CONTROL_SIZE_OFFSET = 40
HEADER_MAGIC_OFFSET = 44
INIT_STATE_OFFSET = 48


def _paint_foreign_payload(buf, size: int) -> bytes:
    """Fill a raw segment past the header with a recognizable pattern, so any
    stray write into a segment we do not own shows up."""
    payload = bytes((i * 7 + 1) & 0xFF for i in range(size - HEADER_SIZE))
    buf[HEADER_SIZE:size] = payload
    return payload


def _expect_header_untouched(buf) -> None:
    header = bytes(buf[0:HEADER_SIZE])
    assert header == bytes(HEADER_SIZE), \
        "a creator wrote into the header of a segment it did not create"


def _close_raw(shm) -> None:
    try:
        shm._mmap = None
        shm.close()
    except Exception:
        pass


def _forget_raw(shm) -> None:
    """Retract the tracker registration for a raw segment this test created and
    then disposed of by name rather than through shm.unlink(). Only the process
    that registered a name may retract it, so the test cleans up after its own
    SharedMemory - SlickStreamBuffer.remove() deliberately touches only what the
    library itself registered."""
    if sys.platform == 'win32':
        return
    try:
        resource_tracker.unregister(shm._name, "shared_memory")
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

        data, length, cursor = client.read(0, dbg)
        assert data == b"hello shm"
        assert length == 9
        assert cursor == 1

        data2, _, cursor = client.read(cursor, dbg)
        assert data2 is None

        publish_message(server, b"more")
        data3, length3, cursor = client.read(cursor, dbg)
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
        data, length, _ = client.read(0, dbg)
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
            data, _, cursor = client.read(cursor, dbg)
            assert data is None

            publish_message(server, b"fresh")
            data, length, cursor = client.read(cursor, dbg)
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

        data, length, cursor = client.read(0, dbg)
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
    from slick_stream_buffer_py import (
        SlickStreamBuffer as _SSB,
        counting_read_traits as dbg,
    )
    import time as _time

    client = _SSB(name=name)
    try:
        cursor = 0
        expected = 0
        deadline = _time.time() + 60
        while expected < n_messages:
            data, length, cursor = client.read(cursor, dbg)
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
            data, length, cursor = server.read(cursor, dbg)
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


def test_creator_does_not_overwrite_foreign_zeroed_segment():
    """A foreign segment whose init-state slot happens to be zero must not be
    treated as ours to initialize - only the process that actually created the
    segment has that right, and a CAS on an in-band slot cannot tell a fresh
    segment from a zero-filled foreign one."""
    name = "ssb_py_foreign_zero"
    size = 64 * 1024
    _unlink_raw(name)
    raw = shared_memory.SharedMemory(name=name, create=True, size=size)
    try:
        payload = _paint_foreign_payload(raw.buf, size)
        try:
            SlickStreamBuffer(capacity=1024, control_size=16, name=name)
            assert False, "expected RuntimeError"
        except RuntimeError as e:
            assert "remove(" in str(e), str(e)

        _expect_header_untouched(raw.buf)
        assert bytes(raw.buf[HEADER_SIZE:size]) == payload, \
            "the creator wrote over a foreign segment's payload"
    finally:
        _close_raw(raw)
        _unlink_raw(name)


def test_creator_rejects_foreign_magic_segment():
    """Same collision, but the foreign segment carries a magic of its own - that
    is provably not ours, so the creator rejects it immediately instead of
    waiting out the init handshake."""
    name = "ssb_py_foreign_magic"
    size = 64 * 1024
    _unlink_raw(name)
    raw = shared_memory.SharedMemory(name=name, create=True, size=size)
    try:
        payload = _paint_foreign_payload(raw.buf, size)
        struct.pack_into("<I", raw.buf, HEADER_MAGIC_OFFSET, 0xDEADBEEF)

        start = time.monotonic()
        try:
            SlickStreamBuffer(capacity=1024, control_size=16, name=name)
            assert False, "expected RuntimeError"
        except RuntimeError as e:
            assert "magic mismatch" in str(e), str(e)
        elapsed = time.monotonic() - start
        assert elapsed < 1.0, f"a foreign magic must fail fast, took {elapsed:.2f}s"

        assert struct.unpack_from("<I", raw.buf, HEADER_MAGIC_OFFSET)[0] == 0xDEADBEEF
        assert bytes(raw.buf[HEADER_SIZE:size]) == payload
    finally:
        _close_raw(raw)
        _unlink_raw(name)


def test_undersized_segment_raises():
    """A header whose declared geometry does not fit in the mapped segment must
    be rejected rather than trusted into out-of-bounds control/data slices."""
    name = "ssb_py_undersized"
    _unlink_raw(name)
    raw = shared_memory.SharedMemory(name=name, create=True, size=4096)
    try:
        struct.pack_into("<Q", raw.buf, CAPACITY_OFFSET, 1 << 20)  # far beyond the mapping
        struct.pack_into("<I", raw.buf, CONTROL_SIZE_OFFSET, 16)
        struct.pack_into("<I", raw.buf, HEADER_MAGIC_OFFSET, HEADER_MAGIC)
        struct.pack_into("<I", raw.buf, INIT_STATE_OFFSET, INIT_STATE_READY)

        try:
            SlickStreamBuffer(name=name)
            assert False, "expected RuntimeError"
        except RuntimeError as e:
            assert "smaller than the geometry" in str(e), str(e)
    finally:
        _close_raw(raw)
        _unlink_raw(name)


def test_second_creator_attaches_without_reinitializing():
    """A second creator with matching geometry attaches to the live segment: it
    must not claim ownership and must not reinitialize the cursors out from
    under the first one."""
    name = "ssb_py_second_creator"
    server = SlickStreamBuffer(capacity=1024, control_size=16, name=name)
    attached = None
    try:
        publish_message(server, b"published")

        attached = SlickStreamBuffer(capacity=1024, control_size=16, name=name)
        assert not attached.own_buffer
        assert attached.use_shm
        assert attached.capacity == 1024
        assert attached.control_size == 16

        data, length, _ = attached.read(0, dbg)
        assert data == b"published"
        assert length == 9
    finally:
        _cleanup(*(b for b in (attached, server) if b is not None))


def test_wedged_segment_reports_recovery_step():
    """A creator that dies between INITIALIZING and READY leaves the segment
    wedged. Construction must fail with a message that names the recovery step
    rather than a bare timeout."""
    name = "ssb_py_wedged_msg"
    _unlink_raw(name)
    raw = shared_memory.SharedMemory(name=name, create=True, size=4096)
    try:
        struct.pack_into("<I", raw.buf, INIT_STATE_OFFSET, INIT_STATE_INITIALIZING)
        try:
            SlickStreamBuffer(name=name)
            assert False, "expected RuntimeError"
        except RuntimeError as e:
            what = str(e)
            assert "INITIALIZING" in what, what
            assert f'remove("{name}")' in what, what
    finally:
        _close_raw(raw)
        _unlink_raw(name)


def test_wedged_segment_wait_is_wall_clock():
    """The 2-second wait is a real deadline. Counting 1 ms sleeps instead would
    stretch it by an order of magnitude wherever the OS rounds them up to the
    timer granularity (~15.6 ms on Windows)."""
    name = "ssb_py_wedged_clock"
    _unlink_raw(name)
    raw = shared_memory.SharedMemory(name=name, create=True, size=4096)
    try:
        struct.pack_into("<I", raw.buf, INIT_STATE_OFFSET, INIT_STATE_INITIALIZING)
        start = time.monotonic()
        try:
            SlickStreamBuffer(name=name)
            assert False, "expected RuntimeError"
        except RuntimeError:
            pass
        elapsed = time.monotonic() - start
        assert 1.9 <= elapsed < 5.0, f"expected a ~2s deadline, waited {elapsed:.2f}s"
    finally:
        _close_raw(raw)
        _unlink_raw(name)


def test_remove_clears_wedged_segment():
    """The documented recovery: once nothing is using the wedged segment,
    remove() clears the name and the next creator starts from a fresh segment
    it genuinely owns."""
    name = "ssb_py_wedged"
    _unlink_raw(name)
    raw = shared_memory.SharedMemory(name=name, create=True, size=4096)
    struct.pack_into("<I", raw.buf, INIT_STATE_OFFSET, INIT_STATE_INITIALIZING)
    _close_raw(raw)  # last handle closed - on Windows the section is gone here
    _forget_raw(raw)  # remove() unlinks the name; the registration is ours to drop

    assert SlickStreamBuffer.remove(name)

    server = SlickStreamBuffer(capacity=1024, control_size=16, name=name)
    client = None
    try:
        assert server.own_buffer
        publish_message(server, b"recovered")

        client = SlickStreamBuffer(name=name)
        data, length, _ = client.read(0, dbg)
        assert data == b"recovered"
        assert length == 9
    finally:
        _cleanup(*(b for b in (client, server) if b is not None))


def test_remove_missing_segment_is_harmless():
    """remove() on a name that is not there reports nothing was removed on
    POSIX, and is the documented no-op returning True on Windows."""
    result = SlickStreamBuffer.remove("ssb_py_never_existed_xyz")
    assert result is (sys.platform == 'win32')


def test_unlink_after_close_removes_segment():
    """Regression: unlink() used to be a silent no-op once close() had run,
    because close() drops the SharedMemory object it went through - so the
    documented shared_memory idiom, close every handle then unlink the name
    once, left the segment and its tracker registration behind on POSIX."""
    name = "ssb_py_unlink_after_close"
    _unlink_raw(name)
    buf = SlickStreamBuffer(capacity=1024, control_size=16, name=name)
    prefixed = buf.get_shm_name()
    buf.close()
    buf.unlink()

    try:
        SlickStreamBuffer(name=name)
        assert False, "the segment outlived unlink()"
    except FileNotFoundError:
        pass
    assert prefixed not in ssb._tracked_shm_names, \
        "unlink() left the segment registered with the resource tracker"

    buf.unlink()  # unlinking twice is a no-op, not a second unlink by name


def test_remove_does_not_unregister_unknown_name():
    """Regression: remove() must not ask the resource tracker to unregister a
    name this process never registered. The tracker services UNREGISTER with
    set.remove() on its own cache, so an unknown name makes it raise KeyError
    and print an asynchronous traceback from its own process - and sending the
    message at all starts that process if it was not already running. Startup
    recovery, where the segment is a previous run's, hit this every time."""
    name = "ssb_py_foreign_reg"
    _unlink_raw(name)
    raw = shared_memory.SharedMemory(name=name, create=True, size=4096)
    _close_raw(raw)
    _forget_raw(raw)  # the segment outlives the handle; no registration is ours

    calls = []
    original = resource_tracker.unregister
    resource_tracker.unregister = lambda n, rtype: calls.append((n, rtype))
    try:
        assert SlickStreamBuffer.remove(name) is True
    finally:
        resource_tracker.unregister = original

    assert calls == [], \
        "remove() told the tracker about a name this process never registered"


def test_remove_retracts_own_registration():
    """The other half of the rule: a segment this process created is registered
    with the tracker on POSIX, so remove()ing it by name must retract that
    registration - the tracker would otherwise warn at exit and try to unlink a
    name that is already gone."""
    name = "ssb_py_own_reg"
    _unlink_raw(name)
    buf = SlickStreamBuffer(capacity=1024, control_size=16, name=name)
    prefixed = buf.get_shm_name()  # None once closed, so read it while attached
    buf.close()

    calls = []
    original = resource_tracker.unregister

    def spy(n, rtype):
        calls.append((n, rtype))
        original(n, rtype)  # really retract it - the tracker holds this one

    resource_tracker.unregister = spy
    try:
        assert SlickStreamBuffer.remove(name) is True
    finally:
        resource_tracker.unregister = original

    expected = [] if sys.platform == 'win32' else [(prefixed, "shared_memory")]
    assert calls == expected, calls
    assert prefixed not in ssb._tracked_shm_names

    # And only once: the name is gone from the registry, so a second remove()
    # has nothing left to retract.
    resource_tracker.unregister = spy
    try:
        SlickStreamBuffer.remove(name)
    finally:
        resource_tracker.unregister = original
    assert calls == expected, calls


def test_stale_cursor_after_reset_resynchronizes_via_shm():
    """read() resynchronizes a consumer whose cursor outlived a reset(). The
    condition is a slot holding a sequence at or above next_seq_, which no live
    producer can publish, so it is staged by writing the control slot directly."""
    name = "ssb_py_reset_detect"
    server = SlickStreamBuffer(capacity=1024, control_size=16, name=name)
    client = None
    try:
        publish_message(server, b"first")
        publish_message(server, b"secon")  # next_seq_ == 2

        client = SlickStreamBuffer(name=name)

        raw = shared_memory.SharedMemory(name=client.get_shm_name().lstrip('/'), create=False)
        try:
            slot = HEADER_SIZE + 3 * RECORD_SIZE          # control_[3]
            struct.pack_into("<Q", raw.buf, slot + 8, 0)  # offset -> record 0's bytes
            struct.pack_into("<I", raw.buf, slot + 16, 5)  # length
            struct.pack_into("<Q", raw.buf, slot, 99)     # seq 99 >= next_seq_
        finally:
            _close_raw(raw)

        cursor = 3  # a cursor from before the (simulated) reset
        data, length, cursor = client.read(cursor, dbg)
        assert data == b"first"
        assert length == 5
        assert cursor == 1, f"the consumer should have restarted from the beginning, got {cursor}"
    finally:
        _cleanup(*(b for b in (client, server) if b is not None))


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
        ("CreatorDoesNotOverwriteForeignZeroedSegment",
         test_creator_does_not_overwrite_foreign_zeroed_segment),
        ("CreatorRejectsForeignMagicSegment", test_creator_rejects_foreign_magic_segment),
        ("UndersizedSegmentRaises", test_undersized_segment_raises),
        ("SecondCreatorAttachesWithoutReinitializing",
         test_second_creator_attaches_without_reinitializing),
        ("WedgedSegmentReportsRecoveryStep", test_wedged_segment_reports_recovery_step),
        ("WedgedSegmentWaitIsWallClock", test_wedged_segment_wait_is_wall_clock),
        ("RemoveClearsWedgedSegment", test_remove_clears_wedged_segment),
        ("RemoveMissingSegmentIsHarmless", test_remove_missing_segment_is_harmless),
        ("UnlinkAfterCloseRemovesSegment", test_unlink_after_close_removes_segment),
        ("RemoveDoesNotUnregisterUnknownName", test_remove_does_not_unregister_unknown_name),
        ("RemoveRetractsOwnRegistration", test_remove_retracts_own_registration),
        ("StaleCursorAfterResetResynchronizesViaShm",
         test_stale_cursor_after_reset_resynchronizes_via_shm),
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
