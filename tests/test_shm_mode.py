"""
Shared memory mode tests for SlickStreamBuffer.

Mirrors the C++ test suite in slick-stream-buffer/tests/stream_buffer_shm_tests.cpp,
plus Python-specific regressions (geometry from header despite Windows
mapping-size rounding, cross-process broadcast via multiprocessing).
"""
import os
import signal
import struct
import subprocess
import sys
import tempfile
import threading
import time
import warnings
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
    # Through the library's own opener, so this raw handle and the buffer below do
    # not both claim the name in the resource tracker before Python 3.13
    raw = ssb._open_shm("ssb_py_bad_magic", create=True, size=4096)
    try:
        struct.pack_into("<I", raw.buf, 44, 0xDEADBEEF)  # header magic slot
        struct.pack_into("<I", raw.buf, 48, 3)           # INIT_STATE_READY
        try:
            SlickStreamBuffer(name="ssb_py_bad_magic")
            assert False, "expected RuntimeError"
        except RuntimeError as e:
            assert "magic mismatch" in str(e), str(e)
    finally:
        _close_raw(raw)
        SlickStreamBuffer.remove("ssb_py_bad_magic")


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
    once, left the segment behind on POSIX."""
    name = "ssb_py_unlink_after_close"
    _unlink_raw(name)
    buf = SlickStreamBuffer(capacity=1024, control_size=16, name=name)
    buf.close()
    buf.unlink()

    try:
        SlickStreamBuffer(name=name)
        assert False, "the segment outlived unlink()"
    except FileNotFoundError:
        pass


def test_unlink_twice_is_noop():
    """A repeated unlink() must not delete a segment a later run created under
    the same name."""
    name = "ssb_py_unlink_twice"
    _unlink_raw(name)
    first = SlickStreamBuffer(capacity=1024, control_size=16, name=name)
    first.close()
    first.unlink()

    second = SlickStreamBuffer(capacity=1024, control_size=16, name=name)
    try:
        assert second.own_buffer
        first.unlink()
        SlickStreamBuffer(name=name).close()  # still there
    finally:
        _cleanup(second)


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


def test_remove_leaves_tracker_alone_for_own_segment():
    """A buffer never leaves its segment registered with the resource tracker,
    so remove()ing one this process created has nothing to retract either."""
    name = "ssb_py_own_reg"
    _unlink_raw(name)
    buf = SlickStreamBuffer(capacity=1024, control_size=16, name=name)
    buf.close()

    calls = []
    original = resource_tracker.unregister
    resource_tracker.unregister = lambda n, rtype: calls.append((n, rtype))
    try:
        assert SlickStreamBuffer.remove(name) is True
    finally:
        resource_tracker.unregister = original
    assert calls == [], calls


def test_remove_shm_local_buffer_returns_false():
    buf = SlickStreamBuffer(capacity=1024, control_size=16)
    try:
        assert buf.remove_shm() is False
    finally:
        buf.close()


def test_remove_shm_keeps_open_mapping_valid():
    name = "ssb_py_remove_shm"
    _unlink_raw(name)
    buf = SlickStreamBuffer(capacity=1024, control_size=16, name=name)
    try:
        publish_message(buf, b"before")
        assert buf.remove_shm() is True
        # The mapping outlives its name
        publish_message(buf, b"after")
        data, _, cursor = buf.read(0, dbg)
        assert data == b"before"
        data, _, _ = buf.read(cursor, dbg)
        assert data == b"after"

        if sys.platform != 'win32':
            # The next creator starts fresh rather than reattaching
            assert buf.remove_shm() is False  # already gone
            fresh = SlickStreamBuffer(capacity=1024, control_size=16, name=name)
            try:
                assert fresh.own_buffer
                assert fresh.read(0, dbg)[0] is None
            finally:
                _cleanup(fresh)
    finally:
        _cleanup(buf)


# A fresh interpreter, so it has its own resource tracker - a multiprocessing
# child would share the parent's, and the parent's exit would be what unlinked
# the name.
_LIFETIME_CHILD = r"""
import sys
sys.path.insert(0, sys.argv[1])
from slick_stream_buffer_py import SlickStreamBuffer
name, mode, payload = sys.argv[2], sys.argv[3], sys.argv[4].encode()
if mode == 'create':
    buf = SlickStreamBuffer(capacity=1024, control_size=16, name=name)
    assert buf.own_buffer, "child expected to create the segment"
else:
    buf = SlickStreamBuffer(name=name)
mv = buf.prepare(len(payload))
mv[:] = payload
buf.commit(len(payload))
buf.consume(len(payload))
buf.close()
"""


def _run_lifetime_child(name: str, mode: str, payload: str) -> None:
    result = subprocess.run(
        [sys.executable, "-c", _LIFETIME_CHILD, str(Path(__file__).parent.parent),
         name, mode, payload],
        capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, f"child failed:\n{result.stdout}\n{result.stderr}"
    # The tracker warns about every registered name it cleans up at shutdown
    assert "resource_tracker" not in result.stderr, result.stderr


def test_creator_exit_keeps_segment():
    """A creator's exit used to unlink the name on POSIX through the resource
    tracker, stranding any peer on an orphaned mapping (C++ v2.1.0 stopped its
    destructor doing the same). On Windows a section dies with its last handle,
    which no one else holds here."""
    if sys.platform == 'win32':
        return
    name = "ssb_py_creator_exit"
    _unlink_raw(name)
    _run_lifetime_child(name, 'create', "from child")
    buf = SlickStreamBuffer(name=name)
    try:
        assert buf.read(0, dbg)[0] == b"from child"
        # A restarted creator reattaches and keeps the contents
        again = SlickStreamBuffer(capacity=1024, control_size=16, name=name)
        try:
            assert not again.own_buffer
        finally:
            again.close()
    finally:
        _cleanup(buf)


def test_attacher_exit_keeps_segment():
    """Before Python 3.13 attaching registered the name with the attacher's
    resource tracker too, so any peer's exit unlinked the segment on POSIX."""
    name = "ssb_py_attacher_exit"
    _unlink_raw(name)
    buf = SlickStreamBuffer(capacity=1024, control_size=16, name=name)
    try:
        _run_lifetime_child(name, 'open', "first")
        _run_lifetime_child(name, 'open', "second")
        reopened = SlickStreamBuffer(name=name)
        try:
            data, _, cursor = reopened.read(0, dbg)
            assert data == b"first"
            assert reopened.read(cursor, dbg)[0] == b"second"
        finally:
            reopened.close()
    finally:
        _cleanup(buf)


def test_unlink_after_remove_shm_spares_replacement():
    """Regression: remove_shm() left unlink()'s pending name in place, so once the
    old segment was removed and a replacement created under the same name, the
    old buffer's unlink() deleted the replacement. Also when remove_shm() finds
    the name already gone, since a replacement can take it just the same."""
    name = "ssb_py_rm_unlink"
    for gone_first in (False, True):
        SlickStreamBuffer.remove(name)
        old = SlickStreamBuffer(capacity=1024, control_size=16, name=name)
        try:
            if gone_first:
                SlickStreamBuffer.remove(name)  # someone else got there first
                assert old.remove_shm() is (sys.platform == 'win32')
            else:
                assert old.remove_shm() is True
            replacement = SlickStreamBuffer(capacity=1024, control_size=16, name=name)
            try:
                if sys.platform != 'win32':
                    assert replacement.own_buffer
                old.unlink()
                SlickStreamBuffer(name=name).close()  # still there
            finally:
                _cleanup(replacement)
        finally:
            old.close()


def test_never_talks_to_resource_tracker():
    """No REGISTER or UNREGISTER for a buffer's segment reaches
    multiprocessing's resource tracker - not on create, attach, close or
    unlink. Before Python 3.13 the library registered each name and then
    retracted it at once, which raced between processes sharing one tracker
    and needed a lock that a fork could leave held (see the two tests below)."""
    name = "ssb_py_no_tracker"
    SlickStreamBuffer.remove(name)
    calls = []
    orig_register, orig_unregister = resource_tracker.register, resource_tracker.unregister
    resource_tracker.register = lambda n, rtype: calls.append(("register", n))
    resource_tracker.unregister = lambda n, rtype: calls.append(("unregister", n))
    try:
        creator = SlickStreamBuffer(capacity=1024, control_size=16, name=name)
        opener = SlickStreamBuffer(name=name)
        second = SlickStreamBuffer(capacity=1024, control_size=16, name=name)  # attaches
        for buf in (second, opener, creator):
            buf.close()
        creator.unlink()
    finally:
        resource_tracker.register, resource_tracker.unregister = orig_register, orig_unregister
        SlickStreamBuffer.remove(name)
    assert calls == [], calls


def test_fork_during_construction_does_not_deadlock():
    """A child forked while another thread is constructing a buffer can still
    construct one. Python < 3.13 held a module lock around SharedMemory() for
    the tracker fallback, and a fork inside that window handed the child a copy
    of the lock that nothing would ever release, so its next construction hung."""
    if not hasattr(os, 'fork'):
        return
    name = "ssb_py_fork"
    SlickStreamBuffer.remove(name)
    keeper = SlickStreamBuffer(capacity=1024, control_size=16, name=name)
    stop = threading.Event()

    def churn():
        while not stop.is_set():
            SlickStreamBuffer(name=name).close()

    thread = threading.Thread(target=churn, daemon=True)
    thread.start()
    try:
        with warnings.catch_warnings():
            # Python 3.12+ warns about forking a multi-threaded process
            warnings.simplefilter("ignore", DeprecationWarning)
            for attempt in range(20):
                pid = os.fork()
                if pid == 0:
                    try:
                        SlickStreamBuffer(name=name).close()
                    except BaseException:
                        os._exit(1)
                    os._exit(0)
                deadline = time.monotonic() + 5
                while True:
                    done, status = os.waitpid(pid, os.WNOHANG)
                    if done:
                        break
                    if time.monotonic() > deadline:
                        os.kill(pid, signal.SIGKILL)
                        os.waitpid(pid, 0)
                        raise AssertionError(
                            f"forked child {attempt} hung constructing a buffer")
                    time.sleep(0.01)
                assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, \
                    f"forked child {attempt} failed (status {status})"
    finally:
        stop.set()
        thread.join()
        _cleanup(keeper)


_RELATED_PROCESSES_SCRIPT = r"""
import multiprocessing
import sys
sys.path.insert(0, sys.argv[1])
from slick_stream_buffer_py import SlickStreamBuffer


def churn(name, n):
    for _ in range(n):
        SlickStreamBuffer(name=name).close()


if __name__ == '__main__':
    name = sys.argv[2]
    SlickStreamBuffer.remove(name)
    buf = SlickStreamBuffer(capacity=1024, control_size=16, name=name)
    ctx = multiprocessing.get_context('spawn')
    procs = [ctx.Process(target=churn, args=(name, 200)) for _ in range(4)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    buf.close()
    buf.unlink()
    # Every worker must succeed: max() let a killed one's negative code hide
    # behind any worker that exited 0
    codes = [p.exitcode for p in procs]
    if codes != [0] * len(procs):
        sys.exit(f"worker exit codes: {codes}")
"""


def test_related_processes_do_not_race_in_tracker():
    """multiprocessing children share their parent's resource tracker, so
    before Python 3.13 two of them attaching at once could send REGISTER,
    REGISTER, UNREGISTER, UNREGISTER and make the tracker print a KeyError
    traceback. The tracker writes to the stderr it inherited, so the scenario
    runs in a fresh interpreter whose stderr this test captures. A spawned
    child re-imports the script by path, hence a file rather than -c."""
    with tempfile.TemporaryDirectory() as tmp:
        script = Path(tmp) / "related_processes.py"
        script.write_text(_RELATED_PROCESSES_SCRIPT)
        result = subprocess.run(
            [sys.executable, str(script), str(Path(__file__).parent.parent),
             "ssb_py_related"],
            capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, f"script failed:\n{result.stdout}\n{result.stderr}"
    assert "KeyError" not in result.stderr, result.stderr
    assert "resource_tracker" not in result.stderr, result.stderr


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

        raw = ssb._open_shm(client.get_shm_name().lstrip('/'), create=False)
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
        ("UnlinkTwiceIsNoop", test_unlink_twice_is_noop),
        ("RemoveDoesNotUnregisterUnknownName", test_remove_does_not_unregister_unknown_name),
        ("RemoveLeavesTrackerAloneForOwnSegment",
         test_remove_leaves_tracker_alone_for_own_segment),
        ("RemoveShmLocalBufferReturnsFalse", test_remove_shm_local_buffer_returns_false),
        ("RemoveShmKeepsOpenMappingValid", test_remove_shm_keeps_open_mapping_valid),
        ("CreatorExitKeepsSegment", test_creator_exit_keeps_segment),
        ("AttacherExitKeepsSegment", test_attacher_exit_keeps_segment),
        ("UnlinkAfterRemoveShmSparesReplacement",
         test_unlink_after_remove_shm_spares_replacement),
        ("NeverTalksToResourceTracker", test_never_talks_to_resource_tracker),
        ("ForkDuringConstructionDoesNotDeadlock",
         test_fork_during_construction_does_not_deadlock),
        ("RelatedProcessesDoNotRaceInTracker",
         test_related_processes_do_not_race_in_tracker),
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
