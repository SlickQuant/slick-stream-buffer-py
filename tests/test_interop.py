"""
C++/Python interoperability tests for SlickStreamBuffer.

Verifies binary compatibility of the shared memory layout by exchanging
messages between real C++ processes (built against the actual
slick/stream_buffer.hpp) and this Python implementation, in both directions.

The framed message format must match tests/interop_common.hpp exactly:
    u32 worker_id (LE) | u32 msg_num (LE) | payload
    total length = 8 + (msg_num * 7) % 57
    payload fill = (msg_num * 31 + 7) & 0xFF

Requires the C++ test binaries to be built first:
    cmake -S . -B build && cmake --build build --config Debug
"""
import argparse
import struct
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from slick_stream_buffer_py import SlickStreamBuffer, counting_read_traits as dbg


# ---------------------------------------------------------------------------
# Message framing (must match interop_common.hpp)
# ---------------------------------------------------------------------------

def interop_msg_length(msg_num: int) -> int:
    return 8 + (msg_num * 7) % 57


def interop_fill_byte(msg_num: int) -> int:
    return (msg_num * 31 + 7) & 0xFF


def interop_make_msg(worker_id: int, msg_num: int) -> bytes:
    length = interop_msg_length(msg_num)
    return struct.pack("<II", worker_id, msg_num) + bytes([interop_fill_byte(msg_num)]) * (length - 8)


def interop_check_msg(data: bytes, expected_worker: int) -> int:
    """Validate a received message; returns msg_num or raises AssertionError."""
    assert len(data) >= 8, f"message too short: {len(data)}"
    worker_id, msg_num = struct.unpack_from("<II", data, 0)
    assert worker_id == expected_worker, f"worker_id {worker_id} != {expected_worker}"
    assert len(data) == interop_msg_length(msg_num), \
        f"length {len(data)} != expected {interop_msg_length(msg_num)} for msg {msg_num}"
    fill = interop_fill_byte(msg_num)
    assert all(b == fill for b in data[8:]), f"payload corrupted in msg {msg_num}"
    return msg_num


def publish_msg(buf: SlickStreamBuffer, worker_id: int, msg_num: int) -> None:
    data = interop_make_msg(worker_id, msg_num)
    mv = buf.prepare(len(data))
    mv[:] = data
    buf.commit(len(data))
    buf.consume(len(data))


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------

def find_cpp_executable(name: str) -> str:
    """Find a C++ test executable in the CMake build directory."""
    root = Path(__file__).parent.parent
    search_paths = [
        root / "build" / name,
        root / "build" / "tests" / name,
        root / "build" / f"{name}.exe",
        root / "build" / "tests" / "Debug" / f"{name}.exe",
        root / "build" / "tests" / "Release" / f"{name}.exe",
        root / "build" / "tests" / f"{name}.exe",
    ]
    for path in search_paths:
        if path.exists():
            return str(path)
    raise FileNotFoundError(
        f"C++ executable '{name}' not found. Build the project first: "
        f"cmake -S . -B build && cmake --build build --config Debug")


def python_read_strict(buf: SlickStreamBuffer, worker_id: int, n_messages: int,
                       timeout_s: float = 30.0) -> None:
    """Read exactly n_messages sequential messages from cursor 0."""
    cursor = 0
    received = 0
    deadline = time.time() + timeout_s
    while received < n_messages:
        assert time.time() < deadline, f"timed out after {received} messages"
        data, length, cursor = buf.read(cursor, dbg)
        if data is None:
            time.sleep(0)
            continue
        msg_num = interop_check_msg(data, worker_id)
        assert msg_num == received, f"expected msg {received}, got {msg_num}"
        received += 1


def python_read_lossy(buf: SlickStreamBuffer, worker_id: int, n_messages: int,
                      timeout_s: float = 30.0) -> int:
    """Read until msg n_messages-1 is seen; verify content and monotonic order.
    Returns the number of messages received (loss allowed)."""
    cursor = 0
    received = 0
    last_msg_num = -1
    deadline = time.time() + timeout_s
    while True:
        assert time.time() < deadline, \
            f"timed out after {received} messages (last msg {last_msg_num})"
        data, length, cursor = buf.read(cursor, dbg)
        if data is None:
            time.sleep(0)
            continue
        msg_num = interop_check_msg(data, worker_id)
        assert msg_num > last_msg_num, f"msg_num went backwards: {msg_num} after {last_msg_num}"
        last_msg_num = msg_num
        received += 1
        if msg_num == n_messages - 1:
            return received


def _cleanup(buf: SlickStreamBuffer) -> None:
    try:
        buf.close()
    except Exception:
        pass
    try:
        buf.unlink()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_python_producer_cpp_consumer():
    """Python creates + publishes; C++ consumes all messages strictly in order."""
    print("TEST: Python producer -> C++ consumer")
    n = 100
    worker = 1
    buf = SlickStreamBuffer(capacity=1 << 16, control_size=1024, name="ssb_interop_py2cpp")
    try:
        consumer = subprocess.Popen(
            [find_cpp_executable("cpp_consumer"), buf.get_shm_name(), str(n), str(worker)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        for i in range(n):
            publish_msg(buf, worker, i)

        out, err = consumer.communicate(timeout=60)
        assert consumer.returncode == 0, f"cpp_consumer failed (rc={consumer.returncode}): {err}"
        assert f"CONSUMED {n}" in out, f"unexpected output: {out}"
        print(f"  C++ consumed all {n} messages published by Python")
    finally:
        _cleanup(buf)


def test_cpp_producer_python_consumer():
    """Python creates; C++ opens + publishes; Python consumes strictly in order."""
    print("TEST: C++ producer -> Python consumer")
    n = 100
    worker = 2
    buf = SlickStreamBuffer(capacity=1 << 16, control_size=1024, name="ssb_interop_cpp2py")
    try:
        producer = subprocess.Popen(
            [find_cpp_executable("cpp_producer"), buf.get_shm_name(), str(n), str(worker)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        python_read_strict(buf, worker, n)

        out, err = producer.communicate(timeout=60)
        assert producer.returncode == 0, f"cpp_producer failed (rc={producer.returncode}): {err}"
        assert f"PUBLISHED {n}" in out, f"unexpected output: {out}"
        print(f"  Python consumed all {n} messages published by C++")
    finally:
        _cleanup(buf)


def test_cpp_creates_python_reads():
    """C++ creates the segment; Python opens by name only (geometry from
    header) and reads everything."""
    print("TEST: C++ creates segment -> Python opens by name only")
    n = 100
    worker = 3
    capacity = 1 << 16
    control_size = 1024
    shm_name = "ssb_interop_cppcreate"
    # C++ v2.1.0 leaves a segment it created in place when it exits, so this test
    # deletes the name itself: first anything an interrupted run left, then its
    # own segment once the producer is gone.
    SlickStreamBuffer.remove(shm_name)

    producer = subprocess.Popen(
        [find_cpp_executable("cpp_producer"), shm_name, str(n), str(worker),
         "create", str(capacity), str(control_size), "--hold"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        line = producer.stdout.readline()
        assert f"PUBLISHED {n}" in line, f"unexpected producer output: {line!r}"

        # Python opens with name only - geometry must come from the header
        buf = SlickStreamBuffer(name=shm_name)
        try:
            assert buf.capacity == capacity, f"capacity {buf.capacity} != {capacity}"
            assert buf.control_size == control_size
            assert not buf.own_buffer
            python_read_strict(buf, worker, n)
            print(f"  Python adopted geometry {buf.capacity}/{buf.control_size} "
                  f"and read all {n} messages")
        finally:
            buf.close()
    finally:
        try:
            producer.stdin.write("exit\n")
            producer.stdin.flush()
        except Exception:
            pass
        try:
            producer.wait(timeout=30)
        except subprocess.TimeoutExpired:
            producer.kill()
            producer.wait()
        SlickStreamBuffer.remove(shm_name)


def test_wrap_relocation_interop():
    """Small data ring laps many times: wrap relocation must be
    binary-identical across languages (loss allowed, order/content must hold,
    final message must survive)."""
    print("TEST: wrap relocation across languages (capacity 256)")
    n = 100
    capacity = 256   # 100 messages of 8..64 bytes lap this ring many times
    control_size = 1024  # control ring never laps; only the data ring does

    # Direction 1: Python publishes across many wraps, C++ reads (lossy)
    worker = 4
    buf = SlickStreamBuffer(capacity=capacity, control_size=control_size,
                            name="ssb_interop_wrap1")
    try:
        for i in range(n):
            publish_msg(buf, worker, i)

        result = subprocess.run(
            [find_cpp_executable("cpp_consumer"), buf.get_shm_name(), str(n), str(worker), "--lossy"],
            capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, f"cpp_consumer failed: {result.stderr}"
        print(f"  C++ (lossy): {result.stdout.strip()}")
    finally:
        _cleanup(buf)

    # Direction 2: C++ publishes across many wraps, Python reads (lossy)
    worker = 5
    buf = SlickStreamBuffer(capacity=capacity, control_size=control_size,
                            name="ssb_interop_wrap2")
    try:
        result = subprocess.run(
            [find_cpp_executable("cpp_producer"), buf.get_shm_name(), str(n), str(worker)],
            capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, f"cpp_producer failed: {result.stderr}"

        received = python_read_lossy(buf, worker, n)
        assert received >= 1
        print(f"  Python (lossy): received {received}/{n} valid messages incl. the last")
    finally:
        _cleanup(buf)


def test_read_last_interop():
    """read_last() in both directions."""
    print("TEST: read_last across languages")
    worker = 6
    buf = SlickStreamBuffer(capacity=1 << 16, control_size=1024, name="ssb_interop_rlast")
    try:
        # Python publishes 3 messages; C++ read_last must see msg 2
        for i in range(3):
            publish_msg(buf, worker, i)

        result = subprocess.run(
            [find_cpp_executable("cpp_consumer"), buf.get_shm_name(), "3", str(worker), "--read-last"],
            capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, f"cpp_consumer --read-last failed: {result.stderr}"
        parts = result.stdout.split()
        assert parts[0] == "LAST" and int(parts[2]) == 2, f"unexpected output: {result.stdout}"
        print(f"  C++ read_last saw Python's newest message (msg 2)")

        # C++ publishes 5 more; Python read_last must see msg 4
        result = subprocess.run(
            [find_cpp_executable("cpp_producer"), buf.get_shm_name(), "5", str(worker)],
            capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, f"cpp_producer failed: {result.stderr}"

        data, length = buf.read_last()
        assert data is not None, "read_last returned nothing"
        msg_num = interop_check_msg(data, worker)
        assert msg_num == 4, f"expected newest msg 4, got {msg_num}"
        print(f"  Python read_last saw C++'s newest message (msg 4)")
    finally:
        _cleanup(buf)


def test_python_attaches_to_cpp_segment_as_creator():
    """C++ creates the segment; Python constructs with the *creator* form and
    the same geometry. Only the process that actually created a segment may
    initialize it, so Python must attach to the live one - not claim ownership
    and not reset the cursors out from under the C++ producer."""
    print("TEST: C++ creates segment -> Python creator form attaches to it")
    n = 100
    worker = 6
    capacity = 1 << 16
    control_size = 1024
    shm_name = "ssb_interop_attach"
    # C++ v2.1.0 leaves a segment it created in place when it exits, so this test
    # deletes the name itself: first anything an interrupted run left, then its
    # own segment once the producer is gone.
    SlickStreamBuffer.remove(shm_name)

    producer = subprocess.Popen(
        [find_cpp_executable("cpp_producer"), shm_name, str(n), str(worker),
         "create", str(capacity), str(control_size), "--hold"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        line = producer.stdout.readline()
        assert f"PUBLISHED {n}" in line, f"unexpected producer output: {line!r}"

        buf = SlickStreamBuffer(capacity=capacity, control_size=control_size, name=shm_name)
        try:
            assert not buf.own_buffer, "Python must not claim a segment C++ created"
            assert buf.capacity == capacity
            assert buf.control_size == control_size
            # The records published before we attached are still there, which
            # they would not be had the attach reinitialized the segment.
            assert buf.initial_reading_index() == n, \
                f"next_seq_ was reset: {buf.initial_reading_index()} != {n}"
            python_read_strict(buf, worker, n)
            print(f"  Python attached without reinitializing and read all {n} messages")
        finally:
            buf.close()
    finally:
        try:
            producer.stdin.write("exit\n")
            producer.stdin.flush()
        except Exception:
            pass
        try:
            producer.wait(timeout=30)
        except subprocess.TimeoutExpired:
            producer.kill()
            producer.wait()
        SlickStreamBuffer.remove(shm_name)


ALL_TESTS = {
    "python_producer_cpp_consumer": test_python_producer_cpp_consumer,
    "cpp_producer_python_consumer": test_cpp_producer_python_consumer,
    "cpp_creates_python_reads": test_cpp_creates_python_reads,
    "python_attaches_to_cpp_segment_as_creator": test_python_attaches_to_cpp_segment_as_creator,
    "wrap_relocation_interop": test_wrap_relocation_interop,
    "read_last_interop": test_read_last_interop,
}


def main():
    parser = argparse.ArgumentParser(description="C++/Python interop tests")
    parser.add_argument("--test", choices=sorted(ALL_TESTS), default=None,
                        help="run a single test (default: all)")
    args = parser.parse_args()

    tests = {args.test: ALL_TESTS[args.test]} if args.test else ALL_TESTS

    print("=" * 70)
    print("Running C++/Python Interop Tests")
    print("=" * 70)

    passed = 0
    failed = 0
    for name, func in tests.items():
        try:
            print(f"\n[TEST] {name}...")
            func()
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
    success = main()
    sys.exit(0 if success else 1)
