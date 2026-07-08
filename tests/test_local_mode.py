"""
Local memory mode tests for SlickStreamBuffer.

Mirrors the C++ test suite in slick-stream-buffer/tests/stream_buffer_tests.cpp
one-for-one (adapted to the Python API: read() returns (data, length, cursor),
prepare() returns a writable memoryview, and data/record payloads are bytes
copies rather than pointers).
"""
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from slick_stream_buffer_py import SlickStreamBuffer, K_INVALID_SEQ


def write_bytes(buf: SlickStreamBuffer, data: bytes) -> None:
    """prepare + write + commit in one step."""
    mv = buf.prepare(len(data))
    assert len(mv) == len(data)
    mv[:] = data
    buf.commit(len(data))


def publish_filled(buf: SlickStreamBuffer, value: int, n: int) -> None:
    """Publish one message of n bytes filled with the given byte value."""
    mv = buf.prepare(n)
    mv[:] = bytes([value]) * n
    buf.commit(n)
    buf.consume(n)


def test_empty_read_returns_none():
    buf = SlickStreamBuffer(capacity=1024, control_size=16)
    data, length, cursor = buf.read(0)
    assert data is None
    assert length == 0
    assert cursor == 0
    assert buf.size() == 0
    assert buf.capacity == 1024
    assert buf.control_size == 16
    assert buf.own_buffer
    assert not buf.use_shm
    buf.close()


def test_invalid_sizes_raise():
    for kwargs in (
        dict(capacity=1000, control_size=16),   # capacity not pow2
        dict(capacity=1024, control_size=15),   # control_size not pow2
        dict(capacity=0, control_size=16),
        dict(capacity=1024, control_size=0),
        dict(capacity=1024),                    # missing control_size
        dict(control_size=16),                  # missing capacity
        dict(),                                 # local mode requires both
    ):
        try:
            SlickStreamBuffer(**kwargs)
            assert False, f"expected ValueError for {kwargs}"
        except ValueError:
            pass


def test_prepare_commit_consume_read_roundtrip():
    buf = SlickStreamBuffer(capacity=1024, control_size=16)
    write_bytes(buf, b"hello")
    assert buf.size() == 5
    assert bytes(buf.data()) == b"hello"

    record = buf.consume(5)
    assert buf.size() == 0

    # consume() returns the record exactly as consumers will see it
    assert bool(record)
    assert record.sequence == 0
    assert record.length == 5
    assert record.data == b"hello"

    data, length, cursor = buf.read(0)
    assert data == b"hello"
    assert length == 5
    assert cursor == 1

    data2, length2, cursor = buf.read(cursor)
    assert data2 is None
    assert length2 == 0
    buf.close()


def test_consume_splits_into_records():
    buf = SlickStreamBuffer(capacity=1024, control_size=16)
    write_bytes(buf, b"0123456789")
    r1 = buf.consume(4)
    r2 = buf.consume(6)
    assert buf.size() == 0
    assert r1.sequence == 0
    assert r2.sequence == 1
    assert r1.length == 4
    assert r2.length == 6

    data, length, cursor = buf.read(0)
    assert data == b"0123"
    assert length == 4

    data, length, cursor = buf.read(cursor)
    assert data == b"456789"
    assert length == 6
    buf.close()


def test_partial_commit_keeps_remainder_prepared():
    buf = SlickStreamBuffer(capacity=1024, control_size=16)
    mv = buf.prepare(8)
    assert len(mv) == 8
    mv[:] = b"abcdefgh"
    buf.commit(3)
    assert buf.size() == 3
    buf.commit(5)
    assert buf.size() == 8
    assert bytes(buf.data()) == b"abcdefgh"
    buf.close()


def test_re_prepare_without_commit():
    buf = SlickStreamBuffer(capacity=1024, control_size=16)
    buf.prepare(8)
    assert buf.size() == 0

    mv = buf.prepare(16)
    assert len(mv) == 16
    mv[:] = b"x" * 16
    buf.commit(16)
    assert buf.size() == 16
    buf.close()


def test_prepare_zero_does_not_discard_prepared_bytes():
    buf = SlickStreamBuffer(capacity=1024, control_size=16)
    mv = buf.prepare(8)
    assert len(mv) == 8
    mv[:] = b"abcdefgh"

    empty = buf.prepare(0)
    assert len(empty) == 0

    buf.commit(8)
    assert buf.size() == 8
    assert bytes(buf.data()) == b"abcdefgh"
    buf.close()


def test_commit_more_than_prepared_clamps():
    buf = SlickStreamBuffer(capacity=1024, control_size=16)
    mv = buf.prepare(8)
    mv[:] = b"y" * 8
    buf.commit(100)
    assert buf.size() == 8
    buf.close()


def test_consume_more_than_size_clamps():
    buf = SlickStreamBuffer(capacity=1024, control_size=16)
    write_bytes(buf, b"abcde")
    record = buf.consume(100)  # clamps to 5, publishes one 5-byte record
    assert buf.size() == 0
    assert record.length == 5

    data, length, cursor = buf.read(0)
    assert data is not None
    assert length == 5
    buf.close()


def test_consume_zero_is_noop():
    buf = SlickStreamBuffer(capacity=1024, control_size=16)
    write_bytes(buf, b"abc")
    record = buf.consume(0)
    assert not bool(record)  # nothing published
    assert record.data is None
    assert record.length == 0
    assert buf.size() == 3

    data, length, cursor = buf.read(0)
    assert data is None
    assert length == 0

    # empty buffer: consume(0) publishes nothing either
    buf.consume(3)
    buf.consume(0)
    data, length, cursor = buf.read(cursor)
    assert data == b"abc"
    assert length == 3
    data, length, cursor = buf.read(cursor)
    assert data is None
    assert length == 0
    buf.close()


def test_prepare_too_large_raises():
    buf = SlickStreamBuffer(capacity=64, control_size=16)
    # larger than the whole ring
    try:
        buf.prepare(65)
        assert False, "expected ValueError"
    except ValueError:
        pass

    # unconsumed bytes + n exceed capacity even though n alone fits
    mv = buf.prepare(10)
    mv[:] = b"z" * 10
    buf.commit(10)
    try:
        buf.prepare(55)
        assert False, "expected ValueError"
    except ValueError:
        pass
    # boundary: exactly capacity is fine
    buf.prepare(54)
    buf.close()


def test_wrap_relocation_preserves_unconsumed_bytes():
    buf = SlickStreamBuffer(capacity=64, control_size=16)

    # fill 48 bytes 0..47, publish the first 40, keep 8 unconsumed
    src = bytes(range(80))
    write_bytes(buf, src[:48])
    buf.consume(40)

    # read the published record before its bytes get overwritten
    data, length, cursor = buf.read(0)
    assert data == src[:40]
    assert length == 40

    # prepare(32) doesn't fit at ring position 48 -> relocates the 8
    # unconsumed bytes to the ring start
    mv = buf.prepare(32)
    assert len(mv) == 32
    assert buf.size() == 8
    assert bytes(buf.data()) == src[40:48]  # bytes 40..47 preserved, contiguous

    mv[:] = src[48:80]
    buf.commit(32)
    assert buf.size() == 40
    assert bytes(buf.data()) == src[40:80]

    # publish the relocated region as one record and read it back
    buf.consume(40)
    data, length, cursor = buf.read(cursor)
    assert data == src[40:80]
    assert length == 40
    assert buf.loss_count() == 0
    buf.close()


def test_records_across_jump_read_in_order():
    buf = SlickStreamBuffer(capacity=64, control_size=16)

    publish_filled(buf, ord('A'), 40)   # record 0: ring [0, 40)

    # consume record 0 before its ring bytes can be reused
    data, length, cursor = buf.read(0)
    assert data is not None
    assert length == 40

    publish_filled(buf, ord('B'), 16)   # record 1: ring [40, 56)
    publish_filled(buf, ord('C'), 16)   # forces jump (56 + 16 > 64); record 2: ring [0, 16)

    data, length, cursor = buf.read(cursor)
    assert data == b"B" * 16

    data, length, cursor = buf.read(cursor)
    assert data == b"C" * 16

    assert buf.loss_count() == 0
    assert cursor == 3
    buf.close()


def test_control_ring_lapping_detects_loss():
    buf = SlickStreamBuffer(capacity=256, control_size=4)  # tiny control ring

    for i in range(8):
        publish_filled(buf, i, 1)

    # slots now hold records 4..7; a cursor at 0 must skip to 4 and count the loss
    data, length, cursor = buf.read(0)
    assert data is not None
    assert length == 1
    assert data[0] == 4
    assert buf.loss_count() == 4
    assert cursor == 5

    for i in range(5, 8):
        data, length, cursor = buf.read(cursor)
        assert data is not None
        assert data[0] == i
    buf.close()


def test_data_ring_lapping_detects_loss():
    buf = SlickStreamBuffer(capacity=64, control_size=64)  # data ring laps first

    for i in range(4):
        publish_filled(buf, i, 32)  # records at offsets 0, 32, 64, 96

    # record 0 and 1 bytes were overwritten by records 2 and 3; the validity
    # check must skip them
    data, length, cursor = buf.read(0)
    assert data is not None
    assert length == 32
    assert data[0] == 2
    assert buf.loss_count() == 2

    data, length, cursor = buf.read(cursor)
    assert data is not None
    assert data[0] == 3
    buf.close()


def test_read_last_returns_newest_record():
    buf = SlickStreamBuffer(capacity=1024, control_size=16)
    data, length = buf.read_last()
    assert data is None

    publish_filled(buf, ord('a'), 4)
    publish_filled(buf, ord('b'), 7)

    data, length = buf.read_last()
    assert data == b"b" * 7
    assert length == 7
    buf.close()


def test_late_joiner_initial_reading_index():
    buf = SlickStreamBuffer(capacity=1024, control_size=16)
    publish_filled(buf, ord('a'), 3)
    publish_filled(buf, ord('b'), 3)
    publish_filled(buf, ord('c'), 3)

    cursor = buf.initial_reading_index()
    assert cursor == 3
    data, length, cursor = buf.read(cursor)
    assert data is None  # nothing new yet

    publish_filled(buf, ord('d'), 5)
    data, length, cursor = buf.read(cursor)
    assert data == b"d" * 5
    assert length == 5
    data, length, cursor = buf.read(cursor)
    assert data is None
    buf.close()


def test_multi_consumer_broadcast():
    k_messages = 500
    k_consumers = 3
    buf = SlickStreamBuffer(capacity=1 << 16, control_size=1024)  # nothing laps

    received = [0] * k_consumers
    ok = [True] * k_consumers

    def consumer(c: int):
        cursor = 0
        expected = 0
        while received[c] < k_messages:
            data, length, cursor = buf.read(cursor)
            if data is None:
                time.sleep(0)  # yield
                continue
            if length != 8:
                ok[c] = False
                break
            value = int.from_bytes(data, 'little')
            if value != expected:
                ok[c] = False
                break
            expected += 1
            received[c] += 1

    threads = [threading.Thread(target=consumer, args=(c,)) for c in range(k_consumers)]
    for t in threads:
        t.start()

    for i in range(k_messages):
        mv = buf.prepare(8)
        mv[:] = i.to_bytes(8, 'little')
        buf.commit(8)
        buf.consume(8)

    for t in threads:
        t.join(timeout=120)
        assert not t.is_alive(), "consumer thread timed out"
    for c in range(k_consumers):
        assert ok[c], f"consumer {c} observed unexpected data"
        assert received[c] == k_messages
    assert buf.loss_count() == 0
    buf.close()


def test_discard_invalidates_partial_message():
    buf = SlickStreamBuffer(capacity=1024, control_size=16)

    # a complete message is published, then a partial message is committed
    # before the connection drops
    write_bytes(buf, b"complete")
    buf.consume(8)
    write_bytes(buf, b"part")
    assert buf.size() == 4

    buf.discard()
    assert buf.size() == 0

    # the published record is still readable because the discarded bytes did
    # not wrap over it
    data, length, cursor = buf.read(0)
    assert data == b"complete"
    assert length == 8

    # new connection: fresh bytes must not be contaminated by the discarded ones
    write_bytes(buf, b"fresh")
    assert buf.size() == 5
    assert bytes(buf.data()) == b"fresh"
    buf.consume(5)
    data, length, cursor = buf.read(cursor)
    assert data == b"fresh"
    assert length == 5
    assert buf.loss_count() == 0
    buf.close()


def test_discard_keeps_overwrite_loss_for_lagging_consumers():
    buf = SlickStreamBuffer(capacity=64, control_size=16)

    publish_filled(buf, ord('A'), 40)

    # A wrapped prepare can overwrite bytes of an older published record
    # before the partial message is discarded. A lagging consumer must still
    # observe loss.
    mv = buf.prepare(32)
    assert len(mv) == 32
    mv[:] = b"X" * 32
    buf.discard()

    data, length, cursor = buf.read(0)
    assert data is None
    assert length == 0
    assert cursor == 1
    assert buf.loss_count() == 1

    write_bytes(buf, b"ok")
    buf.consume(2)
    data, length, cursor = buf.read(cursor)
    assert data == b"ok"
    assert length == 2
    buf.close()


def test_discard_drops_prepared_region():
    buf = SlickStreamBuffer(capacity=1024, control_size=16)
    mv = buf.prepare(8)
    mv[:] = b"x" * 8

    buf.discard()
    buf.commit(8)  # a stale commit after discard must commit nothing
    assert buf.size() == 0

    write_bytes(buf, b"ok")
    assert buf.size() == 2
    assert bytes(buf.data()) == b"ok"
    buf.close()


def test_discard_then_refill_across_wrap():
    buf = SlickStreamBuffer(capacity=64, control_size=16)

    publish_filled(buf, ord('A'), 40)
    data, length, cursor = buf.read(0)
    assert data is not None
    assert length == 40

    # partial message committed near the end of the ring, then dropped;
    # reserve_end_ stays at its high-water mark and must not cause false loss
    mv = buf.prepare(20)
    assert len(mv) == 20
    mv[:] = b"X" * 20
    buf.commit(20)
    buf.discard()
    assert buf.size() == 0

    # refill across the wrap and read every record back intact
    for i in range(4):
        publish_filled(buf, i, 16)
        data, length, cursor = buf.read(cursor)
        assert data == bytes([i]) * 16
        assert length == 16
    assert buf.loss_count() == 0
    buf.close()


def test_discard_on_empty_buffer_is_noop():
    buf = SlickStreamBuffer(capacity=1024, control_size=16)
    buf.discard()
    assert buf.size() == 0

    publish_filled(buf, ord('a'), 3)
    buf.discard()  # nothing unconsumed - published record stays readable
    data, length, cursor = buf.read(0)
    assert data == b"a" * 3
    assert length == 3
    buf.close()


def test_reset():
    buf = SlickStreamBuffer(capacity=1024, control_size=16)
    publish_filled(buf, ord('a'), 10)
    write_bytes(buf, b"leftover")
    assert buf.size() == 8

    buf.reset()
    assert buf.size() == 0
    assert buf.loss_count() == 0
    assert buf.initial_reading_index() == 0
    data, length = buf.read_last()
    assert data is None

    data, length, cursor = buf.read(0)
    assert data is None

    publish_filled(buf, ord('z'), 6)
    data, length, cursor = buf.read(cursor)
    assert data == b"z" * 6
    assert length == 6
    buf.close()


def test_reset_detection_snaps_cursor():
    """White-box: a consumer observing a stale slot from before a reset
    (seq >= next_seq_) must snap its cursor back to 0
    (stream_buffer.hpp:453-457)."""
    buf = SlickStreamBuffer(capacity=1024, control_size=16)
    for _ in range(3):
        publish_filled(buf, ord('a'), 4)
    buf.reset()

    # simulate a stale pre-reset slot a mid-reset consumer might observe
    buf._a_slot_seq[5].store_release(7)  # 7 >= next_seq (0)

    data, length, cursor = buf.read(5)
    assert data is None
    assert cursor == 0, f"cursor must snap to 0 on reset detection, got {cursor}"
    buf.close()


def run_all_tests():
    tests = [
        ("EmptyReadReturnsNone", test_empty_read_returns_none),
        ("InvalidSizesRaise", test_invalid_sizes_raise),
        ("PrepareCommitConsumeReadRoundtrip", test_prepare_commit_consume_read_roundtrip),
        ("ConsumeSplitsIntoRecords", test_consume_splits_into_records),
        ("PartialCommitKeepsRemainderPrepared", test_partial_commit_keeps_remainder_prepared),
        ("RePrepareWithoutCommit", test_re_prepare_without_commit),
        ("PrepareZeroDoesNotDiscardPreparedBytes", test_prepare_zero_does_not_discard_prepared_bytes),
        ("CommitMoreThanPreparedClamps", test_commit_more_than_prepared_clamps),
        ("ConsumeMoreThanSizeClamps", test_consume_more_than_size_clamps),
        ("ConsumeZeroIsNoop", test_consume_zero_is_noop),
        ("PrepareTooLargeRaises", test_prepare_too_large_raises),
        ("WrapRelocationPreservesUnconsumedBytes", test_wrap_relocation_preserves_unconsumed_bytes),
        ("RecordsAcrossJumpReadInOrder", test_records_across_jump_read_in_order),
        ("ControlRingLappingDetectsLoss", test_control_ring_lapping_detects_loss),
        ("DataRingLappingDetectsLoss", test_data_ring_lapping_detects_loss),
        ("ReadLastReturnsNewestRecord", test_read_last_returns_newest_record),
        ("LateJoinerInitialReadingIndex", test_late_joiner_initial_reading_index),
        ("MultiConsumerBroadcast", test_multi_consumer_broadcast),
        ("DiscardInvalidatesPartialMessage", test_discard_invalidates_partial_message),
        ("DiscardKeepsOverwriteLossForLaggingConsumers", test_discard_keeps_overwrite_loss_for_lagging_consumers),
        ("DiscardDropsPreparedRegion", test_discard_drops_prepared_region),
        ("DiscardThenRefillAcrossWrap", test_discard_then_refill_across_wrap),
        ("DiscardOnEmptyBufferIsNoop", test_discard_on_empty_buffer_is_noop),
        ("Reset", test_reset),
        ("ResetDetectionSnapsCursor", test_reset_detection_snaps_cursor),
    ]

    print("=" * 70)
    print("Running Local Mode Tests")
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
    success = run_all_tests()
    sys.exit(0 if success else 1)
