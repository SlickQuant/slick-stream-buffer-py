# Changelogs

## [v2.1.0] - 2026-10-17

Matches C++ [slick-stream-buffer v2.1.0](https://github.com/SlickQuant/slick-stream-buffer/blob/main/CHANGELOG.md):
no buffer removes its shared-memory segment on its own any more, and `remove_shm()` is how the
coordinating process does. The shared memory format is unchanged (`SSB1`), so 2.0 and 2.1
processes in either language can share a segment.

### Segment lifetime (behavior change)
- On POSIX, a segment is no longer unlinked when the process that created it exits.
  `SharedMemory(create=True)` registered it with `multiprocessing`'s resource tracker, which
  unlinks what is registered when the process exits, so a producer that exited first stranded
  every consumer still attached on an orphaned mapping, while its restart silently created a
  fresh segment under the same name that those consumers never saw. C++ v2.1.0 stopped its
  destructor doing the same. A name now lives until `unlink()`, `remove_shm()` or
  `remove(name)`, and a restarted process reattaches to the existing segment and keeps its
  contents. Windows is unaffected: a section dies with its last handle.
- The buffer sends the resource tracker nothing at all: `track=False` on Python 3.13+, and on
  older POSIX Pythons a `SharedMemory` subclass that maps the segment without registering it.
  The 2.0.0 bookkeeping of which names this process had registered is gone, and `remove()` no
  longer contacts the tracker.
- Migration: code that relied on the cleanup at exit now leaves segments in `/dev/shm`, and its
  next run attaches to them - `own_buffer` is `False`, reading resumes from the old cursors, and
  a creator with a different geometry raises `RuntimeError` ("geometry mismatch"). Call
  `remove(name)` at startup for a clean start, or `unlink()` / `remove_shm()` at shutdown once
  no peer will attach again.

### Added
- `remove_shm() -> bool`, matching C++: unlinks this buffer's segment name so the next creator
  starts fresh, leaving open mappings - this buffer's own included - valid. It unlinks by name
  on every call, where `unlink()` runs once, and leaves `unlink()` nothing to do afterwards, so a
  later `unlink()` cannot delete a replacement created under the name. Returns `False` for a
  local-memory buffer or when the name is already gone, and always `True` for a shared buffer on
  Windows. C++ v2.1.0's
  other addition, `shm_name()`, is the existing `get_shm_name()` (`None` rather than `""` for a
  local-memory buffer).

### Fixed
- Before Python 3.13 on POSIX, a process that only attached to a segment unlinked it when it
  exited, because attaching registered the name with that process's resource tracker too.
  2.0.0 avoided this only on 3.13+, through `track=False`.

### Tests
- Segment lifetime: `unlink()` repeated, `remove_shm()` on local and shared buffers, `unlink()`
  after `remove_shm()` sparing a replacement segment, a
  creator's and an attacher's exit in a fresh interpreter, no resource-tracker traffic at all,
  a child forked while another thread is constructing a buffer, and spawned children attaching
  at once. A test that `remove()` leaves the tracker alone for this process's own segment
  replaces the 2.0.0 one that it retracted the registration.
- The interop tests build against C++ slick-stream-buffer `v2.1.0` (was `v2.0.0`). The two in
  which C++ creates the segment remove it before starting and after the producer exits, since
  C++ now leaves it in place; a rerun used to attach to the previous run's segment.
- Tests that hand-build a raw segment open it through the library's opener, so that handle and
  the buffer no longer both claim the name in the resource tracker.

Checked against C++ slick-stream-buffer v2.1.0 with the interop tests on Windows and on Linux,
run three times on Linux so a segment left behind would fail a rerun, and on Linux under
Python 3.12 (the subclass) and 3.14 (`track=False`).

## [v2.0.0] - 2026-09-12

Matches C++ [slick-stream-buffer v2.0.0](https://github.com/SlickQuant/slick-stream-buffer/blob/main/CHANGELOG.md).
The version number jumps from 0.1.0 to 2.0.0 so that the Python package and the C++ header it
is binary-compatible with carry the same version from here on.

The shared memory format is unchanged (`SSB1`): a segment written by a v1.x C++ process or by
slick-stream-buffer-py 0.1.0 is still readable.

### Breaking
- `read()` takes a per-call configuration, `read_traits`, mirroring the C++ template parameter:
  `detect_reset` (default on) resynchronizes a cursor that outlived a `reset()`, `count_loss`
  (default off) accumulates `loss_count()`. Ordinary callers still write `buf.read(cursor)`;
  pass `counting_read_traits` (or a subclass of `read_traits`) as the second argument to change
  a flag.
- `loss_count()` now reads 0 unless the reads were made with a traits whose `count_loss` is
  true. Records are still skipped correctly - only the counter is silent. This matches the C++
  default, where counting measured ~20% of `read()` even in a run that loses nothing.
- `consume()` raises `ValueError` with a message naming the 4 GiB record limit instead of the
  previous generic one, and the constructor raises `ValueError` for a geometry whose total
  exceeds this build's address space.

### Fixed
- A creator could overwrite a shared memory segment belonging to another application.
  Initialization authority now comes from the OS-backed creator flag - whether
  `SharedMemory(create=True)` succeeded (`O_CREAT|O_EXCL` on POSIX, `ERROR_ALREADY_EXISTS` on
  Windows) - instead of a CAS on the in-band init-state slot, which cannot tell a fresh segment
  from a zero-filled foreign one. An existing segment is validated (magic, geometry, mapped
  size) before a byte is written, and attaching never claims ownership of it.
- A header declaring more capacity than the mapping holds is rejected instead of yielding
  out-of-bounds control/data slices; every header read is size-checked first.
- The init handshake fails fast when the segment carries a foreign magic, and its 2-second
  timeout is a real wall-clock deadline - it counted 1 ms sleeps before, which stretched to
  ~30 s on Windows at the default timer granularity.
- `unlink()` was a silent no-op once `close()` had run, because `close()` drops the
  `SharedMemory` object it went through. The documented `shared_memory` idiom - close every
  handle, then unlink the name once - therefore leaked the segment and its resource-tracker
  registration on POSIX. The name is now kept alongside the mapping, so `unlink()` after
  `close()` unlinks by name exactly as `remove()` does, and unlinking twice stays a no-op.
- On POSIX, `remove()` retracts the `multiprocessing` resource-tracker registration only for
  segments this process registered. It retracted unconditionally before, so the normal startup
  call - where the segment is a previous run's and nothing here registered it - handed the
  tracker a name it had never seen, making it raise `KeyError` and print a traceback from its
  own process; the message also started that process when it was not already running.

### Added
- `SlickStreamBuffer.remove(name)`, the explicit stale-segment recovery step. Construction over
  a segment stuck in `INITIALIZING` (a creator that died mid-initialization) now fails with a
  message naming it. Recovery is deliberately manual: nothing portable distinguishes a dead
  creator from a slow one.
- `read_traits` / `counting_read_traits` (with `ReadTraits` / `CountingReadTraits` as the PEP 8
  spellings), exported from the package.
- Tests mirroring the new C++ suites: default traits are silent, `detect_reset` can be turned
  off, geometry beyond the address space, the 4 GiB record limit (opt-in via `SSB_PY_TEST_4GIB=1`),
  foreign zeroed/foreign magic segments, undersized segments, a second creator attaching, the
  wedged-segment message and its wall-clock deadline, `remove()` recovery, and stale-cursor
  resynchronization through shared memory. Plus an interop test where a Python creator attaches
  to a segment C++ created without reinitializing it, and coverage of the two lifecycle fixes
  above: `unlink()` after `close()`, and which resource-tracker registrations `remove()` will
  and will not retract.

### Performance
- Producer-side shadows for `next_seq_` and `reserve_end_`, matching the committed/consumed
  ones: publishing and `prepare()` no longer re-load counters only the producer writes.

### Changed
- `close()` and the construction-failure path share one release routine.
- The interop test build pins slick-stream-buffer to `v2.0.0` and raises the CMake floor to
  3.21, which is what that project now requires.

## [v0.1.0] - 2026-07-08

### Added
- Initial release: Python implementation of slick-stream-buffer, a lock-free SPMC byte
  stream buffer, binary-compatible with the C++ version (`slick/stream_buffer.hpp` v1.0.x,
  header magic `'SSB1'`) over shared memory
- `SlickStreamBuffer` with the full C++ API surface:
  - Producer: `prepare()` (zero-copy memoryview, wrap relocation), `commit()`, `consume()`,
    `discard()`, `data()`, `size()`, `reset()`
  - Consumer: `read(cursor)` (lossy, skips overwritten records), `read_last()`,
    `initial_reading_index()`, `loss_count()`
  - Lifecycle: `close()`, `unlink()`, `get_shm_name()`, context manager support
- Three construction modes matching the C++ constructors: local memory, shared memory
  create-or-open (CAS init-state handshake), and open-existing (geometry adopted from the
  segment header)
- `ssb_atomic_ops` module + `ssb_atomic_ops_ext` C++ extension providing real
  `std::atomic` 64-bit and 32-bit load/store/CAS with the same acquire/release memory
  orderings as the C++ implementation (libatomic/`__sync_*` fallbacks on POSIX)
- Test suites: atomic operations (10), local mode mirroring the C++ gtest suite (25),
  shared memory mode (11), and C++ interop tests (5) that exchange messages with real
  C++ binaries built against the actual `stream_buffer.hpp` in both directions
- CMake/ctest wiring with FetchContent (local source overrides supported via
  `FETCHCONTENT_SOURCE_DIR_SLICK-STREAM-BUFFER` / `FETCHCONTENT_SOURCE_DIR_SLICK-SHM`)
- CI workflow (Windows/Linux/macOS, Debug/Release), README, API_DIFFERENCES.md
