# Changelogs

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
