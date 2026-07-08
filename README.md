# slick-stream-buffer-py

A Python implementation of [slick-stream-buffer](https://github.com/SlickQuant/slick-stream-buffer) —
a lock-free single-producer multi-consumer (SPMC) byte stream buffer with shared memory support.

**Maintains exact binary compatibility with the C++ version**: a Python producer and a C++
consumer (or vice versa) communicate seamlessly through the same shared memory segment, using
the same `std::atomic` synchronization primitives.

## Features

- **Byte stream, message records**: the producer writes raw bytes (`prepare`/`commit`) and
  publishes them as discrete message records (`consume`), like a network receive buffer
  feeding a framing layer
- **Lock-free SPMC**: one producer, any number of consumers, no locks anywhere
- **Binary-compatible with C++**: identical 64-byte header, 32-byte control records, and
  acquire/release memory orderings (`slick/stream_buffer.hpp`, header magic `'SSB1'`)
- **Shared memory IPC**: built on `multiprocessing.shared_memory`, matching the C++
  slick-shm naming conventions on Windows, Linux, and macOS
- **Lossy by design**: slow consumers skip overwritten data and count the loss instead of
  blocking the producer
- **Local memory mode**: same API without shared memory for single-process use
- **No runtime dependencies**: Python 3.8+ standard library plus a small bundled C++
  extension for the atomics

## Requirements

- Python 3.8+
- 64-bit platform (Windows x86-64, Linux x86-64/ARM64, macOS x86-64/ARM64)
- A C++17 compiler to build the `ssb_atomic_ops_ext` extension (MSVC 2017+, GCC 5+, Clang 3.8+)

## Installation

```bash
pip install -e .
# or just build the extension in place:
python setup.py build_ext --inplace
```

## Quick Start

### Producer (creates the shared memory segment)

```python
from slick_stream_buffer_py import SlickStreamBuffer

# 64 MB data ring, 65536 message records
stream = SlickStreamBuffer(capacity=1 << 26, control_size=1 << 16, name="market_data")

while True:
    mv = stream.prepare(64 * 1024)      # contiguous writable memoryview (zero-copy)
    n = sock.recv_into(mv)              # write network bytes directly into the ring
    stream.commit(n)

    # publish every complete package as one message record
    while (package_size := find_complete_package(stream.data(), stream.size())):
        stream.consume(package_size)
```

### Consumer (opens the existing segment — can be C++ or Python)

```python
from slick_stream_buffer_py import SlickStreamBuffer

stream = SlickStreamBuffer(name="market_data")   # geometry read from the segment header

cursor = stream.initial_reading_index()          # skip history; use 0 to read from the start
while True:
    data, length, cursor = stream.read(cursor)
    if data is None:
        continue
    handle_package(data, length)
```

### Local memory mode (single process)

```python
buf = SlickStreamBuffer(capacity=1024, control_size=16)

mv = buf.prepare(5)
mv[:] = b"hello"
buf.commit(5)
buf.consume(5)                     # publish as one record

data, length, cursor = buf.read(0)  # -> b"hello", 5, 1
```

## C++ Interoperability

The C++ side uses the identical layout — either side can create the segment, the other
attaches to it:

```cpp
#include <slick/stream_buffer.hpp>

slick::stream_buffer stream("market_data");   // open segment created by Python

uint64_t cursor = stream.initial_reading_index();
for (;;) {
    auto [data, length] = stream.read(cursor);
    if (data == nullptr) continue;
    handle_package(data, length);
}
```

Shared memory naming: use `get_shm_name()` to obtain the exact name to pass to C++
(on POSIX it includes the leading `/` that `shm_open()` requires; on Windows it is the
raw name).

### Shared memory layout

```
[HEADER: 64 bytes]
  0-7   atomic<uint64> committed_    - monotonic end of committed bytes
  8-15  atomic<uint64> consumed_     - monotonic publish boundary
  16-23 atomic<uint64> next_seq_     - next record sequence number
  24-31 atomic<uint64> reserve_end_  - prepared-region high-water mark
  32-39 uint64 capacity_             - data ring size in bytes (power of 2)
  40-43 uint32 control_size_         - control ring record count (power of 2)
  44-47 uint32 header_magic          - 0x53534231 ('SSB1')
  48-51 atomic<uint32> init_state    - 0=uninit, 2=initializing, 3=ready
  52-63 padding
[CONTROL RING: 32 bytes x control_size]
  each record: atomic<uint64> seq | uint64 offset | uint32 length | 12 bytes padding
[DATA RING: capacity bytes]
```

Total segment size: `64 + 32 * control_size + capacity`.

## API Summary

| Method | Role | Description |
|---|---|---|
| `prepare(n) -> memoryview` | producer | contiguous writable region; may relocate unconsumed bytes on wrap |
| `commit(n)` | producer | move n prepared bytes into the readable region (clamped) |
| `consume(n) -> PublishedRecord` | producer | publish the first n readable bytes as ONE record (clamped) |
| `discard()` | producer | drop unconsumed + prepared bytes without publishing |
| `data() -> memoryview` / `size()` | producer | the committed-but-unconsumed region |
| `read(cursor) -> (data, length, cursor)` | consumer | next record, or `(None, 0, cursor)`; skips lost records |
| `read_last() -> (data, length)` | consumer | newest record without a cursor |
| `initial_reading_index()` | consumer | cursor for a late joiner (skips history) |
| `loss_count()` | consumer | records skipped by this instance due to overwrite |
| `reset()` | owner | clear all state (not thread-safe) |
| `close()` / `unlink()` | lifecycle | detach / delete the segment |

See [API_DIFFERENCES.md](API_DIFFERENCES.md) for the exact deviations from the C++ API
(cursor passed by value, bytes copies vs pointers, exceptions).

### Caveats (same as C++)

- Producer methods (`prepare`/`commit`/`consume`/`discard`/`data`/`size`/`reset`) must be
  called from a single thread.
- The buffer is lossy: if the producer outruns a consumer by more than the control ring or
  data ring size, the consumer skips ahead and the loss is counted.
- `prepare()` may relocate the readable region: memoryviews previously returned by `data()`
  or `prepare()` are invalidated.
- A single message (one `consume()` call) is limited to < 4 GiB.

## Building and Testing

```bash
# 1. Build the atomics extension
python setup.py build_ext --inplace

# 2. Pure-Python tests
python tests/test_atomic_ops.py
python tests/test_local_mode.py
python tests/test_shm_mode.py

# 3. C++ interop tests (requires CMake + a C++20 compiler)
cmake -S . -B build
cmake --build build --config Debug
cd build && ctest -C Debug --output-on-failure
```

The interop tests fetch [slick-stream-buffer](https://github.com/SlickQuant/slick-stream-buffer)
(which pulls [slick-shm](https://github.com/SlickQuant/slick-shm)) from GitHub and build real
C++ producer/consumer binaries against the actual `stream_buffer.hpp`. To build against local
checkouts instead (no network):

```bash
cmake -S . -B build \
  -DFETCHCONTENT_SOURCE_DIR_SLICK-STREAM-BUFFER=/path/to/slick-stream-buffer \
  -DFETCHCONTENT_SOURCE_DIR_SLICK-SHM=/path/to/slick-shm
```

If a failed test run leaves segments behind: `python tests/cleanup_shm.py`

## Related Projects

- [slick-stream-buffer](https://github.com/SlickQuant/slick-stream-buffer) — the C++ implementation
- [slick-queue-py](https://github.com/SlickQuant/slick-queue-py) / [slick-queue](https://github.com/SlickQuant/slick-queue) — MPMC fixed-element queue with the same interop approach

## License

MIT
