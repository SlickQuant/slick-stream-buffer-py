# API Differences: Python vs C++

The Python implementation is binary-compatible with the C++ `slick::SlickStreamBuffer`
at the shared memory level — every byte, offset, and memory ordering matches. The
*language-level* API differs in the following ways.

## Construction

| C++ | Python |
|---|---|
| `stream_buffer buf(capacity, control_size)` | `SlickStreamBuffer(capacity=..., control_size=...)` |
| `stream_buffer buf(capacity, control_size, "name")` | `SlickStreamBuffer(capacity=..., control_size=..., name="name")` |
| `stream_buffer buf("name")` | `SlickStreamBuffer(name="name")` |
| throws `std::invalid_argument` (bad sizes) | raises `ValueError` |
| throws `std::runtime_error` (shm failures) | raises `RuntimeError` (or `FileNotFoundError` when opening a missing segment) |

## Consumer cursor is passed by value

C++ updates the cursor through a reference; Python has no by-reference ints, so the
updated cursor is returned:

```cpp
// C++
uint64_t cursor = 0;
auto [data, length] = buf.read(cursor);   // cursor updated in place
```

```python
# Python
cursor = 0
data, length, cursor = buf.read(cursor)   # updated cursor returned
```

`read()` returns `(None, 0, cursor)` where C++ returns `{nullptr, 0}`.
`read_last()` returns `(None, 0)` / `(bytes, length)`.

## Zero-copy vs copies

| Operation | C++ | Python |
|---|---|---|
| `prepare(n)` | `uint8_t*` into the ring | writable `memoryview` into the ring (zero-copy) |
| `data()` | `const uint8_t*` into the ring | read-only `memoryview` into the ring (zero-copy) |
| `read(cursor)` | pointer into the ring | **`bytes` copy** |
| `read_last()` | pointer into the ring | **`bytes` copy** |
| `consume(n).data` | pointer into the ring | **`bytes` copy** |

Consumer-side data is copied because the ring is lossy: the producer may overwrite the
bytes at any moment after the validity check (the same caveat exists in C++, where the
caller must finish with the pointer before the producer laps). A `bytes` copy makes the
Python API safe by default.

`prepare()`/`data()` memoryviews are invalidated by the next `prepare()` (which may
relocate the region) — same semantics as C++ flat_buffer-style invalidation.

## prepare() overflow

C++ throws `std::length_error` when `size() + n > capacity()`; Python raises
`ValueError`. C++ `consume()` asserts messages are < 4 GiB (debug builds); Python
always raises `ValueError`.

## consume() return value

Both return the published record. Python's `PublishedRecord` mirrors C++
`published_record` (`sequence`, `data`, `length`) and is falsy when nothing was
published (`data is None`), matching C++ `operator bool`.

## loss_count()

C++ counts losses only when `SLICK_STREAM_BUFFER_ENABLE_LOSS_DETECTION` is enabled
(default: debug builds only). Python always counts. The counter is per-instance in
both implementations, not shared through the segment.

## Memory orderings

Python routes every shared-atomic access through C++ `std::atomic` (via the
`ssb_atomic_ops_ext` extension), using the same orderings on the synchronization
edges (record `seq` store-release / load-acquire, `next_seq_`/`reserve_end_`
release/acquire, `init_state` acq_rel CAS). Where the C++ side uses
`memory_order_relaxed` (e.g. `committed_`/`consumed_` stores), Python uses
release/acquire — strictly stronger, therefore still correct.

## Lifecycle

Python adds explicit lifecycle management that C++ handles via RAII:

- `close()` — detach from the segment (does not delete it)
- `unlink()` — delete the segment (call once, from the owner, after all users closed)
- context manager support: `with SlickStreamBuffer(...) as buf: ...` (closes on exit)
- `get_shm_name()` — the exact segment name to pass to C++ (`/`-prefixed on POSIX)

On POSIX with Python >= 3.13, attaching to an existing segment uses
`SharedMemory(track=False)` so Python's resource tracker never unlinks a segment the
process does not own. On older Pythons the tracker may log a harmless warning at exit
(see `tests/run_test.py`, which filters it).
