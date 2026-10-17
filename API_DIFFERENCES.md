# API Differences: Python vs C++

The Python implementation is binary-compatible with the C++ `slick::SlickStreamBuffer`
(`slick/stream_buffer.hpp` v2.1.0) at the shared memory level — every byte, offset, and
memory ordering matches. The *language-level* API differs in the following ways.

## Construction

| C++ | Python |
|---|---|
| `stream_buffer buf(capacity, control_size)` | `SlickStreamBuffer(capacity=..., control_size=...)` |
| `stream_buffer buf(capacity, control_size, "name")` | `SlickStreamBuffer(capacity=..., control_size=..., name="name")` |
| `stream_buffer buf("name")` | `SlickStreamBuffer(name="name")` |
| `stream_buffer::remove("name")` | `SlickStreamBuffer.remove("name")` (static, returns `bool`) |
| throws `std::invalid_argument` (bad sizes) | raises `ValueError` |
| throws `std::length_error` (geometry past the address space) | raises `ValueError` |
| throws `std::runtime_error` (shm failures) | raises `RuntimeError` (or `FileNotFoundError` when opening a missing segment) |

Both implementations take initialization authority from the OS, not from the in-band
init-state slot: C++ uses `slick::shm`'s `is_creator()`, Python uses whether
`SharedMemory(create=True)` succeeded. Both reduce to `O_CREAT|O_EXCL` on POSIX and
`ERROR_ALREADY_EXISTS` on Windows, so neither ever initializes a segment it did not create.

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

## Read traits are a runtime argument, not a template parameter

C++ configures `read()` with a template parameter, so the flags are compile-time constants
and a misconfiguration cannot cross a translation unit. Python has no equivalent, so the
same traits class is passed as an ordinary second argument:

```cpp
// C++
struct counting : slick::read_traits { static constexpr bool count_loss = true; };
auto [data, len] = buf.read<counting>(cursor);
```

```python
# Python
from slick_stream_buffer_py import counting_read_traits
data, length, cursor = buf.read(cursor, counting_read_traits)
```

The flags and their defaults are identical (`detect_reset = True`, `count_loss = False`),
and subclassing `read_traits` works the same way as deriving from `slick::read_traits`.
Python also exports `ReadTraits` / `CountingReadTraits` as PEP 8 spellings of the same two
classes. The performance argument for making `count_loss` opt-in is a C++ one (~20% of
`read()` on MSVC); in Python the cost is two attribute lookups per call, but the *semantics*
match so that `loss_count()` means the same thing on both sides.

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

## prepare() / consume() overflow

C++ throws `std::length_error` when `size() + n > capacity()`; Python raises `ValueError`.
`consume()` behaves the same on both sides — a message of 4 GiB or more is rejected before
any state moves, so nothing is published and the bytes stay readable — but C++ throws
`std::length_error` and Python raises `ValueError`.

## consume() return value

Both return the published record. Python's `PublishedRecord` mirrors C++
`published_record` (`sequence`, `data`, `length`) and is falsy when nothing was
published (`data is None`), matching C++ `operator bool`.

## loss_count()

Identical semantics on both sides: losses are counted only for reads made with a traits
whose `count_loss` is true, so `loss_count()` reads 0 under the defaults. Records are still
skipped correctly either way. The counter is per-instance in both implementations, not
shared through the segment.

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
- `unlink()` — delete the segment this instance is attached to (call once, from the owner,
  after all users closed - including this instance, so `close()` then `unlink()` is fine).
  A repeat is a no-op, so it cannot delete a segment a later run created under the same name
- context manager support: `with SlickStreamBuffer(...) as buf: ...` (closes on exit)
- `get_shm_name()` — the exact segment name to pass to C++ (`/`-prefixed on POSIX); C++
  `shm_name()` returns `""` for a local-memory buffer where Python returns `None`

Neither side deletes a segment on its own, matching C++ v2.1.0: not the C++ destructor, and not
Python's `close()`, context manager or process exit. `remove_shm()` is shared with the same
contract: unlink this buffer's segment name, on every call, leaving open mappings valid, and
return `False` for a local-memory buffer.

`remove(name)` is the one lifecycle call both sides share, with the same contract: unlink a
segment by name without attaching to it, returning `True` on success and, on Windows, always
`True` because a section dies with its last handle. Python routes it through
`_posixshmem.shm_unlink` and never sends the resource tracker an unregister: the tracker answers
one for a name it does not hold by raising `KeyError` and printing a traceback from its own
process.

Python keeps every segment it creates or attaches to out of `multiprocessing`'s resource tracker,
which would otherwise unlink it when the process exits, and sends the tracker nothing at all:
`SharedMemory(track=False)` on Python 3.13+, and on older POSIX Pythons a `SharedMemory` subclass
that maps the segment without registering it. Registering and retracting at once instead would
race between processes sharing one tracker - `multiprocessing` children share their parent's -
and need a lock that a fork could leave held. A `SharedMemory` you open yourself is still tracked
unless you pass `track=False` (Python 3.13+).
