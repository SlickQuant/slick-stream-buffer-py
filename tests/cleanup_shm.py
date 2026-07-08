"""
Cleanup utility for shared memory segments from test runs.

Run this if tests fail to clean up properly and you get "File exists" errors.
"""
from multiprocessing import shared_memory
import sys

# Known test shared memory segment names
TEST_SHM_NAMES = [
    "ssb_py_roundtrip",
    "ssb_py_geometry",
    "ssb_py_pagesize",
    "ssb_py_geo_mismatch",
    "ssb_py_bad_magic",
    "ssb_py_late",
    "ssb_py_lossy",
    "ssb_py_read_last",
    "ssb_py_broadcast",
    "ssb_py_xproc",
    "ssb_interop_py2cpp",
    "ssb_interop_cpp2py",
    "ssb_interop_cppcreate",
    "ssb_interop_wrap1",
    "ssb_interop_wrap2",
    "ssb_interop_rlast",
    "ssb_smoke",
]


def cleanup_shm():
    """Attempt to unlink all test shared memory segments."""
    cleaned = []
    failed = []

    for name in TEST_SHM_NAMES:
        try:
            shm = shared_memory.SharedMemory(name=name)
            try:
                shm.close()
            except BufferError:
                pass
            shm.unlink()
            cleaned.append(name)
            print(f"Cleaned up: {name}")
        except FileNotFoundError:
            pass
        except Exception as e:
            failed.append((name, str(e)))
            print(f"Failed to clean {name}: {e}")

    print(f"\n{len(cleaned)} segment(s) cleaned, {len(failed)} failed")
    return len(failed) == 0


if __name__ == '__main__':
    sys.exit(0 if cleanup_shm() else 1)
