"""
Cleanup utility for shared memory segments from test runs.

Run this if tests fail to clean up properly and you get "File exists" errors.

Unlinking goes through SlickStreamBuffer.remove(), which never maps the
segment - so it also clears one left wedged mid-initialization, or one whose
header a test deliberately corrupted.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from slick_stream_buffer_py import SlickStreamBuffer

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
    "ssb_py_foreign_zero",
    "ssb_py_foreign_magic",
    "ssb_py_undersized",
    "ssb_py_second_creator",
    "ssb_py_wedged",
    "ssb_py_wedged_msg",
    "ssb_py_wedged_clock",
    "ssb_py_reset_detect",
    "ssb_py_oversized",
    "ssb_interop_py2cpp",
    "ssb_interop_cpp2py",
    "ssb_interop_cppcreate",
    "ssb_interop_attach",
    "ssb_interop_wrap1",
    "ssb_interop_wrap2",
    "ssb_interop_rlast",
    "ssb_smoke",
]


def cleanup_shm():
    """Attempt to unlink all test shared memory segments."""
    if sys.platform == 'win32':
        # A Windows section disappears once its last handle closes, so there is
        # no name left to unlink and nothing for this script to do.
        print("Windows: shared memory sections are freed with their last handle; "
              "nothing to clean up.")
        return True

    cleaned = []
    failed = []

    for name in TEST_SHM_NAMES:
        try:
            if SlickStreamBuffer.remove(name):
                cleaned.append(name)
                print(f"Cleaned up: {name}")
        except Exception as e:
            failed.append((name, str(e)))
            print(f"Failed to clean {name}: {e}")

    print(f"\n{len(cleaned)} segment(s) cleaned, {len(failed)} failed")
    return len(failed) == 0


if __name__ == '__main__':
    sys.exit(0 if cleanup_shm() else 1)
