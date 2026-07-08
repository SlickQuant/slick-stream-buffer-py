"""
Wrapper to run tests without showing harmless BufferError warnings.

The BufferError is a known issue with ctypes + SharedMemory and doesn't
affect functionality. This wrapper suppresses it for cleaner output.
"""
import sys
import subprocess

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python run_test.py <test_file.py>")
        sys.exit(1)

    test_file = sys.argv[1]

    # Run test with warnings suppressed
    # Note: We suppress stderr to hide the BufferError during cleanup
    result = subprocess.run(
        [sys.executable, test_file],
        capture_output=True,
        text=True
    )

    # Print stdout (test results)
    print(result.stdout, end='')

    # Filter stderr to remove BufferError warnings
    # The BufferError traceback typically spans multiple lines, so we need
    # to detect the start of the exception block and skip everything until
    # we see a line that's not part of the traceback
    stderr_lines = result.stderr.split('\n')
    filtered_stderr = []
    in_buffer_error_block = False

    for line in stderr_lines:
        # Detect start of BufferError exception block
        if 'Exception ignored in:' in line and 'SharedMemory.__del__' in line:
            in_buffer_error_block = True
            continue

        # If we're in a BufferError block, skip lines until we exit
        if in_buffer_error_block:
            # Check if this line is still part of the traceback
            if (line.startswith('Traceback') or
                line.startswith('  File') or
                line.strip().startswith('self.') or
                'BufferError' in line or
                'cannot close exported pointers exist' in line or
                'shared_memory.py' in line or
                line.strip() == ''):
                continue
            else:
                # We've exited the BufferError block
                in_buffer_error_block = False
                # Don't skip this line - it's not part of the error

        # Keep all other lines
        if not in_buffer_error_block:
            filtered_stderr.append(line)

    # Print filtered stderr
    filtered_output = '\n'.join(filtered_stderr).strip()
    if filtered_output:
        print(filtered_output, file=sys.stderr)

    sys.exit(result.returncode)
