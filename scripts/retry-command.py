"""Retry registry installation for at most two minutes, including command time."""

import subprocess
import sys
import time


def retry(command, timeout=120, delay=10):
    deadline = time.monotonic() + timeout
    attempt = 0
    last_exit = 1
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            print("Registry installation did not succeed before the deadline.", flush=True)
            return last_exit
        attempt += 1
        print(f"Registry installation attempt {attempt}; {remaining:.0f}s remaining.", flush=True)
        try:
            result = subprocess.run(command, check=False, timeout=remaining)
        except subprocess.TimeoutExpired:
            print("Registry installation exceeded the deadline.", flush=True)
            return 124
        if result.returncode == 0:
            return 0
        last_exit = result.returncode if result.returncode > 0 else 1
        remaining = deadline - time.monotonic()
        if remaining > 0:
            wait = min(delay, remaining)
            print(f"Installation exited with {last_exit}; retrying in {wait:.0f}s.", flush=True)
            time.sleep(wait)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Usage: python scripts/retry-command.py COMMAND [ARG ...]")
    sys.exit(retry(sys.argv[1:]))
