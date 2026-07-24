#!/usr/bin/env python3
"""
Script to test non-overlapping byte-range locks on NFS.
Multiple clients should be able to hold non-overlapping locks simultaneously.
"""

import fcntl
import os
import sys
import time

if len(sys.argv) < 5:
    print(
        "Usage: non_overlapping_locks.py <mount_path> <client_id> <start> <length>",
        flush=True,
    )
    sys.exit(1)

mount_path = sys.argv[1]
client_id = sys.argv[2]
LOCK_START = int(sys.argv[3])
LOCK_LEN = int(sys.argv[4])
TEST_FILE = os.path.join(mount_path, "non_overlapping_locks_test_file")
LOCK_DURATION = 10

os.makedirs(os.path.dirname(TEST_FILE), exist_ok=True)
with open(TEST_FILE, "a"):
    pass  # Ensure file exists

print(f"[Client {client_id}] Opening file: {TEST_FILE}", flush=True)
print(
    f"[Client {client_id}] Attempting non-overlapping lock: start={LOCK_START}, len={LOCK_LEN}",
    flush=True,
)

with open(TEST_FILE, "r+") as f:
    try:
        # Try to acquire exclusive lock on non-overlapping range
        fcntl.lockf(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB, LOCK_LEN, LOCK_START)
        print(f"[Client {client_id}] Lock acquired successfully!", flush=True)

        # Hold the lock for specified duration
        time.sleep(LOCK_DURATION)

        # Release the lock
        fcntl.lockf(f.fileno(), fcntl.LOCK_UN, LOCK_LEN, LOCK_START)
        print(f"[Client {client_id}] Lock released.", flush=True)
        print(f"[Client {client_id}] SUCCESS", flush=True)

    except BlockingIOError:
        # Lock is held by another process - this should NOT happen for non-overlapping locks
        print(
            f"[Client {client_id}] Failed to acquire lock - already held by another process",
            flush=True,
        )
        print(
            f"[Client {client_id}] FAILED - Non-overlapping lock was blocked unexpectedly",
            flush=True,
        )
        sys.exit(1)
    except IOError as e:
        print(f"[Client {client_id}] IOError: {e}", flush=True)
        print(f"[Client {client_id}] FAILED", flush=True)
        sys.exit(1)

print(f"[Client {client_id}] Test completed.", flush=True)
