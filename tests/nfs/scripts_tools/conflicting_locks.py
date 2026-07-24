#!/usr/bin/env python3
"""
Script to test conflicting byte-range locks on NFS.
Tests behavior when multiple clients try to acquire the same or overlapping lock ranges.
"""

import fcntl
import os
import sys
import time

if len(sys.argv) < 5:
    print(
        "Usage: conflicting_locks.py <mount_path> <client_id> <start> <length>",
        flush=True,
    )
    sys.exit(1)

mount_path = sys.argv[1]
client_id = sys.argv[2]
LOCK_START = int(sys.argv[3])
LOCK_LEN = int(sys.argv[4])
TEST_FILE = os.path.join(mount_path, "conflicting_locks_test_file")
LOCK_DURATION = 12

os.makedirs(os.path.dirname(TEST_FILE), exist_ok=True)
with open(TEST_FILE, "a"):
    pass  # Ensure file exists

print(f"[Client {client_id}] Opening file: {TEST_FILE}", flush=True)
print(
    f"[Client {client_id}] Attempting conflicting lock: start={LOCK_START}, len={LOCK_LEN}",
    flush=True,
)

with open(TEST_FILE, "r+") as f:
    lock_acquired = False
    retry_count = 0
    max_retries = 10

    while not lock_acquired and retry_count < max_retries:
        try:
            # Try to acquire exclusive lock
            fcntl.lockf(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB, LOCK_LEN, LOCK_START)
            lock_acquired = True
            msg = f"[Client {client_id}] Lock acquired successfully after {retry_count} retries!"
            print(msg, flush=True)

            # Hold the lock for specified duration
            time.sleep(LOCK_DURATION)

            # Release the lock
            fcntl.lockf(f.fileno(), fcntl.LOCK_UN, LOCK_LEN, LOCK_START)
            print(f"[Client {client_id}] Lock released.", flush=True)
            print(f"[Client {client_id}] SUCCESS", flush=True)

        except BlockingIOError:
            # Lock is held by another process - expected for conflicting locks
            retry_count += 1
            if retry_count < max_retries:
                print(
                    f"[Client {client_id}] Lock conflict detected - retry {retry_count}/{max_retries}",
                    flush=True,
                )
                time.sleep(2)  # Wait before retrying
            else:
                print(
                    f"[Client {client_id}] Failed to acquire lock after {max_retries} retries",
                    flush=True,
                )
                print(f"[Client {client_id}] BLOCKED", flush=True)
        except IOError as e:
            print(f"[Client {client_id}] IOError: {e}", flush=True)
            print(f"[Client {client_id}] FAILED", flush=True)
            sys.exit(1)

print(f"[Client {client_id}] Test completed.", flush=True)
