#!/usr/bin/env python3
"""
Script to test overlapping byte-range locks on NFS.
Tests both non-overlapping (should succeed) and overlapping (should block) scenarios.
For sequential tests, clients will retry to acquire locks after previous clients release them.
"""

import fcntl
import os
import sys
import time

if len(sys.argv) < 5:
    print(
        "Usage: overlapping_locks.py <mount_path> <client_id> <start> <length>",
        flush=True,
    )
    sys.exit(1)

mount_path = sys.argv[1]
client_id = sys.argv[2]
LOCK_START = int(sys.argv[3])
LOCK_LEN = int(sys.argv[4])
TEST_FILE = os.path.join(mount_path, "overlapping_locks_test_file")
LOCK_DURATION = 15
MAX_WAIT_TIME = 40  # Maximum time to wait for lock acquisition (for sequential tests)

os.makedirs(os.path.dirname(TEST_FILE), exist_ok=True)
with open(TEST_FILE, "a"):
    pass  # Ensure file exists

print(f"[Client {client_id}] Opening file: {TEST_FILE}", flush=True)
print(
    f"[Client {client_id}] Attempting lock: start={LOCK_START}, len={LOCK_LEN}",
    flush=True,
)

start_time = time.time()
lock_acquired = False
retry_count = 0
initial_block = False

with open(TEST_FILE, "r+") as f:
    while not lock_acquired and (time.time() - start_time) < MAX_WAIT_TIME:
        try:
            # Try to acquire exclusive lock
            fcntl.lockf(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB, LOCK_LEN, LOCK_START)
            lock_acquired = True
            acquisition_time = time.time() - start_time

            if retry_count > 0:
                msg = (
                    f"[Client {client_id}] Lock acquired after {acquisition_time:.2f} seconds "
                    f"({retry_count} retries)"
                )
                print(msg, flush=True)
            else:
                print(f"[Client {client_id}] Lock acquired successfully!", flush=True)

            # Hold the lock for specified duration
            time.sleep(LOCK_DURATION)

            # Release the lock
            fcntl.lockf(f.fileno(), fcntl.LOCK_UN, LOCK_LEN, LOCK_START)
            print(f"[Client {client_id}] Lock released.", flush=True)
            print(f"[Client {client_id}] SUCCESS", flush=True)

        except BlockingIOError:
            # Lock is held by another process
            if retry_count == 0:
                initial_block = True
                msg = f"[Client {client_id}] Lock initially blocked - waiting for release..."
                print(msg, flush=True)

            retry_count += 1
            if retry_count % 5 == 0:
                elapsed = time.time() - start_time
                print(
                    f"[Client {client_id}] Still waiting... (elapsed: {elapsed:.2f}s, retries: {retry_count})",
                    flush=True,
                )
            time.sleep(1)  # Wait before retrying

        except IOError as e:
            print(f"[Client {client_id}] IOError: {e}", flush=True)
            print(f"[Client {client_id}] FAILED", flush=True)
            sys.exit(1)

if not lock_acquired:
    elapsed = time.time() - start_time
    if initial_block:
        # Was blocked initially and never acquired - this is expected for some test scenarios
        print(
            f"[Client {client_id}] Lock remained blocked after {elapsed:.2f} seconds",
            flush=True,
        )
        print(f"[Client {client_id}] BLOCKED", flush=True)
    else:
        print(
            f"[Client {client_id}] Failed to acquire lock within {MAX_WAIT_TIME} seconds",
            flush=True,
        )
        print(f"[Client {client_id}] TIMEOUT", flush=True)
        sys.exit(1)

print(f"[Client {client_id}] Test completed.", flush=True)
