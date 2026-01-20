#!/usr/bin/env python3
"""
Script to test queue management for byte-range locks on NFS.
Tests FIFO (First In First Out) behavior when multiple clients request the same lock.
"""

import fcntl
import os
import sys
import time

if len(sys.argv) < 5:
    print(
        "Usage: queue_management_locks.py <mount_path> <client_id> <start> <length>",
        flush=True,
    )
    sys.exit(1)

mount_path = sys.argv[1]
client_id = sys.argv[2]
LOCK_START = int(sys.argv[3])
LOCK_LEN = int(sys.argv[4])
TEST_FILE = os.path.join(mount_path, "queue_management_locks_test_file")
LOCK_DURATION = 8
# Maximum time to wait for lock acquisition
# For 4 clients: 4 clients × 8 seconds lock duration = 32 seconds minimum
# Add buffer for retry delays and network overhead
MAX_WAIT_TIME = 120  # Increased to 120 seconds to handle multiple clients

os.makedirs(os.path.dirname(TEST_FILE), exist_ok=True)
with open(TEST_FILE, "a"):
    pass  # Ensure file exists

print(f"[Client {client_id}] Opening file: {TEST_FILE}", flush=True)
print(
    f"[Client {client_id}] Requesting lock: start={LOCK_START}, len={LOCK_LEN}",
    flush=True,
)

start_time = time.time()
lock_acquired = False
retry_count = 0

# Open file once and keep it open for the entire retry loop
# This ensures we're working with the same file handle
with open(TEST_FILE, "r+") as f:
    while not lock_acquired and (time.time() - start_time) < MAX_WAIT_TIME:
        try:
            # Try to acquire exclusive lock (non-blocking)
            fcntl.lockf(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB, LOCK_LEN, LOCK_START)
            lock_acquired = True
            acquisition_time = time.time() - start_time
            msg = (
                f"[Client {client_id}] Lock acquired after {acquisition_time:.2f} seconds "
                f"(retry {retry_count})"
            )
            print(msg, flush=True)
            print(
                f"[Client {client_id}] ACQUIRED_AT {acquisition_time:.2f}", flush=True
            )

            # Hold the lock for specified duration
            time.sleep(LOCK_DURATION)

            # Release the lock
            fcntl.lockf(f.fileno(), fcntl.LOCK_UN, LOCK_LEN, LOCK_START)
            print(f"[Client {client_id}] Lock released.", flush=True)
            print(f"[Client {client_id}] SUCCESS", flush=True)

        except BlockingIOError:
            # Lock is held by another process - wait and retry
            retry_count += 1
            elapsed = time.time() - start_time
            if retry_count % 5 == 0:
                msg = (
                    f"[Client {client_id}] Waiting in queue... "
                    f"(elapsed: {elapsed:.2f}s, retries: {retry_count}, max_wait: {MAX_WAIT_TIME}s)"
                )
                print(msg, flush=True)
            # Check if we're approaching timeout
            if elapsed >= MAX_WAIT_TIME - 1:
                msg = (
                    f"[Client {client_id}] Approaching timeout "
                    f"(elapsed: {elapsed:.2f}s, max: {MAX_WAIT_TIME}s)"
                )
                print(msg, flush=True)
            time.sleep(1)  # Wait before retrying

        except IOError as e:
            print(f"[Client {client_id}] IOError: {e}", flush=True)
            print(f"[Client {client_id}] FAILED", flush=True)
            sys.exit(1)

if not lock_acquired:
    elapsed = time.time() - start_time
    msg = (
        f"[Client {client_id}] Failed to acquire lock within {MAX_WAIT_TIME} seconds "
        f"(waited {elapsed:.2f}s)"
    )
    print(msg, flush=True)
    print(f"[Client {client_id}] TIMEOUT", flush=True)
    sys.exit(1)

print(f"[Client {client_id}] Test completed.", flush=True)
