#!/usr/bin/env python3
import fcntl
import os
import sys
import time

if len(sys.argv) < 4:
    print("Usage: multi_client_byte_range.py <mount_path> <start> <length>")
    sys.exit(1)

mount_path = sys.argv[1]
LOCK_START = int(sys.argv[2])
LOCK_LEN = int(sys.argv[3])
TEST_FILE = os.path.join(mount_path, "multi_client_lock_test_file")

os.makedirs(os.path.dirname(TEST_FILE), exist_ok=True)
with open(TEST_FILE, "a"):
    pass  # Ensure file exists

print(f"[LOCKER] Trying lock start={LOCK_START}, len={LOCK_LEN}", flush=True)

with open(TEST_FILE, "r+") as f:
    lock_acquired = False
    while not lock_acquired:
        try:
            fcntl.lockf(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB, LOCK_LEN, LOCK_START)
            lock_acquired = True
            print("[LOCKER] Lock acquired", flush=True)

            # Hold the lock
            time.sleep(20)

            fcntl.lockf(f.fileno(), fcntl.LOCK_UN, LOCK_LEN, LOCK_START)
            print("[LOCKER] Lock released", flush=True)

        except BlockingIOError:
            # Another client holds the lock
            print(
                "[LOCKER] Lock currently held by another client, retrying...",
                flush=True,
            )
            time.sleep(2)  # Wait before retrying
