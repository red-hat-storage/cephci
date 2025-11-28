import fcntl
import os
import sys
import time

mount_path = sys.argv[1]
TEST_FILE = os.path.join(mount_path, "single_client_byte_range_file")
LOCK_START = 0
LOCK_LEN = 10
LOCK_DURATION = 15

os.makedirs(os.path.dirname(TEST_FILE), exist_ok=True)
with open(TEST_FILE, "a"):
    pass  # Create file if missing

print(f"Opening file: {TEST_FILE}")

with open(TEST_FILE, "r+") as f:
    print(f"Acquiring byte-range lock: start={LOCK_START}, len={LOCK_LEN}")

    try:
        fcntl.lockf(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB, LOCK_LEN, LOCK_START)
        print("Lock acquired successfully!")

        # Hold the lock
        time.sleep(LOCK_DURATION)

    except IOError:
        print("Failed to acquire lock — already locked!")

    finally:
        # Unlock
        fcntl.lockf(f.fileno(), fcntl.LOCK_UN, LOCK_LEN, LOCK_START)
        print("Lock released.")

print("Byte-range lock test completed.")
