import argparse
import errno
import fcntl
import time

parser = argparse.ArgumentParser(description="Lock or unlock a file using fcntl")
parser.add_argument("filename", help="The name of the file to lock or unlock")
parser.add_argument(
    "lock_action", choices=["lock", "unlock"], help="The action to perform on the lock"
)
parser.add_argument(
    "--timeout",
    type=float,
    default=5,
    help="Timeout value in seconds. Default is 5 seconds.",
)
args = parser.parse_args()

filename = args.filename
lock_action = args.lock_action
timeout = args.timeout

# Open the file in read-write mode
with open(filename, "r+") as file:
    if lock_action == "lock":
        # Try to acquire an exclusive (write) lock with timeout
        try:
            fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            time.sleep(timeout)
        except OSError as e:
            if e.errno == errno.EWOULDBLOCK:
                print(
                    f"The file is already locked or could not be locked within {timeout} seconds."
                )
            else:
                # Other errors
                print(f"Error: {e}")
        else:
            # The lock was acquired successfully
            print("Lock acquired successfully.")
    elif lock_action == "unlock":
        # Release the lock
        fcntl.flock(file, fcntl.LOCK_UN)
        print("Lock released successfully.")
    else:
        # Invalid lock action
        print("Error: Invalid lock action specified. Must be one of: lock, unlock.")
