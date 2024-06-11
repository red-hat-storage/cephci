import argparse
import fcntl
import os
import sys
import time
from typing import Any, Optional

LOCK_DIR = "/run/cephadm"


class _Acquire_ReturnProxy(object):
    def __init__(self, lock: "FileLock") -> None:
        self.lock = lock
        return None

    def __enter__(self) -> "FileLock":
        return self.lock

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.lock.release()
        return None


class FileLock(object):
    def __init__(self, name: str, timeout: int = -1) -> None:
        if not os.path.exists(LOCK_DIR):
            os.mkdir(LOCK_DIR, 0o700)
        self._lock_file = os.path.join(LOCK_DIR, name + ".lock")

        self._lock_file_fd: Optional[int] = None
        self.timeout = timeout
        self._lock_counter = 0
        return None

    @property
    def is_locked(self) -> bool:
        return self._lock_file_fd is not None

    def acquire(
        self, timeout: Optional[int] = None, poll_intervall: float = 0.05
    ) -> _Acquire_ReturnProxy:
        # Use the default timeout, if no timeout is provided.
        if timeout is None:
            timeout = self.timeout

        # Increment the number right at the beginning.
        # We can still undo it, if something fails.
        self._lock_counter += 1

        lock_id = id(self)  # noqa
        lock_filename = self._lock_file  # noqa
        start_time = time.time()
        try:
            while True:
                if not self.is_locked:
                    self._acquire()

                if self.is_locked:
                    break
                elif timeout >= 0 and time.time() - start_time > timeout:
                    raise Timeout(self._lock_file)  # noqa
                else:
                    time.sleep(poll_intervall)
        except Exception:
            # Something did go wrong, so decrement the counter.
            self._lock_counter = max(0, self._lock_counter - 1)

            raise
        return _Acquire_ReturnProxy(lock=self)

    def release(self, force: bool = False) -> None:
        if self.is_locked:
            self._lock_counter -= 1

            if self._lock_counter == 0 or force:
                self._release()
                self._lock_counter = 0

        return None

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.release()
        return None

    def __del__(self) -> None:
        self.release(force=True)
        return None

    def _acquire(self) -> None:
        open_mode = os.O_RDWR | os.O_CREAT | os.O_TRUNC
        fd = os.open(self._lock_file, open_mode)

        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (IOError, OSError):
            os.close(fd)
        else:
            self._lock_file_fd = fd
        return None

    def _release(self) -> None:
        fd = self._lock_file_fd
        self._lock_file_fd = None
        fcntl.flock(fd, fcntl.LOCK_UN)  # type: ignore
        os.close(fd)  # type: ignore
        return None


def command_hold_lock(args):
    lock = FileLock(args.fsid, args.timeout)
    lock.acquire()
    while True:
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser(
        description="Hold cephadm lock",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(help="sub-command")
    parser_connect = subparsers.add_parser("hold-lock", help="hold onto cephadm lock")
    parser_connect.set_defaults(func=command_hold_lock)
    parser_connect.add_argument(
        "--fsid", help="cluster fsid for which to hold lock", required=True
    )
    parser_connect.add_argument(
        "--timeout", help="timeout for holding lock", required=False, default=-1
    )

    args = parser.parse_args()
    if "func" not in args:
        print("No command specified")
        sys.exit()
    # print(args)
    r = args.func(args)

    if not r:
        r = 0
    sys.exit()


if __name__ == "__main__":
    main()
