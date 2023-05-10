"""Helper object to encapsulate waiting for timeouts"""

import time


class WaitUntil(object):
    """A wait-retry loop as iterable.

    This object abstracts away the wait logic allowing functions
    to write the retry logic in a for-loop.
    """

    def __init__(self, timeout=60, interval=1):
        self.timeout = timeout
        self.interval = interval
        self.expired = False
        self._attempt = 0
        self._start = None

    def __iter__(self):
        return self

    def __next__(self):
        if self._start is None:
            self._start = time.time()
        if time.time() - self._start > self.timeout:
            self.expired = True
            raise StopIteration()
        if self._attempt != 0:
            time.sleep(self.interval)
        self._attempt += 1
        return self
