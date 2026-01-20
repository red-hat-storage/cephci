# -*- code: utf-8 -*-
"""
This module provides a context manager for running methods concurrently.

For backward compatability, spawn method is leveraged however one can also
choose to move submit. Likewise, thread pool executor is the default executor.

Timeout is an inherited feature provided by concurrent futures. Additionally,
one wait for all the threads/process to complete even when on thread or process
encounters an exception. This is useful when multiple test modules are
executing different test scenarios.

When a test module controls the threads then it can forcefully terminate all
threads when an exception is encountered.

Changelog:
    Version 1.0 used gevent module for parallel method execution.
    Version 2.0 uses concurrent.futures module instead of gevent.

You add functions to be run with the spawn method::

    with parallel() as p:
        for foo in bar:
            p.spawn(quux, foo, baz=True)

You can iterate over the results (which are in arbitrary order)::

    with parallel() as p:
        for foo in bar:
            p.spawn(quux, foo, baz=True)
        for result in p:
            print result

In version 2, you can choose whether to use threads or processes by

    with parallel(thread_pool=False, timeout=10) as p:
        _r = [p.spawn(quux, x) for name in names]

You can also specify the maximum number of worker threads/processes:
    with parallel(max_workers=20) as p:
        for foo in bar:
            p.spawn(quux, foo, baz=True)

If one of the spawned functions throws an exception, it will be thrown
when iterating over the results, or when the with block ends.

When the scope of with block changes, the main thread waits until all
spawned functions have completed within the given timeout. On timeout,
all pending threads/processes are issued shutdown command.
"""

import logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime, timedelta
from time import sleep

logger = logging.getLogger(__name__)


class parallel:
    """This class is a context manager for concurrent method execution."""

    def __init__(
        self,
        thread_pool=True,
        timeout=None,
        shutdown_cancel_pending=False,
        max_workers=None,
    ):
        """Object initialization method.

        Args:
            thread_pool (bool)          Whether to use threads or processes.
            timeout (int | float)       Maximum allowed time.
            shutdown_cancel_pending (bool) If enabled, it would cancel pending tasks.
        """
        if thread_pool:
            self._executor = ThreadPoolExecutor(max_workers=max_workers)
        else:
            self._executor = ProcessPoolExecutor(max_workers=max_workers)
        self._timeout = timeout
        self._cancel_pending = shutdown_cancel_pending
        self._futures = list()
        self._results = list()
        self._iter_index = 0

    @property
    def count(self):
        return len(self._futures)

    @property
    def results(self):
        return self._results

    def spawn(self, fun, *args, **kwargs):
        """Triggers the first class method.

        Args:
            func:       Function to be executed.
            args:       A list of variables to be passed to the function.
            kwargs      A dictionary of named variables.

        Returns:
            None
        """
        _future = self._executor.submit(fun, *args, **kwargs)
        self._futures.append(_future)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, trackback):
        _not_done = self._futures[:]
        _end_time = datetime.now() + timedelta(
            seconds=self._timeout if self._timeout else 3600
        )

        # Wait for all futures to complete within the given time or 1 hour.
        while datetime.now() < _end_time:
            # if the list is empty break
            if len(_not_done) == 0:
                break

            sleep(2.0)
            for _f in _not_done:
                if _f.done():
                    _not_done.remove(_f)

        # Graceful shutdown of running threads
        if _not_done:
            self._executor.shutdown(wait=False, cancel_futures=self._cancel_pending)

        if exc_value is not None:
            logger.exception(trackback)
            return False

        # Check for any exceptions and raise
        # At this point, all threads/processes should have completed or cancelled
        try:
            for _f in self._futures:
                self._results.append(_f.result())
        except Exception:
            logger.exception("Encountered an exception during parallel execution.")
            raise

        return True

    def __iter__(self):
        return self

    def __next__(self):
        if self.count == 0 or self._iter_index == self.count:
            self._iter_index = 0  # reset the counter
            raise StopIteration()

        try:
            # Keeping timeout consistent when called within the context
            _timeout = self._timeout if self._timeout else 3600
            out = self._futures[self._iter_index].result(timeout=_timeout)
        except Exception as e:
            logger.exception(e)
            out = e

        self._iter_index += 1
        return out
