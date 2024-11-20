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

If one of the spawned functions throws an exception, it will be thrown
when iterating over the results, or when the with block ends.

At the end of the with block, the main thread waits until all
spawned functions have completed, or, if one exited with an exception,
kills the rest and raises the exception.
"""

import logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


class parallel:
    """This class is a context manager for concurrent method execution."""

    def __init__(
        self,
        thread_pool=True,
        timeout=None,
        shutdown_wait=True,
        shutdown_cancel_pending=False,
    ):
        """Object initialization method.

        Args:
            thread_pool (bool)          Whether to use threads or processes.
            timeout (int | float)       Maximum allowed time.
            shutdown_wait (bool)        If disabled, it would not wait for executing
                                        threads/process to complete.
            shutdown_cancel_pending (bool) If enabled, it would cancel pending tasks.
        """
        self._executor = ThreadPoolExecutor() if thread_pool else ProcessPoolExecutor()
        self._timeout = timeout
        self._shutdown_wait = shutdown_wait
        self._cancel_pending = shutdown_cancel_pending
        self._futures = list()
        self._results = list()

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
        _exceptions = []
        exception_count = 0

        for _f in as_completed(self._futures, timeout=self._timeout):
            try:
                self._results.append(_f.result())
            except Exception as e:
                logger.exception(e)
                _exceptions.append(e)
                exception_count += 1

            if exception_count > 0 and not self._shutdown_wait:
                # At this point we are ignoring results
                self._executor.shutdown(wait=False, cancel_futures=self._cancel_pending)
                raise _exceptions[0]

        if len(_exceptions) > 0:
            raise _exceptions[0]

        return False if exception_count == 0 else True

    def __iter__(self):
        return self

    def __next__(self):
        for r in self._results:
            yield r
