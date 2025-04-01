import time
from functools import wraps

from utility.log import Log

logger = Log(__name__)


def timer():
    """Decorator factory with logging option"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                func(*args, **kwargs)
            finally:
                duration = time.perf_counter() - start
                msg = f"{func.__name__} executed in {duration:.6f} seconds"
                logger.info(f"[TIMER] {msg}")

                # Store duration as function attribute
                wrapper.last_execution_time = duration
                wrapper.total_time = getattr(wrapper, "total_time", 0) + duration

        return wrapper

    return decorator
