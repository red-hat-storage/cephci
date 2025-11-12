import time
import traceback
from functools import wraps

from utility.log import Log

logger = Log(__name__)


def retry(exception_to_check, tries=4, delay=3, backoff=2):
    """
    Retry calling the decorated function using exponential backoff.

    Args:
        exception_to_check: the exception to check. may be a tuple of exceptions to check
        tries: number of times to try (not retry) before giving up
        delay: initial delay between retries in seconds
        backoff: backoff multiplier e.g. value of 2 will double the delay each retry
    """

    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exception_to_check as e:
                    # Get exception type name safely
                    try:
                        exception_name = type(e).__name__
                    except Exception:
                        exception_name = "UnknownException"

                    # Get function name (including class if it's a method)
                    try:
                        func_name = f.__name__
                        if args and hasattr(args[0], "__class__"):
                            class_name = args[0].__class__.__name__
                            caller_info = f"{class_name}.{func_name}"
                        else:
                            caller_info = func_name
                    except Exception:
                        caller_info = "unknown_function"

                    # Log warning with caller and exception name
                    logger.warning(
                        f"{caller_info} raised {exception_name}, "
                        f"Retrying in {mdelay} seconds... "
                        f"(Attempt {tries - mtries + 1}/{tries})"
                    )

                    # Log full exception details at debug level
                    try:
                        logger.debug(
                            f"Full exception details for {caller_info}:\n"
                            f"Exception: {exception_name}\n"
                            f"Message: {str(e)}\n"
                            f"Traceback:\n{traceback.format_exc()}"
                        )
                    except Exception:
                        # Fallback if traceback formatting fails
                        logger.debug(
                            f"Exception in {caller_info}: {exception_name} - {str(e)}"
                        )

                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry

    return deco_retry
