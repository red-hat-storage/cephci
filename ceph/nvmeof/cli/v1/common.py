"""Ceph-NVMeoF Common/Generic utility module."""

from functools import wraps

from utility.log import Log

LOG = Log(__name__)


def substitute_keys(key_map: dict):
    """Decorator factory that renames keys in kwargs based on mapping.

    This key map is to support the existing test case(s) which works with
     existing Container CLI args.

    Args:
        key_map (dict): Key dict mapping
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if "args" in kwargs and isinstance(kwargs["args"], dict):
                kwargs["args"] = {
                    (key_map.get(k, k)): v for k, v in kwargs["args"].items()
                }
            return func(*args, **kwargs)

        return wrapper

    return decorator
