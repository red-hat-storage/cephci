"""Contains common functions that can used across the module."""
from typing import Dict


def config_dict_to_string(data: Dict) -> str:
    """
    Convert the provided data to a string of optional arguments.

    Args:
        data:   Key/value pairs that are CLI optional arguments

    Return:
        string instead of the a data dict
    """
    rtn = ""
    for key, value in data.items():
        rtn += f" -{key}" if len(key) == 1 else f" --{key}"

        if not isinstance(value, bool):
            rtn += f" {value}"

    return rtn


def fetch_method(obj, method):
    """
    fetch object attributes by name
    Args:
        obj: class object
        method: name of the method
    Returns:
        function
    """
    try:
        return getattr(obj, method)
    except AttributeError:
        raise NotImplementedError(f"{obj.__class__} has not implemented {method}")
