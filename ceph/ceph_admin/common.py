"""Contains common functions that can used across the module."""

from typing import Dict


def config_dict_to_string(data: Dict) -> str:
    """
    Convert the provided data to a string of optional arguments.

    Args:
        data (Dict):   Key/value pairs that are CLI optional arguments

    Return:
        string instead of the a data dict (Str)
    """
    rtn = ""
    for key, value in data.items():
        if isinstance(value, bool) and value is False:
            continue

        rtn += f" -{key}" if len(key) == 1 else f" --{key}"

        if not isinstance(value, bool):
            rtn += f" {value}"

    return rtn


def fetch_method(obj, method):
    """
    Fetch object attributes by name

    Args:
        obj (CephAdmin): class object
        method (Str): name of the method

    Returns:
        function (Func)
    """
    try:
        return getattr(obj, method)
    except AttributeError:
        raise NotImplementedError(f"{obj.__class__} has not implemented {method}")
