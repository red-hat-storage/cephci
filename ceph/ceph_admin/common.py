"""Contains common functions that can used across the module."""

import shlex
from typing import Dict


def _quote_cli_value(value) -> str:
    """Quote a CLI value once for the remote shell.

    Callers such as ``configure_hosts`` still pass ``repr("*")``, which is
    already the shell token ``'*'``. Quoting that again sends the quote
    characters to the command. A value that is already exactly one
    shell-quoted token is returned unchanged. Anything else is quoted.
    """
    text = str(value)
    try:
        tokens = shlex.split(text)
    except ValueError:
        tokens = []
    if len(tokens) == 1 and shlex.quote(tokens[0]) == text:
        return text
    return shlex.quote(text)


def config_dict_to_string(data: Dict) -> str:
    """
    Convert the provided data to a string of optional arguments.

    Values are shell-quoted when they contain metacharacters. The string is
    joined into a remote shell command, so an unquoted ``*`` is expanded to
    a filename before it reaches ``ceph``. A value that is already a single
    shell-quoted token is not quoted a second time.

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
            rtn += f" {_quote_cli_value(value)}"

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
