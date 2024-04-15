"""This module defines custom Exceptions"""


class HTTPError(Exception):
    """Base class for HTTP errors."""

    def __init__(self, *args, **kwargs):
        """Constructor for HTTP Error"""
        self.response = kwargs.pop("response", None)
        super(Exception, self).__init__(*args, **kwargs)


class CommandExecutionError(Exception):
    """Base class any command execution error"""


class CommandTimeoutError(Exception):
    """Base class for commands execution timeout errors."""
