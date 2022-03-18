# -*- coding: utf-8 -*-
"""
Implements CephCI logging interface.

In this module, we implement a singleton LOG object that can be used by all components
of CephCI. It supports the below logging methods

    - error
    - warning
    - info
    - debug

Along with the above, it provides support for pushing events to

    - local file
    - logstash server
"""
import logging
from copy import deepcopy
from typing import Any, Dict

from .config import TestMetaData

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"


class LoggerInitializationException:
    pass


class Log:
    """CephCI Logger object to help streamline logging."""

    def __init__(self, filename="") -> None:
        """
        Initializes the logging mechanism based on the inputs provided."""
        self._logger = logging.getLogger(filename)
        self._log_level = logging.DEBUG
        self._log_dir = None
        self.log_format = LOG_FORMAT
        self.rp_logger = None

    def set_rp_logger(self, logger):
        self.rp_logger = logger

    @property
    def log_dir(self) -> str:
        """Return the absolute path to the logging folder."""
        return self._log_dir

    @property
    def log_level(self) -> int:
        """Return the logging level."""
        return self._log_level

    @property
    def config(self) -> Dict:
        """Return the CephCI run configuration."""
        return TestMetaData()

    @property
    def run_id(self) -> str:
        """Return the unique identifier of the execution run."""
        return self.config.get("run_id")

    @property
    def metadata(self) -> Dict:
        """Return the metadata of the execution run."""
        return dict(
            {
                "test_run_id": self.run_id,
                "testing_tool": "cephci",
                "rhcs": self.config.get("rhcs"),
                "test_build": self.config.get("rhbuild", "released"),
            }
        )

    def _log(self, level: str, message: Any, *args, **kwargs) -> None:
        """
        Log the given message using the provided level along with the metadata.

        *Args:
            level (str):        Log level
            message (Any):      The message that needs to be logged
        **kwargs:
            metadata (dict):    Extra information to be appended to logstash

        Returns:
            None.
        """
        log = {
            "info": self._logger.info,
            "debug": self._logger.debug,
            "warning": self._logger.warning,
            "error": self._logger.error,
            "exception": self._logger.exception,
        }
        extra = deepcopy(self.metadata)
        extra.update(kwargs.get("metadata", {}))
        log[level](message, *args, extra=extra, **kwargs)

    def info(self, message: Any, *args, **kwargs) -> None:
        """
        Log with info level the provided message and extra data.

        Args:
            message (Any):      The message to be logged.
            metadata (dict):    The metadata that would be sent along with the message.

        Returns:
            None
        """
        self._log(
            "info",
            message,
            *args,
            **kwargs,
        )

    def debug(self, message: Any, *args, **kwargs) -> None:
        """
        Log with debug level the provided message and extra data.

        Args:
            message (str):      The message to be logged.
            metadata (dict):    The metadata that would be sent along with the message.
            kwargs (Any):       Support keyword arguments by logging
        Returns:
            None
        """

        self._log(
            "debug",
            message,
            *args,
            **kwargs,
        )

    def warning(self, message: Any, *args, **kwargs) -> None:
        """
        Log with warning level the provided message and extra data.

        Args:
            message (Any):      The message to be logged.
            metadata (dict):    The metadata that would be sent along with the message.
            kwargs (Any):       Support keyword arguments by logging
        Returns:
            None
        """
        self._log("warning", message, *args, **kwargs)

    def error(
        self,
        message: Any,
        *args,
        **kwargs,
    ) -> None:
        """
        Log with error level the provided message and extra data.

        Args:
            message (Any):      The message to be logged.
            metadata (dict):    The metadata that would be sent along with the message.
            exc_info (boo):     Exception details to be logged.
            kwargs (Any):       Support keyword arguments by logging
        Returns:
            None
        """
        kwargs["exc_info"] = kwargs.get("exc_info", True)
        if self.rp_logger:
            self.rp_logger.log(message=message, level="ERROR")
        self._log("error", message, *args, **kwargs)

    def exception(
        self,
        message: Any,
        *args,
        **kwargs,
    ) -> None:
        """
        Log the given message under exception log level.

        Args:
            message (Any):  Message or record to be emitted.
            exc_info (boo):     Exception details to be logged.
            kwargs (Any):       Support keyword arguments by logging
        Returns:
            None
        """
        kwargs["exc_info"] = kwargs.get("exc_info", True)
        self._log("exception", message, *args, **kwargs)
