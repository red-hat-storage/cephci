import logging
import logging.handlers
import os
import re
from copy import deepcopy
from typing import Dict

from .config import TestMetaData

LOG_FORMAT = (
    "%(asctime)s - %(name)s - %(module)s:%(lineno)d - %(levelname)s - %(message)s"
)

magna_server = "http://magna002.ceph.redhat.com"
magna_url = f"{magna_server}/cephci-jenkins/"


class LoggerInitializationException(Exception):
    """Exception raised for logger initialization errors."""

    pass


class Log(logging.Logger):
    """CephCI Logger object to help streamline logging."""

    def __init__(self, name=None) -> None:
        """
        Initializes the logging mechanism.
        Args:
            name (str): Logger name (module name or other identifier).
        """
        super().__init__(name)
        logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
        # self._logger = logging.getLogger(name)
        self._logger = logging.getLogger("cephci")

        # Set logger name
        if name:
            self.name = f"cephci.{name}"

        # Additional attributes
        self._log_level = self.getEffectiveLevel()
        self._log_dir = None
        self.log_format = LOG_FORMAT
        self._log_errors = []
        self.info = self._logger.info
        self.debug = self._logger.debug
        self.warning = self._logger.warning
        self.error = self._logger.error
        self.exception = self._logger.exception

    @property
    def rp_logger(self):
        return self.config.get("rp_logger")

    @property
    def log_dir(self) -> str:
        """Return the absolute path to the logging folder."""
        return self._log_dir

    @property
    def log_level(self) -> int:
        """Return the logging level."""
        return self._log_level

    @property
    def logger(self) -> logging.Logger:
        """Return the logger."""
        return self._logger

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
                "rp_logger": self.config.get("rp_logger", None),
            }
        )

    def log_error(self, message: str) -> None:
        """Logs an error and appends it to the internal error tracker.

        Args:
            message (str): The error message to log and track.
        """
        self._log_errors.append(message)
        self.error(message)

    def configure_logger(self, test_name, run_dir, disable_console_log, **kwargs):
        """Configures a new FileHandler for the root logger.

        Args:
            test_name: name of the test being executed. used for naming the logfile
            run_dir: directory where logs are being placed
        Returns:
            URL where the log file can be viewed or None if the run_dir does not exist
        """
        if not os.path.isdir(run_dir):
            self._logger.error(
                f"Run directory '{run_dir}' does not exist, logs will not output to file."
            )
            return None

        self.close_and_remove_filehandlers()
        pass_filter = SensitiveLogFilter(name="cephci_filter")

        log_format = logging.Formatter(self.log_format)
        full_log_name = f"{test_name}.log"
        test_logfile = os.path.join(run_dir, full_log_name)
        self._logger.info(f"Test logfile: {test_logfile}")

        if disable_console_log:
            self._logger.propagate = False

        _handler = logging.FileHandler(test_logfile)
        _handler = logging.handlers.RotatingFileHandler(
            test_logfile,
            maxBytes=10 * 1024 * 1024,  # Set the maximum log file size to 10 MB
            backupCount=20,  # Keep up to 20 old log files which will be 200 MB per test case
        )
        _handler.setFormatter(log_format)
        _handler.addFilter(pass_filter)
        self._logger.addHandler(_handler)

        # error file handler
        err_logfile = os.path.join(run_dir, f"{test_name}.err")
        _err_handler = logging.FileHandler(err_logfile)
        _err_handler.setFormatter(log_format)
        _err_handler.setLevel(logging.ERROR)
        _err_handler.addFilter(pass_filter)
        self._logger.addHandler(_err_handler)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(log_format)
        console_handler.addFilter(pass_filter)
        if not any(isinstance(h, logging.StreamHandler) for h in self._logger.handlers):
            self._logger.addHandler(console_handler)

        url_base = (
            magna_url + run_dir.split("/")[-1]
            if "/ceph/cephci-jenkins" in run_dir
            else run_dir
        )
        log_url = f"{url_base}/{full_log_name}"
        self._logger.debug("Completed log configuration")

        return log_url

    def close_and_remove_filehandlers(self):
        """Close FileHandlers and then remove them from the logger's handlers list."""
        handlers = self._logger.handlers[:]
        for handler in handlers:
            if isinstance(handler, logging.FileHandler):
                handler.close()
                self._logger.removeHandler(handler)


class SensitiveLogFilter(logging.Filter):
    """Filter known sensitive data from being logged."""

    excluded_words = [
        "access-key",
        "access_key",
        "keyring",
        "password",
        "token",
    ]

    def redact_list(self, data):
        """Redact values in the iterator."""
        for i, v in enumerate(data):
            if isinstance(v, list):
                self.redact_list(data[i])
            elif isinstance(v, dict):
                self.redact_dict(data[i])
            elif isinstance(v, tuple):
                data[i] = self.redact(v)
            elif isinstance(v, (str, bytearray, bytes)):
                data[i] = self.redact_str(v)

    def redact_dict(self, data):
        """Redact values based on keys"""
        for _key in data.keys():
            if _key in self.excluded_words:
                data[_key] = "<masked>"
            elif isinstance(data[_key], dict):
                self.redact_dict(data[_key])
            elif isinstance(data[_key], list):
                self.redact_list(data[_key])
            elif isinstance(data[_key], tuple):
                data[_key] = self.redact(data[_key])
            elif isinstance(data[_key], (str, bytearray, bytes)):
                data[_key] = self.redact_str(data[_key])

    def redact_str(self, data):
        """Redact strings containing sensitive keys."""
        _words = "|".join(self.excluded_words)
        rtn = re.sub(
            rf'({_words})\s*[:=]?\s*(["\']?)([^\s"\']+)(\2)(\s|$)',
            r"\1 <masked>\5",
            data,
            flags=re.IGNORECASE,
        )
        return rtn

    def redact(self, msg):
        """Return the redacted message if sensitive data found.

        The method replaces strings that are captured after known words. If
        the method encounters a dict, the keys of the dict are scanned for
        excluded fields.
        """
        data = deepcopy(msg)

        if isinstance(data, dict):
            self.redact_dict(data)
            return data

        if isinstance(data, list):
            self.redact_list(data)
            return data

        if isinstance(data, tuple):
            return tuple(self.redact(arg) for arg in data)

        if isinstance(data, (str, bytearray, bytes)):
            data = data if isinstance(data, str) else str(data, "utf-8")
            return self.redact_str(data)

        # Basic types that require no processing
        return data

    def filter(self, record):
        """Modifies the log record.

        When password or passwd is found in the message, then the next word is
        masked. This is the assumption we are following based on

        - logging of passwords when registering the server
        - logging of password using as authentication.
        """
        record.msg = self.redact(record.msg)
        if isinstance(record.args, dict):
            for k in record.args.keys():
                record.args[k] = self.redact(record.args[k])
        else:
            record.args = tuple(self.redact(arg) for arg in record.args)

        return True
