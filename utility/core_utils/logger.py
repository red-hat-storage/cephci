import logging
import sys


class LoggerMixin(logging.Handler):
    """
    Base class for logging mechanism.
    """

    def __init__(
        self,
        logger_name,
        log_format="%(asctime)s - %(levelname)s - %(name)s:%(lineno)d - %(message)s",
    ):
        """
        Initializes the logging mechanism and its properties based on the inputs provided.
        """
        self.formatter = logging.Formatter(log_format)
        self.logger = logging.getLogger(logger_name)
        self.logger.name = logger_name

    def get_console_handler(self):
        """Defines handler which writes DEBUG messages or higher to the sys.stderr.

        Args:
            None

        Returns:
            handler
        """
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(self.formatter)
        self.logger.setLevel(logging.INFO)
        return console_handler

    def get_file_handler(self, log_file):
        """Defines handler which writes ERROR messages or higher to the sys.stderr to file.

        Args:
            None

        Returns:
            handler
        """
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(self.formatter)
        self.logger.setLevel(logging.ERROR)
        return file_handler

    def get_logger(self, log_file=None):
        """Create a logging object for STDOUT and FILE to avoid print statements.

        Args:
            logger_name (str): name for the logger used in getLogger()

        """
        self.logger.addHandler(self.get_console_handler())
        if log_file:
            self.logger.addHandler(self.get_file_handler(log_file))

        return self.logger

    def warning(self, msg):
        """
        Log the given message under warning log level.

        Args:
            message (Any):  Message or record to be emitted.
        Returns:
            None
        """
        self.logger.warning(msg)

    def debug(self, msg):
        """
        Log the given message under debug log level.

        Args:
            message (Any):  Message or record to be emitted.
        Returns:
            None
        """
        self.logger.debug(msg)

    def info(self, msg):
        """
        Log the given message under info log level.

        Args:
            message (Any):  Message or record to be emitted.
        Returns:
            None
        """
        self.logger.info(msg)

    def error(self, msg):
        """
        Log the given message under error log level.

        Args:
            message (Any):  Message or record to be emitted.
        Returns:
            None
        """
        self.logger.error(msg)
