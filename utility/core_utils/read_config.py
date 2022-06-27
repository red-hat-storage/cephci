import logging

from src.utilities.loader import LoaderMixin

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(name)s:%(lineno)d - %(message)s"
)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)

file_handler = logging.FileHandler("startup.log", mode="a")
file_handler.setLevel(logging.ERROR)

file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


class ReadConfig:
    """
    Singleton class to read configuration file given by user.
    """

    __instance__ = None
    _config = None

    def __new__(cls):
        if not ReadConfig.__instance__:
            ReadConfig.__instance__ = super(ReadConfig, cls).__new__(cls)
        return ReadConfig.__instance__

    def load_config(self, filename, kind=None):
        """
        Loads configuration files using kind attribute method.
        Args:
            file(str)  :  file of the configuration file to be loaded

        Returns:
            None
        """
        if not filename:
            logger.error("No config file was provided as input for {kind}")
            raise Exception("Please provide valid configuration file")
        if not self._config:
            logger.info(f"Started loading config file {filename} ")
            self._config = LoaderMixin().load_file(filename, kind)
        return self._config

    def get_config(self, property, filename):
        """
        Returns configuration dictionary according to property given.
        Args:
            property(str)  :  name of the configuration file

        Returns:
            dict : configuration file in dictionary format
        """
        self._config = self.load_config(filename)
        if property not in self._config.keys():
            logger.error(f"{property} specified is not valid")
            raise Exception(f"{property} specified is not valid")
        return self._config.get(property)
