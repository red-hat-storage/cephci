import json
import logging
import os

import yaml

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

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_FOLDER = os.path.join(BASE, "models")


class LoaderMixin:
    def get_file_path(self, filename, kind, base=CONFIG_FOLDER):
        """
        Get the path for the object.
        Args:
            kind(str)      :  type of file.
            base(str)      :  root path of module.
            filename(str)  :  name of file

        Returns:
            Dict : get path of file based on kind and filename.

        Raises:
            Exception: An error occurred if given file is not found
        """
        filename = f"{base}/{kind}/{filename}"
        if not (filename.endswith(".json") or filename.endswith(".yaml")):
            logger.error(f"Neither json nor yaml {filename}")
            raise Exception(f"Neither json nor yaml {filename}")
        if os.path.isfile(filename):
            return filename
        logger.error(f"No {filename} was found")
        raise Exception(f"No {filename} was found")

    def _load_file(self, filename):
        """
        Loads file and returns content based on file extension.
        Args:
            filename(str)  :  name of file

        Returns:
            Dict : content from configuration file in dictionary format.
        """
        with open(filename, "r") as f:
            if filename[-4:] == "json":
                content = json.load(f)
            else:
                content = yaml.safe_load(f) or {}
            return content

    def load_file(self, filename, kind=None, base=CONFIG_FOLDER):
        """
        Returns content as dictionary present in the filename given.
        Args:
            kind(str)      :  type of file.
            base(str)      :  root path of module.
            filename(str)  :  name of file

        Returns:
            Dict : content from configuration file in dictionary format
        """
        if not kind:
            return self._load_file(f"{base}/{filename}")
        else:
            filename = self.get_file_path(filename, kind)
            return self._load_file(filename)

    def load_all_provisioners(self, filename, base=CONFIG_FOLDER):
        """
        Returns content from provisioners directory.
        Args:
            filename(str)  :  name of provisioner spec file
            base(str)      :  root path of module.

        Returns:
            Dict : content read from given file
        """
        return self.load_file(filename, base + "/provisioners")

    def load_all_installers(self, filename, base=CONFIG_FOLDER):
        """
        Returns content from installers directory.
        Args:
            filename(str)  :  name of orchestrator spec file
            base(str)      :  root path of module.

        Returns:
            Dict : content read from given file
        """
        return self.load_file(filename, base + "/installers")

    def load_all_suites(self, filename, base=CONFIG_FOLDER):
        """
        Returns content from suites directory.
        Args:
            filename(str)  :  name of test suite spec file
            base(str)      :  root path of module.

        Returns:
            Dict : content read from given file
        """
        return self.load_file(filename, base + "/suites")
