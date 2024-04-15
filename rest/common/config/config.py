"""Config file reader"""


import json
import os


class Config(object):
    """
    Config python class to read the config.json and return python object for other consumers
    """

    def __init__(self, absFPath=None):
        """constructor for setting up config json file

        Args:
            :absFPath (str): absolute file path to any json file.
                             Default: None. if None, config.json is used.

        Returns:
            None
        """

        self._fname = absFPath if absFPath else self.__absPath("config.json")

    def get_config(self):
        """
        To read the config json file as file path by self._fname
        Returns:
            python object of the config json file
        """
        with open(self._fname, "r") as fp:
            self.config = json.load(fp)
        return self.config

    @staticmethod
    def __absPath(fileName):
        """return absolute path of file located under common folder.

        Args:
            :fileName (str): fileName to populate the absolute path

        Returns:
            config.json path (str)
        """

        fpath = os.path.abspath(__file__)
        dir = os.path.dirname(fpath)
        filename = f"{dir}/config.json"
        return filename
