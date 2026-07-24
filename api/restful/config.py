from api import Api


class Config(Api):
    """Interface for RESTful endpoint `/config`"""

    def __init__(self, url, api="/config"):
        super().__init__(url, api)

    def get(self, username, key, check_sc=False):
        """
        Get request method for API

        Args:
            username (str): API user
            key (str): API user key
            check_sc (bool): Check for status code validation
        """
        return super().get(auth=(username, key), check_sc=check_sc)

    def patch(self, username, key, data, check_sc=False):
        """Patch request method for API

        Args:
            username (str): API user
            key (str): API user key
            data (str): Data to be posted
            check_sc (bool): Check for status code validation
        """
        return super().patch(data=data, auth=(username, key), check_sc=check_sc)
