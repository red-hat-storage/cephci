from api import Api


class Perf(Api):
    """Interface for RESTful endpoint `/perf`"""

    def __init__(self, url, api="/perf"):
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
