from api import Api


class Crush(Api):
    """Interface for RESTful endpoint `/crush/rule`"""

    def __init__(self, url, api="/crush/rule"):
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
