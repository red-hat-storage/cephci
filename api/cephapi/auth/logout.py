from api import Api


class Logout(Api):
    """Interface for `api/auth/logout` cephapi"""

    def __init__(self, url, api="api/logout"):
        super().__init__(url, api)

    def post(self, check_sc=False):
        """Post request method for API

        Args:
            check_sc (bool): Check for status code validation
        """
        return super().post(check_sc=check_sc)
