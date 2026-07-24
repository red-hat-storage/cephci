from api import Api


class Check(Api):
    """Interface for `api/auth/check` cephapi"""

    def __init__(self, url, header, api="api/check"):
        self.header = header
        super().__init__(url, api)

    def post(self, token, check_sc=False):
        """Post request method for API

        Args:
            token (str): API authorization token
            check_sc (bool): Check for status code validation
        """
        data = {"token": token}

        return super().post(data=data, header=self.header, check_sc=check_sc)
