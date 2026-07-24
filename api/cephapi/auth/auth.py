from api import Api

from .check import Check
from .logout import Logout


class Auth(Api):
    """Interface for `api/auth` cephapi"""

    def __init__(self, url, header, api="api/auth"):
        super().__init__(url, api)

        self.header = header
        self.check = Check(url=url, header=self.header, api=f"{api}/check")
        self.logout = Logout(url=url, api=f"{api}/logout")

    def post(self, username, password, check_sc=False):
        """Post request method for API

        Args:
            username (str): Dashboard username
            password (str): Dashboard password
            check_sc (bool): Check for status code validation
        """
        data = {
            "username": username,
            "password": password,
        }

        return super().post(data=data, header=self.header, check_sc=check_sc)
