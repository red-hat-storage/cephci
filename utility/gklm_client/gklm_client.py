from utility.gklm_client.auth import GklmAuth
from utility.gklm_client.certs import GklmCertificate
from utility.gklm_client.client_mgmt import GklmClientManagement
from utility.gklm_client.objects import GklmObjects


class GklmClient:
    def __init__(
        self,
        host: str,
        port: int = 9443,
        user: str = None,
        password: str = None,
        verify: bool = False,
    ):
        self.auth = GklmAuth(host, port, user, password, verify)
        self.certificates = GklmCertificate(self.auth)
        self.clients = GklmClientManagement(self.auth)
        self.objects = GklmObjects(self.auth)
        self.auth.login()

    def login(self):
        return self.auth.login()

    def logout(self):
        return self.auth.logout()

    def headers(self):
        return self.auth._headers()
