from utility.gklm_client.auth import GklmAuth
from utility.gklm_client.certs import GklmCertificate
from utility.gklm_client.client_mgmt import GklmClientManagement
from utility.gklm_client.objects import GklmObjects
from utility.gklm_client.server_mgmt import GklmServer


class GklmClient:
    def __init__(
        self,
        ip: str,
        user: str,
        password: str,
        port: int = 9443,
        verify: bool = False,
    ):
        self.auth = GklmAuth(
            host=ip, port=port, user=user, password=password, verify=verify
        )
        self.certificates = GklmCertificate(self.auth)
        self.clients = GklmClientManagement(self.auth)
        self.objects = GklmObjects(self.auth)
        self.server = GklmServer(self.auth)
        self.auth.login()

    def login(self):
        return self.auth.login()

    def logout(self):
        return self.auth.logout()

    def headers(self):
        return self.auth._headers()
