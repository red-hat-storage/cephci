from typing import Any, Dict, Optional

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
        rest_prefix: Optional[str] = None,
    ):
        self.auth = GklmAuth(
            host=ip,
            port=port,
            user=user,
            password=password,
            verify=verify,
            rest_prefix=rest_prefix,
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


def build_gklm_client(gklm_params: Dict[str, Any], verify: bool = False) -> GklmClient:
    """
    Construct GklmClient from ``load_gklm_config()`` output.

    Optional key ``gklm_rest_prefix`` selects the REST root, e.g. ``/GKLM/rest/v1``
    for IBM Guardium Key Lifecycle Manager 5.x; defaults to ``/SKLM/rest/v1``.
    """
    kw: Dict[str, Any] = {
        "ip": gklm_params["gklm_ip"],
        "user": gklm_params["gklm_user"],
        "password": gklm_params["gklm_password"],
        "verify": verify,
    }
    rp = gklm_params.get("gklm_rest_prefix")
    if rp:
        kw["rest_prefix"] = str(rp).strip()
    return GklmClient(**kw)
