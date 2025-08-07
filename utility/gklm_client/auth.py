from typing import Dict

import requests


class GklmAuth:
    def __init__(self, host, user, password, port=9443, verify=False):
        self.base_url = f"https://{host}:{port}/SKLM/rest/v1"
        self.user = user
        self.password = password
        self.verify = verify
        self.authnId = None

    def login(self) -> str:
        resp = requests.post(
            f"{self.base_url}/ckms/login",
            json={"userid": self.user, "password": self.password},
            verify=self.verify,
        )
        resp.raise_for_status()
        self.authnId = resp.json()["UserAuthId"]
        return self.authnId

    def logout(self) -> None:
        if self.authnId:
            requests.post(
                f"{self.base_url}/ckms/logout",
                headers=self._headers(),
                verify=self.verify,
            ).raise_for_status()
        self.authnId = None

    def _headers(self) -> Dict[str, str]:
        if not self.authnId:
            raise RuntimeError("Authenticate first with login()")
        return {
            "Accept": "application/json",
            "Accept-Language": "en",
            "Authorization": f"SKLMAuth userAuthId={self.authnId}",
        }
