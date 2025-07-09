import re
from typing import Any, Dict, List, Optional

import requests

from utility.gklm_client.auth import GklmAuth


class GklmClientManagement:
    def __init__(self, auth: GklmAuth):
        self.auth = auth
        self.base_url = auth.base_url
        self.verify = auth.verify

    def get_client_info(self, client_name) -> Dict[str, Any]:
        resp = requests.get(
            f"{self.base_url}/objects?clientName={client_name}",
            headers=self.auth._headers(),
            verify=self.verify,
        )
        resp.raise_for_status()
        return resp.json()

    def create_client(
        self, client_name: str, application_usage: str = "Generic"
    ) -> Dict[str, Any]:

        name = client_name.strip()
        if not re.match(r"^[A-Za-z0-9_]+$", name):
            raise ValueError("Client name must be alphanumeric or underscore only.")
        if set(name) == {"_"}:
            raise ValueError("Client name cannot be only underscores.")
        if len(name) > 256:
            raise ValueError("Client name must be ≤256 characters.")

        VALID_APP = {
            "Oracle",
            "MongoDB",
            "VMware",
            "FileNet",
            "NetApp",
            "Db2",
            "Generic",
        }
        if application_usage not in VALID_APP:
            raise ValueError(f"applicationUsage must be one of {sorted(VALID_APP)}")

        payload = {"clientName": name, "applicationUsage": application_usage}
        resp = requests.post(
            f"{self.base_url}/clients",
            json=payload,
            headers=self.auth._headers(),
            verify=self.verify,
        )
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            msg = resp.json().get("message", resp.text) if resp.text else resp.text
            raise RuntimeError(
                f"Create client failed ({resp.status_code}): {msg}"
            ) from e

        return resp.json()

    def list_clients(self) -> List[Dict[str, Any]]:
        resp = requests.get(
            f"{self.base_url}/clients", headers=self.auth._headers(), verify=self.verify
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("client", [])

        # Error path
        content_type = resp.headers.get("Content-Type", "")
        if "application/json" in content_type:
            err_detail = resp.json().get("message", resp.text)
        else:
            err_detail = resp.text or f"HTTP {resp.status_code}"

        raise RuntimeError(f"Failed to list clients ({resp.status_code}): {err_detail}")

    def delete_client(self, client_name: str) -> Optional[Dict[str, Any]]:

        resp = requests.delete(
            f"{self.base_url}/clients/{client_name}",
            headers=self.auth._headers(),
            verify=self.verify,
        )
        if resp.status_code == 200:
            if resp.text:
                try:
                    return resp.json()
                except ValueError:
                    return None
            return None

        # Error path
        content_type = resp.headers.get("Content-Type", "")
        if "application/json" in content_type:
            err_msg = resp.json().get("message", resp.text)
        else:
            err_msg = resp.text or f"HTTP {resp.status_code}"

        raise RuntimeError(f"Delete client failed ({resp.status_code}): {err_msg}")

    def assign_client_certificate(
        self, client_name: str, alias: str, cert_pem: str, fmt: str = "PEM"
    ) -> dict:
        url = f"{self.base_url}/clients/{client_name}/assignCertificate"
        cert_pem = repr(cert_pem).replace("'", "")
        cert_pem = cert_pem.rstrip("\\n")
        files = {
            "certText": (None, cert_pem),
            "format": (None, fmt),
            "alias": (None, alias),
        }

        resp = requests.post(
            url, headers=self.auth._headers(), files=files, verify=False
        )
        resp.raise_for_status()
        return resp.json()

    def assign_users_to_generic_kmip_client(
        self, client_name: str, users: List[str]
    ) -> Dict[str, Any]:
        url = f"{self.base_url}/clients/{client_name}/assignUsers"
        payload = {"users": users}

        resp = requests.put(
            url, json=payload, headers=self.auth._headers(), verify=self.verify
        )
        if resp.status_code == 200:
            return resp.json()

        content_type = resp.headers.get("Content-Type", "")
        if "application/json" in content_type:
            err_detail = resp.json().get("message", resp.text)
        else:
            err_detail = resp.text or f"HTTP {resp.status_code}"

        raise RuntimeError(f"Failed to list clients ({resp.status_code}): {err_detail}")
