import os
from typing import Any, Dict, List, Optional

import requests

from utility.gklm_client.auth import GklmAuth
from utility.utils import generate_self_signed_certificate


class GklmCertificate:
    def __init__(self, auth: GklmAuth):
        self.auth = auth
        self.base_url = auth.base_url
        self.verify = auth.verify

    def get_certificates(self, subject: dict, create_files=False) -> tuple:
        key, cert, ca = generate_self_signed_certificate(subject=subject)
        if create_files:
            with open(f"{subject['common_name']}.key", "w") as f:
                f.write(key)
            with open(f"{subject['common_name']}.crt", "w") as f:
                f.write(cert)
            if ca:
                with open(f"{subject['common_name']}.ca", "w") as f:
                    f.write(ca)
            abs_path = (
                os.path.abspath(f"{subject['common_name']}.key"),
                os.path.abspath(f"{subject['common_name']}.crt"),
                os.path.abspath(f"{subject['common_name']}.ca") if ca else None,
            )
            return abs_path
        return generate_self_signed_certificate(subject)

    def list_certificates(self) -> List[Dict[str, Any]]:

        url = f"{self.base_url}/certificates"
        resp = requests.get(url, headers=self.auth._headers(), verify=self.verify)
        if resp.status_code != 200:
            content_type = resp.headers.get("Content-Type", "")
            if "application/json" in content_type:
                err = resp.json().get("message", resp.text)
            else:
                err = resp.text or f"HTTP {resp.status_code}"
            raise RuntimeError(f"List certificates failed ({resp.status_code}): {err}")

        return resp.json()

    def delete_certificate(self, alias: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}/certificates/{alias}"
        resp = requests.delete(url, headers=self.auth._headers(), verify=self.verify)
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
            err = resp.json().get("message", resp.text)
        else:
            err = resp.text or f"HTTP {resp.status_code}"

        raise RuntimeError(f"Delete certificate failed ({resp.status_code}): {err}")

    def export_certificate(self, uuid: str, file_name: str) -> Dict[str, Any]:
        url = f"{self.base_url}/certificates/export"
        payload = {"uuid": uuid, "fileName": file_name}
        resp = requests.put(
            url, json=payload, headers=self.auth._headers(), verify=self.verify
        )
        if resp.status_code in (200, 201):
            return resp.json()

        content_type = resp.headers.get("Content-Type", "")
        if "application/json" in content_type:
            msg = resp.json().get("message", resp.text)
        else:
            msg = resp.text or f"HTTP {resp.status_code}"
        raise RuntimeError(f"Export certificate failed ({resp.status_code}): {msg}")
