import os
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests
import urllib3

from utility.gklm_client.auth import GklmAuth
from utility.utils import generate_self_signed_certificate

# Suppress the InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _coerce_certificate_list(data: Any) -> List[Dict[str, Any]]:
    """Normalize GET /certificates JSON across SKLM / GKLM 5.x."""
    if data is None:
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ("certificate", "certificates", "Certificate", "cert", "items"):
            v = data.get(key)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
    return []


def _extract_system_certificate_entries(data: Any) -> List[Dict[str, Any]]:
    """Parse GET /system/certificates JSON (SKLM / GKLM 5.x shapes)."""
    if data is None:
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in (
            "certificate",
            "certificates",
            "systemCertificate",
            "systemCertificates",
            "Certificate",
            "items",
        ):
            v = data.get(key)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
    return []


def _cert_entry_alias(entry: Dict[str, Any]) -> Optional[str]:
    return (
        entry.get("alias")
        or entry.get("Alias")
        or entry.get("certAlias")
        or entry.get("name")
    )


class GklmCertificate:
    def __init__(self, auth: GklmAuth):
        self.auth = auth
        self.base_url = auth.base_url
        self.verify = auth.verify

    def get_system_certificate(self, cert_name: str) -> str:
        """
        Export CA certificate from GKLM server.

        Args:
            cert_name: Name of the certificate to export
                      (e.g., 'ceph-nfs-gklm-server-e7y1nk-node1-installer')

        Returns:
            Certificate content as string (PEM format)

        Raises:
            RuntimeError: If the HTTP request fails with an error status
        """
        endpoint = "/system/certificates/export/{}".format(
            quote(str(cert_name), safe="")
        )
        url = "{}{}".format(self.base_url, endpoint)

        headers = self.auth._headers()
        headers["Accept"] = "application/octet-stream"

        try:
            resp = requests.get(url, headers=headers, verify=self.verify)

            if resp.status_code == 200:
                # Convert bytes to string and normalize line endings
                cert_content = resp.content.decode("utf-8")
                # Remove carriage returns and normalize newlines
                cert_content = cert_content.replace("\r\n", "\n").replace("\r", "")
                return cert_content

            else:
                content_type = resp.headers.get("Content-Type", "")
                if "application/json" in content_type:
                    err = resp.json().get("message", resp.text)
                else:
                    err = resp.text or "HTTP {}".format(resp.status_code)

                raise RuntimeError(
                    "Export CA certificate '{}' failed ({}): {}".format(
                        cert_name, resp.status_code, err
                    )
                )

        except requests.exceptions.RequestException as e:
            raise RuntimeError(
                "An error occurred while exporting CA certificate '{}': {}".format(
                    cert_name, str(e)
                )
            )

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

    def list_system_certificates(self) -> List[Dict[str, Any]]:
        """
        List **system** TLS/KMIP certificates (GET /system/certificates).

        GKLM 5.x does not include these in GET /certificates; use this for BYOK CA checks.
        """
        url = "{}/system/certificates".format(self.base_url)
        headers = self.auth._headers()
        headers["Accept"] = "application/json"
        resp = requests.get(url, headers=headers, verify=self.verify)
        if resp.status_code != 200:
            content_type = resp.headers.get("Content-Type", "")
            if "application/json" in content_type:
                err = resp.json().get("message", resp.text)
            else:
                err = resp.text or f"HTTP {resp.status_code}"
            raise RuntimeError(
                "List system certificates failed ({}): {}".format(resp.status_code, err)
            )
        return _extract_system_certificate_entries(resp.json())

    def system_certificate_alias_exists(self, alias: str) -> bool:
        """Return True if a system certificate with this alias is registered."""
        want = str(alias)
        for entry in self.list_system_certificates():
            a = _cert_entry_alias(entry)
            if a and str(a) == want:
                return True
        return False

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

        return _coerce_certificate_list(resp.json())

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

    def create_system_certificate(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a system certificate on the GKLM server (POST /system/certificates).

        Args:
            payload: Dictionary matching the GKLM API for creating a certificate,
               e.g. {
                    "type": "Self-signed",
                    "alias": "my_system_cert_alias",
                    "cn": "gklm.example.com",
                    "ou": "Security",
                    "o": "IBM",
                    "locality": "Armonk",
                    "state": "KA",
                    "country": "IN",
                    "validity": "3650",
                    "algorithm": "RSA",
                    "usageSubtype": "KEYSERVING_TLS"
                    }

        Returns:
            Parsed JSON response from the GKLM server on success.

        Raises:
            RuntimeError: On HTTP error or request failure.
        """
        url = "{}/system/certificates".format(self.base_url)

        headers = self.auth._headers()
        headers["Accept"] = "application/json"
        headers["Accept-Language"] = "en"
        headers["Content-Type"] = "application/json"

        resp = requests.post(url, json=payload, headers=headers, verify=self.verify)
        if resp.status_code in (200, 201):
            try:
                return resp.json()
            except ValueError:
                return {"status": resp.status_code, "text": resp.text}
        content_type = resp.headers.get("Content-Type", "")
        if "application/json" in content_type and resp.text:
            try:
                err = resp.json().get("message", resp.text)
            except ValueError:
                err = resp.text
        else:
            err = resp.text or f"HTTP {resp.status_code}"
        raise RuntimeError(
            "Create system certificate failed ({}): {}".format(resp.status_code, err)
        )

    def update_system_certificate(
        self, alias: str, add_usage_subtype: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update system certificate usage subtype (PUT /system/certificates).

        Args:
            alias: Certificate alias to update
            add_usage_subtype: Usage subtype to add to the certificate
                             (e.g., 'KEYSERVING_TLS')

        Returns:
            Parsed JSON response from the GKLM server on success.

        Raises:
            RuntimeError: If the HTTP request fails with an error status
        """
        url = "{}/system/certificates".format(self.base_url)

        headers = self.auth._headers()
        headers["Accept"] = "application/json"
        headers["Accept-Language"] = "en"
        headers["Content-Type"] = "application/json"

        payload = {"alias": alias}
        if add_usage_subtype:
            payload["addUsageSubtype"] = add_usage_subtype
        resp = requests.put(url, json=payload, headers=headers, verify=self.verify)
        if resp.status_code in (200, 201):
            try:
                return resp.json()
            except ValueError:
                return {"status": resp.status_code, "text": resp.text}
        content_type = resp.headers.get("Content-Type", "")
        if "application/json" in content_type and resp.text:
            try:
                err = resp.json().get("message", resp.text)
            except ValueError:
                err = resp.text
        else:
            err = resp.text or f"HTTP {resp.status_code}"
        raise RuntimeError(
            "Update system certificate failed ({}): {}".format(resp.status_code, err)
        )

    def delete_system_certificate(self, alias: str) -> Optional[Dict[str, Any]]:
        """
        Delete a system certificate by alias.
        Args:
            alias: System certificate alias to delete
                  (e.g., 'my_system_cert_alias')
        Returns:
            Response JSON if successful, None otherwise
        Raises:
            RuntimeError: If the HTTP request fails with an error status
        """
        url = "{}/system/certificates/{}".format(
            self.base_url, quote(str(alias), safe="")
        )

        headers = self.auth._headers()
        headers["Accept"] = "application/json"
        headers["Accept-Language"] = "en"

        resp = requests.delete(url, headers=headers, verify=self.verify)
        if resp.status_code in (200, 201):
            try:
                return resp.json()
            except ValueError:
                return {"status": resp.status_code, "text": resp.text}
        content_type = resp.headers.get("Content-Type", "")
        if "application/json" in content_type and resp.text:
            try:
                err = resp.json().get("message", resp.text)
            except ValueError:
                err = resp.text
        else:
            err = resp.text or f"HTTP {resp.status_code}"
        raise RuntimeError(
            "Delete system certificate failed ({}): {}".format(resp.status_code, err)
        )
