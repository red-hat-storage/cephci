import logging
import os
import re

import requests
import urllib3

from utility.utils import generate_self_signed_certificate

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from typing import Any, Dict, List, Optional


class GklmClient:
    """GKLM REST client for comprehensive cert and key management."""

    def __init__(
        self,
        host: str,
        port: int = 9443,
        user: str = None,
        password: str = None,
        verify: bool = False,
    ):
        self.base_url = f"https://{host}:{port}/SKLM/rest/v1"
        self.user, self.password, self.verify = user, password, verify
        self.authnId: Optional[str] = self.login()

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
            "Authorization": f"SKLMAuth userAuthId={self.login()}",
        }

    def get_certificates(self, subject: dict, create_files=False) -> tuple:
        """
        Generate a self-signed certificate for the given subject.
        subject = {
        "common_name": "ceph-pri-hsm-gklm-4zf24q-node6",
        "ip_address": "10.0.210.86" }
        key, cert, ca = generate_self_signed_certificate(subject=subject)
        """
        key, cert, ca = generate_self_signed_certificate(subject=subject)
        if create_files:
            with open(f"{subject['common_name']}.key", "w") as f:
                f.write(key)
            # verify if cert file exists

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

    def get_client_info(self, client_name) -> Dict[str, Any]:
        resp = requests.get(
            f"{self.base_url}/objects?clientName={client_name}",
            headers=self._headers(),
            verify=self.verify,
        )
        resp.raise_for_status()
        return resp.json()

    def create_client(
        self, client_name: str, application_usage: str = "Generic"
    ) -> Dict[str, Any]:
        """
        Create a new client in GKLM using a valid applicationUsage.

        Valid applicationUsage values:
          Oracle, MongoDB, VMware, FileNet, NetApp, Db2, Generic

        POST /clients
        """
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
            headers=self._headers(),
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
        """
        Retrieve all registered clients from GKLM via REST.

        HTTP Method: GET
        Endpoint: /clients
        URL: https://<host>:<port>/SKLM/rest/v1/clients

        Reference:
          - List All Clients REST Service: GET /clients :contentReference[oaicite:1]{index=1}

        Returns:
          A list of client dictionaries with basic client details.
        """
        resp = requests.get(
            f"{self.base_url}/clients", headers=self._headers(), verify=self.verify
        )

        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            # Capture and explain any GKLM-specific error messaging
            try:
                err_detail = resp.json().get("message", resp.text)
            except ValueError:
                err_detail = resp.text
            raise RuntimeError(
                f"Failed to list clients ({resp.status_code}): {err_detail}"
            ) from e

        # The API returns JSON with a top-level "client" list
        data = resp.json()
        return data.get("client", [])

    def delete_client(self, client_name: str) -> Optional[Dict[str, Any]]:
        """
        Delete a client from GKLM via REST.

        Uses DELETE /clients/{clientName}.
        Returns parsed JSON if present, otherwise None.
        Raises RuntimeError on HTTP error.
        """
        resp = requests.delete(
            f"{self.base_url}/clients/{client_name}",
            headers=self._headers(),
            verify=self.verify,
        )
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            try:
                err_msg = resp.json().get("message", resp.text)
            except ValueError:
                err_msg = resp.text
            raise RuntimeError(
                f"Delete client failed ({resp.status_code}): {err_msg}"
            ) from e

        if resp.text:
            try:
                return resp.json()
            except ValueError:
                return None
        return None

    def assign_client_certificate(
        self, client_name: str, alias: str, cert_pem: str, fmt: str = "PEM"
    ) -> dict:
        url = f"{self.base_url}/clients/{client_name}/assignCertificate"
        print(cert_pem)
        cert_pem = repr(cert_pem).replace("'", "")
        cert_pem = cert_pem.rstrip("\\n")
        print(cert_pem)
        # Use files param to send form-data parts
        files = {
            "certText": (None, cert_pem),
            "format": (None, fmt),
            "alias": (None, alias),
        }

        resp = requests.post(url, headers=self._headers(), files=files, verify=False)
        resp.raise_for_status()
        return resp.json()

    def create_symmetric_key_object(
        self,
        number_of_objects: Optional[int],
        client_name: Optional[str] = None,
        alias_prefix_name: str = "pre",
        cryptoUsageMask: Optional[str] = "Encrypt_Decrypt",
    ) -> Dict[str, Any]:
        """
        Creates symmetric keys and associates them with a client.

        Endpoint: POST /objects/symmetrickey
        Ref: Create/Register Symmetric Key REST Service (GKLM v5.x) :contentReference[oaicite:1]{index=1}

        Required/optional fields:
        - clientName: existing GKLM client
        - numberOfObjects: count of keys to generate
        - prefixName: alias prefix for generated keys
        - cryptoUsageMask: use-case mask like 'Decrypt', 'Encrypt,Decrypt'
        Gives enctag key for encryption/decryption operations. for byok
        """
        payload: Dict[str, Any] = {
            "clientName": client_name,
            "numberOfObjects": str(number_of_objects),
            "prefixName": alias_prefix_name,
            "cryptoUsageMask": cryptoUsageMask,
        }

        url = f"{self.base_url}/objects/symmetrickey"
        resp = requests.post(
            url, json=payload, headers=self._headers(), verify=self.verify
        )
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            try:
                err = resp.json().get("message", resp.text)
            except ValueError:
                err = resp.text or f"HTTP {resp.status_code}"
            raise RuntimeError(
                f"Create symmetric key failed ({resp.status_code}): {err}"
            ) from e

        return resp.json()

    def list_client_objects(
        self,
        client_name: str,
        object_type: Optional[str] = None,
        range_header: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve all cryptographic objects for a client (optionally filtered).

        HTTP Method: GET
        Endpoint: /objects
        Query params:
          - clientName (required)
          - objectType (optional): PUBLIC_KEY, PRIVATE_KEY, SYMMETRIC_KEY,
            CERTIFICATE, OPAQUE_OBJECT, SECRET_DATA

        Optional pagination via Range header: e.g., "items=0-49"
        Reference: List All Objects REST Service (GKLM v4.2/5.x) :contentReference[oaicite:1]{index=1}
        """
        params: Dict[str, str] = {"clientName": client_name}
        if object_type:
            params["objectType"] = object_type

        headers = self._headers().copy()
        if range_header:
            headers["Range"] = range_header

        resp = requests.get(
            f"{self.base_url}/objects",
            headers=headers,
            params=params,
            verify=self.verify,
        )
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            try:
                err = resp.json().get("error", resp.json().get("message", resp.text))
            except ValueError:
                err = resp.text
            raise RuntimeError(
                f"List objects failed ({resp.status_code}): {err}"
            ) from e

        data = resp.json()
        return data.get("managedObject", [])

    def delete_object(self, object_id: str) -> Optional[Dict[str, Any]]:
        """
        Delete a specific cryptographic object from GKLM.

        HTTP DELETE /objects/{objectId}

        :param object_id: The UUID of the object (e.g., SYMMETRIC_KEY-<uuid>)
        :return: JSON response {"messageId": "...", "error": "..."} if present; otherwise None
        :raises: RuntimeError with details on HTTP failure
        """
        url = f"{self.base_url}/objects/{object_id}"
        resp = requests.delete(url, headers=self._headers(), verify=self.verify)

        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            # Attempt to access JSON error payload
            try:
                err_json = resp.json()
                err = err_json.get("error") or err_json.get("message", resp.text)
            except ValueError:
                err = resp.text or f"HTTP {resp.status_code}"
            raise RuntimeError(
                f"Delete object failed ({resp.status_code}): {err}"
            ) from e

        # Return JSON if present; otherwise None
        if resp.text:
            try:
                return resp.json()
            except ValueError:
                return None
        return None

    def assign_users_to_generic_kmip_client(
        self, client_name: str, users: List[str]
    ) -> Dict[str, Any]:
        """
        Assign users to a generic KMIP client.
        HTTP Method: PUT
        Endpoint: /clients/{clientName}/assignUsers
        Payload: {"users": ["user1", "user2"]}
        """
        url = f"{self.base_url}/clients/{client_name}/assignUsers"
        payload = {"users": users}

        resp = requests.put(
            url, json=payload, headers=self._headers(), verify=self.verify
        )
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            try:
                err = resp.json().get("message", resp.text)
            except ValueError:
                err = resp.text or f"HTTP {resp.status_code}"
            raise RuntimeError(
                f"Assign users failed ({resp.status_code}): {err}"
            ) from e
        return resp.json()

    def list_certificates(
        self,
        uuid: Optional[str] = None,
        alias: Optional[str] = None,
        attributes: Optional[str] = None,
        usage: Optional[str] = None,
        offset: Optional[int] = None,
        count: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Return certificate info via GKLM REST.

        GET /certificates
        Ref: Certificate List REST Service – GKLM v5.x :contentReference[oaicite:1]{index=1}

        Parameters:
        - uuid, alias, attributes, usage: filter query fields
        - offset, count: pagination (default returns first 2000 certs)

        Returns:
        - List of certificate objects (each contains uuid, alias, usage, state, keystore info, etc.)
        """
        url = f"{self.base_url}/certificates"
        resp = requests.get(url, headers=self._headers(), verify=self.verify)
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            err = ""
            try:
                err = resp.json().get("message", resp.text)
            except ValueError:
                err = resp.text
            raise RuntimeError(
                f"List certificates failed ({resp.status_code}): {err}"
            ) from e

        return resp.json()

    def delete_certificate(self, alias: str) -> Optional[Dict[str, Any]]:
        """
        Deletes a certificate from GKLM by alias.

        DELETE /certificates/{alias}
        Ref: Delete Certificate REST Service (GKLM v5.x) :contentReference[oaicite:1]{index=1}

        :param alias: Alias of the certificate to delete
        :return: JSON response {"code": "0", "status": "Succeeded"} if present, else None
        :raises: RuntimeError on HTTP failure with error info
        """
        url = f"{self.base_url}/certificates/{alias}"
        resp = requests.delete(url, headers=self._headers(), verify=self.verify)

        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            try:
                err = resp.json().get("message", resp.text)
            except ValueError:
                err = resp.text or f"HTTP {resp.status_code}"
            raise RuntimeError(
                f"Delete certificate failed ({resp.status_code}): {err}"
            ) from e

        if resp.text:
            try:
                return resp.json()  # expected {"code": "0", "status": "Succeeded"}
            except ValueError:
                return None
        return None

    def export_certificate(
        self,
        uuid: str,
        file_name: str,
    ) -> Dict[str, Any]:
        """
        Export a certificate from GKLM by UUID.

        Endpoint: PUT /certificates/export

        :param uuid: Certificate UUID (e.g. 'CERTIFICATE-78d68704-...').
        :param file_name: Path on GKLM server to write the exported file.
        :return: JSON response, e.g. {"code":"0","status":"Succeeded"}.
        :raises RuntimeError: on HTTP error with detailed message.
        """
        url = f"{self.base_url}/certificates/export"
        payload = {"uuid": uuid, "fileName": file_name}

        resp = requests.put(
            url, json=payload, headers=self._headers(), verify=self.verify
        )
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            try:
                msg = resp.json().get("message", resp.text)
            except ValueError:
                msg = resp.text or f"HTTP {resp.status_code}"
            raise RuntimeError(
                f"Export certificate failed ({resp.status_code}): {msg}"
            ) from e

        return resp.json()
