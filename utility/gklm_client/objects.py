from typing import Any, Dict, List, Optional

import requests

from utility.gklm_client.auth import GklmAuth


class GklmObjects:
    def __init__(self, auth: GklmAuth):
        self.auth = auth
        self.base_url = auth.base_url
        self.verify = auth.verify

    def list_client_objects(
        self,
        client_name: str,
        object_type: Optional[str] = None,
        range_header: Optional[str] = None,
    ) -> List[Dict[str, Any]]:

        params: Dict[str, str] = {"clientName": client_name}
        if object_type:
            params["objectType"] = object_type

        headers = self.auth._headers().copy()
        if range_header:
            headers["Range"] = range_header

        resp = requests.get(
            f"{self.base_url}/objects",
            headers=headers,
            params=params,
            verify=self.verify,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("managedObject", [])

        # Handle error response
        content_type = resp.headers.get("Content-Type", "")
        if "application/json" in content_type:
            error_json = resp.json()
            err = error_json.get("error") or error_json.get("message", resp.text)
        else:
            err = resp.text or f"HTTP {resp.status_code}"

        raise RuntimeError(f"List objects failed ({resp.status_code}): {err}")

    def delete_object(self, object_id: str) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}/objects/{object_id}"
        resp = requests.delete(url, headers=self.auth._headers(), verify=self.verify)

        # Success status codes for DELETE operations
        if resp.status_code in [200, 201, 202, 204]:
            if resp.text:
                try:
                    return resp.json()
                except ValueError:
                    # DELETE operations often return empty responses
                    return None
            return None

        # Handle 404 as success for delete operations (already deleted)
        if resp.status_code == 404:
            return None  # Object already deleted or doesn't exist

        # Error path for other status codes
        content_type = resp.headers.get("Content-Type", "")
        if "application/json" in content_type and resp.text:
            try:
                body = resp.json()
                err = body.get("error") or body.get("message", resp.text)
            except (ValueError, requests.exceptions.JSONDecodeError):
                # If JSON parsing fails, use the raw text
                err = resp.text or f"HTTP {resp.status_code}"
        else:
            err = resp.text or f"HTTP {resp.status_code}"

        raise RuntimeError(f"Delete object failed ({resp.status_code}): {err}")

    def create_symmetric_key_object(
        self,
        number_of_objects: Optional[int],
        client_name: Optional[str] = None,
        alias_prefix_name: str = "pre",
        cryptoUsageMask: Optional[str] = "Encrypt_Decrypt",
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "clientName": client_name,
            "numberOfObjects": str(number_of_objects),
            "prefixName": alias_prefix_name,
            "cryptoUsageMask": cryptoUsageMask,
        }

        url = f"{self.base_url}/objects/symmetrickey"
        resp = requests.post(
            url, json=payload, headers=self.auth._headers(), verify=self.verify
        )
        if resp.status_code in (200, 201):
            return resp.json()

        content_type = resp.headers.get("Content-Type", "")
        if "application/json" in content_type:
            err = resp.json().get("message", resp.text)
        else:
            err = resp.text or f"HTTP {resp.status_code}"

        raise RuntimeError(f"Create symmetric key failed ({resp.status_code}): {err}")
