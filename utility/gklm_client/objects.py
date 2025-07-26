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
        url = f"{self.base_url}/objects/{object_id}"
        resp = requests.delete(url, headers=self.auth._headers(), verify=self.verify)

        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            try:
                err_json = resp.json()
                err = err_json.get("error") or err_json.get("message", resp.text)
            except ValueError:
                err = resp.text or f"HTTP {resp.status_code}"
            raise RuntimeError(
                f"Delete object failed ({resp.status_code}): {err}"
            ) from e

        if resp.text:
            try:
                return resp.json()
            except ValueError:
                return None
        return None

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
