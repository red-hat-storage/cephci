from typing import Any, Dict

import requests
import urllib3

from utility.gklm_client.auth import GklmAuth

# Suppress the InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class GklmServer:
    """GKLM Server Management REST API client."""

    def __init__(self, auth: GklmAuth):
        """
        Initialize the GKLM Server client.

        Args:
            auth: GklmAuth object containing authentication credentials
        """
        self.auth = auth
        self.base_url = auth.base_url
        self.verify = auth.verify

    def get_license(self) -> Dict[str, Any]:
        """
        Get license information from GKLM server.

        Returns:
            License information as dictionary

        Raises:
            RuntimeError: If the HTTP request fails with an error status
        """
        url = "{}/license".format(self.base_url)
        headers = self.auth._headers()
        headers["Accept"] = "application/json"

        try:
            resp = requests.get(url, headers=headers, verify=self.verify)

            if resp.status_code == 200:
                return resp.json()
            else:
                content_type = resp.headers.get("Content-Type", "")
                if "application/json" in content_type:
                    err = resp.json().get("message", resp.text)
                else:
                    err = resp.text or "HTTP {}".format(resp.status_code)

                raise RuntimeError(
                    "Get license failed ({}): {}".format(resp.status_code, err)
                )

        except requests.exceptions.RequestException as e:
            raise RuntimeError(
                "An error occurred while getting license: {}".format(str(e))
            )

    def get_system_info(self) -> Dict[str, Any]:
        """
        Get system information from GKLM server.

        Returns:
            System information as dictionary

        Raises:
            RuntimeError: If the HTTP request fails with an error status
        """
        url = "{}/systemDetails".format(self.base_url)
        headers = self.auth._headers()
        headers["Accept"] = "application/json"

        try:
            resp = requests.get(url, headers=headers, verify=self.verify)

            if resp.status_code == 200:
                return resp.json()
            else:
                content_type = resp.headers.get("Content-Type", "")
                if "application/json" in content_type:
                    err = resp.json().get("message", resp.text)
                else:
                    err = resp.text or "HTTP {}".format(resp.status_code)

                raise RuntimeError(
                    "Get system info failed ({}): {}".format(resp.status_code, err)
                )

        except requests.exceptions.RequestException as e:
            raise RuntimeError(
                "An error occurred while getting system info: {}".format(str(e))
            )

    def get_system_certificates(self) -> Dict[str, Any]:
        """
        Get system certificates from GKLM server.

        Returns:
            System certificates information as dictionary

        Raises:
            RuntimeError: If the HTTP request fails with an error status
        """
        url = "{}/system/certificates".format(self.base_url)
        headers = self.auth._headers()
        headers["Accept"] = "application/json"

        try:
            resp = requests.get(url, headers=headers, verify=self.verify)

            if resp.status_code == 200:
                return resp.json()
            else:
                content_type = resp.headers.get("Content-Type", "")
                if "application/json" in content_type:
                    err = resp.json().get("message", resp.text)
                else:
                    err = resp.text or "HTTP {}".format(resp.status_code)

                raise RuntimeError(
                    "Get system certificates failed ({}): {}".format(
                        resp.status_code, err
                    )
                )

        except requests.exceptions.RequestException as e:
            raise RuntimeError(
                "An error occurred while getting system certificates: {}".format(str(e))
            )

    def restart_server(self) -> Dict[str, Any]:
        """
        Restart GKLM server.

        Returns:
            Response from server restart request

        Raises:
            RuntimeError: If the HTTP request fails with an error status
        """
        url = "{}/ckms/servermanagement/restartServer".format(self.base_url)
        headers = self.auth._headers()
        headers["Accept"] = "application/json"
        headers["Content-Type"] = "application/json"

        try:
            resp = requests.post(url, headers=headers, verify=self.verify)

            if resp.status_code in (200, 201, 202):
                try:
                    return resp.json()
                except ValueError:
                    return {
                        "status": resp.status_code,
                        "message": "Server restart initiated",
                    }
            else:
                content_type = resp.headers.get("Content-Type", "")
                if "application/json" in content_type:
                    err = resp.json().get("message", resp.text)
                else:
                    err = resp.text or "HTTP {}".format(resp.status_code)

                raise RuntimeError(
                    "Restart server failed ({}): {}".format(resp.status_code, err)
                )

        except requests.exceptions.RequestException as e:
            raise RuntimeError(
                "An error occurred while restarting server: {}".format(str(e))
            )

    def health_check(self) -> bool:
        """
        Perform health check on GKLM server.

        Returns:
            True if server is healthy, False otherwise
        """
        url = "{}/health".format(self.base_url)
        headers = self.auth._headers()

        try:
            resp = requests.get(url, headers=headers, verify=self.verify)
            return resp.text
        except requests.exceptions.RequestException:
            return False
