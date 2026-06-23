from typing import Dict, Optional

import requests

# IBM Security Key Lifecycle Manager (SKLM) / legacy path
DEFAULT_REST_PREFIX = "/SKLM/rest/v1"
# IBM Guardium Key Lifecycle Manager (GKLM) 5.x REST root (see swagger_en.yaml)
GKLM5_REST_PREFIX = "/GKLM/rest/v1"


def normalize_rest_prefix(rest_prefix: Optional[str]) -> str:
    """Return a path like /GKLM/rest/v1 with leading slash, no trailing slash."""
    if not rest_prefix:
        return DEFAULT_REST_PREFIX
    p = str(rest_prefix).strip()
    if not p.startswith("/"):
        p = "/" + p
    p = p.rstrip("/")
    return p if p else DEFAULT_REST_PREFIX


def _extract_user_auth_id(body: dict) -> str:
    """Parse login JSON for session id (field name varies slightly by release)."""
    if not isinstance(body, dict):
        raise RuntimeError(f"Login response is not a JSON object: {body!r}")
    for key in ("UserAuthId", "userAuthId", "userauthid"):
        if key in body and body[key] is not None:
            return str(body[key])
    for k, v in body.items():
        if k and k.lower() == "userauthid" and v is not None:
            return str(v)
    raise RuntimeError(f"Login response missing UserAuthId: {body!r}")


class GklmAuth:
    def __init__(
        self,
        host,
        user,
        password,
        port=9443,
        verify=False,
        rest_prefix: Optional[str] = None,
        request_timeout: int = 30,
    ):
        prefix = normalize_rest_prefix(rest_prefix)
        self.base_url = f"https://{host}:{port}{prefix}"
        self.user = user
        self.password = password
        self.verify = verify
        self.request_timeout = request_timeout
        self.authnId = None

    def login(self) -> str:
        resp = requests.post(
            f"{self.base_url}/ckms/login",
            json={"userid": self.user, "password": self.password},
            verify=self.verify,
            timeout=self.request_timeout,
        )
        resp.raise_for_status()
        self.authnId = _extract_user_auth_id(resp.json())
        return self.authnId

    def logout(self) -> None:
        if not self.authnId:
            return
        headers = self._headers()
        url = f"{self.base_url}/ckms/logout"
        # GKLM 5.x OpenAPI: DELETE /ckms/logout; older SKLM often used POST.
        resp = requests.delete(
            url, headers=headers, verify=self.verify, timeout=self.request_timeout
        )
        if resp.status_code == 405:
            resp = requests.post(
                url, headers=headers, verify=self.verify, timeout=self.request_timeout
            )
        resp.raise_for_status()
        self.authnId = None

    def _headers(self) -> Dict[str, str]:
        if not self.authnId:
            raise RuntimeError("Authenticate first with login()")
        return {
            "Accept": "application/json",
            "Accept-Language": "en",
            "Authorization": f"SKLMAuth userAuthId={self.authnId}",
        }
