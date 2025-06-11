import json

from rest.common.config.config import Config
from utility.log import Log

log = Log(__name__)


class RGW:
    def __init__(self, rest):
        self._config = Config()
        self._rest = rest
        self._endpoints = self._config.get_config()["endpoints"]["rgw"]

    def create_user(self, **kwargs):
        """
        POST /api/rgw/user
        Required payload:
        {
            "uid": "string",
            "display_name": "string"
        }
        """
        url = self._endpoints["CREATE_USER"]
        data = {
            "uid": kwargs.get("uid"),
            "display_name": kwargs.get("display_name") or kwargs.get("uid"),
        }
        resp = self._rest.post(relative_url=url, data=json.dumps(data))
        log.info(f"create_user response: {resp}")
        return resp

    def list_user(self):
        """
        GET /api/rgw/user
        Returns list of user ids (strings)
        """
        url = self._endpoints["LIST_USER"]
        resp = self._rest.get(relative_url=url)
        log.info(f"list_user response: {resp}")
        if isinstance(resp, dict) and "users" in resp:
            # assuming response contains key 'users' with list of user dicts with 'uid'
            return [user.get("uid") for user in resp.get("users", [])]
        if isinstance(resp, list):
            # fallback if it returns list directly
            return resp
        return []

    def create_bucket(self, **kwargs):
        """
        POST /api/rgw/bucket
        Required payload:
        {
            "bucket": "string",
            "uid": "string"
        }
        """
        url = self._endpoints["CREATE_BUCKET"]
        data = {"bucket": kwargs.get("bucket"), "uid": kwargs.get("uid")}
        resp = self._rest.post(relative_url=url, data=json.dumps(data))
        log.info(f"create_bucket response: {resp}")
        return resp

    def list_bucket(self):
        """
        GET /api/rgw/bucket
        Returns list of bucket names (strings)
        """
        url = self._endpoints["LIST_BUCKET"]
        resp = self._rest.get(relative_url=url)
        log.info(f"list_bucket response: {resp}")
        if isinstance(resp, dict) and "buckets" in resp:
            # assuming response has key 'buckets' with list of bucket dicts with 'name'
            return [b.get("name") for b in resp.get("buckets", [])]
        if isinstance(resp, list):
            return resp
        return []
