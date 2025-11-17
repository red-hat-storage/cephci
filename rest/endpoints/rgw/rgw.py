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

    def list_daemons(self):
        """
        GET /api/rgw/daemon
        Returns list of RGW daemons
        """
        url = self._endpoints["LIST_DAEMONS"]
        resp = self._rest.get(relative_url=url)
        log.info(f"list_daemons response: {resp}")
        return resp

    def get_daemon(self, **kwargs):
        """
        GET /api/rgw/daemon/{svc_id}
        Get full details of 1 RGW daemons service ID
        """
        data = {"svc_id": kwargs.get("svc_id")}
        url = self._endpoints["GET_DAEMON"].format(svc_id=data["svc_id"])
        resp = self._rest.get(relative_url=url)
        log.info(f"Get_daemon response: {resp}")
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

    def list_bucket_lifecycle(self, **kwargs):
        """
        GET /api/rgw/bucket/lifecycle
        Returns lifecycle configuration on the bucket
        """
        url = self._endpoints["LIST_BUCKET_LIFECYCLE"]
        data = {"bucket": kwargs.get("bucket")}
        resp = self._rest.get(relative_url=url, data=json.dumps(data))
        log.info(f"Get_bucket_lifecycle response: {resp}")
        return resp

    def list_bucket_encryptionConfig(self):
        """
        GET /api/rgw/bucket/getEncryptionConfig
        Returns encryption configuration on the bucket
        """
        url = self._endpoints["LIST_BUCKET_ENCRYPTCONF"]
        resp = self._rest.get(relative_url=url)
        log.info(f"Get_bucket_encryptionConfig response: {resp}")
        return resp

    def list_bucket_encryption(self, **kwargs):
        """
        GET /api/rgw/bucket/getEncryption
        Returns encryption on the bucket
        """
        url = self._endpoints["LIST_BUCKET_ENCRYPTION"]
        data = {"bucket": kwargs.get("bucket")}
        resp = self._rest.get(relative_url=url, data=json.dumps(data))
        log.info(f"Get_bucket_encryption response: {resp}")
        return resp

    def list_bucket_ratelimit_global(self):
        """
        GET /api/rgw/bucket/ratelimit
        Returns Global ratelimits on the bucket
        """
        url = self._endpoints["LIST_BUCKET_RATELIMIT_GLOBAL"]
        resp = self._rest.get(relative_url=url)
        log.info(f"Get_bucket_encryption response: {resp}")
        return resp

    def list_bucket_ratelimit(self, **kwargs):
        """
        GET /api/rgw/bucket/{uid}/ratelimit
        Returns ratelimits on the bucket
        """
        data = {"bucket": kwargs.get("bucket")}
        url = self._endpoints["LIST_BUCKET_RATELIMIT"].format(uid=data["bucket"])
        resp = self._rest.get(relative_url=url)
        log.info(f"Get_bucket_encryption response: {resp}")
        return resp

    def set_bucket_ratelimit(self, **kwargs):
        """
        PUT /api/rgw/bucket/{uid}/ratelimit
        Returns ratelimits on the bucket
        """
        data = {"bucket": kwargs.get("bucket"), "ratelimit": kwargs.get("ratelimit")}
        url = self._endpoints["UPDATE_BUCKET_RATELIMIT"].format(uid=data["bucket"])
        ratelimit = data["ratelimit"]
        resp = self._rest.put(relative_url=url, data=ratelimit)
        log.info(f"Set_bucket_ratelimit response: {resp}")
        return resp

    def update_bucket_lifecycle(self, **kwargs):
        """
        PUT /api/rgw/bucket/lifecycle
        Required payload:
        {
            "bucket": "string",
            "lifecycle": "string"
        }
        """
        url = self._endpoints["UPDATE_BUCKET_LIFECYCLE"]
        data = {"bucket": kwargs.get("bucket"), "lifecycle": kwargs.get("lifecycle")}
        resp = self._rest.put(relative_url=url, data=json.dumps(data))
        log.info(f"Put_bucket_lifecycle response: {resp}")
        return resp

    def get_bucket(self, **kwargs):
        """
        GET /api/rgw/bucket/{bucket}
        Get details of the specified bucket
        """
        data = {"bucket": kwargs.get("bucket")}
        url = self._endpoints["GET_BUCKET"].format(bucket=data["bucket"])
        resp = self._rest.get(relative_url=url)
        log.info(f"get_bucket response: {resp}")
        return resp

    def delete_bucket(self, **kwargs):
        """
        DELETE /api/rgw/bucket/{bucket}
        Deletes the specified bucket
        """
        data = {"bucket": kwargs.get("bucket")}
        url = self._endpoints["DELETE_BUCKET"].format(bucket=data["bucket"])
        resp = self._rest.delete(relative_url=url)
        log.info(f"delete_bucket response: {resp}")
        return resp
