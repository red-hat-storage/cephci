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

    def delete_user(self, **kwargs):
        """
        DELETE /api/rgw/user/{uid}
        """
        data = {"uid": kwargs.get("uid")}
        url = self._endpoints["DELETE_USER"].format(uid=data["uid"])
        resp = self._rest.delete(relative_url=url)
        log.info(f"delete_user response: {resp}")
        return resp

    def create_subuser(self, **kwargs):
        """
        POST /api/rgw/user/{uid}/subuser
        Required payload:
        {
            "uid": "string",
            "subuser": "string",
            "access": "string",
            "key_type": "string",
            "generate_secret": "string"
        }
        """
        url = self._endpoints["CREATE_SUBUSER"].format(uid=kwargs.get("uid"))
        data = {
            "subuser": kwargs.get("subuser"),
            "access": "full",
            "key_type": "s3",
            "generate_secret": "true",
        }
        resp = self._rest.post(relative_url=url, data=json.dumps(data))
        log.info(f"create_subuser response: {resp}")
        return resp

    def delete_subuser(self, **kwargs):
        """
        DELETE /api/rgw/user/{uid}/subuser/{subuser}
        """
        data = {"uid": kwargs.get("uid"), "subuser": kwargs.get("subuser")}
        url = self._endpoints["DELETE_SUBUSER"].format(
            uid=data["uid"], subuser=data["subuser"]
        )
        resp = self._rest.delete(relative_url=url)
        log.info(f"delete_subuser response: {resp}")
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

    def list_user_emails(self):
        """
        GET /api/rgw/user/get_emails
        Returns list of user emails(strings)
        """
        url = self._endpoints["LIST_USER_EMAILS"]
        resp = self._rest.get(relative_url=url)
        log.info(f"list_user_emails response: {resp}")
        return resp

    def get_user_stats(self, **kwargs):
        """
        GET /api/rgw/user/{uid}
        Returns user stats
        """
        data = {"uid": kwargs.get("uid")}
        url = self._endpoints["GET_USER_STATS"].format(uid=data["uid"])
        resp = self._rest.get(relative_url=url)
        log.info(f"Get_user_stats response: {resp}")
        return resp

    def edit_user_details(self, **kwargs):
        """
        PUT /api/rgw/user/{uid}
        Returns user stats
        """
        data = {"uid": kwargs.get("uid"), "user_details": kwargs.get("user_details")}
        url = self._endpoints["EDIT_USER_DETAILS"].format(uid=data["uid"])
        resp = self._rest.put(relative_url=url, data=data["user_details"])
        log.info(f"Get_user_stats response: {resp}")
        return resp

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
        data = {"bucket": kwargs.get("bucket")}
        url = self._endpoints["LIST_BUCKET_ENCRYPTION"].format(bucket=data["bucket"])
        resp = self._rest.get(relative_url=url, data=data)
        log.info(f"Get_bucket_encryption response: {resp}")
        return resp

    def list_roles(self):
        """
        GET /api/rgw/roles
        Returns List of RGW roles
        """
        url = self._endpoints["LIST_ROLES"]
        resp = self._rest.get(relative_url=url)
        log.info(f"List Roles response: {resp}")
        return resp

    def create_role(self, **kwargs):
        """
        POST /api/rgw/roles
        Required payload:
        {
            "role_name": "",
            "role_path": "",
            "role_assume_policy_doc": ""
        }
        """
        url = self._endpoints["CREATE_ROLE"]
        role_name = "S3role1"
        data = {
            "role_name": role_name,
            "role_path": "/",
            "role_assume_policy_doc": kwargs.get("role_policy"),
        }
        resp = self._rest.post(relative_url=url, data=json.dumps(data))
        log.info(f"Create Role response: {resp}")
        return resp

    def delete_role(self, **kwargs):
        """
        DELETE /api/rgw/roles/{role_name}
        """
        data = {"role_name": kwargs.get("role_name")}
        url = self._endpoints["DELETE_ROLE"].format(role_name=data["role_name"])
        resp = self._rest.delete(relative_url=url, data=json.dumps(data))
        log.info(f"Delete Role response: {resp}")
        return resp

    def edit_role(self, **kwargs):
        """
        PUT /api/rgw/roles
        Edit the max_session_duration in hours 1-12 (3600-43200 sec)
        """
        data = {"role_name": kwargs.get("role_name"), "max_session_duration": 2}
        url = self._endpoints["EDIT_ROLE"]
        resp = self._rest.put(relative_url=url, data=json.dumps(data))
        log.info(f"Edit Role response: {resp}")
        return resp

    def create_user_caps(self, **kwargs):
        """
        POST /api/rgw/user/{uid}/capability
        Required payload:
        {
            "type": "string",
            "perm": "string",
            "daemon_name": null
        }
        """
        data = {"type": "ratelimit", "perm": "*"}
        url = self._endpoints["CREATE_USER_CAPS"].format(uid=kwargs.get("uid"))
        resp = self._rest.post(relative_url=url, data=json.dumps(data))
        log.info(f"Create User caps response: {resp}")
        return resp

    def delete_user_caps(self, **kwargs):
        """
        DELETE /api/rgw/user/{uid}/capability
        Required payload:
        {
            "uid": "string,
            "type": "string",
            "perm": "string",
            "daemon_name": null
        }
        """
        url = self._endpoints["DELETE_USER_CAPS"].format(
            uid=kwargs.get("uid"), type="ratelimit", perm="*"
        )
        resp = self._rest.delete(relative_url=url)
        log.info(f"Delete User caps response: {resp}")
        return resp

    def create_user_key(self, **kwargs):
        """
        POST /api/rgw/user/{uid}/key
        Required payload:
        {
            "key_type": "string",
            "generate_key": "string",
            "access_key": "string"
        }
        """
        data = {"key_type": "s3", "generate_key": "true", "access_key": "abc123"}
        url = self._endpoints["CREATE_USER_KEY"].format(uid=kwargs.get("uid"))
        resp = self._rest.post(relative_url=url, data=json.dumps(data))
        log.info(f"Create User key response: {resp}")
        return resp

    def delete_user_key(self, **kwargs):
        """
        DELETE /api/rgw/user/{uid}/key
        Required payload:
        {
            "key_type": "string",
            "access_key": "string",
        }
        """
        url = self._endpoints["DELETE_USER_KEY"].format(
            uid=kwargs.get("uid"), key_type="s3", access_key="abc123"
        )
        resp = self._rest.delete(relative_url=url)
        log.info(f"Delete User key response: {resp}")
        return resp

    def get_user_quota(self, **kwargs):
        """
        GET /api/rgw/user/{uid}/quota
        """
        url = self._endpoints["GET_USER_QUOTA"].format(uid=kwargs.get("uid"))
        resp = self._rest.get(relative_url=url)
        log.info(f"Get User quota response: {resp}")
        return resp

    def edit_user_quota(self, **kwargs):
        """
        PUT /api/rgw/user/{uid}/quota
        """
        url = self._endpoints["EDIT_USER_QUOTA"].format(uid=kwargs.get("uid"))
        data = {
            "quota_type": "user",
            "enabled": "true",
            "max_size_kb": "0",
            "max_objects": "100",
        }
        resp = self._rest.put(relative_url=url, data=json.dumps(data))
        log.info(f"Put User quota response: {resp}")
        return resp

    def list_bucket_ratelimit_global(self):
        """
        GET /api/rgw/bucket/ratelimit
        Returns Global ratelimits on the bucket
        """
        url = self._endpoints["LIST_BUCKET_RATELIMIT_GLOBAL"]
        resp = self._rest.get(relative_url=url)
        log.info(f"Global bucket ratelimits response: {resp}")
        return resp

    def list_user_ratelimit_global(self):
        """
        GET /api/rgw/user/ratelimit
        Returns Global ratelimits on the bucket
        """
        url = self._endpoints["LIST_USER_RATELIMIT_GLOBAL"]
        resp = self._rest.get(relative_url=url)
        log.info(f"Global user ratelimits response: {resp}")
        return resp

    def list_user_ratelimit(self, **kwargs):
        """
        GET /api/rgw/user/{uid}/ratelimit
        Returns local ratelimits on the user
        """
        data = {"uid": kwargs.get("uid")}
        url = self._endpoints["LIST_USER_RATELIMIT"].format(uid=data["uid"])
        resp = self._rest.get(relative_url=url)
        log.info(f"List_user_ratelimit response: {resp}")
        return resp

    def list_bucket_ratelimit(self, **kwargs):
        """
        GET /api/rgw/bucket/{uid}/ratelimit
        Returns ratelimits on the bucket
        """
        data = {"bucket": kwargs.get("bucket")}
        url = self._endpoints["LIST_BUCKET_RATELIMIT"].format(uid=data["bucket"])
        resp = self._rest.get(relative_url=url)
        log.info(f"List_bucket_ratelimit response: {resp}")
        return resp

    def set_user_ratelimit(self, **kwargs):
        """
        PUT /api/rgw/user/{uid}/ratelimit
        Sets ratelimits on the user
        """
        data = {"uid": kwargs.get("uid"), "ratelimit": kwargs.get("user_ratelimit")}
        url = self._endpoints["UPDATE_USER_RATELIMIT"].format(uid=data["uid"])
        ratelimit = data["ratelimit"]
        resp = self._rest.put(relative_url=url, data=ratelimit)
        log.info(f"Set_user_ratelimit response: {resp}")
        return resp

    def set_bucket_ratelimit(self, **kwargs):
        """
        PUT /api/rgw/bucket/{uid}/ratelimit
        Sets ratelimits on the bucket
        """
        data = {
            "bucket": kwargs.get("bucket"),
            "ratelimit": kwargs.get("bucket_ratelimit"),
        }
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
        data = {
            "bucket_name": kwargs.get("bucket"),
            "lifecycle": kwargs.get("lifecycle"),
        }
        resp = self._rest.put(relative_url=url, data=json.dumps(data).replace(" ", ""))
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
