import json
from copy import deepcopy
from rest.common.config.config import Config
from utility.log import Log

log = Log(__name__)


class RGW:
    def __init__(self, rest):
        self._config = Config()
        self._rest = rest
        config_file_reader = self._config.get_config()
        self._list_bucket = config_file_reader["endpoints"]["rgw"]
        ["LIST_BUCKET"]
        self._create_bucket = config_file_reader["endpoints"]["rgw"]
        ["CREATE_BUCKET"]
        self.created_buckets = []

    def create_bucket(self, **kw):
        """
        REST POST endpoint /api/rgw/bucket
        Request data details
        {
            "bucket": "string",
            "uid": "string",
            "zonegroup": null,
            "placement_target": null,
            "lock_enabled": "false",
            "lock_mode": null,
            "lock_retention_period_days": null,
            "lock_retention_period_years": null,
            "encryption_state": "false",
            "encryption_type": null,
            "key_id": null,
            "tags": null,
            "bucket_policy": null,
            "canned_acl": null,
            "replication": "false",
            "daemon_name": null
        }

        Args:
            kw: create subsystem related kw args
        """
        data = deepcopy(kw)
        response = self._rest.post(
            relative_url=self._create_bucket, data=json.dumps(data)
        )
        return response

    def list_subsystem(self):
        """
        REST GET endpoint /api/rgw/bucket
        """
        response = self._rest.get(relative_url=self._list_bucket)
        return response
