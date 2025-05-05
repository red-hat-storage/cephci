from utility.log import Log

log = Log(__name__)


def create_bucket_verify(self, **kw):
    """
    Creates and verifies an RGW bucket.
    Args:
        kw: Keyword arguments required for bucket creation and verification.
    """

    bucket_name = kw.get("bucket")
    if not bucket_name:
        log.error("Bucket name is required for creation.")
        return 1

    log.info(f"Creating bucket with name: {bucket_name}")
    try:
        bucket_data = {
            "bucket": bucket_name,
            "uid": kw.get("uid", ""),
            "zonegroup": kw.get("zonegroup", None),
            "placement_target": kw.get("placement_target", None),
            "lock_enabled": kw.get("lock_enabled", "false"),
            "lock_mode": kw.get("lock_mode", None),
            "lock_retention_period_days":
            kw.get("lock_retention_period_days", None),
            "lock_retention_period_years":
            kw.get("lock_retention_period_years", None),
            "encryption_state": kw.get("encryption_state", "false"),
            "encryption_type": kw.get("encryption_type", None),
            "key_id": kw.get("key_id", None),
            "tags": kw.get("tags", None),
            "bucket_policy": kw.get("bucket_policy", None),
            "canned_acl": kw.get("canned_acl", None),
            "replication": kw.get("replication", "false"),
            "daemon_name": kw.get("daemon_name", None),
        }

        self.created_buckets.append(bucket_name)

        log_entry = {
            "action": "create_bucket_verify",
            "bucket": bucket_name,
            "details": bucket_data,
        }
        log.info(f"Successfully created bucket {bucket_name}.")
        log.info(log_entry)
        log.info("Verifying via listing created buckets...")
        if self.created_buckets:
            log.info(f"Buckets found: {self.created_buckets}")
            return 0
        else:
            log.warning("No buckets found after creation (unexpected).")
            return 1

    except Exception as e:
        log.error(f"Failed to create and verify bucket {bucket_name}: {str(e)}")
        return 1
