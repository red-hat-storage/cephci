import copy
import time

from rest.common.utils.rest import rest
from rest.workflows.rgw.rgw import create_bucket_verify
from utility.log import Log

log = Log(__name__)


def sanity_rgw_workflow(config):
    """
    Execute the RGW bucket sanity workflow using the provided configuration.

    Args:
        config (dict): Configuration dictionary containing:
            - "ceph_cluster": Identifier or configuration for the Ceph cluster.
            - Other keys representing bucket names with nested configs (must include "uid").

    Returns:
        int:
            0 if all buckets are created and verified successfully.
            1 if any required config is missing or an error occurs during the workflow.
    """
    if not config:
        log.error("No config provided")
        return 1

    ceph_cluster = config.get("ceph_cluster")
    if not ceph_cluster:
        log.error("ceph_cluster missing in config")
        return 1

    _rest = rest(ceph_cluster=ceph_cluster)
    _rest_v1 = rest(
        ceph_cluster=ceph_cluster, accept="application/vnd.ceph.api.v1.1+json"
    )
    log.info("Step 1: Starting RGW bucket sanity workflow")

    for bucket_name, bucket_cfg in config.items():
        if bucket_name == "ceph_cluster":
            continue

        uid = bucket_cfg.get("uid")
        lifecycle = bucket_cfg.get("lifecycle")
        ratelimit_config = bucket_cfg.get("ratelimit")
        if not uid:
            log.error(f"Missing 'uid' for bucket '{bucket_name}'")
            return 1

        if not lifecycle:
            log.error(f"Missing 'lifecycle' for bucket '{bucket_name}'")
            return 1

        log.info(f"Step 2: Creating and verifying bucket '{bucket_name}'")
        rc = create_bucket_verify(
            bucket=bucket_name,
            uid=uid,
            rest=_rest,
            rest_v1=_rest_v1,
            lifecycle=lifecycle,
            ratelimit=ratelimit_config,
        )
        if rc != 0:
            log.error(
                f"FAILED: Bucket creation/verification failed for '{bucket_name}'"
            )
            return 1

    log.info("All bucket operations completed successfully")
    return 0


def run(**kwargs):
    """
    Entry point to trigger the RGW sanity workflow.

    Args:
        kwargs (dict): Keyword arguments containing:
            - "config": A dict with an "args" sub-dict containing bucket details.
            - "ceph_cluster": Cluster context to be added to the config.

    Returns:
        int:
            0 if the workflow completes successfully.
            1 if configuration is missing or any step fails.
    """
    config = kwargs.get("config")
    if not config:
        log.error("Config missing in run()")
        return 1

    config_copy = copy.deepcopy(config.get("args", {}))
    config_copy["ceph_cluster"] = kwargs.get("ceph_cluster")
    time.sleep(120)

    rc = sanity_rgw_workflow(config_copy)
    return rc
