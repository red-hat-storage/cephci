from rest.common.utils.rest import rest
from rest.workflows.rgw.rgw import create_bucket_verify
from utility.log import Log

log = Log(__name__)


def sanity_rgw_workflow(config):
    """
    Executes RGW bucket creation and verification workflow.
    Args:
        config: Dictionary containing bucket configuration details.
    """
    _rest = rest()

    log.info("Step 1: Starting bucket creation workflow")

    buckets = config.get("buckets", [])
    for bucket_cfg in buckets:
        log.info(f"Step 2: Creating and verifying bucket {bucket_cfg.get('bucket')}")
        rc_create = create_bucket_verify(**bucket_cfg, rest=_rest)
        if rc_create:
            log.error(
                f"FAILED: Bucket creation or verification failed for {bucket_cfg.get('bucket')}"
            )
            return 1

    return 0


def run(**kwargs):
    """
    Entry point for running the RGW sanity workflow.
    Args:
        kwargs: Keyword arguments containing the config object.
    """
    config = kwargs.get("config")
    try:
        rc = sanity_rgw_workflow(config)
    except Exception as e:
        log.error(f"Exception occurred during sanity workflow: {str(e)}")
        rc = 1
    return rc
