import json
import time

from utility.log import Log

log = Log(__name__)


def add_snapshot_scheduling(rbd, **kw):
    """
    Add snapshot scheduling to an rbd mirror cluster
    """
    pool = kw.get("pool")
    image = kw.get("image")
    level = kw.get("level")
    interval = kw.get("interval")

    if level == "cluster":
        out, err = rbd.mirror.snapshot.schedule.add_(interval=interval)
    elif level == "pool":
        out, err = rbd.mirror.snapshot.schedule.add_(pool=pool, interval=interval)
    else:
        out, err = rbd.mirror.snapshot.schedule.add_(
            pool=pool, image=image, interval=interval
        )

    return out, err


def verify_snapshot_schedule(rbd, pool, image, interval="1m"):
    """
    This will verify the snapshot roll overs on the image
    snapshot based mirroring is enabled
    Args:
        pool: pool name
        image: image name
        interval : this is interval and specified in min
    Returns:
        0 if snapshot schedule is verified successfully
        1 if fails
    """
    try:
        status_spec = {"pool": pool, "image": image, "format": "json"}
        out, err = rbd.mirror.snapshot.schedule.ls(**status_spec)
        if err:
            log.error(
                f"Error while fetching snapshot schedule list for image {pool}/{image}"
            )
            return 1

        schedule_list = json.loads(out)
        schedule_present = [
            schedule for schedule in schedule_list if schedule["interval"] == interval
        ]
        if not schedule_present:
            log.error(
                f"Snapshot schedule not listed for image {pool}/{image} at interval {interval}"
            )
            return 1

        output, err = rbd.mirror.image.status(**status_spec)
        if err:
            log.error(
                f"Error while fetching mirror image status for image {pool}/{image}"
            )
            return 1
        json_dict = json.loads(output)
        snapshot_ids = [i["id"] for i in json_dict.get("snapshots")]
        log.info(f"snapshot_ids Before : {snapshot_ids}")
        interval_int = int(interval[:-1])
        time.sleep(interval_int * 120)
        output, err = rbd.mirror.image.status(**status_spec)
        if err:
            log.error(
                f"Error while fetching mirror image status for image {pool}/{image}"
            )
            return 1
        json_dict = json.loads(output)
        snapshot_ids_1 = [i["id"] for i in json_dict.get("snapshots")]
        log.info(f"snapshot_ids After : {snapshot_ids_1}")
        if snapshot_ids != snapshot_ids_1:
            log.info(
                f"Snapshot schedule verification successful for image {pool}/{image}"
            )
            return 0
        log.error(f"Snapshot schedule verification failed for image {pool}/{image}")
        return 1
    except Exception as e:
        log.error(
            f"Snapshot verification failed for image {pool}/{image} with error {e}"
        )
        return 1
