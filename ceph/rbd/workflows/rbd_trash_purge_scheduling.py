import json
import time
from copy import deepcopy

from ceph.rbd.utils import getdict
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from utility.log import Log

log = Log(__name__)


def add_trash_purge_scheduling(rbd, **kw):
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


def run_io_verify_snap_schedule_single_image(**kw):
    """
    Run IOs on the given image and verify snapshot schedule
    kw: {
        "rbd": <>,
        "client": <>,
        "pool": <>,
        "image": <>,
        "mount_path": <>,
        "skip_mkfs": <>,
        "image_config": {
            "size": <>,
            "io_size": <>,
            "snap_schedule_intervals":[]
        }
    }
    """
    rbd = kw.get("rbd")
    client = kw.get("client")
    pool = kw.get("pool")
    image = kw.get("image")
    image_spec = f"{pool}/{image}"
    image_config = kw.get("image_config")

    io_size = image_config.get("io_size", int(int(image_config["size"][:-1]) / 3))
    io_config = {
        "rbd_obj": rbd,
        "client": client,
        "size": image_config["size"],
        "do_not_create_image": True,
        "config": {
            "file_size": io_size,
            "file_path": [f"{kw['mount_path']}"],
            "get_time_taken": True,
            "image_spec": [image_spec],
            "operations": {
                "fs": "ext4",
                "io": True,
                "mount": True,
                "nounmap": False,
                "device_map": True,
            },
            "skip_mkfs": kw["skip_mkfs"],
        },
    }
    krbd_io_handler(**io_config)
    kw["io_config"] = io_config
    for interval in image_config.get("snap_schedule_intervals"):
        out = verify_snapshot_schedule(rbd, pool, image, interval)
        if out:
            log.error(f"Snapshot verification failed for image {pool}/{image}")
            if kw.get("raise_exception"):
                raise Exception(
                    f"Snapshot verification failed for image {pool}/{image}"
                )
            return 1
    return 0


def run_io_verify_snap_schedule(**kw):
    """
    Run IOs on the given images and verify snapshot schedule
    """
    pool_type = kw.get("pool_type")
    rbd = kw.get("rbd")
    client = kw.get("client")
    config = deepcopy(kw.get("config").get(pool_type))
    for pool, pool_config in getdict(config).items():
        multi_image_config = getdict(pool_config)
        multi_image_config.pop("test_config", {})
        for image, image_config in multi_image_config.items():
            rc = run_io_verify_snap_schedule_single_image(
                rbd=rbd,
                client=client,
                pool=pool,
                image=image,
                image_config=image_config,
                mount_path=kw.get("mount_path"),
                skip_mkfs=kw.get("skip_mkfs"),
            )
            if rc:
                log.error(
                    f"Run IO and verify snap schedule failed for image {pool}/{image}"
                )
                return 1
    return 0
