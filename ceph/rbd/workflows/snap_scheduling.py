import ast
import json
import time
from copy import deepcopy

from ceph.rbd.utils import getdict
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from utility.log import Log

log = Log(__name__)


def add_snapshot_scheduling(rbd, **kw):
    """
    Add snapshot scheduling to an rbd mirror cluster

    Args:
        rbd: RBD object
        kw: Dictionary with keys - pool, image, level,
        group (optional), namespace (optional), interval
    Returns:
        Tuple (out, err) from the executed command
    """
    pool = kw.get("pool")
    image = kw.get("image")
    level = kw.get("level")
    group = kw.get("group", "")
    namespace = kw.get("namespace", "")
    interval = kw.get("interval")

    if level == "cluster":
        out, err = rbd.mirror.snapshot.schedule.add_(interval=interval)

    elif level == "pool":
        out, err = rbd.mirror.snapshot.schedule.add_(pool=pool, interval=interval)

    elif level == "group":
        group_kw = {"pool": pool, "interval": interval, "group": group}
        if namespace:
            group_kw["namespace"] = namespace
        out, err = rbd.mirror.group.snapshot.schedule.add_(**group_kw)

    elif level == "namespace":
        if namespace:
            namespace_kw = {"pool": pool, "interval": interval, "namespace": namespace}
            out, err = rbd.mirror.snapshot.schedule.add_(**namespace_kw)
        else:
            # Default namespace: treat as image-level schedule
            image_kw = {"pool": pool or "rbd", "image": image, "interval": interval}
            out, err = rbd.mirror.snapshot.schedule.add_(**image_kw)

    else:
        # Default case: treat it as image-level snapshot schedule
        # If pool is not specified, assume default pool "rbd"
        pool = pool or "rbd"

        image_kw = {"pool": pool, "image": image, "interval": interval}
        if namespace:
            image_kw["namespace"] = namespace

        out, err = rbd.mirror.snapshot.schedule.add_(**image_kw)

    return out, err


def remove_snapshot_scheduling(rbd, **kw):
    """
    Remove snapshot scheduling from an rbd mirror cluster

    Args:
        rbd: RBD object
        kw: Dictionary with keys - pool, image, level,
        group (optional), namespace (optional), interval
    Returns:
        Tuple (out, err) from the executed command
    """
    pool = kw.get("pool")
    image = kw.get("image")
    level = kw.get("level")
    group = kw.get("group", "")
    namespace = kw.get("namespace", "")
    interval = kw.get("interval")

    if level == "cluster":
        out, err = rbd.mirror.snapshot.schedule.rm(interval=interval)

    elif level == "pool":
        out, err = rbd.mirror.snapshot.schedule.remove_(pool=pool, interval=interval)

    elif level == "group":
        group_kw = {"pool": pool, "interval": interval, "group": group}
        if namespace:
            group_kw["namespace"] = namespace
        out, err = rbd.mirror.group.snapshot.schedule.remove_(**group_kw)

    elif level == "namespace":
        if not namespace:
            # Default namespace: treat as image-level schedule
            image_kw = {"pool": pool or "rbd", "image": image, "interval": interval}
            out, err = rbd.mirror.snapshot.schedule.remove_(**image_kw)
        else:
            namespace_kw = {"pool": pool, "interval": interval, "namespace": namespace}
            out, err = rbd.mirror.snapshot.schedule.remove_(**namespace_kw)

    else:
        # Default case: treat it as image-level snapshot schedule
        pool = pool or "rbd"
        image_kw = {"pool": pool, "image": image, "interval": interval}
        if namespace:
            image_kw["namespace"] = namespace
        out, err = rbd.mirror.snapshot.schedule.remove_(**image_kw)

    return out, err


def verify_snapshot_schedule(
    rbd, pool, image=None, interval="1m", namespace=None, **kw
):
    """
    Verify snapshot schedule on an image or namespace or pool level,
    where snapshot-based mirroring is enabled.

    Args:
        rbd         : RBD object to run commands
        pool        : Pool name
        interval    : Schedule interval string like "1m"
        image       : (Optional) Image name
        namespace   : (Optional) Namespace name

    Returns:
        0 if snapshot schedule is verified successfully
        1 if verification fails
    """
    try:
        status_spec = {"pool": pool, "format": "json"}
        out, err = None, None

        if namespace:
            status_spec["namespace"] = namespace

            # First try namespace-level
            out, err = rbd.mirror.snapshot.schedule.ls(**status_spec)

            if (not out or out.strip() == "[]") and image:
                # No namespace-level schedule found, fall back to namespace+image
                status_spec["image"] = image
                out, err = rbd.mirror.snapshot.schedule.ls(**status_spec)

        elif image:
            # No namespace, image-level only
            status_spec["image"] = image
            out, err = rbd.mirror.snapshot.schedule.ls(**status_spec)

        else:
            # Pure pool-level schedule
            out, err = rbd.mirror.snapshot.schedule.ls(**status_spec)

        # Error handling
        if err:
            log.error(
                "Error fetching snapshot schedule list for {0}{1}{2}".format(
                    pool,
                    "/" + namespace if namespace else "",
                    "/" + image if image else "",
                )
            )
            return 1

        # Build image list spec
        if image:
            image_list = [image]
        else:
            image_list_spec = {"pool": pool, "format": "json"}
            if namespace:
                image_list_spec["namespace"] = namespace

            out, err = rbd.ls(**image_list_spec)
            if err:
                log.error(
                    "Failed to list images in {0}/{1}: {2}".format(
                        pool, namespace or "", err
                    )
                )
                return 1

            try:
                image_list = ast.literal_eval(out.strip())
            except Exception as e:
                log.error(
                    "Failed to parse image list output: {0}, error: {1}".format(out, e)
                )
                return 1

        for image in image_list:
            # Rebuild image status_spec for mirror image status query
            image_status_spec = {"pool": pool, "image": image, "format": "json"}
            if namespace:
                image_status_spec["namespace"] = namespace

            # Check initial snapshot state
            output, err = rbd.mirror.image.status(**image_status_spec)
            if err:
                log.error(
                    "Error fetching mirror image status for {0}/{1}/{2}: {3}".format(
                        pool, namespace or "", image, err
                    )
                )
                return 1

            json_dict = json.loads(output)
            log.info("Initial image mirror status: \n{0}".format(json_dict))

            snapshot_ids = [snap["id"] for snap in json_dict.get("snapshots", [])]
            log.info("Snapshot IDs before interval: {0}".format(snapshot_ids))

            # Wait for snapshots to mirror in remote cluster
            interval_int = int(interval[:-1])
            time.sleep(interval_int * 120)

            # Check snapshot state again
            output, err = rbd.mirror.image.status(**image_status_spec)
            if err:
                log.error(
                    "Error fetching mirror image status after interval for {0}/{1}/{2}: {3}".format(
                        pool, namespace or "", image, err
                    )
                )
                return 1

            json_dict = json.loads(output)
            log.info("Post-wait image mirror status: \n{0}".format(json_dict))

            snapshot_ids_after = [snap["id"] for snap in json_dict.get("snapshots", [])]
            log.info("Snapshot IDs after interval: {0}".format(snapshot_ids_after))

            if snapshot_ids != snapshot_ids_after:
                log.info(
                    "Snapshot schedule verification successful for {0}/{1}{2}".format(
                        pool, namespace + "/" if namespace else "", image
                    )
                )
            else:
                log.error(
                    "Snapshot schedule verification failed for {0}/{1}{2}".format(
                        pool, namespace + "/" if namespace else "", image
                    )
                )
                return 1

    except Exception as e:
        log.error(
            "Snapshot schedule verification failed for {0}/{1}{2} with error: {3}".format(
                pool, namespace + "/" if namespace else "", image if image else "", e
            )
        )
        return 1

    return 0


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


def verify_namespace_snapshot_schedule(
    rbd, pool, namespace, interval="1m", image="all", **kw
):
    """
    This will verify the snapshot schedules at namespace level and image level
    Args:
        pool: pool name
        namespace: namespace name
        interval: interval and specified in min
        image: image name for specific image or 'all' for all images in namespace
    """

    status_spec = {"pool": pool, "namespace": namespace, "format": "json"}
    out, err = rbd.mirror.snapshot.schedule.ls(**status_spec)
    if err:
        raise Exception(err)

    schedule_list = json.loads(out)
    log.info(f"Snap schedule list: {schedule_list}")
    schedule_present = [
        schedule for schedule in schedule_list if schedule["interval"] == interval
    ]
    if not schedule_present:
        raise Exception(
            f"Snapshot schedule not listed for namespace {pool}/{namespace} at interval {interval}"
        )
    if image == "all":
        out, err = rbd.ls(**{"pool-spec": f"{pool}/{namespace}", "format": "json"})
        images = ast.literal_eval(out.strip())
        for img in images:
            check_image_status(rbd, pool, namespace, img, interval=interval)
    else:
        check_image_status(rbd, pool, namespace, image, interval=interval)


def check_image_status(rbd, pool, namespace, img, interval="1m"):
    """
    This will verify the snapshot schedules for a given image
    Args:
        pool: pool name
        namespace: namespace name
        imag: image name
        interval: interval specified in min
    """
    status_spec = {"pool": pool, "namespace": namespace, "image": img, "format": "json"}
    output, err = rbd.mirror.image.status(**status_spec)
    if err:
        raise Exception(
            f"Error while fetching mirror image status for image {pool}/{namespace}/{img} with {err}"
        )
    json_dict = json.loads(output)
    log.info(f"Image status : \n {json_dict}")
    snapshot_ids = [i["id"] for i in json_dict.get("snapshots")]
    log.info(f"snapshot_ids Before : {snapshot_ids}")
    interval_int = int(interval[:-1])
    time.sleep(interval_int * 120)
    output, err = rbd.mirror.image.status(**status_spec)
    if err:
        raise Exception(
            f"Error while fetching mirror image status for image {pool}/{namespace}/{img} with {err}"
        )

    json_dict = json.loads(output)
    log.info(f"Image status : \n {json_dict}")
    snapshot_ids_1 = [i["id"] for i in json_dict.get("snapshots")]
    log.info(f"snapshot_ids After : {snapshot_ids_1}")
    if snapshot_ids != snapshot_ids_1:
        log.info(
            "Snapshot schedule verification successful for image "
            + pool
            + "/"
            + namespace
            + "/"
            + img
        )
    else:
        raise Exception(
            f"Snapshot schedule verification failed for image {pool}/{namespace}/{img}"
        )
