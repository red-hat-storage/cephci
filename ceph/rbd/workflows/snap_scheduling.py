import ast
import json
import time
from copy import deepcopy

from ceph.ceph import CommandFailed
from ceph.rbd.utils import exec_cmd, getdict, random_string
from ceph.rbd.workflows.cleanup import device_cleanup
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from utility.log import Log
from utility.utils import run_fio

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
    run_time = image_config.get("run_time", 300)
    io_config = {
        "rbd_obj": rbd,
        "client": client,
        "size": image_config["size"],
        "do_not_create_image": True,
        "config": {
            "file_size": io_size,
            "run_time": run_time,
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
    rc, err = krbd_io_handler(**io_config)
    kw["io_config"] = io_config
    if rc:
        log.error(
            f"krbd_io_handler failed for image {image_spec}: {err if err else rc}"
        )
        if kw.get("raise_exception"):
            raise Exception(f"krbd_io_handler failed for image {image_spec}")
        return 1
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
                raise_exception=kw.get("raise_exception"),
            )
            if rc:
                log.error(
                    f"Run IO and verify snap schedule failed for image {pool}/{image}"
                )
                return 1
    return 0


def prepare_upgrade_io_mount(**kw):
    """Map, mkfs and mount an RBD image once, keeping the mapping active.

    Unlike run_io_verify_snap_schedule_single_image, this does not run fio and
    intentionally leaves the device mapped and filesystem mounted for reuse
    across rolling-upgrade IO phases.

    Map is done via krbd_io_handler; mkfs/mount are done explicitly so mkfs
    failures (e.g. stale already-mounted NBD) are not ignored.

    kw:
        rbd, client, pool, image, image_config, pool_type (optional),
        mount_path (optional), fs (optional, default ext4)

    Returns:
        (mount_ctx dict, 0) on success or (None, 1) on failure.
    """
    rbd = kw.get("rbd")
    client = kw.get("client")
    pool = kw.get("pool")
    image = kw.get("image")
    image_config = kw.get("image_config", {})
    image_spec = f"{pool}/{image}"
    mount_path = kw.get("mount_path") or f"/tmp/mnt_{random_string(len=5)}"
    fs_type = kw.get("fs", "ext4")

    log.info(
        "Preparing persistent RBD mount for upgrade test: "
        f"image={image_spec} mount={mount_path}"
    )

    # Map only; do mkfs/mount ourselves so failures are not ignored.
    io_config = {
        "rbd_obj": rbd,
        "client": client,
        "size": image_config.get("size", "1G"),
        "do_not_create_image": True,
        "config": {
            "image_spec": [image_spec],
            "operations": {
                "fs": fs_type,
                "io": False,
                "mount": False,
                "nounmap": True,
                "device_map": True,
            },
            "skip_mkfs": True,
        },
    }
    rc, err = krbd_io_handler(**io_config)
    if rc:
        log.error(
            f"Failed to map RBD image for persistent mount {image_spec}: "
            f"{err if err else rc}"
        )
        return None, 1

    device_names = io_config["config"].get("device_names") or []
    if not device_names:
        log.error(f"No device returned after mapping {image_spec}")
        return None, 1

    # Match krbd_io_handler device_names handling
    if isinstance(device_names[-1], tuple):
        device_name = device_names[-1][0].strip()
    else:
        device_name = str(device_names[-1]).strip()

    # Stale NBD mounts are a common cause of:
    #   mkfs: /dev/nbdX is mounted; will not make a filesystem here!
    already_mounted = exec_cmd(
        cmd=f"findmnt -rn -S {device_name}",
        node=client,
        output=True,
    )
    if already_mounted and already_mounted != 1:
        stale_target = str(already_mounted).strip().split()[0]
        log.error(
            f"Mapped device {device_name} for {image_spec} is already mounted at "
            f"'{stale_target}'. Refusing to mkfs/mount over stale NBD state."
        )
        device_cleanup(
            rbd=rbd,
            client=client,
            file_name=stale_target,
            device_name=device_name,
            **{"device-type": "nbd"},
        )
        return None, 1

    if exec_cmd(cmd=f"mkfs -t {fs_type} {device_name}", node=client):
        log.error(f"mkfs -t {fs_type} failed on {device_name} for {image_spec}")
        device_cleanup(
            rbd=rbd,
            client=client,
            device_name=device_name,
            **{"device-type": "nbd"},
        )
        return None, 1

    _, mount_err = exec_cmd(
        cmd=f"mkdir -p {mount_path}; mount {device_name} {mount_path}",
        node=client,
        all=True,
    )
    if mount_err:
        log.error(
            f"Mount failed for {image_spec} on {device_name} -> {mount_path}: "
            f"{mount_err}"
        )
        device_cleanup(
            rbd=rbd,
            client=client,
            file_name=mount_path,
            device_name=device_name,
            **{"device-type": "nbd"},
        )
        return None, 1

    mount_ctx = {
        "pool_type": kw.get("pool_type"),
        "pool": pool,
        "image": image,
        "image_spec": image_spec,
        "device_name": device_name,
        "mount_path": mount_path,
        "client": client,
        "rbd": rbd,
        "image_config": image_config,
    }

    if verify_persistent_rbd_mount(mount_ctx):
        log.error(
            f"Post-prepare validation failed for {image_spec} "
            f"(device={device_name}, mount={mount_path})"
        )
        device_cleanup(
            rbd=rbd,
            client=client,
            file_name=mount_path,
            device_name=device_name,
            **{"device-type": "nbd"},
        )
        return None, 1

    log.info(
        "Prepared persistent RBD mount for upgrade test: "
        f"image={image_spec} device={device_name} mount={mount_path}"
    )
    return mount_ctx, 0


def prepare_upgrade_io_mounts(rbd, client, pool_types, **kw):
    """Prepare one persistent mount context per image across pool types.

    Returns:
        (list of mount_ctx, 0) on success or (partial list, 1) on failure.
    """
    # Clear leftover NBD mounts/maps from prior runs so mkfs does not hit
    # "/dev/nbdX is mounted" and leave an inconsistent client state.
    log.info("Cleaning stale NBD mappings/mounts before preparing upgrade IO mounts")
    device_cleanup(rbd=rbd, client=client, all=True, device_type="nbd")

    mount_contexts = []
    config = deepcopy(kw.get("config", {}))
    for pool_type in pool_types:
        pool_type_config = deepcopy(config.get(pool_type, {}))
        for pool, pool_config in getdict(pool_type_config).items():
            multi_image_config = getdict(pool_config)
            multi_image_config.pop("test_config", {})
            for image, image_config in multi_image_config.items():
                mount_ctx, rc = prepare_upgrade_io_mount(
                    rbd=rbd,
                    client=client,
                    pool=pool,
                    image=image,
                    image_config=image_config,
                    pool_type=pool_type,
                )
                if rc:
                    log.error(f"Failed to prepare upgrade mount for {pool}/{image}")
                    return mount_contexts, 1
                mount_contexts.append(mount_ctx)
    return mount_contexts, 0


def verify_persistent_rbd_mount(mount_ctx, remount=False):
    """Verify that the expected mount and NBD mapping are still active.

    When ``remount`` is True (e.g. after a cluster upgrade that may have
    restarted rbd-nbd), the function attempts to remap and remount the image
    when it detects the device is gone, updating ``mount_ctx`` in-place.
    When ``remount`` is False the original strict behaviour is preserved:
    a missing mount or mapping is immediately reported as a failure.
    """
    client = mount_ctx["client"]
    rbd = mount_ctx["rbd"]
    mount_path = mount_ctx["mount_path"]
    device_name = mount_ctx["device_name"]
    image_spec = mount_ctx["image_spec"]
    image = mount_ctx["image"]
    pool = mount_ctx["pool"]

    findmnt_out = exec_cmd(
        cmd=f"findmnt -rn {mount_path}",
        node=client,
        output=True,
    )
    mount_ok = findmnt_out and findmnt_out != 1

    if not mount_ok:
        if not remount:
            log.error(
                f"Expected persistent RBD mount {mount_path} for {image_spec} "
                "is no longer mounted."
            )
            return 1

        log.warning(
            f"Persistent mount {mount_path} for {image_spec} is gone "
            "(likely due to rbd-nbd restart during upgrade). "
            "Attempting to remap and remount."
        )
        # Remap using rbd device map (nbd)
        map_out, map_err = rbd.device.map(
            **{
                "pool": pool,
                "image": image,
                "device-type": "nbd",
            }
        )
        if map_err:
            log.error(f"Failed to remap {image_spec} after upgrade: {map_err}")
            return 1
        new_device = map_out.strip()
        mount_ctx["device_name"] = new_device
        device_name = new_device
        _, mount_err = exec_cmd(
            cmd=f"mount {device_name} {mount_path}",
            node=client,
            all=True,
        )
        if mount_err:
            log.error(
                f"Remount of {image_spec} on {device_name} -> {mount_path} "
                f"failed: {mount_err}"
            )
            return 1
        log.info(
            f"Remapped and remounted {image_spec}: "
            f"device={device_name} mount={mount_path}"
        )
        return 0

    if device_name not in str(findmnt_out):
        log.error(
            f"Mount {mount_path} for {image_spec} is active but not backed by "
            f"expected device {device_name}. findmnt={findmnt_out}"
        )
        return 1

    list_out = exec_cmd(
        cmd="rbd device list --device-type nbd --format json",
        node=client,
        output=True,
    )
    if not list_out or list_out == 1:
        log.error(
            f"Failed to list NBD devices while validating mapping for {image_spec}"
        )
        return 1

    try:
        devices = json.loads(list_out)
    except (TypeError, json.JSONDecodeError) as exc:
        log.error(f"Unable to parse rbd device list output '{list_out}': {exc}")
        return 1

    mapped = False
    for entry in devices or []:
        entry_device = str(entry.get("device") or "")
        entry_pool = entry.get("pool") or ""
        entry_image = entry.get("image") or ""
        if entry_device == device_name and entry_pool == pool and entry_image == image:
            mapped = True
            break
        if entry_device == device_name and image in (
            entry_image,
            str(entry.get("spec", "")),
        ):
            mapped = True
            break

    if not mapped:
        # Plain-text fallback for older/variant device-list schemas
        plain_out = exec_cmd(
            cmd="rbd device list --device-type nbd",
            node=client,
            output=True,
        )
        plain_text = str(plain_out or "")
        if device_name not in plain_text or image not in plain_text:
            log.error(
                f"Expected persistent RBD mapping for {image_spec} on "
                f"{device_name} is no longer present. device_list={list_out}"
            )
            return 1

    log.info(
        f"Validated persistent RBD mount: image={image_spec} "
        f"device={device_name} mount={mount_path}"
    )
    return 0


def run_io_on_existing_mount(
    mount_ctx,
    file_name,
    verify_snap_schedule=True,
    raise_exception=False,
):
    """Run fio on an already-mapped/mounted RBD filesystem.

    Does not map, mount, mkfs, unmount or unmap.
    When verify_snap_schedule is True, snapshot schedule verification runs
    after fio. Keep verify_snap_schedule=False while fio runs in parallel with
    cluster upgrade so snap-interval sleeps do not serialize behind upgrade.
    """
    client = mount_ctx["client"]
    rbd = mount_ctx["rbd"]
    pool = mount_ctx["pool"]
    image = mount_ctx["image"]
    image_spec = mount_ctx["image_spec"]
    mount_path = mount_ctx["mount_path"]
    device_name = mount_ctx["device_name"]
    image_config = mount_ctx["image_config"]
    file_path = f"{mount_path}/{file_name}"

    log.info(
        "Reusing existing RBD mount for IO: "
        f"device={device_name} mount={mount_path} file={file_path}"
    )

    remount = mount_ctx.get("remount_after_upgrade", False)
    if verify_persistent_rbd_mount(mount_ctx, remount=remount):
        msg = (
            f"Persistent mount validation failed for {image_spec} "
            f"(device={device_name}, mount={mount_path})"
        )
        log.error(msg)
        if raise_exception:
            raise Exception(msg)
        return 1
    # Refresh device_name in case remount updated mount_ctx
    device_name = mount_ctx["device_name"]

    try:
        # Preserve the same fio defaults used by krbd_io_handler.
        # Derive io_size with the image's unit suffix (e.g. "3G") so fio
        # receives a valid size argument and does not silently write 3 bytes.
        _size_str = image_config.get("size", "1G")
        _unit = _size_str[-1] if _size_str and _size_str[-1].isalpha() else "G"
        _size_num = int(_size_str[:-1]) if _size_str[:-1].isdigit() else 1
        _derived_io_size = f"{max(1, _size_num // 3)}{_unit}"
        io_size = image_config.get("io_size", _derived_io_size)
        run_time = image_config.get("run_time", 300)
        run_fio(
            client_node=client,
            filename=file_path,
            run_time=run_time,
            size=io_size,
            get_time_taken=True,
            io_type=image_config.get("io_type", "write"),
            num_jobs=image_config.get("num_jobs", "4"),
            iodepth=image_config.get("iodepth", "32"),
            rwmixread=image_config.get("rwmixread", "70"),
            direct=image_config.get("direct", "1"),
            invalidate=image_config.get("invalidate", "1"),
        )
    except CommandFailed as exc:
        log.error(f"fio failed on existing mount {file_path} for {image_spec}: {exc}")
        if raise_exception:
            raise
        return 1
    except Exception as exc:
        log.error(
            f"Unexpected error running fio on {file_path} for {image_spec}: {exc}"
        )
        if raise_exception:
            raise
        return 1

    if not verify_snap_schedule:
        return 0

    for interval in image_config.get("snap_schedule_intervals") or []:
        out = verify_snapshot_schedule(rbd, pool, image, interval)
        if out:
            log.error(f"Snapshot verification failed for image {image_spec}")
            if raise_exception:
                raise Exception(f"Snapshot verification failed for image {image_spec}")
            return 1
    return 0


def run_io_on_existing_mounts(
    mount_contexts,
    file_name,
    verify_snap_schedule=True,
    raise_exception=False,
):
    """Run IO (and optional snap verification) for each persistent mount context."""
    for mount_ctx in mount_contexts:
        rc = run_io_on_existing_mount(
            mount_ctx,
            file_name=file_name,
            verify_snap_schedule=verify_snap_schedule,
            raise_exception=raise_exception,
        )
        if rc:
            return 1
    return 0


def cleanup_upgrade_io_mount(mount_ctx):
    """Unmount then unmap a persistent upgrade-test mount.

    Unmap is skipped automatically by device_cleanup when unmount fails.
    """
    if not mount_ctx:
        return 0

    client = mount_ctx["client"]
    rbd = mount_ctx["rbd"]
    mount_path = mount_ctx["mount_path"]
    device_name = mount_ctx["device_name"]
    image_spec = mount_ctx.get("image_spec")

    log.info(
        "Cleaning persistent RBD mount: "
        f"image={image_spec} device={device_name} mount={mount_path}"
    )
    # Best-effort sync before teardown; ignore failure.
    exec_cmd(cmd="sync", node=client)

    return device_cleanup(
        rbd=rbd,
        client=client,
        file_name=mount_path,
        device_name=device_name,
        **{"device-type": "nbd"},
    )


def cleanup_upgrade_io_mounts(mount_contexts):
    """Clean up all persistent upgrade-test mounts. Always attempts each ctx."""
    flag = 0
    for mount_ctx in mount_contexts or []:
        if cleanup_upgrade_io_mount(mount_ctx):
            flag = 1
    return flag


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
