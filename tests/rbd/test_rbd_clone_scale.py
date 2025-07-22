from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from utility.log import Log

log = Log(__name__)


def rbd_clone_scale(rbd_obj, **kw):
    """
    Verify clone operations at scale per snap
    Args:
        rbd: rbd object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        **kw: Key/value pairs of configuration information to be used in the test
    """

    for pool_type in rbd_obj.get("pool_types"):
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = getdict(rbd_config)
        clone_formats = rbd_config.pop("clone_formats")
        rbd = rbd_obj.get("rbd")
        for pool, pool_config in multi_pool_config.items():
            multi_image_config = getdict(pool_config)
            for image, clone_format in zip(multi_image_config.keys(), clone_formats):
                log.info(f"Creating snapshot and clone for image {pool}/{image}")
                snap_1 = kw["config"][pool_type].get("snap", f"{image}_snap")

                snap_spec = f"{pool}/{image}@{snap_1}"
                clone_spec = f"{pool}/clone_{image}"
                snap_config = {"snap-spec": snap_spec}

                out, err = rbd.snap.create(**snap_config)
                if out or err and "100% complete" not in err:
                    log.error(f"Snapshot creation failed for {snap_spec}")
                    return 1

                out, err = rbd.snap.protect(**snap_config)
                if out or err:
                    log.error(f"Snapshot protect failed for {snap_spec}")
                    return 1

                kw["client"] = kw["ceph_cluster"].get_nodes(role="client")[0]
                err = run_IO(rbd, pool, image, **kw)
                if err:
                    return 1

                create_clone_at_scale(rbd, snap_spec, clone_spec, clone_format, **kw)

                # Flatten Clone
                for i in range(1, 101):
                    out, err = rbd.flatten(pool=pool, image=clone_spec + str(i))
                    if "100% complete...done" not in out + err:
                        log.error(
                            f"Flatten clone failed for {pool}/{clone_spec + str(i)} with error {err}"
                        )

                # unprotect and delete the snap and verify snap deletion
                out, err = rbd.snap.unprotect(**snap_config)
                if out or err:
                    log.error(f"Snapshot unprotect failed for {snap_spec}")
                    return 1

                out, err = rbd.snap.rm(**snap_config)
                if out or err and "100% complete" not in err:
                    err_msg = f"Snapshot remove failed for {snap_spec}"
                    log.info(err_msg)

                log.info("Test clone operations at scale per snap passed Successfully")
    return 0


def create_clone_at_scale(rbd, snap_spec, clone_spec, clone_format, **kw):
    for i in range(1, 101):
        clone_config = {
            "source-snap-spec": snap_spec,
            "dest-image-spec": clone_spec + str(i),
        }
        if kw.get("clone_format"):
            clone_config.update({"rbd-default-clone-format": clone_format})

        out, err = rbd.clone(**clone_config)
        if out or err:
            log.error(f"Clone creation failed for {clone_spec}")
            return 1


def run_IO(rbd, pool, image, **kw):
    fio = kw.get("config", {}).get("fio", {})
    io_config = {
        "rbd_obj": rbd,
        "client": kw["client"],
        "size": fio["size"],
        "do_not_create_image": True,
        "config": {
            "file_size": fio["size"],
            "file_path": ["/mnt/mnt_" + random_string(len=5) + "/file"],
            "get_time_taken": True,
            "image_spec": [pool + "/" + image],
            "operations": {
                "fs": "ext4",
                "io": True,
                "mount": True,
                "device_map": True,
            },
            "cmd_timeout": 2400,
            "io_type": "write",
        },
    }
    out, err = krbd_io_handler(**io_config)
    if err:
        log.error("Map, mount and run IOs failed for " + pool + "/" + image)
        return 1
    else:
        log.info("Map, mount and IOs successful for " + pool + "/" + image)


def run(**kw):
    """CEPH-83584436 - Perform clone operations at scale per snap

    Args:
        kw: test data

    Returns:
        int: The return value. 0 for success, 1 otherwise

    Pre-requisites :
    We need atleast one client node with ceph-common package,
    conf and keyring files

    Test cases covered -
    1. Create pool/image and run IO
    2. Create snap Eg rbd snap create --pool rbd_scale_snap_pool --image rbd_scale_snap_image --snap snap_0
    3. Perform snap clone parallely Eg rbd clone rbd_scale_snap_pool/rbd_scale_snap_image@snap_0
    rbd_scale_snap_pool/clone_SfiDL
    4. Remove snap Eg rbd snap rm --pool rbd_scale_snap_pool --image rbd_scale_snap_image --snap snap_0
    """
    try:
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        ret_val = rbd_clone_scale(rbd_obj=rbd_obj, **kw)
    except Exception as e:
        log.error(
            f"Perform clone operations at scale per snap failed with error {str(e)}"
        )
        ret_val = 1
    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)
    return ret_val
