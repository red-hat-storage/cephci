import json

from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import getdict
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.rbd import create_snap_and_clone
from utility.log import Log

log = Log(__name__)


def verify_deep_flatten(clone_format, rbd, pool, image, snap_spec):
    """ """
    snap_conf = {"snap-spec": snap_spec}
    out, err = rbd.snap.unprotect(**snap_conf)
    if clone_format == 1:
        if out or err:
            err_msg = f"Snapshot unprotect failed as expected for {snap_spec} since deep-flatten"
            err_msg += (
                f" is not enabled on the image {image} and v1 clone contains snapshots"
            )
            log.info(err_msg)
            log.info(f"Error message for snapshot unprotect: {out} {err}")
        else:
            log.error("Snapshot unprotect succeeded even though v1 clone had snapshots")
            return 1
    else:
        out, err = rbd.snap.rm(**snap_conf)
        if out or err and "100% complete" not in err:
            err_msg = f"Snapshot remove failed for {snap_spec}"
            log.info(err_msg)
            log.info(f"Error message for snapshot remove {out} {err}")
        else:
            snap_ls_conf = {
                "image-spec": f"{pool}/{image}",
                "all": True,
                "format": "json",
            }
            out, err = rbd.snap.ls(**snap_ls_conf)
            if err:
                log.error("Error while fetch snap list")
                return 1
            else:
                snap_list = json.loads(out)
                snap_name = snap_spec.replace(f"{pool}/{image}@", "")
                snap_req = [
                    snap_ls
                    for snap_ls in snap_list
                    if snap_name in snap_ls.get("namespace")
                ]
                if all(
                    x in snap_ls.get("namespace")
                    for snap_ls in snap_req
                    for x in ["trash", snap_name]
                ):
                    err_msg = f"Snapshot remove resulted in snapshot {snap_spec} moving to trash as expected"
                    err_msg += f" since deep-flatten is not enabled on the image {image} and v2 clone"
                    err_msg += " contains snapshots"
                    log.info(err_msg)
                    log.info(f"Snap list: {snap_list}")
                else:
                    log.error(
                        "Snapshot removed completely even though v2 clone had snapshots"
                    )
                    return 1


def test_deep_flatten_negative_scenario(rbd_obj, **kw):
    """ """
    for pool_type in rbd_obj.get("pool_types"):
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = getdict(rbd_config)
        clone_formats = rbd_config.pop("clone_formats")
        for pool, pool_config in multi_pool_config.items():
            multi_image_config = getdict(pool_config)
            for image, clone_format in zip(multi_image_config.keys(), clone_formats):
                log.info(f"Creating snapshot and clone for image {pool}/{image}")
                rbd = rbd_obj.get("rbd")
                # Start by creating cli for rbd feature commands
                feature_spec = {
                    "image-spec": f"{pool}/{image}",
                    "features": "deep-flatten",
                }
                out, err = rbd.feature.disable(**feature_spec)
                if out or err:
                    log.error(f"Feature deep-flatten was not disabled for {image}")
                    return 1
                info_spec = {"image-or-snap-spec": f"{pool}/{image}", "format": "json"}
                out, err = rbd.info(**info_spec)
                if err:
                    log.info(f"Error while fetching info for image {image}")
                    return 1
                log.info(f"Image info: {out}")

                out_json = json.loads(out)
                if "deep-flatten" in out_json["features"]:
                    log.error(f"Feature deep-flatten was not disabled for {image}")
                    return 1

                snap_spec = f"{pool}/{image}@snap_{image}"
                clone_spec = f"{pool}/clone_{image}"
                create_snap_and_clone(
                    rbd, snap_spec, clone_spec, clone_format=clone_format
                )
                # Create snapshot for the clone
                clone_snap_spec = {
                    "snap-spec": f"{pool}/clone_{image}@snap_clone_{image}"
                }
                out, err = rbd.snap.create(**clone_snap_spec)
                if out or err and "100% complete" not in err:
                    log.error(f"Snapshot creation failed for {clone_snap_spec}")
                    return 1

                flatten_config = {"image-spec": clone_spec}
                out, err = rbd.flatten(**flatten_config)
                if out or err and "100% complete" not in err:
                    log.error(f"Flatten clone failed for {clone_spec}")
                    return 1

                if verify_deep_flatten(clone_format, rbd, pool, image, snap_spec):
                    log.error(f"deep-flatten verification failed for {clone_format}")
                    return 1
    return 0


def run(**kw):
    """Test parent snapshot deletion on an image where deep-flatten is disabled
     (Negative scenario)

    Pre-requisites :
    We need atleast one client node with ceph-common and fio packages,
    conf and keyring files

    Test cases covered -
    1) CEPH-9831 - Test parent snapshot deletion on an image where deep-flatten
     is disabled (Negative scenario)

    Test Case Flow
    1. Create image and disable deep-flatten feature on that image
    2. Create a snapshot on this image, protect the snapshot and create a clone on that image
    3. Create a snapshot for the clone
    4. Flatten the clone
    5. Unprotect the parent snapshot

    """
    log.info("Running deep-flatten negative scenario test CEPH-9831")

    try:
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        ret_val = test_deep_flatten_negative_scenario(rbd_obj=rbd_obj, **kw)
    except Exception as e:
        log.error(f"deep-flatten negative scenario tests failed with error {str(e)}")
        ret_val = 1
    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)
    return ret_val
