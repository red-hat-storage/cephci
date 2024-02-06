import json
import pdb

from ceph.parallel import parallel
from ceph.rbd.utils import get_md5sum_rbd_image, random_string
from ceph.rbd.workflows.rbd import wrapper_for_image_ops
from ceph.rbd.workflows.snap_scheduling import (
    run_io_verify_snap_schedule_single_image,
    verify_snapshot_schedule,
)
from utility.log import Log

log = Log(__name__)


def snap_list(**kw):
    """
    List all snapshots for an image
    kw: {
        "rbd": <>,
        "pool": <>,
        "image": <>,
    }
    """
    rbd = kw.get("rbd")
    pool = kw.get("pool")
    image = kw.get("image")
    out, err = rbd.snap.ls(pool=pool, image=image, all=True, format="json")
    if err:
        log.error(f"Snapshot listing failed for image {pool}/{image}")
        return False
    snaps = json.loads(out)
    return snaps


def snap_exists(**kw):
    """
    List snapshots for an image and verify if given
    snapshot is present
    kw: {
        "rbd": <>,
        "pool": <>,
        "image": <>,
        "snap_name": <>,
    }
    """
    rbd = kw.get("rbd")
    pool = kw.get("pool")
    image = kw.get("image")
    snap_name = kw.get("snap_name")
    snaps = snap_list(rbd=rbd, pool=pool, image=image)
    if not snaps:
        log.error(f"Error while listing snapshots for image {pool}/{image}")
        return False
    snap_exists = [snap for snap in snaps if snap["name"] == snap_name]
    if not snap_exists:
        log.error(f"Snapshot {snap_name} not present for image {pool}/{image}")
        return False
    return True


def get_user_defined_snaps(**kw):
    """
    Fetch all user defined snapshots for an image
    kw: {
        "rbd": <>,
        "pool": <>,
        "image": <>
    }
    """
    rbd = kw.get("rbd")
    pool = kw.get("pool")
    image = kw.get("image")
    snaps = snap_list(rbd=rbd, pool=pool, image=image)
    if not snaps:
        log.error(f"Error while listing snapshots for image {pool}/{image}")
        return None
    user_defined_snaps = [
        snap for snap in snaps if snap.get("namespace", {}).get("type") == "user"
    ]
    if not user_defined_snaps:
        log.error(f"User defined snapshots are not present for image {pool}/{image}")
        return None
    return user_defined_snaps


def snap_create_list_and_verify(**kw):
    """
    Create snapshots for all the images and verify
    kw: {
        "rbd":<>,
        "sec_obj":<>,
        "is_secondary":<True/False>,
        "pool": <>,
        "image": <>,
        <multipool and multiimage config>
    }
    """
    rbd = kw.get("rbd")
    sec_rbd = kw.get("sec_obj")
    is_secondary = kw.get("is_secondary")
    pool = kw.get("pool")
    image = kw.get("image")
    image_config = kw.get("image_config")
    snap_name = f"snap_{random_string(len=5)}"
    out, err = rbd.snap.create(pool=pool, image=image, snap=snap_name)
    if (
        is_secondary
        and "failed to create snapshot: (30) Read-only file system" in out + err
    ):
        log.info(
            f"Snapshot creation failed as expected in secondary cluster for {pool}/{image}"
        )
    elif not is_secondary and "100% complete...done" in out + err:
        log.info(f"Snapshot creation successful in primary cluster for {pool}/{image}")
        if not snap_exists(rbd=rbd, pool=pool, image=image, snap_name=snap_name):
            log.error(
                f"Snapshot {snap_name} does not exist in snap ls for {pool}/{image} for primary cluster"
            )
            return 1
        for interval in image_config.get("snap_schedule_intervals"):
            out = verify_snapshot_schedule(rbd, pool, image, interval)
            if out:
                log.error(f"Snapshot verification failed for image {pool}/{image}")
                return 1
        if sec_rbd and not snap_exists(
            rbd=sec_rbd, pool=pool, image=image, snap_name=snap_name
        ):
            log.error(
                f"Snapshot {snap_name} does not exist in snap ls for {pool}/{image} for secondary cluster"
            )
            return 1
    else:
        log.error(f"Snapshot creation did not behave as expected for {pool}/{image}")
        return 1
    return 0


def clone_ops(**kw):
    """
    Protect snapshots, create clones, unprotect snapshots, flatten clones and verify
    kw:{
        "pool": <>,
        "image": <>,
        "rbd": <>,
        "is_secondary": <True/False>,
        "pri_rbd": <>,
        "image_config": <>,
        <multipool and multiimage config>,
        "operations": {
            "protect_snap": <True/False>,
            "create_clone": <True/False>,
            "num_clones_per_snap": <>,
            "unprotect_snap": <True/False>,
            "flatten_clone": <True/False>
        }
    }
    """
    rbd = kw.get("rbd")
    is_secondary = kw.get("is_secondary")
    pri_rbd = kw.get("pri_rbd")
    pool = kw.get("pool")
    image = kw.get("image")
    image_config = kw.get("image_config")
    user_defined_snaps = get_user_defined_snaps(rbd=rbd, pool=pool, image=image)
    operations = kw.get("operations")
    num_clones_per_snap = operations.pop("num_clones_per_snap", 1)
    if operations.get("protect_snap"):
        log.info(
            f"Performing snapshot protect for all user snapshots in {pool}/{image}"
        )
        for snap in user_defined_snaps:
            _, err = rbd.snap.protect(pool=pool, image=image, snap=snap["name"])
            if err:
                if is_secondary and "Read-only file system" in err:
                    log.info(
                        f"Snapshot protection failed as expected for {pool}/{image} with error {err}"
                    )
                    if pri_rbd and snap["protected"] == "false":
                        _, err = pri_rbd.snap.protect(
                            pool=pool, image=image, snap=snap["name"]
                        )
                        if err:
                            log.error(
                                f"Error while protecting snap {snap['name']} at primary"
                            )
                        for interval in image_config.get("snap_schedule_intervals"):
                            out = verify_snapshot_schedule(
                                pri_rbd, pool, image, interval
                            )
                            if out:
                                log.error(
                                    f"Snapshot verification failed for image {pool}/{image}"
                                )
                elif not is_secondary:
                    log.error(
                        f"Snapshot protection failed for {pool}/{image}@{snap['name']} with error {err}"
                    )
                    return 1
            elif is_secondary and not err:
                log.error(f"Snapshot protect did not fail for {pool}/{image}")
                return 1

    if operations.get("create_clone"):
        log.info(
            f"Creating {num_clones_per_snap} number of clones per snapshot for each user\
                    defined snapshot for {pool}/{image}"
        )
        for snap in user_defined_snaps:
            clone_spec = {
                "source-snap-spec": f"{pool}/{image}@{snap['name']}",
                "dest-image-spec": f"{pool}/clone_{random_string(len=5)}",
            }
            _, err = rbd.clone(**clone_spec)
            if err:
                log.error(
                    f"Clone creation failed for {pool}/{image}@{snap['name']} with error {err}"
                )
                return 1

    if operations.get("flatten_clone"):
        log.info(
            f"Performing flatten operations for all clones of all user snapshots in {pool}/{image}"
        )
        for snap in user_defined_snaps:
            out, err = rbd.children(
                pool=pool, image=image, snap=snap["name"], format="json"
            )
            if err:
                log.error(
                    f"Fetching children failed for snap {pool}/{image}@{snap['name']} with error {err}"
                )
                return 1
            clones = json.loads(out)
            for clone in clones:
                _, err = rbd.flatten(pool=clone["pool"], image=clone["image"])
                if "100% complete...done" not in out + err:
                    log.error(
                        f"Flatten clone failed for {clone['pool']}/{clone['image']} with error {err}"
                    )
                    return 1

    if operations.get("unprotect_snap"):
        log.info(
            f"Performing snapshot unprotect for all user snapshots in {pool}/{image}"
        )
        for snap in user_defined_snaps:
            _, err = rbd.snap.unprotect(pool=pool, image=image, snap=snap["name"])
            if err:
                if is_secondary and "Read-only file system" in err:
                    log.info(
                        f"Snapshot unprotect failed as expected for {pool}/{image} with error {err}"
                    )
                elif not is_secondary:
                    log.error(
                        f"Snapshot unprotect failed for {pool}/{image}@{snap['name']} with error {err}"
                    )
                    return 1
            elif is_secondary and not err:
                log.error(f"Snapshot unprotect did not fail for {pool}/{image}")
                return 1
    return 0


def test_snap_rollback(**kw):
    """
    Test snapshot rollback functionality for the user defined snapshot for an image
    kw:{
        "pool": <>,
        "image": <>,
        "image_config": <>,
        "rbd": <>,
        "client": <>,
        "is_secondary": <True/False>,
        "mount_path": <>,
        <multipool and multiimage config>
    }
    """
    rbd = kw.get("rbd")
    is_secondary = kw.get("is_secondary")
    client = kw.get("client")
    pool = kw.get("pool")
    image = kw.get("image")
    image_config = kw.get("image_config")
    user_defined_snap = get_user_defined_snaps(rbd=rbd, pool=pool, image=image)[0]
    if is_secondary:
        _, err = rbd.snap.rollback(
            pool=pool, image=image, snap=user_defined_snap["name"]
        )
        if err and "Read-only file system" in err:
            log.info(
                f"Snap rollback failed as expected for {pool}/{image}@{user_defined_snap['name']} with error {err}"
            )
        else:
            log.error(
                f"Snap rollback did not fail as expected for {pool}/{image}@{user_defined_snap['name']}"
            )
            return 1
    else:
        md5_sum_before_io = get_md5sum_rbd_image(
            image_spec=f"{pool}/{image}",
            rbd=rbd,
            client=client,
            file_path=f"/tmp/{random_string(len=3)}",
        )
        log.info(f"md5sum before IO: {md5_sum_before_io}")
        rc = run_io_verify_snap_schedule_single_image(
            rbd=rbd,
            client=client,
            pool=pool,
            image=image,
            image_config=image_config,
            mount_path=kw.get("mount_path"),
            skip_mkfs=True,
        )
        if rc:
            log.error(
                f"Run IO and verify snap schedule failed for image {pool}/{image}"
            )
            return 1

        md5_sum_before_rollback = get_md5sum_rbd_image(
            image_spec=f"{pool}/{image}",
            rbd=rbd,
            client=client,
            file_path=f"/tmp/{random_string(len=3)}",
        )
        log.info(f"md5sum before rollback: {md5_sum_before_rollback}")

        out, err = rbd.snap.rollback(
            pool=pool, image=image, snap=user_defined_snap["name"]
        )
        if err and "100% complete" not in out + err:
            log.error(
                f"Snapshot rollback failed for {pool}/{image}@{user_defined_snap['name']} with error {out+err}"
            )
            return 1
        md5_sum_after_rollback = get_md5sum_rbd_image(
            image_spec=f"{pool}/{image}",
            rbd=rbd,
            client=client,
            file_path=f"/tmp/{random_string(len=3)}",
        )
        log.info(f"md5sum after rollback: {md5_sum_after_rollback}")

        if (
            not md5_sum_before_io == md5_sum_after_rollback
            and md5_sum_before_rollback != md5_sum_after_rollback
        ):
            log.error(
                f"Rollback operation did not happen as expected for image {pool}/{image}"
            )
            return 1
    return 0


def remove_snap_and_verify(**kw):
    """
    Remove a user defined snapshot for the given images and verify
    kw:{
        "pool": <>,
        "image": <>,
        "rbd": <>,
        "is_secondary": <True/False>,
        <multipool and multiimage config>
    }
    """
    rbd = kw.get("rbd")
    is_secondary = kw.get("is_secondary")
    pool = kw.get("pool")
    image = kw.get("image")
    user_defined_snap = get_user_defined_snaps(rbd=rbd, pool=pool, image=image)[0]
    if not is_secondary and user_defined_snap["protected"] == "true":
        _, err = rbd.snap.unprotect(
            pool=pool, image=image, snap=user_defined_snap["name"]
        )
        if err:
            log.error(
                f"Error while unprotecting snapshot {user_defined_snap['name']}: {err}"
            )
            return 1
    out, err = rbd.snap.rm(pool=pool, image=image, snap=user_defined_snap["name"])
    if is_secondary:
        if err and "Read-only file system" in err:
            log.info(
                f"Snap remove failed as expected for {pool}/{image}@{user_defined_snap['name']} with error {err}"
            )
        else:
            log.error(
                f"Snap remove did not fail as expected for {pool}/{image}@{user_defined_snap['name']}"
            )
            return 1
    else:
        if err and "100% complete...done" not in out + err:
            log.error(
                f"Snapshot remove failed for {pool}/{image}@{user_defined_snap['name']} with error {err}"
            )
            return 1
    if not is_secondary:
        if snap_exists(
            rbd=rbd, pool=pool, image=image, snap_name=user_defined_snap["name"]
        ):
            log.error(
                f"Snapshot {pool}/{image}@{user_defined_snap['name']} exists even after removal"
            )
            return 1
    return 0


def purge_snap_and_verify(**kw):
    """
    Purge all snapshots for the given images and verify
    kw: {
        "pool": <>,
        "image": <>,
        "rbd": <>,
        "is_secondary": <True/False>,
        <multipool and multiimage config>
    }
    """
    rbd = kw.get("rbd")
    is_secondary = kw.get("is_secondary")
    pool = kw.get("pool")
    image = kw.get("image")
    if is_secondary:
        _, err = rbd.snap.purge(pool=pool, image=image)
        if err and "Read-only file system" in err:
            log.info(
                f"Snap purge failed as expected for {pool}/{image} with error {err}"
            )
        else:
            log.error(f"Snap purge did not fail as expected for {pool}/{image}")
            return 1
    else:
        snaps = snap_list(rbd=rbd, pool=pool, image=image)
        for snap in snaps:
            if snap["protected"] == "true":
                _, err = rbd.snap.unprotect(pool=pool, image=image, snap=snap["name"])
                if err:
                    log.error(
                        f"Error while unprotecting snapshot {snap['name']}: {err}"
                    )
                    return 1
        if not snaps:
            log.error(f"Error while listing snapshots for image {pool}/{image}")
            return 1
        elif not len(snaps) >= 1:
            snap_name = f"snap_{random_string(len=5)}"
            out, err = rbd.snap.create(pool=pool, image=image, snap=snap_name)
            if "100% complete...done" not in out + err:
                log.error(
                    f"Snapshot creation failed for {pool}/{image}@{snap_name} with error {err}"
                )
                return 1
        out, err = rbd.snap.purge(pool=pool, image=image)
        if err and "100% complete...done" not in out + err:
            log.error(f"Snap purge failed for {pool}/{image} with error {err}")
            return 1
        snaps = get_user_defined_snaps(rbd=rbd, pool=pool, image=image)
        if snaps:
            log.error(f"Snapshot purge did not delete all snaps for {pool}/{image}")
            return 1
    return 0


def create_clone_and_verify(**kw):
    """
    Create a single snap and clone for the given image

    Args: kw{
        "rbd": <>,
        "pool": <>,
        "image": <>,
        "snap": <>,
        "clone": <>,
        "test_ops_parallely": <>
    }
    """
    # pdb.set_trace()
    rbd = kw.get("rbd")
    pool = kw.get("pool")
    image = kw.get("image")
    snap_name = kw.get("snap")
    clone = kw.get("clone")
    test_ops_parallely = kw.get("test_ops_parallely")
    clone_spec = {
        "source-snap-spec": f"{pool}/{image}@{snap_name}",
        "dest-image-spec": f"{pool}/{clone}",
    }
    out, err = rbd.clone(**clone_spec)
    if out or err and "100% complete" not in out + err:
        log.error(f"Clone creation failed for {pool}/{clone}")
        if test_ops_parallely:
            raise Exception(f"Clone creation failed for {pool}/{clone}")
        return 1

    out, err = rbd.children(pool=pool, image=image, snap=snap_name, format="json")
    if err:
        log.error(f"Fetching children for snap {pool}/{image}@{snap_name} failed")
        if test_ops_parallely:
            raise Exception(
                f"Fetching children for snap {pool}/{image}@{snap_name} failed"
            )
        return 1

    children = json.loads(out)
    if not [child.get("image") for child in children if child.get("image") == clone]:
        log.error(f"Clone {pool}/{clone} doesn't exist after creation")
        if test_ops_parallely:
            raise Exception(f"Clone {pool}/{clone} doesn't exist after creation")
        return 1

    log.info(f"Clone {pool}/{clone} creation successful")
    return 0


def create_snap_and_clones(**kw):
    """
    Create snap and clones based on the input specified for the given image

    Args: kw{
        "rbd": <>,
        "pool": <>,
        "image": <>,
        "snap_spec": <snaps and clones to be created>
                        Ex: {
                            "snap_1": ["clone_11", "clone_12",..],
                        }
        "test_parallely": <>
    }
    """
    # pdb.set_trace()
    rbd = kw.get("rbd")
    pool = kw.get("pool")
    image = kw.get("image")
    snap_spec = kw.get("snap_spec")
    test_ops_parallely = kw.get("test_ops_parallely")

    snap_name = list(snap_spec.keys())[0]
    clones = snap_spec.get(snap_name)

    out, err = rbd.snap.create(pool=pool, image=image, snap=snap_name)
    if out or err and "100% complete" not in out + err:
        log.error(f"Snap creation failed for {pool}/{image}@{snap_name}")
        if test_ops_parallely:
            raise Exception(f"Snap creation failed for {pool}/{image}@{snap_name}")
        return 1

    if not snap_exists(rbd=rbd, pool=pool, image=image, snap_name=snap_name):
        log.error(f"Snapshot {snap_name} does not exist in snap ls for {pool}/{image}")
        return 1

    if clones:
        out, err = rbd.snap.protect(pool=pool, image=image, snap=snap_name)
        if out or err and "100% complete" not in out + err:
            log.error(f"Snap protect failed for {pool}/{image}@{snap_name}")
            if test_ops_parallely:
                raise Exception(f"Snap protect failed for {pool}/{image}@{snap_name}")
            return 1

    if test_ops_parallely:
        with parallel() as p:
            for clone in clones:
                p.spawn(
                    create_clone_and_verify,
                    rbd=rbd,
                    pool=pool,
                    image=image,
                    snap=snap_name,
                    clone=clone,
                    test_ops_parallely=test_ops_parallely,
                )
    else:
        for clone in clones:
            rc = create_clone_and_verify(
                rbd=rbd, pool=pool, image=image, snap=snap_name, clone=clone
            )
            if rc:
                log.error(f"Clone creation failed for {pool}/{clone}")
                return 1

    return 0


def create_snaps_and_clones(**kw):
    """
    Create snap and clones based on the input specified for the given image

    Args: kw{
        "rbd": <>,
        "pool": <>,
        "image": <>,
        "image_conf": <snaps and clones to be created>
                        Ex: {
                            "snap_1": ["clone_11", "clone_12",..],
                        }
        "test_parallely": <>
    }
    """
    # pdb.set_trace()
    rbd = kw.get("rbd")
    pool = kw.get("pool")
    image = kw.get("image")
    snaps_spec = kw.get("image_conf", {}).get("snap_spec")
    test_ops_parallely = kw.get("test_ops_parallely")

    if test_ops_parallely:
        with parallel() as p:
            for snap, clones in snaps_spec.items():
                p.spawn(
                    create_snap_and_clones,
                    rbd=rbd,
                    pool=pool,
                    image=image,
                    snap_spec={snap: clones},
                    test_ops_parallely=test_ops_parallely,
                )
    else:
        for snap, clones in snaps_spec.items():
            rc = create_snap_and_clones(
                rbd=rbd, pool=pool, image=image, snap_spec={snap: clones}
            )
            if rc:
                log.error(f"Creation of snaps and clones failed for {pool}/{image}")
                return 1

    return 0


def is_snap_present(**kw):
    """
    Check if the given image has atleast one snapshot

    Args:
        kw:{
            "rbd": <>,
            "pool": <>,
            "image": <>,
            "test_ops_parallely": <>
        }
    """
    rbd = kw.get("rbd")
    pool = kw.get("pool")
    image = kw.get("image")
    test_ops_parallely = kw.get("test_ops_parallely")

    out, err = rbd.snap.ls(pool=pool, image=image, format="json")
    if err:
        log.error(f"Error while fetching snapshots for image {pool}/{image}")
        if test_ops_parallely:
            raise Exception(f"Error while fetching snapshots for image {pool}/{image}")
        return 1

    snaps = json.loads(out)
    if len(snaps) > 0:
        return True
    else:
        return False


def is_children_present(**kw):
    """
    Check if the given image has atleast one child

    Args:
        kw: {
            "rbd": <>,
            "pool": <>,
            "image": <>,
            "test_ops_parallely": <>
        }
    """
    rbd = kw.get("rbd")
    pool = kw.get("pool")
    image = kw.get("image")
    test_ops_parallely = kw.get("test_ops_parallely")

    out, err = rbd.children(pool=pool, image=image, format="json")
    if err:
        log.error(f"Error while fetching children for image {pool}/{image}")
        if test_ops_parallely:
            raise Exception(f"Error while fetching children for image {pool}/{image}")
        return 1

    children = json.loads(out)
    if len(children) > 0:
        return True
    else:
        return False


def get_images_without_snap_and_or_clone(**kw):
    """
    Get all images in a pool having zero snaps and/or clones

    Args:
        kw: {
            "rbd": <>,
            "pool": <>,
            "test_ops_parallely": <>
        }
    """
    rbd = kw.get("rbd")
    pool = kw.get("pool")
    test_ops_parallely = kw.get("test_ops_parallely")
    req_images = []

    out, err = rbd.ls(pool=pool, format="json")
    if err:
        log.error(f"Error while fetching images from pool {pool}: {err}")
        return 1
    images_in_pool = json.loads(out)
    for image in images_in_pool:
        if not (
            is_snap_present(
                rbd=rbd, pool=pool, image=image, test_ops_parallely=test_ops_parallely
            )
            or is_children_present(
                rbd=rbd, pool=pool, image=image, test_ops_parallely=test_ops_parallely
            )
        ):
            req_images.append(image)

    return req_images
