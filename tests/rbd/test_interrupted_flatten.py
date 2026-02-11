import json
import random

from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import get_md5sum_rbd_image, getdict
from ceph.rbd.workflows.cleanup import cleanup
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def interrupted_flatten_test(rbd, client, pool_type, **kw):
    """
    Test to verify interrupted flatten operations.
    Args:
        rbd: Rbd object from cli.rbd.rbd
        client: Client node object
        pool_type: Type of pool (rep_pool_config or ec_pool_config)
        kw: Configuration dictionary
    Returns:
        0 if test is successful, 1 otherwise
    """
    try:
        config = kw.get("config")
        pool_type_config = config.get(pool_type)

        pool_config = getdict(pool_type_config)
        pool_name = list(pool_config.keys())[0]

        image_config = getdict(pool_config.get(pool_name))
        image_name = list(image_config.keys())[0]

        image_spec = f"{pool_name}/{image_name}"
        snap = f"{image_name}_snap"
        snap_spec = f"{image_spec}@{snap}"
        clone = f"{image_name}_clone"
        clone_spec = f"{pool_name}/{clone}"

        # Write data to image
        run_fio(
            client_node=client,
            pool_name=pool_name,
            image_name=image_name,
            size=config.get("io_size", "500M"),
            io_type="write",
        )

        # Calculate MD5 sum of original image
        md5_parent = get_md5sum_rbd_image(
            image_spec=image_spec,
            file_path=f"/tmp/{image_name}_original",
            rbd=rbd,
            client=client,
        )
        log.info(f"MD5 sum of original image: {md5_parent}")

        # Create and protect snapshot
        snap_config = {"snap-spec": snap_spec}
        out, err = rbd.snap.create(**snap_config)

        if out or err and "100% complete" not in err:
            log.error(f"Snapshot creation failed for {snap_spec}")
            return 1

        out, err = rbd.snap.protect(**snap_config)
        if out or err:
            log.error(f"Snapshot protect failed for {snap_spec}")
            return 1

        # Loop for multiple iterations
        iterations = config.get("iterations", 1)
        for i in range(iterations):
            log.info(f"Iteration {i+1} of {iterations}")

            clone_config = {
                "source-snap-spec": snap_spec,
                "dest-image-spec": clone_spec,
            }

            # Create clone of an iamge
            out, err = rbd.clone(**clone_config)
            if out or err:
                log.error(f"Clone creation failed for {clone_spec}")
                return 1

            log.info("Starting flatten and interrupting it...")

            # Run flatten in background, then kill it.
            cmd = f"nohup rbd flatten {clone_spec} > /dev/null 2>&1 & pid=$!; kill -9 $pid || true"
            client.exec_command(cmd=cmd, sudo=True)

            info_spec = {"image-or-snap-spec": clone_spec, "format": "json"}

            # Verify if flatten was actually interrupted using rbd info
            out, err = rbd.info(**info_spec)
            image_info = json.loads(out)
            if "parent" not in image_info:
                log.warning("Flatten finished too quickly")
            else:
                log.info(
                    "Verified: Image still has a parent. Flatten was successfully interrupted."
                )

            # Resume flatten
            if "parent" in image_info:
                flatten_config = {"image-spec": clone_spec}
                out, err = rbd.flatten(**flatten_config)
                if "100% complete...done" not in out + err:
                    log.error(f"Flatten clone failed for {clone_spec} with error {err}")
            else:
                log.info("Image already flattened")

            # Verify flatten completed (parent should be gone)
            out, err = rbd.info(**info_spec)
            if "parent" in json.loads(out):
                log.error("Image still has parent after resume flatten")
                return 1
            log.info("Verified: Image has no parent after resume flatten")

            # Calculate MD5 sum after flatten
            md5_clone = get_md5sum_rbd_image(
                image_spec=clone_spec,
                file_path=f"/tmp/{clone}_after",
                rbd=rbd,
                client=client,
            )
            log.info(f"MD5 sum of cloned image: {md5_clone}")

            if md5_parent != md5_clone:
                log.error(
                    f"Data mismatch! Original image: {md5_parent}, Cloned image: {md5_clone}"
                )
                return 1
            log.info("Data is consistent after flatten the clone")

            # Cleanup clone for next iteration
            remove_config = {"image-spec": clone_spec}
            rbd.rm(**remove_config)

        # Cleanup snapshot and image (outside loop)
        rbd.snap.unprotect(snap_spec=snap_spec)
        rbd.snap.purge(image_spec=image_spec)

        return 0

    except Exception as e:
        log.error(f"Exception in interrupted_flatten_test: {e}")
        return 1


def run(**kw):
    """
    This test verifies the integrity of interrupted flatten operations.

    Args:
        **kw: Keyword arguments

    Returns:
        0 if test is successful, 1 otherwise
    """
    log.info(
        "Running CEPH-83606284 Test to verify interrupted flatten operations integrity"
    )

    config = initial_rbd_config(**kw)
    rbd_obj = config.get("rbd")
    client = config.get("client")
    pool_types = config.get("pool_types")

    try:
        # Execute test either Replicated or EC pool
        pool_type = random.choice(pool_types)
        log.info(f"Running test on {pool_type}")

        rc = interrupted_flatten_test(rbd_obj, client, pool_type, **kw)
        if rc != 0:
            log.error(f"Test failed for {pool_type}")
            return 1
        log.info(f"Test passed for {pool_type}")

    except Exception as e:
        log.error(f"Test encountered unexpected exception: {e}")
        return 1

    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        obj = {cluster_name: config}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)

    return 0
