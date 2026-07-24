from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.utils import getdict
from ceph.rbd.workflows.cleanup import cleanup
from utility.log import Log

log = Log(__name__)


def test_image_remove_resize(rbd, pool, image, image_size, cluster_type):
    """
    Perform image remove and resize and verify the output based on
    whether its primary or secondary cluster
    """
    size_num = int(image_size[:-1])
    expand_size = str(2 * size_num) + "G"
    resize_config = {"image-spec": f"{pool}/{image}", "size": expand_size}
    out, err = rbd.resize(**resize_config)
    if cluster_type == "primary" and "100% complete" not in out + err:
        log.error(
            f"Image {image} expand failed in {cluster_type} cluster with error {out} {err}"
        )
        return 1
    elif cluster_type == "secondary" and "Read-only file system" not in out + err:
        log.error(
            f"Image {image} expand did not fail as expected in {cluster_type} cluster.\
                Error: {out} {err}"
        )
        return 1

    resize_config["size"] = image_size
    resize_config.update({"allow-shrink": True})
    out, err = rbd.resize(**resize_config)
    if cluster_type == "primary" and "100% complete" not in out + err:
        log.error(
            f"Image {image} shrink failed in {cluster_type} cluster with error {out} {err}"
        )
        return 1
    elif (
        cluster_type == "secondary"
        and "new size is equal to original size" not in out + err
    ):
        log.error(
            f"Image {image} shrink did not fail as expected in {cluster_type} cluster\
                Error: {out} {err}"
        )
        return 1

    remove_config = {"image-spec": f"{pool}/{image}"}
    out, err = rbd.rm(**remove_config)
    if cluster_type == "primary" and "100% complete" not in out + err:
        log.error(
            f"Image {image} remove failed in {cluster_type} cluster with error {out} {err}"
        )
        return 1
    elif (
        cluster_type == "secondary" and "mirrored image is not primary" not in out + err
    ):
        log.error(
            f"Image {image} remove did not fail as expected in {cluster_type} cluster\
                Error: {out} {err}"
        )
        return 1
    return 0


def test_image_operations(**kw):
    """
    Test image operations like expand/shrink/remove in primary
    and secondary clusters
    """
    for pool_type in kw.get("pool_types"):
        config = kw.get("config", {}).get(pool_type)
        multi_pool_config = getdict(config)
        for pool, pool_config in multi_pool_config.items():
            multi_image_config = getdict(pool_config)
            for image, image_config in multi_image_config.items():
                image_size = image_config.get("size")

                test_obj = {
                    "secondary": kw.get("sec_obj"),
                    "primary": kw.get("rbd_obj"),
                }
                for cluster_type, rbd in test_obj.items():
                    log.info(
                        f"Testing image expand/shrink/remove on {cluster_type} cluster for image {image}"
                    )
                    ret_val = test_image_remove_resize(
                        rbd=rbd,
                        pool=pool,
                        image=image,
                        image_size=image_size,
                        cluster_type=cluster_type,
                    )
                    if ret_val:
                        log.error(
                            f"Image expand/shrink/remove did not work as expected on \
                                  {cluster_type} cluster for image {image}"
                        )
                        return 1
                    log.info(
                        f"Image expand/shrink/remove completed successfully on {cluster_type} \
                             cluster for image {image}"
                    )
                return 0
    return 0


def run(**kw):
    """Verify image operations from primary and secondary cluster
    Pre-requisites :
    We need atleast one client node with ceph-common, fio and rbd-nbd packages,
    conf and keyring files in both clusters with snapshot based RBD mirroring
    enabled between the clusters.
    Test cases covered -
    1) CEPH-83574857 - Configure two-way rbd-mirror (replicated and ec pool) on
    Stand alone CEPH cluster on image with snapshot based mirroring and perform
    image operations like expand/shrink/remove from primary and secondary clusters.
    kw:
        config:
            rep_pool_config:
              num_pools: 1
              num_images: 10
              mode: pool
              mirrormode: snapshot
            ec_pool_config:
              num_pools: 1
              num_images: 10
              mode: pool
              mirrormode: snapshot

    Test Case Flow
    1. Bootstrap two CEPH clusters and create a pool and 10 images each for replicated and ec pools
    2. Setup snapshot based mirroring in between these clusters.
    3. Run IOs on the Primary cluster for the image.
    4. Expand the image while IO is in progress in primary cluster
    5. Shrink the image while IO is in progress in primary cluster
    6. Remove the image while IO is in progress in primary cluster
    7. Try expand/shrink/remove operation from secondary cluster
    """
    pool_types = ["rep_pool_config", "ec_pool_config"]
    log.info(
        "CEPH-83574857 - Running verify image operations from primary and secondary cluster"
    )
    try:
        ret_val = 0
        kw.get("config", {})["do_not_run_io"] = True
        mirror_obj = initial_mirror_config(**kw)
        cluster_name = kw["ceph_cluster"].name
        sec_cluster_name = [
            k for k in kw.get("ceph_cluster_dict").keys() if k != cluster_name
        ][0]
        pool_types = mirror_obj.get(cluster_name).get("pool_types")
        rbd_obj = mirror_obj.get(cluster_name).get("rbd")
        sec_obj = mirror_obj.get(sec_cluster_name).get("rbd")
        ret_val = test_image_operations(
            rbd_obj=rbd_obj, sec_obj=sec_obj, pool_types=pool_types, **kw
        )
        if ret_val:
            log.error(
                "Testing image operations from primary and secondary cluster failed"
            )
    except Exception as e:
        log.error(
            f"Testing image operations from primary and secondary cluster\
             failed with error {str(e)}"
        )
        ret_val = 1
    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)
    return ret_val
