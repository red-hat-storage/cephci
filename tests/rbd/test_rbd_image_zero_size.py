"""Module to verify resize operation of RBD image with zero size.

Test case covered -
CEPH-83597243 -  Test to verify RBD image with zero size

Pre-requisites :
1. Cluster must be up and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

Test Case Flow:
1. Create a pool with name as 'images'
2. Create an image inside this pool with 0 size' (rbd create images/image_zero1 --size 0)
3. Create another image inside this pool with size 1024 (rbd create images/image_1024size --size 1024)
4. Use rbd du command to check that image size for both images
5. Resize second image size to 0
6. Remove that images

"""

import json

from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import getdict
from ceph.rbd.workflows.cleanup import cleanup
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def resize_image_with_zero_size(rbd_obj, client, **kw):
    """
        Test to verify RBD image with zero size.
    Args:
        rbd: RBD object
        pool_type: pool type (ec_pool_config or rep_pool_config)
        **kw: test data
    """
    kw["client"] = client
    rbd_op = Rbd(client)

    resize_to = kw["config"]["resize_to"]
    list_of_pool_types = rbd_obj.get("pool_types")
    log.info(f"pools in the cluster {list_of_pool_types}")
    for pool_type in rbd_obj.get("pool_types"):
        log.info(f"Executing for {pool_type}")
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = getdict(rbd_config)
        for pool, pool_config in multi_pool_config.items():
            if "data_pool" in pool_config.keys():
                _ = pool_config.pop("data_pool")
            for image, image_config in pool_config.items():
                if "1024" in image:
                    log.info(f"Resize Image {image} to zero")
                    resize_config = {
                        "image-spec": f"{pool}/{image}",
                        "size": resize_to,
                        "allow-shrink": True,
                    }
                    out, err = rbd_op.resize(**resize_config)
                    if "100% complete" not in out + err:
                        log.error(
                            f"Image {image} resize to 0 failed with error {out} {err}"
                        )
                log.info(f"Verify Image {image} size from image info")
                info_spec = {"image-or-snap-spec": f"{pool}/{image}", "format": "json"}
                out, err = rbd_op.info(**info_spec)
                if err:
                    log.error(f"Error while fetching info for image {image}")
                    return 1
                log.info(f"Image info: {out}")
                out_json = json.loads(out)
                if "0" not in str(out_json["size"]):
                    log.error(
                        f"Image size Verification failed for {image}: Image size should be zero"
                    )
                    return 1
                log.info(f"Verifying Image {image} size with du command")
                image_spec = pool + "/" + image
                image_config = {"image-spec": image_spec}
                out = rbd_op.image_usage(**image_config)
                image_data = out[0]
                image_size = image_data.split("\n")[1].split()[3].strip() + "G"
                log.info(f"Image size captured : {image_size}")
                if image_size != "0G":
                    log.error(
                        f"Image size Verification failed for {image}: Image size should be zero"
                    )
                    return 1
    return 0


def run(**kw):
    """Test to verify RBD image with zero size.
    This test verifies images with zero size
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    try:
        log.info("CEPH-83597243 -  Test to verify RBD image with zero size")

        if kw.get("client_node"):
            client = get_node_by_id(kw.get("ceph_cluster"), kw.get("client_node"))
        else:
            client = kw.get("ceph_cluster").get_nodes(role="client")[0]
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        if rbd_obj:
            log.info("Executing test on Replication and EC pool")
            if resize_image_with_zero_size(rbd_obj, client, **kw):
                return 1

    except Exception as e:
        log.error(f"Testing RBD image with Zero size failed: {str(e)}")
        return 1

    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)

    return 0
