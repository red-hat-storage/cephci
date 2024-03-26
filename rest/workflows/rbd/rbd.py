import math

from ceph.rbd.workflows.pool import check_pool_exists
from rest.endpoints.ceph import CEPH
from rest.endpoints.rbd.rbd import RBD
from utility.log import Log

log = Log(__name__)


def convert_to_bytes(size):
    """
    Convert size conventions to bytes
    Args:
        size(string): size in conventions Eg 4G
    Returns:
        bytes(int): bytes Eg 4294967296
    """
    size_unit = size[-1]
    size_number = size[:-1]
    if size_unit == "G":
        bytes = float(size_number) * 1024 * 1024 * 1024
    elif size_unit == "M":
        bytes = float(size_number) * 1024 * 1024
    elif size_unit == "K":
        bytes = float(size_number) * 1024
    return int(bytes)


def convert_to_size(size_bytes):
    """
    converts raw bytes to MB, GB etc
     Args:
         size_bytes(string): size in bytes Eg 4294967296
     Returns:
         Size(int): in G,M,K format Eg '4G'
    """
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "G", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s%s" % (int(s), size_name[i])


def create_image_in_pool_verify(**kw):
    """
    Workflow to create pool and image, followed by verifications
    Args:
        kw(dict): Pool and image REST data related kw paramaters
    """
    # Basic checks
    client = kw.get("client", None)
    pool = kw.get("pool", None)
    image_config = kw.get("image_config", None)
    if pool is None:
        log.error(f"Pool name is must {pool}")
        return 1
    if image_config is None:
        log.error(f"Image config is must {image_config}")
        return 1

    # 1. pool creation using REST API and verify using REST GET and REST GET list
    _rest = kw.pop("rest", None)
    ceph_rest = CEPH(rest=_rest)
    try:
        if kw.get("pool_type", None) == "rep_pool_config":
            pool_create_response = ceph_rest.create_pool(
                pool=pool,
                pool_type="replicated",
                rule_name="replicated_rule",
                pg_num=1,
                size=3,
                pg_autoscale_mode="on",
                application_metadata=["rbd"],
            )
            log.info(f"pool create response {pool_create_response}")
        elif kw.get("pool_type", None) == "ec_pool_config":
            data_pool = image_config.pop("data_pool", None)
            # Currently in REST API we can't use erasure type pool as data_pool
            # + pool_type="erasure", erasure_code_profile="default"
            # - size=3
            pool_create_response1 = ceph_rest.create_pool(
                pool=data_pool,
                pool_type="replicated",
                rule_name="replicated_rule",
                pg_num=1,
                size=3,
                pg_autoscale_mode="on",
                application_metadata=["rbd"],
            )
            log.info(f"pool create response {pool_create_response1}")
            pool_create_response2 = ceph_rest.create_pool(
                pool=pool,
                pool_type="replicated",
                rule_name="replicated_rule",
                pg_num=1,
                size=3,
                pg_autoscale_mode="on",
                application_metadata=["rbd"],
            )
            log.info(f"pool create response {pool_create_response2}")
        pool_get_response = ceph_rest.get_a_pool(pool=pool)
        if pool_get_response["pool_name"] != pool:
            log.error(f"Pool {pool} REST GET verification failed")
            return 1
        log.info(f"Pool {pool} REST GET verification successful")
        pool_list_response = ceph_rest.list_pool()
        found_pool = False
        for pool_details in pool_list_response:
            if pool_details["pool_name"] == pool:
                found_pool = True
                break
        if not found_pool:
            log.erorr(f"Pool {pool} REST list GET verification failed")
            return 1
        log.info(f"Pool {pool} REST list GET verification successful")

        # This is to demonstrate one can seamlessly add existing cli based checks, if needed
        check_pool_rc = check_pool_exists(client=client, pool_name=pool)
        if check_pool_rc:
            log.eror("Pool verification failed through cli")
            return 1
    except Exception as e:
        log.error(f"pool {pool} creation using REST failed with error {str(e)}")
        return 1

    # 2. image create using REST API
    log.info(f"Image config  for pool {pool} is {image_config}")
    rbd_rest = RBD(rest=_rest)
    try:
        for image, image_meta in image_config.items():
            if kw.get("pool_type", None) == "rep_pool_config":
                image_create_response = rbd_rest.create_image(
                    name=image,
                    pool_name=pool,
                    size=convert_to_bytes(image_meta["size"]),
                )
            elif kw.get("pool_type", None) == "ec_pool_config":
                image_create_response = rbd_rest.create_image(
                    name=image,
                    pool_name=pool,
                    data_pool=data_pool,
                    size=convert_to_bytes(image_meta["size"]),
                )
            log.info(f"Image create response {image_create_response}")
            image_get_response = rbd_rest.get_image(image_spec=f"{pool}%2F{image}")
            if image_get_response["name"] != image:
                log.error(f"Image {image} REST verification failed ")
                return 1
        return 0
    except Exception as e:
        log.error(f"Image create and/or validations failed {str(e)}")
        return 1
