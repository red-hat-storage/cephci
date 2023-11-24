from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import getdict
from ceph.rbd.workflows.cleanup import cleanup
from utility.log import Log

log = Log(__name__)


def test_rbd_python_module(rbd_obj, **kw):
    """
    For each image, call rbd python module to create image,
    write data and read back and verify that the data matches.
    """
    for pool_type in rbd_obj.get("pool_types"):
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = getdict(rbd_config)
        for pool, pool_config in multi_pool_config.items():
            multi_image_config = getdict(pool_config)
            for image in multi_image_config.keys():
                log.info(f"Creating image and running IOs for image {pool}/{image}")
                client = kw.get("client")
                cmd = f"python3 rbd_python.py --pool {pool} --image {image}"
                cmd += " --image-size 4294967296 --conf-file /etc/ceph/ceph.conf"
                if pool_type == "ec_pool_config":
                    data_pool = pool_config.get("data_pool")
                    cmd += f" --data-pool {data_pool}"
                out, err = client.exec_command(cmd=cmd)
                if err or "don't match" in out:
                    log.error(out)
                    log.error(
                        f"Executing rbd python to create image, read and\
                               write data failed for image {pool}/{image}"
                    )
                    return 1
                log.info(out)
    return 0


def run(**kw):
    """Create random different data and perform writes using rbd python module
    which provides file access to rbd images and verify

    Pre-requisites :
    We need atleast one client node with ceph-common and fio packages,
    conf and keyring files

    Test cases covered -
    1) CEPH-83574791 - Create random different data and perform writes using rbd
    python module which provides file access to rbd images and verify

    Test Case Flow
    1. create RBD pool and image
    2. create random different data and perform writes using rbd python module
    which provides file access to rbd images and verify

    """
    log.info(
        "Running python rbd module image creation, write and read data - CEPH-83574791"
    )

    try:
        client = kw.get("ceph_cluster").get_ceph_object(role="client").node
        client.exec_command(cmd="sudo pip install docopt")
        client.exec_command(
            cmd="git clone --depth 1 https://github.com/red-hat-storage/cephci.git"
        )
        client.exec_command(cmd="cp cephci/ceph/rbd/workflows/rbd_python.py .")
        client.exec_command(cmd="rm -rf cephci")

        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        ret_val = test_rbd_python_module(rbd_obj=rbd_obj, client=client, **kw)
        if ret_val:
            log.error("python rbd module tests failed")
    except Exception as e:
        log.error(f"python rbd module tests failed with error {str(e)}")
        ret_val = 1
    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)
        client.exec_command(cmd="rm -f rbd_python.py")
    return ret_val
