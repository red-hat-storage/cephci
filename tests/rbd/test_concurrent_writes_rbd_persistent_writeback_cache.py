"""RBD Persistent write back cache, Test concurrent writes to same image

Test Case Covered:
CEPH-83574720 - 
Concurrent writes to check exclusive lock feature with persistent cache enables via same or different IO tools in SSD mode

Steps : 
1) Setup persistent write cache for an image
2) Write data to the same image concurrently using two or more long running fio jobs
3) Verify that data written is not corrupted using rbd status command

Environment and limitations:
 - The cluster should have 5 nodes + 1 SSD cache node
 - cluster/global-config-file: config/quincy/upi/octo-5-node-env.yaml
 - Should be Bare-metal.

Support
- Configure cluster with PWL Cache.
- Only replicated pool supported, No EC pools.
"""
import json
import pdb

from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.rbd.rbd_peristent_writeback_cache import (
    PersistentWriteAheadLog,
    PWLException,
    fio_ready,
    get_entity_level,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio
from tests.rbd.krbd_io_handler import krbd_io_handler

log = Log(__name__)


def unmount(rbd, mount_point):
    """ """
    flag = 0
    umount_cmd = f"umount -f {mount_point}"
    # if kw.get("read_only"):
    #     umount_cmd += " -o ro,noload"
    if rbd.exec_cmd(cmd=umount_cmd, sudo=True):
        log.error(f"Umount failed for {mount_point}")
        flag = 1

    if rbd.exec_cmd(cmd=f"rm -rf {mount_point}", sudo=True):
        log.error(f"Remove dir failed for {mount_point}")
        flag = 1

    return flag


def device_cleanup(rbd, **kw):
    """
    """
    cmd = "lsblk --include 43 --json"
    out = rbd.exec_cmd(cmd=cmd, sudo=True, output=True)
    if out:
        nbd_devices = json.loads(out)

        for devices in nbd_devices.get("blockdevices"):
            device_name = f"/dev/{devices.get('name')}"
            mount_point = devices.get("mountpoint")
            unmount(rbd=rbd, mount_point=mount_point)
            if rbd.device_map(
                "unmap",
                f"{device_name}",
                kw.get("device_type", "nbd"),
            ):
                log.error(
                    f"Device unmap failed for {device_name} "
                )


def validate_concurrent_writes(cache, cfg, client):
    """
    Perform concurrent writes to an image with exclusive lock
    and persistent write back cache enabled, and verify

    Args:
        cache: PersistentWriteAheadLog object
        cfg: test config
        client: cache client node
    """
    try:
        pdb.set_trace()
        config_level, entity = get_entity_level(cfg)
        pool, image = cfg["image_spec"].split("/")
        cache.rbd.toggle_image_feature(
            pool, image, feature_name="object-map,exclusive-lock", action="enable"
        )
        cache.configure_pwl_cache(
            cfg["rbd_persistent_cache_mode"],
            config_level,
            entity,
            cfg["cache_file_size"],
        )

        io_config = {
            "rbd_obj": cache.rbd,
            "client": client,
            # "read_only": False,
            "size": "10G",
            "do_not_create_image": True,
            "config": {
                "file_size": "100M",
                "image_spec": [cfg["image_spec"]],
                "operations": {
                    "fs": "ext4",
                    "io": False,
                    "mount": True,
                    "nounmap": False,
                    "device_map": True,
                },
                "skip_mkfs": False,
                # "runtime": 30,
                # "encryption_config": [],
            },
        }
        pdb.set_trace()
        krbd_io_handler(**io_config)

        io_config["config"]["operations"]["io"] = True
        io_config["config"]["operations"]["nounmap"] = True
        io_config["config"]["skip_mkfs"] = True

        with parallel() as p:
            # p.spawn(run_fio, **fio_ready(cfg, client, test_name="test-1", size="100M"))
            # p.spawn(run_fio, **fio_ready(cfg, client, test_name="test-2", size="100M"))
            p.spawn(krbd_io_handler, **io_config)
            p.spawn(krbd_io_handler, **io_config)
            try:
                cache.check_cache_file_exists(
                    cfg["image_spec"],
                    cfg["fio"].get("runtime", 120),
                    **cfg,
                )
                log.info(
                    "PWL Cache file created with exclusive lock for concurrent writes..."
                )
            except PWLException as error:
                log.error(f"{error} in entire FIO execution...")
                raise Exception(error)

        pdb.set_trace()
        cmd = f"rbd -p {pool} du --format json"
        pool_info = json.loads(cache.rbd.exec_cmd(cmd=cmd, output=True))
        used_size = [
            img["used_size"] for img in pool_info.get("images", []) if img["name"] == image
        ][0]
        used_size = used_size / 1048576  # in MB
        if int(used_size) != 200:  # 1024:
            log.error("Writing two 100MB fios but the used size of image is not 200MB")
            raise Exception(
                "Writing two 100MB fios but the used size of image is not 200MB"
            )
    except Exception as e:
        log.error(f"Validation of concurrent writes failed with error: {e}")
        raise Exception(f"Validation of concurrent writes failed with error: {e}")
    finally:
        device_cleanup(cache.rbd)


def run(ceph_cluster, **kw):
    """Concurrent writes with exclusive lock. persistent write cache

    Args:
        ceph_cluster: ceph cluster object
        **kw: test parameters

    Pre-requisites :
        - need client node with ceph-common package, conf and keyring files
        - FIO should be installed on the client.

    """
    pdb.set_trace()
    log.info("Running PWL....")
    log.info(
        "Running test - Concurrent writes with exclusive lock. persistent write cache"
    )
    config = kw.get("config")
    rbd_obj = initial_rbd_config(**kw)["rbd_reppool"]
    cache_client = get_node_by_id(ceph_cluster, config["client"])
    pool = config["rep_pool_config"]["pool"]
    image = f"{config['rep_pool_config']['pool']}/{config['rep_pool_config']['image']}"
    config["image_spec"] = image

    pwl = PersistentWriteAheadLog(rbd_obj, cache_client, config.get("drive"))
    config_level, entity = get_entity_level(config)

    try:
        # Configure PWL
        pwl.configure_cache_client()

        validate_concurrent_writes(pwl, config, cache_client)

        return 0
    except Exception as err:
        log.error(err)
    finally:
        if config.get("cleanup"):
            pwl.remove_pwl_configuration(config_level, entity)
            rbd_obj.clean_up(pools=[pool])
            pwl.cleanup()

    return 1
