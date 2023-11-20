"""RBD Persistent write back cache, Test performance with/without cache enabled
Test Case Covered:
CEPH-83574711 - verify the performance with and without write back cache in SSD mode
Steps :
1) Create a pool and an image.
2) Enable client, pool and image level pwl cache on the pool.
3) Mount image and write some amount of data
4) Note time taken for data write
5) Disable pwl cache setting
6) Write same amount of data as before
7) Note time taken for data write
8) Verify that pwl cache enabled image takes lesser time to write data
Environment and limitations:
 - The cluster should have 5 nodes + 1 SSD cache node
 - cluster/global-config-file: config/quincy/upi/octo-5-node-env.yaml
 - Should be Bare-metal.
Support
- Configure cluster with PWL Cache.
- Only replicated pool supported, No EC pools.
"""
import datetime
import re
from time import sleep

from ceph.utils import get_node_by_id
from tests.rbd.krbd_io_handler import krbd_io_handler
from tests.rbd.rbd_peristent_writeback_cache import (
    PersistentWriteAheadLog,
    device_cleanup,
    get_entity_level,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

log = Log(__name__)


def write_data_compare_time_taken(cache, cfg, client):
    """
    Write data to an image with/without cache and
    compare the time taken to write data
    Args:
        cache: PersistentWriteAheadLog object
        cfg: test config
        client: cache client node
    """
    try:
        config_level, entity = get_entity_level(cfg)
        cache.configure_pwl_cache(
            cfg["rbd_persistent_cache_mode"],
            config_level,
            entity,
            cfg["cache_file_size"],
        )

        mount_path_1 = f"/tmp/mnt_{cache.rbd.random_string(len=5)}"
        io_config = {
            "rbd_obj": cache.rbd,
            "client": client,
            "size": cfg["rep_pool_config"]["size"],
            "do_not_create_image": True,
            "config": {
                "file_size": cfg["rep_pool_config"]["io_size"],
                "file_path": [f"{mount_path_1}/file_1"],
                "get_time_taken": True,
                "image_spec": [cfg["image_spec"]],
                "operations": {
                    "fs": "ext4",
                    "io": True,
                    "mount": True,
                    "nounmap": True,
                    "device_map": True,
                },
                "skip_mkfs": False,
                "cmd_timeout": 2400,
            },
        }
        krbd_io_handler(**io_config)

        out_with_cache = io_config["config"].get("time_taken", "")

        device_cleanup(cache.rbd)

        image_status = cache.rbd.image_status(image_spec=cfg["image_spec"], output=True)

        while "Watchers: none" not in image_status:
            sleep(10)
            image_status = cache.rbd.image_status(
                image_spec=cfg["image_spec"], output=True
            )

        cache.remove_pwl_configuration(config_level, entity)
        cache.cleanup()

        io_config["config"]["file_path"] = [f"{mount_path_1}/file_2"]
        io_config["config"]["operations"]["nounmap"] = True
        io_config["config"]["skip_mkfs"] = True

        krbd_io_handler(**io_config)

        out_without_cache = io_config["config"].get("time_taken", "")

        regex = r"(\d{1,2})m(\d{1,2}).(\d{1,3})s"

        time_with_cache_in_str = re.findall(regex, out_with_cache[1], flags=re.I)
        time_without_cache_in_str = re.findall(regex, out_without_cache[1], flags=re.I)

        if time_with_cache_in_str and time_without_cache_in_str:
            time_with_cache_in_str, time_without_cache_in_str = (
                time_with_cache_in_str[0],
                time_without_cache_in_str[0],
            )
            time_with_cache_in_str = [int(i) for i in time_with_cache_in_str]
            time_without_cache_in_str = [int(i) for i in time_without_cache_in_str]
            time_with_cache = datetime.time(0, *time_with_cache_in_str)
            time_without_cache = datetime.time(0, *time_without_cache_in_str)
            log.info(
                f"Time taken to write 1G data with cache: {time_with_cache_in_str}"
            )
            log.info(
                f"Time taken to write 1G data without cache: {time_without_cache_in_str}"
            )
            if time_without_cache > time_with_cache:
                log.info("Time taken to write without cache is greater than with cache")
                log.info("Performance is better with cache enabled")
            else:
                log.error("Time taken to write without cache is lesser than with cache")
                raise Exception("Performance validation with/without cache failed")
        else:
            log.error("Error fetching time taken to write")
            raise Exception("Performance validation with/without cache failed")

    except Exception as e:
        log.error(f"Performance validation with/without cache failed with error: {e}")
        raise Exception(
            f"Performance validation with/without cache failed with error: {e}"
        )
    finally:
        device_cleanup(cache.rbd)


def run(ceph_cluster, **kw):
    """Test performance with/without cache enabled. persistent write cache
    Args:
        ceph_cluster: ceph cluster object
        **kw: test parameters
    Pre-requisites :
        - need client node with ceph-common package, conf and keyring files
        - FIO should be installed on the client.
    """
    log.info("Running PWL....")
    log.info(
        "Running test - CEPH-83574711 Test performance with/without \
            cache enabled. persistent write cache"
    )
    config = kw.get("config")
    for level in config.get("levels"):
        cache_client = get_node_by_id(ceph_cluster, config["client"])
        kw["ceph_client"] = cache_client
        rbd_obj = initial_rbd_config(**kw)["rbd_reppool"]
        pool = config["rep_pool_config"]["pool"]
        image = (
            f"{config['rep_pool_config']['pool']}/{config['rep_pool_config']['image']}"
        )
        config["image_spec"] = image

        pwl = PersistentWriteAheadLog(rbd_obj, cache_client, config.get("drive"))

        config["level"] = level

        try:
            # Configure PWL
            pwl.configure_cache_client()

            write_data_compare_time_taken(pwl, config, cache_client)
        except Exception as err:
            log.error(err)
            return 1
        finally:
            if config.get("cleanup"):
                rbd_obj.clean_up(pools=[pool])
    return 0
