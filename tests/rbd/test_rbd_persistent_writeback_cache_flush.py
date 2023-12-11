"""RBD Persistent write back cache, Test concurrent writes to same image

Test Case Covered:
CEPH-83574893 - Verify cache flush command when IO is stopped abruptly
with persistent cache enables via same or different IO tools in SSD mode

Steps :
1) Setup persistent write cache for an image
2) Write data to the image using fio jobs
3) Stop fio abruplty and verify that cache flush command

Environment and limitations:
 - The cluster should have 5 nodes + 1 SSD cache node
 - cluster/global-config-file: config/quincy/upi/octo-5-node-env.yaml
 - Should be Bare-metal.

Support
- Configure cluster with PWL Cache.
- Only replicated pool supported, No EC pools.
"""
import re
from time import sleep

from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.rbd.rbd_peristent_writeback_cache import (
    PersistentWriteAheadLog,
    fio_ready,
    get_entity_level,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def kill_fio(rbd):
    """
    Kill the fio process running on the client
    """
    sleep(60)
    cmd = "ps -ef | grep fio"
    out = rbd.exec_cmd(cmd=cmd, sudo=True, output=True)
    if out and re.findall(r"(fio).*(--name)", out, re.I):
        proc_id = re.search(r"\d+", out).group()
        cmd = f"kill -9 {proc_id}"
        rbd.exec_cmd(cmd=cmd, sudo=True)


def validate_cache_flush(cache, cfg, client):
    """
    Kill an IO process in between and perform cache flush

    Args:
        cache: PersistentWriteAheadLog object
        cfg: test config
        client: cache client node
    """
    config_level, entity = get_entity_level(cfg)

    cache.configure_pwl_cache(
        cfg["rbd_persistent_cache_mode"],
        config_level,
        entity,
        cfg["cache_file_size"],
    )

    with parallel() as p:
        p.spawn(run_fio, **fio_ready(cfg, client))
        p.spawn(kill_fio, cache.rbd)

    out, err = cache.flush(cfg["image_spec"])
    if not out:
        log.info(f"Cache flush for image {cfg['image_spec']} is success")
    else:
        log.error(f"Cache flush failed for image {cfg['image_spec']} with error {err}")
        raise Exception(
            f"Cache flush failed for image {cfg['image_spec']} with error {err}"
        )


def run(ceph_cluster, **kw):
    """Concurrent writes with exclusive lock. persistent write cache

    Args:
        ceph_cluster: ceph cluster object
        **kw: test parameters

    Pre-requisites :
        - need client node with ceph-common package, conf and keyring files
        - FIO should be installed on the client.

    """
    log.info(
        "Running test - Concurrent writes with exclusive lock. persistent write cache"
    )
    config = kw.get("config")
    cache_client = get_node_by_id(ceph_cluster, config["client"])
    kw["ceph_client"] = cache_client
    rbd_obj = initial_rbd_config(**kw)["rbd_reppool"]
    pool = config["rep_pool_config"]["pool"]
    image = f"{config['rep_pool_config']['pool']}/{config['rep_pool_config']['image']}"
    config["image_spec"] = image

    pwl = PersistentWriteAheadLog(rbd_obj, cache_client, config.get("drive"))
    config_level, entity = get_entity_level(config)

    try:
        # Configure PWL
        pwl.configure_cache_client()

        validate_cache_flush(pwl, config, cache_client)

        return 0
    except Exception as err:
        log.error(err)
    finally:
        if config.get("cleanup"):
            pwl.remove_pwl_configuration(config_level, entity)
            rbd_obj.clean_up(pools=[pool])
            pwl.cleanup()

    return 1
