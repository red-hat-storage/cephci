"""RBD Persistent write back cache.

Environment and limitations:
 - The cluster should have 5 nodes + 1 SSD cache node
 - cluster/global-config-file: config/quincy/upi/octo-5-node-env.yaml
 - Should be Bare-metal.

Support
- Configure cluster with PWL Cache.
- Only replicated pool supported, No EC pools.
- Cannot be executed in parallel, if it is same pool or image.
"""
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.rbd.rbd_peristent_writeback_cache import PersistentWriteAheadLog
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def run(ceph_cluster, **kw):
    """RBD persistent write back cache.

    Args:
        ceph_cluster: ceph cluster object
        **kw: test parameters

    Pre-requisites :
        - need client node with ceph-common package, conf and keyring files
        - FIO should be installed on the client.

    """
    log.info("Running PWL....")
    config = kw.get("config")
    rbd_obj = initial_rbd_config(**kw)["rbd_reppool"]
    cache_client = get_node_by_id(ceph_cluster, config["client"])
    pool = config["rep_pool_config"]["pool"]
    image = f"{config['rep_pool_config']['pool']}/{config['rep_pool_config']['image']}"

    config_level = config["level"]
    entity = "client"
    if config_level == "client":
        config_level = "global"
    elif config_level == "pool":
        entity = pool
    elif config_level == "image":
        entity = image

    pwl = PersistentWriteAheadLog(rbd_obj, cache_client, config.get("drive"))
    try:
        # Configure PWL
        pwl.configure_cache_client()
        pwl.configure_pwl_cache(
            config["rbd_persistent_cache_mode"],
            config_level,
            entity,
            config["cache_file_size"],
        )

        # Run FIO, validate cache file existence in self.pwl_path under cache node
        with parallel() as p:
            fio_args = config["fio"]
            fio_args["client_node"] = cache_client
            fio_args["long_running"] = True
            p.spawn(run_fio, **fio_args)
            pwl.check_cache_file_exists(
                image,
                fio_args.get("runtime", 120),
                **config,
            )

        return 0
    except Exception as err:
        log.error(err)
    finally:
        if config.get("cleanup"):
            pwl.remove_pwl_configuration(config_level, entity)
            rbd_obj.clean_up(pools=[pool])
            pwl.cleanup()

    return 1
