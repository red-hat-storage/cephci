"""RBD Persistent write back cache with cluster operations.

Test Case Covered:
CEPH-83574891 - Verify ceph cluster operations with persistent
cache enabled in SSD mode with IO in-progress.

Steps :
1) Setup persistent write cache for an image
2) Write data to the image using fio jobs check cache status
3) Stop writing data to the image by killing the fio process, then start again
4) Perform cluster operation like restarting mon, osd, overall ceph.target
5) Remove mon node then add it back while IO keeps going
6) Remove osd node then add it back while IO keeps going
7) Initiate OSD scrub operation while IO keeps going
8) check ceph health and data corruption every time while IO keeps going
9) Repeat the test on client, pool and image level

Pre-requisites :
- need client node with ceph-common package, conf and keyring files
- FIO should be installed on the client.

Environment and limitations:
- The cluster should have 5 nodes + 1 SSD cache node
- cluster/global-config-file: config/quincy/upi/octo-5-node-env.yaml
- Should be Bare-metal.

Support
- Configure cluster with PWL Cache.
- Only replicated pool supported, No EC pools."""


import time

from ceph.ceph_admin import CephAdmin
from ceph.parallel import parallel
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.monitor_workflows import MonitorWorkflows
from ceph.rbd.workflows.cluster_operations import (
    check_health,
    operation,
    osd_remove_and_add_back,
    restart_ceph_target,
    restart_osd,
)
from tests.rbd.rbd_peristent_writeback_cache import (
    PersistentWriteAheadLog,
    fio_ready,
    get_entity_level,
    kill_fio,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)


def verify_cache(pwl, config, cache_client, image_spec):
    """Method to verify rbd persistant cache test.

    Args:
        pwl (PersistentWriteAheadLog): The PersistentWriteAheadLog instance.
        config (dict): The test configuration.
        cache_client (Node): The client node for cache testing.
        image_spec (str): The image specification for testing.
    """
    log.info("Writing data to the image using FIO jobs and checking cache status")
    with parallel() as p:
        p.spawn(
            run_fio, **fio_ready(config, cache_client, verify="crc32", verify_fatal=1)
        )
        p.spawn(
            pwl.check_cache_file_exists,
            image_spec,
            config["fio"].get("runtime", 30),
            **config,
        )


def run(ceph_cluster, **kw):
    """RBD Persistent write back cache with cluster operations.

    Args:
        ceph_cluster: ceph cluster object
        **kw: test parameters
    """
    log.info(
        "Running test - CEPH-83574891 - Verify ceph cluster operations "
        "with persistent cache enabled in SSD mode with IO in-progress"
    )
    config = kw.get("config")
    for level in config.get("levels"):
        config["level"] = level
        rbd_obj = initial_rbd_config(**kw)["rbd_reppool"]
        pool = config["rep_pool_config"]["pool"]
        image_spec = (
            f"{config['rep_pool_config']['pool']}/{config['rep_pool_config']['image']}"
        )
        cache_client = ceph_cluster.get_nodes(role="client")[0]
        admin_node = ceph_cluster.get_nodes(role="_admin")[0]
        cephadm = CephAdmin(cluster=ceph_cluster, **config)
        rados_obj = RadosOrchestrator(node=cephadm)
        mon_obj = MonitorWorkflows(node=cephadm)
        pwl = PersistentWriteAheadLog(rbd_obj, cache_client, config.get("drive"))
        config_level, entity = get_entity_level(config)

        try:
            # Configute cache client
            pwl.configure_cache_client()

            # Configure PWL
            pwl.configure_pwl_cache(
                config["rbd_persistent_cache_mode"],
                config_level,
                entity,
                config["cache_file_size"],
            )

            # Test to verify persistent write back cache test
            verify_cache(pwl, config, cache_client, image_spec)

            log.info(
                "Test to kill FIO process and restarting FIO to check cache status"
            )
            with parallel() as p:
                p.spawn(run_fio, **fio_ready(config, cache_client))
                p.spawn(kill_fio, pwl.rbd)

            verify_cache(pwl, config, cache_client, image_spec)

            # Parallel execution of cluster operations with corresponding verify cache tests
            with parallel() as p:
                log.info("Test to restart mon service along with cache test")
                p.spawn(operation, rados_obj, "restart_daemon_services", daemon="mon")
                p.spawn(verify_cache, pwl, config, cache_client, image_spec)

            with parallel() as p:
                log.info("Test to restart osd service along with cache test")
                p.spawn(restart_osd, client_node=cache_client)
                p.spawn(verify_cache, pwl, config, cache_client, image_spec)

            with parallel() as p:
                log.info(
                    "Test to restart ceph target service along with pwl cache test"
                )
                p.spawn(restart_ceph_target, admin_node=admin_node)
                p.spawn(verify_cache, pwl, config, cache_client, image_spec)

            mon_host = ceph_cluster.get_nodes(role="mon")[0]
            with parallel() as p:
                log.info("Test to remove mon service along with pwl cache test")
                cmd = f"ceph orch host label rm {mon_host.hostname} mon"
                out, err = cache_client.exec_command(cmd=cmd, sudo=True)

                # Check for errors
                if err:
                    raise Exception(f"ceph mon remove command failed as {err}")

                p.spawn(
                    operation, mon_obj, "remove_mon_service", host=mon_host.hostname
                )
                p.spawn(verify_cache, pwl, config, cache_client, image_spec)

            with parallel() as p:
                log.info(
                    "Test to add the mon service back to the cluster with pwl cache test"
                )
                cmd = f"ceph orch host label add {mon_host.hostname} mon"
                out, err = cache_client.exec_command(cmd=cmd, sudo=True)

                # Check for errors
                if err:
                    raise Exception(f"ceph mon add command failed as {err}")

                time.sleep(10)
                p.spawn(
                    operation,
                    mon_obj,
                    "check_mon_exists_on_host",
                    host=mon_host.hostname,
                )
                p.spawn(verify_cache, pwl, config, cache_client, image_spec)

            with parallel() as p:
                log.info(
                    "Test to remove the osd service and add back to the cluster with pwl cache test"
                )
                p.spawn(
                    osd_remove_and_add_back,
                    ceph_cluster=ceph_cluster,
                    rados_obj=rados_obj,
                    pool=pool,
                )
                p.spawn(verify_cache, pwl, config, cache_client, image_spec)

            with parallel() as p:
                log.info("Test to initiate OSD scrub operation with pwl cache test")
                p.spawn(operation, rados_obj, "run_scrub")
                p.spawn(verify_cache, pwl, config, cache_client, image_spec)

            # Test to check ceph health
            check_health(cache_client)

        except Exception as err:
            log.error(err)
            return 1

        finally:
            if config.get("cleanup"):
                pwl.remove_pwl_configuration(config_level, entity)
                rbd_obj.clean_up(pools=[pool])
                pwl.cleanup()

    return 0
