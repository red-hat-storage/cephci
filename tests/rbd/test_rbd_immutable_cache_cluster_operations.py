"""Perform cluster-related operations along with
immutable object cache test parallel.

Pre-requisites :
We need atleast one client node with ceph-common and fio packages,
conf and keyring files

Test cases covered -
CEPH-83574132 - Immutable object cache with cluster operations.

Test Case Flow -

1)Create multiple rbd pool and multiple images using rbd commands
2)Write some data to the images using FIO
3)Create multiple clones images in different pools from the multiple parent image created
4)Read the cloned images of different pools in parallel and check the cache status
5) Restart the Mon, OSD, and cluster target and parallelly run the IO and cache status
6) Remove mon and add mon back to cluster with parallelly run the IO and cache status
8) check ceph health status
9) Perform test on both Replicated and EC pool
"""

import time

from test_rbd_immutable_cache import configure_immutable_cache

from ceph.ceph_admin import CephAdmin
from ceph.parallel import parallel
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.monitor_workflows import MonitorWorkflows
from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.cluster_operations import (
    check_health,
    operation,
    restart_ceph_target,
    restart_osd,
)
from ceph.rbd.workflows.rbd import create_snap_and_clone
from utility.log import Log
from utility.utils import run_fio

log = Log(__name__)

immutable_cache_path = "/mnt/immutable-cache/"


def verify_immutable_cache_objects(rbd, node, **kw):
    """
    Method to validate cache objects after configuring immutable cache daemon.

    Args:
        rbd: RBD object
        node: cache client node
        **kw: test data

    Returns:
        int: The return value. 0 for success, 1 for failure.
    """
    config = kw.get("config")
    image_spec = config.get("image_spec")
    pool = image_spec.split("/")[0]
    image = image_spec.split("/")[1]

    fio_args = {
        "client_node": node,
        "pool_name": pool,
        "image_name": image,
        "io_type": "write",
    }

    if kw:
        fio_args.update(kw)

    try:
        run_fio(**fio_args)

        # create snapshot, protect and clone it for the image
        snap = "image_snap" + random_string(len=5)
        clone = "image_clone" + random_string(len=5)
        snap_spec = f"{pool}/{image}@{snap}"
        clone_spec = f"{pool}/{clone}"

        create_snap_and_clone(rbd, snap_spec, clone_spec)

        clone_snap_spec = {"snap-spec": f"{pool}/{clone}@snap_clone_{image}"}
        out, err = rbd.snap.create(**clone_snap_spec)

        if out or err and "100% complete" not in err:
            log.error(f"Snapshot creation failed for {clone_snap_spec}")
            return 1

        # Read IO from cloned images to get cache files
        clone = clone_spec.split("/")[1]
        fio_args["image_name"] = clone
        fio_args["io_type"] = "read"

        run_fio(**fio_args)

        # waiting for cache files getting generated
        time.sleep(60)
        i = 0
        while i < 20:
            full_path = f"{immutable_cache_path}{i}"

            # Execute the command for the current iteration
            out = node.exec_command(cmd=f"ls {full_path}", sudo=True)

            # Check if "rbd_data" is present in the command output
            if "rbd_data" in out[0]:
                log.info(f"Cache files found in {full_path}: {out}")
                break

            i += 1

        # If no cache files are found, log an error
        else:
            log.debug("No cache files found in any directory")
            return 1
        return 0

    except Exception as err:
        log.error(err)
        return 1


def test_immutable_cache(rbd_obj, **kw):
    """
    Test the immutable object cache with cluster operations.

    Args:
        rbd_obj: RBD object
        **kw: test data

    Returns:
        int: The return value. 0 for success, 1 for failure.
    """

    config = kw.get("config")
    ceph_cluster = kw.get("ceph_cluster")
    client_node = ceph_cluster.get_nodes(role="client")[0]
    admin_node = ceph_cluster.get_nodes(role="_admin")[0]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonitorWorkflows(node=cephadm)

    try:
        if configure_immutable_cache(rbd_obj, client_node, "user1"):
            log.error("Immutable object cache configuration failed")
            return 1
        log.info("Immutable object cache configuration completed successfully.")

        for pool_type in rbd_obj.get("pool_types"):
            rbd_config = kw.get("config", {}).get(pool_type, {})
            multi_pool_config = getdict(rbd_config)

            for pool, pool_config in multi_pool_config.items():
                multi_image_config = getdict(pool_config)
                for image in multi_image_config.keys():
                    image_spec = f"{pool}/{image}"
                    config["image_spec"] = image_spec
                    rbd = rbd_obj.get("rbd")

                    with parallel() as p:
                        log.info("Test to restart mon service along with cache test")
                        p.spawn(
                            operation,
                            rados_obj,
                            "restart_daemon_services",
                            daemon="mon",
                        )
                        p.spawn(
                            verify_immutable_cache_objects,
                            rbd,
                            client_node,
                            **kw,
                        )

                    with parallel() as p:
                        log.info("Test to restart osd service along with cache test")
                        p.spawn(restart_osd, client_node=client_node)
                        p.spawn(
                            verify_immutable_cache_objects,
                            rbd,
                            client_node,
                            **kw,
                        )

                    with parallel() as p:
                        log.info(
                            "Test to restart ceph target service along with cache test"
                        )
                        p.spawn(restart_ceph_target, admin_node=admin_node)
                        p.spawn(
                            verify_immutable_cache_objects,
                            rbd,
                            client_node,
                            **kw,
                        )

                    mon_host = ceph_cluster.get_nodes(role="mon")[0]
                    with parallel() as p:
                        log.info("Test to remove mon service along with cache test")
                        cmd = f"ceph orch host label rm {mon_host.hostname} mon"
                        out, err = client_node.exec_command(cmd=cmd, sudo=True)

                        if err:
                            raise Exception(f"ceph mon remove command failed as {err}")

                        p.spawn(
                            operation,
                            mon_obj,
                            "remove_mon_service",
                            host=mon_host.hostname,
                        )
                        p.spawn(
                            verify_immutable_cache_objects,
                            rbd,
                            client_node,
                            **kw,
                        )

                    with parallel() as p:
                        log.info(
                            "Test to add the mon service back to the cluster with cache test"
                        )
                        cmd = f"ceph orch host label add {mon_host.hostname} mon"
                        out, err = client_node.exec_command(cmd=cmd, sudo=True)

                        if err:
                            raise Exception(f"ceph mon add command failed as {err}")

                        time.sleep(10)
                        p.spawn(
                            operation,
                            mon_obj,
                            "check_mon_exists_on_host",
                            host=mon_host.hostname,
                        )
                        p.spawn(
                            verify_immutable_cache_objects,
                            rbd,
                            client_node,
                            **kw,
                        )

                    check_health(client_node)
    except Exception as err:
        log.error(err)
        return 1

    finally:
        client_node.exec_command(cmd=f"rm -rf {immutable_cache_path}", sudo=True)

    return 0


def run(**kw):
    """RBD Immutable cache test with cluster operations.

    Args:
        **kw: test data
    """
    log.info(
        "Running test CEPH-83574132 - Immutable object cache with cluster operations."
    )

    try:
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        ret_val = test_immutable_cache(rbd_obj=rbd_obj, **kw)
    except Exception as e:
        log.error(
            f"Immutable object cache with cluster operations test failed for the error as {str(e)}"
        )
        ret_val = 1
    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)
    return ret_val
