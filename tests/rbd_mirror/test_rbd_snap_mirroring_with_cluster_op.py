import time

from ceph.ceph_admin import CephAdmin
from ceph.parallel import parallel
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.monitor_workflows import MonitorWorkflows
from ceph.rbd.initial_config import initial_mirror_config, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.cluster_operations import (
    check_health,
    operation,
    restart_ceph_target,
    restart_osd,
)
from ceph.rbd.workflows.rbd_mirror import toggle_rbd_mirror_daemon_status_and_verify
from ceph.rbd.workflows.snap_scheduling import run_io_verify_snap_schedule
from utility.log import Log

log = Log(__name__)


def test_snap_mirror_cluster_ops(
    rbd_obj, sec_obj, client_node, sec_client, pool_type, **kw
):
    """
    Test the snap mirroring with cluster operations.

    Args:
        rbd_obj: RBD object
        **kw: test data

    Returns:
        int: The return value. 0 for success, 1 for failure.
    """
    config = kw.get("config")
    ceph_cluster = kw.get("ceph_cluster")
    admin_node = ceph_cluster.get_nodes(role="_admin")[0]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonitorWorkflows(node=cephadm)

    try:
        log.info("Running cluster operations while testing snap mirroring")

        mount_path = f"/tmp/mnt_{random_string(len=5)}"

        with parallel() as p:
            log.info(
                "Test to restart rbd mirror daemon on primary along with snap mirroring"
            )
            p.spawn(
                toggle_rbd_mirror_daemon_status_and_verify,
                client=client_node,
                rbd=rbd_obj,
                is_secondary=False,
                pool_type=pool_type,
                raise_exception=True,
                **kw,
            )
            p.spawn(
                run_io_verify_snap_schedule,
                pool_type=pool_type,
                rbd=rbd_obj,
                client=client_node,
                skip_mkfs=False,
                raise_exception=True,
                mount_path=f"{mount_path}/file_00",
                **kw,
            )

        with parallel() as p:
            log.info(
                "Test to restart rbd mirror daemon on secondary along with snap mirroring"
            )
            p.spawn(
                toggle_rbd_mirror_daemon_status_and_verify,
                client=sec_client,
                rbd=sec_obj,
                is_secondary=True,
                pool_type=pool_type,
                raise_exception=True,
                **kw,
            )
            p.spawn(
                run_io_verify_snap_schedule,
                pool_type=pool_type,
                rbd=rbd_obj,
                client=client_node,
                skip_mkfs=False,
                raise_exception=True,
                mount_path=f"{mount_path}/file_01",
                **kw,
            )

        with parallel() as p:
            log.info("Test to restart mon service along with snap mirroring")
            p.spawn(
                operation,
                rados_obj,
                "restart_daemon_services",
                daemon="mon",
            )
            p.spawn(
                run_io_verify_snap_schedule,
                pool_type=pool_type,
                rbd=rbd_obj,
                client=client_node,
                skip_mkfs=False,
                raise_exception=True,
                mount_path=f"{mount_path}/file_1",
                **kw,
            )

        with parallel() as p:
            log.info("Test to restart osd service along with snap mirroring")
            p.spawn(restart_osd, client_node=client_node)
            p.spawn(
                run_io_verify_snap_schedule,
                pool_type=pool_type,
                rbd=rbd_obj,
                client=client_node,
                skip_mkfs=True,
                raise_exception=True,
                mount_path=f"{mount_path}/file_2",
                **kw,
            )

        with parallel() as p:
            log.info("Test to restart ceph target service along with snap mirroring")
            p.spawn(restart_ceph_target, admin_node=admin_node)
            p.spawn(
                run_io_verify_snap_schedule,
                pool_type=pool_type,
                rbd=rbd_obj,
                client=client_node,
                skip_mkfs=True,
                raise_exception=True,
                mount_path=f"{mount_path}/file_3",
                **kw,
            )

        mon_host = ceph_cluster.get_nodes(role="mon")[0]
        with parallel() as p:
            log.info("Test to remove mon service along with snap mirroring")
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
                run_io_verify_snap_schedule,
                pool_type=pool_type,
                rbd=rbd_obj,
                client=client_node,
                skip_mkfs=True,
                raise_exception=True,
                mount_path=f"{mount_path}/file_4",
                **kw,
            )

        with parallel() as p:
            log.info(
                "Test to add the mon service back to the cluster with snap mirroring"
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
                run_io_verify_snap_schedule,
                pool_type=pool_type,
                rbd=rbd_obj,
                client=client_node,
                skip_mkfs=True,
                raise_exception=True,
                mount_path=f"{mount_path}/file_5",
                **kw,
            )

        check_health(client_node)
    except Exception as err:
        log.error(err)
        return 1

    return 0


def run(**kw):
    """CEPH-83574895 - Configure two-way rbd-mirror on Stand alone
    CEPH cluster on image (with replicated and ec pools)with snapshot
    based mirroring and perform cluster operations
    Pre-requisites :
    We need atleast one client node with ceph-common, fio and rbd-nbd packages,
    conf and keyring files in both clusters with snapshot based RBD mirroring
    enabled between the clusters.
    kw:
        clusters:
            ceph-rbd1:
            config:
                rep_pool_config:
                num_pools: 1
                num_images: 5
                size: 10G
                mode: image # compulsory argument if mirroring needs to be setup
                mirrormode: snapshot
                snap_schedule_levels:
                    - image
                snap_schedule_intervals: #one value for each level specified above
                    - 5m
                io_percentage: 30 #percentage of space in each image to be filled
                ec_pool_config:
                num_pools: 1
                num_images: 5
                mode: image # compulsory argument if mirroring needs to be setup
                mirrormode: snapshot
                snap_schedule_levels:
                    - image
                snap_schedule_intervals:
                    - 5m
                io_size: 200M
            ceph-rbd2:
            config:
                rep_pool_config:
                num_pools: 1
                num_images: 5
                size: 10G
                mode: image # compulsory argument if mirroring needs to be setup
                mirrormode: snapshot
                snap_schedule_levels:
                    - image
                snap_schedule_intervals: #one value for each level specified above
                    - 5m
                io_percentage: 30 #percentage of space in each image to be filled
                ec_pool_config:
                num_pools: 1
                num_images: 5
                mode: image # compulsory argument if mirroring needs to be setup
                mirrormode: snapshot
                snap_schedule_levels:
                    - image
                snap_schedule_intervals:
                    - 5m
                io_size: 200M
    Test Case Flow
    1. Bootstrap two CEPH clusters and setup snapshot based mirroring in between these clusters
    2. Create pools and images as specified, enable snapshot based mirroring for all these images
    3. Schedule snapshots for each of these images and run IOs on each of the images
    4. Perform cluster operations on both the clusters
    5. Make sure that snapshot mirroring is not affected by any of the cluster operations
    """
    pool_types = ["rep_pool_config", "ec_pool_config"]
    log.info("Running cluster operations on snapshot based mirroring clusters")

    try:
        kw.get("config", {})["do_not_run_io"] = True
        mirror_obj = initial_mirror_config(**kw)
        mirror_obj.pop("output", [])

        for val in mirror_obj.values():
            if not val.get("is_secondary", False):
                rbd = val.get("rbd")
                client = val.get("client")
            else:
                sec_rbd = val.get("rbd")
                sec_client = val.get("client")
        log.info("Initial configuration complete")

        pool_types = list(mirror_obj.values())[0].get("pool_types")
        for pool_type in pool_types:
            rc = test_snap_mirror_cluster_ops(
                rbd, sec_rbd, client, sec_client, pool_type, **kw
            )
            if rc:
                log.error(
                    "Cluster operations on snapshot based mirroring clusters failed"
                )
                return 1
    except Exception as e:
        log.error(
            f"Testing cluster operations on snap mirroring clusters failed with error {str(e)}"
        )
        return 1
    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)
    return 0
