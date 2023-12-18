from copy import deepcopy

from ceph.parallel import parallel
from ceph.rbd.initial_config import initial_mirror_config, random_string
from ceph.rbd.utils import getdict
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.execute import execute
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.snap_scheduling import verify_snapshot_schedule
from utility.log import Log

log = Log(__name__)


def run_io_verify_snap_schedule(**kw):
    """
    Run IOs on the given image and verify snapshot schedule
    """
    pool_type = kw.get("pool_type")
    rbd = kw.get("rbd")
    client = kw.get("client")
    config = deepcopy(kw.get("config").get(pool_type))
    for pool, pool_config in getdict(config).items():
        multi_image_config = getdict(pool_config)
        multi_image_config.pop("test_config", {})
        for image, image_config in multi_image_config.items():
            image_spec = f"{pool}/{image}"

            io_size = image_config.get(
                "io_size", int(int(image_config["size"][:-1]) / 3)
            )
            io_config = {
                "rbd_obj": rbd,
                "client": client,
                "size": image_config["size"],
                "do_not_create_image": True,
                "config": {
                    "file_size": io_size,
                    "file_path": [f"{kw['mount_path']}"],
                    "get_time_taken": True,
                    "image_spec": [image_spec],
                    "operations": {
                        "fs": "ext4",
                        "io": True,
                        "mount": True,
                        "nounmap": False,
                        "device_map": True,
                    },
                    "skip_mkfs": kw["skip_mkfs"],
                },
            }
            krbd_io_handler(**io_config)
            kw["io_config"] = io_config
            for interval in image_config.get("snap_schedule_intervals"):
                out = verify_snapshot_schedule(rbd, pool, image, interval)
                if out:
                    log.error(f"Snapshot verification failed for image {pool}/{image}")
                    if kw.get("raise_exception"):
                        raise Exception(
                            f"Snapshot verification failed for image {pool}/{image}"
                        )
                    return 1
    return 0


def test_cluster_upgrade(**kw):
    """
    Upgrade cluster and run IOs in parallel while upgrading
    """
    try:
        ceph_cluster = kw.get("ceph_cluster")
        mon_node = ceph_cluster.get_nodes("installer")[0]
        rc = ceph_cluster.check_health(
            kw["config"]["installed_version"],
            client=mon_node,
            timeout=300,
        )

        if rc != 0:
            log.error("Ceph health not OK")
            return 1

        client = kw.get("client")
        client.exec_command(cmd="ceph osd set noout", sudo=True)
        client.exec_command(cmd="ceph osd set noscrub", sudo=True)
        client.exec_command(cmd="ceph osd set nodeep-scrub", sudo=True)

        with parallel() as p:
            p.spawn(
                execute,
                mod_file_name="tests.cephadm.test_cephadm_upgrade",
                args=kw,
                test_name="Upgrade cluster",
                raise_exception=True,
            )
            for pool_type in kw.get("pool_types"):
                mount_path = kw["mount_paths"][pool_type]
                p.spawn(
                    run_io_verify_snap_schedule,
                    pool_type=pool_type,
                    raise_exception=True,
                    mount_path=f"{mount_path}/file_2",
                    **kw,
                )

        client.exec_command(cmd="ceph osd unset noout", sudo=True)
        client.exec_command(cmd="ceph osd unset noscrub", sudo=True)
        client.exec_command(cmd="ceph osd unset nodeep-scrub", sudo=True)
    except Exception as e:
        log.error(f"Cluster upgrade failed with error {e}")
        return 1
    return 0


def run(**kw):
    """CEPH-83574895 - Configure two-way rbd-mirror on Stand alone
    CEPH cluster on image (with replicated and ec pools)with snapshot
    based mirroring and perform upgrade
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
                io_percentage: 30
                command: start
                service: upgrade
                verify_cluster_health: true
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
                io_percentage: 30
                command: start
                service: upgrade
                verify_cluster_health: true
    Test Case Flow
    1. Bootstrap two CEPH clusters and setup snapshot based mirroring in between these clusters
    2. Create pools and images as specified, enable snapshot based mirroring for all these images
    3. Schedule snapshots for each of these images and run IOs on each of the images
    4. Perform upgrades on both the clusters
    5. Check data and run IOs again on the upgraded clusters
    """
    pool_types = ["rep_pool_config", "ec_pool_config"]
    log.info("Running rbd mirror cluster upgrade with snapshot mirroring enabled")

    try:
        kw.get("config", {})["do_not_run_io"] = True
        mirror_obj = initial_mirror_config(**kw)
        mirror_obj.pop("output", [])

        for val in mirror_obj.values():
            if not val.get("is_secondary", False):
                rbd = val.get("rbd")
                client = val.get("client")
        log.info("Initial configuration complete")

        pool_types = list(mirror_obj.values())[0].get("pool_types")
        log.info("Run IOs on images before upgrade")
        mount_paths = {}
        for pool_type in pool_types:
            mount_path = f"/tmp/mnt_{random_string(len=5)}"
            mount_paths[pool_type] = mount_path
            rc = run_io_verify_snap_schedule(
                pool_type=pool_type,
                rbd=rbd,
                client=client,
                skip_mkfs=False,
                mount_path=f"{mount_path}/file_1",
                **kw,
            )
            if rc:
                log.error(f"Run IOs before upgrade failed for pool type {pool_type}")
                return 1

        log.info("Upgrade cluster and run IOs in parallel")
        rc = test_cluster_upgrade(
            pool_types=pool_types,
            rbd=rbd,
            client=client,
            skip_mkfs=True,
            mount_paths=mount_paths,
            **kw,
        )
        if rc:
            log.error(f"Cluster upgrade failed for pool type {pool_type}")
            return 1

        log.info("Run IOs on images after upgrade")
        for pool_type in pool_types:
            rc = run_io_verify_snap_schedule(
                pool_type=pool_type,
                rbd=rbd,
                client=client,
                skip_mkfs=True,
                mount_path=f"{mount_paths[pool_type]}/file_3",
                **kw,
            )
            if rc:
                log.error(f"Run IOs after upgrade failed for pool type {pool_type}")
                return 1
    except Exception as e:
        log.error(
            f"Rbd mirror cluster upgrade with snapshot mirroring enabled failed with error {str(e)}"
        )
        return 1
    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)
    return 0
