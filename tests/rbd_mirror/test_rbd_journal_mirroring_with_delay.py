from copy import deepcopy
from time import sleep

from ceph.rbd.initial_config import initial_mirror_config, random_string
from ceph.rbd.utils import getdict
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.journal_mirror_ops import (
    config_mirroring_delay,
    run_io_wait_for_replay_complete,
    write_data_and_verify_no_mirror_till_delay,
)
from utility.log import Log

log = Log(__name__)


def test_delayed_replication(
    rbd_obj, sec_obj, client_node, sec_client, pool_type, delay_at_primary, **kw
):
    """
    Apply rbd mirror delay at specified level and verify
    """
    try:
        config = deepcopy(kw.get("config").get(pool_type))
        delays = config.get("journal_delay", ["600"])
        cluster_name = kw["ceph_cluster"].name
        sec_cluster_name = [
            k for k in kw.get("ceph_cluster_dict").keys() if k != cluster_name
        ][0]
        for delay in delays:
            for pool, pool_config in getdict(config).items():
                multi_image_config = getdict(pool_config)
                multi_image_config.pop("test_config", {})
                for image, image_config in multi_image_config.items():
                    image_spec = f"{pool}/{image}"
                    mount_path = f"/tmp/mnt_{random_string(len=5)}"
                    rc = run_io_wait_for_replay_complete(
                        rbd=rbd_obj,
                        sec_rbd=sec_obj,
                        client=client_node,
                        pool=pool,
                        image=image,
                        image_config=image_config,
                        mount_path=f"{mount_path}/file_00",
                        sec_cluster_name=sec_cluster_name,
                        skip_mkfs=False,
                    )
                    if rc:
                        log.error(
                            f"Run IO and wait for replay failed for image {pool}/{image}"
                        )
                        return 1
                    sleep(120)
                    if delay_at_primary:
                        rc = config_mirroring_delay(
                            delay=delay,
                            delay_per_image=True,
                            rbd=rbd_obj,
                            pool=pool,
                            image=image,
                            operation="set",
                        )
                    else:
                        rc = config_mirroring_delay(
                            delay=delay, client=sec_client, operation="set"
                        )
                    if rc:
                        log.error("Setting mirror delay failed")
                        return 1

                    rc = write_data_and_verify_no_mirror_till_delay(
                        rbd=rbd_obj,
                        client=client_node,
                        sec_rbd=sec_obj,
                        sec_client=sec_client,
                        pool=pool,
                        image=image,
                        mount_path=f"{mount_path}/file_01",
                        skip_mkfs=True,
                        image_config=image_config,
                        delay=int(delay),
                    )

                    if rc:
                        log.error(
                            f"Verification of no mirror until delay failed for image {image_spec}"
                        )
                        return 1
    except Exception as e:
        log.error(str(e))
        return 1
    finally:
        if delay_at_primary:
            rc = config_mirroring_delay(
                delay_per_image=True,
                rbd=rbd_obj,
                pool=pool,
                image=image,
                operation="remove",
            )
        else:
            rc = config_mirroring_delay(client=sec_client, operation="remove")
        if rc:
            log.error("Removing mirror delay failed")
            return 1
    return 0


def run(**kw):
    """CEPH-11491 - [Rbd Mirror] - Delayed Replication - Need
    to test the limit of delay that can be set. Set a reasonable high delay.
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
                    mode: pool # compulsory argument if mirroring needs to be setup
                    journal_delay:
                        - "2m"
                    io_size: 200M
                ec_pool_config:
                    num_pools: 1
                    num_images: 5
                    size: 10G
                    mode: pool # compulsory argument if mirroring needs to be setup
                    journal_delay:
                        - "2m"
                    io_size: 200M
            ceph-rbd2:
            config:
                rep_pool_config:
                    num_pools: 1
                    num_images: 5
                    size: 10G
                    mode: pool # compulsory argument if mirroring needs to be setup
                    journal_delay:
                        - "2m"
                    io_size: 200M
                ec_pool_config:
                    num_pools: 1
                    num_images: 5
                    size: 10G
                    mode: pool # compulsory argument if mirroring needs to be setup
                    journal_delay:
                        - "2m"
                    io_size: 200M
    Test Case Flow
    1. Bootstrap two CEPH clusters and setup snapshot based mirroring in between these clusters
    2. Create pools and images as specified, enable journal based mirroring for all these pools
    3. Set mirroring delay using ceph config set, verify the delay
    4. Set mirroring delay using rbd image-meta set conf_rbd_mirroring_replay_delay, verify
    """
    pool_types = ["rep_pool_config", "ec_pool_config"]
    log.info(
        "CEPH-11491 - Testing mirroring delay for journal based mirroring clusters"
    )

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
            log.info(
                f"Testing delayed replication on journal based mirroring for {pool_type}"
            )
            rc = test_delayed_replication(
                rbd_obj=rbd,
                sec_obj=sec_rbd,
                client_node=client,
                sec_client=sec_client,
                pool_type=pool_type,
                delay_at_primary=True,
                **kw,
            )
            if rc:
                log.error(
                    f"RBD Mirror delay on journal mirroring clusters failed for {pool_type} at image level failed"
                )
                return 1
            log.info(
                f"Testing delayed replication on journal based mirroring passed for {pool_type}"
            )
    except Exception as e:
        log.error(
            f"Testing rbd mirror delay on journal mirroring clusters failed with error {str(e)}"
        )
        return 1
    finally:
        cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)
    return 0
