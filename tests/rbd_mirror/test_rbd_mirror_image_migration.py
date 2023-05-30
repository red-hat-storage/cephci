"""Module to execute Live migration feature test case on mirrored images.

Pre-requisites :
Two ceph clusters with rbd mirror configured along with:
        1. rbd mirror daemons per cluster
        2. 1 client

Test cases covered -
CEPH-83573320 - Copy source image(Live migration) which is mirrored and check the behaviour

Test Case Flow -
1. Create an source EC pool with different k_m values and an image
2. Create an EC pool for destination pool
3. Mount the images and run io from clients
4. Stop IO and prepare migration process from source pools to destination pool
5. Start IO from client on destination images
6. Execute migration
7. Commit migration.
8. repeate above steps(1-7) for Replication pool
"""

from tests.rbd.exceptions import RbdBaseException
from tests.rbd.rbd_utils import Rbd, verify_migration_commit, verify_migration_state
from tests.rbd_mirror import rbd_mirror_utils as rbdmirror
from tests.rbd_mirror.rbd_mirror_utils import rbd_mirror_config
from utility.log import Log

log = Log(__name__)


def run_fio_and_migrate_image(
    rbd1, mirror1, image, src_spec, dest_pool, dest_spec, io
):
    # run io's on the source image
    mirror1.benchwrite(imagespec=src_spec, io=io)

    # Perpare the migration process
    rbd1.migration_prepare(src_spec, dest_spec)

    # verify migration prepare
    verify_migration_state(rbd1, dest_spec)

    # run io's on the destination image
    mirror1.benchwrite(imagespec=dest_spec, io=io)

    # Execute migration process
    rbd1.migration_action(action="execute", dest_spec=dest_spec)

    # verify migration execute
    verify_migration_state(rbd1, dest_spec)

    # commit migration process
    rbd1.migration_action(action="commit", dest_spec=dest_spec)

    # verify commit migration
    verify_migration_commit(rbd1, dest_pool, image)


def run(**kw):
    """Image live migration for mirrored images.

    Args:
        kw: Key/value pairs of configuration information to be used in the test
            Example::
            config:
                "ec-pool-only": True
                ec_pool_config:
                    pool: rbd_pool_4
                    data_pool: rbd_ec_pool_4
                    ec_profile: rbd_ec_profile_4
                    image: rbd_image_4
                    size: 10G

    Returns:
        int: The return value. 0 for success, 1 otherwise
    """
    log.info(
        "Starting execution of test case :"
        " CEPH-83573320 - Copy source image(Live migration) which is mirrored"
    )
    config = kw.get("config")
    mirror1, mirror2 = [
    rbdmirror.RbdMirror(cluster, config)
    for cluster in kw.get("ceph_cluster_dict").values()
    ]

    rbd1, rbd2 = [
        Rbd(**kw, req_cname=cluster_name)
        for cluster_name in kw.get("ceph_cluster_dict").keys()
    ]
    for key, value in kw["config"].items():
        if key == "journal":
            for key, value in kw["config"]["journal"].items():
                if key == "source":
                    kw["config"].update(value)
                    rbd_mirror_config(**kw)
                    for key in kw["config"]["journal"]["source"]:
                        kw["config"].pop(key)
                    log.info(kw["config"])
                    kw["config"].pop("ec-pool-k-m")
                elif key == "destination":
                    kw["config"].update(value)
                    rbd_mirror_config(**kw)
                    for key in kw["config"]["journal"]["destination"]:
                        kw["config"].pop(key)
                    log.info(kw["config"])
                    kw["config"].pop("ec-pool-k-m")
            src_rep_pool = kw["config"]["journal"]["source"]["rep_pool_config"]["pool"]
            rep_image = kw["config"]["journal"]["source"]["rep_pool_config"]["image"]
            src_rep_spec = src_rep_pool + "/" + rep_image
            dest_rep_pool = kw["config"]["journal"]["destination"]["rep_pool_config"][
                "pool"
            ]
            dest_rep_spec = dest_rep_pool + "/" + rep_image
            src_ec_pool = kw["config"]["journal"]["source"]["ec_pool_config"]["pool"]
            src_data_pool = kw["config"]["journal"]["source"]["ec_pool_config"][
                "data_pool"
            ]
            ec_image = kw["config"]["journal"]["source"]["ec_pool_config"]["image"]
            src_ec_spec = src_ec_pool + "/" + ec_image
            dest_ec_pool = kw["config"]["journal"]["destination"]["ec_pool_config"][
                "pool"
            ]
            dest_data_pool = kw["config"]["journal"]["destination"]["ec_pool_config"][
                "data_pool"
            ]
            dest_ec_spec = dest_ec_pool + "/" + ec_image
            io_total = kw["config"]["journal"]["source"]["rep_pool_config"]["io_total"]
            mode_type = "journal"
            log.info("running test for Journal Pool based mirroring")
        elif key == "snapshot":
            log.info(kw["config"])
            for key, value in kw["config"]["snapshot"].items():
                if key == "source":
                    kw["config"].update(value)
                    rbd_mirror_config(**kw)
                    for key in kw["config"]["snapshot"]["source"]:
                        kw["config"].pop(key)
                    log.info(kw["config"])
                    kw["config"].pop("ec-pool-k-m")
                elif key == "destination":
                    kw["config"].update(value)
                    rbd_mirror_config(**kw)
                    for key in kw["config"]["snapshot"]["destination"]:
                        kw["config"].pop(key)
                    log.info(kw["config"])
                    kw["config"].pop("ec-pool-k-m")
            src_rep_pool = kw["config"]["snapshot"]["source"]["rep_pool_config"]["pool"]
            rep_image = kw["config"]["snapshot"]["source"]["rep_pool_config"]["image"]
            src_rep_spec = src_rep_pool + "/" + rep_image
            dest_rep_pool = kw["config"]["snapshot"]["destination"]["rep_pool_config"][
                "pool"
            ]
            dest_rep_spec = dest_rep_pool + "/" + rep_image
            src_ec_pool = kw["config"]["snapshot"]["source"]["ec_pool_config"]["pool"]
            src_data_pool = kw["config"]["snapshot"]["source"]["ec_pool_config"][
                "data_pool"
            ]
            ec_image = kw["config"]["snapshot"]["source"]["ec_pool_config"]["image"]
            src_ec_spec = src_ec_pool + "/" + ec_image
            dest_ec_pool = kw["config"]["snapshot"]["destination"]["ec_pool_config"][
                "pool"
            ]
            dest_data_pool = kw["config"]["snapshot"]["source"]["ec_pool_config"][
                "data_pool"
            ]
            dest_ec_spec = dest_ec_pool + "/" + ec_image
            io_total = kw["config"]["snapshot"]["source"]["rep_pool_config"]["io_total"]
            mode_type = "snapshot"
            log.info("running test for snapshot image based mirroring")

        try:
            log.info("Running test on Replication pool")
            if mode_type == "snapshot":
                mirror2.wait_for_snapshot_complete(src_rep_spec)
            else:
                 mirror2.wait_for_replay_complete(src_rep_spec)

            run_fio_and_migrate_image(
                rbd1,
                mirror1,
                rep_image,
                src_rep_spec,
                dest_rep_pool,
                dest_rep_spec,
                io_total,
            )
            if mirror1.image_exists(dest_rep_spec):
                log.error(
                    f"migrated image is not found in {dest_rep_pool} in primary cluster"
                )
                return 1
            log.info(
                f"image {rep_image} is successfully migrated in {dest_rep_pool} in primary cluster"
            )
            if mode_type == "snapshot":
                mirror2.wait_for_snapshot_complete(dest_rep_spec)
            else:
                 mirror2.wait_for_replay_complete(dest_rep_spec)
            if mirror2.image_exists(dest_rep_spec):
                log.error(
                    f"Migrated image not found in {dest_rep_pool} in secondary cluster"
                )
                return 1
            log.info(
                f"Image {rep_image} is successfully migrated in {dest_rep_pool} in secondary cluster"
            )

            log.info("Runnng test on EC pool")
            run_fio_and_migrate_image(
                rbd1,
                mirror1,
                ec_image,
                src_ec_spec,
                dest_ec_pool,
                dest_ec_spec,
                io_total,
            )
            if mirror1.image_exists(dest_rep_spec):
                log.error(
                    f"migrated image is not found in {dest_rep_pool} in primary cluster"
                )
                return 1
            log.info(
                f"image {rep_image} is successfully migrated in {dest_rep_pool} in primary cluster"
            )

            if mode_type == "snapshot":
                mirror2.wait_for_snapshot_complete(dest_ec_spec)
            else:
                 mirror2.wait_for_replay_complete(dest_ec_spec)
            if mirror2.image_exists(dest_ec_spec):
                log.error(
                    f"Migrated image not found in {dest_ec_pool} in secondary cluster"
                )
                return 1
            log.info(
                f"Image {ec_image} is successfully migrated in {dest_ec_pool} in secondary cluster"
            )

        except RbdBaseException as error:
            log.error(error.message)
            return 1

        finally:
            if not kw.get("config").get("do_not_cleanup_pool"):
                mirror1.clean_up(
                    peercluster=mirror2,
                    pools=[
                        src_rep_pool,
                        dest_rep_pool,
                        src_ec_pool,
                        dest_ec_pool,
                        src_data_pool,
                        dest_data_pool,
                    ],
                )
    return 0
