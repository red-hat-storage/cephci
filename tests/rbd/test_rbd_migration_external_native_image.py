"""Module to verify successful live migration of RBD images from
one ceph cluster to another ceph cluster with native data format.

Test case covered -
CEPH-83597689 - Live migration of Native RBD images
from one ceph cluster to another ceph cluster

Pre-requisites:
- Two Ceph clusters deployed and accessible.
- A common client node configured to access both clusters.
- `ceph-common` package installed with live migration binaries available.

Test Case Flow:
1. Deploy two ceph clusters as source(cluster1) and destination(cluster2)
along with mon,mgr,osd’s
2. Install ceph-common package on one common client node
3.Create one client node common among both clusters by copying the both
the ceph.conf and ceph.client.admin.keyring from both the clusters
into the common client node.
4. Create one Replicated pool on both clusters and initialize it.
5. Create one RBD image inside that pool
6. Write some data to image using rbd bench or fio or file mount
also note down it's md5sum for data consistency
7. Create one snapshots to that image using rbd snap create command
8. Create a native source spec file for migration
9. Execute prepare migration with import-only option with
--source-spec-path for RBD image migration
10. Initiate migration execution using the migration execute command
11. Commit the Migration using the migration commit command
12. Export the migrated image using rbd export command and
note down it's md5sum checksum
13. Verify md5sum checksum of before migration and after
migration should be same
14. Repeat the above test for EC pool
15. unmount, unmap, cleanup the pools and images
"""

from copy import deepcopy

from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import (
    configure_common_client_node,
    get_md5sum_rbd_image,
    getdict,
    random_string,
)
from ceph.rbd.workflows.cleanup import cleanup, pool_cleanup
from ceph.rbd.workflows.migration import (
    prepare_migration_source_spec,
    verify_migration_state,
)
from ceph.rbd.workflows.rbd import (
    create_single_pool_and_images,
    run_io_and_check_rbd_status,
)
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def test_external_rbd_image_migration(rbd_obj, c1_client, c2_client, **kw):
    """
    Test to perform live migration of images with native data format.
    Args:
        rbd_obj: rbd object
        c1_client: Cluster1 client node object
        c2_client: Cluster2 client node object
        kw: Key/value pairs of configuration information to be used in the test
    """
    c1 = "ceph"
    c2 = "cluster2"
    snap_name = "snap1"
    rbd2 = Rbd(c2_client)

    for pool_type in rbd_obj.get("pool_types"):
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = deepcopy(getdict(rbd_config))
        rbd = rbd_obj.get("rbd")
        for pool, pool_config in multi_pool_config.items():
            if "data_pool" in pool_config.keys():
                _ = pool_config.pop("data_pool")
            multi_image_config = getdict(pool_config)
            for image_name, image_conf in multi_image_config.items():
                # Run IO on the image
                try:
                    io_rc = run_io_and_check_rbd_status(
                        rbd=rbd,
                        pool=pool,
                        image=image_name,
                        client=c1_client,
                        image_conf=image_conf,
                    )
                    if io_rc:
                        log.error(f"IO on image {image_name} failed")
                        return 1

                    # create snapshot for the image
                    rbd.snap.create(
                        pool=pool,
                        image=image_name,
                        snap=snap_name,
                    )

                    # get md5sum of image before migration for data consistency check
                    md5_sum_before_migration = get_md5sum_rbd_image(
                        image_spec=f"{pool}/{image_name}",
                        rbd=rbd,
                        client=c1_client,
                        file_path=f"/tmp/{random_string(len=3)}",
                    )
                    log.info(f"md5sum before Migration: {md5_sum_before_migration}")

                    # prepare migration source spec
                    source_spec_path = prepare_migration_source_spec(
                        cluster_name=c1,
                        client=c1_client,
                        pool_name=pool,
                        image_name=image_name,
                        snap_name=snap_name,
                    )

                    # Create a target pool where image needs to be migrated on cluster2
                    is_ec_pool = True if "ec" in pool_type else False
                    config = kw.get("config", {})
                    target_pool = "target_pool_" + random_string(len=5)
                    target_pool_config = {}
                    pools_to_delete = [target_pool]
                    if is_ec_pool:
                        data_pool_target = "data_pool_new_" + random_string(len=5)
                        target_pool_config["data_pool"] = data_pool_target
                        pools_to_delete.append(data_pool_target)

                    rc = create_single_pool_and_images(
                        config=config,
                        pool=target_pool,
                        pool_config=target_pool_config,
                        client=c2_client,
                        cluster="ceph",
                        rbd=rbd2,
                        ceph_version=int(config.get("rhbuild")[0]),
                        is_ec_pool=is_ec_pool,
                        is_secondary=False,
                        do_not_create_image=True,
                    )
                    if rc:
                        log.error(f"Creation of target pool {target_pool} failed")
                        return rc

                    # Exceute prepare migration for external cluster
                    target_image = "target_image_" + random_string(len=5)
                    rbd.migration.prepare_import(
                        source_spec_path=source_spec_path,
                        dest_spec=f"{target_pool}/{target_image}",
                        cluster_name=c2,
                    )

                    # verify prepare migration status
                    if verify_migration_state(
                        action="prepare",
                        image_spec=f"{target_pool}/{target_image}",
                        cluster_name=c2,
                        client=c1_client,
                        **kw,
                    ):
                        log.error("Failed to prepare migration")
                        return 1

                    # execute migration from cluster2
                    rbd.migration.action(
                        action="execute",
                        dest_spec=f"{target_pool}/{target_image}",
                        cluster_name=c2,
                    )

                    # verify execute migration status
                    if verify_migration_state(
                        action="execute",
                        image_spec=f"{target_pool}/{target_image}",
                        cluster_name=c2,
                        client=c1_client,
                        **kw,
                    ):
                        log.error("Failed to execute migration")
                        return 1

                    # commit migration for external cluster
                    rbd.migration.action(
                        action="commit",
                        dest_spec=f"{target_pool}/{target_image}",
                        cluster_name=c2,
                    )

                    # verify commit migration status
                    if verify_migration_state(
                        action="commit",
                        image_spec=f"{target_pool}/{target_image}",
                        cluster_name=c2,
                        client=c1_client,
                        **kw,
                    ):
                        log.error("Failed to commit migration")
                        return 1

                    # verify checksum post migration
                    md5_sum_after_migration = get_md5sum_rbd_image(
                        image_spec=f"{target_pool}/{target_image}",
                        rbd=rbd2,
                        client=c2_client,
                        file_path=f"/tmp/{random_string(len=5)}",
                    )
                    log.info(f"md5sum after migration: {md5_sum_after_migration}")

                    if md5_sum_before_migration != md5_sum_after_migration:
                        log.error(
                            "Data integrity check failed, md5sum checksums are not same"
                        )
                        return 1
                    log.info("md5sum checksum is same on both clusters after migration")

                except Exception as e:
                    log.error(f"Error during migration: {str(e)}")
                    return 1

                finally:
                    if source_spec_path:
                        log.info(f"Cleaning up source spec path: {source_spec_path}")
                        out, err = c1_client.exec_command(
                            sudo=True, cmd=f"rm -f {source_spec_path}"
                        )
                        if err:
                            log.error(f"Failed to delete file {source_spec_path}")

                    pool_cleanup(
                        client=c2_client,
                        pools=pools_to_delete,
                        ceph_version=int(kw["config"].get("rhbuild")[0]),
                    )

    return 0


def run(**kw):
    """
    Test to execute Live image migration with native data format
    from external ceph cluster.
    Args:
        kw: Key/value pairs of configuration information to be used in the test
            Example::
            config:
                do_not_create_image: True
                rep_pool_config:
                    num_pools: 2
                    size: 4G
                ec_pool_config:
                    num_pools: 2
                    size: 4G
                create_pool_parallely: true
    """
    log.info(
        "Executing CEPH-83597689: Live migration of images with native \
        data format from external ceph cluster"
    )

    try:
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")

        cluster1_client = (
            kw.get("ceph_cluster_dict").get("ceph-rbd1").get_nodes(role="client")[0]
        )

        cluster2_client = (
            kw.get("ceph_cluster_dict").get("ceph-rbd2").get_nodes(role="client")[0]
        )

        if configure_common_client_node(
            client1=cluster1_client,
            client2=cluster2_client,
        ):
            log.error("Common client node configuration failed")
            return 1

        if rbd_obj:
            log.info("Executing test on Replicated and EC pool")

            ret_val = test_external_rbd_image_migration(
                rbd_obj=rbd_obj,
                c1_client=cluster1_client,
                c2_client=cluster2_client,
                **kw,
            )

            if ret_val == 0:
                log.info(
                    "Testing RBD image migration with external ceph native format Passed"
                )

    except Exception as e:
        log.error(
            f"RBD image migration with external ceph native format failed with the error {str(e)}"
        )
        ret_val = 1

    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(cluster1_client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)

    return ret_val
