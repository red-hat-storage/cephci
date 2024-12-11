"""
Module to verify successful live migration of RBD images from
one ceph cluster to another ceph cluster with native data format.

Pre-requisites:
- Two Ceph clusters deployed and accessible.
- A common client node configured to access both clusters.
- `ceph-common` package installed with live migration binaries available.

Test steps covered:
1. Configure the common client node to access both clusters.
2. Create replicated pools and EC pools and initialize them on both clusters.
3. Create and write data to an RBD image, including a snapshot.
4. Execute live migration using native RBD source specification.
"""

import json
import tempfile

from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import copy_file, get_md5sum_rbd_image, getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.migration import verify_migration_state
from ceph.rbd.workflows.rbd import (
    create_single_pool_and_images,
    run_io_and_check_rbd_status,
)
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def configure_common_client_node(client1, client2):
    """
    Configure the common client node (client1) to access both clusters.
    Args:
        client1: Cluster1 client node object
        client2: Cluster2 client node object
    """
    # Ensure /etc/ceph directory exists and is writable on client1
    client1.exec_command(cmd="sudo mkdir -p /etc/ceph && sudo chmod 777 /etc/ceph")

    # Copy cluster2 configuration and keyring files to client1
    # import pdb

    # pdb.set_trace()
    cluster2_files = [
        ("/etc/ceph/ceph.conf", "/etc/ceph/cluster2.conf"),
        (
            "/etc/ceph/ceph.client.admin.keyring",
            "/etc/ceph/cluster2.client.admin.keyring",
        ),
    ]
    for file, dest_path in cluster2_files:
        copy_file(file_name=file, src=client2, dest=client1, dest_file_name=dest_path)

    client1.exec_command(sudo=True, cmd="chmod 644 /etc/ceph/*")

    # verify cluster accessibility for both clusters
    for cluster_name in ["ceph", "cluster2"]:
        out, err = client1.exec_command(
            cmd=f"ceph -s --cluster {cluster_name}", output=True
        )
        log.info(f"Cluster {cluster_name} status: {out}")
        if err:
            raise Exception(
                f"Unable to access cluster {cluster_name} from common client node"
            )
            return 1
    log.info("Common client node configured successfully.")
    return 0


def prepare_migration_source_spec(
    cluster_name, client, pool_name, image_name, snap_name
):
    """
    Create a native source spec file for migration.
    Args:
        cluster_name: Name of the source cluster
        pool_name: Name of the source pool
        image_name: Name of the source image
        snap_name: Name of the snapshot
    Returns:
        Path to the native spec file
    """
    native_spec = {
        "cluster_name": cluster_name,
        "type": "native",
        "pool_name": pool_name,
        "image_name": image_name,
        "snap_name": snap_name,
    }

    temp_file = tempfile.NamedTemporaryFile(dir="/tmp", suffix=".json")
    spec_file = client.remote_file(sudo=True, file_name=temp_file.name, file_mode="w")
    spec_file.write(json.dumps(native_spec, indent=4))
    spec_file.flush()

    return temp_file.name


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

    # create source image
    for pool_type in rbd_obj.get("pool_types"):
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = getdict(rbd_config)
        rbd = rbd_obj.get("rbd")
        for pool, pool_config in multi_pool_config.items():
            if "data_pool" in pool_config.keys():
                _ = pool_config.pop("data_pool")
            multi_image_config = getdict(pool_config)
            # images = list(multi_image_config.keys())
            # run fio on that image
            for image_name, image_conf in multi_image_config.items():
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

                # create snapshot
                rbd.snap.create(
                    pool=pool,
                    image=image_name,
                    snap=snap_name,
                )

                # get md5sum of image before migration
                import pdb

                pdb.set_trace()
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

                # Create a target pool where the encrypted clone is to be migrated
                is_ec_pool = True if "ec" in pool_type else False
                config = kw.get("config", {})
                target_pool = "target_pool_" + random_string(len=5)
                target_pool_config = {}
                if is_ec_pool:
                    data_pool_target = "data_pool_new_" + random_string(len=5)
                    target_pool_config["data_pool"] = data_pool_target

                rc = create_single_pool_and_images(
                    config=config,
                    pool=target_pool,
                    pool_config=target_pool_config,
                    client=c2_client,
                    cluster="ceph",
                    rbd=rbd,
                    ceph_version=int(config.get("rhbuild")[0]),
                    is_ec_pool=is_ec_pool,
                    is_secondary=False,
                    do_not_create_image=True,
                )
                if rc:
                    log.error(f"Creation of target pool {target_pool} failed")
                    return rc

                # Adding the new pool details to config so that they are handled in cleanup
                if pool_type == "rep_pool_config":
                    kw["config"]["rep_pool_config"][target_pool] = {}
                elif pool_type == "ec_pool_config":
                    kw["config"]["ec_pool_config"][target_pool] = {
                        "data_pool": data_pool_target
                    }

                # Prepare Migration
                target_image = "target_image_" + random_string(len=5)

                # Exceute prepare migration for external cluster

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

                # Exceute migration for external cluster
                rbd.migration.action_external(
                    action="execute",
                    dest_spec=f"{target_pool}/{target_image}",
                    cluster_name=c2,
                    client=c1_client,
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
                rbd.migration.action_external(
                    action="commit",
                    dest_spec=f"{target_pool}/{target_image}",
                    cluster_name=c2,
                    client=c1_client,
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

                # verify data integrity
                md5_sum_after_migration = get_md5sum_rbd_image(
                    image_spec=f"{target_pool}/{target_image}",
                    rbd=rbd2,
                    client=c2_client,
                    file_path=f"/tmp/{random_string(len=5)}",
                )
                log.info(f"md5sum after migration: {md5_sum_after_migration}")

                if md5_sum_before_migration != md5_sum_after_migration:
                    log.error("Data integrity check failed")
                    return 1
                log.info("md5sum checksum is same on both clusters after migration")
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

        # if configure_common_client_node(
        #     client1=cluster1_client,
        #     client2=cluster2_client,
        # ):
        #     log.error("Common client node configuration failed")
        #     return 1

        ret_val = test_external_rbd_image_migration(
            rbd_obj=rbd_obj,
            c1_client=cluster1_client,
            c2_client=cluster2_client,
            **kw,
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
