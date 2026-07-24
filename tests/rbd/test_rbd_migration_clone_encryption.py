"""Module to verify successful Live migration of parent image with LUKS1 and child image with LUKS2.

Test case covered -
CEPH-83598689 - Live migration of parent image with LUKS1 and child image with LUKS2

Pre-requisites:
- Two Ceph clusters deployed and accessible.
- A common client node configured to access both clusters.
- `ceph-common` package installed with live migration binaries available.

Test Case Flow:
1. Deploy two ceph clusters as source(cluster1) and destination(cluster2) along with mon,mgr,osd’s
2. Install ceph-common package on one common client node
3. Create one client node common among both clusters by copying the both the ceph.conf and ceph.client.admin.keyring
from both the clusters into the common client node.
4. Create one Replicated pool on both clusters and initialize it.
5. Create one RBD image inside that pool and enable LUKS1 encryption to it
6. Write some data to image using rbd bench or fio or file mount also note down it's md5sum for data consistency
7. Create one snapshots to that image using rbd snap create command
8. Create clone and format it using LUKS2 encryption
9. Create a native source spec file for migration as below
[root@ceph-rbd1-sangadi-bz-1w14jg-node2 ~]#  jq . /tmp/native_spec
{
  "cluster_name": "c1",
  "type": "native",
  "pool_id": "5",
  "image_name": "image1",
  "snap_id": "3"
}
10.  Execute prepare migration with import-only option with --source-spec-path for both parent and child RBD images
11. Initiate migration execution using the migration execute command
12. Commit the Migration using the migration commit command
13. Export the migrated image using rbd export command and note down it's md5sum checksum
14. Verify md5sum checksum of before migration and after migration should be same
15. Decrypt both images using the respective keys (LUKS1 for the parent, LUKS2 for the child)
16. unmount, unmap, cleanup the pools and images
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
from ceph.rbd.workflows.encryption import create_passphrase_file
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.migration import (
    prepare_migration_source_spec,
    verify_migration_state,
)
from ceph.rbd.workflows.rbd import create_single_pool_and_images
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def test_migration_clone_encryption(rbd_obj, c1_client, c2_client, **kw):
    """
    Test to perform live migration of clone images with encryption from one cluster to another with native data format.
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
            encryption_type_rbd_image = "luks1"
            encryption_type_clone_image = "luks2"
            for image_name, image_conf in multi_image_config.items():
                try:
                    image_spec = f"{pool}/{image_name}"
                    passphrase_parent = format_image(
                        rbd, c1_client, encryption_type_rbd_image, image_spec
                    )

                    # Map, mount and run IOs
                    fio = kw.get("config", {}).get("fio", {})
                    io_config = {
                        "rbd_obj": rbd,
                        "client": c1_client,
                        "size": fio["size"],
                        "do_not_create_image": True,
                        "config": {
                            "file_size": fio["size"],
                            "file_path": [f"/mnt/mnt_{random_string(len=3)}/file"],
                            "get_time_taken": True,
                            "image_spec": [f"{pool}/{image_name}"],
                            "operations": {
                                "fs": "ext4",
                                "io": True,
                                "mount": True,
                                "device_map": True,
                            },
                            "cmd_timeout": 2400,
                            "io_type": "write",
                        },
                    }

                    # Include the encryption details in io config
                    encryption_config = list()
                    encryption_config.append(
                        {"encryption-format": encryption_type_rbd_image}
                    )
                    encryption_config.append(
                        {"encryption-passphrase-file": passphrase_parent}
                    )
                    io_config["config"]["encryption_config"] = encryption_config
                    out, err = krbd_io_handler(**io_config)
                    if err:
                        raise Exception(
                            "Map, mount and run IOs failed for encrypted "
                            + pool
                            + "/"
                            + image_name
                        )
                    else:
                        log.info(
                            "Map, mount and IOs successful for encrypted "
                            + pool
                            + "/"
                            + image_name
                        )

                    # Create snapshot for the parent image
                    out, err = rbd.snap.create(
                        pool=pool,
                        image=image_name,
                        snap=snap_name,
                    )
                    if out or err and "100% complete" not in err:
                        raise Exception("Snapshot creation failed for " + snap_name)

                    # Protect snapshot
                    out, err = rbd.snap.protect(
                        pool=pool, image=image_name, snap=snap_name
                    )
                    if "failed to Protect snapshot" in out + err:
                        raise Exception(
                            "Snapshot Protection failed for " + pool + "/" + image_name
                        )
                    else:
                        log.info(
                            "Successfully Protected snapshot "
                            + snap_name
                            + " for "
                            + pool
                            + "/"
                            + image_name
                        )

                    # Create a clone of this image
                    clone_image = f"clone_{image_name}"
                    clone_spec = {
                        "source-snap-spec": f"{pool}/{image_name}@{snap_name}",
                        "dest-image-spec": f"{pool}/{clone_image}",
                    }
                    _, err = rbd.clone(**clone_spec)
                    if err:
                        raise Exception(
                            "Clone creation failed for "
                            + pool
                            + "/"
                            + image_name
                            + "/"
                            + snap_name
                            + " with error "
                            + err
                        )
                    else:
                        log.info("Cloning of snap " + snap_name + " is complete")

                    # Create snap of the cloned Image
                    clone_snap_name = image_name + "_clone"
                    out, err = rbd.snap.create(
                        pool=pool,
                        image=clone_image,
                        snap=clone_snap_name,
                    )
                    if out or err and "100% complete" not in err:
                        raise Exception(
                            "Snapshot creation failed for " + clone_snap_name
                        )

                    # Encrypt the cloned image with LUKS2 format
                    image_spec_clone = f"{pool}/{clone_image}"
                    passphrase_clone = format_image(
                        rbd, c1_client, encryption_type_clone_image, image_spec_clone
                    )
                    log.info(
                        "Clone image formatted with luks2 using file "
                        + passphrase_clone
                    )

                    # get md5sum of the parent & clone image before migration for data consistency check
                    md5sum_list_before_migration = []
                    for i in [image_name, clone_image]:
                        md5_sum_before_migration = get_md5sum_rbd_image(
                            image_spec=f"{pool}/{i}",
                            rbd=rbd,
                            client=c1_client,
                            file_path=f"/tmp/{random_string(len=3)}",
                        )
                        log.info(
                            "md5sum before Migration of "
                            + i
                            + " : "
                            + md5_sum_before_migration
                        )
                        md5sum_list_before_migration.append(md5_sum_before_migration)

                    md5sum_list_after_migration = []
                    for i in [image_name, clone_image]:
                        if "clone" not in i:
                            snap_name = snap_name
                        else:
                            snap_name = clone_snap_name
                        # prepare migration source spec
                        source_spec_path = prepare_migration_source_spec(
                            cluster_name=c1,
                            client=c1_client,
                            pool_name=pool,
                            image_name=i,
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
                            raise Exception(
                                "Creation of target pool " + target_pool + " failed"
                            )

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
                            raise Exception("Failed to prepare migration")

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
                            raise Exception("Failed to execute migration")

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
                            raise Exception("Failed to commit migration")

                        # verify checksum post migration
                        md5_sum_after_migration = get_md5sum_rbd_image(
                            image_spec=f"{target_pool}/{target_image}",
                            rbd=rbd2,
                            client=c2_client,
                            file_path=f"/tmp/{random_string(len=5)}",
                        )
                        log.info("md5sum after migration: " + md5_sum_after_migration)
                        md5sum_list_after_migration.append(md5_sum_after_migration)

                    if md5sum_list_before_migration != md5sum_list_after_migration:
                        raise Exception(
                            "Data integrity check failed, md5sum checksums are not same"
                        )
                    log.info("md5sum checksum is same on both clusters after migration")

                except Exception as e:
                    log.error("Error during migration: " + str(e))
                    return 1

                finally:
                    if source_spec_path:
                        log.info("Cleaning up source spec path: " + source_spec_path)
                        out, err = c1_client.exec_command(
                            sudo=True, cmd=f"rm -f {source_spec_path}"
                        )
                        if err:
                            log.error("Failed to delete file " + source_spec_path)
                        for file in [passphrase_parent, passphrase_clone]:
                            out, err = c1_client.exec_command(
                                sudo=True, cmd=f"rm -f {file}"
                            )
                            if err:
                                log.error(
                                    "Failed to delete passphrase file "
                                    + file
                                    + " with error: "
                                    + err
                                )

                    pool_cleanup(
                        client=c2_client,
                        pools=pools_to_delete,
                        ceph_version=int(kw["config"].get("rhbuild")[0]),
                    )

    return 0


def format_image(rbd, c1_client, encryption_type, image_spec):
    # Format the parent image with encryption luks1
    passphrase = f"{encryption_type}_passphrase_" + random_string(len=3) + ".bin"
    create_passphrase_file(c1_client, passphrase)
    out, err = rbd.encryption_format(
        **{
            "image-spec": image_spec,
            "format": encryption_type,
            "passphrase-file": passphrase,
        }
    )
    if err:
        raise Exception(
            "Encryption format with "
            + encryption_type
            + " failed on "
            + image_spec
            + " with "
            + err
        )
    else:
        log.info(
            "Successfully formatted the image "
            + image_spec
            + " with encryption type "
            + encryption_type
        )
    return passphrase


def run(**kw):
    """
    Test to execute Live migration of parent image with LUKS1 and child image with LUKS2 with native data format
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
        "Executing CEPH-83598689 - Live migration of parent image with LUKS1 and child image with LUKS2"
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

            ret_val = test_migration_clone_encryption(
                rbd_obj=rbd_obj,
                c1_client=cluster1_client,
                c2_client=cluster2_client,
                **kw,
            )

            if ret_val == 0:
                log.info(
                    "Testing Live migration of parent image with LUKS1 and child image with LUKS2 Passed"
                )

    except Exception as e:
        log.error(
            "Live migration of parent image with LUKS1 and child image with LUKS2 failed with the error "
            + str(e)
        )
        ret_val = 1

    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(cluster1_client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)

    return ret_val
