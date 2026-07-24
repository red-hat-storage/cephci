"""Module to verify successful  live migration of images with external data source as
RAW data format from one ceph cluster to another ceph cluster with an NBD source

Test case covered -
CEPH-83598705: Live migration of sparse import with an NBD source of RAW data format

Pre-requisites:
- Two Ceph clusters deployed and accessible.
- A common client node configured to access both clusters.
- `ceph-common` package installed with live migration binaries available.

Test Case Flow:
1. Deploy two ceph clusters as source(cluster1) and destination(cluster2)
along with mon,mgr,osds
2. Install ceph-common package on one common client node
3. Create one client node common among both clusters by copying the both
the ceph.conf and ceph.client.admin.keyring from both the clusters
into the common client node.
4. Create one Replicated pool on both clusters and initialize it.
5. Create one RBD image inside that pool
6. Write some data to image using rbd bench or fio or file mount also note down
  it's md5sum for data consistency
7. Create a RAW formatted data for the RBD mapped disk externally using rbd export
8. Note down md5sum checksum of eaw formatted data.
9. Map the RBD image from Cluster1. Start the NBD server using the mapped RAW image
10. Create an NBD source spec file for the RAW image
11. Execute prepare migration with import-only option with --source-spec-path
  for RBD image migration
12. Initiate migration execution using the migration execute command
13. Commit the Migration using the migration commit command
14. Export the migrated image using rbd export command and note down it's md5sum checksum
15. Verify md5sum checksum of before migration and after migration should be same
16. unmount, unmap, cleanup the pools and images
17. Repeat the above test for EC pool
"""

import json
import tempfile
from copy import deepcopy

from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import (
    configure_common_client_node,
    get_md5sum_rbd_image,
    getdict,
    random_string,
)
from ceph.rbd.workflows.cleanup import cleanup, pool_cleanup
from ceph.rbd.workflows.migration import verify_migration_state
from ceph.rbd.workflows.rbd import (
    create_single_pool_and_images,
    run_io_and_check_rbd_status,
)
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def test_external_nbd_migration(rbd_obj, c1_client, c2_client, **kw):
    """
    Test to perform Live migration of sparse import with an NBD
    source of RAW data format from external ceph cluster
    Args:
        rbd_obj: rbd object
        c1_client: Cluster1 client node object
        c2_client: Cluster2 client node object
        kw: Key/value pairs of configuration information to be used in the test
    """
    c2 = "cluster2"
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
                try:
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

                    md5_sum_before_migration = get_md5sum_rbd_image(
                        image_spec=f"{pool}/{image_name}",
                        rbd=rbd,
                        client=c1_client,
                        file_path=f"/tmp/{random_string(len=3)}",
                        remove_file=True,
                    )
                    log.info(f"md5sum before Migration: {md5_sum_before_migration}")

                    unix_socket_path = "/tmp/nbd_socket_" + random_string(len=3)

                    out, err = rbd.map(**{"image-or-snap-spec": f"{pool}/{image_name}"})
                    device = out.strip()

                    out, err = c1_client.exec_command(
                        sudo=True,
                        cmd=f"qemu-nbd -f raw --shared 10 --persistent --fork --socket {unix_socket_path} {device}",
                    )
                    if err:
                        raise Exception(
                            "Failed to export the RBD device as NBD server over unix socket"
                        )
                        return 1
                    else:
                        log.info(
                            "Successfully exported the RBD device as NBD server over unix socket"
                        )

                    out, err = c1_client.exec_command(
                        sudo=True,
                        cmd=f"chmod 666 {unix_socket_path}",
                    )
                    if err:
                        raise Exception(
                            "Failed to change the permissions of unix socket"
                        )
                        return 1
                    else:
                        log.info("Successfully changed the permissions of unix socket")

                    kw["cleanup_files"].append(unix_socket_path)

                    nbd_source_spec = {
                        "type": "raw",
                        "stream": {
                            "type": "nbd",
                            "uri": f"nbd+unix:///?socket={unix_socket_path}",
                        },
                    }

                    temp_file = tempfile.NamedTemporaryFile(dir="/tmp", suffix=".json")
                    spec_file = c1_client.remote_file(
                        sudo=True, file_name=temp_file.name, file_mode="w"
                    )
                    spec_file.write(json.dumps(nbd_source_spec, indent=4))
                    spec_file.flush()
                    spec_path = temp_file.name
                    kw["cleanup_files"].append(spec_path)

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
                        source_spec_path=spec_path,
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

                    md5_sum_after_migration = get_md5sum_rbd_image(
                        image_spec=f"{target_pool}/{target_image}",
                        rbd=rbd2,
                        cluster_name=c2,
                        client=c2_client,
                        file_path=f"/tmp/{random_string(len=3)}",
                        remove_file=True,
                    )
                    log.info(f"md5sum after Migration: {md5_sum_after_migration}")

                    if md5_sum_before_migration != md5_sum_after_migration:
                        raise Exception("Data on the migrated image is inconsistent")
                    else:
                        log.info(
                            "Data on the migrated image is consistent with the source"
                        )

                except Exception as e:
                    log.error(f"Error during migration: {str(e)}")
                    return 1

                finally:
                    pool_cleanup(
                        client=c2_client,
                        pools=pools_to_delete,
                        ceph_version=int(kw["config"].get("rhbuild")[0]),
                    )

    return 0


def run(**kw):
    """
    Test to execute Live migration of sparse import with an NBD
    source of RAW data format from external ceph cluster.
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
        "Executing CEPH-83598705: Live migration of sparse import "
        "with an NBD source of raw data format"
    )

    try:
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        kw.update({"cleanup_files": []})

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

            ret_val = test_external_nbd_migration(
                rbd_obj=rbd_obj,
                c1_client=cluster1_client,
                c2_client=cluster2_client,
                **kw,
            )

            if ret_val == 0:
                log.info(
                    "Testing RBD image migration with NBD source as raw format Passed"
                )

    except Exception as e:
        log.error(
            f"RBD image migration with NBD source as raw format failed with the error {str(e)}"
        )
        ret_val = 1

    finally:
        try:
            for file in kw["cleanup_files"]:
                out, err = cluster1_client.exec_command(sudo=True, cmd=f"rm -f {file}")
                if err:
                    log.error(f"Failed to delete file {file}")
        except Exception as e:
            log.error(f"Failed to cleanup temp files with err {e}")
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(cluster1_client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)

    return ret_val
