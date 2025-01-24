"""Module to verify Rollback after Failed Migration

Test case covered -
CEPH-83584090 - Rollback after Failed Migration using migration abort in single ceph cluster

Pre-requisites :
1. Cluster must be up and running with capacity to create pool
2. We need atleast one client node with ceph-common package,
   conf and keyring files

Test Case Flow:
1.Deploy single ceph cluster along with mon,mgr,osd’s
2. Create two pools with rbd application enabled  for migration as source and destination pool
3.Store the external qcow2 data in json spec
    E.g: testspec.json
    {
        "type": "qcow",
        "stream": {"type": "http", "url": f"https://download.ceph.com/qa/ubuntu-12.04.qcow2"},
    }
4.Execute prepare migration with import-only option for RBD image from an
  external source of qcowdata  (specified by the JSON string in the echo command)
  E.g:
  echo '{"type":"qcow","stream":{"type":"http","url":"https://download.ceph.com/qa/ubuntu-12.04.raw"}}'
    | rbd migration prepare --import-only --source-spec-path - <pool_name>/<image_name> --cluster <cluster-source>
5. Initiate migration execute using migration execute command
   E.g:
   rbd migration execute TARGET_POOL_NAME/SOURCE_IMAGE_NAME
6. Intentionally cause some failure to the cluster either osd node reboot or network failure
7. Check the progress of migration using rbd status
8. Attempt to rollback the failed migration using migration abort command
9. verify the target image does not exist


"""

from copy import deepcopy

from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import getdict, random_string
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.migration import verify_migration_state
from ceph.rbd.workflows.rbd import create_single_pool_and_images
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from tests.rbd.rbd_utils import Rbd as rbdutils
from utility.log import Log

log = Log(__name__)


def rollback_migration(rbd_obj, client, **kw):
    """
        Test to verify Rollback using Migration abort
    Args:
        rbd_obj: RBD object
        client : client node object
        **kw: any other arguments
    """

    kw["client"] = client
    rbd = rbd_obj.get("rbd")

    for pool_type in rbd_obj.get("pool_types"):
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = deepcopy(getdict(rbd_config))

        for pool, pool_config in multi_pool_config.items():
            kw["pool-name"] = pool

            # Create an RBD image in pool
            image = "image_" + random_string(len=4)
            out, err = rbd.create(**{"image-spec": f"{pool}/{image}", "size": "5G"})
            if err:
                log.error(f"Create image {pool}/{image} failed with error {err}")
                return 1
            else:
                log.info(f"Successfully created image {pool}/{image}")

            qcow_spec = {
                "type": "qcow",
                "stream": {
                    "type": "http",
                    "url": "https://download.ceph.com/qa/ubuntu-12.04.qcow2",
                },
            }
            client.exec_command(
                cmd="sudo curl -o /mnt/ubuntu-12.04.qcow2 http://download.ceph.com/qa/ubuntu-12.04.qcow2",
                long_running=True,
            )
            out, err = client.exec_command(
                cmd="sudo du -h /mnt/ubuntu-12.04.qcow2", output=True
            )
            qcow_data_size = out.split()[0]

            client.exec_command(
                cmd="sudo rm -rf /mnt/ubuntu-12.04.qcow2",
            )

            # Create a target pool where the image is to be migrated
            is_ec_pool = True if "ec" in pool_type else False
            config = kw.get("config", {})
            target_pool = "target_pool_" + random_string()
            target_pool_config = {}
            if is_ec_pool:
                data_pool_target = "data_pool_new_" + random_string()
                target_pool_config["data_pool"] = data_pool_target
            rc = create_single_pool_and_images(
                config=config,
                pool=target_pool,
                pool_config=target_pool_config,
                client=client,
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
            target_image = "target_image_" + random_string()
            rbd.migration.prepare(
                source_spec=qcow_spec,
                dest_spec=f"{target_pool}/{target_image}",
                client_node=client,
                long_running=True,
            )

            # Verify prepare migration status
            if verify_migration_state(
                action="prepare",
                image_spec=f"{target_pool}/{target_image}",
                **kw,
            ):
                log.error("Failed to prepare migration")
                return 1
            else:
                log.info("Migration prepare status verfied successfully")

            # # execute migration
            rbd.migration.action(
                action="execute",
                dest_spec=f"{target_pool}/{target_image}",
                client_node=client,
                long_running=True,
            )

            # verify execute migration status
            if verify_migration_state(
                action="execute",
                image_spec=f"{target_pool}/{target_image}",
                **kw,
            ):
                log.error("Failed to execute migration")
                return 1
            else:
                log.info("Migration executed successfully")

            image_config = {"image-spec": f"{target_pool}/{target_image}"}

            out = rbd.image_usage(**image_config)
            image_data = out[0]

            migrated_image_size = image_data.split("\n")[1].split()[3].strip() + "G"

            log.info(f"External qcow data format original size {qcow_data_size}")
            log.info(
                f"After migration to RBD image qcow data size {migrated_image_size}"
            )

            # Verification of external data migration.
            if migrated_image_size == qcow_data_size:
                log.info(
                    "Image size after Migration Execute is same as the size of external source"
                )
            else:
                log.error(
                    "Image size after Migration Execute is not same as the size of external source"
                )
                return 1

            # Abort the migration
            rbd.migration.action(
                action="abort",
                dest_spec=f"{target_pool}/{target_image}",
                client_node=client,
            )
            log.info("Migration abort executed successfully")

            # verify target image does not exist after abort
            rbdutil = rbdutils(**kw)
            if rbdutil.image_exists(target_pool, target_image):
                log.error(
                    f"Image still exist after aborting the image migration in pool {target_pool}"
                )
                return 1
            else:
                log.info(
                    f"Image {target_image} is not found in pool {target_pool} after aborting migration"
                )
    return 0


def run(**kw):
    """
    This test verifies Rollback using migration abort
    Args:
        kw: test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    try:
        log.info(
            "CEPH-83584090 - Rollback after Failed Migration using migration abort in single ceph cluster"
        )

        if kw.get("client_node"):
            client = get_node_by_id(kw.get("ceph_cluster"), kw.get("client_node"))
        else:
            client = kw.get("ceph_cluster").get_nodes(role="client")[0]
        rbd_obj = initial_rbd_config(**kw)
        pool_types = rbd_obj.get("pool_types")
        kw.update({"cleanup_files": []})
        if rbd_obj:
            log.info("Executing test on Replicated and EC pool")
            if rollback_migration(rbd_obj, client, **kw):
                return 1
            log.info(
                "Test Rollback after Failed Migration using migration abort passed"
            )

    except Exception as e:
        log.error(
            f"Test Rollback after Failed Migration using migration abort failed: {str(e)}"
        )
        return 1

    finally:
        try:
            for file in kw["cleanup_files"]:
                out, err = client.exec_command(sudo=True, cmd=f"rm -f {file}")
                if err:
                    log.error(f"Failed to delete file {file}")
        except Exception as e:
            log.error(f"Failed to cleanup temp files with err {e}")
        cluster_name = kw.get("ceph_cluster", {}).name
        if "rbd_obj" not in locals():
            rbd_obj = Rbd(client)
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)

    return 0
