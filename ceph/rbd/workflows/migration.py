import json
import tempfile

from ceph.rbd.utils import random_string
from ceph.rbd.workflows.rbd import create_single_pool_and_images
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def verify_migration_state(action, image_spec, cluster_name="ceph", **kw):
    """verify the migration status at each action.

    This method will verify the migration state for an image for
    destination pool after executingprepare migration and
    execute migration steps for live image migration.

    Args:
        action: prepare or execute
        image_spec: pool_name/image_name
        kw: Key/value pairs of test configuration

    Returns:
        0: If migration state is as expected
        1: If migration state is not as expected
    """
    rbd = Rbd(kw["client"])
    log.info("verifying migration state")
    status_config = {
        "image-spec": image_spec,
        "cluster": cluster_name,
        "format": "json",
    }
    out, err = rbd.status(**status_config)
    log.info(out)
    status = json.loads(out)
    try:
        if action == "prepare" and "prepared" in status["migration"]["state"]:
            log.info(f"Live Migration successfully prepared for {image_spec}")
        elif action == "execute" and "executed" in status["migration"]["state"]:
            log.info(f"Live migration successfully executed for {image_spec}")
        elif action == "commit" and "migration" not in status:
            log.info(f"Live migration successfully committed for {image_spec}")
        return 0
    except Exception as error:
        log.error(error)
        return 1


def prepare_migration_source_spec(
    cluster_name, client, pool_name, image_name, snap_name, namespace_name=None
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
    if namespace_name is not None:
        native_spec["pool_namespace"] = namespace_name

    temp_file = tempfile.NamedTemporaryFile(dir="/tmp", suffix=".json")
    spec_file = client.remote_file(sudo=True, file_name=temp_file.name, file_mode="w")
    spec_file.write(json.dumps(native_spec, indent=4))
    spec_file.flush()

    return temp_file.name


def run_prepare_execute_commit(rbd, pool, image, **kw):
    """
    Function to carry out the following:
      - Create Target/destination pool for migration
      - Migration prepare
      - Migration Execute
      - Migration commit
    Args:
        kw: rbd object, pool, image, test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    # Create Target Pool/ Destination Pool for migration
    is_ec_pool = True if "ec" in kw[pool]["pool_type"] else False
    config = kw.get("config", {})
    target_pool = "target_pool_" + random_string(len=3)
    target_pool_config = {}
    if is_ec_pool:
        data_pool_target = "data_pool_new_" + random_string(len=3)
        target_pool_config["data_pool"] = data_pool_target
    rc = create_single_pool_and_images(
        config=config,
        pool=target_pool,
        pool_config=target_pool_config,
        client=kw["client"],
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
    if kw[pool]["pool_type"] == "rep_pool_config":
        kw["config"]["rep_pool_config"][target_pool] = {}
    elif kw[pool]["pool_type"] == "ec_pool_config":
        kw["config"]["ec_pool_config"][target_pool] = {"data_pool": data_pool_target}

    # Prepare Migration
    target_image = "target_image_" + random_string(len=3)
    rbd.migration.prepare(
        source_spec=kw[pool]["spec"],
        dest_spec=f"{target_pool}/{target_image}",
        client_node=kw["client"],
    )
    kw[pool].update({"target_pool": target_pool})
    kw[pool].update({"target_image": target_image})

    # Verify prepare migration status
    if verify_migration_state(
        action="prepare",
        image_spec=f"{target_pool}/{target_image}",
        **kw,
    ):
        log.error("Failed to prepare migration")
        return 1
    else:
        log.info("Migration prepare status verified successfully")

    # execute migration
    rbd.migration.action(
        action="execute",
        dest_spec=f"{target_pool}/{target_image}",
        client_node=kw["client"],
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

    # commit migration
    rbd.migration.action(
        action="commit",
        dest_spec=f"{target_pool}/{target_image}",
        client_node=kw["client"],
    )

    # verify commit migration status
    if verify_migration_state(
        action="commit",
        image_spec=f"{target_pool}/{target_image}",
        **kw,
    ):
        log.error("Failed to commit migration")
        return 1
    else:
        log.info("Migration committed successfully")
