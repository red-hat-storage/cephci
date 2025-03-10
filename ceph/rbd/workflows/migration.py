import json
import tempfile

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
