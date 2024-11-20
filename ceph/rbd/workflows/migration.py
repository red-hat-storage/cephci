# -*- code: utf-8 -*-

import json
from logging import getLogger

from cli.rbd.rbd import Rbd

log = getLogger(__name__)


def verify_migration_state(action, image_spec, **kw):
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
    status_config = {"image-spec": image_spec, "format": "json"}
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
