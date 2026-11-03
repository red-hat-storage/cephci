"""Suite step: rotate client.admin or a non-admin client (config.target)."""

import traceback

from cli.exceptions import OperationFailedError
from tests.cephfs.cephfs_upgrade.cephfs_cephx_key_helpers import (
    DEFAULT_KEY_TYPE,
    DEFAULT_UPGRADE_CONFIG_PATH,
    _auth_rotate_available,
    _cephadm_shell,
    _deploy_client_credentials,
    _get_entity_key,
    _mask_key,
    _rotate_client_admin,
    admin_keyring_nodes,
    client_by_entity,
    load_cephx_mount_recipes,
    recipes_for_entity,
)
from utility.log import Log

log = Log(__name__)


def _rotate_admin(ceph_cluster, **kw):
    """Config keys: key_type."""
    config = kw.get("config") or {}
    key_type = config.get("key_type", DEFAULT_KEY_TYPE)
    installer = ceph_cluster.get_nodes("installer")[0]
    clients = ceph_cluster.get_ceph_objects("client")
    # Remount/new-mount steps run ``ceph auth get*`` on client nodes;
    # those use local admin keyring — must refresh installer + all clients.
    key_nodes = admin_keyring_nodes(ceph_cluster, clients)

    if not _auth_rotate_available(installer):
        raise OperationFailedError("ceph auth rotate not available on this build")

    log.info(
        "=== rotate client.admin; refresh admin key file on %s ===",
        [n.hostname for n in key_nodes],
    )
    _rotate_client_admin(installer, key_nodes, key_type)
    log.info("client.admin rotate PASSED")


def _rotate_client(ceph_cluster, **kw):
    """Config keys: upgrade_config_path, key_type, rotate_client."""
    config = kw.get("config") or {}
    config_path = config.get("upgrade_config_path", DEFAULT_UPGRADE_CONFIG_PATH)
    key_type = config.get("key_type", DEFAULT_KEY_TYPE)
    entity = config.get("rotate_client", "client.2")
    clients = ceph_cluster.get_ceph_objects("client")
    installer = ceph_cluster.get_nodes("installer")[0]
    recipes = load_cephx_mount_recipes(clients[0], path=config_path)
    recipes_for_entity(recipes, entity)  # ensure mounts exist for entity
    client = client_by_entity(clients, entity)

    if not _auth_rotate_available(installer):
        raise OperationFailedError("ceph auth rotate not available on this build")

    before = _get_entity_key(installer, entity)
    log.info("Rotating %s (baseline %s)", entity, _mask_key(before))
    _cephadm_shell(installer, f"ceph auth rotate {entity} --key-type {key_type}")
    after = _get_entity_key(installer, entity)
    if before == after:
        raise OperationFailedError(f"Key unchanged after rotate for {entity}")
    _deploy_client_credentials(installer, client, entity)
    log.info("%s rotate + credential deploy PASSED", entity)


WORKFLOWS = {
    "admin": _rotate_admin,
    "client": _rotate_client,
}


def run(ceph_cluster, **kw):
    """Config keys: target (admin|client), plus workflow-specific keys."""
    try:
        config = kw.get("config") or {}
        target = str(config.get("target", "client")).strip().lower()
        if target not in WORKFLOWS:
            raise OperationFailedError(
                f"Unknown rotate target {target!r}; use admin|client"
            )
        WORKFLOWS[target](ceph_cluster, **kw)
        log.info("CephX rotate PASSED target=%s", target)
        return 0
    except Exception as exc:
        log.error(exc)
        log.error(traceback.format_exc())
        return 1
