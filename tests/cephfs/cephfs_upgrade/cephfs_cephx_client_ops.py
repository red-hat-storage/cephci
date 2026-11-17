"""
Suite step: CephX client post-upgrade ops driven by config.action.

Actions:
  pre_io      — light IO on all CephX recipe mounts (pre-upgrade)
  holdout     — client.1 existing + new mounts with old credentials
  remount     — umount then remount recipe mounts
  reboot      — reboot client; optional fstab mount verify
  mount_io    — probe IO on remounted or new mounts (mount_set)
  new_mounts  — create new kernel/fuse mounts with rotated credentials
"""

import shlex
import time
import traceback

from cli.exceptions import OperationFailedError
from tests.cephfs.cephfs_upgrade.cephfs_cephx_key_helpers import (
    DEFAULT_UPGRADE_CONFIG_PATH,
    _cleanup_probes,
    _probe_io,
    _remount_recipe,
    _umount_cephfs,
    assert_paths_mounted,
    client_by_entity,
    load_cephx_mount_recipes,
    load_cephx_new_mounts,
    load_cephx_section,
    mount_at,
    recipes_for_entity,
    update_cephx_section,
)
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

DEFAULT_REBOOT_TIMEOUT = 900  # 15 minutes
# Extra wait after SSH reconnects before polling fstab mounts.
# SSH becomes available ~20-40 s after boot; _netdev remote-fs.target
# mounts are brought up by systemd only after network-online.target
# completes, which typically takes 30-90 s more.
_POST_RECONNECT_SETTLE = 30  # seconds


def _pre_io(ceph_cluster, **kw):
    """Config keys: upgrade_config_path."""
    config = kw.get("config") or {}
    config_path = config.get("upgrade_config_path", DEFAULT_UPGRADE_CONFIG_PATH)
    clients = ceph_cluster.get_ceph_objects("client")
    recipes = load_cephx_mount_recipes(clients[0], path=config_path)
    for r in recipes:
        num = int(r.entity().split(".", 1)[1])
        client = clients[num - 1]
        tag = f"preio_{r.mount_kind}"
        _probe_io(client, r.mnt_pt, tag=tag)
        log.info("OK IO %s on %s", r.entity(), r.mnt_pt)
    log.info("CephX pre-upgrade IO PASSED on %d mounts", len(recipes))


def _holdout(ceph_cluster, **kw):
    """
    Client1 only: IO on existing mounts; create new kernel+fuse mounts with
    the same old client.1 credentials (no dnf, no auth rotate).

    Config keys: holdout_client (default client.1), upgrade_config_path.
    """
    config = kw.get("config") or {}
    created = []
    client = None
    try:
        config_path = config.get("upgrade_config_path", DEFAULT_UPGRADE_CONFIG_PATH)
        holdout_entity = config.get("holdout_client", "client.1")
        fs_util = FsUtils(ceph_cluster, test_data=kw.get("test_data"))
        clients = ceph_cluster.get_ceph_objects("client")
        recipes = load_cephx_mount_recipes(clients[0], path=config_path)
        c1_recipes = [r for r in recipes if r.entity() == holdout_entity]
        if not c1_recipes:
            raise OperationFailedError(
                f"No mounts in CephX config for {holdout_entity}"
            )
        client = client_by_entity(clients, holdout_entity)

        log.info("=== Client1 holdout: existing mount IO ===")
        for r in c1_recipes:
            tag = f"holdout_exist_{r.mount_kind}"
            _probe_io(client, r.mnt_pt, tag=tag)
            log.info("OK existing IO on %s", r.mnt_pt)

        log.info("=== Client1 holdout: new mounts with old key ===")
        mon_ips = ",".join(fs_util.get_mon_node_ips())
        by_kind = {r.mount_kind: r for r in c1_recipes}
        for kind, base in by_kind.items():
            new_mnt = f"/mnt/cephx_holdout_new_{kind}"
            client.exec_command(sudo=True, cmd=f"mkdir -p {shlex.quote(new_mnt)}")
            mount_at(fs_util, client, base, new_mnt, mon_ips)
            created.append(new_mnt)
            try:
                _probe_io(client, new_mnt)
            finally:
                _cleanup_probes(client, new_mnt)
            log.info("OK new %s mount at %s", kind, new_mnt)

        log.info("Client1 holdout PASSED")
    finally:
        # Drop holdout-only mounts so a failed/partial run does not leave
        # busy mount points (success path also cleans; not reused later).
        if client is not None:
            for m in created:
                client.exec_command(
                    sudo=True,
                    cmd=f"umount -l {shlex.quote(m)} || true",
                    timeout=120,
                    check_ec=False,
                )


def _remount(ceph_cluster, **kw):
    """Config keys: upgrade_config_path, client."""
    config = kw.get("config") or {}
    config_path = config.get("upgrade_config_path", DEFAULT_UPGRADE_CONFIG_PATH)
    entity = config.get("client", "client.2")
    fs_util = FsUtils(ceph_cluster, test_data=kw.get("test_data"))
    clients = ceph_cluster.get_ceph_objects("client")
    recipes = recipes_for_entity(
        load_cephx_mount_recipes(clients[0], path=config_path), entity
    )
    client = client_by_entity(clients, entity)

    log.info("=== umount %s recipe mounts ===", entity)
    for r in recipes:
        _umount_cephfs(client, r.mnt_pt)

    log.info("=== remount %s recipe mounts ===", entity)
    for r in recipes:
        _remount_recipe(fs_util, client, r)
        log.info("Remounted %s at %s", r.mount_kind, r.mnt_pt)

    log.info("%s umount/remount PASSED (%s mounts)", entity, len(recipes))


def _reboot(ceph_cluster, **kw):
    """Config keys: upgrade_config_path, client, reboot_timeout, verify_mounts."""
    config = kw.get("config") or {}
    config_path = config.get("upgrade_config_path", DEFAULT_UPGRADE_CONFIG_PATH)
    entity = config.get("client", "client.3")
    timeout = int(config.get("reboot_timeout", DEFAULT_REBOOT_TIMEOUT))
    verify_mounts = bool(config.get("verify_mounts", True))
    settle = int(config.get("post_reboot_settle", _POST_RECONNECT_SETTLE))
    fs_util = FsUtils(ceph_cluster, test_data=kw.get("test_data"))
    clients = ceph_cluster.get_ceph_objects("client")
    client = client_by_entity(clients, entity)
    recipes = recipes_for_entity(
        load_cephx_mount_recipes(clients[0], path=config_path), entity
    )

    log.info(
        "=== reboot %s (%s); wait up to %ss ===",
        entity,
        client.node.hostname,
        timeout,
    )
    fs_util.reboot_node(client, timeout=timeout)
    log.info(
        "%s SSH back after reboot; settling %ss for _netdev fstab mounts",
        client.node.hostname,
        settle,
    )
    time.sleep(settle)

    if verify_mounts:
        assert_paths_mounted(client, [r.mnt_pt for r in recipes])

    log.info("%s reboot PASSED", entity)


def _mount_io(ceph_cluster, **kw):
    """
    Config keys: upgrade_config_path, client, mount_set (remounted|new),
    verify_mounted.
    """
    config = kw.get("config") or {}
    config_path = config.get("upgrade_config_path", DEFAULT_UPGRADE_CONFIG_PATH)
    entity = config.get("client", "client.2")
    mount_set = (config.get("mount_set") or "remounted").lower()
    clients = ceph_cluster.get_ceph_objects("client")
    client = client_by_entity(clients, entity)

    if mount_set == "new":
        targets = [
            m["mnt_pt"]
            for m in load_cephx_new_mounts(clients[0], path=config_path)
            if m.get("client_entity", entity) == entity
        ]
        if not targets:
            raise OperationFailedError(f"No CephX.new_mounts for {entity}")
    elif mount_set == "remounted":
        recipes = recipes_for_entity(
            load_cephx_mount_recipes(clients[0], path=config_path), entity
        )
        targets = [r.mnt_pt for r in recipes]
    else:
        raise OperationFailedError(
            f"Unknown mount_set {mount_set!r}; use remounted|new"
        )

    if bool(config.get("verify_mounted", False)):
        assert_paths_mounted(client, targets)

    log.info("=== IO on %s (%s) mounts ===", entity, mount_set)
    for mnt_pt in targets:
        try:
            _probe_io(client, mnt_pt)
        finally:
            _cleanup_probes(client, mnt_pt)
        log.info("OK IO on %s", mnt_pt)

    log.info("%s %s mount IO PASSED", entity, mount_set)


def _new_mounts(ceph_cluster, **kw):
    """Config keys: upgrade_config_path, client."""
    config = kw.get("config") or {}
    new_mounts = []
    client = None
    try:
        config_path = config.get("upgrade_config_path", DEFAULT_UPGRADE_CONFIG_PATH)
        entity = config.get("client", "client.2")
        fs_util = FsUtils(ceph_cluster, test_data=kw.get("test_data"))
        clients = ceph_cluster.get_ceph_objects("client")
        recipes = recipes_for_entity(
            load_cephx_mount_recipes(clients[0], path=config_path), entity
        )
        client = client_by_entity(clients, entity)
        entity_short = entity.split(".", 1)[1]
        mon_ips = ",".join(fs_util.get_mon_node_ips())

        by_kind = {r.mount_kind: r for r in recipes}
        log.info("=== new kernel/fuse mounts for %s ===", entity)
        for kind, base in by_kind.items():
            new_mnt = f"/mnt/cephx_c{entity_short}_new_{kind}"
            client.exec_command(sudo=True, cmd=f"mkdir -p {shlex.quote(new_mnt)}")
            mount_at(fs_util, client, base, new_mnt, mon_ips)
            new_mounts.append(
                {
                    "mnt_pt": new_mnt,
                    "client_entity": entity,
                    "mount_kind": kind,
                }
            )
            log.info("Created new %s mount at %s", kind, new_mnt)

        # Merge with other clients' new_mounts (safe if C2/C3 run sequentially).
        existing = [
            m
            for m in (
                load_cephx_section(clients[0], path=config_path).get("new_mounts") or []
            )
            if m.get("client_entity") != entity
        ]
        existing.extend(new_mounts)
        update_cephx_section(clients[0], {"new_mounts": existing}, path=config_path)

        log.info("%s new mounts PASSED (%s)", entity, len(new_mounts))
    except Exception:
        if client is not None:
            for m in new_mounts:
                client.exec_command(
                    sudo=True,
                    cmd=f"umount -l {shlex.quote(m['mnt_pt'])}",
                    timeout=120,
                    check_ec=False,
                )
        raise


ACTIONS = {
    "pre_io": _pre_io,
    "holdout": _holdout,
    "remount": _remount,
    "reboot": _reboot,
    "mount_io": _mount_io,
    "new_mounts": _new_mounts,
}


def run(ceph_cluster, **kw):
    """Config keys: action, plus action-specific keys."""
    try:
        config = kw.get("config") or {}
        action = config.get("action")
        if action not in ACTIONS:
            raise OperationFailedError(
                f"Unknown action {action!r}; use " f"{'|'.join(sorted(ACTIONS))}"
            )
        ACTIONS[action](ceph_cluster, **kw)
        log.info("CephX client_ops PASSED action=%s", action)
        return 0
    except Exception as exc:
        log.error(exc)
        log.error(traceback.format_exc())
        return 1
