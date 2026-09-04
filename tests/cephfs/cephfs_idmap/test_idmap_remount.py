"""
TC-S9: Remount / client recovery for idmapped CephFS mounts.
"""

import traceback

from tests.cephfs.cephfs_idmap.lib.cephfs_idmap_lib import (
    DEFAULT_IDMAP_SPEC,
    IDMAP_BASE_UID,
    IdmapTestHelper,
    init_idmap_test,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """TC-S9 — idmap behavior survives umount/remount."""
    plain_mount = idmap_mount = None
    try:
        _config, _test_data, build, clients, helper = init_idmap_test(ceph_cluster, kw)
        helper.prepare_clients(clients, build)
        client = clients[0]

        plain_mount, idmap_mount, suffix = helper.setup_plain_and_idmap_mounts(client)
        marker = f"{idmap_mount}/remount-test.txt"
        helper.exec_cmd(client, f'echo "pre-remount-marker" > {marker}')
        helper.exec_cmd(client, f"cat {marker}")

        helper.umount_idmap_stack(client, idmap_mount, plain_mount)
        plain_mount, idmap_mount, _suffix = helper.setup_plain_and_idmap_mounts(
            client, map_spec=DEFAULT_IDMAP_SPEC, suffix=suffix
        )

        out, _err, _rc = helper.exec_cmd(client, f"cat {idmap_mount}/remount-test.txt")
        if "pre-remount-marker" not in (out or ""):
            raise AssertionError("Marker file missing after remount")

        helper.exec_cmd(
            client, f'echo "post-remount-write" > {idmap_mount}/post-remount.txt'
        )
        helper.assert_stat_uid_gid(
            client, f"{plain_mount}/post-remount.txt", IDMAP_BASE_UID, IDMAP_BASE_UID
        )

        log.info("TC-S9 remount recovery passed")
        return 0

    except Exception as exc:
        log.error("TC-S9 failed: %s", exc)
        log.error(traceback.format_exc())
        clients = ceph_cluster.get_ceph_objects("client")
        if clients:
            IdmapTestHelper(ceph_cluster).capture_failure_artifacts(clients[0])
        return 1

    finally:
        if plain_mount and idmap_mount and clients:
            IdmapTestHelper(ceph_cluster).umount_idmap_stack(
                clients[0], idmap_mount, plain_mount
            )
