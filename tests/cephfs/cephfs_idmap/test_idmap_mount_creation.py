"""
TC-S2: Idmapped CephFS mount creation (core blocker test).
"""

import traceback

from tests.cephfs.cephfs_idmap.lib.cephfs_idmap_lib import (
    IDMAP_BASE_UID,
    IdmapTestHelper,
    init_idmap_test,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """TC-S2 — verify CephFS accepts an idmapped bind mount."""
    plain_mount = idmap_mount = None
    try:
        _config, _test_data, build, clients, helper = init_idmap_test(ceph_cluster, kw)
        helper.prepare_clients(clients, build)
        client = clients[0]

        plain_mount, idmap_mount, _suffix = helper.setup_plain_and_idmap_mounts(client)
        marker = f"{idmap_mount}/idmap-ok"
        # Host root (uid 0) on the idmap view creates on-disk 100000:100000.
        # setup_plain_and_idmap_mounts() chowns the plain root into the idmap
        # range first; without that, on-disk 0:0 maps to overflow uid 65534 and
        # touch returns EOVERFLOW (Ceph tracker #62217).
        helper.exec_cmd(client, f"touch {marker}")
        helper.exec_cmd(client, f"test -f {plain_mount}/idmap-ok")
        helper.assert_stat_uid_gid(
            client, f"{plain_mount}/idmap-ok", IDMAP_BASE_UID, IDMAP_BASE_UID
        )
        helper.assert_dmesg_clean(client)

        log.info("TC-S2 idmapped mount creation passed")
        return 0

    except Exception as exc:
        log.error("TC-S2 failed: %s", exc)
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
