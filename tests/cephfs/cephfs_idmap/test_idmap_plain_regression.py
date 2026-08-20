"""
TC-S5: Regression — plain kernel mount without idmap.
"""

import traceback

from tests.cephfs.cephfs_idmap.lib.cephfs_idmap_lib import (
    IdmapTestHelper,
    init_idmap_test,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """TC-S5 — confirm non-idmapped workloads are unaffected."""
    plain_mount = None
    try:
        _config, _test_data, build, clients, helper = init_idmap_test(ceph_cluster, kw)
        helper.prepare_clients(clients, build)
        client = clients[0]

        plain_mount, _idmap, _suffix = helper.mount_paths()
        helper.kernel_mount_plain(client, plain_mount)

        legacy = f"{plain_mount}/legacy.txt"
        helper.exec_cmd(client, f'echo "legacy write test" > {legacy}')
        helper.exec_cmd(client, f"chown 2000:2000 {legacy}")
        helper.assert_stat_uid_gid(client, legacy, 2000, 2000)
        helper.exec_cmd(client, f"cat {legacy}")
        helper.assert_dmesg_clean(client)

        log.info("TC-S5 plain mount regression passed")
        return 0

    except Exception as exc:
        log.error("TC-S5 failed: %s", exc)
        log.error(traceback.format_exc())
        clients = ceph_cluster.get_ceph_objects("client")
        if clients:
            IdmapTestHelper(ceph_cluster).capture_failure_artifacts(clients[0])
        return 1

    finally:
        if plain_mount and clients:
            IdmapTestHelper(ceph_cluster).umount_plain(clients[0], plain_mount)
