"""
TC-S7: POSIX chmod, chown, chgrp, and directory CRUD on idmapped CephFS.
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
    """TC-S7 — standard POSIX operations on idmapped CephFS."""
    plain_mount = idmap_mount = None
    try:
        _config, _test_data, build, clients, helper = init_idmap_test(ceph_cluster, kw)
        helper.prepare_clients(clients, build)
        client = clients[0]

        plain_mount, idmap_mount, _suffix = helper.setup_plain_and_idmap_mounts(client)
        base = f"{idmap_mount}/posix-test"
        helper.exec_cmd(client, f"mkdir -p {base}/dir1/subdir1")

        file1 = f"{base}/dir1/file1.txt"
        renamed = f"{base}/dir1/file1-renamed.txt"
        helper.exec_cmd(client, f"touch {file1}")
        helper.exec_cmd(client, f'echo "posix-test-content" > {file1}')
        helper.exec_cmd(client, f"mv {file1} {renamed}")
        helper.exec_cmd(client, f"cp {renamed} {base}/dir1/file1-copy.txt")
        helper.exec_cmd(client, f"chmod 640 {renamed}")
        helper.exec_cmd(client, f"chmod 755 {base}/dir1/subdir1")

        chown_test = f"{base}/dir1/chown-test.txt"
        helper.exec_cmd(client, f"touch {chown_test}")
        helper.exec_cmd(client, f"chown 1001:1001 {chown_test}")
        helper.exec_cmd(client, f"chgrp 1001 {renamed}")

        helper.exec_cmd(client, f"mkdir -p {base}/dir-to-delete/nested")
        helper.exec_cmd(client, f"touch {base}/dir-to-delete/nested/file.txt")
        helper.exec_cmd(client, f"rm -rf {base}/dir-to-delete")

        # file1-renamed: created as root (uid 100000 on disk), then chgrp 1001 only
        helper.assert_stat_uid_gid(
            client,
            f"{plain_mount}/posix-test/dir1/file1-renamed.txt",
            IDMAP_BASE_UID,
            IDMAP_BASE_UID + 1001,
        )
        helper.assert_stat_uid_gid(
            client,
            f"{plain_mount}/posix-test/dir1/chown-test.txt",
            IDMAP_BASE_UID + 1001,
            IDMAP_BASE_UID + 1001,
        )

        log.info("TC-S7 POSIX operations passed")
        return 0

    except Exception as exc:
        log.error("TC-S7 failed: %s", exc)
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
