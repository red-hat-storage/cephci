"""
TC-S6: Multi-client RWX sharing with idmapped mounts.
"""

import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_idmap.lib.cephfs_idmap_lib import (
    IDMAP_BASE_UID,
    IdmapTestHelper,
    init_idmap_test,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """TC-S6 — shared files across two clients with the same idmap."""
    mounts = []
    try:
        _config, _test_data, build, clients, helper = init_idmap_test(ceph_cluster, kw)
        if len(clients) < 2:
            raise CommandFailed(
                f"TC-S6 requires at least 2 clients, found {len(clients)}"
            )

        helper.prepare_clients(clients, build)
        client_a, client_b = clients[0], clients[1]
        suffix = helper.random_suffix()
        shared = "shared"

        for client in (client_a, client_b):
            plain = f"/mnt/cephfs_idmap_plain_{suffix}"
            idmap = f"/mnt/cephfs_idmap_view_{suffix}"
            helper.kernel_mount_plain(client, plain)
            helper.idmap_bind_mount(client, plain, idmap)
            helper.exec_cmd(client, f"mkdir -p {idmap}/{shared}")
            mounts.append((client, plain, idmap))

        idmap_a = mounts[0][2]
        idmap_b = mounts[1][2]
        plain_a = mounts[0][1]

        helper.exec_cmd(
            client_a,
            f'echo "data from client-A" > {idmap_a}/{shared}/from-A.txt',
        )
        out, _err, _rc = helper.exec_cmd(client_b, f"cat {idmap_b}/{shared}/from-A.txt")
        if "data from client-A" not in (out or ""):
            raise AssertionError("Client-B could not read Client-A file")

        helper.exec_cmd(
            client_b,
            f'echo "data from client-B" > {idmap_b}/{shared}/from-B.txt',
        )
        out, _err, _rc = helper.exec_cmd(client_a, f"cat {idmap_a}/{shared}/from-B.txt")
        if "data from client-B" not in (out or ""):
            raise AssertionError("Client-A could not read Client-B file")

        for idx in range(1, 51):
            helper.exec_cmd(
                client_a,
                f'echo "concurrent-{idx}" > {idmap_a}/{shared}/concurrent-{idx}.txt',
            )

        out, _err, _rc = helper.exec_cmd(client_b, f"ls {idmap_b}/{shared} | wc -l")
        file_count = int((out or "0").strip())
        if file_count < 52:
            raise AssertionError(
                f"Expected at least 52 shared files, found {file_count}"
            )

        helper.assert_stat_uid_gid(
            client_a, f"{plain_a}/{shared}/from-A.txt", IDMAP_BASE_UID, IDMAP_BASE_UID
        )

        log.info("TC-S6 multi-client RWX sharing passed")
        return 0

    except Exception as exc:
        log.error("TC-S6 failed: %s", exc)
        log.error(traceback.format_exc())
        clients = ceph_cluster.get_ceph_objects("client")
        if clients:
            IdmapTestHelper(ceph_cluster).capture_failure_artifacts(clients[0])
        return 1

    finally:
        helper = IdmapTestHelper(ceph_cluster)
        for client, plain, idmap in mounts:
            helper.umount_idmap_stack(client, idmap, plain)
