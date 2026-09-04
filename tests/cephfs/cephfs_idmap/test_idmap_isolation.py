"""
TC-S4: Security isolation between different idmapped views.
"""

import traceback

from tests.cephfs.cephfs_idmap.lib.cephfs_idmap_lib import (
    ALT_IDMAP_SPEC,
    DEFAULT_IDMAP_SPEC,
    IDMAP_BASE_UID,
    IDMAP_BASE_UID_B,
    IdmapTestHelper,
    init_idmap_test,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """TC-S4 — different maps must not bypass chmod 600 restrictions."""
    plain_mount = idmap_a = idmap_b = None
    try:
        _config, _test_data, build, clients, helper = init_idmap_test(ceph_cluster, kw)
        helper.prepare_clients(clients, build)
        client = clients[0]

        plain_mount, _idmap, suffix = helper.mount_paths()
        idmap_a = f"/mnt/idmap-A_{suffix}"
        idmap_b = f"/mnt/idmap-B_{suffix}"

        helper.kernel_mount_plain(client, plain_mount)
        helper.prepare_idmap_mount_root(client, plain_mount, DEFAULT_IDMAP_SPEC)
        helper.idmap_bind_mount(client, plain_mount, idmap_a, DEFAULT_IDMAP_SPEC)
        helper.idmap_bind_mount(client, plain_mount, idmap_b, ALT_IDMAP_SPEC)

        secret = f"{idmap_a}/secret.txt"
        helper.unshare_run(
            client,
            f'echo "pod-A private data" > {secret} && chmod 600 {secret}',
            inner_uid=1000,
            outer_uid=IDMAP_BASE_UID,
            inner_gid=1000,
            outer_gid=IDMAP_BASE_UID,
            run_as_inner=True,
        )

        _out, _err, exit_code = helper.unshare_run(
            client,
            f"cat {idmap_b}/secret.txt",
            inner_uid=1000,
            outer_uid=IDMAP_BASE_UID_B,
            inner_gid=1000,
            outer_gid=IDMAP_BASE_UID_B,
            run_as_inner=True,
            expect_fail=True,
        )
        if exit_code == 0:
            raise AssertionError("Map B was able to read Map A's chmod 600 file")

        helper.assert_stat_uid_gid(
            client, f"{plain_mount}/secret.txt", IDMAP_BASE_UID, IDMAP_BASE_UID
        )

        log.info("TC-S4 security isolation passed")
        return 0

    except Exception as exc:
        log.error("TC-S4 failed: %s", exc)
        log.error(traceback.format_exc())
        clients = ceph_cluster.get_ceph_objects("client")
        if clients:
            IdmapTestHelper(ceph_cluster).capture_failure_artifacts(clients[0])
        return 1

    finally:
        if plain_mount and clients:
            client = clients[0]
            helper = IdmapTestHelper(ceph_cluster)
            for mount in (idmap_b, idmap_a, plain_mount):
                if mount:
                    client.exec_command(
                        sudo=True, cmd=f"umount {mount}", check_ec=False
                    )
            client.exec_command(
                sudo=True,
                cmd=f"rm -rf {plain_mount} {idmap_a or ''} {idmap_b or ''}",
                check_ec=False,
            )
