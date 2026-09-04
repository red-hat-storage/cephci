"""
TC-S3: File ownership correctness across plain and idmapped views.
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
    """TC-S3 — verify UID/GID translation on create."""
    plain_mount = idmap_mount = None
    try:
        _config, _test_data, build, clients, helper = init_idmap_test(ceph_cluster, kw)
        helper.prepare_clients(clients, build)
        client = clients[0]

        plain_mount, idmap_mount, _suffix = helper.setup_plain_and_idmap_mounts(client)
        own_file = f"{idmap_mount}/own-test"
        root_file = f"{idmap_mount}/rootfile"

        helper.unshare_run(
            client,
            f"touch {own_file} && ls -ln {own_file}",
            inner_uid=1000,
            outer_uid=IDMAP_BASE_UID,
            inner_gid=1000,
            outer_gid=IDMAP_BASE_UID,
            run_as_inner=True,
        )
        helper.assert_stat_uid_gid(
            client, f"{plain_mount}/own-test", IDMAP_BASE_UID, IDMAP_BASE_UID
        )

        helper.unshare_run(
            client,
            f"touch {root_file} && ls -ln {root_file}",
            map_root=True,
        )
        helper.assert_stat_uid_gid(
            client, f"{plain_mount}/rootfile", IDMAP_BASE_UID, IDMAP_BASE_UID
        )

        log.info("TC-S3 ownership correctness passed")
        return 0

    except Exception as exc:
        log.error("TC-S3 failed: %s", exc)
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
