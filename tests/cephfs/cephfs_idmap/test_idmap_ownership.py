"""
TC-S3: File ownership correctness across plain and idmapped views.

Includes first/last UID/GID in the mapped range and out-of-range rejection.
"""

import traceback

from tests.cephfs.cephfs_idmap.lib.cephfs_idmap_lib import (
    IDMAP_BASE_UID,
    IDMAP_LAST_DISK_UID,
    IDMAP_LAST_VIEW_UID,
    IDMAP_OUT_OF_RANGE_DISK_UID,
    IDMAP_OUT_OF_RANGE_VIEW_UID,
    IdmapTestHelper,
    init_idmap_test,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """TC-S3 — verify UID/GID translation on create."""
    plain_mount = idmap_mount = None
    clients = []
    try:
        _config, _test_data, build, clients, helper = init_idmap_test(ceph_cluster, kw)
        helper.prepare_clients(clients, build)
        client = clients[0]

        plain_mount, idmap_mount, _suffix = helper.setup_plain_and_idmap_mounts(client)
        own_file = f"{idmap_mount}/own-test"
        root_file = f"{idmap_mount}/rootfile"
        first_file = f"{idmap_mount}/first-uid"
        out_of_range_file = f"{idmap_mount}/out-of-range"

        # K8s-like mapping: pod UID 1000 -> host 100000 -> view UID 0 on idmap mount
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

        # Root-in-container: --map-root-user only (no conflicting --map-user)
        helper.unshare_run(
            client,
            f"touch {root_file} && ls -ln {root_file}",
            map_root=True,
        )
        helper.assert_stat_uid_gid(
            client, f"{plain_mount}/rootfile", IDMAP_BASE_UID, IDMAP_BASE_UID
        )

        # First UID in mapped range: view 0 -> disk 100000
        helper.unshare_run(
            client,
            f"touch {first_file} && ls -ln {first_file}",
            inner_uid=0,
            outer_uid=IDMAP_BASE_UID,
            inner_gid=0,
            outer_gid=IDMAP_BASE_UID,
            run_as_inner=True,
        )
        helper.assert_stat_uid_gid(
            client, f"{plain_mount}/first-uid", IDMAP_BASE_UID, IDMAP_BASE_UID
        )

        # Last UID in mapped range: view 65535 -> disk 165535.
        # Create on the plain mount (idmapped create with outer UID EOVERFLOW).
        # Mount root is 100000:100000, so use a world-writable parent directory.
        last_dir = "boundary-last-uid"
        plain_last_dir = f"{plain_mount}/{last_dir}"
        idmap_last_dir = f"{idmap_mount}/{last_dir}"
        plain_last = f"{plain_last_dir}/last-uid"
        idmap_last = f"{idmap_last_dir}/last-uid"
        helper.exec_cmd(
            client, f"mkdir -p {plain_last_dir} && chmod 777 {plain_last_dir}"
        )
        helper.unshare_run(
            client,
            f"touch {plain_last}",
            inner_uid=IDMAP_LAST_VIEW_UID,
            outer_uid=IDMAP_LAST_DISK_UID,
            inner_gid=IDMAP_LAST_VIEW_UID,
            outer_gid=IDMAP_LAST_DISK_UID,
            run_as_inner=True,
            use_outer_cred=True,
        )
        helper.assert_stat_uid_gid(
            client, plain_last, IDMAP_LAST_DISK_UID, IDMAP_LAST_DISK_UID
        )
        helper.assert_stat_uid_gid(
            client, idmap_last, IDMAP_LAST_VIEW_UID, IDMAP_LAST_VIEW_UID
        )

        # UID outside mapped range should fail (needs true outer cred, not root via
        # --map-user overlap).
        _out, _err, exit_code = helper.unshare_run(
            client,
            f"touch {out_of_range_file}",
            inner_uid=IDMAP_OUT_OF_RANGE_VIEW_UID,
            outer_uid=IDMAP_OUT_OF_RANGE_DISK_UID,
            inner_gid=IDMAP_OUT_OF_RANGE_VIEW_UID,
            outer_gid=IDMAP_OUT_OF_RANGE_DISK_UID,
            run_as_inner=True,
            use_outer_cred=True,
            expect_fail=True,
        )
        if exit_code == 0:
            raise AssertionError(
                "Out-of-range UID create succeeded; expected failure "
                f"(view UID {IDMAP_OUT_OF_RANGE_VIEW_UID})"
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
