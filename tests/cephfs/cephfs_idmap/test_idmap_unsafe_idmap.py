"""
TC-S14: enable_unsafe_idmap module-parameter fallback (conditional).

Validates param=0 rejection and param=1 override when MDS lacks
CEPHFS_FEATURE_HAS_OWNER_UIDGID. Skipped when MDS has the feature (GA path).
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
    """TC-S14 — enable_unsafe_idmap fallback paths."""
    plain_mount = idmap_mount = None
    clients = []
    original_unsafe = None
    try:
        _config, test_data, build, clients, helper = init_idmap_test(ceph_cluster, kw)
        helper.prepare_clients(clients, build)
        client = clients[0]

        plain_mount, idmap_mount, _suffix = helper.mount_paths()
        helper.kernel_mount_plain(client, plain_mount)

        mds_has_feature = (
            (test_data or {}).get("idmap_suite", {}).get("mds_has_owner_uidgid")
        )
        if mds_has_feature is None:
            mds_has_feature = helper.mds_has_owner_uidgid(client, plain_mount)

        if mds_has_feature:
            log.info(
                "TC-S14 skipped: MDS has CEPHFS_FEATURE_HAS_OWNER_UIDGID (GA path)"
            )
            return 0

        original_unsafe = helper.get_enable_unsafe_idmap(client)
        helper.set_enable_unsafe_idmap(client, False)
        helper.prepare_idmap_mount_root(client, plain_mount)
        helper.idmap_bind_mount(client, plain_mount, idmap_mount)

        reject_file = f"{idmap_mount}/unsafe-reject-test"
        override_file = f"{idmap_mount}/unsafe-override-test"

        _out, _err, reject_rc = helper.unshare_run(
            client,
            f"touch {reject_file}",
            inner_uid=1000,
            outer_uid=IDMAP_BASE_UID,
            inner_gid=1000,
            outer_gid=IDMAP_BASE_UID,
            run_as_inner=True,
            expect_fail=True,
        )
        if reject_rc == 0:
            raise AssertionError(
                "Expected create failure with enable_unsafe_idmap=0 on MDS "
                "without owner UID/GID support"
            )

        helper.set_enable_unsafe_idmap(client, True)
        helper.unshare_run(
            client,
            f"touch {override_file}",
            inner_uid=1000,
            outer_uid=IDMAP_BASE_UID,
            inner_gid=1000,
            outer_gid=IDMAP_BASE_UID,
            run_as_inner=True,
        )
        helper.assert_stat_uid_gid(
            client,
            f"{plain_mount}/unsafe-override-test",
            IDMAP_BASE_UID,
            IDMAP_BASE_UID,
        )

        log.info("TC-S14 enable_unsafe_idmap fallback passed")
        return 0

    except Exception as exc:
        log.error("TC-S14 failed: %s", exc)
        log.error(traceback.format_exc())
        if clients:
            IdmapTestHelper(ceph_cluster).capture_failure_artifacts(clients[0])
        return 1

    finally:
        if clients:
            client = clients[0]
            helper = IdmapTestHelper(ceph_cluster)
            if plain_mount and idmap_mount:
                helper.umount_idmap_stack(client, idmap_mount, plain_mount)
            elif plain_mount:
                helper.umount_plain(client, plain_mount)
            if original_unsafe is not None:
                helper.set_enable_unsafe_idmap(
                    client, original_unsafe in ("1", "Y", "y")
                )
            else:
                helper.set_enable_unsafe_idmap(client, True)
