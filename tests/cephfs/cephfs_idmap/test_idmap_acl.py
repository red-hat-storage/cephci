"""
TC-S7a: POSIX ACL UID mapping on idmapped CephFS.
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
    """TC-S7a — setfacl/getfacl UID translation across idmapped views."""
    plain_mount = idmap_mount = None
    clients = []
    try:
        _config, _test_data, build, clients, helper = init_idmap_test(ceph_cluster, kw)
        helper.prepare_clients(clients, build)
        helper.install_acl_tools(clients[0])
        client = clients[0]

        plain_mount, idmap_mount, _suffix = helper.setup_plain_and_idmap_mounts(client)
        base = f"{idmap_mount}/acl-test"
        acl_file = f"{base}/acl-file.txt"
        plain_acl_file = f"{plain_mount}/acl-test/acl-file.txt"

        helper.exec_cmd(client, f"mkdir -p {base}")
        helper.exec_cmd(client, f"touch {acl_file}")
        helper.exec_cmd(client, f"chmod 000 {acl_file}")
        helper.exec_cmd(client, f"setfacl -m u:1001:rw {acl_file}")

        view_acl = helper.getfacl_user_permissions(client, acl_file)
        if not IdmapTestHelper.acl_perm_satisfies(view_acl.get(1001), "rw"):
            raise AssertionError(
                f"Expected user:1001:rw on idmapped view, got {view_acl!r}"
            )

        plain_acl = helper.getfacl_user_permissions(client, plain_acl_file)
        expected_plain_uid = IDMAP_BASE_UID + 1001
        if not IdmapTestHelper.acl_perm_satisfies(
            plain_acl.get(expected_plain_uid), "rw"
        ):
            raise AssertionError(
                f"Expected user:{expected_plain_uid}:rw on plain mount, got {plain_acl!r}"
            )

        # Enforce ACL on the plain mount where the entry is stored as disk UID
        # 101001; capsh cannot drop caps inside a user namespace on RHEL.
        helper.unshare_run(
            client,
            f"cat {plain_acl_file}",
            inner_uid=1001,
            outer_uid=expected_plain_uid,
            inner_gid=1001,
            outer_gid=expected_plain_uid,
            run_as_inner=True,
            use_outer_cred=True,
        )

        _out, _err, exit_code = helper.unshare_run(
            client,
            f"cat {plain_acl_file}",
            inner_uid=1002,
            outer_uid=IDMAP_BASE_UID + 1002,
            inner_gid=1002,
            outer_gid=IDMAP_BASE_UID + 1002,
            run_as_inner=True,
            use_outer_cred=True,
            expect_fail=True,
        )
        if exit_code == 0:
            raise AssertionError("UID 1002 was able to read ACL-protected file")

        log.info("TC-S7a POSIX ACL UID mapping passed")
        return 0

    except Exception as exc:
        log.error("TC-S7a failed: %s", exc)
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
