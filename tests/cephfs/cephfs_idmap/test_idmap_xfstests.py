"""
TC-S13: xfstests idmapped group on kernel-mounted CephFS.
"""

import traceback

from tests.cephfs.cephfs_idmap.lib.cephfs_idmap_lib import (
    FS_NAME,
    IdmapTestHelper,
    init_idmap_test,
)
from tests.cephfs.lib.xfs_lib.xfs_utils import XfsTestSetup
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """TC-S13 — run upstream xfstests idmapped group on CephFS."""
    test_mount = scratch_mount = None
    xfs_test = None
    mount_info = None
    client = None
    clients = []
    try:
        _config, _test_data, build, clients, helper = init_idmap_test(ceph_cluster, kw)
        helper.prepare_clients(clients, build)
        client = clients[0]

        helper.cleanup_xfstests_artifacts(client)

        xfs_test = XfsTestSetup(ceph_cluster, client)
        if xfs_test.setup_environment():
            log.error("Failed to set up xfstests environment")
            return 1
        if xfs_test.clone_and_build_xfstests():
            log.error("Failed to clone and build xfstests")
            return 1

        suffix = helper.random_suffix()
        test_mount = f"/mnt/cephfs_xfs_test_{suffix}"
        scratch_mount = f"/mnt/cephfs_xfs_scratch_{suffix}"
        test_dev = f"xfs_test_{suffix}"
        scratch_dev = f"xfs_scratch_{suffix}"

        mount_info = {
            "test_mount": test_mount,
            "scratch_mount": scratch_mount,
            "mount_type": "kernel",
            "FSTYP": "ceph",
            "fs_name": FS_NAME,
            "test_dev": test_dev,
            "scratch_dev": scratch_dev,
        }

        # Match xfs_test.py: mount CephFS root, create subpaths on the FS, then
        # let xfstests remount mon:/subdir devices from local.config.
        if xfs_test.mount_fs(mount_info):
            log.error("Failed to mount CephFS for xfstests setup")
            return 1

        helper.configure_idmap_xfstests_local_config(
            client, xfs_test.mon_node_ips, mount_info
        )

        out, _err, exit_code, _duration = client.exec_command(
            sudo=True,
            cmd="cd /root/xfstests-dev && ./check -g idmapped",
            check_ec=False,
            verbose=True,
            timeout=7200,
        )
        log.info("xfstests idmapped group output:\n%s", out)

        failed_tests, _ = client.exec_command(
            sudo=True,
            cmd=r"find /root/xfstests-dev/results -name '*.out.bad' 2>/dev/null | wc -l",
            check_ec=False,
        )
        failed_count = int((failed_tests or "0").strip() or "0")
        if failed_count > 0:
            bad_out, _ = client.exec_command(
                sudo=True,
                cmd=r"find /root/xfstests-dev/results -name '*.out.bad' -exec basename -s .out.bad {} \;",
                check_ec=False,
            )
            log.error("xfstests failed cases:\n%s", bad_out)

        if exit_code != 0 or failed_count > 0:
            log.error(
                "xfstests idmapped group failed (exit=%s, bad=%s)",
                exit_code,
                failed_count,
            )
            return 1

        log.info("TC-S13 xfstests idmapped group passed")
        return 0

    except Exception as exc:
        log.error("TC-S13 failed: %s", exc)
        log.error(traceback.format_exc())
        clients = ceph_cluster.get_ceph_objects("client")
        if clients:
            IdmapTestHelper(ceph_cluster).capture_failure_artifacts(clients[0])
        return 1

    finally:
        if xfs_test and client:
            IdmapTestHelper(ceph_cluster).cleanup_xfstests_artifacts(client)
        elif test_mount and scratch_mount and clients:
            IdmapTestHelper(ceph_cluster).umount_plain_mounts(
                clients[0], test_mount, scratch_mount
            )
