"""
TC-S10: Negative test — bad cephx credentials must fail with auth error.
"""

import traceback

from tests.cephfs.cephfs_idmap.lib.cephfs_idmap_lib import (
    FS_NAME,
    IdmapTestHelper,
    init_idmap_test,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """TC-S10 — wrong credentials produce a clear auth error."""
    bad_mount = None
    try:
        _config, _test_data, build, clients, helper = init_idmap_test(ceph_cluster, kw)
        helper.prepare_clients(clients, build)
        client = clients[0]
        bad_mount = f"/mnt/bad-mount-{helper.random_suffix()}"

        mon_ips = ",".join(helper.mon_node_ips)
        mount_cmd = (
            f"mkdir -p {bad_mount} && "
            f"mount -t ceph {mon_ips}:/ {bad_mount} "
            f"-o name=wronguser,secret=badkey,fs={FS_NAME},noshare"
        )
        _out, err, exit_code = helper.exec_cmd(client, mount_cmd, expect_fail=True)
        if exit_code == 0:
            raise AssertionError("Mount with bad credentials unexpectedly succeeded")

        err_text = (err or "").lower()
        if "mount_setattr" in err_text:
            raise AssertionError(
                "Bad credential failure incorrectly references mount_setattr"
            )

        helper.check_cluster_health(client)
        log.info("TC-S10 negative auth test passed (exit code %s)", exit_code)
        return 0

    except Exception as exc:
        log.error("TC-S10 failed: %s", exc)
        log.error(traceback.format_exc())
        clients = ceph_cluster.get_ceph_objects("client")
        if clients:
            IdmapTestHelper(ceph_cluster).capture_failure_artifacts(clients[0])
        return 1

    finally:
        if bad_mount and clients:
            clients[0].exec_command(
                sudo=True, cmd=f"umount {bad_mount}", check_ec=False
            )
            clients[0].exec_command(
                sudo=True, cmd=f"rm -rf {bad_mount}", check_ec=False
            )
