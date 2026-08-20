"""
TC-S11: Negative test — invalid filesystem name must fail clearly.
"""

import traceback

from tests.cephfs.cephfs_idmap.lib.cephfs_idmap_lib import (
    IdmapTestHelper,
    init_idmap_test,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """TC-S11 — non-existent fs name produces filesystem-not-found error."""
    bad_mount = None
    try:
        _config, _test_data, build, clients, helper = init_idmap_test(ceph_cluster, kw)
        helper.prepare_clients(clients, build)
        client = clients[0]
        hostname = client.node.hostname
        bad_mount = f"/mnt/badfs-{helper.random_suffix()}"

        client.exec_command(
            sudo=True,
            cmd=f"ceph auth get-key client.{hostname} -o /etc/ceph/{hostname}.secret",
        )
        mon_ips = ",".join(helper.mon_node_ips)
        mount_cmd = (
            f"mkdir -p {bad_mount} && "
            f"mount -t ceph {mon_ips}:/ {bad_mount} "
            f"-o name={hostname},secretfile=/etc/ceph/{hostname}.secret,"
            f"fs=this-filesystem-does-not-exist,noshare"
        )
        _out, _err, exit_code = helper.exec_cmd(client, mount_cmd, expect_fail=True)
        if exit_code == 0:
            raise AssertionError("Mount with invalid fs name unexpectedly succeeded")

        out, _ = client.exec_command(
            sudo=True, cmd=f"mount | grep {bad_mount}", check_ec=False
        )
        if bad_mount in (out or ""):
            raise AssertionError(
                "Partial mount state left behind after invalid fs name"
            )

        helper.check_cluster_health(client)
        log.info("TC-S11 negative fs name test passed (exit code %s)", exit_code)
        return 0

    except Exception as exc:
        log.error("TC-S11 failed: %s", exc)
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
