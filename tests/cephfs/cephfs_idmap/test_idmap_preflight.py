"""
TC-S0: Environment validation gate for CephFS idmapped mount tests.

Confirms cluster health, kernel CephFS client availability, and plain mount smoke.
"""

import traceback

from tests.cephfs.cephfs_idmap.lib.cephfs_idmap_lib import (
    CONFIGURED_CLIENT_COUNT,
    FS_NAME,
    IdmapTestHelper,
    init_idmap_test,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """TC-S0 — gate all idmap functional tests."""
    plain_mount = None
    clients = []
    try:
        _config, test_data, build, clients, helper = init_idmap_test(ceph_cluster, kw)
        if not clients:
            log.error("No client nodes found in cluster configuration")
            return 1

        helper.prepare_clients(clients, build)

        for client in clients:
            helper.record_environment_versions(client)
            helper.check_cephfs_kernel_module(client)

        helper.check_cluster_health(clients[0])

        plain_mount, _idmap_mount, suffix = helper.mount_paths()
        helper.kernel_mount_plain(clients[0], plain_mount)
        helper.exec_cmd(
            clients[0],
            f"touch {plain_mount}/preflight && rm -f {plain_mount}/preflight",
        )
        helper.check_mds_high_uid_support(clients[0], plain_mount)
        helper.umount_plain(clients[0], plain_mount)

        if test_data is not None:
            test_data["idmap_suite"] = {
                "fs_name": FS_NAME,
                "validated_suffix": suffix,
                "client_count": len(clients),
                "configured_client_count": CONFIGURED_CLIENT_COUNT,
            }

        log.info("TC-S0 preflight passed on %d client(s)", len(clients))
        return 0

    except Exception as exc:
        log.error("TC-S0 preflight failed: %s", exc)
        log.error(traceback.format_exc())
        if clients:
            IdmapTestHelper(ceph_cluster).capture_failure_artifacts(clients[0])
        if plain_mount and clients:
            IdmapTestHelper(ceph_cluster).umount_plain(clients[0], plain_mount)
        return 1
