"""
TC-S1: User namespace baseline on the client OS (no CephFS idmap).
"""

import traceback

from tests.cephfs.cephfs_idmap.lib.cephfs_idmap_lib import (
    IdmapTestHelper,
    init_idmap_test,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """TC-S1 — confirm user namespace UID/GID mapping works on the client."""
    try:
        _config, _test_data, build, clients, helper = init_idmap_test(ceph_cluster, kw)
        helper.prepare_clients(clients, build)
        client = clients[0]

        out, _err, _rc = helper.unshare_run(
            client,
            "id -u; id -g",
            inner_uid=1000,
            outer_uid=1454113768,
            inner_gid=1000,
            outer_gid=1454113768,
            run_as_inner=True,
        )
        lines = [line.strip() for line in (out or "").splitlines() if line.strip()]
        if len(lines) < 2:
            raise AssertionError(f"Unexpected unshare id output: {out!r}")

        if lines[0] != "1000" or lines[1] != "1000":
            raise AssertionError(
                f"Inside user namespace expected uid/gid 1000, got uid={lines[0]} gid={lines[1]}"
            )

        log.info("TC-S1 user namespace baseline passed")
        return 0

    except Exception as exc:
        log.error("TC-S1 failed: %s", exc)
        log.error(traceback.format_exc())
        clients = ceph_cluster.get_ceph_objects("client")
        if clients:
            IdmapTestHelper(ceph_cluster).capture_failure_artifacts(clients[0])
        return 1
