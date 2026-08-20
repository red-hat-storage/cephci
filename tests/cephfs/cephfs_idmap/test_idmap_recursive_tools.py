"""
TC-S8: Recursive tools (cp, tar, rsync, rm) on idmapped CephFS.
"""

import traceback

from tests.cephfs.cephfs_idmap.lib.cephfs_idmap_lib import (
    IdmapTestHelper,
    init_idmap_test,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """TC-S8 — recursive file operations on idmapped CephFS."""
    plain_mount = idmap_mount = None
    try:
        _config, _test_data, build, clients, helper = init_idmap_test(ceph_cluster, kw)
        helper.prepare_clients(clients, build)
        client = clients[0]
        helper.install_recursive_tools(client)

        plain_mount, idmap_mount, _suffix = helper.setup_plain_and_idmap_mounts(client)
        base = f"{idmap_mount}/recursive-test"
        source = f"{base}/source"
        helper.exec_cmd(client, f"mkdir -p {source}/level1/level2")
        helper.exec_cmd(client, f'echo "file-L0" > {source}/root.txt')
        helper.exec_cmd(client, f'echo "file-L1" > {source}/level1/l1.txt')
        helper.exec_cmd(client, f'echo "file-L2" > {source}/level1/level2/l2.txt')
        helper.exec_cmd(client, f"chmod 644 {source}/root.txt")
        helper.exec_cmd(client, f"chmod 755 {source}/level1")

        dest_cp = f"{base}/dest-cp"
        helper.exec_cmd(client, f"cp -r {source} {dest_cp}")
        helper.exec_cmd(client, f"diff -r {source} {dest_cp}")

        archive = "/tmp/test-archive-idmap.tar.gz"
        dest_tar = f"{base}/dest-tar"
        helper.exec_cmd(client, f"tar -czf {archive} -C {base} source")
        helper.exec_cmd(client, f"mkdir -p {dest_tar}")
        helper.exec_cmd(client, f"tar -xzf {archive} -C {dest_tar}")
        helper.exec_cmd(client, f"diff -r {source} {dest_tar}/source")

        dest_rsync = f"{base}/dest-rsync"
        helper.exec_cmd(client, f"mkdir -p {dest_rsync}")
        helper.exec_cmd(client, f"rsync -a {source}/ {dest_rsync}/")
        helper.exec_cmd(client, f"diff -r {source} {dest_rsync}")

        helper.exec_cmd(client, f"rm -rf {dest_cp} {dest_tar} {dest_rsync} {archive}")
        out, _err, _rc = helper.exec_cmd(client, f"ls {base}")
        if "source" not in (out or "") or "dest" in (out or ""):
            raise AssertionError(f"Unexpected directory listing after cleanup: {out}")

        log.info("TC-S8 recursive tools passed")
        return 0

    except Exception as exc:
        log.error("TC-S8 failed: %s", exc)
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
