"""
Regional FS parity — SNAP-01
Delete parent snapshot while writing to a cloned NFS export.
"""

import random
import string
import traceback
from threading import Thread

from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsv1
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.nfs.nfs_snap_clone.snap_clone_nfs_lib import (
    DEFAULT_NFS_NAME,
    ensure_nfs_cluster,
    wait_clone_complete,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Steps:
      1. Create subvolume, NFS export, write data, take snapshot.
      2. Clone from snapshot; export+mount the clone over NFS.
      3. Start continuous write on clone mount.
      4. Delete the parent snapshot while write continues.
      5. Verify clone I/O still succeeds and cluster stays healthy.
    """
    writer = {"stop": False, "error": None}
    try:
        config = kw.get("config", {})
        test_data = kw.get("test_data")
        fs_util = FsUtilsv1(ceph_cluster, test_data=test_data)
        cephfs_common_utils = CephFSCommonUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        if not clients:
            log.error("Need at least one client")
            return 1
        client = clients[0]
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)

        fs_name = config.get("fs_name", "cephfs")
        nfs_name = config.get("nfs_name", DEFAULT_NFS_NAME)
        group = config.get("group_name", "ganeshagroup_snap")
        subvol = config.get("subvol_name", "sv_parent_snap")
        snap = config.get("snap_name", "snap_parent_1")
        clone = config.get("clone_name", "sv_clone_under_io")
        rand = "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
        group = f"{group}_{rand}"
        subvol = f"{subvol}_{rand}"
        clone = f"{clone}_{rand}"

        if cephfs_common_utils.wait_for_healthy_ceph(client, 300):
            log.error("Cluster not healthy before test")
            return 1

        # Reuse existing NFS; do not create a second cluster (port 2049 clash).
        nfs = ensure_nfs_cluster(
            client, ceph_cluster, nfs_name=nfs_name, fs_util=fs_util
        )
        nfs_name = nfs["nfs_name"]
        nfs_server = nfs["nfs_server"]

        fs_util.create_subvolumegroup(client, vol_name=fs_name, group_name=group)
        fs_util.create_subvolume(
            client, vol_name=fs_name, subvol_name=subvol, group_name=group
        )
        path, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name} {subvol} --group_name {group}",
        )
        path = path.strip()

        export = f"/export_{rand}"
        mount = f"/mnt/nfs_parent_{rand}"
        client.exec_command(
            sudo=True,
            cmd=(
                f"ceph nfs export create cephfs {nfs_name} {export} {fs_name} "
                f"{path}"
            ),
        )
        client.exec_command(sudo=True, cmd=f"mkdir -p {mount}")
        client.exec_command(
            sudo=True,
            cmd=f"mount -t nfs -o vers=4.2 {nfs_server}:{export} {mount}",
        )
        client.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/urandom of={mount}/base.bin bs=1M count=16 status=none",
        )
        fs_util.create_snapshot(
            client, vol_name=fs_name, subvol_name=subvol, snap_name=snap, group_name=group
        )
        fs_util.create_clone(
            client,
            vol_name=fs_name,
            subvol_name=subvol,
            snap_name=snap,
            target_subvol_name=clone,
            group_name=group,
            target_group_name=group,
        )
        wait_clone_complete(fs_util, client, fs_name, clone, group_name=group)

        clone_path, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name} {clone} --group_name {group}",
        )
        clone_path = clone_path.strip()
        clone_export = f"/export_clone_{rand}"
        clone_mount = f"/mnt/nfs_clone_{rand}"
        client.exec_command(
            sudo=True,
            cmd=(
                f"ceph nfs export create cephfs {nfs_name} {clone_export} {fs_name} "
                f"{clone_path}"
            ),
        )
        client.exec_command(sudo=True, cmd=f"mkdir -p {clone_mount}")
        client.exec_command(
            sudo=True,
            cmd=f"mount -t nfs -o vers=4.2 {nfs_server}:{clone_export} {clone_mount}",
        )

        def _writer():
            i = 0
            while not writer["stop"]:
                try:
                    client.exec_command(
                        sudo=True,
                        cmd=(
                            f"dd if=/dev/urandom of={clone_mount}/w_{i}.bin "
                            f"bs=1M count=2 status=none conv=fsync"
                        ),
                    )
                    i += 1
                except Exception as exc:  # noqa: BLE001
                    writer["error"] = str(exc)
                    break

        thr = Thread(target=_writer, daemon=True)
        thr.start()
        time_mod = __import__("time")
        time_mod.sleep(5)
        log.info("Deleting parent snapshot %s while clone I/O runs", snap)
        fs_util.remove_snapshot(
            client, vol_name=fs_name, subvol_name=subvol, snap_name=snap, group_name=group
        )
        time_mod.sleep(10)
        writer["stop"] = True
        thr.join(timeout=60)
        if writer["error"]:
            raise RuntimeError(f"Clone I/O failed after parent snap delete: {writer['error']}")

        client.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/urandom of={clone_mount}/final.bin bs=1M count=2 status=none conv=fsync",
        )
        log.info("SNAP-01 passed: parent snap deleted under clone NFS I/O")
        return 0
    except Exception as exc:  # noqa: BLE001
        log.error(traceback.format_exc())
        log.error("SNAP-01 failed: %s", exc)
        return 1
    finally:
        writer["stop"] = True
