"""
Regional FS parity — SNAP-02
Cascaded snap → clone → snap → clone with checksum verification.
"""

import json
import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsv1
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.nfs.snapshot_clone.snap_clone_nfs_lib import (
    checksum_file,
    wait_clone_complete,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Build a two-generation clone chain and verify content checksums at each stage.
    """
    try:
        config = kw.get("config", {})
        test_data = kw.get("test_data")
        fs_util = FsUtilsv1(ceph_cluster, test_data=test_data)
        cephfs_common_utils = CephFSCommonUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client = clients[0]
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)

        fs_name = config.get("fs_name", "cephfs")
        nfs_name = config.get("nfs_name", "cephfs-nfs-cascade")
        rand = "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
        group = f"ganeshagroup_cas_{rand}"
        sv0 = f"sv0_{rand}"
        snap0 = f"snap0_{rand}"
        sv1 = f"sv1_{rand}"
        snap1 = f"snap1_{rand}"
        sv2 = f"sv2_{rand}"

        if cephfs_common_utils.wait_for_healthy_ceph(client, 300):
            return 1
        setup_params = cephfs_common_utils.test_setup(
            fs_name, client, nfs_name=nfs_name
        )
        nfs_server = setup_params["nfs_server"]

        fs_util.create_subvolumegroup(client, vol_name=fs_name, group_name=group)
        fs_util.create_subvolume(
            client, vol_name=fs_name, subvol_name=sv0, group_name=group
        )
        path0, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name} {sv0} --group_name {group}",
        )
        path0 = path0.strip()
        export0 = f"/export0_{rand}"
        mount0 = f"/mnt/cas0_{rand}"
        client.exec_command(
            sudo=True,
            cmd=f"ceph nfs export create cephfs {nfs_name} {export0} {fs_name} {path0}",
        )
        client.exec_command(sudo=True, cmd=f"mkdir -p {mount0}")
        client.exec_command(
            sudo=True, cmd=f"mount -t nfs -o vers=4.2 {nfs_server}:{export0} {mount0}"
        )
        marker = f"cascade-marker-{rand}"
        client.exec_command(
            sudo=True,
            cmd=(
                f"bash -c 'printf %s {marker!r} > {mount0}/payload.bin; "
                f"dd if=/dev/zero bs=1M count=4 status=none >> {mount0}/payload.bin'"
            ),
        )
        c0 = checksum_file(client, f"{mount0}/payload.bin")
        log.info("Generation-0 checksum=%s", c0)

        fs_util.create_snapshot(
            client, vol_name=fs_name, subvol_name=sv0, snap_name=snap0, group_name=group
        )
        fs_util.create_clone(
            client,
            vol_name=fs_name,
            subvol_name=sv0,
            snap_name=snap0,
            target_subvol_name=sv1,
            group_name=group,
            target_group_name=group,
        )
        wait_clone_complete(fs_util, client, fs_name, sv1, group_name=group)

        path1, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name} {sv1} --group_name {group}",
        )
        path1 = path1.strip()
        export1 = f"/export1_{rand}"
        mount1 = f"/mnt/cas1_{rand}"
        client.exec_command(
            sudo=True,
            cmd=f"ceph nfs export create cephfs {nfs_name} {export1} {fs_name} {path1}",
        )
        client.exec_command(sudo=True, cmd=f"mkdir -p {mount1}")
        client.exec_command(
            sudo=True, cmd=f"mount -t nfs -o vers=4.2 {nfs_server}:{export1} {mount1}"
        )
        c1 = checksum_file(client, f"{mount1}/payload.bin")
        if c1 != c0:
            raise RuntimeError(f"Gen1 checksum mismatch: {c1} != {c0}")

        # second generation
        client.exec_command(
            sudo=True,
            cmd=f"bash -c 'echo gen2-{rand} >> {mount1}/payload.bin'",
        )
        c1b = checksum_file(client, f"{mount1}/payload.bin")
        fs_util.create_snapshot(
            client, vol_name=fs_name, subvol_name=sv1, snap_name=snap1, group_name=group
        )
        fs_util.create_clone(
            client,
            vol_name=fs_name,
            subvol_name=sv1,
            snap_name=snap1,
            target_subvol_name=sv2,
            group_name=group,
            target_group_name=group,
        )
        wait_clone_complete(fs_util, client, fs_name, sv2, group_name=group)

        path2, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name} {sv2} --group_name {group}",
        )
        path2 = path2.strip()
        export2 = f"/export2_{rand}"
        mount2 = f"/mnt/cas2_{rand}"
        client.exec_command(
            sudo=True,
            cmd=f"ceph nfs export create cephfs {nfs_name} {export2} {fs_name} {path2}",
        )
        client.exec_command(sudo=True, cmd=f"mkdir -p {mount2}")
        client.exec_command(
            sudo=True, cmd=f"mount -t nfs -o vers=4.2 {nfs_server}:{export2} {mount2}"
        )
        c2 = checksum_file(client, f"{mount2}/payload.bin")
        if c2 != c1b:
            raise RuntimeError(f"Gen2 checksum mismatch: {c2} != {c1b}")

        log.info(
            "SNAP-02 passed: cascade checksums %s",
            json.dumps({"g0": c0, "g1": c1, "g1b": c1b, "g2": c2}),
        )
        return 0
    except Exception as exc:  # noqa: BLE001
        log.error(traceback.format_exc())
        log.error("SNAP-02 failed: %s", exc)
        return 1
