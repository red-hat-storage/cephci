"""
Regional FS parity — UPG thrash
Continuous NFS export + CephFS snapshot churn (for parallel with cephadm upgrade).
"""

import random
import string
import time
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsv1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Loop: create/delete exports and snapshots for `duration` seconds.
    Intended to run in suite parallel: with test_cephadm_upgrade.py.
    """
    try:
        config = kw.get("config", {})
        test_data = kw.get("test_data")
        fs_util = FsUtilsv1(ceph_cluster, test_data=test_data)
        clients = ceph_cluster.get_ceph_objects("client")
        client = clients[0]
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)

        fs_name = config.get("fs_name", "cephfs")
        nfs_name = config.get("nfs_name", "cephfs-nfs")
        duration = int(config.get("duration", 600))
        group = config.get("group_name", "ganeshagroup_upg")
        rand = "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
        group = f"{group}_{rand}"
        subvol = f"sv_upg_{rand}"

        fs_util.create_subvolumegroup(client, vol_name=fs_name, group_name=group)
        fs_util.create_subvolume(
            client, vol_name=fs_name, subvol_name=subvol, group_name=group
        )
        path, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume getpath {fs_name} {subvol} --group_name {group}",
        )
        path = path.strip()

        end = time.time() + duration
        cycle = 0
        while time.time() < end:
            cycle += 1
            export = f"/export_upg_{rand}_{cycle}"
            snap = f"snap_upg_{rand}_{cycle}"
            log.info("Churn cycle %s export=%s snap=%s", cycle, export, snap)
            client.exec_command(
                sudo=True,
                cmd=(
                    f"ceph nfs export create cephfs {nfs_name} {export} "
                    f"{fs_name} {path}"
                ),
            )
            fs_util.create_snapshot(
                client,
                vol_name=fs_name,
                subvol_name=subvol,
                snap_name=snap,
                group_name=group,
            )
            time.sleep(int(config.get("cycle_sleep", 5)))
            fs_util.remove_snapshot(
                client,
                vol_name=fs_name,
                subvol_name=subvol,
                snap_name=snap,
                group_name=group,
                force=True,
            )
            client.exec_command(
                sudo=True,
                cmd=f"ceph nfs export delete {nfs_name} {export}",
                check_ec=False,
            )

        log.info("UPG thrash completed %s cycles in %ss", cycle, duration)
        return 0
    except Exception as exc:  # noqa: BLE001
        log.error(traceback.format_exc())
        log.error("UPG thrash failed: %s", exc)
        return 1
