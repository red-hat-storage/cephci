import json
import random
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.cephfs.snapshot_clone.cephfs_snap_utils import SnapUtils
from utility.log import Log

log = Log(__name__)


def snap_visibility_test():
    """
    Test Steps:
    a) Verify CLI option default behavior on existing subvolume, new subvolume.
    b) Verify CLI option modify on existing and new subvolume

    Args:
        snap_req_params data type is dict with below params,
        snap_req_params = {
            "config" : pre_upgrade_config,
            "fs_util" : fs_util,
            "clients" : clients,
            "vol_name" : default_fs
        }
        param data types:
        Required -
        pre_upgrade_config - dict data having pre upgrade snapshot configuration
        generated from cephfs_upgrade/upgrade_pre_req.py
        fsutil - fsutil testlib object
        clients - list type, having client objects
        Optional -
        vol_name - cephfs volume name, default is 'cephfs'

    Returns:
        None
    Raises:
        BaseException
    """
    config = test_reqs["config"]
    vol_name = test_reqs.get("vol_name", "cephfs")
    log.info("Get snapshot configuration from pre-upgrade config")
    sv_snap = {}
    for svg in config["CephFS"][vol_name]:
        if "svg" in svg:
            for sv in config["CephFS"][vol_name][svg]:
                sv_data = config["CephFS"][vol_name][svg][sv]
                if sv_data.get("snap_list"):
                    sv_snap.update(
                        {
                            sv: {
                                "snap_list": sv_data["snap_list"],
                                "svg": svg,
                                "mnt_pt": sv_data["mnt_pt"],
                                "mnt_client": sv_data["mnt_client"],
                            }
                        }
                    )
    clients = test_reqs["clients"]
    client = clients[0]
    log.info(
        "Get default CLI option for a existing subvolume snapshot visibility and validate value"
    )
    sv = random.choice(list(sv_snap.keys()))
    sv_old = {
        "vol_name": vol_name,
        "sub_name": sv,
        "group_name": sv_snap[sv]["svg"],
    }
    mnt_client_name = sv_snap[sv]["mnt_client"]
    mnt_client = [i for i in clients if i.node.hostname == mnt_client_name][0]
    sv_new = {
        "vol_name": vol_name,
        "subvol_name": "new_svt_post_upgrade",
        "group_name": sv_snap[sv]["svg"],
    }
    fs_util.create_subvolume(client, **sv_new)
    sv_new.update({"sub_name": "new_svt_post_upgrade"})
    log.info(
        "Get default CLI option for a existing and new subvolume snapshot visibility and validate value"
    )
    for kwargs in [sv_old, sv_new]:
        actual_snap_visibility = snap_util.snapshot_visibility(client, "get", **kwargs)
        if actual_snap_visibility != 1:
            str1 = f"Expected : 1,Actual:{actual_snap_visibility}"
            log.error(
                "Snapshot Visibility on existing subvolume post upgrade is not as Expected.%s",
                str1,
            )
            log.info("This is due to BZ2404075, ignoring the issue")
            # return 1

    def get_nfs_details():
        nfs_config = config["NFS"]
        for i in nfs_config:
            nfs_name = i
            break
        nfs_config = config["NFS"][nfs_name]
        for i in nfs_config:
            nfs_export_name = i
            break
        nfs_server_name = nfs_config[nfs_export_name]["nfs_server"]
        return nfs_name, nfs_server_name

    mnt_list = ["fuse"]
    if ibm_build:
        nfs_name, nfs_server_name = get_nfs_details()
        mnt_list.append("nfs")
    for sv_iter in [sv_old, sv_new]:
        sv_iter.update(
            {
                "value": "false",
                "mnt_type": random.choice(mnt_list),
                "client_respect_snapshot_visibility": "true",
            }
        )
        if ibm_build:
            sv_iter.update(
                {
                    "nfs_server": nfs_server_name,
                    "nfs_name": nfs_name,
                }
            )
    log.info("Toggle CLI option on existing and new subvolume")
    for kwargs in [sv_old, sv_new]:
        actual_snap_visibility = snap_util.snapshot_visibility(client, "set", **kwargs)
        snap_util.snapshot_visibility(client, "set", **kwargs)
        snap_util.snapshot_visibility_client_mgr(client, "client", "set", **kwargs)
        snap_util.snapshot_visibility_client_mgr(client, "mgr", "set", **kwargs)
        log.info("Validate snapshot visibility on mountpoint")
        if snap_util.validate_snapshot_visibility(client, mnt_client, **kwargs):
            return 1

    return 0


def run(ceph_cluster, **kw):
    """
    Test Details:
    1. Toggle Snapshot Visibility :
        CEPH-83621389 : Verify CLI option default behavior and option modify on existing subvolume, new subvolume

    """
    try:
        global ibm_build, common_util, fs_util, snap_util, test_reqs
        fs_util = FsUtils(ceph_cluster)
        snap_util = SnapUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        common_util = CephFSCommonUtils(ceph_cluster)
        test_data = kw.get("test_data")
        log.info("Get the Ceph pre-upgrade config data from cephfs_upgrade_config.json")
        f = clients[0].remote_file(
            sudo=True,
            file_name="/home/cephuser/cephfs_upgrade_config.json",
            file_mode="r",
        )
        pre_upgrade_config = json.load(f)
        test_reqs = {
            "config": pre_upgrade_config,
            "clients": clients,
            "nfs_servers": nfs_servers,
        }
        f.close()
        ibm_build = fs_util.get_custom_config_value(test_data, "ibm-build")
        space_str = "\t\t\t\t\t\t\t\t\t"

        log.info(
            f"\n\n {space_str}Test1 : Post-upgrade Snapshot Visibility Toggle Validation\n"
        )
        test_status = snap_visibility_test()
        if test_status == 1:
            log.error("Test 1 : Post upgrade Snapshot Visibility validation failed")
            return 1
        log.info("Post upgrade Snapshot Visibility validation succeeded \n")

        return 0

    except Exception as e:
        log.info(traceback.format_exc())
        log.error(e)
        return 1


# HELPER ROUTINES
