import json
import logging
import threading
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.nfs.byok.byok_tools import clean_up_gklm, load_gklm_config
from utility.gklm_client.gklm_client import GklmClient
from utility.log import Log
from utility.utils import get_cephci_config

# from ceph.ceph import CommandFailed


log = Log(__name__)
logger = logging.getLogger("run_log")

logging_thread = None
mds_logging_thread = None
stop_event = threading.Event()


def run(ceph_cluster, **kw):
    """
    System test - Cleanup all test objects from System test

    """
    try:
        global common_util, nested_dir
        fs_util = FsUtilsV1(ceph_cluster)
        common_util = CephFSCommonUtils(ceph_cluster)
        cephfs_config = {}
        clients = ceph_cluster.get_ceph_objects("client")
        file = "cephfs_systest_data.json"
        config = kw.get("config")
        cephci_data = get_cephci_config()
        # ------------------- Retrieving GKLM DATA -------------------
        custom_data = kw.get("test_data", None)
        gklm_params = load_gklm_config(custom_data, config, cephci_data)
        gklm_rest_client = GklmClient(
            gklm_params["gklm_ip"],
            user=gklm_params["gklm_user"],
            password=gklm_params["gklm_password"],
            verify=False,
        )
        client1 = clients[0]
        f = client1.remote_file(
            sudo=True,
            file_name=f"/home/cephuser/{file}",
            file_mode="r",
        )
        cephfs_config = json.load(f)
        log.info(f"cephfs_config:{cephfs_config}")
        sv_objs = []
        fs_list = []
        for i in cephfs_config:
            if "CLUS_MONITOR" not in i:
                fs_list.append(i)
                for j in cephfs_config[i]["group"]:
                    for sv_type in cephfs_config[i]["group"][j]:
                        sv_info = cephfs_config[i]["group"][j][sv_type]
                        for k in sv_info:
                            if k not in ["sv_prefix", "sv_cnt"]:
                                sv_obj = sv_info[k]
                                sv_objs.append(sv_obj)
        for sv_obj in sv_objs:
            for client_tmp in clients:
                if client_tmp.node.hostname == sv_obj["mnt_client1"]:
                    client_tmp.exec_command(
                        sudo=True,
                        cmd=f"umount -l {sv_obj['mnt_pt1']};rm -rf {sv_obj['mnt_pt1']}",
                    )
                if client_tmp.node.hostname == sv_obj["mnt_client2"]:
                    client_tmp.exec_command(
                        sudo=True,
                        cmd=f"umount -l {sv_obj['mnt_pt2']};rm -rf {sv_obj['mnt_pt2']}",
                    )
        for i in cephfs_config:
            if "CLUS_MONITOR" not in i:
                if cephfs_config[i].get("byok"):
                    clean_up_gklm(
                        gklm_rest_client=gklm_rest_client,
                        gkml_client_name=cephfs_config[i]["byok"]["gklm_client_name"],
                        gklm_cert_alias=cephfs_config[i]["byok"]["gklm_cert_alias"],
                    )
                if cephfs_config[i].get("nfs"):
                    nfs_name = cephfs_config[i]["nfs"]["nfs_name"]
                    fs_util.remove_nfs_cluster(client1, nfs_name)
        for vol_name in fs_list:
            fs_util.remove_fs(client1, vol_name)

        for client in clients:
            client.exec_command(
                sudo=True,
                cmd=f"rm -f /home/cephuser/{file}",
            )
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
