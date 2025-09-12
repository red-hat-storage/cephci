import json
import random
import string
import traceback
from json import JSONDecodeError
from threading import Thread

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_IO_lib import FSIO
from tests.cephfs.cephfs_system.cephfs_system_utils import CephFSSystemUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsv1
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from tests.nfs.byok.byok_tools import (
    clean_up_gklm,
    get_enctag,
    load_gklm_config,
    nfs_byok_test_setup,
)
from utility.log import Log
from utility.utils import get_cephci_config

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Prerequisites:
    #   • CephCI environment configured for your cloud (openstack or baremetal)
    #   • GKLM credentials supplied via:
    #       a) --custom-config-file <gklm_file.yaml>
    #          gklm_file.yaml should include:
    #            gklm:
    #              gklm_ip:           "0.0.0.0"    # GKLM server IP
    #              gklm_user:         "admin"      # GKLM REST API user
    #              gklm_password:     "password"   # GKLM REST API password
    #              gklm_node_username:"user"       # SSH user on GKLM host
    #              gklm_node_password:"password"   # SSH password on GKLM host
    #              gklm_hostname:     "myhost"     # GKLM server hostname
    #
    #       OR
    #       b) Multiple --custom-config entries:
    #          --custom-config "gklm_ip=0.0.0.0"
    #          --custom-config "gklm_user=admin"
    #          --custom-config "gklm_password=password"
    #          --custom-config "gklm_node_username=user"
    #          --custom-config "gklm_node_password=password"
    #          --custom-config "gklm_hostname=myhost"
    #
    #       OR
    #       c) In ~/.cephci.yaml under 'gklm_config':
    #          gklm_config:
    #            openstack:
    #              gklm_ip:             "0.0.0.0"
    #              gklm_user:           "user"
    #              gklm_password:       "password"
    #              gklm_node_username:  "user"
    #              gklm_node_password:  "password"
    #              gklm_hostname:       "hostname"
    #            ibmc:
    #              gklm_ip:             "0.0.0.0"
    #              gklm_user:           "user"
    #              gklm_password:       "password"
    #              gklm_node_username:  "user"
    #              gklm_node_password:  "password"
    #              gklm_hostname:       "hostname"
    # ===============================================================================
    NFS BYOK CephFS feature interop testing - Polarion TC CEPH-83625558
    ---------------------------------------   -------------------------
    1.Create subvolume with enctag from kmip server, create nfs export using same enctag
    2.Perform nfs mount, run IO tools - df,smallfile,crefi,tar
    3.Verify fuse-mount of nfs export path has data as encrypted.
    4.Create snapshot and Clone using snapshot.
    5.Run fuse-mount of Clone subvolume and verify data is encrypted-locked.
    6.Create NFS export of clone path using same enctag and verify NFS mount of clone path suceeds
      and data is accessible.
    7.Modify enctag with new key-id from kmip server. Create new NFS export with new enc tag for
      clone path and verify NFS mount.Export creation will faill and NFS mount doens't suceed.
    8.Run MDS failover.
    9.Verify NFS mounts are intact and data is accessible.

    Clean Up:
        Unmount subvolumes
        Remove subvolumes and subvolumegroup
        Remove FS volume if created
    """
    try:
        global byok_test_params, cephfs_common_utils, fs_util, fs_system_utils, fs_io
        test_data = kw.get("test_data")
        fs_util = FsUtilsv1(ceph_cluster, test_data=test_data)
        cephfs_common_utils = CephFSCommonUtils(ceph_cluster)
        fs_system_utils = CephFSSystemUtils(ceph_cluster)
        fs_io = FSIO(ceph_cluster)
        erasure = (
            FsUtilsv1.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        byok_test_params = None
        setup_params = None
        config = kw.get("config")
        cephci_data = get_cephci_config()
        clients = ceph_cluster.get_ceph_objects("client")
        if len(clients) < 2:
            log.error(
                "This test requires minimum 2 client nodes.This has only %s clients",
                len(clients),
            )
            return 1

        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        default_fs = config.get("fs_name", "cephfs")
        default_fs = default_fs if not erasure else "cephfs-ec"
        cleanup = config.get("cleanup", 1)
        client = clients[0]
        # BYOK params
        nfs_nodes = ceph_cluster.get_nodes("nfs")
        installer = ceph_cluster.get_nodes(role="installer")[0]

        # ------------------- Retrieving GKLM DATA -------------------
        custom_data = kw.get("test_data", None)
        #
        gklm_params = load_gklm_config(custom_data, config, cephci_data)
        gklm_ip = gklm_params["gklm_ip"]
        gklm_user = gklm_params["gklm_user"]
        gklm_password = gklm_params["gklm_password"]
        gklm_node_user = gklm_params["gklm_node_username"]
        gklm_node_password = gklm_params["gklm_node_password"]
        gklm_hostname = gklm_params["gklm_hostname"]

        nfs_name = config.get("nfs_instance_name", "nfs_byok")
        rand_str = "".join(
            [random.choice(string.ascii_lowercase + string.digits) for _ in range(3)]
        )
        gklm_client_name = f"cephfs_byok_func_client_{rand_str}"
        gklm_cert_alias = f"cephfs_byok_func_cert_{rand_str}"
        gklm_ca_cert_alias = config.get("ca_cert_alias", "scale")
        log.info("Verify Cluster is healthy before test")
        if cephfs_common_utils.wait_for_healthy_ceph(client, 300):
            log.error("Cluster health is not OK even after waiting for 300secs")
            return 1
        log.info("Cleanup existing nfs clusters")
        try:
            out, rc = client.exec_command(sudo=True, cmd="ceph nfs cluster ls -f json")
            nfscluster_ls = json.loads(out)
        except JSONDecodeError:
            nfscluster_ls = json.dumps([out])
        for nfs_cluster_name in nfscluster_ls:
            fs_util.remove_nfs_cluster(client, nfs_cluster_name)
        byok_setup_params = {
            "gklm_ip": gklm_ip,
            "gklm_user": gklm_user,
            "gklm_password": gklm_password,
            "gklm_node_user": gklm_node_user,
            "gklm_node_password": gklm_node_password,
            "gklm_hostname": gklm_hostname,
            "gklm_client_name": gklm_client_name,
            "gklm_cert_alias": gklm_cert_alias,
            "gklm_ca_cert_alias": gklm_ca_cert_alias,
            "nfs_nodes": nfs_nodes,
            "installer": installer,
            "nfs_name": nfs_name,
        }
        enctag = None
        byok_status, enctag, gklm_rest_client, cert = nfs_byok_test_setup(
            byok_setup_params
        )
        if byok_status:
            log.error("NFS BYOK setup failed")
            return 1
        byok_setup_params.update(
            {"gklm_rest_client": gklm_rest_client, "enctag": enctag, "cert": cert}
        )
        setup_params = cephfs_common_utils.test_setup(
            default_fs, client, nfs_name=nfs_name
        )

        log.info("Pre-requisites for MDS failover test")
        cmd = f"ceph fs set {default_fs} max_mds 2"
        client.exec_command(sudo=True, cmd=cmd)
        cephfs_common_utils.wait_for_two_active_mds(client, default_fs)
        cmd = f'ceph orch apply mds {default_fs} --placement="label:mds"'
        client.exec_command(sudo=True, cmd=cmd)
        cephfs_common_utils.wait_for_two_active_mds(client, default_fs)

        byok_test_params = {
            "clients": clients,
            "nfs_name": nfs_name,
            "nfs_node": nfs_nodes[0],
            "setup_params": setup_params,
            "byok_params": byok_setup_params,
        }
        test_name = "test_nfs_byok_cephfs_interop"
        test_status = nfs_byok_cephfs_test_run()
        if test_status == 1:
            result_str = f"Test {test_name} failed"
            log.error(result_str)
            return 1
        else:
            result_str = f"Test {test_name} passed"
            log.info(result_str)

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean Up in progess")
        wait_time_secs = 300
        if cephfs_common_utils.wait_for_healthy_ceph(client, wait_time_secs):
            log.error(
                "Cluster health is not OK even after waiting for %s secs",
                wait_time_secs,
            )
        if cleanup:
            try:
                if enctag is not None:
                    clean_up_gklm(
                        gklm_rest_client=byok_setup_params["gklm_rest_client"],
                        gkml_client_name=byok_setup_params["gklm_client_name"],
                        gklm_cert_alias=byok_setup_params["gklm_cert_alias"],
                    )
                if byok_test_params is not None:
                    if byok_test_params.get("gklm_client_name_1"):
                        clean_up_gklm(
                            gklm_rest_client=byok_setup_params["gklm_rest_client"],
                            gkml_client_name=byok_test_params["gklm_client_name_1"],
                            gklm_cert_alias=byok_test_params["gklm_cert_alias_1"],
                        )

            except Exception as ex:
                log.error("GKLM Cleanup failed, Error - %s", ex)
            if byok_test_params is not None:
                sv_list = byok_test_params["setup_params"]["sv_list"]
                for sv in sv_list:
                    if sv.get("nfs_mount_dir"):
                        try:
                            sv["nfs_client"].exec_command(
                                sudo=True, cmd=f"umount -l {sv['nfs_mount_dir']}"
                            )
                        except Exception as ex:
                            log.error(ex)
                if byok_test_params.get("export_list"):
                    for export in byok_test_params["export_list"]:
                        client.exec_command(
                            sudo=True,
                            cmd=f"ceph nfs export rm {nfs_name} {export}",
                            check_ec=False,
                        )
            client.exec_command(
                sudo=True,
                cmd=f"ceph nfs cluster delete {nfs_name}",
                check_ec=False,
            )
            if setup_params is not None:
                cephfs_common_utils.test_cleanup(client, setup_params, {})
                fs_util.remove_fs(client, default_fs)


def nfs_byok_cephfs_test_run():
    """
    Test workflow - CephFS interop testing with nfs byok
    """
    log.info("CephFS interop testing with nfs byok")
    setup_params = byok_test_params["setup_params"]
    clients = byok_test_params["clients"]
    nfs_name = byok_test_params["nfs_name"]
    client = clients[0]
    log.info("Apply enctag to subvolume")
    sv_list = setup_params["sv_list"]
    fs_name = setup_params["fs_name"]
    nfs_node_name = byok_test_params["nfs_node"].hostname
    byok_test_params["export_list"] = []
    test_status = 0
    for sv in sv_list:
        enc_args = {
            "enc_tag": byok_test_params["byok_params"]["enctag"],
            "add_suffix": False,
            "group_name": sv.get("group_name", None),
            "fs_name": sv["vol_name"],
        }
        sv_args = {
            "subvolume_name": sv["subvol_name"],
            "subvolume_group": sv.get("group_name", None),
        }
        sv["path"] = cephfs_common_utils.subvolume_get_path(
            client, sv["vol_name"], **sv_args
        )
        sv.update(
            {
                "nfs_export": f"/byok_{sv['subvol_name']}",
                "nfs_mount_dir": f"/mnt/byok_{sv['subvol_name']}",
                "nfs_client": "",
            }
        )
        for op in ["set", "get"]:
            op_status = cephfs_common_utils.enc_tag(
                client, op, sv["subvol_name"], **enc_args
            )
            if op == "set" and op_status:
                log.error("Enctag set failed")
                return 1

    log.info("Create NFS export to subvolume using enctag as kmip_key_id")
    extra_args = f" --kmip_key_id {byok_test_params['byok_params']['enctag']}"
    for sv in sv_list:
        if byok_nfs_export_create(client, nfs_name, sv, extra_args):
            return 1
        export_list = byok_test_params["export_list"]
        export_list.append(sv["nfs_export"])
        byok_test_params["export_list"] = export_list
    log.info("Verify NFS mount with encrypted export suceeds")
    for sv in sv_list:
        sv["nfs_client"] = random.choice(clients)
        fs_util.cephfs_nfs_mount(
            sv["nfs_client"], nfs_node_name, sv["nfs_export"], sv["nfs_mount_dir"]
        )
        log.info(
            "NFS mount for export with kmip_key suceeded for %s", sv["subvol_name"]
        )
    setup_params["sv_list"] = sv_list
    byok_test_params["setup_params"] = setup_params
    log.info("Run CephFS IO tools - df,smallfile,file_extract,postgressIO")
    io_args = {"run_time": 1}
    io_tools = ["dd", "smallfile", "file_extract", "postgresIO"]
    write_procs = []
    for sv in sv_list:
        p = Thread(
            target=fs_io.run_ios_V1,
            args=(sv["nfs_client"], sv["nfs_mount_dir"], io_tools),
            kwargs=io_args,
        )
        p.start()
        if "file_extract" in io_tools:
            io_tools.remove("file_extract")
        write_procs.append(p)
    for p in write_procs:
        fs_system_utils.wait_for_proc(p, 300)
    log.info("Verify fuse-mount of nfs export path has data as encrypted")
    for sv in sv_list:
        test_status = verify_byok_on_fuse_mount(sv, clients)
    log.info("Remove file_extract io dir,as it delays clone operation")
    for sv in sv_list:
        sv["nfs_client"].exec_command(
            sudo=True, cmd=f"rm -rf  {sv['nfs_mount_dir']}/file_extract_dir*"
        )
    log.info("Create Snapshot and Clone using snapshot")
    clone_list = []
    for sv in sv_list:
        snap_name = f"{sv['subvol_name']}_snap"
        snap_clone_args = {
            "vol_name": sv["vol_name"],
            "subvol_name": sv["subvol_name"],
            "snap_name": snap_name,
            "group_name": sv.get("group_name", None),
        }
        fs_util.create_snapshot(client, **snap_clone_args, validate=False)
        clone_name = f"{sv['subvol_name']}_clone"
        sv["clone"] = clone_name
        snap_clone_args.update(
            {
                "target_subvol_name": clone_name,
                "target_group_name": sv.get("group_name", None),
            }
        )
        clone_obj = {
            "vol_name": sv["vol_name"],
            "subvol_name": clone_name,
            "group_name": sv.get("group_name", None),
            "parent_subvol": sv,
            "nfs_export": f"/byok_{clone_name}",
            "nfs_mount_dir": f"/mnt/byok_{clone_name}",
            "nfs_client": "",
        }
        fs_util.create_clone(client, **snap_clone_args, validate=True)
        sv_args = {
            "subvolume_name": clone_obj["subvol_name"],
            "subvolume_group": clone_obj.get("group_name", None),
        }
        fs_util.validate_clone_state(
            client, snap_clone_args, expected_state="complete", timeout=300
        )
        clone_obj["path"] = cephfs_common_utils.subvolume_get_path(
            client, sv["vol_name"], **sv_args
        )
        clone_list.append(clone_obj)
    sv_list_tmp = byok_test_params["setup_params"]["sv_list"]
    sv_list_tmp.extend(clone_list)
    byok_test_params["setup_params"]["sv_list"] = sv_list_tmp
    log.info("Run fuse-mount of Clone subvolume and verify data is encrypted-locked")
    for clone in clone_list:
        clone_iter = clone.copy()
        clone_iter["nfs_mount_dir"] = clone["parent_subvol"]["nfs_mount_dir"]
        clone_iter["nfs_client"] = clone["parent_subvol"]["nfs_client"]
        test_status_tmp = verify_byok_on_fuse_mount(clone_iter, clients)
        test_status += test_status_tmp
    log.info("Create NFS export of clone path using same enctag and verify it suceeds")
    for clone in clone_list:
        if byok_nfs_export_create(client, nfs_name, clone, extra_args):
            return 1

    log.info("Verify NFS mount of clone path suceeds and data is accessible")
    for clone in clone_list:
        clone["nfs_client"] = random.choice(clients)
        fs_util.cephfs_nfs_mount(
            clone["nfs_client"],
            nfs_node_name,
            clone["nfs_export"],
            clone["nfs_mount_dir"],
        )
        log.info(
            "NFS mount for export with kmip_key suceeded for %s", clone["subvol_name"]
        )
        try:
            nfs_file_path, _ = clone["nfs_client"].exec_command(
                sudo=True, cmd=f"find {clone['nfs_mount_dir']} -maxdepth 1 -type f"
            )
            nfs_file_path = nfs_file_path.strip().split()[0]
            clone["nfs_client"].exec_command(
                sudo=True, cmd=f'echo "test" >> {nfs_file_path}'
            )
        except CommandFailed as ex:
            if "Required key not available" not in str(ex):
                log.error("IO on NFS Clone mount path failed:%s", str(ex))
                test_status += 1
    log.info("Get new enctag from kmip server")
    gklm_rest_client = byok_test_params["byok_params"]["gklm_rest_client"]
    gklm_user = byok_test_params["byok_params"]["gklm_user"]
    cert = byok_test_params["byok_params"]["cert"]
    rand_str = "".join(
        [random.choice(string.ascii_lowercase + string.digits) for _ in range(3)]
    )
    gklm_client_name_1 = f"cephfs_byok_func_client_{rand_str}"
    gklm_cert_alias_1 = f"cephfs_byok_func_cert_{rand_str}"
    byok_test_params.update(
        {
            "gklm_client_name_1": gklm_client_name_1,
            "gklm_cert_alias_1": gklm_cert_alias_1,
        }
    )

    created_client_data = gklm_rest_client.clients.create_client(gklm_client_name_1)
    new_enctag = get_enctag(
        gklm_rest_client,
        gklm_client_name_1,
        gklm_cert_alias_1,
        gklm_user,
        cert,
        created_client_data,
    )
    log.info("Umount Clone and remove NFS export")
    for clone in clone_list:
        clone["nfs_client"].exec_command(
            sudo=True, cmd=f"umount -l {clone['nfs_mount_dir']}"
        )
        fs_util.remove_nfs_export(client, nfs_name, clone["nfs_export"])

    log.info("Add new enctag to Clone subvolume")
    for clone in clone_list:
        enc_args = {
            "enc_tag": new_enctag,
            "group_name": clone.get("group_name", None),
            "fs_name": clone["vol_name"],
        }
        cephfs_common_utils.enc_tag(client, "set", clone["subvol_name"], **enc_args)
    log.info("Create new NFS export for Clone with new enctag and verify")
    extra_args = f" --kmip_key_id {new_enctag}"
    for clone in clone_list:
        if byok_nfs_export_create(client, nfs_name, clone, extra_args):
            return 1
        export_list = byok_test_params["export_list"]
        export_list.append(sv["nfs_export"])
        byok_test_params["export_list"] = export_list
    log.info("Verify NFS mount of Clone subvolume with new key")
    test_clone = random.choice(clone_list)
    test_clone["nfs_client"] = random.choice(clients)
    try:
        fs_util.cephfs_nfs_mount(
            test_clone["nfs_client"],
            nfs_node_name,
            test_clone["nfs_export"],
            test_clone["nfs_mount_dir"],
        )
        log.error(
            "FAIL:NFS mount for export with new kmip_key suceeded for %s",
            clone["subvol_name"],
        )
        test_status += 1
    except CommandFailed as ex:
        log.info(
            "Expected:NFS mount for export with new kmip_key failed for %s", str(ex)
        )

    log.info("Start IO on NFS mount in background")
    log.info("Run CephFS IO tools - df,smallfile,postgressIO")
    io_args = {"run_time": 1}
    io_tools = ["dd", "smallfile", "postgresIO"]
    write_procs = []
    for sv in sv_list:
        p = Thread(
            target=fs_io.run_ios_V1,
            args=(sv["nfs_client"], sv["nfs_mount_dir"], io_tools),
            kwargs=io_args,
        )
        p.start()
        write_procs.append(p)
    log.info("Run MDS failover test")
    cephfs_common_utils.rolling_mds_failover(client, fs_name, mds_fail_cnt=2)

    log.info("Verify NFS mount is accessible and IO can continue")
    for sv in sv_list:
        try:
            nfs_file_path, _ = sv["nfs_client"].exec_command(
                sudo=True, cmd=f"find {sv['nfs_mount_dir']} -maxdepth 1 -type f"
            )
            nfs_file_path = nfs_file_path.strip().split()[0]
            sv["nfs_client"].exec_command(
                sudo=True, cmd=f'echo "test" >> {nfs_file_path}'
            )
        except CommandFailed as ex:
            log.error("IO on NFS mount path failed:%s", str(ex))
            test_status += 1
    for p in write_procs:
        fs_system_utils.wait_for_proc(p, 300)

    return test_status


# HELPER ROUTINES


def verify_byok_on_fuse_mount(sv_obj, clients):
    """
    This method verifies that byok nfs mount data is seen as encrypted through fuse mount
    """
    byok_status = 0
    sv_obj["fuse_mount_dir"] = f"/mnt/cephfs_fuse_{sv_obj['subvol_name']}/"
    sv_obj["fuse_client"] = random.choice(clients)
    sv_obj["fuse_client"].exec_command(
        sudo=True, cmd=f"mkdir -p {sv_obj['fuse_mount_dir']}"
    )
    fs_util.fuse_mount(
        [sv_obj["fuse_client"]],
        sv_obj["fuse_mount_dir"],
        extra_params=f" -r {sv_obj['path']} --client_fs {sv_obj['vol_name']}",
    )
    nfs_file_path, _ = sv_obj["nfs_client"].exec_command(
        sudo=True, cmd=f"find {sv_obj['nfs_mount_dir']} -maxdepth 1 -type f"
    )
    fuse_file_path, _ = sv_obj["fuse_client"].exec_command(
        sudo=True, cmd=f"find {sv_obj['fuse_mount_dir']} -maxdepth 1 -type f"
    )
    nfs_file_path = nfs_file_path.strip().split()[0]
    fuse_file_path = fuse_file_path.strip().split()[0]
    out, _ = sv_obj["nfs_client"].exec_command(
        sudo=True, cmd=f"basename {nfs_file_path}| wc -c"
    )
    charcount_nfs = out.strip()
    out, _ = sv_obj["fuse_client"].exec_command(
        sudo=True, cmd=f"basename {fuse_file_path}| wc -c"
    )
    charcount_fuse = out.strip()
    if int(charcount_nfs) != int(charcount_fuse):
        log.info(
            "Verified that file names are encrypted in fuse-mount for %s",
            sv_obj["subvol_name"],
        )
    try:
        out, _ = sv_obj["fuse_client"].exec_command(
            sudo=True, cmd=f'echo "test" >> {fuse_file_path}'
        )
        log.error("FAIL:IO succeded on encrypted path of fuse-mount")
        byok_status += 1
    except CommandFailed as ex:
        if "Required key not available" not in str(ex):
            log.error(
                "Read on Encrypted path in fuse mount failed with unexpected error:%s",
                str(ex),
            )
            byok_status += 1
    sv_obj["fuse_client"].exec_command(
        sudo=True,
        cmd=f"umount -l {sv_obj['fuse_mount_dir']};rm -rf {sv_obj['fuse_mount_dir']}",
    )
    return byok_status


def byok_nfs_export_create(client, nfs_name, sv_obj, extra_args):
    """
    This method will create NFS export for given subvolume with kmip_key and verify
    """
    try:
        extra_args_ext = f" --path {sv_obj['path']} {extra_args}"
        export_args = {"extra_args": extra_args_ext}
        log.info(export_args["extra_args"])
        fs_util.create_nfs_export(
            client, nfs_name, sv_obj["nfs_export"], sv_obj["vol_name"], **export_args
        )
        log.info("NFS export with kmip_key suceeded for %s", sv_obj["subvol_name"])
        return 0
    except CommandFailed as ex:
        log.error("NFS export create failed:%s", str(ex))
        return 1
