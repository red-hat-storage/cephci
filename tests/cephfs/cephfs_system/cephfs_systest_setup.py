import json
import os
import random
import secrets
import string
import traceback

from tests.cephfs.cephfs_scale.cephfs_scale_utils import CephfsScaleUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

global log
log = Log(__name__)


LOG_FORMAT = "%(asctime)s (%(name)s) [%(levelname)s] - %(message)s"


def run(ceph_cluster, **kw):
    """
    Setup to deploy CephFS System test configuration.

    This function sets up the clients, prepares the CephFS environment, creates subvolumes,
    mounts them, runs IO operations, and collects logs and status information.

    :param ceph_cluster: Ceph cluster object
    :param kw: Additional keyword arguments for configuration
    :return: 0 if successful, 1 if an error occurs
    """
    try:
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        fs_scale_utils = CephfsScaleUtils(ceph_cluster)
        config = kw.get("config")
        cephfs_config = {}
        build = config.get("build", config.get("rhbuild"))
        systest_monitor = config.get("systest_monitor", 1)
        clients = ceph_cluster.get_ceph_objects("client")
        fs_util_v1.prepare_clients(clients, build)
        fs_util_v1.auth_list(clients)
        file = "cephfs_systest_data.json"
        mnt_type_list = ["kernel", "fuse", "nfs"]
        client1 = clients[0]
        ephemeral_pin = config.get("ephemeral_pin", 1)
        client1.upload_file(
            sudo=True,
            src=f"tests/cephfs/cephfs_system/{file}",
            dst=f"/home/cephuser/{file}",
        )
        f = client1.remote_file(
            sudo=True,
            file_name=f"/home/cephuser/{file}",
            file_mode="r",
        )
        cephfs_config = json.load(f)

        nfs_name = "cephfs_systest_nfs"
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        nfs_server = "magna027"
        client1.exec_command(
            sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server}"
        )
        if wait_for_process(client=client1, process_name=nfs_name, ispresent=True):
            log.info("ceph nfs cluster created successfully")
            nfs_export = "/export_" + "".join(
                secrets.choice(string.digits) for i in range(3)
            )
            nfs_export_list = []
        if ephemeral_pin == 1:
            log.info("Setup Ephemeral Random pinning")
            cmd = "ceph config set mds mds_export_ephemeral_random true;"
            cmd += "ceph config set mds mds_export_ephemeral_random_max 0.75"
            client1.exec_command(sudo=True, cmd=cmd)

        for i in cephfs_config:
            mds_pin_cnt = 1
            fs_details = fs_util_v1.get_fs_info(client1, fs_name=i)
            if not fs_details:
                log.info(f"Creating FileSystem {i}")
                fs_util_v1.create_fs(client1, i)
                active_mds_cnt = cephfs_config[i]["mds"]["active"]
                standby_replay = cephfs_config[i]["mds"]["standby_replay"]
                cmd = f'ceph orch apply mds {i} --placement="label:mds"'
                cmd += f"ceph fs set {i} max_mds {active_mds_cnt};"
                cmd += f"ceph fs set {i} allow_standby_replay {standby_replay}"
                client1.exec_command(sudo=True, cmd=cmd)
            for j in cephfs_config[i]["group"]:
                if "default" not in j:
                    svg_iter = {"vol_name": i, "group_name": j}
                    log.info(f"Creating subvolgroup {j} in FS volume {i}")
                    fs_util_v1.create_subvolumegroup(client1, **svg_iter)
                sv_info = cephfs_config[i]["group"][j]
                for type in sv_info:
                    sv_info_tmp = sv_info[type]
                    if "default" in j and "shared" in type:
                        sv_cnt = config.get(
                            "sv_def_shared_cnt", int(sv_info_tmp["sv_cnt"])
                        )
                    elif "default" in j and "unique" in type:
                        sv_cnt = config.get("sv_def_cnt", int(sv_info_tmp["sv_cnt"]))
                    elif "default" not in j and "shared" in type:
                        sv_cnt = config.get("sv_shared_cnt", int(sv_info_tmp["sv_cnt"]))
                    else:
                        sv_cnt = config.get("sv_cnt", int(sv_info_tmp["sv_cnt"]))
                    for k in range(0, sv_cnt):
                        sv_name = f"{sv_info_tmp['sv_prefix']}_{k}"
                        sv_iter = {
                            "vol_name": i,
                            "subvol_name": sv_name,
                        }
                        if "default" not in j:
                            sv_iter.update({"group_name": j})
                        log.info(
                            f"Creating subvolume {sv_name} in {j} group in FS volume {i}"
                        )
                        fs_util_v1.create_subvolume(client1, **sv_iter)
                        cephfs_config[i]["group"][j][type].update({sv_name: {}})
                        mnt_client1 = random.choice(clients)
                        mnt_client2 = random.choice(clients)
                        cmd = f"ceph fs subvolume getpath {i} {sv_name}"
                        if "default" not in j:
                            cmd += f" {j}"
                        subvol_path, rc = client1.exec_command(
                            sudo=True,
                            cmd=cmd,
                        )
                        mnt_path = subvol_path.strip()
                        mount_params = {
                            "fs_util": fs_util_v1,
                            "mnt_path": mnt_path,
                            "fs_name": i,
                            "export_created": 0,
                        }
                        mnt_type = random.choice(mnt_type_list)
                        if mnt_type == "nfs":
                            nfs_export_name = f"{nfs_export}_{sv_name}"
                            nfs_export_list.append(nfs_export_name)
                            mount_params.update(
                                {
                                    "nfs_name": nfs_name,
                                    "nfs_export_name": nfs_export_name,
                                    "nfs_server": nfs_server,
                                }
                            )
                        log.info(f"Perform {mnt_type} mount of {sv_name}")
                        mount_params.update({"client": mnt_client1})
                        mounting_dir1, _ = fs_util_v1.mount_ceph(mnt_type, mount_params)
                        mount_params.update({"client": mnt_client2})
                        mounting_dir2, _ = fs_util_v1.mount_ceph(mnt_type, mount_params)
                        cephfs_config[i]["group"][j][type][sv_name].update(
                            {
                                "mnt_pt1": mounting_dir1,
                                "mnt_client1": mnt_client1.node.hostname,
                                "mnt_pt2": mounting_dir2,
                                "mnt_client2": mnt_client2.node.hostname,
                                "mnt_type": mnt_type,
                            }
                        )
                        if mnt_type == "nfs":
                            cephfs_config[i]["group"][j][type][sv_name].update(
                                {
                                    "nfs_export": nfs_export_name,
                                    "nfs_name": nfs_name,
                                    "nfs_server": nfs_server,
                                }
                            )
                        elif (ephemeral_pin == 1) and (mds_pin_cnt < 5):
                            log.info(f"Configure MDS pinning : {mds_pin_cnt}")
                            cmd = f"setfattr -n ceph.dir.pin.random -v 0.75 {mounting_dir1}"
                            mnt_client1.exec_command(
                                sudo=True,
                                cmd=cmd,
                            )
                            cmd = f"setfattr -n ceph.dir.pin.random -v 0.75 {mounting_dir2}"
                            mnt_client2.exec_command(
                                sudo=True,
                                cmd=cmd,
                            )
                            mds_pin_cnt += 1

        log.info(f"CephFS System Test config : {cephfs_config}")
        for client in clients:
            f = client.remote_file(
                sudo=True,
                file_name=f"/home/cephuser/{file}",
                file_mode="w",
            )
            f.write(json.dumps(cephfs_config, indent=4))
            f.write("\n")
            f.flush()

        log_base_dir = os.path.dirname(log.logger.handlers[0].baseFilename)

        log_dir = f"{log_base_dir}/cephfs_systest_data"
        file_path = f"{log_dir}/cephfs_systest_config.json"
        os.mkdir(log_dir)
        cmd = f"touch {file_path}"
        os.system(cmd)

        src_path = f"/home/cephuser/{file}"
        dst_path = file_path
        client1.download_file(src=src_path, dst=dst_path, sudo=True)
        log.info(f"Downloaded {src_path} to {dst_path}")
        if systest_monitor == 1:
            cmd_list = [
                "ceph crash ls-new",
                "ceph fs status",
                "ceph fs dump",
                "ceph -s",
                "ceph df",
            ]
            log.info("Start logging the Ceph Cluster Cluster status to a log dir")
            fs_scale_utils.start_logging(clients[0], cmd_list, log_dir)
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Stop logging the Ceph Cluster Cluster status to a log dir")
        fs_scale_utils.stop_logging()
