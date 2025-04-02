"""
This is cephfs utilsV1 extension to include further common reusable methods for FS regression testing

"""

import datetime
import random
import string
import time

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


class CephFSCommonUtils(FsUtils):
    def __init__(self, ceph_cluster):
        """
        FS Utility V2 object
        Args:
            ceph_cluster (ceph.ceph.Ceph): ceph cluster
        """
        self.ceph_cluster = ceph_cluster
        super().__init__(ceph_cluster)

    def wait_for_healthy_ceph(self, client, wait_time):
        """
        This method will run ceph status and if its not HEALTH_OK, will wait for wait_time for it to be healthy
        Args:
        Required:
        Client : Client object to run command
        wait_time : Time to wait for HEALTH_OK, in seconds

        Returns 0 if healthy, 1 if unhealthy even after wait_time
        """
        ceph_healthy = 0
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=wait_time)
        while ceph_healthy == 0 and (datetime.datetime.now() < end_time):
            if self.check_ceph_status(client, "HEALTH_OK"):
                ceph_healthy = 1
            else:
                out, _ = client.exec_command(sudo=True, cmd="ceph health detail")
                if "experiencing slow operations in BlueStore" in str(out):
                    log.info("Ignoring the known warning for Bluestore Slow ops")
                    ceph_healthy = 1
                else:
                    log.info(
                        "Wait for sometime to check if Cluster health can be OK, current state : %s",
                        out,
                    )
                    time.sleep(5)

        if ceph_healthy == 0:
            client.exec_command(
                sudo=True,
                cmd="ceph fs status;ceph -s;ceph health detail",
            )
            return 1
        return 0

    def check_ceph_status(self, client, exp_msg):
        """
        This method checks for expected string in ceph health detail and returns True if found else False
        """
        out, _ = client.exec_command(sudo=True, cmd="ceph health detail")
        if exp_msg in out:
            return True
        return False

    def test_setup(self, fs_name, client):
        """
        This method is Setup to create test configuration - subvolumegroup,subvolumes,nfs servers
        Returns setup_params as dict variable upon setup sucess else 1
        """
        log.info("Create fs volume if the volume is not there")

        fs_details = self.get_fs_info(client, fs_name)
        fs_vol_created = 0
        if not fs_details:
            self.create_fs(client, fs_name)
            fs_vol_created = 1

        nfs_servers = self.ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"

        client.exec_command(
            sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server}"
        )
        if wait_for_process(client=client, process_name=nfs_name, ispresent=True):
            log.info("ceph nfs cluster created successfully")
        else:
            raise CommandFailed("Failed to create nfs cluster")

        log.info(
            "Create subvolumegroup, Create subvolume in subvolumegroup and default group"
        )
        subvolumegroup = {"vol_name": fs_name, "group_name": "subvolgroup_1"}
        self.create_subvolumegroup(client, **subvolumegroup)
        sv_list = []
        for i in range(1, 3):
            sv_def = {
                "vol_name": fs_name,
                "subvol_name": f"sv_def_{i}",
                "size": "5368706371",
            }
            self.create_subvolume(client, **sv_def)
            sv_list.append(sv_def)
        sv_non_def = {
            "vol_name": fs_name,
            "subvol_name": "sv_non_def_1",
            "group_name": "subvolgroup_1",
            "size": "5368706371",
        }
        self.create_subvolume(client, **sv_non_def)
        sv_list.append(sv_non_def)
        setup_params = {
            "fs_name": fs_name,
            "subvolumegroup": subvolumegroup,
            "sv_list": sv_list,
            "nfs_name": nfs_name,
            "nfs_server": nfs_server,
            "fs_vol_created": fs_vol_created,
        }
        return setup_params

    def test_mount(self, clients, setup_params):
        """
        This method is to run mount on test subvolumes
        Params:
        setup_params : This is the return object from test_setup
        Returns mount_details as below upon mount sucess else 1
        mount_details : {
        sv_name : {
        "kernel" : {'mounting_dir':mnt_path,
                     'mnt_client' : mnt_client,
                   }}}
        """
        try:
            client = clients.pop(0)

            mounting_dir = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(4))
            )

            mnt_type_list = ["kernel", "fuse", "nfs"]
            mnt_type_list = ["kernel"]
            mount_details = {}
            sv_list = setup_params["sv_list"]
            fs_name = setup_params["fs_name"]
            for sv in sv_list:
                sv_name = sv["subvol_name"]
                mount_details.update({sv_name: {}})
                cmd = f"ceph fs subvolume getpath {fs_name} {sv_name}"
                if sv.get("group_name"):
                    cmd += f" {sv['group_name']}"

                subvol_path, rc = client.exec_command(
                    sudo=True,
                    cmd=cmd,
                )

                mnt_path = subvol_path.strip()
                mnt_path = subvol_path
                nfs_export_name = f"/export_{sv_name}_" + "".join(
                    random.choice(string.digits) for i in range(3)
                )
                mnt_client = random.choice(clients)
                mount_params = {
                    "client": mnt_client,
                    "mnt_path": mnt_path,
                    "fs_name": fs_name,
                    "export_created": 0,
                    "nfs_export_name": nfs_export_name,
                    "nfs_server": setup_params["nfs_server"],
                    "nfs_name": setup_params["nfs_name"],
                }
                for mnt_type in mnt_type_list:
                    mounting_dir, _ = self.mount_ceph(mnt_type, mount_params)
                    mount_details[sv_name].update(
                        {
                            mnt_type: {
                                "mountpoint": mounting_dir,
                                "mnt_client": mnt_client,
                            }
                        }
                    )
                    if mnt_type == "nfs":
                        mount_details[sv_name][mnt_type].update(
                            {"nfs_export": nfs_export_name}
                        )

            return mount_details
        except Exception as ex:
            log.error(ex)
            return 1

    def test_cleanup(self, client, setup_params, mount_details):
        """
        This method is to run cleanup on test subvolumes including unmount
        Params:
        setup_params : This is return variable from test_setup
        mount_details : This is the return variable from test_mount
        """
        try:
            for sv_name in mount_details:
                for mnt_type in mount_details[sv_name]:
                    mountpoint = mount_details[sv_name][mnt_type]["mountpoint"]
                    mnt_client = mount_details[sv_name][mnt_type]["mnt_client"]
                    cmd = f"umount -l {mountpoint}"
                    mnt_client.exec_command(
                        sudo=True,
                        cmd=cmd,
                    )
                    if mnt_type == "nfs":
                        nfs_export = mount_details[sv_name][mnt_type]["nfs_export"]
                        cmd = f"ceph nfs export rm {setup_params['nfs_name']} {nfs_export}"
                        client.exec_command(
                            sudo=True,
                            cmd=cmd,
                        )
            client.exec_command(
                sudo=True,
                cmd=f"ceph nfs cluster delete {setup_params['nfs_name']}",
                check_ec=False,
            )
            sv_list = setup_params["sv_list"]
            fs_name = setup_params["fs_name"]
            for i in range(len(sv_list)):
                subvol_name = sv_list[i]["subvol_name"]
                fs_name = sv_list[i]["vol_name"]
                self.remove_subvolume(
                    client,
                    fs_name,
                    subvol_name,
                    validate=True,
                    group_name=sv_list[i].get("group_name", None),
                )
                if sv_list[i].get("group_name"):
                    group_name = sv_list[i]["group_name"]
            self.remove_subvolumegroup(client, fs_name, group_name, validate=True)
        except CommandFailed as ex:
            log.error("Cleanup failed with error : %s", ex)
            return 1
        return 0
