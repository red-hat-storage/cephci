"""
This is cephfs utilsV1 extension to include further common reusable methods for FS regression testing

"""

import datetime
import json
import random
import secrets
import string
import time

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from tests.cephfs.exceptions import UnsupportedFeature
from tests.smb.smb_operations import (
    deploy_smb_service_imperative,
    smb_cifs_mount,
    smbclient_check_shares,
)
from utility.log import Log
from utility.retry import retry

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

    def wait_for_healthy_ceph(self, client, wait_time=300):
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
        accepted_list = [
            "experiencing slow operations in BlueStore",
            "Slow OSD heartbeats",
            "stray daemon(s) not managed by cephadm",
        ]
        while ceph_healthy == 0 and (datetime.datetime.now() < end_time):
            if self.check_ceph_status(client, "HEALTH_OK"):
                ceph_healthy = 1
            else:
                out, _ = client.exec_command(sudo=True, cmd="ceph health detail")
                if any(msg in str(out) for msg in accepted_list):
                    log.info(
                        "Ignoring the known warning for Bluestore Slow ops and OSD heartbeats"
                    )
                    log.warning("Cluster health can be OK, current state : %s", out)
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

    def test_setup(self, fs_name, client, nfs_name="cephfs-nfs"):
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
        out, _ = client.exec_command(sudo=True, cmd="ceph nfs cluster ls")
        if nfs_name not in out:
            self.create_nfs(
                client,
                nfs_cluster_name=nfs_name,
                nfs_server_name=nfs_server,
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

    def test_mount(
        self, clients, setup_params, mnt_type_list=["kernel", "fuse", "nfs"]
    ):
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
            client = clients[0]

            mounting_dir = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(4))
            )

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
                listsnapshot_cmd = (
                    f"ceph fs subvolume snapshot ls {fs_name} {subvol_name}"
                )
                if sv_list[i].get("group_name"):
                    listsnapshot_cmd += f" --group_name {sv_list[i]['group_name']}"
                out, rc = client.exec_command(
                    sudo=True, cmd=f"{listsnapshot_cmd} --format json"
                )
                snapshot_ls = json.loads(out)
                for j in snapshot_ls:
                    snap_name = j["name"]
                    self.remove_snapshot(
                        client,
                        fs_name,
                        subvol_name,
                        snap_name,
                        validate=True,
                        group_name=sv_list[i].get("group_name", None),
                    )
                self.remove_subvolume(
                    client,
                    fs_name,
                    subvol_name,
                    validate=True,
                    group_name=sv_list[i].get("group_name", None),
                )
            if setup_params.get("subvolumegroup"):
                self.remove_subvolumegroup(
                    client,
                    fs_name,
                    setup_params["subvolumegroup"]["group_name"],
                    validate=True,
                )
        except CommandFailed as ex:
            log.error("Cleanup failed with error : %s", ex)
            return 1
        return 0

    def generate_mount_dir(self):
        """Generate a random mounting directory name."""
        return "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
        )

    @retry(CommandFailed, tries=5, delay=60)
    def subvolume_get_path(self, client, fs_name, **kwargs):
        """
        Get the path of a subvolume.

        Args:
            client: The client node where the subvolume is located.
            fs_name (str): The name of the Ceph file system.
            **kwargs: Optional parameters:
                - subvolume_name (str): Name of the subvolume.
                - subvolume_group (str): Name of the subvolume group.

        Returns:
            str: The path of the subvolume if successful.
            int: Returns 1 in case of a failure.
        """
        try:
            cmd = f"ceph fs subvolume getpath {fs_name} {kwargs['subvolume_name']}"
            if kwargs.get("subvolume_group"):
                cmd += f" {kwargs['subvolume_group']}"
            out, _ = client.exec_command(sudo=True, cmd=cmd)
            return out.strip()
        except CommandFailed as ex:
            log.error("Failed to get subvolume path with error: %s", ex)
            return 1

    def setup_cephfs_mount(self, client, fs_name, mount_type, **kwargs):
        """Set up a CephFS mount after creating the subvolume and subvolume group.
        Args:
            client: The client node where the mount will be set up.
            fs_name (str): The name of the Ceph file system.
            mount_type (str): The type of mount (e.g., 'fuse', 'smb', 'nfs').
            **kwargs: Optional parameters for the mount.
        Returns:
            str: The path of the mount if successful.
            int: Returns 1 in case of a failure.
        """
        log.info("Setting up cephfs mount for {}".format(mount_type))
        mounting_dir = self.generate_mount_dir()
        mount_path = f"/mnt/cephfs_{mount_type}_{mounting_dir}/"

        subvolume_name = kwargs.get("subvolume_name")
        subvolume_group = kwargs.get("subvolume_group")

        if subvolume_name:
            if subvolume_group:
                self.create_subvolumegroup(client, fs_name, subvolume_group)
            self.create_subvolume(
                client, fs_name, subvolume_name, group_name=subvolume_group
            )

            if subvolume_group:
                subvol_path = self.subvolume_get_path(
                    client,
                    fs_name,
                    subvolume_name=subvolume_name,
                    subvolume_group=subvolume_group,
                )
            else:
                subvol_path = self.subvolume_get_path(
                    client, fs_name, subvolume_name=subvolume_name
                )

            kwargs.update({"subvol_path": subvol_path})
        kwargs.update({"mount_path": mount_path})

        if mount_type == "fuse":
            return self.setup_fuse_mount(client, fs_name, **kwargs)
        elif mount_type == "smb":
            return self.setup_smb_mount(client, fs_name, **kwargs)
        elif mount_type == "nfs":
            return self.setup_nfs_mount(client, fs_name, **kwargs)
        else:
            raise UnsupportedFeature(
                "Invalid mount type. Choose from 'fuse', 'smb', or 'nfs'."
            )

    def setup_fuse_mount(self, client, fs_name, **kwargs):
        """Set up a FUSE mount for a CephFS file system.
        Args:
            client: The client node where the mount will be set up.
            fs_name (str): The name of the Ceph file system.
            **kwargs: Optional parameters for the mount.
        Returns:
            str: The path of the mount if successful.
            int: Returns 1 in case of a failure.
        """
        log.info("Setting up fuse mount")
        for key, value in kwargs.items():
            log.info(f"{key} = {value}")
        mount_path = kwargs.get("mount_path")
        subvolume_name = kwargs.get("subvolume_name")
        subvol_path = kwargs.get("subvol_path")

        if subvolume_name:
            extra_params = f" --client_fs {fs_name} -r {subvol_path.strip()}"
        else:
            extra_params = f" --client_fs {fs_name}"

        self.fuse_mount([client], mount_path, extra_params=extra_params)
        return mount_path

    def setup_smb_mount(self, client, fs_name, **kwargs):
        """Set up an SMB mount for a CephFS file system.
        Args:
            client: The client node where the mount will be set up.
            fs_name (str): The name of the Ceph file system.
            **kwargs: Optional parameters for the mount.
        Returns:
            str: The path of the mount if successful.
            int: Returns 1 in case of a failure.
        """
        log.info("Setting up smb mount")
        mount_path = kwargs.get("mount_path")
        smb_nodes = self.ceph_cluster.get_nodes("smb")
        installer = kwargs.get(
            "installer", self.ceph_cluster.get_nodes(role="installer")[0]
        )

        try:
            deploy_smb_service_imperative(
                kwargs.get("installer", installer),
                fs_name,
                kwargs.get("subvolume_group", "smb"),
                kwargs.get("subvolume_name", ["sv1"]),
                kwargs.get("smb_subvolume_mode", "0777"),
                kwargs.get("smb_cluster_id", "smb1"),
                kwargs.get("auth_mode", "user"),
                kwargs.get("smb_user_name", "user1"),
                kwargs.get("smb_user_password", "passwd"),
                kwargs.get("smb_shares", ["share1", "share2"]),
                kwargs.get("path", "/"),
                kwargs.get("domain_realm"),
                kwargs.get("custom_dns"),
            )

            smbclient_check_shares(
                smb_nodes,
                client,
                kwargs.get("smb_shares", ["share1", "share2"]),
                kwargs.get("smb_user_name", "user1"),
                kwargs.get("smb_user_password", "passwd"),
                kwargs.get("auth_mode", "user"),
                kwargs.get("domain_realm"),
            )

            smb_cifs_mount(
                smb_nodes[0],
                client,
                kwargs.get("smb_shares", ["share1", "share2"])[0],
                kwargs.get("smb_user_name", "user1"),
                kwargs.get("smb_user_password", "passwd"),
                kwargs.get("auth_mode", "user"),
                kwargs.get("domain_realm"),
                mount_path,
            )
        except Exception as e:
            log.error("failed to setup smb with error: {}".format(e))
            return 1

        return mount_path

    def setup_nfs_mount(self, client, fs_name, **kwargs):
        """Set up an NFS mount for a CephFS file system.
        Args:
            client: The client node where the mount will be set up.
            fs_name (str): The name of the Ceph file system.
            **kwargs: Optional parameters for the mount.
        Returns:
            str: The path of the mount if successful.
            int: Returns 1 in case of a failure.
        """
        log.info("Setting up nfs mount")
        mount_path = kwargs.get("mount_path")
        subvol_path = kwargs.get("subvol_path")
        cluster = kwargs.get("nfs_cluster_name", "nfs-cluster-1")
        server = kwargs.get("nfs_server_name")
        binding = kwargs.get(
            "binding",
            "/export_" + "".join(secrets.choice(string.digits) for _ in range(3)),
        )

        self.create_nfs(client, cluster, nfs_server_name=server)

        wait_for_process(client, process_name=cluster)

        subvolume_name = kwargs.get("subvolume_name")
        if subvolume_name:
            self.create_nfs_export(
                client, cluster, binding, fs_name, path=subvol_path.strip()
            )
        else:
            self.create_nfs_export(client, cluster, binding, fs_name)

        rc = self.cephfs_nfs_mount(client, server, binding, mount_path)
        if not rc:
            log.error("cephfs nfs export mount failed")
            return 1

        return mount_path

    def enc_tag(
        self,
        client,
        op_type,
        subvol,
        enc_tag="cephfs_enctag",
        add_suffix=True,
        validate=True,
        **kwargs,
    ):
        """
        This method is to set,get or remove the enctag in subvolume command
        Required_params:
        op_type : This could be get,set or rm , Type - str
        subvol : Subvolume name where enc_tag needs to be updated
        Optional_params:
        fs_name: FS name, default - 'cephfs' , type - str
        group_name : If subvol belongs to non-default group,specify group name
        enc_tag : Value for enctag field, Type - str, default - "cephfs_enctag"

        Returns: Upon sucess - 0 for op_type set/rm, enc_tag string if op_type is get,1 upon failure
        """
        rand_str = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(4))
        )

        cmd = f"ceph fs subvolume enctag {op_type} {kwargs['fs_name']} {subvol}"
        if kwargs.get("group_name"):
            cmd += f" --group_name {kwargs['group_name']}"
        if op_type == "set":
            enc_tag = kwargs.get(enc_tag, enc_tag)
            if kwargs.get(add_suffix, add_suffix):
                enc_tag = f"{enc_tag}_{rand_str}"
            cmd += f" --enctag {enc_tag}"
        out, _ = client.exec_command(
            sudo=True,
            cmd=cmd,
            check_ec=False,
        )
        if op_type == "get":
            enc_tag_val = out.strip()
            return enc_tag_val
        if validate:
            cmd = f"ceph fs subvolume info {kwargs['fs_name']} {subvol} --f json"
            if kwargs.get("group_name"):
                cmd += f" --group_name {kwargs['group_name']}"
            out, _ = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
            parsed_data = json.loads(out)
            if op_type == "set":
                if enc_tag in parsed_data.get("enctag", None):
                    log.info("%s set on %s", enc_tag, subvol)
                    return 0
                else:
                    log.error(("%s NOT set on %s", enc_tag, subvol))
                    return 1
            elif op_type == "rm":
                if parsed_data.get("enctag", None):
                    log.error(
                        "enc_tag %s not removed on %s", parsed_data["enctag"], subvol
                    )
                    return 1
                else:
                    log.info(
                        ("enc_tag %s removed on %s", parsed_data["enctag"], subvol)
                    )
                    return 0
        return 0

    def client_mount_cleanup(self, client, mount_path_prefix="/mnt/cephfs_"):
        """
        Unmount and remove the specified mount path on the client.
        :param client: Client object to execute the command
        :param mount_path_prefix: Path prefix to be unmounted and removed
        """
        try:
            client.exec_command(
                sudo=True,
                cmd=f"umount -l {mount_path_prefix}*/",
            )
            log.info(f"Successfully unmounted on {client.node.hostname}")

            client.exec_command(
                sudo=True,
                cmd=f"rm -rf {mount_path_prefix}*/",
            )
            log.info(f"Successfully deleted mount path on {client.node.hostname}")
            return 0
        except CommandFailed as ex:
            if "Command exceed the allocated execution time" in str(ex):
                log.error("Client mount cleanup failed: %s", str(ex))
                return 1
            elif "not mounted" in str(ex):
                return 0

    def rolling_mds_failover(self, client, fs_name, mds_fail_cnt=3):
        """
        This method will perform Rolling MDS failover on given FS, wait for active MDS
        Return 0 on success, 1 on failure
        """
        mds_ls = self.get_active_mdss(client, fs_name=fs_name)
        log.info("Rolling failures of MDS's")
        out, rc = client.exec_command(
            sudo=True, cmd=f"ceph fs status {fs_name} --format json"
        )
        log.info(out)
        output = json.loads(out)
        st1 = "standby"
        st2 = "standby-replay"
        standby_replay_mds = [
            mds["name"] for mds in output["mdsmap"] if (mds["state"] == st2)
        ]
        sample_cnt = min(mds_fail_cnt, len(standby_replay_mds))
        if len(standby_replay_mds) == 0:
            standby_mds = [
                mds["name"] for mds in output["mdsmap"] if (mds["state"] == st1)
            ]
            sample_cnt = min(mds_fail_cnt, len(standby_mds))
        mds_to_fail = random.sample(mds_ls, sample_cnt)
        for mds in mds_to_fail:
            out, rc = client.exec_command(cmd=f"ceph mds fail {mds}", client_exec=True)
            log.info(out)
            time.sleep(1)

        log.info(
            f"Waiting for atleast 2 active MDS after failing {len(mds_to_fail)} MDS"
        )
        if self.wait_for_two_active_mds(client, fs_name):
            log.error("Wait for 2 active MDS failed")
            return 1
        return 0

    def wait_for_two_active_mds(
        self, client1, fs_name, max_wait_time=180, retry_interval=10
    ):
        """
        Wait until two active MDS (Metadata Servers) are found or the maximum wait time is reached.
        Args:
            max_wait_time (int): Maximum wait time in seconds (default: 180 seconds).
            retry_interval (int): Interval between retry attempts in seconds (default: 5 seconds).
        Returns:
            0 on success, 1 on failure
        """
        start_time = time.time()
        while time.time() - start_time < max_wait_time:
            out, rc = client1.exec_command(
                cmd=f"ceph fs status {fs_name} -f json", client_exec=True
            )
            log.info(out)
            parsed_data = json.loads(out)
            active_mds = [
                mds
                for mds in parsed_data.get("mdsmap", [])
                if mds.get("rank", -1) in [0, 1] and mds.get("state") == "active"
            ]
            if len(active_mds) == 2:
                return 0  # Two active MDS found
            else:
                time.sleep(retry_interval)  # Retry after the specified interval

        return 1

    def is_subscription_registered(self, client):
        """Identify if the system is subscribed or not"""
        try:
            cmd = (
                "subscription-manager status | grep 'Overall Status' | awk '{print $3}'"
            )
            out, _ = client.exec_command(sudo=True, cmd=cmd)
            status = out.strip()
            log.info("Subscription status output: {}".format(status))
            # In RHEL-9, "Disabled" means registered
            # In RHEL-10, "Registered" means registered
            is_subscribed = status.lower() in ["registered", "disabled"]
            log.info(
                "System is {}".format(
                    "subscribed" if is_subscribed else "not subscribed"
                )
            )
            return is_subscribed
        except Exception as e:
            raise CommandFailed(
                "Failed to identify subscription status \n error: {0}".format(e)
            )
