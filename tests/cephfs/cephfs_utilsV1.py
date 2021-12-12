"""
This is cephfs utility module
It contains all the re-useable functions related to cephfs
It installs all the pre-requisites on client nodes

"""
import datetime
import json
import logging
import random
import string
from time import sleep

from ceph.ceph import CommandFailed

log = logging.getLogger(__name__)


class FsUtils(object):
    def __init__(self, ceph_cluster):
        """
        FS Utility object
        Args:
            ceph_cluster (ceph.ceph.Ceph): ceph cluster
        """
        self.result_vals = {}
        self.return_counts = {}
        self.ceph_cluster = ceph_cluster

    def prepare_clients(self, clients, build):
        """
        Installs all the required rpms and clones the tools required for running Tests on clients
        Args:
            clients:
            build:
        Returns:

        """
        for client in clients:
            pkgs = [
                "python3",
                "python3-pip",
                "fio",
                "fuse",
                "ceph-fuse",
                "attr",
                "gcc",
                "python3-devel",
                "git",
            ]
            if build.endswith("7") or build.startswith("3"):
                pkgs.extend(
                    [
                        "@development",
                        "rh-python36",
                        "rh-python36-numpy rh-python36-scipy",
                        "rh-test-python-tools rh-python36-python-six",
                        "libffi libffi-devel",
                    ]
                )
            cmd = "yum install -y --nogpgcheck " + " ".join(pkgs)
            client.node.exec_command(sudo=True, cmd=cmd, long_running=True)
            client.node.exec_command(
                sudo=True, cmd="pip3 install xattr", long_running=True
            )
            out, rc = client.node.exec_command(sudo=True, cmd="ls /home/cephuser")
            output = out.read().decode()
            output.split()
            if "smallfile" not in output:
                client.node.exec_command(
                    cmd="git clone https://github.com/bengland2/smallfile.git"
                )

    @staticmethod
    def get_fs_info(client, fs_name="cephfs"):
        """
        Gets the fs info for the given filesystem.
        if fs_name not given.it fetches the default cephfs info
        Args:
            client:
            fs_name:

        Returns:
            dictonary with fs_name, metadata_pool,data_pool
        """
        out, rc = client.exec_command(sudo=True, cmd="ceph fs ls --format json-pretty")
        all_fs_info = json.loads(out.read().decode())
        output_dict = {}
        for fs in all_fs_info:
            if fs_name == fs["name"]:
                output_dict["fs_name"] = fs["name"]
                output_dict["metadata_pool_name"] = fs["metadata_pool"]
                output_dict["data_pool_name"] = fs["data_pools"][0]
        return output_dict

    def auth_list(self, clients, **kwargs):
        """
        Creates ceph.client.<hostname>.keyring file for the given clients
        Args:
            clients:
            **kwargs:

        Returns:

        """

        for client in clients:
            log.info("Giving required permissions for clients:")
            client.exec_command(
                sudo=True,
                cmd=f"ceph auth get client.{client.node.hostname}",
                check_ec=False,
            )
            if client.node.exit_status == 0:
                client.exec_command(
                    sudo=True, cmd=f"ceph auth del client.{client.node.hostname}"
                )
            client.exec_command(
                sudo=True,
                cmd=f"ceph auth get-or-create client.{client.node.hostname}"
                f" mon 'allow *' mds "
                f"'allow *, allow * path=/' osd 'allow *'"
                f" -o /etc/ceph/ceph.client.{client.node.hostname}.keyring",
            )
            client.exec_command(
                sudo=True,
                cmd=f"chmod 644 /etc/ceph/ceph.client.{client.node.hostname}.keyring",
            )

    def fuse_mount(self, fuse_clients, mount_point, **kwargs):
        """
        Mounts the drive using Fuse mount
        Args:
            fuse_clients:
            mount_point:
            **kwargs:
                extra_params : we can include extra parameters that needs to be passed to ceph fs fuse mount
        Returns:

        Exceptions:
            assertion error will occur if the device is not mounted
        """
        for client in fuse_clients:
            log.info("Creating mounting dir:")
            client.exec_command(sudo=True, cmd="mkdir %s" % mount_point)
            log.info("Mounting fs with ceph-fuse on client %s:" % client.node.hostname)
            if kwargs.get("new_client_hostname"):
                client.exec_command(
                    sudo=True,
                    cmd=f"ceph auth get "
                    f"client.{kwargs.get('new_client_hostname')} "
                    f"-o /etc/ceph/ceph.client"
                    f".{kwargs.get('new_client_hostname')}.keyring",
                )
            fuse_cmd = f"ceph-fuse -n client.{kwargs.get('new_client_hostname', client.node.hostname)} {mount_point} "
            if kwargs.get("extra_params"):
                fuse_cmd += f"{kwargs.get('extra_params')}"
            client.exec_command(sudo=True, cmd=fuse_cmd, long_running=True)
            out, rc = client.exec_command(cmd="mount")
            mount_output = out.read().decode().rstrip("\n")
            mount_output = mount_output.split()
            log.info("Validate Fuse Mount:")
            assert mount_point.rstrip("/") in mount_output, "Fuse mount failed"

    def kernel_mount(self, kernel_clients, mount_point, mon_node_ip, **kwargs):
        """
        Mounts the drive using kernel mount
        Args:
            kernel_clients:
            mount_point:
            mon_node_ip:
            **kwargs:
                extra_params : we can include extra parameters that needs to be passed to ceph fs fuse mount
                sub_dir: if you want to mount specific directory
        Returns:

        Exceptions:
            assertion error will occur if the device is not mounted
        """
        for client in kernel_clients:
            log.info("Creating mounting dir:")
            client.exec_command(sudo=True, cmd="mkdir %s" % mount_point)
            out, rc = client.exec_command(
                sudo=True,
                cmd=f"ceph auth get-key client.{kwargs.get('new_client_hostname', client.node.hostname)} -o "
                f"/etc/ceph/{kwargs.get('new_client_hostname', client.node.hostname)}.secret",
            )

            kernel_cmd = (
                f"mount -t ceph {mon_node_ip}:{kwargs.get('sub_dir','/')} {mount_point} "
                f"-o name={kwargs.get('new_client_hostname', client.node.hostname)},"
                f"secretfile=/etc/ceph/{kwargs.get('new_client_hostname', client.node.hostname)}.secret"
            )

            if kwargs.get("extra_params"):
                kernel_cmd += f"{kwargs.get('extra_params')}"
            client.exec_command(
                sudo=True,
                cmd=kernel_cmd,
                long_running=True,
            )
            out, rc = client.exec_command(cmd="mount")
            mount_output = out.read().decode()
            mount_output = mount_output.split()
            log.info("validate kernel mount:")
            assert mount_point.rstrip("/") in mount_output, "Kernel mount failed"

    def get_mon_node_ips(self):
        """
        Returns:
            All the mon ips as a list
        """
        return [node.ip_address for node in self.ceph_cluster.get_nodes(role="mon")]

    @staticmethod
    def client_clean_up(
        *args, fuse_clients=[], kernel_clients=[], mounting_dir="", **kwargs
    ):

        """
        This method cleans up all the client nodes and mount points
        Args:
            fuse_clients:
            kernel_clients:
            mounting_dir:
            *args:
                umount : if this argument is passed this will unmounts all the devices
            **kwargs:

        Returns:
            0 if all the clean up is passed
        """
        clients = fuse_clients + kernel_clients
        for client in clients:
            log.info("Removing files:")
            client.exec_command(
                sudo=True,
                cmd=f"rm -rf {mounting_dir}*",
                long_running=True,
                timeout=3600,
            )

            if "umount" in args:
                if client in fuse_clients:
                    log.info("Unmounting fuse client:")
                    cmd = f"fusermount -u {mounting_dir} -z"
                else:
                    log.info("Unmounting Kernel client:")
                    cmd = f"umount {mounting_dir} -l"
                client.exec_command(sudo=True, cmd=cmd)
                log.info("Removing mounting directory:")
                client.exec_command(sudo=True, cmd=f"rmdir {mounting_dir}")
                log.info("Removing keyring file:")
                client.exec_command(
                    sudo=True,
                    cmd=f"rm -rf /etc/ceph/ceph.client.{kwargs.get('client_name', client.node.hostname)}.keyring",
                )
                log.info("Removing permissions:")
                client.exec_command(
                    sudo=True,
                    cmd=f"ceph auth del client.{kwargs.get('client_name', client.node.hostname)}",
                )
                client.exec_command(
                    cmd="find /home/cephuser -type f -not -name 'authorized_keys' "
                    " -name 'Crefi' -name 'smallfile' -delete",
                    long_running=True,
                    timeout=3600,
                )
                client.exec_command(
                    cmd="cd /home/cephuser && ls -a | grep -v 'authorized_keys' |"
                    "xargs sudo rm -f",
                    long_running=True,
                    timeout=3600,
                )
                client.exec_command(sudo=True, cmd="iptables -F", check_ec=False)

        return 0

    def get_all_subvolumes(self, client, fs_list):
        """
        it gets all the subvolume details in fs provided
        if fs_list is empty it will iterate through all the fs present and gets back the list
        Args:
            client:
            fs_list:

        Returns:
            list of all sub volumes present in the group
        """
        if not fs_list:
            out, rc = client.exec_command(sudo=True, cmd="ceph fs ls --format json")
            all_fs_info = json.loads(out.read().decode())
            fs_list = [i["name"] for i in all_fs_info]
        subvolumes = []
        for fs in fs_list:
            out, rc = client.exec_command(
                sudo=True, cmd=f"ceph fs subvolume ls {fs} --format json"
            )
            all_sub_info = json.loads(out.read().decode())
            subvolumes.extend([i["name"] for i in all_sub_info])
        return subvolumes

    def create_fs(self, client, vol_name, validate=True, **kwargs):
        """
        This Function creates the cephfs volume with vol_name given
        It validates the creation operation by default.
        It supports below optional arguments also
        Args:
            client:
            vol_name:
            validate:
            **kwargs:
                check_ec = True
        Returns:

        """
        fs_cmd = f"ceph fs create {vol_name}"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=fs_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            out, rc = client.exec_command(sudo=True, cmd="ceph fs ls --format json")
            volname_ls = json.loads(out.read().decode())
            if vol_name not in [i["name"] for i in volname_ls]:
                raise CommandFailed(f"Creation of filesystem: {vol_name} failed")
        return cmd_out, cmd_rc

    def create_subvolumegroup(
        self, client, vol_name, group_name, validate=True, **kwargs
    ):
        """
        Create subvolume group with vol_name, group_name
        It supports below optional arguments also
        Args:
            vol_name:
            group_name:
            **kwargs:
                pool_layout
                uid
                gid
                mode
                validate = True
                check_ec = True
        Returns:
            Returns the cmd_out and cmd_rc for Create cmd
        """
        subvolumegroup_cmd = f"ceph fs subvolumegroup create {vol_name} {group_name}"
        if kwargs.get("pool_layout"):
            subvolumegroup_cmd += f" --pool_layout {kwargs.get('pool_layout')}"
        if kwargs.get("uid"):
            subvolumegroup_cmd += f" --uid {kwargs.get('uid')}"
        if kwargs.get("gid"):
            subvolumegroup_cmd += f" --gid {kwargs.get('gid')}"
        if kwargs.get("mode"):
            subvolumegroup_cmd += f" --mode {kwargs.get('mode')}"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=subvolumegroup_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            out, rc = client.exec_command(
                sudo=True, cmd=f"ceph fs subvolumegroup ls {vol_name} --format json"
            )
            subvolumegroup_ls = json.loads(out.read().decode())
            if group_name not in [i["name"] for i in subvolumegroup_ls]:
                raise CommandFailed(f"Creation of subvolume group: {group_name} failed")
        return cmd_out, cmd_rc

    def create_subvolume(self, client, vol_name, subvol_name, validate=True, **kwargs):
        """
        Creates Subvolume with given arguments
        It supports below optional arguments also
        Args:
            client:
            vol_name:
            subvol_name:
            validate:
            **kwargs:
                size : str in mb
                group_name : str
                pool_layout : str
                uid : str
                gid : str
                mode : str
                namespace-isolated : boolean
                check_ec = True
        Returns:
            Returns the cmd_out and cmd_rc for Create cmd
        """
        subvolume_cmd = f"ceph fs subvolume create {vol_name} {subvol_name}"
        if kwargs.get("size"):
            subvolume_cmd += f" --size {kwargs.get('size')}"
        if kwargs.get("group_name"):
            subvolume_cmd += f" --group_name {kwargs.get('group_name')}"
        if kwargs.get("pool_layout"):
            subvolume_cmd += f" --pool_layout {kwargs.get('pool_layout')}"
        if kwargs.get("uid"):
            subvolume_cmd += f" --uid {kwargs.get('uid')}"
        if kwargs.get("gid"):
            subvolume_cmd += f" --gid {kwargs.get('gid')}"
        if kwargs.get("mode"):
            subvolume_cmd += f" --mode {kwargs.get('mode')}"
        if kwargs.get("namespace-isolated"):
            subvolume_cmd += " --namespace-isolated"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=subvolume_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            listsubvolumes_cmd = f"ceph fs subvolume ls {vol_name}"
            if kwargs.get("group_name"):
                listsubvolumes_cmd += f" --group_name {kwargs.get('group_name')}"
            out, rc = client.exec_command(
                sudo=True, cmd=f"{listsubvolumes_cmd} --format json"
            )
            subvolume_ls = json.loads(out.read().decode())
            if subvol_name not in [i["name"] for i in subvolume_ls]:
                raise CommandFailed(f"Creation of subvolume : {subvol_name} failed")
        return cmd_out, cmd_rc

    def create_snapshot(
        self, client, vol_name, subvol_name, snap_name, validate=True, **kwargs
    ):
        """
        Create snapshot with vol_name, subvol_name, snap_name
        It supports below optional arguments also
        Args:
            client:
            vol_name:
            subvol_name:
            snap_name:
            validate:
            **kwargs:
                group_name : str
                check_ec = True
        Returns:
            Returns the cmd_out and cmd_rc for Create cmd
        """
        snapshot_cmd = (
            f"ceph fs subvolume snapshot create {vol_name} {subvol_name} {snap_name}"
        )
        if kwargs.get("group_name"):
            snapshot_cmd += f" --group_name {kwargs.get('group_name')}"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=snapshot_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            listsnapshot_cmd = f"ceph fs subvolume snapshot ls {vol_name} {subvol_name}"
            if kwargs.get("group_name"):
                listsnapshot_cmd += f" --group_name {kwargs.get('group_name')}"
            out, rc = client.exec_command(
                sudo=True, cmd=f"{listsnapshot_cmd} --format json"
            )
            snapshot_ls = json.loads(out.read().decode())
            if snap_name not in [i["name"] for i in snapshot_ls]:
                raise CommandFailed(f"Creation of subvolume : {snap_name} failed")
        return cmd_out, cmd_rc

    def create_clone(
        self,
        client,
        vol_name,
        subvol_name,
        snap_name,
        target_subvol_name,
        validate=True,
        **kwargs,
    ):
        """
        Creates clone based on the arguments vol_name,subvol_name,snap_name,target_subvol_name
        It supports below optional arguments also
        Args:
            client:
            vol_name:
            subvol_name:
            snap_name:
            target_subvol_name:
            validate:
            **kwargs:
                group_name
                target_group_name
                pool_layout
                check_ec = True
        Returns:
            Returns the cmd_out and cmd_rc for Create cmd
        """
        clone_cmd = f"ceph fs subvolume snapshot clone {vol_name} {subvol_name} {snap_name} {target_subvol_name}"
        if kwargs.get("group_name"):
            clone_cmd += f" --group_name {kwargs.get('group_name')}"
        if kwargs.get("target_group_name"):
            clone_cmd += f" --target_group_name {kwargs.get('target_group_name')}"
        if kwargs.get("pool_layout"):
            clone_cmd += f" --pool_layout {kwargs.get('pool_layout')}"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=clone_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            listsubvolumes_cmd = f"ceph fs subvolume ls {vol_name}"
            if kwargs.get("target_group_name"):
                listsubvolumes_cmd += f" --group_name {kwargs.get('target_group_name')}"
            out, rc = client.exec_command(
                sudo=True, cmd=f"{listsubvolumes_cmd} --format json"
            )
            subvolume_ls = json.loads(out.read().decode())
            if target_subvol_name not in [i["name"] for i in subvolume_ls]:
                raise CommandFailed(f"Creation of clone : {target_subvol_name} failed")
        return cmd_out, cmd_rc

    def remove_snapshot(
        self, client, vol_name, subvol_name, snap_name, validate=True, **kwargs
    ):
        """
        Removes the snapshot by taking snap_name,vol_name, subvol_name
        It supports below optional arguments also
        Args:
            client:
            vol_name:
            subvol_name:
            snap_name:
            validate:
            **kwargs:
                group_name : str
                force : boolean
                check_ec : boolean

        Returns:
            Returns the cmd_out and cmd_rc for remove cmd
        """
        rmsnapshot_cmd = (
            f"ceph fs subvolume snapshot rm {vol_name} {subvol_name} {snap_name}"
        )
        if kwargs.get("group_name"):
            rmsnapshot_cmd += f" --group_name {kwargs.get('group_name')}"
        if kwargs.get("force"):
            rmsnapshot_cmd += " --force"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=rmsnapshot_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            listsnapshot_cmd = f"ceph fs subvolume snapshot ls {vol_name} {subvol_name}"
            if kwargs.get("group_name"):
                listsnapshot_cmd += f" --group_name {kwargs.get('group_name')}"
            out, rc = client.exec_command(
                sudo=True, cmd=f"{listsnapshot_cmd} --format json"
            )
            snapshot_ls = json.loads(out.read().decode())
            if snap_name in [i["name"] for i in snapshot_ls]:
                raise CommandFailed(f"Remove of snapshot : {snap_name} failed")
        return cmd_out, cmd_rc

    def remove_subvolume(self, client, vol_name, subvol_name, validate=True, **kwargs):
        """
        Removes the subvolume based subvol_name,vol_name
        It supports below optional arguments also
        Args:
            client:
            vol_name:
            subvol_name:
            validate:
            **kwargs:
                group_name : str
                retain_snapshots : boolean
                force : boolean
                check_ec : boolean
        Returns:
            Returns the cmd_out and cmd_rc for remove cmd
        """
        rmsubvolume_cmd = f"ceph fs subvolume rm {vol_name} {subvol_name}"
        if kwargs.get("group_name"):
            rmsubvolume_cmd += f" --group_name {kwargs.get('group_name')}"
        if kwargs.get("retain_snapshots"):
            rmsubvolume_cmd += " --retain-snapshots"
        if kwargs.get("force"):
            rmsubvolume_cmd += " --force"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=rmsubvolume_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            listsubvolumes_cmd = f"ceph fs subvolume ls {vol_name}"
            if kwargs.get("group_name"):
                listsubvolumes_cmd += f" --group_name {kwargs.get('group_name')}"
            out, rc = client.exec_command(
                sudo=True, cmd=f"{listsubvolumes_cmd} --format json"
            )
            subvolume_ls = json.loads(out.read().decode())
            if subvol_name in [i["name"] for i in subvolume_ls]:
                raise CommandFailed(f"Deletion of clone : {subvol_name} failed")
        return cmd_out, cmd_rc

    def remove_subvolumegroup(
        self, client, vol_name, group_name, validate=True, **kwargs
    ):
        """
        Removes the sub volume group with the group_name,vol_name as argument
        It supports below optional arguments also
        Args:
            client:
            vol_name:
            group_name:
            validate:
            **kwargs:
                force
                check_ec : boolean

        Returns:
            Returns the cmd_out and cmd_rc for remove cmd
        """
        rmsubvolumegroup_cmd = f"ceph fs subvolumegroup rm {vol_name} {group_name}"
        if kwargs.get("force"):
            rmsubvolumegroup_cmd += " --force"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=rmsubvolumegroup_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            out, rc = client.exec_command(
                sudo=True, cmd=f"ceph fs subvolumegroup ls {vol_name} --format json"
            )
            subvolumegroup_ls = json.loads(out.read().decode())
            if group_name in [i["name"] for i in subvolumegroup_ls]:
                raise CommandFailed(f"Deletion of subvolume group: {group_name} failed")
        return cmd_out, cmd_rc

    def remove_fs(self, client, vol_name, validate=True, **kwargs):
        """
        Removes the filesystem with the vol_name as argument
        It supports below optional arguments also
        Args:
            client:
            vol_name:
            validate:
            **kwargs:
                check_ec : boolean

        Returns:
            Returns the cmd_out and cmd_rc for remove cmd
        """
        rmvolume_cmd = f"ceph fs volume rm {vol_name} --yes-i-really-mean-it"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=rmvolume_cmd, check_ec=kwargs.get("check_ec", True)
        )
        if validate:
            out, rc = client.exec_command(sudo=True, cmd="ceph fs ls --format json")
            volname_ls = json.loads(out.read().decode())
            if vol_name in [i["name"] for i in volname_ls]:
                raise CommandFailed(f"Creation of filesystem: {vol_name} failed")
        return cmd_out, cmd_rc

    def fs_client_authorize(
        self, client, fs_name, client_name, dir_name, permission, **kwargs
    ):
        """
        We can create ceph clients for cephfs using this module.
        We can create client with permissions on directories in cephfs.

        Args:
            client: Client_node
            fs_name: cephfs name
            client_name: ceph client
            dir_name: Directory in cephfs
            permission: r/rw (read-only/read-write)
            **kwargs:
                extra_params : we can include extra parameters as more directories & permissions

        Returns:

        """
        command = (
            f"ceph fs authorize {fs_name} client.{client_name} {dir_name} {permission} "
        )
        if kwargs.get("extra_params"):
            command += f"{kwargs.get('extra_params')}"
        out, rc = client.exec_command(sudo=True, cmd=command)
        return 0

    def activate_multiple_mdss(self, clients):
        """
        Activate Multiple MDS for ceph filesystem
        Args:
            clients: Client_nodes
        """
        for client in clients:
            fs_info = self.get_fs_info(client)
            fs_name = fs_info.get("fs_name")
            log.info("Activating Multiple MDSs:")
            client.exec_command(cmd="ceph -v | awk {'print $3'}")
            command = f"ceph fs set {fs_name} max_mds 2"
            client.exec_command(sudo=True, cmd=command)
            return 0

    def mds_cleanup(self, nodes, dir_fragmentation):
        """
        Deactivating multiple mds activated, by setting it to single mds server
        Args:
            nodes: Client_nodes
            dir_fragmentation: fragmentation directory
        """
        log.info("Deactivating Multiple MDSs")
        for node in nodes:
            fs_info = self.get_fs_info(node)
            fs_name = fs_info.get("fs_name")
            log.info("Deactivating Multiple MDSs")
            log.info("Setting Max mds to 1:")
            command = f"ceph fs set {fs_name} max_mds 1"
            node.exec_command(sudo=True, cmd=command)
            if dir_fragmentation is not None:
                log.info("Disabling directory fragmentation")
                node.exec_command(
                    sudo=True,
                    cmd="ceph fs set %s allow_dirfrags 0" % fs_info.get("fs_name"),
                )
            break
        return 0

    def mds_fail_over(self, node):
        """
        method for validating MDS fail-over functionality
        Args:
            node: Client_node
        """
        timeout = 120
        timeout = datetime.timedelta(seconds=timeout)
        start = datetime.datetime.now()
        while True:
            fs_info = self.get_fs_info(node)
            fs_name = fs_info.get("fs_name")
            out, rc = node.exec_command(
                sudo=True, cmd=f"ceph fs status {fs_name} --format json"
            )
            output = json.loads(out.read().decode())
            active_mds = [
                mds["name"] for mds in output["mdsmap"] if mds["state"] == "active"
            ]
            if len(active_mds) == 2:
                log.info("Failing MDS 1")
                node.exec_command(sudo=True, cmd="ceph mds fail 1")
                break
            else:
                log.info("waiting for active-active mds state")
                if datetime.datetime.now() - start > timeout:
                    log.error("Failed to get active-active mds")
                    return 1
        return 0

    def io_verify(self, client):
        """
        Client IO Verification
        Args:
            client: Client_node
        """
        if client.node.exit_status == 0:
            self.return_counts.update({client.node.hostname: client.node.exit_status})
            log.info("Client IO is going on,success")
        else:
            self.return_counts.update({client.node.hostname: client.node.exit_status})
            print("------------------------------------")
            print(self.return_counts)
            print("------------------------------------")
            log.error("Client IO got interrupted")
        return self.return_counts

    def pinned_dir_io_mdsfailover(
        self,
        clients,
        mounting_dir,
        dir_name,
        range1,
        range2,
        num_of_files,
        mds_fail_over,
    ):
        """
        Pinnging directories on mds failover
        Args:
            clients: Client_nodes
            mounting_dir: mounted directory
            dir_name: dir name
            range1: range2: ranges
            num_of_files: number of files
            mds_fail_over: mds failover method
        """
        log.info("Performing IOs on clients")
        for client in clients:
            for num in range(int(range1), int(range2)):
                working_dir = dir_name + "_" + str(num)
                out, rc = client.exec_command(f"sudo ls {mounting_dir}")
                output = out.read().strip().decode()
                if working_dir not in output:
                    client.exec_command(cmd=f"mkdir {mounting_dir}{dir_name}_{num}")
                log.info("Performing MDS failover:")
                mds_fail_over(client)
                command = "python3 /home/cephuser/smallfile/smallfile_cli.py "
                f"--operation create --threads 1 --file-size 100  --files  {num_of_files} "
                f"--top {mounting_dir}{dir_name}_{num}"
                client.exec_command(
                    sudo=True, cmd=command, long_running=True, timeout=300
                )
                self.return_counts = self.io_verify(client)
            break
        return self.return_counts, 0

    def get_clone_status(self, client, vol_name, clone_name, **kwargs):
        """
        Returns the clone status
        Args:
            clients: Client_nodes
            vol_name:
            clone_name:
            **kwargs:
                group_name

        """
        clone_status_cmd = f"ceph fs clone status {vol_name} {clone_name}"
        if kwargs.get("group_name"):
            clone_status_cmd += f" --group_name {kwargs.get('group_name')}"
        clone_status_cmd += " --format json"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=clone_status_cmd, check_ec=kwargs.get("check_ec", True)
        )

        return cmd_out, cmd_rc

    def clone_cancel(self, client, vol_name, clone_name, **kwargs):
        """
        Cancels the clone operation
        Args:
            clients: Client_nodes
            vol_name:
            clone_name:
            **kwargs:
                group_name

        """
        clone_status_cmd = f"ceph fs clone cancel {vol_name} {clone_name}"
        if kwargs.get("group_name"):
            clone_status_cmd += f" --group_name {kwargs.get('group_name')}"
        clone_status_cmd += " --format json"
        cmd_out, cmd_rc = client.exec_command(
            sudo=True, cmd=clone_status_cmd, check_ec=kwargs.get("check_ec", True)
        )

        return cmd_out, cmd_rc

    def validate_clone_state(
        self, client, clone, expected_state="complete", timeout=300
    ):
        """
        Validates the clone status based on the expected_state
        Args:
            clients: Client_nodes
            clone_obj:
                "vol_name": Volume name where clone as been created Ex: cephfs,
                "subvol_name": sub Volume name from where this clone has been created,
                "snap_name": snapshot name,
                "target_subvol_name": "clone_status_1",
                "group_name": "subvolgroup_1",
        """
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        clone_transistion_states = []
        cmd_out, cmd_rc = self.get_clone_status(
            client,
            clone["vol_name"],
            clone["target_subvol_name"],
            group_name=clone.get("target_group_name", ""),
        )
        status = json.loads(cmd_out.read().decode())
        if status["status"]["state"] not in clone_transistion_states:
            clone_transistion_states.append(status["status"]["state"])
        while status["status"]["state"] != expected_state:
            cmd_out, cmd_rc = self.get_clone_status(
                client,
                clone["vol_name"],
                clone["target_subvol_name"],
                group_name=clone.get("target_group_name", ""),
            )
            status = json.loads(cmd_out.read().decode())
            log.info(
                f"Clone Status of {clone['vol_name']} : {status['status']['state']}"
            )
            if status["status"]["state"] not in [
                "in-progress",
                "complete",
                "pending",
                "canceled",
            ]:
                raise CommandFailed(f'{status["status"]["state"]} is not valid status')
            if end_time < datetime.datetime.now():
                raise CommandFailed(
                    f"Clone creation has not reached to Complete state even after {timeout} sec"
                    f'Current state of the clone is {status["status"]["state"]}'
                )
        return clone_transistion_states

    def reboot_node(self, ceph_node, timeout=300):
        ceph_node.exec_command(sudo=True, cmd="reboot", check_ec=False)
        endtime = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        while datetime.datetime.now() < endtime:
            try:
                ceph_node.node.reconnect()
                return
            except BaseException:
                log.error(
                    "Failed to reconnect to the node {node} after reboot ".format(
                        node=ceph_node.node.ip_address
                    )
                )
                sleep(5)
        raise RuntimeError(
            "Failed to reconnect to the node {node} after reboot ".format(
                node=ceph_node.node.ip_address
            )
        )

    def create_file_data(self, client, directory, no_of_files, file_name, data):
        """
        This function will write files to the directory with the data given
        :param client:
        :param directory:
        :param no_of_files:
        :param file_name:
        :param data:
        :return:
        """
        files = [f"{file_name}_{i}" for i in range(0, no_of_files)]
        client.exec_command(
            sudo=True,
            cmd=f"cd {directory};echo {data * random.randint(100, 500)} | tee {' '.join(files)}",
        )

    def get_files_and_checksum(self, client, directory):
        """
        This will collect the filenames and their respective checksums and returns the dictionary
        :param client:
        :param directory:
        :return:
        """
        out, rc = client.exec_command(
            sudo=True, cmd=f"cd {directory};ls -lrt |  awk {{'print $9'}}"
        )
        file_list = out.read().decode().strip().split()
        file_dict = {}
        for file in file_list:
            out, rc = client.exec_command(sudo=True, cmd=f"md5sum {directory}/{file}")
            md5sum = out.read().decode().strip().split()
            file_dict[file] = md5sum[0]
        return file_dict

    def set_quota_attrs(self, client, file, bytes, directory, **kwargs):
        """
        Args:
            file: sets the value as total number of files limit
            bytes: sets the value as total number of bytes limit
            directory: sets the above limits to this directory
            **kwargs:

        Returns:

        """
        if file:
            quota_set_cmd = f"setfattr -n ceph.quota.max_files -v {file}"
            client.exec_command(sudo=True, cmd=f"{quota_set_cmd} {directory}")
        if bytes:
            quota_set_cmd = f"setfattr -n ceph.quota.max_bytes -v {bytes}"
            client.exec_command(sudo=True, cmd=f"{quota_set_cmd} {directory}")

    def get_quota_attrs(self, client, directory, **kwargs):
        """
        Gets the quota of the given directory
        Args:
            client:
            directory: Gets the quota values for the given directory
            **kwargs:

        Returns:
            quota_dict
            will have values in this format {file : 0, bytes: 0}
        """
        quota_dict = {}
        out, rc = client.exec_command(
            f"getfattr --only-values -n ceph.quota.max_files {directory}"
        )
        file_quota = out.read().decode()
        quota_dict["files"] = int(file_quota)
        out, rc = client.exec_command(
            f"getfattr --only-values -n ceph.quota.max_bytes {directory}"
        )
        byte_quota = out.read().decode()
        quota_dict["bytes"] = int(byte_quota)
        return quota_dict

    def file_quota_test(self, client, mounting_dir, quota_attrs):
        total_files = quota_attrs.get("files")
        temp_str = "".join([random.choice(string.ascii_letters) for _ in range(3)])
        files_in_dir = int(self.get_total_no_files(client, mounting_dir))
        if files_in_dir >= total_files:
            files = 1
        else:
            files = total_files - files_in_dir
        for i in range(1, files + 15):
            out, rc = client.exec_command(
                sudo=True,
                cmd=f"cd {mounting_dir};touch file_{temp_str}_{i}.txt",
                long_running=True,
                check_ec=False,
            )
            if rc == 1 and i < files:
                raise CommandFailed(
                    f"total allowed files {files} and current iteration {i}"
                )
            log.info(
                f"Return value for file_{temp_str}_{i}.txt is {rc} and total_allowed_files: {files}"
            )
        if rc == 0 and total_files != 0:
            raise CommandFailed("We are able to create more files than what we set")
        elif rc == 1 and total_files == 0:
            raise CommandFailed(
                f"File Attribute is set to {total_files} still we are not able to create files"
            )
        else:
            pass

    def get_total_no_files(self, client, directory):
        out, rc = client.exec_command(f"find {directory} -type f -print | wc -l")
        total_files = out.read().decode()
        out, rc = client.exec_command(f"find {directory} -type d -print | wc -l")
        total_dir = out.read().decode()
        print(f"total dir : {total_dir}")
        return total_files

    def subvolume_authorize(self, client, vol_name, subvol_name, client_name, **kwargs):
        """
        Create client with permissions on cephfs subvolume
        Args:
            client: client node
            vol_name: Cephfs volume name
            subvol_name: Cephfs subvolume name
            client_name: Name of client to be created
            **kwargs:
                extra_params : subvolumegroup name, access level etc.
        """
        command = f"ceph fs subvolume authorize {vol_name} {subvol_name} {client_name}"
        if kwargs.get("extra_params"):
            command += f" {kwargs.get('extra_params')}"
            client.exec_command(sudo=True, cmd=command)

    def get_pool_df(self, client, pool_name, **kwargs):
        """
        Gets the pool Avaialble space and used space details
        Args:
            client: client node
            pool_name: Name of the pool
            **kwargs:
                vol_name : Name of the fs volume for which we need the status
        Return:
            returns pool details if present else returns None
        sample pool Return dictonary:
            {
            "avail": 33608753152,
            "id": 5,
            "name": "cephfs-metadata",
            "type": "metadata",
            "used": 278888448
        },

        """
        fs_status_cmd = "ceph fs status"
        if kwargs.get("vol_name"):
            fs_status_cmd += f" {kwargs.get('vol_name')}"
        fs_status_cmd += " --format json"
        out, rc = client.exec_command(sudo=True, cmd=fs_status_cmd)
        fs_status = json.loads(out.read().decode())
        pool_status = fs_status["pools"]
        for pool in pool_status:
            if pool["name"] == pool_name:
                return pool
        else:
            return None
