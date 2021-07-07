"""
This is cephfs utility module
It contains all the re-useable functions related to cephfs
It installs all the pre-requisites on client nodes

"""
import json
import logging

from ceph.ceph import CommandFailed

log = logging.getLogger(__name__)


class FsUtils(object):
    def __init__(self, ceph_cluster):
        """
        FS Utility object
        Args:
            ceph_cluster (ceph.ceph.Ceph): ceph cluster
        """
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
            fuse_cmd = f"ceph-fuse -n client.{kwargs.get('new_client_hostname', client.node.hostname)} {mount_point} "
            if kwargs.get("extra_params"):
                fuse_cmd += f"{kwargs.get('extra_params')}"
            client.exec_command(sudo=True, cmd=fuse_cmd)
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
                f"mount -t ceph {mon_node_ip}:/{kwargs.get('sub_dir','')} {mount_point} "
                f"-o name={kwargs.get('new_client_hostname', client.node.hostname)},"
                f"secretfile=/etc/ceph/{kwargs.get('new_client_hostname', client.node.hostname)}.secret"
            )

            if kwargs.get("extra_params"):
                kernel_cmd += f"{kwargs.get('extra_params')}"
            client.exec_command(
                sudo=True,
                cmd=kernel_cmd,
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
                cmd=f"rm -rf {mounting_dir}/*",
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

        Args:
            client:
            vol_name:

        Returns:

        """
        fs_cmd = f"ceph fs create {vol_name}"
        client.exec_command(sudo=True, cmd=fs_cmd)
        if validate:
            out, rc = client.exec_command(sudo=True, cmd="ceph fs ls --format json")
            volname_ls = json.loads(out.read().decode())
            if vol_name not in [i["name"] for i in volname_ls]:
                raise CommandFailed(f"Creation of filesystem: {vol_name} failed")

    def create_subvolumegroup(
        self, client, vol_name, group_name, validate=True, **kwargs
    ):
        """
        Args:
            vol_name:
            group_name:
            **kwargs:
                pool_layout
                uid
                gid
                mode
                validate = True
        Returns:
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
        client.exec_command(sudo=True, cmd=subvolumegroup_cmd)
        if validate:
            out, rc = client.exec_command(
                sudo=True, cmd=f"ceph fs subvolumegroup ls {vol_name} --format json"
            )
            subvolumegroup_ls = json.loads(out.read().decode())
            if group_name not in [i["name"] for i in subvolumegroup_ls]:
                raise CommandFailed(f"Creation of subvolume group: {group_name} failed")

    def create_subvolume(self, client, vol_name, subvol_name, validate=True, **kwargs):
        """

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
        Returns:

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
        client.exec_command(sudo=True, cmd=subvolume_cmd)
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

    def create_snapshot(
        self, client, vol_name, subvol_name, snap_name, validate=True, **kwargs
    ):
        """

        Args:
            client:
            vol_name:
            subvol_name:
            snap_name:
            validate:
            **kwargs:
                group_name : str
        Returns:

        """
        snapshot_cmd = (
            f"ceph fs subvolume snapshot create {vol_name} {subvol_name} {snap_name}"
        )
        if kwargs.get("group_name"):
            snapshot_cmd += f" --group_name {kwargs.get('group_name')}"
        client.exec_command(sudo=True, cmd=snapshot_cmd)
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

        Returns:

        """
        clone_cmd = f"ceph fs subvolume snapshot clone {vol_name} {subvol_name} {snap_name} {target_subvol_name}"
        if kwargs.get("group_name"):
            clone_cmd += f" --group_name {kwargs.get('group_name')}"
        if kwargs.get("target_group_name"):
            clone_cmd += f" --target_group_name {kwargs.get('target_group_name')}"
        if kwargs.get("pool_layout"):
            clone_cmd += f" --pool_layout {kwargs.get('pool_layout')}"
        client.exec_command(sudo=True, cmd=clone_cmd)
        if validate:
            listsubvolumes_cmd = f"ceph fs subvolume ls {vol_name}"
            if kwargs.get("target_group_name"):
                listsubvolumes_cmd += (
                    f" --target_group_name {kwargs.get('target_group_name')}"
                )
            out, rc = client.exec_command(
                sudo=True, cmd=f"{listsubvolumes_cmd} --format json"
            )
            subvolume_ls = json.loads(out.read().decode())
            if target_subvol_name not in [i["name"] for i in subvolume_ls]:
                raise CommandFailed(f"Creation of clone : {target_subvol_name} failed")

    def remove_snapshot(
        self, client, vol_name, subvol_name, snap_name, validate=True, **kwargs
    ):
        """

        Args:
            client:
            vol_name:
            subvol_name:
            snap_name:
            validate:
            **kwargs:
                group_name : str
                force : boolean

        Returns:

        """
        rmsnapshot_cmd = (
            f"ceph fs subvolume snapshot rm {vol_name} {subvol_name} {snap_name}"
        )
        if kwargs.get("group_name"):
            rmsnapshot_cmd += f" --group_name {kwargs.get('group_name')}"
        if kwargs.get("force"):
            rmsnapshot_cmd += " --force"
        client.exec_command(sudo=True, cmd=rmsnapshot_cmd)
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

    def remove_subvolume(self, client, vol_name, subvol_name, validate=True, **kwargs):
        """

        Args:
            client:
            vol_name:
            subvol_name:
            validate:
            **kwargs:
                group_name : str
                retain-snapshots : boolean
                force : boolean

        Returns:

        """
        rmsubvolume_cmd = f"ceph fs subvolume rm {vol_name} {subvol_name}"
        if kwargs.get("group_name"):
            rmsubvolume_cmd += f" --group_name {kwargs.get('group_name')}"
        if kwargs.get("retain-snapshots"):
            rmsubvolume_cmd += " --retain-snapshots"
        if kwargs.get("force"):
            rmsubvolume_cmd += " --force"
        client.exec_command(sudo=True, cmd=rmsubvolume_cmd)
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

    def remove_subvolumegroup(
        self, client, vol_name, group_name, validate=True, **kwargs
    ):
        """

        Args:
            client:
            vol_name:
            group_name:
            validate:
            **kwargs:
                --force

        Returns:

        """
        rmsubvolumegroup_cmd = f"ceph fs subvolumegroup rm {vol_name} {group_name}"
        if kwargs.get("force"):
            rmsubvolumegroup_cmd += " --force"
        client.exec_command(sudo=True, cmd=rmsubvolumegroup_cmd)
        if validate:
            out, rc = client.exec_command(
                sudo=True, cmd=f"ceph fs subvolumegroup ls {vol_name} --format json"
            )
            subvolumegroup_ls = json.loads(out.read().decode())
            if group_name in [i["name"] for i in subvolumegroup_ls]:
                raise CommandFailed(f"Deletion of subvolume group: {group_name} failed")

    def remove_fs(self, client, vol_name, validate=True, **kwargs):
        """

        Args:
            client:
            vol_name:
            validate:
            **kwargs:

        Returns:

        """
        rmvolume_cmd = f"ceph fs volume rm {vol_name} --yes-i-really-mean-it"
        client.exec_command(sudo=True, cmd=rmvolume_cmd)
        if validate:
            out, rc = client.exec_command(sudo=True, cmd="ceph fs ls --format json")
            volname_ls = json.loads(out.read().decode())
            if vol_name in [i["name"] for i in volname_ls]:
                raise CommandFailed(f"Creation of filesystem: {vol_name} failed")

