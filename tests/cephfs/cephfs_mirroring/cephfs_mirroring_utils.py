"""
This is cephfs mirroring  utility module
It contains all the re-useable functions related to cephfs mirroring feature

"""
import json

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


class CephfsMirroringUtils(object):
    def __init__(self, source_ceph_cluster, target_ceph_cluster):
        """
        CephFS Mirroring Utility object
        Args:
            ceph_cluster (ceph.ceph.Ceph): ceph cluster
        """

        self.source_mons = source_ceph_cluster.get_ceph_objects("mon")
        self.target_mons = target_ceph_cluster.get_ceph_objects("mon")
        self.source_mgrs = source_ceph_cluster.get_ceph_objects("mgr")
        self.target_mgrs = target_ceph_cluster.get_ceph_objects("mgr")
        self.source_mdss = source_ceph_cluster.get_ceph_objects("mds")
        self.target_mdss = target_ceph_cluster.get_ceph_objects("mds")
        self.source_osds = source_ceph_cluster.get_ceph_objects("osd")
        self.target_osds = target_ceph_cluster.get_ceph_objects("osd")
        self.source_clients = source_ceph_cluster.get_ceph_objects("client")
        self.target_clients = target_ceph_cluster.get_ceph_objects("client")
        self.source_mirrors = source_ceph_cluster.get_ceph_objects("cephfs-mirror")
        self.fs_util_ceph1 = FsUtils(source_ceph_cluster)
        self.fs_util_ceph2 = FsUtils(target_ceph_cluster)

    def enable_mirroring_module(self, client):
        """
        Enable the mirroring mgr module on the specified Ceph client.
        Args:
            client (CephNode): The Ceph client where the mirroring module should be enabled.
        Returns:
            None
        Raises:
            json.JSONDecodeError: If there is an issue parsing the JSON response.
            Exception: If an unexpected error occurs during the process.

        The function first checks if the mirroring module is already enabled on the client.
        If not, it enables the mirroring mgr module and verifies that it has been enabled.
        """
        log.info("Verifying mirroring mgr module status")
        module_status, _ = client.exec_command(
            sudo=True, cmd="ceph mgr module ls --format json-pretty"
        )
        try:
            module_status = json.loads(module_status)
            if "mirroring" in module_status.get("enabled_modules", []):
                log.info("Mirroring mgr module is already enabled.")
            else:
                log.info("Mirroring mgr module is not enabled.")
                log.info("Enabling mirroring mgr module")
                client.exec_command(sudo=True, cmd="ceph mgr module enable mirroring")
                module_status, _ = client.exec_command(
                    sudo=True, cmd="ceph mgr module ls --format json-pretty"
                )
                module_status = json.loads(module_status)
                if "mirroring" in module_status.get("enabled_modules", []):
                    log.info("Mirroring mgr module is enabled.")
                else:
                    log.error("Failed to enable mirroring mgr module.")
                    return 1
            return 0
        except json.JSONDecodeError:
            log.error("Failed to parse module status as JSON.")
            return 1
        except Exception as ex:
            log.error(f"Error: {ex}")
            return 1

    def disable_mirroring_module(self, client):
        """
        Disable the mirroring mgr module on the specified Ceph client.
        Args:
            client (CephNode): The Ceph client where the mirroring module should be disabled.
        Returns:
            None
        Raises:
            json.JSONDecodeError: If there is an issue parsing the JSON response.
            Exception: If an unexpected error occurs during the process.

        The function first checks if the mirroring module is already disabled on the client.
        If not, it disables the mirroring mgr module and verifies that it has been disabled.
        """
        log.info("Verifying mirroring mgr module status")
        module_status, _ = client.exec_command(
            sudo=True, cmd="ceph mgr module ls --format json-pretty"
        )
        try:
            module_status = json.loads(module_status)
            if "mirroring" not in module_status.get("enabled_modules", []):
                log.info("Mirroring mgr module is already disabled.")
            else:
                log.info("Disable mirroring mgr module")
                client.exec_command(sudo=True, cmd="ceph mgr module disable mirroring")
                module_status, _ = client.exec_command(
                    sudo=True, cmd="ceph mgr module ls --format json-pretty"
                )
                module_status = json.loads(module_status)
                if "mirroring" not in module_status.get("enabled_modules", []):
                    log.info("Disabled mirroring mgr module successfully")
                else:
                    log.error("Failed to disable mirroring mgr module")
                    return 1
            return 0
        except json.JSONDecodeError:
            log.error("Failed to parse module status as JSON.")
            return 1
        except Exception as ex:
            log.error(f"Error: {ex}")
            return 1

    def create_authorize_user(
        self, target_fs, target_user, target_client, permissions="rwps"
    ):
        """
        Create a user and authorize it for the specified Ceph filesystem.
        Args:
            target_fs (str): The name of the Ceph filesystem.
            target_user (str): The name of the user to be created and authorized.
            target_client (CephNode): The Ceph client used to perform the authorization.
            permissions (str): The permissions to be granted to the user. Default is "rwps".
        Returns:
            None
        Raises:
            json.JSONDecodeError: If there is an issue parsing the JSON response.
            Exception: If an unexpected error occurs during the process.

        This function creates a user and authorizes it with the specified permissions for the given filesystem.
        It then validates the authorization to ensure the expected capabilities match the authorized capabilities.
        """
        log.info(
            f"Create a user and authorize it for the filesystem with permissions: {permissions}"
        )
        auth_command = (
            f"ceph fs authorize {target_fs} client.{target_user} / {permissions}"
        )
        target_client.exec_command(sudo=True, cmd=auth_command)

        log.info("Validate the authorization")
        auth_info, _ = target_client.exec_command(
            sudo=True,
            cmd=f"ceph auth get client.{target_user} " f"--format json-pretty",
        )
        auth_data = json.loads(auth_info)
        expected_caps = {
            "mds": f"allow {permissions} fsname={target_fs}",
            "mon": f"allow r fsname={target_fs}",
            "osd": f"allow rw tag cephfs data={target_fs}",
        }
        if len(auth_data) > 0 and auth_data[0]["caps"] == expected_caps:
            log.info(f"Authorization for client.{target_user} is validated.")
        else:
            log.error(f"Authorization for client.{target_user} is not as expected.")
            return 1
        return 0

    def remove_user_used_for_peer_connection(self, target_user, target_client):
        log.info(f"Delete the user {target_user} used for creating peer bootstrap")
        auth_remove_command = f"ceph auth rm client.{target_user}"
        target_client.exec_command(sudo=True, cmd=auth_remove_command)

    def enable_snapshot_mirroring(self, source_fs_name, source_client):
        """
        Enable snapshot mirroring for a specific Ceph filesystem.
        Args:
            source_fs_name (str): The name of the source Ceph filesystem to enable snapshot mirroring.
            source_client (CephNode): The Ceph client used to perform the enabling.
        Returns:
            None

        This function enables snapshot mirroring for the specified Ceph filesystem.
        """
        log.info(f"Enabling snapshot mirroring on {source_fs_name}")
        command = f"ceph fs snapshot mirror enable {source_fs_name}"
        source_client.exec_command(sudo=True, cmd=command)

    def disable_snapshot_mirroring(self, source_fs_name, source_client):
        """
        Disable snapshot mirroring for a specific Ceph filesystem.
        Args:
            source_fs_name (str): The name of the source Ceph filesystem to disable snapshot mirroring.
            source_client (CephNode): The Ceph client used to perform the disabling.
        Returns:
            None

        This function disables snapshot mirroring for the specified Ceph filesystem. It stops mirroring snapshots of
        this filesystem to a remote location
        """
        log.info(f"Disabling snapshot mirroring on {source_fs_name}")
        command = f"ceph fs snapshot mirror disable {source_fs_name}"
        source_client.exec_command(sudo=True, cmd=command)

    def create_peer_bootstrap(
        self, target_fs_name, target_user, target_site_name, target_client
    ):
        """
        Create a peer bootstrap token for setting up mirroring with a remote Ceph filesystem.
        Args:
            target_fs_name (str): The name of the target Ceph filesystem to mirror to.
            target_user (str): The name of the user authorized to mirror snapshots.
            target_site_name (str): The name of the remote site.
            target_client (CephNode): The Ceph client used to create the bootstrap token.
        Returns:
            str: The peer bootstrap token for setting up mirroring.

        This function generates a peer bootstrap token that allows the source Ceph filesystem to establish mirroring
        with a remote target Ceph filesystem. It is used to securely set up the mirroring connection.
        """
        log.info("Creating the peer bootstrap")
        command = (
            f"ceph fs snapshot mirror peer_bootstrap create "
            f"{target_fs_name} client.{target_user} {target_site_name} -f json"
        )
        bootstrap_key, _ = target_client.exec_command(sudo=True, cmd=command)
        token = json.loads(bootstrap_key).get("token")
        log.info(f"Bootstrap token value is : {token}")
        return token

    def import_peer_bootstrap(self, source_fs, token, source_client):
        """
        Import a peer bootstrap token for mirroring setup from a remote Ceph filesystem.
        Args:
            source_fs (str): The name of the source Ceph filesystem where the mirroring is being set up.
            token (str): The peer bootstrap token generated on the remote target Ceph filesystem.
            source_client (CephNode): The Ceph client used to import the peer bootstrap token.

        This function imports a peer bootstrap token that was generated on a remote target Ceph filesystem. The token
        allows the source Ceph filesystem to establish mirroring with the target filesystem. Successful token import
        sets up mirroring between the source and target filesystems.
        """
        log.info("Import the bootstrap on source")
        command = f"ceph fs snapshot mirror peer_bootstrap import {source_fs} {token}"
        source_client.exec_command(sudo=True, cmd=command)
        log.info("Peer bootstrap token import completed successfully on the source.")

    def validate_peer_connection(
        self, source_clients, source_fs, target_site_name, target_user, target_fs_name
    ):
        """
        Validate the peer connection information for snapshot mirroring setup.
        Args:
            source_clients (CephNode): The source Ceph clients responsible for mirroring setup.
            source_fs (str): The name of the source Ceph filesystem where the mirroring is configured.
            target_site_name (str): The name of the target site for mirroring.
            target_user (str): The user authorized for peer connection on the target Ceph filesystem.
            target_fs_name (str): The name of the target Ceph filesystem.

        This function fetches peer connection information from the source system and validates it against the expected
        target parameters, including site name, user, filesystem name, and monitor hosts. A successful validation
        indicates that the peer connection for mirroring is established correctly.

        Raises:
            Exception: If an error occurs during the validation process.
        """
        log.info("Get Peer Connection Information")
        log.info("Fetch peer connection information from the source system")
        out, _ = source_clients.exec_command(
            f"ceph fs snapshot mirror peer_list {source_fs} --format json-pretty"
        )
        try:
            data = json.loads(out)
            log.info(f"Snapshot Mirror peer list details : {data}")
            id = next(iter(data))
            site_name = data[id]["site_name"]
            client_name = data[id]["client_name"]
            fs_name = data[id]["fs_name"]
            mon_hosts = data[id]["mon_host"]
            mon_host_list = list(
                set(host.split(":")[1] for host in mon_hosts.split(","))
            )
            log.info(f"Mon Hosts from the Output : {mon_host_list}")
            target_mon_node_ip_list = self.fs_util_ceph2.get_mon_node_ips()
            log.info(f"Target Mon Hosts : {target_mon_node_ip_list}")

            target_site_name = target_site_name
            target_user = f"client.{target_user}"
            target_fs_name = target_fs_name
            # target_mon_hosts = target_mon_node_ip_list

            if (
                site_name == target_site_name
                and client_name == target_user
                and fs_name == target_fs_name
                # and set(mon_host_list) == set(target_mon_hosts)
                # BZ : https://bugzilla.redhat.com/show_bug.cgi?id=2248176 -
                # Will remove the check once the issue is fixed
            ):
                log.info(
                    "Peer Connection validated and mirroring is successfully established."
                )
            else:
                log.error(
                    "Peer Connection validation failed. Mirroring may not be established."
                )
                return 1
            return 0
        except Exception as e:
            log.error(f"Error validating Peer Connection: {e}")
            return 1

    def add_path_for_mirroring(self, source_clients, source_fs, path):
        """
        Add a path for mirroring on the source filesystem.
        Args:
            source_clients (CephNode): The source Ceph clients responsible for mirroring setup.
            source_fs (str): The name of the source Ceph filesystem where mirroring is configured.
            path (str): The path to be added for mirroring within the source filesystem.

        This function adds the specified path to the mirroring configuration of the source filesystem. This path
        will be mirrored to the target filesystem for snapshot synchronization.

        Note:
            The specified path should exist within the source filesystem.
        """
        source_clients.exec_command(
            sudo=True, cmd=f"ceph fs snapshot mirror add {source_fs} {path}"
        )

    def remove_path_from_mirroring(self, source_clients, source_fs, path):
        """
        Remove a path from mirroring on the source filesystem.
        Args:
            source_clients (CephNode): The source Ceph clients responsible for mirroring setup.
            source_fs (str): The name of the source Ceph filesystem where mirroring is configured.
            path (str): The path to be removed from mirroring within the source filesystem.

        This function removes the specified path from the mirroring configuration of the source filesystem. The path
        will no longer be mirrored to the target filesystem.

        Note:
            The specified path should exist within the source filesystem and have been previously added for mirroring.
        """
        source_clients.exec_command(
            sudo=True, cmd=f"ceph fs snapshot mirror remove {source_fs} {path}"
        )

    def get_fsid(self, cephfs_mirror_node):
        """
        Fetches the FSID (File System ID) of the Ceph cluster.
        Args:
            cephfs_mirror_node (CephNode): The CephFS mirror node used to execute the command.
        Returns:
            str: The FSID of the Ceph cluster.
        """
        log.info("Fetch the FSID of the ceph cluster")
        out, _ = cephfs_mirror_node.exec_command(sudo=True, cmd="cephadm ls")
        data = json.loads(out)
        fsid = data[0]["fsid"]
        return fsid

    def get_daemon_name(self, source_clients):
        """
        Fetches the name of the cephfs-mirror daemon.
        Args:
            source_clients (CephNode): The source Ceph clients responsible for mirroring setup.
        Returns:
            str: The name of the cephfs-mirror daemon.
        """
        log.info("Fetch the cephfs-mirror daemon name")
        out, _ = source_clients.exec_command(
            sudo=True, cmd="ceph orch ps --daemon_type cephfs-mirror -f json"
        )
        data = json.loads(out)
        daemon_name = data[0]["daemon_name"]
        return daemon_name

    def get_filesystem_id_by_name(self, source_clients, fs_name):
        """
        Fetches the filesystem ID of a specified Ceph filesystem.
        Args:
            source_clients (CephNode): The source Ceph clients responsible for mirroring setup.
            fs_name (str): The name of the Ceph filesystem.
        Returns:
            str or None: The filesystem ID if found, or None if the filesystem with the specified name is not found.
        """
        log.info("Fetch the filesystem id of a filesystem")
        out, _ = source_clients.exec_command(
            sudo=True, cmd="ceph fs snapshot mirror daemon status -f json"
        )
        data = json.loads(out)
        filesystem_id = None
        for filesystem in data[0]["filesystems"]:
            if filesystem["name"] == fs_name:
                filesystem_id = filesystem["filesystem_id"]
                break
        return filesystem_id

    def get_peer_uuid_by_name(self, source_clients, fs_name):
        """
        Fetches the peer UUID of a specified Ceph filesystem.
        Args:
            source_clients (CephNode): The source Ceph clients responsible for mirroring setup.
            fs_name (str): The name of the Ceph filesystem.
        Returns:
            str or None: The peer UUID if found, or None if the filesystem with the specified name is not found.
        """
        log.info("Fetch the peer_uuid of a filesystem")
        out, _ = source_clients.exec_command(
            sudo=True, cmd="ceph fs snapshot mirror daemon status -f json"
        )
        data = json.loads(out)
        peer_uuid = None
        for filesystem in data[0]["filesystems"]:
            if filesystem["name"] == fs_name:
                peers = filesystem["peers"]
                for peer in peers:
                    peer_uuid = peer["uuid"]
                    break
        return peer_uuid

    def get_asok_file(self, cephfs_mirror_node, fsid, daemon_name):
        """
        Fetches the asok file of the cephfs-mirror daemon.
        Args:
            cephfs_mirror_node (CephNode): The CephFS mirror node used to execute the command.
            fsid (str): The FSID (File System ID) of the Ceph cluster.
            daemon_name (str): The name of the cephfs-mirror daemon.
        Returns:
            str: The path to the asok file of the cephfs-mirror daemon.
        """
        log.info("Fetch the asok file of the cephfs-mirror daemon.")
        cmd = f"cd /var/run/ceph/{fsid}/ ; ls -1tr ceph-client.{daemon_name}* | head -n 1 | tr -d '\\n'"
        file = cephfs_mirror_node.exec_command(sudo=True, cmd=cmd)
        asok_file = file[0].replace("\\n", "")
        return asok_file

    @retry(CommandFailed, tries=5, delay=30)
    def validate_synchronization(
        self, cephfs_mirror_node, source_clients, fs_name, snap_count
    ):
        """
        Validates the synchronization status of snapshots on the target cluster.
        Args:
            cephfs_mirror_node (CephNode): The CephFS mirror node to perform the validation.
            source_clients (CephNode): The source Ceph clients responsible for mirroring setup.
            fs_name (str): The name of the Ceph filesystem being synchronized.
            snap_count (int): The expected number of snapshots to be synchronized.
        Raises:
            CommandFailed: If an error occurs during synchronization validation or if the snapshot count doesn't match.
        """
        log.info("Validate the Synchronisation on Target Cluster")
        log.info("Install ceph-common on cephfs-mirror node")
        cephfs_mirror_node.exec_command(
            sudo=True, cmd="yum install -y ceph-common --nogpgcheck"
        )
        fsid = self.get_fsid(cephfs_mirror_node)
        daemon_name = self.get_daemon_name(source_clients)
        filesystem_id = self.get_filesystem_id_by_name(source_clients, fs_name)
        asok_file = self.get_asok_file(cephfs_mirror_node, fsid, daemon_name)
        log.info("Get filesystem mirror status")
        out, _ = cephfs_mirror_node.exec_command(
            sudo=True,
            cmd=f"cd /var/run/ceph/{fsid}/ ; ceph --admin-daemon {asok_file} "
            f"fs mirror status {fs_name}@{filesystem_id} -f json",
        )
        data = json.loads(out)
        if not data.get("snap_dirs"):
            raise CommandFailed("Unable to get the Snap Dir")
        snap_dir_count = data.get("snap_dirs").get("dir_count")
        log.info(f"dir count {snap_dir_count}")
        if snap_dir_count == snap_count:
            log.info(f"Snap Directory Count : {snap_dir_count}")
            log.info("Synchronization is successful")
        else:
            log.error("Snap Directory Count is not matching, Synchronization failed")
            raise CommandFailed(
                "Snap Directory Count is not matching, Synchronization failed"
            )

    @retry(CommandFailed, tries=5, delay=30)
    def validate_snapshot_sync_status(
        self,
        cephfs_mirror_node,
        fs_name,
        snapshot_name,
        fsid,
        asok_file,
        filesystem_id,
        peer_uuid,
    ):
        """
        Validate the synchronization status of a specific snapshot in the target cluster.
        Args:
            cephfs_mirror_node (CephNode): The CephNode representing the CephFS mirror node.
            fs_name (str): The name of the Ceph filesystem being synchronized.
            snapshot_name (str): The name of the snapshot to be validated.
            fsid (str): The unique FSID of the Ceph cluster.
            asok_file (str): The admin socket file for the CephFS mirror.
            filesystem_id (str): The ID of the filesystem being synchronized.
            peer_uuid (str): The UUID of the peer cluster.

        This function validates the synchronization status of a specific snapshot by checking the
        last synchronized snapshot and its details in the target cluster.

        Returns:
            dict: A dictionary with details of the synchronized snapshot, including snapshot name, sync duration,
            sync timestamp, and the number of snaps synced.
        Note:
            If the specified snapshot is not found or not synchronized, the function returns None.
        Raises:
            json.JSONDecodeError: If there's an error decoding JSON from the Ceph mirror status response.
        """
        log.info("Get peer mirror status")
        log.info("Validate the snapshot sync to target cluster.")
        cmd = (
            f"cd /var/run/ceph/{fsid}/ ; ceph --admin-daemon {asok_file} fs mirror peer status "
            f"{fs_name}@{filesystem_id} {peer_uuid} -f json"
        )
        out, _ = cephfs_mirror_node.exec_command(sudo=True, cmd=cmd)
        data = json.loads(out)
        for path, status in data.items():
            last_synced_snap = status.get("last_synced_snap")
            if last_synced_snap:
                if last_synced_snap.get("name") == snapshot_name:
                    sync_duration = last_synced_snap.get("sync_duration")
                    sync_time_stamp = last_synced_snap.get("sync_time_stamp")
                    snaps_synced = status.get("snaps_synced")
                    return {
                        "snapshot_name": snapshot_name,
                        "sync_duration": sync_duration,
                        "sync_time_stamp": sync_time_stamp,
                        "snaps_synced": snaps_synced,
                    }
                    log.info("ALl snapshots are synced")
        else:
            log.error(f"{snapshot_name} not synced, last synced data is {data}")
            raise CommandFailed("One or more snapshots are not synced")

    def remove_snapshot_mirror_peer(self, source_clients, fs_name, peer_uuid):
        """
        Remove a peer connection from snapshot mirroring and verify its removal.
        Args:
            source_clients (CephNode): The CephNode representing the source client where the
                                        operation is performed.
            fs_name (str): The name of the Ceph filesystem.
            peer_uuid (str): The UUID of the peer connection to be removed.

        This function removes a peer connection from snapshot mirroring for a specific Ceph filesystem
        and verifies its removal by checking the list of peers for the filesystem.

        Returns:
            bool: True if the peer is successfully removed, False otherwise.

        Note:
            The function sends a command to remove the peer connection from snapshot mirroring,
            then retrieves the list of peers for the specified filesystem. If the peer UUID is not
            found in the list of peers, the function returns True, indicating successful removal.
            If the peer is not removed or if an unexpected error occurs, it returns False.
        """
        try:
            cmd_remove = f"ceph fs snapshot mirror peer_remove {fs_name} {peer_uuid}"
            source_clients.exec_command(sudo=True, cmd=cmd_remove)

            cmd_list = f"ceph fs snapshot mirror peer_list {fs_name} -f json"
            out, _ = source_clients.exec_command(sudo=True, cmd=cmd_list)
            peer_list = json.loads(out)
            if peer_uuid not in peer_list:
                log.info(
                    f"Peer '{peer_uuid}' has been successfully removed for filesystem '{fs_name}'"
                )
                return True
            else:
                log.error(
                    f"Error: Peer '{peer_uuid}' still exists for filesystem '{fs_name}'"
                )
                return False
        except Exception as ex:
            log.error(f"Error: An unexpected error occurred: {ex}")
            return False

    def list_and_verify_remote_snapshots_and_data(
        self,
        target_clients,
        target_mount_path,
        target_client_user,
        source_path,
        snapshot_name,
        expected_files,
        target_fs_name,
    ):
        """
        List and verify remote snapshots and data in a target Ceph cluster.
        Args:
            target_clients: Ceph client on the target cluster.
            target_mount_path (str): The mount path on the target cluster.
            target_client_user (str): The user for the target client.
            source_path (str): The source path where snapshots and data are located.
            snapshot_name (str): The name of the snapshot to verify.
            expected_files (list): List of expected files in the snapshot.
        Returns:
            tuple: A tuple containing a boolean indicating success or failure, and a message.
        """
        try:
            target_clients.exec_command(sudo=True, cmd=f"mkdir -p {target_mount_path}")
            target_clients.exec_command(
                sudo=True,
                cmd=f"ceph-fuse -n {target_client_user} {target_mount_path} --client_fs {target_fs_name}",
            )
            snapshot_list_command = f"ls {target_mount_path}{source_path}.snap"
            snapshots, _ = target_clients.exec_command(
                sudo=True, cmd=snapshot_list_command
            )
            snapshots = snapshots.strip().split()
            log.info(f"Available Snapshots : {snapshots}")
            if snapshot_name in snapshots:
                snapshot_path = f"{target_mount_path}{source_path}.snap/{snapshot_name}"
                snapshot_files_command = f"ls {snapshot_path}"
                snapshot_files, _ = target_clients.exec_command(
                    sudo=True, cmd=snapshot_files_command
                )
                snapshot_files = snapshot_files.strip().split()

                if expected_files in snapshot_files:
                    return (
                        True,
                        f"Snapshot '{snapshot_name}' and '{expected_files}' file found in path: '{snapshot_path}'",
                    )
                else:
                    return (
                        False,
                        f"Snapshot '{snapshot_name}' found, but '{expected_files}' file is missing.",
                    )
            else:
                return (
                    False,
                    f"Snapshot '{snapshot_name}' not found in path: '{target_mount_path}/{source_path}'",
                )
        except Exception as e:
            return False, f"Error: {str(e)}"

    def cleanup_target_client(self, target_clients, target_mount_path):
        """
        This function cleans up the target client by unmounting and removing a specified path.
        Args:
            target_clients: A list of target clients to perform the cleanup.
            target_mount_path: The path to unmount and remove on the target client.
        Returns:
            None
        """
        try:
            log.info("Cleanup target client")
            log.info(f"Unmount: {target_mount_path}")
            target_clients.exec_command(
                sudo=True, cmd=f"umount -l {target_mount_path}", check_ec=False
            )
            log.info(f"Remove {target_mount_path}")
            target_clients.exec_command(
                sudo=True, cmd=f"rm -rf {target_mount_path}", check_ec=False
            )
        except Exception as e:
            log.error(f"Error during cleanup: {str(e)}")
