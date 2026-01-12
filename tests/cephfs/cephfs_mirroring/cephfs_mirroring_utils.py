"""
This is cephfs mirroring  utility module
It contains all the re-useable functions related to cephfs mirroring feature

"""

import csv
import json
import random
import secrets
import string

from looseversion import LooseVersion

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry
from utility.utils import get_ceph_version_from_cluster

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

            target_site_name = target_site_name
            target_user = f"client.{target_user}"
            target_fs_name = target_fs_name

            if (
                site_name == target_site_name
                and client_name == target_user
                and fs_name == target_fs_name
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
            ceph_version (str): Ceph version string (e.g., "7.1.", "7.2").
                            Validation is skipped for Ceph < 7.0.0.
        This function adds the specified path to the mirroring configuration of the source filesystem.
        For Ceph builds >= 7.0.0, validates the configuration by listing mirrored paths.        Note:
        The specified path should exist within the source filesystem.
        """
        path = path.rstrip("/")  # to avoid trailing slash mismatch
        ceph_version = get_ceph_version_from_cluster(source_clients)

        log.info(
            f"Adding path '{path}' to mirroring configuration for filesystem '{source_fs}'..."
        )
        source_clients.exec_command(
            sudo=True, cmd=f"ceph fs snapshot mirror add {source_fs} {path}"
        )

        if LooseVersion(ceph_version) < LooseVersion("19.2"):
            log.info(
                "Skipping mirror path validation as ceph_version '%s' < 19.2.0 and it's not supported",
                ceph_version,
            )
            return
        else:
            out, _ = source_clients.exec_command(
                sudo=True, cmd=f"ceph fs snapshot mirror ls {source_fs}"
            )
            try:
                mirrored_paths = [p.rstrip("/") for p in json.loads(out.strip())]
                log.debug(f"Mirrored paths returned: {mirrored_paths}")
            except json.JSONDecodeError:
                log.error(f"Failed to parse mirror ls output: {out}")
                raise
            if path in mirrored_paths:
                log.info(
                    f"Successfully added path '{path}' for mirroring on filesystem '{source_fs}'."
                )
            else:
                log.error(
                    f"Path '{path}' was not found in mirror list for filesystem '{source_fs}'."
                )
                raise Exception(f"Mirroring path addition failed: {path}")

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
        log.info(fsid)
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
        daemon_names = [daemon_info["daemon_name"] for daemon_info in data]
        log.info(daemon_names)
        return daemon_names

    @retry(CommandFailed, tries=5, delay=30)
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
        log.info(filesystem_id)
        return filesystem_id

    @retry(CommandFailed, tries=5, delay=60)
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
        log.info(peer_uuid)
        if not peer_uuid:
            raise CommandFailed("Peer uuid not created yet")
        return peer_uuid

    def get_asok_file(self, cephfs_mirror_node, fsid, daemon_names):
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
        asok_files = {}
        if not isinstance(daemon_names, list):
            daemon_names = [daemon_names]
        for daemon_name in daemon_names:
            cmd = f"cd /var/run/ceph/{fsid}/ ; ls -1tr ceph-client.{daemon_name}* | head -n 1 | tr -d '\\n'"
            if isinstance(cephfs_mirror_node, list):
                for node in cephfs_mirror_node:
                    file = node.exec_command(sudo=True, cmd=cmd)
                    asok_file = file[0].replace("\\n", "")
                    if asok_file:
                        asok_files[node.node.hostname] = [node, asok_file]
                    log.info(asok_file)
            else:
                file = cephfs_mirror_node.exec_command(sudo=True, cmd=cmd)
                asok_file = file[0].replace("\\n", "")
                asok_files[cephfs_mirror_node.node.hostname] = [
                    cephfs_mirror_node,
                    asok_file,
                ]
                log.info(asok_file)
        return asok_files

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
        if not isinstance(cephfs_mirror_node, list):
            cephfs_mirror_node = [cephfs_mirror_node]
        for node in cephfs_mirror_node:
            node.exec_command(sudo=True, cmd="dnf install -y ceph-common --nogpgcheck")
        fsid = self.get_fsid(cephfs_mirror_node[0])
        daemon_names = self.get_daemon_name(source_clients)
        filesystem_id = self.get_filesystem_id_by_name(source_clients, fs_name)
        asok_files = self.get_asok_file(cephfs_mirror_node, fsid, daemon_names)
        log.info("Get filesystem mirror status")
        for node, asok_file in asok_files.items():
            out, _ = asok_file[0].exec_command(
                sudo=True,
                cmd=f"cd /var/run/ceph/{fsid}/ ; ceph --admin-daemon {asok_file[1]} "
                f"fs mirror status {fs_name}@{filesystem_id} -f json",
            )
            data = json.loads(out)
            if data.get("snap_dirs"):
                break
        else:
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
    def get_synced_dir_count(
        self,
        cephfs_mirror_node,
        source_clients,
        fs_name,
        daemon_name,
    ):
        """
        Gets the count of synced directories from the CephFS mirror status.

        Args:
        - cephfs_mirror_node: The node where the CephFS mirror is hosted
        - source_clients: Source clients information
        - fs_name: The name of the file system
        - daemon_name: The name of the daemon

        Returns:
        - int: The count of synced directories

        Raises:
        - CommandFailed: If the directory count is not found or synchronization fails
        """
        if not isinstance(cephfs_mirror_node, list):
            cephfs_mirror_node = [cephfs_mirror_node]
        fsid = self.get_fsid(cephfs_mirror_node[0])
        filesystem_id = self.get_filesystem_id_by_name(source_clients, fs_name)
        asok_files = self.get_asok_file(cephfs_mirror_node, fsid, daemon_name)
        log.info("Get the details of synced dir from cephfs-mirror status")
        log.info("Install ceph-common on cephfs-mirror node")
        for node in cephfs_mirror_node:
            node.exec_command(sudo=True, cmd="yum install -y ceph-common --nogpgcheck")
        log.info("Get filesystem mirror status")
        for node, asok_file in asok_files.items():
            out, _ = asok_file[0].exec_command(
                sudo=True,
                cmd=f"cd /var/run/ceph/{fsid}/ ; ceph --admin-daemon {asok_file[1]} "
                f"fs mirror status {fs_name}@{filesystem_id} -f json",
            )
            data = json.loads(out)
            if data.get("snap_dirs"):
                break
        else:
            raise CommandFailed("Unable to get the Snap Dir")
        dir_count = data.get("snap_dirs", {}).get("dir_count")
        if not data.get("snap_dirs"):
            raise CommandFailed("Unable to get the Snap Dir")
        if dir_count:
            log.info(f"Snap Directory Count : {dir_count}")
        else:
            log.error(
                "Snap Directory Count is not matching, Synchronization of snapshots failed"
            )
            raise CommandFailed("Snap Directory Count is not matching.")
        log.info(dir_count)
        return dir_count

    @retry(CommandFailed, tries=5, delay=30)
    def extract_synced_snapshots(
        self,
        cephfs_mirror_node,
        fs_name,
        fsid,
        asok_file,
        filesystem_id,
        peer_uuid,
    ):
        """
        Extracts the name of the last synced snapshot from the CephFS mirror status.

        Args:
        - cephfs_mirror_node: The node where the CephFS mirror is hosted
        - fs_name: The name of the file system
        - fsid: The FSID of the file system
        - asok_file: The ASOK file for administrative commands
        - filesystem_id: The ID of the filesystem
        - peer_uuid: The UUID of the peer cluster

        Returns:
        - str: The name of the last synced snapshot, if found

        Raises:
        - CommandFailed: If the snapshot is not found or not synced after multiple attempts
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
                return last_synced_snap.get("name")

        log.error("last synced Snapshot not found or not synced")
        raise CommandFailed("last synced Snapshot not found or not synced")

    @retry(CommandFailed, tries=10, delay=60)
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
        daemon_names = self.get_daemon_name(self.source_clients[0])

        asok_files = self.get_asok_file(cephfs_mirror_node, fsid, daemon_names)
        log.info(f"Admin Socket file of cephfs-mirror daemon : {asok_files}")
        filesystem_id = self.get_filesystem_id_by_name(self.source_clients[0], fs_name)
        log.info(f"filesystem id of {fs_name} is : {filesystem_id}")
        peer_uuid = self.get_peer_uuid_by_name(self.source_clients[0], fs_name)
        log.info(f"peer uuid of {fs_name} is : {peer_uuid}")
        for node, asok in asok_file.items():
            asok[0].exec_command(
                sudo=True, cmd="dnf install -y ceph-common --nogpgcheck"
            )
            cmd = (
                f"cd /var/run/ceph/{fsid}/ ; ceph --admin-daemon {asok[1]} fs mirror peer status "
                f"{fs_name}@{filesystem_id} {peer_uuid} -f json"
            )
            out, _ = asok[0].exec_command(sudo=True, cmd=cmd)
            data = json.loads(out)
            for path, status in data.items():
                last_synced_snap = status.get("last_synced_snap")
                if last_synced_snap:
                    if last_synced_snap.get("name") == snapshot_name:
                        sync_duration = last_synced_snap.get("sync_duration")
                        sync_time_stamp = last_synced_snap.get("sync_time_stamp")
                        snaps_synced = status.get("snaps_synced")
                        log.info("ALl snapshots are synced")
                        return {
                            "snapshot_name": snapshot_name,
                            "sync_duration": sync_duration,
                            "sync_time_stamp": sync_time_stamp,
                            "snaps_synced": snaps_synced,
                        }

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

    def list_and_verify_remote_snapshots_and_data_checksum(
        self,
        target_clients,
        target_client_user,
        source_path,
        source_client,
        source_mount_path,
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
            mounting_dir = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(10))
            )
            target_mount_path = f"/mnt/{mounting_dir}"
            target_clients.exec_command(sudo=True, cmd=f"mkdir -p {target_mount_path}")
            target_clients.exec_command(
                sudo=True,
                cmd=f"ceph-fuse -n {target_client_user} {target_mount_path} --client_fs {target_fs_name}",
            )
            snapshot_list_command = f"ls {target_mount_path}{source_path}.snap"
            retry_cmd = retry(CommandFailed, tries=3, delay=30)(
                target_clients.exec_command
            )
            snapshots, _ = retry_cmd(sudo=True, cmd=snapshot_list_command)
            snapshots = snapshots.strip().split()
            log.info(f"Available Snapshots : {snapshots}")

            out_target, rc = target_clients.exec_command(
                sudo=True,
                cmd=f"bash /root/md5sum_script.sh {target_mount_path}{source_path}",
            )
            log.info(f"Checksums of the files in target cluster : \n {out_target}")
            out_source, rc = source_client.exec_command(
                sudo=True,
                cmd=f"bash /root/md5sum_script.sh {source_mount_path}{source_path}",
            )
            log.info(f"Checksums of the files in source cluster : \n {out_source}")
            if out_target == out_source:
                log.info("Checksums are matching and all files synced")
            out_source_snap, rc = target_clients.exec_command(
                sudo=True,
                cmd=f"bash /root/md5sum_script.sh {target_mount_path}{source_path}/.snap",
            )
            log.info(
                f"Checksums of the files in source cluster snap dir : \n {out_source_snap}"
            )
            out_target_snap, rc = source_client.exec_command(
                sudo=True,
                cmd=f"bash /root/md5sum_script.sh {source_mount_path}{source_path}/.snap",
            )
            log.info(
                f"Checksums of the files in target cluster snap dir : \n {out_target_snap}"
            )
            if out_target_snap != out_source_snap:
                return False, "Checksums are not matching in snapshot folders"

            log.info("Checksums are matching and all files synced in snapshot folder")

            if out_target != out_source:
                return False, "Checksums are not matching in subvolume folders"
            log.info("Checksums are matching and all files synced in Subvolume folder")
            return True, "All Files and checksums synced Properly"
        except Exception as e:
            return False, f"Error: {str(e)}"
        finally:
            target_clients.exec_command(sudo=True, cmd=f"umount {target_mount_path}")
            target_clients.exec_command(sudo=True, cmd=f"rm -rf {target_mount_path}")

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

    def deploy_cephfs_mirroring(
        self,
        source_fs,
        source_client,
        target_fs,
        target_client,
        target_user,
        target_site_name,
    ):
        """
        Deploy CephFS mirroring setup between source and target filesystems.

        Args:
        - source_fs (str): Name of the source filesystem.
        - source_client: Client object for the source.
        - target_fs (str): Name of the target filesystem.
        - target_client: Client object for the target.
        - target_user (str): User to be authorized for the target filesystem.
        - target_site_name (str): Name of the target site.

        Raises:
        - Exception: If any step fails during the setup process.

        Returns :
            token(type: str) : Token Generated
        """
        try:
            log.info("Enable mirroring module on source")
            if self.enable_mirroring_module(source_client) != 0:
                raise Exception("Failed to enable mirroring module on Source.")

            log.info("Enable mirroring module on target")
            if self.enable_mirroring_module(target_client) != 0:
                raise Exception("Failed to enable mirroring module on Target.")

            log.info("Create and authorize user for the target filesystem")
            if self.create_authorize_user(target_fs, target_user, target_client) != 0:
                raise Exception(
                    "Failed to create/authorize user for the target filesystem."
                )

            log.info("Enable snapshot mirroring on the source filesystem")
            self.enable_snapshot_mirroring(source_fs, source_client)

            log.info("Create peer bootstrap token")
            token = self.create_peer_bootstrap(
                target_fs, target_user, target_site_name, target_client
            )

            log.info("Import peer bootstrap token on the source filesystem")
            self.import_peer_bootstrap(source_fs, token, source_client)

            log.info("Validate peer connection")
            if (
                self.validate_peer_connection(
                    source_client, source_fs, target_site_name, target_user, target_fs
                )
                != 0
            ):
                raise Exception("Peer connection validation failed.")

            log.info("CephFS mirroring setup deployed successfully.")
            log.info(token)
            return token
        except Exception as e:
            log.error(f"Error deploying CephFS mirroring setup: {e}")

    def destroy_cephfs_mirroring(
        self, source_fs, source_client, target_fs, target_client, target_user, peer_uuid
    ):
        """
        Destroy CephFS mirroring setup between source and target filesystems.

        Args:
        - source_fs (str): Name of the source filesystem.
        - source_client: Client object for the source.
        - target_fs (str): Name of the target filesystem.
        - target_client: Client object for the target.
        - target_user (str): User used for the target filesystem.
        - peer_uuid (str): UUID of the peer connection.

        Raises:
        - Exception: If any step fails during the teardown process.
        """
        try:
            log.info("Remove snapshot mirror peer")
            if not self.remove_snapshot_mirror_peer(
                source_client, source_fs, peer_uuid
            ):
                raise Exception(
                    f"Failed to remove peer '{peer_uuid}' for filesystem '{source_fs}'"
                )

            log.info("Disable snapshot mirroring on the source filesystem")
            self.disable_snapshot_mirroring(source_fs, source_client)

            log.info("Remove user used for peer connection")
            self.remove_user_used_for_peer_connection(target_user, target_client)

            log.info("Disable mirroring module on Source")
            if self.disable_mirroring_module(source_client) != 0:
                raise Exception("Failed to disable mirroring module on Source.")

            log.info("Disable mirroring module on Target")
            if self.disable_mirroring_module(target_client) != 0:
                raise Exception("Failed to disable mirroring module on Target.")

            log.info("CephFS mirroring setup destroyed successfully.")

        except Exception as e:
            log.error(f"Error destroying CephFS mirroring setup: {e}")

    def add_files_and_validate(
        self,
        source_clients,
        kernel_dir,
        kernel_subvol_path,
        fuse_dir,
        fuse_subvol_path,
        cephfs_mirror_node,
        source_fs,
        file_name_prefix,
        snap_count,
    ):
        log.info("Add files into the path and create snapshot on each path")

        snap1 = f"snap_k1_{file_name_prefix}"
        snap2 = f"snap_f1_{file_name_prefix}"
        file_name1 = f"hello_kernel_{file_name_prefix}"
        file_name2 = f"hello_fuse_{file_name_prefix}"
        random_data = "".join(
            random.choices(string.ascii_letters + string.digits, k=100)
        )

        source_clients[0].exec_command(
            sudo=True, cmd=f"touch {kernel_dir}{kernel_subvol_path}{file_name1}"
        )
        source_clients[0].exec_command(
            cmd=f"echo '{random_data}' | sudo tee {kernel_dir}{kernel_subvol_path}{file_name1} > /dev/null"
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"touch {fuse_dir}{fuse_subvol_path}{file_name2}"
        )
        source_clients[0].exec_command(
            cmd=f"echo '{random_data}' | sudo tee {fuse_dir}{fuse_subvol_path}{file_name1} > /dev/null"
        )
        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {kernel_dir}{kernel_subvol_path}.snap/{snap1}"
        )
        log.info(
            "Fetch the daemon_name, fsid, asok_file, filesystem_id and peer_id to validate the synchronisation"
        )
        fsid = self.get_fsid(cephfs_mirror_node[0])
        log.info(f"fsid on ceph cluster : {fsid}")
        daemon_names = self.get_daemon_name(source_clients[0])

        asok_files = self.get_asok_file(cephfs_mirror_node, fsid, daemon_names)
        log.info(f"Admin Socket file of cephfs-mirror daemon : {asok_files}")
        filesystem_id = self.get_filesystem_id_by_name(source_clients[0], source_fs)
        log.info(f"filesystem id of {source_fs} is : {filesystem_id}")
        peer_uuid = self.get_peer_uuid_by_name(source_clients[0], source_fs)
        log.info(f"peer uuid of {source_fs} is : {peer_uuid}")

        log.info("Validate if the Snapshots are syned to Target Cluster")

        result_snap_k1 = self.validate_snapshot_sync_status(
            cephfs_mirror_node,
            source_fs,
            snap1,
            fsid,
            asok_files,
            filesystem_id,
            peer_uuid,
        )

        source_clients[0].exec_command(
            sudo=True, cmd=f"mkdir {fuse_dir}{fuse_subvol_path}.snap/{snap2}"
        )
        result_snap_f1 = self.validate_snapshot_sync_status(
            cephfs_mirror_node,
            source_fs,
            snap2,
            fsid,
            asok_files,
            filesystem_id,
            peer_uuid,
        )
        if result_snap_k1 and result_snap_f1:
            log.info(f"Snapshot '{result_snap_k1['snapshot_name']}' has been synced:")
            log.info(
                f"Sync Duration: {result_snap_k1['sync_duration']} of '{result_snap_k1['snapshot_name']}'"
            )
            log.info(
                f"Sync Time Stamp: {result_snap_k1['sync_time_stamp']} of '{result_snap_k1['snapshot_name']}'"
            )
            log.info(
                f"Snaps Synced: {result_snap_k1['snaps_synced']} of '{result_snap_k1['snapshot_name']}'"
            )

            log.info(f"Snapshot '{result_snap_f1['snapshot_name']}' has been synced:")
            log.info(
                f"Sync Duration: {result_snap_f1['sync_duration']} of '{result_snap_f1['snapshot_name']}'"
            )
            log.info(
                f"Sync Time Stamp: {result_snap_f1['sync_time_stamp']} of '{result_snap_f1['snapshot_name']}'"
            )
            log.info(
                f"Snaps Synced: {result_snap_f1['snaps_synced']} of '{result_snap_f1['snapshot_name']}'"
            )

            log.info("All snapshots synced.")
        else:
            log.error("One or both snapshots not found or not synced.")

        log.info(
            "Validate the synced snapshots and data available on the target cluster"
        )

    def get_cephfs_mirror_counters(self, cephfs_mirror_node, fsid, asok_file):
        """
        Get the cephfs mirror counters from the admin socket file.

        Parameters:
            cephfs_mirror_node (list): List of cephfs mirror nodes.
            fsid (str): Filesystem ID.
            asok_file (str): Admin socket file.

        Returns:
            dict: Dictionary containing the cephfs mirror counters.
        """
        command = f"ceph --admin-daemon {asok_file[cephfs_mirror_node[0].node.hostname][1]} counter dump -f json"
        out, _ = cephfs_mirror_node[0].exec_command(
            sudo=True, cmd=f"cd /var/run/ceph/{fsid}/ ; {command}"
        )
        data = json.loads(out)
        log.info(f"Output of Metrics Report : {data}")
        return data

    def get_labels_and_counters(self, resource_name, filesystem_name, json_data):
        """
        Get labels and counters for a specific resource and filesystem name from JSON data.

        Parameters:
            resource_name (str): Name of the resource.
            filesystem_name (str): Name of the filesystem.
            json_data (dict): JSON data containing labels and counters.

        Returns:
            tuple: Tuple containing labels and counters for the specified resource and filesystem.
        """
        if resource_name in json_data:
            for item in json_data[resource_name]:
                if (
                    "filesystem" in item["labels"]
                    and item["labels"]["filesystem"] == filesystem_name
                ):
                    return item["labels"], item["counters"]
                elif (
                    "source_filesystem" in item["labels"]
                    and item["labels"]["source_filesystem"] == filesystem_name
                ):
                    return item["labels"], item["counters"]
        return None, None

    def inject_sync_failure(
        self,
        target_clients,
        target_mount_path,
        target_client_user,
        source_path,
        snap_target,
        target_fs_name,
    ):
        """
        Injects a sync failure by creating a snapshot on the target filesystem.

        Parameters:
        - target_clients: The client instance to execute commands on the target.
        - target_mount_path: The mount path where the target filesystem will be mounted.
        - target_client_user: The user with permissions to perform operations on the target filesystem.
        - source_path: The source path within the target filesystem.
        - snap_target: The name of the snapshot to be created for injecting the sync failure.
        - target_fs_name: The name of the target filesystem where the snapshot will be created.

        This function performs the following steps:
        1. Creates the target mount path directory.
        2. Mounts the target filesystem using ceph-fuse.
        3. Logs the list of existing snapshots in the source path.
        4. Creates a new snapshot in the source path to inject the sync failure.
        5. Logs the list of snapshots after the failure is injected.
        """
        target_clients.exec_command(sudo=True, cmd=f"mkdir -p {target_mount_path}")
        target_clients.exec_command(
            sudo=True,
            cmd=f"ceph-fuse -n {target_client_user} {target_mount_path} --client_fs {target_fs_name}",
        )
        snapshot_list = f"ls {target_mount_path}{source_path}.snap"
        log.info(f"Existing list of snaps : {snapshot_list}")
        target_clients.exec_command(
            sudo=True, cmd=f"mkdir {target_mount_path}{source_path}.snap/{snap_target}"
        )
        snapshot_list_after_sync_failure = f"ls {target_mount_path}{source_path}.snap"
        log.info(
            f"List of snaps after injecting failure : {snapshot_list_after_sync_failure}"
        )

    def setup_subvolumes_and_mounts(
        self, source_fs, source_clients, fs_util, subvol_group_name, subvol_details
    ):
        """
        Setup subvolumes and mount them based on provided details.

        Args:
        - source_fs (str): Name of the source filesystem.
        - source_clients (list): List of client instances.
        - fs_util (object): Instance of the utility class for Ceph operations.
        - subvol_group_name (str): Name of the subvolume group.
        - subvol_details (list of dicts): Details of subvolumes to create and mount. Each dict should have keys:
            - 'subvol_name': Name of the subvolume.
            - 'subvol_size': Size of the subvolume.
            - 'mount_type': Type of mount ('kernel' or 'fuse').
            - 'mount_dir': Mount directory for the subvolume.

        Returns:
        - list: A list of subvolume paths.
        """
        log.info("Create Subvolumes for adding Data")

        subvolumegroup_list = [
            {"vol_name": source_fs, "group_name": subvol_group_name},
        ]
        for subvolumegroup in subvolumegroup_list:
            fs_util.create_subvolumegroup(source_clients, **subvolumegroup)

        subvolume_paths = []

        for subvol in subvol_details:
            subvol_name = subvol["subvol_name"]
            subvol_size = subvol["subvol_size"]
            mount_type = subvol["mount_type"]
            mount_dir = subvol["mount_dir"]

            # Create subvolume
            subvolume = {
                "vol_name": source_fs,
                "subvol_name": subvol_name,
                "group_name": subvol_group_name,
                "size": subvol_size,
            }
            fs_util.create_subvolume(source_clients, **subvolume)

            # Get subvolume path
            log.info(f"Get the path of subvolume {subvol_name} on filesystem")
            subvol_path, rc = source_clients.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {source_fs} {subvol_name} {subvol_group_name}",
            )
            index = subvol_path.find(f"{subvol_name}/")
            if index != -1:
                subvol_path = subvol_path[: index + len(f"{subvol_name}/")]
            log.info(subvol_path)

            # Store subvolume path in the list
            subvolume_paths.append(subvol_path.strip())

            # Mount subvolume based on mount type
            if mount_type == "kernel":
                mon_node_ips = fs_util.get_mon_node_ips()
                fs_util.kernel_mount(
                    [source_clients],
                    mount_dir,
                    ",".join(mon_node_ips),
                    extra_params=f",fs={source_fs}",
                )
            elif mount_type == "fuse":
                fs_util.fuse_mount(
                    [source_clients],
                    mount_dir,
                    extra_params=f" --client_fs {source_fs}",
                )
            else:
                log.error(
                    f"Invalid mount type: {mount_type}. Please specify 'kernel' or 'fuse'."
                )
        return subvolume_paths

    def get_fs_mirror_status_using_asok(
        self, cephfs_mirror_node, source_clients, fs_name
    ):
        """ """
        log.info("Validate the Synchronisation on Target Cluster")
        log.info("Install ceph-common on cephfs-mirror node")
        if not isinstance(cephfs_mirror_node, list):
            cephfs_mirror_node = [cephfs_mirror_node]
        for node in cephfs_mirror_node:
            node.exec_command(sudo=True, cmd="dnf install -y ceph-common --nogpgcheck")
        fsid = self.get_fsid(cephfs_mirror_node[0])
        daemon_names = self.get_daemon_name(source_clients)
        filesystem_id = self.get_filesystem_id_by_name(source_clients, fs_name)
        asok_files = self.get_asok_file(cephfs_mirror_node, fsid, daemon_names)
        log.info("Get filesystem mirror status")
        for node, asok_file in asok_files.items():
            out, _ = asok_file[0].exec_command(
                sudo=True,
                cmd=f"cd /var/run/ceph/{fsid}/ ; ceph --admin-daemon {asok_file[1]} "
                f"fs mirror status {fs_name}@{filesystem_id} -f json",
            )
            fs_mirror_status = json.dump(out)
            return fs_mirror_status

    def get_fs_mirror_peer_status_using_asok(
        self,
        cephfs_mirror_node,
        source_clients,
        fs_name,
    ):
        """
        Get the CephFS mirror peer status using the specified asok file.

        :param cephfs_mirror_node: List of cephfs mirror nodes
        :param fs_name: Filesystem name
        :param fsid: Filesystem ID
        :param asok_file: Asok file path
        :param filesystem_id: Filesystem ID
        :param peer_uuid: Peer UUID
        :return: JSON response containing fs mirror peer status
        """
        if not isinstance(cephfs_mirror_node, list):
            cephfs_mirror_node = [cephfs_mirror_node]
        for node in cephfs_mirror_node:
            node.exec_command(sudo=True, cmd="dnf install -y ceph-common --nogpgcheck")
        fsid = self.get_fsid(cephfs_mirror_node[0])
        daemon_names = self.get_daemon_name(source_clients)
        filesystem_id = self.get_filesystem_id_by_name(source_clients, fs_name)
        asok_files = self.get_asok_file(cephfs_mirror_node, fsid, daemon_names)
        peer_uuid = self.get_peer_uuid_by_name(source_clients, fs_name)
        log.info("Get filesystem mirror status")
        for node, asok_file in asok_files.items():
            out, _ = asok_file[0].exec_command(
                sudo=True,
                cmd=f"cd /var/run/ceph/{fsid}/ ; ceph --admin-daemon {asok_file[1]} "
                f"fs mirror peer status {fs_name}@{filesystem_id} {peer_uuid} -f json",
            )
            fs_mirror_status = json.loads(out)
            return fs_mirror_status

    def validate_snaps_status_increment(self, json_before, json_after, snap_status):
        validation_results = {}
        for path in json_before:
            before_snaps_synced = json_before[path].get(snap_status, 0)
            after_snaps_synced = json_after[path].get(snap_status, 0)
            validation_results[path] = after_snaps_synced > before_snaps_synced
        return validation_results

    def initialize_csv_file_snapdiff(self, csv_file, ceph_version_out):
        try:
            with open(csv_file, mode="x", newline="") as file:
                writer = csv.writer(file)
                writer.writerow([f"Ceph Version: {ceph_version_out}"])
                writer.writerow([])
                writer.writerow(
                    [
                        "Snapshot Type",
                        "Snapshot Name",
                        "Sync Duration",
                        "Sync Timestamp",
                        "Snaps Synced",
                    ]
                )
        except FileExistsError:
            pass

    def log_snapshot_info_snapdiff(self, snapshot_type, snapshot_info, csv_file):
        with open(csv_file, mode="a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    snapshot_type,
                    snapshot_info["snapshot_name"],
                    snapshot_info["sync_duration"],
                    snapshot_info["sync_time_stamp"],
                    snapshot_info["snaps_synced"],
                ]
            )

    def mount_subvolumes_snapdiff(
        self,
        source_client,
        fs_util_ceph1,
        default_fs,
        subvolume_names,
        subvol_group_name,
        nfs_server,
        nfs_name,
    ):
        """
        Function to mount paths which will be used for snapdiff tests.
        """

        export_created = 0
        mount_paths = {}
        subvol_paths = {}

        for idx, mount_type in enumerate(["fuse", "kernel", "nfs"]):
            subvol_path, _ = source_client.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {default_fs} {subvolume_names[idx]} "
                f"{subvol_group_name if idx in [0, 1, 2] else ''}",
            )
            subvol_path = subvol_path.strip()

            mount_params = {
                "client": source_client,
                "fs_util": fs_util_ceph1,
                "fs_name": default_fs,
                "mnt_path": subvol_path.strip(),
                "export_created": export_created,
            }

            if mount_type == "nfs":
                export_binding = f"/nfs_export_{''.join(secrets.choice(string.digits) for _ in range(3))}"
                mount_params.update(
                    {
                        "nfs_server": nfs_server,
                        "nfs_name": nfs_name,
                        "nfs_export_name": export_binding,
                        "export_created": export_created,
                    }
                )

            mounting_path, export_created = fs_util_ceph1.mount_ceph(
                mount_type, mount_params
            )

            if mount_type == "nfs" and not mounting_path:
                log.error("CephFS NFS export mount failed")
                return 1
            mount_paths[mount_type] = mounting_path
            subvol_name = subvolume_names[idx]
            subvol_index = subvol_path.find(subvol_name)
            subvol_paths[mount_type] = (
                subvol_path[subvol_index:] if subvol_index != -1 else subvol_path
            )

        return mount_paths, subvol_paths, export_binding

    def create_files_for_snapdiff(
        self, client, dir_path, num_files, size, cloud_type=None
    ):
        """
        Uploads and executes the generate_large_file.py script on the remote client.

        :param client: Remote client object
        :param dir_path: Directory where files should be created
        :param num_files: Number of files to create
        :param size: Size of each file (int)
        :param cloud_type: Optional cloud type to determine size unit
        """
        generate_file_script = "generate_files_for_snapdiff.py"
        remote_path = f"/root/{generate_file_script}"
        unit = "MB" if cloud_type in ["ibmc", "openstack"] else "GB"

        cmd = f"python3 {remote_path} {dir_path} {num_files} {size} {unit}"
        client.exec_command(sudo=True, cmd=cmd, timeout=14400)

        log.info(
            "Completed creation of %s files, each of size %s %s, on  Paths %s",
            num_files,
            size,
            unit,
            dir_path,
        )

    def modify_files_for_snapdiff(
        self, client, dir_path, num_files, mode="write", length=5, bytes_size=None
    ):
        """
        Uploads and executes the modify_file_at_10_random_offsets.py script on the remote client.

        :param client: Remote client object
        :param dir_path: Directory where files should be created or read
        :param num_files: Number of files to modify
        :param mode: 'write' or 'read'
        :param length: Number of random offsets
        :param bytes_size: Required only for write mode, size of data to write at each offset (e.g., '1M', '512K')
        """
        modify_script = "modify_file_at_10_random_offsets.py"
        remote_path = f"/root/{modify_script}"

        cmd = f"python3 {remote_path} {dir_path} --file-count {num_files} --mode {mode} --length {length}"
        if mode == "write" and bytes_size:
            cmd += f" --bytes {bytes_size}"

        client.exec_command(
            sudo=True,
            cmd=cmd,
            timeout=14400,
        )

        log.info(
            "Completed '%s' modification on %s files in path: %s using %s random offsets%s",
            mode,
            num_files,
            dir_path,
            length,
            (
                f", writing {bytes_size} at each offset"
                if mode == "write" and bytes_size
                else ""
            ),
        )

    def create_snapshot_snapdiff(
        self,
        fs_util,
        client,
        mounting_dir,
        subvol_path,
        snap_name,
        source_fs,
        subvolume=False,
        subvol_name=None,
        subvol_group=None,
    ):
        """
        Function to create a snapshot (either path-based or subvolume-based).
        """
        if subvolume:
            if not subvol_name or not subvol_group:
                raise ValueError(
                    "subvol_name and subvol_group are required when subvolume=True"
                )

            fs_util.create_snapshot(
                client=client,
                vol_name=source_fs,
                subvol_name=subvol_name,
                snap_name=snap_name,
                validate=True,
                group_name=subvol_group,
            )

        else:
            # Default path-based snapshot creation
            client.exec_command(
                sudo=True, cmd=f"mkdir {mounting_dir}{subvol_path}/.snap/{snap_name}"
            )

    # Function to validate snapshot sync with retry
    @retry(CommandFailed, tries=10, delay=30)
    def validate_snapshot_sync(
        self,
        fs_mirroring_utils,
        cephfs_mirror_node,
        source_fs,
        snap_name,
        fsid,
        asok_file,
        filesystem_id,
        peer_uuid,
    ):
        """
        Function to validate snapshot sync status.
        """
        result = fs_mirroring_utils.validate_snapshot_sync_status(
            cephfs_mirror_node,
            source_fs,
            snap_name,
            fsid,
            asok_file,
            filesystem_id,
            peer_uuid,
        )

        if result:
            log.info(f"Snapshot '{result['snapshot_name']}' has been synced:")
            log.info(
                f"Sync Duration: {result['sync_duration']} of '{result['snapshot_name']}'"
            )
            log.info(
                f"Sync Time Stamp: {result['sync_time_stamp']} of '{result['snapshot_name']}'"
            )
            log.info(
                f"Snaps Synced: {result['snaps_synced']} of '{result['snapshot_name']}'"
            )

            return {
                "snapshot_name": result["snapshot_name"],
                "sync_duration": result["sync_duration"],
                "sync_time_stamp": result["sync_time_stamp"],
                "snaps_synced": result["snaps_synced"],
            }
        else:
            log.error(f"Snapshot '{snap_name}' not found or not synced.")
            raise CommandFailed(f"Snapshot '{snap_name}' not found or not synced.")

    def prepare_env_snapdiff(self, config, ceph_cluster_dict, test_data):
        """
        Prepares environment for CephFS mirroring snapdiff performance tests.
        Returns all initialized variables as a dictionary.
        """
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        fs_util_ceph1 = FsUtils(ceph_cluster_dict.get("ceph1"), test_data=test_data)
        fs_util_ceph2 = FsUtils(ceph_cluster_dict.get("ceph2"), test_data=test_data)
        fs_mirroring_utils = CephfsMirroringUtils(
            ceph_cluster_dict.get("ceph1"), ceph_cluster_dict.get("ceph2")
        )

        build = config.get("build", config.get("rhbuild"))

        source_clients = ceph_cluster_dict.get("ceph1").get_ceph_objects("client")
        target_clients = ceph_cluster_dict.get("ceph2").get_ceph_objects("client")
        cephfs_mirror_node = ceph_cluster_dict.get("ceph1").get_ceph_objects(
            "cephfs-mirror"
        )

        log.info("checking Pre-requisites")
        if not source_clients or not target_clients:
            log.error(
                "This test requires a minimum of 1 client node on both ceph1 and ceph2."
            )
            return 1

        log.info("Preparing Clients...")
        fs_util_ceph1.prepare_clients(source_clients, build)
        fs_util_ceph2.prepare_clients(target_clients, build)
        fs_util_ceph1.auth_list(source_clients)
        fs_util_ceph2.auth_list(target_clients)

        log.info("Create required filesystem on Source Cluster...")
        mds_nodes = ceph_cluster_dict.get("ceph1").get_ceph_objects("mds")
        source_fs = (
            config.get("source_fs") if not erasure else f'{config.get("source_fs")}-ec'
        )
        mds_names = [mds.node.hostname for mds in mds_nodes]
        hosts_list1 = mds_names[0:2]
        mds_hosts_1 = " ".join(hosts_list1) + " "
        log.info(f"MDS host list 1 {mds_hosts_1}")
        fs_util_ceph1.create_fs(
            client=source_clients[0],
            vol_name=source_fs,
            validate=True,
            placement=f"2 {mds_hosts_1}",
        )

        fs_util_ceph1.wait_for_mds_process(source_clients[0], source_fs)

        log.info("Create required filesystem on Target Cluster...")
        mds_nodes = ceph_cluster_dict.get("ceph2").get_ceph_objects("mds")
        target_fs = (
            config.get("target_fs") if not erasure else f'{config.get("target_fs")}-ec'
        )
        mds_names = [mds.node.hostname for mds in mds_nodes]
        hosts_list1 = mds_names[0:2]
        mds_hosts_1 = " ".join(hosts_list1) + " "
        log.info(f"MDS host list 1 {mds_hosts_1}")
        fs_util_ceph1.create_fs(
            client=target_clients[0],
            vol_name=target_fs,
            validate=True,
            placement=f"2 {mds_hosts_1}",
        )

        fs_util_ceph1.wait_for_mds_process(target_clients[0], target_fs)

        ceph_cluster = ceph_cluster_dict.get("ceph1")
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        if not nfs_servers:
            log.error("No NFS servers found in the Ceph cluster.")
            return 1

        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"

        try:
            fs_util_ceph1.create_nfs(
                source_clients[0],
                nfs_name,
                validate=True,
                placement="1 %s" % nfs_server,
            )
            log.info("NFS cluster %s created successfully." % nfs_name)
        except CommandFailed as e:
            log.error("Failed to create NFS cluster: %s" % e)
            return 1

        return {
            "source_clients": source_clients,
            "target_clients": target_clients,
            "fs_util_ceph1": fs_util_ceph1,
            "fs_util_ceph2": fs_util_ceph2,
            "fs_mirroring_utils": fs_mirroring_utils,
            "source_fs": source_fs,
            "target_fs": target_fs,
            "erasure": erasure,
            "cephfs_mirror_node": cephfs_mirror_node,
            "nfs_server": nfs_server,
            "nfs_name": nfs_name,
        }

    def modify_and_create_snapshot_snapdiff(
        self,
        fs_util,
        num_files,
        snap_suffix,
        label_suffix,
        io_dir_paths,
        source_clients,
        mount_paths,
        subvol_paths_without_uuid,
        source_fs,
        subvol_group_name,
        fs_mirroring_utils,
        cephfs_mirror_node,
        fsid,
        asok_file,
        filesystem_id,
        peer_uuid,
        csv_file,
        mode,
    ):
        """
        Modify files and create incremental snapshots for kernel, fuse, and nfs mounts.
        Then validate and log snapshot info.
        """
        snapshots = {}

        # Step 1: Modify files
        log.info(
            f"Modify {num_files} files and take {label_suffix} incremental snapshots"
        )
        for mount_type in ["kernel", "fuse", "nfs"]:
            dir_path = io_dir_paths[mount_type]

            if mode == "write":
                self.modify_files_for_snapdiff(
                    source_clients[0],
                    dir_path,
                    num_files,
                    mode="write",
                    length=5,
                    bytes_size="1M",
                )
            elif mode == "read":
                self.modify_files_for_snapdiff(
                    source_clients[0], dir_path, num_files, mode="read", length=5
                )
            elif mode == "remove":
                self.modify_files_for_snapdiff(
                    source_clients[0], dir_path, num_files, mode="remove", length=5
                )
            else:
                log.warning(
                    f"Unsupported mode: {mode}, skipping modification for {mount_type}"
                )

        # Step 2: Create snapshots
        for mount_type in ["kernel", "fuse", "nfs"]:
            snap_name = f"snap_{mount_type[0]}_{snap_suffix}"
            self.create_snapshot_snapdiff(
                fs_util,
                source_clients[0],
                mount_paths[mount_type],
                subvol_paths_without_uuid[mount_type],
                snap_name,
                source_fs,
                subvolume=True,
                subvol_name=subvol_paths_without_uuid[mount_type].rstrip("/"),
                subvol_group=subvol_group_name,
            )
            snapshots[mount_type] = snap_name

        # Step 3: Validate sync and log
        for mount_type in ["kernel", "fuse", "nfs"]:
            snap_info = self.validate_snapshot_sync(
                fs_mirroring_utils,
                cephfs_mirror_node[0],
                source_fs,
                snapshots[mount_type],
                fsid,
                asok_file,
                filesystem_id,
                peer_uuid,
            )
            if snap_info:
                log.info(
                    f"{mount_type.capitalize()} Snapshot Info - Name: {snap_info['snapshot_name']}, "
                    f"Duration: {snap_info['sync_duration']}, "
                    f"Time Stamp: {snap_info['sync_time_stamp']}, "
                    f"Snaps Synced: {snap_info['snaps_synced']}"
                )
                self.log_snapshot_info_snapdiff(
                    f"{mount_type.capitalize()} Incremental {label_suffix}",
                    snap_info,
                    csv_file,
                )

    def get_rsync_command(
        self, source_path, target_path, target_ip, target_user="root"
    ):
        source_path = source_path.strip()
        target_path = target_path.strip()

        cmd = (
            f"time rsync -av "
            f'-e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" '
            f'"{source_path}" {target_user}@{target_ip}:"{target_path}"'
        )
        return cmd
