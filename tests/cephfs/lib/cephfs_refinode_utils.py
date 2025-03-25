"""
This is cephfs referent inode utility module
It contains all the re-useable functions related to cephfs snapshot with
hardlinks and eliminating global snaprealm and introducing of referent inodes.
"""

import json
import random
import re
import secrets
import string

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


class RefInodeUtils(object):
    def __init__(self, ceph_cluster):
        """
        FS Snapshot with Hardlinks Utility object
        Args:
            ceph_cluster (ceph.ceph.Ceph): ceph cluster
        """

        self.ceph_cluster = ceph_cluster
        self.mons = ceph_cluster.get_ceph_objects("mon")
        self.mgrs = ceph_cluster.get_ceph_objects("mgr")
        self.osds = ceph_cluster.get_ceph_objects("osd")
        self.mdss = ceph_cluster.get_ceph_objects("mds")
        self.clients = ceph_cluster.get_ceph_objects("client")

    def prepare_environment(self, ceph_cluster, test_data, config):
        """
        Initializes CephFS clients, sets up authentication, and retrieves erasure coding settings.

        Args:
            ceph_cluster (CephCluster): The Ceph cluster object.
            test_data (dict): Test-specific configurations.
            config (dict): Test configurations (e.g., build version).

        Returns:
            tuple: (FsUtils instance, list of clients, erasure coding flag)
        """
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        return fs_util, clients, erasure

    def create_subvolumes(self, client, fs_util, default_fs, subvol_group_name):
        """
        Creates a subvolume group and multiple subvolumes, both within and outside the group.

        Args:
            client (CephClient): Ceph client node.
            fs_util (FsUtils): CephFS utility instance.
            default_fs (str): Default Ceph filesystem name.
            subvol_group_name (str): Subvolume group name.

        Returns:
            list: Created subvolume names.
        """
        subvolumegroup = {"vol_name": default_fs, "group_name": subvol_group_name}
        fs_util.create_subvolumegroup(client, **subvolumegroup)

        subvolume_names = []
        for i in range(1, 4):
            subvolume_name = f"subvolume_ref_inode_{i}"
            subvolume_with_group = {
                "vol_name": default_fs,
                "subvol_name": subvolume_name,
                "group_name": subvolumegroup["group_name"],
                "size": "5368706371",
            }
            fs_util.create_subvolume(client, **subvolume_with_group)
            subvolume_names.append(subvolume_name)

        for i in range(4, 7):
            subvolume_name = f"subvolume_name_{i}"
            subvolume_without_group = {
                "vol_name": default_fs,
                "subvol_name": subvolume_name,
                "size": "5368706371",
            }
            fs_util.create_subvolume(client, **subvolume_without_group)
            subvolume_names.append(subvolume_name)

        log.info(f"Captured Subvolume Names: {subvolume_names}")
        return subvolume_names

    def mount_subvolumes(
        self,
        client,
        fs_util,
        default_fs,
        subvolume_names,
        subvol_group_name,
        nfs_server,
        nfs_name,
    ):
        """
        Mounts CephFS subvolumes using different mount types (Fuse, Kernel, NFS).

        Args:
            client: The Ceph client node.
            fs_util: The Ceph filesystem utility object.
            default_fs: The default CephFS volume name.
            subvolume_names: List of subvolume names to be mounted.
            subvol_group_name: The subvolume group name (if applicable).
            nfs_server: The NFS server hostname.
            nfs_name: The NFS cluster name.

        Returns:
            int: 0 if all mounts are successful, 1 if any NFS mount fails.
        """
        export_created = 0

        for idx, mount_type in enumerate(
            ["fuse", "kernel", "nfs", "fuse", "kernel", "nfs"]
        ):
            subvol_path, _ = client.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume getpath {default_fs} {subvolume_names[idx]} "
                f"{subvol_group_name if idx in [0, 1, 2] else ''}",
            )
            mount_params = {
                "client": client,
                "fs_util": fs_util,
                "fs_name": default_fs,
                "mnt_path": subvol_path.strip(),
            }

            if mount_type == "nfs":
                mount_params.update(
                    {
                        "nfs_server": nfs_server,
                        "nfs_name": nfs_name,
                        "nfs_export_name": f"/nfs_export_{''.join(secrets.choice(string.digits) for _ in range(3))}",
                        "export_created": export_created,
                    }
                )

            mounting_path, export_created = fs_util.mount_ceph(mount_type, mount_params)

            if mount_type == "nfs" and not mounting_path:
                log.error("CephFS NFS export mount failed")
                return 1

        return 0

    def mount_rootfs(self, client, fs_util, default_fs, nfs_server, nfs_name):
        """
        Mounts the root of a CephFS filesystem using FUSE, Kernel, and NFS.
        This function:
        - Generates a random directory name for mounting.
        - Mounts the CephFS root using FUSE.
        - Mounts the CephFS root using the Kernel mount.
        - Creates an NFS export for CephFS and mounts it.

        Args:
            client (CephClient): The Ceph client node used to execute commands.
            fs_util (FsUtils): An instance of FsUtils to interact with CephFS.
            default_fs (str): The name of the default Ceph filesystem.
            nfs_server (str): The NFS server hostname or IP.
            nfs_name (str): The name of the NFS cluster.

        Returns:
            tuple: A tuple containing:
                - str: The FUSE mount directory path.
                - str: The Kernel mount directory path.
                - str: The NFS mount directory path.

        Raises:
            Exception: If the NFS export mount fails, the function logs an error and returns 1.
        """
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )

        # Fuse mount
        fuse_mount_dir = f"/mnt/cephfs_root_fuse{mounting_dir}_1/"
        fs_util.fuse_mount(
            [client], fuse_mount_dir, extra_params=f"--client_fs {default_fs}"
        )

        # Kernel mount
        kernel_mount_dir = f"/mnt/cephfs_root_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [client],
            kernel_mount_dir,
            ",".join(mon_node_ips),
            extra_params=f",fs={default_fs}",
        )

        # NFS export creation and mount
        nfs_export_name = "/nfs_export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        try:
            fs_util.create_nfs_export(
                client, nfs_name, nfs_export_name, default_fs, path="/"
            )
        except Exception as e:
            log.error(f"Failed to create NFS export: {str(e)}")
            return 1

        nfs_mounting_dir = f"/mnt/cephfs_root_nfs_{mounting_dir}_1/"
        if not fs_util.cephfs_nfs_mount(
            client, nfs_server, nfs_export_name, nfs_mounting_dir
        ):
            log.error("cephfs nfs export mount failed")
            return 1

        return fuse_mount_dir, kernel_mount_dir, nfs_mounting_dir

    def create_directories(self, client, base_path, dirs):
        for directory in dirs:
            dir_path = f"{base_path}/{directory}"
            client.exec_command(sudo=True, cmd=f"mkdir -p {dir_path}")
            log.info(f"Created directory: {dir_path}")

    def create_file_with_content(self, client, file_path, content):
        client.exec_command(sudo=True, cmd=f'echo "{content}" > {file_path}')
        log.info(f"Created file with content: {file_path}")

    def append_to_file(self, client, file_path, content):
        client.exec_command(sudo=True, cmd=f'echo "{content}" >> {file_path}')
        log.info(f"Appended content to file: {file_path}")

    def rename_file(self, client, old_path, new_path):
        client.exec_command(sudo=True, cmd=f"mv {old_path} {new_path}")
        log.info(f"Renamed file from {old_path} to {new_path}")

    def create_snapshot_on_dir(self, client, dir_path, snapshot_name):
        snap_path = f"{dir_path}/.snap/{snapshot_name}"
        client.exec_command(sudo=True, cmd=f"mkdir -p {snap_path}")
        log.info(f"Created snapshot: {snap_path}")

    def get_rados_object_for_dir(self, client, meta_pool_name, dir_path):
        """
        Retrieves the RADOS object ID corresponding to a given directory in CephFS.

        This function:
        1. Retrieves the inode number of the specified directory using the `stat` command.
        2. Converts the inode number to a hexadecimal representation.
        3. Lists the objects in the specified metadata pool.
        4. Checks if the expected RADOS object ID is present in the pool.

        Args:
            client (CephClient): The Ceph client node used to execute commands.
            meta_pool_name (str): The name of the metadata pool where directory metadata is stored.
            dir_path (str): The absolute path of the directory in CephFS.

        Returns:
            str: The RADOS object ID if found in the metadata pool.
            None: If the corresponding RADOS object is not found.

        Raises:
            Exception: If any command execution fails, logs the error and returns None.
        """
        try:
            # Step 1: Stat the directory to get the inode number
            stdout, stderr = client.exec_command(
                sudo=True, cmd=f"stat -c %i {dir_path}"
            )
            inode_number = stdout.strip()
            log.info(f"Inode number for {dir_path}: {inode_number}")

            # Step 2: Convert inode number to hexadecimal
            inode_hex = format(int(inode_number), "x").zfill(8)
            rados_object_id = f"{inode_hex}.00000000"
            log.info(f"Expected RADOS object ID for {dir_path}: {rados_object_id}")

            # Step 3: List RADOS objects in the specified pool
            stdout, stderr = client.exec_command(
                sudo=True,
                cmd=f"rados -p {meta_pool_name} ls | egrep '([0-9]|[a-f]){{11}}'",
            )
            rados_objects = stdout.splitlines()
            log.info(f"Objects in pool {meta_pool_name}: {rados_objects}")

            # Step 4: Check if the expected RADOS object is present
            if rados_object_id in rados_objects:
                log.info(f"RADOS object for {dir_path} found: {rados_object_id}")
                return rados_object_id
            else:
                log.error(f"RADOS object for {dir_path} not found!")
                return None
        except Exception as e:
            raise Exception(f"Failed tretrieve RADOS object for {dir_path}: {e}")

    def get_rados_object_from_datapool(self, client, data_pool_name):
        """
        Retrieves the list of RADOS objects stored in the specified data pool.
        This function executes a `rados ls` command to list all objects in the given
        data pool and returns them as a list.
        Args:
            client (CephClient): The Ceph client node used to execute commands.
            data_pool_name (str): The name of the Ceph data pool to query.
        Returns:
            list: A list of RADOS object names present in the specified data pool.
        Raises:
            Exception: If the command execution fails, logs the error.
        """
        #  List RADOS objects in the specified pool
        stdout, stderr = client.exec_command(
            sudo=True, cmd=f"rados -p {data_pool_name} ls"
        )
        log.info(f"List Rados Objects : {stdout}")
        rados_objects = stdout.splitlines()
        log.info(f"Objects in pool {data_pool_name}: {rados_objects}")
        return rados_objects

    def list_snapshots_for_object(self, client, data_pool_name, rados_object):
        """
        Fetches and validates snapshot details for a given RADOS object in a specified data pool.

        This function executes a `rados listsnaps` command to retrieve snapshot information
        for the given RADOS object in JSON format. It then parses the output and counts
        the number of snapshots, returning a summary.

        Args:
            client (CephClient): The Ceph client node used to execute commands.
            data_pool_name (str): The name of the Ceph data pool where the object resides.
            rados_object (str): The RADOS object for which snapshots need to be listed.

        Returns:
            str: A formatted message indicating the number of snapshots created for the object,
                along with the JSON output.

        Raises:
            Exception: If the command execution fails, logs the error.
        """
        # Run rados command to fetch snapshot details in JSON format
        cmd = f"rados -p {data_pool_name} listsnaps {rados_object} -f json"
        stdout, stderr = client.exec_command(sudo=True, cmd=cmd)

        # Parse JSON output
        data = json.loads(stdout)
        clones = data.get("clones", [])

        # Check if only "head" exists and no snapshots are present
        if (
            len(clones) == 1
            and clones[0].get("id") == "head"
            and not clones[0].get("snapshots")
        ):
            return f"No new snapshots created.\nOutput:\n{json.dumps(data, indent=4)}"

        # Count the number of snapshots
        snapshot_count = sum(
            len(clone.get("snapshots", []))
            for clone in clones
            if clone.get("id") != "head"
        )

        return f"{snapshot_count} snapshot(s) created.\nOutput:\n{json.dumps(data, indent=4)}"

    def create_hardlink_and_validate(
        self, client, fs_util, file1_path, hl_file_path, data_pool_name, fs_name
    ):
        """
        Creates a hard link for a given file, fetches file statistics, validates inode numbers,
        and checks for the presence of a newly created RADOS object.

        This function performs the following steps:
        1. Captures the initial list of RADOS objects in the specified data pool.
        2. Creates a hard link to the given file.
        3. Flushes the journal on the active MDS nodes to ensure changes are recorded.
        4. Validates that both the original file and the hard link have the same inode number.
        5. Captures the list of RADOS objects again and identifies any new objects created.
        6. Determines whether a new referent inode was created.

        Args:
            client (CephClient): The Ceph client node used to execute commands.
            fs_util (FSUtil): The filesystem utility instance to manage CephFS operations.
            file1_path (str): Path to the original file.
            hl_file_path (str): Path where the hard link will be created.
            data_pool_name (str): Name of the Ceph data pool to monitor RADOS objects.
            fs_name (str): Name of the Ceph filesystem.

        Returns:
            tuple:
                - (bool): `True` if the validation succeeds, `False` otherwise.
                - (str): "yes" if a new RADOS object is created, "no" otherwise.
                - (set): Set of newly created RADOS objects, if any.

        Raises:
            Exception: If any command execution fails.

        """

        def exec_stat(file_path):
            """Executes stat command and extracts inode number."""
            stdout, stderr = client.exec_command(sudo=True, cmd=f"stat {file_path}")
            log.info(f"File statistics for {file_path}:\n{stdout}")
            match = re.search(r"Inode:\s+(\d+)", stdout)
            return match.group(1) if match else None

        # Step 1: Capture initial RADOS objects
        initial_rados_objects = set(
            self.get_rados_object_from_datapool(client, data_pool_name)
        )
        log.info(f"Initial RADOS objects: {initial_rados_objects}")

        # Step 2: Create hard link
        client.exec_command(sudo=True, cmd=f"ln {file1_path} {hl_file_path}")
        log.info(f"Created hardlink: {hl_file_path}")

        # Step 3: Flush journal to ensure changes are recorded
        self.flush_journal_on_active_mdss(fs_util, client, fs_name)

        # Step 4: Compare Inode to validate hardlinks
        inode1, inode2 = exec_stat(file1_path), exec_stat(hl_file_path)
        if inode1 and inode2 and inode1 == inode2:
            log.info(f"Validation Passed: Inodes match ({inode1})")
        else:
            log.error(f"Validation Failed: Inodes do not match ({inode1} != {inode2})")
            return False, "no", None

        # Step 5: Capture RADOS objects after creating hardlink
        post_rados_objects = set(
            self.get_rados_object_from_datapool(client, data_pool_name)
        )
        log.info(f"RADOS objects after creating hardlink: {post_rados_objects}")

        # Step 6: Identify the newly created RADOS object
        new_rados_objects = post_rados_objects - initial_rados_objects
        log.info(f"Newly created RADOS objects: {new_rados_objects}")

        # Step 7: Validate referent inode
        referent_inode = "yes" if len(new_rados_objects) > 0 else "no"
        log.info(f"Referent Inode: {referent_inode}")

        return True, referent_inode, new_rados_objects

    def flush_journal_on_active_mdss(self, fs_util, client, fs_name):
        """
        Flushes the journal on all active MDS nodes for a given Ceph filesystem.

        This function retrieves the list of active Metadata Server (MDS) nodes for the specified
        filesystem and issues the `ceph tell mds.<mds> flush journal` command on each active MDS.
        Flushing the journal ensures that all pending metadata changes are committed to the backing
        storage, improving metadata consistency.

        Args:
            fs_util (FSUtil): The filesystem utility instance used to fetch active MDS nodes.
            client (CephClient): The Ceph client node used to execute commands.
            fs_name (str): Name of the Ceph filesystem.

        Returns:
            None

        Logs:
            - The list of active MDS nodes retrieved.
            - Confirmation messages when journals are flushed on each active MDS.

        Notes:
            - Redirects command output to `/dev/null 2>&1` to suppress standard output and errors.

        Raises:
            Exception: If the execution of any command fails.

        """
        # Retrieve the list of active MDS nodes
        active_mdss = fs_util.get_active_mdss(client, fs_name)
        # Issue flush journal command to each active MDS
        for mds in active_mdss:
            client.exec_command(
                sudo=True, cmd=f"ceph tell mds.{mds} flush journal > /dev/null 2>&1"
            )
            log.info(f"Journal flushed on MDS: mds.{mds}")

    def validate_referent_inode(
        self,
        fs_util,
        client,
        mount_path,
        data_pool,
        fs_name,
    ):
        """
        Automates the creation of directories, files, snapshots, and hardlinks to validate referent inodes.

        This function performs the following steps:
        1. Creates directories and flushes the journal to persist metadata.
        2. Creates a file in one of the directories, captures its RADOS object, and lists snapshots.
        3. Creates a snapshot directly on the directory, modifies the file, and checks snapshot details.
        4. Creates a hardlink for the file in another directory and validates that the referent inode exists.

        Args:
            fs_util (FSUtil): The filesystem utility instance used to perform CephFS operations.
            client (CephClient): The Ceph client node used to execute commands.
            mount_path (str): The mount point of the Ceph filesystem.
            data_pool (str): The name of the data pool where RADOS objects are stored.
            fs_name (str): Name of the Ceph filesystem.

        Returns:
            None

        Logs:
            - Step-by-step execution of directory, file, and snapshot creation.
            - Details of the initial and post-snapshot RADOS objects.
            - Validation results of the referent inode presence after creating a hardlink.

        Notes:
            - This function assumes that the first RADOS object in `get_rados_object_from_datapool`
                corresponds to `dir1`.
            - The `flush_journal_on_active_mdss` function is called after critical
                operations to ensure metadata persistence.

        Raises:
            Exception: If any command execution or validation step fails.

        """
        log.info("Starting validatiopn of the referent inode automation...")

        # Step 1: Create directories
        dirs = ["dir1", "dir2"]
        self.create_directories(client, mount_path, dirs)
        self.flush_journal_on_active_mdss(fs_util, client, fs_name)

        # Step 2: Create files and snapshots in dir1
        file1_path = f"{mount_path}/dir1/file1"
        self.create_file_with_content(client, file1_path, "first file")
        self.flush_journal_on_active_mdss(fs_util, client, fs_name)
        object_id = self.get_rados_object_from_datapool(client, data_pool)
        object_id_dir1 = object_id[0]  # Assuming first object corresponds to dir1
        snapshot_details = self.list_snapshots_for_object(
            client, data_pool, object_id_dir1
        )
        log.info(f"Snapshot Details : {snapshot_details}")

        # Step 3 : Create Snapshot directly on dir path
        self.create_snapshot_on_dir(client, f"{mount_path}/dir1", "snap1")
        self.append_to_file(client, file1_path, "first file second line")
        self.flush_journal_on_active_mdss(fs_util, client, fs_name)
        snapshot_details = self.list_snapshots_for_object(
            client, data_pool, object_id_dir1
        )
        log.info(f"Snapshot Details on Dir1 : {snapshot_details}")

        # Step 4: Create hardlink and validate
        file1_path = f"{mount_path}/dir1/file1"
        hl_file_path = f"{mount_path}/dir2/hl_file1"
        (
            validation_result,
            referent_inode,
            new_rados_objects,
        ) = self.create_hardlink_and_validate(
            client, fs_util, file1_path, hl_file_path, data_pool, fs_name
        )
        log.info(
            f"Validation Result: {validation_result}, Referent Inode exists : {referent_inode} : {new_rados_objects}"
        )
        log.info("Validation of presence of Referent Inode completed successfully!")

    def cleanup_snap_directories(self, client, mount_path):
        """
        Cleans up snapshot directories and files within a specified mount path.

        This function performs the following cleanup operations:
        1. Removes all snapshot entries within the `.snap` directories for each subdirectory.
        2. Deletes all files and directories within the given mount path.
            - The function attempts to remove `.snap` directories first using `rmdir`,
                which only works if they are empty.
            - It then forcefully removes all remaining files and directories within the mount path.
        Args:
            client (CephClient): The Ceph client node used to execute commands.
            mount_path (str): The mount point of the Ceph filesystem where snapshots and directories need to be cleaned.
        Returns:
            None
        Raises:
            Exception: If command execution fails, it logs the error and continues.
        """
        log.info(f"Cleaning up snapshots within {mount_path}...")
        try:
            client.exec_command(sudo=True, cmd=f"rmdir {mount_path}/*/.snap/*")
            client.exec_command(sudo=True, cmd=f"rm -rf {mount_path}/*")
        except Exception as e:
            log.error(f"Failed to clean up .snap directories within {mount_path}: {e}")
            return 1
        log.info("Cleanup of snap directories completed.")

    def get_inode_number(self, client, file_path):
        cmd = f"stat -c %i {file_path}"
        stdout, stderr = client.exec_command(sudo=True, cmd=cmd)

        # Extract the output and validate
        inode_number = stdout.strip()
        if not inode_number.isdigit():
            raise Exception(f"Failed to get inode number: {stdout} {stderr}")

        return int(inode_number)

    def get_inode_details(self, client, fs_name, inode_number, mount_dir):
        """
        Retrieves inode details using 'ceph tell mds' and validates hard links.

        :param client: The client object to execute commands.
        :param fs_name: The CephFS name.
        :param inode_number: The inode number to query.
        :param mount_dir: The mount directory path to validate.
        :return: Dictionary of inode details if present, otherwise None.
        """
        cmd = f"ceph tell mds.{fs_name}:0 dump inode {inode_number} -f json"
        out, rc = client.exec_command(sudo=True, cmd=cmd)

        try:
            inode_data = json.loads(out.strip())  # Correctly parse JSON
        except json.JSONDecodeError:
            raise Exception(f"Failed to parse JSON output: {out}")

        # Return file path for the given inode
        file_path = inode_data.get("path", "")
        log.info(f"File path for inode {inode_number}: {file_path}")

        # Check for hard links
        nlink = inode_data.get("nlink", 1)
        if nlink == 1:
            log.info(f"No hard links found for inode {inode_number}.")
        else:
            log.info(
                f"Hard links found for inode {inode_number}. Total links: {nlink} (Includes original file)"
            )

        # Retrieve referent inodes
        referent_inodes = inode_data.get("referent_inodes", [])
        if referent_inodes:
            log.info(f"Referent inodes for {inode_number}: {referent_inodes}")

        return inode_data  # Returning full inode data instead of just referent inodes

    def get_referent_inode_details(
        self, client, fs_name, source_inode, source_referent_inode_number, mount_dir
    ):
        """
        Retrieves referent inode details and validates linkage to the original file.

        :param client: The client object to execute commands.
        :param fs_name: The CephFS name.
        :param source_referent_inode_number: The inode number whose referent details need to be checked.
        :param mount_dir: The mount directory path to validate.
        :return: Dictionary containing inode details.
        """
        cmd = f"ceph tell mds.{fs_name}:0 dump inode {source_referent_inode_number} -f json"
        out, rc = client.exec_command(sudo=True, cmd=cmd)

        try:
            inode_data = json.loads(out.strip())  # Correctly parse JSON
        except json.JSONDecodeError:
            raise Exception(f"Failed to parse JSON output: {out}")

        # Fetch file path for the given inode
        hl_file_path = inode_data.get("path", "")
        log.info(f"File path for inode {source_referent_inode_number}: {hl_file_path}")

        # Validate that the referent inode is linked to the original file
        remote_ino = inode_data.get("remote_ino")

        if remote_ino:
            if remote_ino == source_inode:
                log.info(
                    f"Remote inode for {hl_file_path} matches the original file with inode: {source_inode}"
                )
            else:
                log.error(
                    f"Remote inode mismatch: Expected {source_referent_inode_number},\n"
                    "Found {remote_ino} for {hl_file_path}"
                )
                return 1

        return inode_data  # Returning the full inode details instead of just the inode number

    def create_hardlink(self, client, source_path, target_path):
        """
        Creates a hard link for a given file.

        Args:
            client (CephClient): The Ceph client node used to execute commands.
            file1_path (str): Path to the original file.
            hl_file_path (str): Path where the hard link will be created.

        Returns:
            bool: True if the hard link is created successfully, False otherwise.
        """

        try:
            client.exec_command(
                sudo=True, cmd=f"ln {source_path} {target_path}", check_ec=True
            )
            log.info(f"Successfully created hard link: {target_path}")
            return True

        except Exception as e:
            log.error(f"Exception while creating hard link: {e}")
            return False

    def allow_referent_inode_feature_enablement(self, clients, fs_name, enable=False):
        """
        Validate if 'allow_referent_inodes' is enabled or disabled.
        If disabled and enable=True, enable it and validate again.

        :param clients: Ceph client object to execute commands
        :param fs_name: Name of the Ceph filesystem
        :param enable: Boolean flag to enable the feature if disabled
        """
        out, _ = clients.exec_command(
            sudo=True,
            cmd=f"ceph fs get {fs_name} -f json",
            check_ec=False,
        )

        json_data = json.loads(out.strip())
        allow_referent_inodes = json_data["mdsmap"]["flags_state"].get(
            "allow_referent_inodes", False
        )

        if allow_referent_inodes:
            log.info(
                f"'allow_referent_inodes' is already enabled for filesystem {fs_name}."
            )
        else:
            log.info(f"'allow_referent_inodes' is disabled for filesystem {fs_name}.")
            if enable:
                log.info(f"Enabling 'allow_referent_inodes' for filesystem {fs_name}.")
                clients.exec_command(
                    sudo=True,
                    cmd=f"ceph fs set {fs_name} allow_referent_inodes true",
                    check_ec=True,
                )

                # Revalidate after enabling
                out, _ = clients.exec_command(
                    sudo=True,
                    cmd=f"ceph fs get {fs_name} -f json",
                    check_ec=False,
                )

                json_data = json.loads(out.strip())
                if json_data["mdsmap"]["flags_state"].get(
                    "allow_referent_inodes", False
                ):
                    log.info(
                        f"Successfully enabled 'allow_referent_inodes' for filesystem {fs_name}."
                    )
                else:
                    log.error(
                        f"Failed to enable 'allow_referent_inodes' for filesystem {fs_name}."
                    )
                    return 1
