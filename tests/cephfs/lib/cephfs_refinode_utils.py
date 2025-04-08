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

    def create_subvolumes(
        self,
        client,
        fs_util,
        default_fs,
        subvol_group_name,
        num_grouped=3,
        num_ungrouped=3,
    ):
        """
        Creates a subvolume group and multiple subvolumes, both within and outside the group.

        Args:
            client (CephClient): Ceph client node.
            fs_util (FsUtils): CephFS utility instance.
            default_fs (str): Default Ceph filesystem name.
            subvol_group_name (str): Subvolume group name.
            num_grouped (int): Number of subvolumes to create within the group.
            num_ungrouped (int): Number of subvolumes to create outside the group.

        Returns:
            list: Created subvolume names.
        """
        subvolumegroup = {"vol_name": default_fs, "group_name": subvol_group_name}
        fs_util.create_subvolumegroup(client, **subvolumegroup)

        subvolume_names = []

        # Create subvolumes within the group
        for i in range(1, num_grouped + 1):
            subvolume_name = "subvolume_ref_inode_grp_%s" % i
            subvolume_with_group = {
                "vol_name": default_fs,
                "subvol_name": subvolume_name,
                "group_name": subvolumegroup["group_name"],
                "size": "5368706371",
            }
            fs_util.create_subvolume(client, **subvolume_with_group)
            subvolume_names.append(subvolume_name)

        # Create subvolumes outside the group
        for i in range(1, num_ungrouped + 1):
            subvolume_name = "subvolume_ref_inode_nongrp_%s" % (i + num_grouped)
            subvolume_without_group = {
                "vol_name": default_fs,
                "subvol_name": subvolume_name,
                "size": "5368706371",
            }
            fs_util.create_subvolume(client, **subvolume_without_group)
            subvolume_names.append(subvolume_name)

        log.info("Captured Subvolume Names: %s", subvolume_names)
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
        Mounts CephFS subvolumes dynamically using Fuse, Kernel, or NFS.

        Returns:
            tuple: (fuse_mount_dirs, kernel_mount_dirs, nfs_mount_dirs)
        """
        mount_types = ["fuse", "kernel", "nfs"]
        export_created = 0
        total_subvols = len(subvolume_names)

        # Separate lists for different mount types
        fuse_mount_dirs = []
        kernel_mount_dirs = []
        nfs_mount_dirs = []

        for idx, subvol in enumerate(subvolume_names):
            mount_type = mount_types[idx % len(mount_types)]  # Rotate mount types

            # Get subvolume path
            if subvol_group_name and idx < total_subvols // 2:
                cmd = "ceph fs subvolume getpath %s %s %s" % (
                    default_fs,
                    subvol,
                    subvol_group_name,
                )
            else:
                cmd = "ceph fs subvolume getpath %s %s" % (default_fs, subvol)

            try:
                subvol_path, _ = client.exec_command(sudo=True, cmd=cmd)
                subvol_path = subvol_path.strip()
                log.info("Subvolume %s path: %s", subvol, subvol_path)
            except Exception as e:
                log.error("Failed to get path for subvolume %s: %s", subvol, e)
                continue  # Skip this subvolume instead of returning

            mount_params = {
                "client": client,
                "fs_util": fs_util,
                "fs_name": default_fs,
                "mnt_path": subvol_path,
                "export_created": export_created,
                "subvol_name": subvol,  # Adding subvolume name for mount suffix
            }

            if mount_type == "nfs":
                export_name = "/nfs_export_%s_%s" % (
                    subvol,
                    "".join(secrets.choice(string.digits) for _ in range(3)),
                )

                log.info("Creating NFS export for %s: %s", subvol, export_name)
                try:
                    fs_util.create_nfs_export(
                        client,
                        nfs_name,
                        export_name,
                        default_fs,
                        validate=True,
                        path=subvol_path,
                    )
                except Exception as e:
                    log.error("Failed creating NFS export for %s: %s", subvol, e)
                    return None, None, None  # Return failure

                mount_params.update(
                    {
                        "nfs_server": nfs_server,
                        "nfs_name": nfs_name,
                        "nfs_export_name": export_name,
                    }
                )

            mount_path, new_export_created = fs_util.mount_ceph(
                mount_type, mount_params
            )
            if not mount_path:
                log.error("Failed to mount %s using %s", subvol, mount_type)
                return None, None, None  # Return failure

            export_created = export_created or bool(
                new_export_created
            )  # Ensure boolean value
            mounting_path = "%s/" % mount_path.rstrip("/")

            # Store the mount path in the respective list
            if mount_type == "fuse":
                fuse_mount_dirs.append(mounting_path)
            elif mount_type == "kernel":
                kernel_mount_dirs.append(mounting_path)
            elif mount_type == "nfs":
                nfs_mount_dirs.append(mounting_path)

        return fuse_mount_dirs, kernel_mount_dirs, nfs_mount_dirs

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
        fuse_mount_dirs = []
        kernel_mount_dirs = []
        nfs_mount_dirs = []
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )

        # Fuse mount
        fuse_mount_dir = "/mnt/cephfs_root_fuse%s_1/" % mounting_dir
        fs_util.fuse_mount(
            [client], fuse_mount_dir, extra_params="--client_fs %s" % default_fs
        )
        fuse_mount_dirs.append(fuse_mount_dir)

        # Kernel mount
        kernel_mount_dir = "/mnt/cephfs_root_kernel%s_1/" % mounting_dir
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [client],
            kernel_mount_dir,
            ",".join(mon_node_ips),
            extra_params=",fs=%s" % default_fs,
        )
        kernel_mount_dirs.append(kernel_mount_dir)

        # NFS export creation and mount
        nfs_export_name = "/nfs_export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        try:
            fs_util.create_nfs_export(
                client, nfs_name, nfs_export_name, default_fs, path="/"
            )
        except Exception as e:
            log.error("Failed to create NFS export: %s" % str(e))
            return 1

        nfs_mounting_dir = "/mnt/cephfs_root_nfs_%s_1/" % mounting_dir
        if not fs_util.cephfs_nfs_mount(
            client, nfs_server, nfs_export_name, nfs_mounting_dir
        ):
            log.error("cephfs nfs export mount failed")
            return 1
        nfs_mount_dirs.append(nfs_mounting_dir)

        return fuse_mount_dirs, kernel_mount_dirs, nfs_mount_dirs

    def create_directories(self, client, base_path, dirs):
        """
        Creates multiple directories at the specified base path.

        Args:
            client: The Ceph client node used to execute commands.
            base_path (str): The base directory path where new directories will be created.
            dirs (list): List of directory names to create.
        """
        for directory in dirs:
            dir_path = "%s/%s" % (base_path, directory)
            client.exec_command(sudo=True, cmd="mkdir -p %s" % dir_path)
            log.info("Created directory: %s", dir_path)

    def create_file_with_content(self, client, file_path, content):
        """
        Creates a file at the specified path and writes content into it.

        Args:
            client: The Ceph client node used to execute commands.
            file_path (str): The full path of the file to be created.
            content (str): The content to write into the file.
        """
        client.exec_command(sudo=True, cmd='echo "%s" > %s' % (content, file_path))
        log.info("Created file with content: %s", file_path)

    def append_to_file(self, client, file_path, content):
        """
        Appends content to an existing file.

        Args:
            client: The Ceph client node used to execute commands.
            file_path (str): The full path of the file to be modified.
            content (str): The content to append to the file.
        """
        client.exec_command(sudo=True, cmd='echo "%s" >> %s' % (content, file_path))
        log.info("Appended content to file: %s", file_path)

    def rename_file(self, client, old_path, new_path):
        """
        Renames or moves a file from old_path to new_path.

        Args:
            client: The Ceph client node used to execute commands.
            old_path (str): The current file path.
            new_path (str): The new file path.
        """
        client.exec_command(sudo=True, cmd="mv %s %s" % (old_path, new_path))
        log.info("Renamed file from %s to %s", old_path, new_path)

    def create_snapshot_on_dir(self, client, dir_path, snapshot_name):
        """
        Creates a snapshot of a given directory.

        Args:
            client: The Ceph client node used to execute commands.
            dir_path (str): The directory path where the snapshot will be created.
            snapshot_name (str): The name of the snapshot.
        """
        snap_path = "%s/.snap/%s" % (dir_path, snapshot_name)
        client.exec_command(sudo=True, cmd="mkdir -p %s" % snap_path)
        log.info("Created snapshot: %s", snap_path)

    def get_rados_object_for_dir(self, client, meta_pool_name, dir_path):
        """
        Get the RADOS object ID for a given directory in CephFS.
        Args:
            client (CephClient): Ceph client node.
            meta_pool_name (str): Metadata pool name.
            dir_path (str): Directory path in CephFS.
        Returns:
            str: RADOS object ID if found, else None.
        """
        try:
            stdout, stderr = client.exec_command(
                sudo=True, cmd="stat -c %i %s" % dir_path
            )
            inode_number = stdout.strip()
            log.info("Inode number for %s: %s" % (dir_path, inode_number))

            inode_hex = format(int(inode_number), "x").zfill(8)
            rados_object_id = "%s.00000000" % inode_hex
            log.info(
                "Expected RADOS object ID for %s: %s" % (dir_path, rados_object_id)
            )

            stdout, stderr = client.exec_command(
                sudo=True,
                cmd="rados -p %s ls | egrep '([0-9]|[a-f]){11}'" % meta_pool_name,
            )
            rados_objects = stdout.splitlines()
            log.info("Objects in pool %s: %s" % (meta_pool_name, rados_objects))

            if rados_object_id in rados_objects:
                log.info("RADOS object for %s found: %s" % (dir_path, rados_object_id))
                return rados_object_id
            else:
                log.error("RADOS object for %s not found!" % dir_path)
                return None
        except Exception as e:
            raise Exception(
                "Failed to retrieve RADOS object for %s: %s" % (dir_path, e)
            )

    def get_rados_object_from_datapool(self, client, data_pool_name):
        """
        List RADOS objects in the given data pool.
        Args:
            client (CephClient): Ceph client node.
            data_pool_name (str): Data pool name.
        Returns:
            list: RADOS object names in the pool.
        """
        stdout, stderr = client.exec_command(
            sudo=True, cmd="rados -p %s ls" % data_pool_name
        )
        log.info("List Rados Objects : %s" % stdout)
        rados_objects = stdout.splitlines()
        log.info("Objects in pool %s: %s" % (data_pool_name, rados_objects))
        return rados_objects

    def list_snapshots_for_object(self, client, data_pool_name, rados_object):
        """
        Get snapshots for a given RADOS object.
        Args:
            client (CephClient): Ceph client node.
            data_pool_name (str): Data pool name.
            rados_object (str): RADOS object name.
        Returns:
            str: Snapshot count with JSON output.
        """
        cmd = "rados -p %s listsnaps %s -f json" % (data_pool_name, rados_object)
        stdout, stderr = client.exec_command(sudo=True, cmd=cmd)

        data = json.loads(stdout)
        clones = data.get("clones", [])

        if (
            len(clones) == 1
            and clones[0].get("id") == "head"
            and not clones[0].get("snapshots")
        ):
            return "No new snapshots created.\nOutput:\n%s" % json.dumps(data, indent=4)

        snapshot_count = sum(
            len(clone.get("snapshots", []))
            for clone in clones
            if clone.get("id") != "head"
        )

        return "%d snapshot(s) created.\nOutput:\n%s" % (
            snapshot_count,
            json.dumps(data, indent=4),
        )

    def create_hardlink_and_validate(
        self, client, fs_util, file1_path, hl_file_path, data_pool_name, fs_name
    ):
        """
        Creates a hard link, validates inode numbers, and checks for new RADOS objects.
        """

        def exec_stat(file_path):
            """Executes stat command and extracts inode number."""
            stdout, stderr = client.exec_command(sudo=True, cmd="stat %s" % file_path)
            log.info("File statistics for %s:\n%s" % (file_path, stdout))
            match = re.search(r"Inode:\s+(\d+)", stdout)
            return match.group(1) if match else None

        # Step 1: Capture initial RADOS objects
        initial_rados_objects = set(
            self.get_rados_object_from_datapool(client, data_pool_name)
        )
        log.info("Initial RADOS objects: %s" % initial_rados_objects)

        # Step 2: Create hard link
        client.exec_command(sudo=True, cmd="ln %s %s" % (file1_path, hl_file_path))
        log.info("Created hardlink: %s" % hl_file_path)

        # Step 3: Flush journal
        self.flush_journal_on_active_mdss(fs_util, client, fs_name)

        # Step 4: Compare Inode to validate hardlinks
        inode1, inode2 = exec_stat(file1_path), exec_stat(hl_file_path)
        if inode1 and inode2 and inode1 == inode2:
            log.info("Validation Passed: Inodes match (%s)" % inode1)
        else:
            log.error(
                "Validation Failed: Inodes do not match (%s != %s)" % (inode1, inode2)
            )
            return False, "no", None

        # Step 5: Capture RADOS objects after creating hardlink
        post_rados_objects = set(
            self.get_rados_object_from_datapool(client, data_pool_name)
        )
        log.info("RADOS objects after creating hardlink: %s" % post_rados_objects)

        # Step 6: Identify the newly created RADOS object
        new_rados_objects = post_rados_objects - initial_rados_objects
        log.info("Newly created RADOS objects: %s" % new_rados_objects)

        # Step 7: Validate referent inode
        referent_inode = "yes" if len(new_rados_objects) > 0 else "no"
        log.info("Referent Inode: %s" % referent_inode)

        return True, referent_inode, new_rados_objects

    def flush_journal_on_active_mdss(self, fs_util, client, fs_name):
        """
        Flushes the journal on active MDS nodes.
        """
        active_mdss = fs_util.get_active_mdss(client, fs_name)
        for mds in active_mdss:
            client.exec_command(
                sudo=True, cmd="ceph tell mds.%s flush journal > /dev/null 2>&1" % mds
            )
            log.info("Journal flushed on MDS: mds.%s" % mds)

    def validate_referent_inode(self, fs_util, client, mount_path, data_pool, fs_name):
        """
        Validates referent inodes by creating directories, files, snapshots, and hardlinks.
        """
        log.info("Starting validation of the referent inode automation...")

        # Step 1: Create directories
        dirs = ["dir1", "dir2"]
        self.create_directories(client, mount_path, dirs)
        self.flush_journal_on_active_mdss(fs_util, client, fs_name)

        # Step 2: Create files and snapshots in dir1
        file1_path = "%s/dir1/file1" % mount_path
        self.create_file_with_content(client, file1_path, "first file")
        self.flush_journal_on_active_mdss(fs_util, client, fs_name)
        object_id = self.get_rados_object_from_datapool(client, data_pool)
        object_id_dir1 = object_id[0]
        snapshot_details = self.list_snapshots_for_object(
            client, data_pool, object_id_dir1
        )
        log.info("Snapshot Details : %s" % snapshot_details)

        # Step 3 : Create Snapshot directly on dir path
        self.create_snapshot_on_dir(client, "%s/dir1" % mount_path, "snap1")
        self.append_to_file(client, file1_path, "first file second line")
        self.flush_journal_on_active_mdss(fs_util, client, fs_name)
        snapshot_details = self.list_snapshots_for_object(
            client, data_pool, object_id_dir1
        )
        log.info("Snapshot Details on Dir1 : %s" % snapshot_details)

        # Step 4: Create hardlink and validate
        file1_path = "%s/dir1/file1" % mount_path
        hl_file_path = "%s/dir2/hl_file1" % mount_path
        validation_result, referent_inode, new_rados_objects = (
            self.create_hardlink_and_validate(
                client, fs_util, file1_path, hl_file_path, data_pool, fs_name
            )
        )
        log.info(
            "Validation Result: %s, Referent Inode exists : %s : %s"
            % (validation_result, referent_inode, new_rados_objects)
        )
        log.info("Validation of presence of Referent Inode completed successfully!")

    def cleanup_snap_directories(self, client, mount_path):
        """
        Cleans up snapshot directories and files within the mount path.
        """
        log.info("Cleaning up snapshots within %s..." % mount_path)
        try:
            client.exec_command(sudo=True, cmd="rmdir %s/*/.snap/*" % mount_path)
            client.exec_command(sudo=True, cmd="rm -rf %s/*" % mount_path)
        except Exception as e:
            log.error(
                "Failed to clean up .snap directories within %s: %s" % (mount_path, e)
            )
            return 1
        log.info("Cleanup of snap directories completed.")

    def get_inode_number(self, client, file_path):
        """Gets the inode number of a file."""
        cmd = "stat -c %s %s" % ("%i", file_path)
        stdout, stderr = client.exec_command(sudo=True, cmd=cmd)

        inode_number = stdout.strip()
        if not inode_number.isdigit():
            raise Exception("Failed to get inode number: %s %s" % (stdout, stderr))

        return int(inode_number)

    def get_inode_details(self, client, fs_name, inode_number, mount_dir):
        """Gets inode details and checks hard links."""
        cmd = "ceph tell mds.%s:0 dump inode %s -f json" % (fs_name, inode_number)
        out, rc = client.exec_command(sudo=True, cmd=cmd)

        try:
            inode_data = json.loads(out.strip())
        except json.JSONDecodeError:
            raise Exception("Failed to parse JSON output: %s" % out)

        file_path = inode_data.get("path", "")
        log.info("File path for inode %s: %s" % (inode_number, file_path))

        nlink = inode_data.get("nlink", 1)
        if nlink == 1:
            log.info("No hard links found for inode %s." % inode_number)
        else:
            log.info(
                "Hard links found for inode %s. Total links: %s" % (inode_number, nlink)
            )

        referent_inodes = inode_data.get("referent_inodes", [])
        if referent_inodes:
            log.info("Referent inodes for %s: %s" % (inode_number, referent_inodes))

        return inode_data

    def get_referent_inode_details(
        self, client, fs_name, source_inode, source_referent_inode_number, mount_dir
    ):
        """Gets referent inode details and validates linkage."""
        cmd = "ceph tell mds.%s:0 dump inode %s -f json" % (
            fs_name,
            source_referent_inode_number,
        )
        out, rc = client.exec_command(sudo=True, cmd=cmd)

        try:
            inode_data = json.loads(out.strip())
        except json.JSONDecodeError:
            raise Exception("Failed to parse JSON output: %s" % out)

        hl_file_path = inode_data.get("path", "")
        log.info(
            "File path for inode %s: %s" % (source_referent_inode_number, hl_file_path)
        )

        remote_ino = inode_data.get("remote_ino")
        if remote_ino:
            if remote_ino == source_inode:
                log.info(
                    "Remote inode for %s matches original inode %s"
                    % (hl_file_path, source_inode)
                )
            else:
                log.error(
                    "Remote inode mismatch: Expected %s, Found %s for %s"
                    % (source_referent_inode_number, remote_ino, hl_file_path)
                )
                return 1

        return inode_data

    def create_hardlink(self, client, source_path, target_path):
        """
        Creates a hard link.
        """
        try:
            client.exec_command(
                sudo=True, cmd="ln %s %s" % (source_path, target_path), check_ec=True
            )
            log.info("Successfully created hard link: %s" % target_path)
            return True
        except Exception as e:
            log.error("Exception while creating hard link: %s" % e)
            return False

    def allow_referent_inode_feature_enablement(self, clients, fs_name, enable=False):
        """
        Checks if 'allow_referent_inodes' is enabled. Enables if needed.
        """
        out, _ = clients.exec_command(
            sudo=True,
            cmd="ceph fs get %s -f json" % fs_name,
            check_ec=False,
        )

        json_data = json.loads(out.strip())
        allow_referent_inodes = json_data["mdsmap"]["flags_state"].get(
            "allow_referent_inodes", False
        )

        if allow_referent_inodes:
            log.info(
                "'allow_referent_inodes' is already enabled for filesystem %s."
                % fs_name
            )
        else:
            log.info("'allow_referent_inodes' is disabled for filesystem %s." % fs_name)
            if enable:
                log.info(
                    "Enabling 'allow_referent_inodes' for filesystem %s." % fs_name
                )
                clients.exec_command(
                    sudo=True,
                    cmd="ceph fs set %s allow_referent_inodes true" % fs_name,
                    check_ec=True,
                )

                # Revalidate after enabling
                out, _ = clients.exec_command(
                    sudo=True,
                    cmd="ceph fs get %s -f json" % fs_name,
                    check_ec=False,
                )

                json_data = json.loads(out.strip())
                if json_data["mdsmap"]["flags_state"].get(
                    "allow_referent_inodes", False
                ):
                    log.info(
                        "Successfully enabled 'allow_referent_inodes' for filesystem %s."
                        % fs_name
                    )
                else:
                    log.error(
                        "Failed to enable 'allow_referent_inodes' for filesystem %s."
                        % fs_name
                    )
                    return 1

    def fetch_all_hardlinks(self, client, fs_name, path, mount_dir):
        """
        Fetch all hard links for a file.
        """
        try:
            inode_number = self.get_inode_number(client, path)
            inode_data = self.get_inode_details(
                client, fs_name, inode_number, mount_dir
            )
            referent_inodes = inode_data.get("referent_inodes", [])

            hardlinks = []
            for ref_inode in referent_inodes:
                ref_inode_data = self.get_inode_details(
                    client, fs_name, ref_inode, mount_dir
                )
                if ref_inode_data and "path" in ref_inode_data:
                    hardlinks.append(ref_inode_data["path"])

            return hardlinks
        except Exception as e:
            log.error("Failed to fetch hard links: %s", e)
            return []

    def unlink_hardlinks(self, client, fs_name, file_path, mount_dir):
        """
        Unlink all hardlinks of a file.
        """
        log.info("Fetching hardlinks for file: %s", file_path)

        hardlinks = self.fetch_all_hardlinks(client, fs_name, file_path, mount_dir)

        if not hardlinks:
            log.info("No hardlinks found for %s", file_path)
            return

        log.info("Found %d hardlinks for %s: %s", len(hardlinks), file_path, hardlinks)

        for link in hardlinks:
            full_path = "%s/%s" % (mount_dir.rstrip("/"), link.lstrip("/"))
            cmd = "rm -f %s" % full_path
            client.exec_command(sudo=True, cmd=cmd)
            log.info("Unlinked hardlink: %s", full_path)

        log.info("Successfully unlinked all hardlinks for %s.", file_path)
