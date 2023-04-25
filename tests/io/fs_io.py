import random
import string

from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.io.io_utils import start_io
from utility.log import Log

log = Log(__name__)


class fs_io:
    """
    This class is a tool for running different IOs on Ceph File System

    Functionalities:
    1. Creates the File system if not present
    2. Mounts the file system on the client nodes supports both fuse and Kernel
    3. Runs the SmallFile IOs on the mounted directory

    Supported IO Tools :
    SmallFile
    """

    def __init__(self, client, fs_config, fs_util):
        self.client = client
        self.file_system = fs_config.get("filesystem", "cephfs")
        self.fill_data = fs_config.get("fill_data", 20)
        self.fsutils = fs_util
        self.timeout = fs_config.get("timeout", -1)
        self.io_tool = fs_config.get("io_tool")
        self.pool = ""
        self.mounting_dir = fs_config.get("mounting_dir", None)
        if not self.mounting_dir:
            self.mounting_dir = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(10))
            )
        fs_details = FsUtils.get_fs_info(client, self.file_system)
        if not fs_details:
            self.fsutils.create_fs(client=client, vol_name=self.file_system)
        if self.io_tool == "smallfile":
            self.client.exec_command(
                cmd="git clone https://github.com/bengland2/smallfile.git",
                check_ec=False,
            )
        self.mount = fs_config.get("mount", "fuse")

    def mount_fs(self):
        """
        Creates the directory Mounting Directory in the client.
        mount the file system

        Returns:
        Mounting Directory
        """
        mounting_dir_1 = f"/mnt/cephfs_io_{self.mounting_dir}_1/"
        if self.mount == "fuse":
            self.fsutils.fuse_mount(
                fuse_clients=[self.client],
                mount_point=mounting_dir_1,
                extra_params=f"--client_fs {self.file_system}",
                new_client_hostname="admin",
            )
        elif self.mount == "kernel":
            mon_node_ips = self.fsutils.get_mon_node_ips()
            self.fsutils.kernel_mount(
                [self.client],
                mounting_dir_1,
                ",".join(mon_node_ips),
                extra_params=f",fs={self.file_system}",
                fstab=True,
            )
        else:
            pass

        return mounting_dir_1

    def run_fs_io(self, **kwargs):
        """
        Triggers IO n the filesystem mounted
        """
        log.info("Create all the pre-requisties and mount")
        fs_details = FsUtils.get_fs_info(self.client, self.file_system)
        self.pool = fs_details["data_pool_name"]
        mounting_dir = self.mount_fs()
        start_io(
            self, mounting_dir, docker_compose=True, compose_file="fs_compose.yaml"
        )
