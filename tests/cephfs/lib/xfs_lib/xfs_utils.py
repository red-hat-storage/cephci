"""
XFS Test Setup for CephFS using xfstests
This script sets up the environment for running XFS tests on CephFS using xfstests

This "tests/cephfs/lib/xfs_lib/xfs_test_list.json" JSON file lists the
    kernel/fuse test cases that are relevant for XFS testing.
Based on the analysis:
https: //docs.google.com/spreadsheets/d/15N5vT6Lot9O5oJPsJ63pgMopCYPDagyvuGVaCtCCaVI/edit?usp=sharing

Kernel Test:
    Total available test cases: 766
    Total not run test cases: 454
    Total excluded test cases: 4(generic/003(Not Run), generic/075, generic/538, generic/531(Not Run))
    Total test cases to run: 310 (766-454-2)
    Test passed in git-repo but failed in ceph-git-repo: 111
    Total test cases to run: 199 (310-111)
"""

import json
import os

from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from utility.log import Log

log = Log(__name__)


class XfsTestSetup:
    """
    Class to set up and run XFS tests on CephFS using xfstests
    """

    def __init__(self, ceph_cluster, client):
        """
        Initialize the XfsTestSetup class with the Ceph cluster and client node
        Args:
            ceph_cluster: The Ceph cluster object
            client: The client node where tests will be run
        """
        self.ceph_cluster = ceph_cluster
        self.client = client
        self.repo = "https://git.ceph.com/xfstests-dev.git"
        self.fs_util = FsUtils(ceph_cluster)
        self.cephfs_common_utils = CephFSCommonUtils(ceph_cluster)
        self.mon_node_ips = self.fs_util.get_mon_node_ips()
        self.subscription_status = self.cephfs_common_utils.is_subscription_registered(
            client
        )

    def setup_environment(self):
        """
        Set up the environment for running XFS tests
        This includes installing required packages and adding users for xfs test
        """
        # Install EPEL based on RHEL version
        rhel_version, _ = self.client.exec_command(
            sudo=True, cmd="cat /etc/os-release | grep VERSION_ID"
        )
        log.debug("RHEL version: %s", rhel_version)

        subscription_cmd = None
        if 'VERSION_ID="8' in rhel_version:
            epel_cmd = "dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm"
        elif 'VERSION_ID="9' in rhel_version:
            epel_cmd = "dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm"
            subscription_cmd = "subscription-manager repos --enable codeready-builder-for-rhel-9-$(arch)-rpms"
        elif 'VERSION_ID="10' in rhel_version:
            epel_cmd = "dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-10.noarch.rpm"
            subscription_cmd = "subscription-manager repos --enable codeready-builder-for-rhel-10-$(arch)-rpms"
        else:
            log.error("Unsupported RHEL version")
            return 1

        if self.subscription_status and subscription_cmd:
            log.info("Enabling CodeReady Builder repo...")
            self.client.exec_command(sudo=True, cmd=subscription_cmd)
        else:
            log.info("Skipping enabling CodeReady Builder repo (system not subscribed)")

        log.info("Installing EPEL release...")
        self.client.exec_command(sudo=True, cmd=epel_cmd)

        # Install dependencies
        packages = (
            "acl attr automake bc dbench e2fsprogs fio gawk gcc gdbm-devel git "
            "indent kernel-devel libacl-devel libaio-devel libcap-devel libtool libuuid-devel "
            "lvm2 make psmisc python3 quota sed sqlite xfsprogs xfsprogs-devel"
        )
        log.info("Installing required packages for xfstests...")
        self.client.exec_command(sudo=True, cmd=f"yum install -y {packages}")

        log.info("Adding users for xfs test...")
        self.client.exec_command(sudo=True, cmd="useradd -m fsgqa", check_ec=False)
        self.client.exec_command(sudo=True, cmd="useradd -m fsgqa2", check_ec=False)
        return 0

    def clone_and_build_xfstests(self):
        """
        Clone the xfstests repository and build it
        """
        try:
            self.client.exec_command(sudo=True, cmd="rm -rf /root/xfstests-dev")
            self.client.exec_command(
                sudo=True, cmd=f"cd /root/ && git clone {self.repo}"
            )
            self.client.exec_command(
                sudo=True, cmd="cd /root/xfstests-dev && make && make install"
            )
        except Exception as e:
            log.error("Failed to clone or build xfstests: {}".format(e))
            return 1
        return 0

    def mount_fs(self, mount_info):
        """
        Mount the CephFS filesystem for testing
        Args:
            mount_info: Dictionary containing mount information
        Sample mount_info:
        {
            "test_mount": "/mnt/test",
            "scratch_mount": "/mnt/scratch",
            "mount_type": "kernel",  # or "fuse"
            "FSTYP": "ceph",  # ceph for kernel, ceph-fuse for fuse
            "fs_name": "cephfs",  # Name of the CephFS
            "test_dev": "test_dev_a",  # Device for test mount
            "scratch_dev": "scratch_dev_b"  # Device for scratch mount
        }
        """
        try:
            if mount_info["mount_type"] == "kernel":
                self.fs_util.kernel_mount(
                    [self.client],
                    mount_info["test_mount"],
                    ",".join(self.mon_node_ips),
                    extra_params=",fs={}".format(mount_info["fs_name"]),
                )
                self.fs_util.kernel_mount(
                    [self.client],
                    mount_info["scratch_mount"],
                    ",".join(self.mon_node_ips),
                    extra_params=",fs={}".format(mount_info["fs_name"]),
                )
            elif mount_info["mount_type"] == "fuse":
                self.fs_util.fuse_mount(
                    [self.client],
                    mount_info["test_mount"],
                    extra_params=" --client_fs {}".format(mount_info["fs_name"]),
                )
                self.fs_util.fuse_mount(
                    [self.client],
                    mount_info["scratch_mount"],
                    extra_params=" --client_fs {}".format(mount_info["fs_name"]),
                )
        except Exception as e:
            log.error("Failed to mount CephFS: {}".format(e))
            return 1
        return 0

    def configure_local_config(self, mount_info):
        """
        Configure the local.config file for xfstests
        Args:
            mount_info: Dictionary containing mount information
        Sample mount_info:
        {
            "test_mount": "/mnt/test",
            "scratch_mount": "/mnt/scratch",
            "test_dev": "test_dev_a",
            "scratch_dev": "scratch_dev_b",
            "FSTYP": "ceph",  # or "ceph-fuse"
            "fs_name": "cephfs"  # Name of the CephFS
        }
        """
        try:
            self.client.exec_command(
                sudo=True,
                cmd=f"mkdir -p {mount_info['test_mount']}/{mount_info['test_dev']}",
            )
            self.client.exec_command(
                sudo=True,
                cmd=f"mkdir -p {mount_info['scratch_mount']}/{mount_info['scratch_dev']}",
            )
            # get admin key
            admin_key, _ = self.client.exec_command(
                sudo=True, cmd="ceph auth get-key client.admin"
            )

            # Prepare the local.config file for kernel or fuse mount
            if mount_info["mount_type"] == "kernel":
                config = f"""
            export TEST_DEV={self.mon_node_ips[0]}:/{mount_info["test_dev"]}
            export SCRATCH_DEV={self.mon_node_ips[1]}:/{mount_info["scratch_dev"]}
            """
            elif mount_info["mount_type"] == "fuse":
                config = f"""
            export TEST_DEV={mount_info["test_dev"]}
            export SCRATCH_DEV=""
            """

            config += f"""
            export TEST_DIR={mount_info["test_mount"]}
            export SCRATCH_MNT={mount_info["scratch_mount"]}
            export FSTYP={mount_info["FSTYP"]}

            COMMON_OPTIONS="name=admin"
            COMMON_OPTIONS+=",secret={admin_key}"

            TEST_FS_MOUNT_OPTS="-o ${{COMMON_OPTIONS}}"
            MOUNT_OPTIONS="-o ${{COMMON_OPTIONS}}"

            export TEST_FS_MOUNT_OPTS
            export MOUNT_OPTIONS
            """
            self.client.exec_command(
                sudo=True, cmd=f"echo '{config}' > /root/xfstests-dev/local.config"
            )
        except Exception as e:
            log.error("Failed to configure local.config: {}".format(e))
            return 1
        return 0

    def run_tests(self, mount_type):
        """
        Run the xfstests on the mounted CephFS
        Args:
            mount_type: Type of mount, either "kernel" or "fuse"
        """
        try:
            json_path = os.path.join(
                os.getcwd(), "tests/cephfs/lib/xfs_lib/xfs_test_list.json"
            )
            with open(json_path, "r") as f:
                data = json.load(f)
            if mount_type == "kernel":
                test_lists = data.get("kernel_tests_to_run", [])
            elif mount_type == "fuse":
                test_lists = data.get("fuse_tests_to_run", [])
            for test in test_lists:
                log.info("Running xfstests for test: %s", test)
                out, _ = self.client.exec_command(
                    sudo=True,
                    cmd="cd /root/xfstests-dev && ./check -d -T {}".format(test),
                    check_ec=False,
                    timeout=7200,
                )
                log.info("Console Log:\n%s", out)

            len_failed_test, len_not_run_test = 0, 0

            failed_tests, _ = self.client.exec_command(
                sudo=True,
                cmd=r"find /root/xfstests-dev/results/generic -name '*.out.bad' -exec basename -s .out.bad {} \;",
                check_ec=False,
            )
            if failed_tests:
                len_failed_test = len(failed_tests.strip().splitlines())

            not_run_tests, _ = self.client.exec_command(
                sudo=True,
                cmd=r"find /root/xfstests-dev/results/generic -name '*.notrun' -exec basename -s .notrun {} \;",
                check_ec=False,
            )
            if not_run_tests:
                len_not_run_test = len(not_run_tests.strip().splitlines())

            log.info("Execution summary for %s:", mount_type)
            log.info("---------------------------------------")
            log.info("Total tests run: %d", len(test_lists))
            log.info(
                "Total Passed tests: %d",
                len(test_lists) - len_failed_test - len_not_run_test,
            )
            log.info("Total Failed tests: %d", len_failed_test)
            log.info("Total Not Run tests: %d", len_not_run_test)
            log.info("----------------------------------------")

            if len_not_run_test > 0:
                log.info("Detailed Summary of Not Run test %s:", mount_type)
                log.info("-----------------------------------")
                out, _ = self.client.exec_command(
                    sudo=True,
                    cmd="cd /root/xfstests-dev && grep -r '' results/generic/*.notrun",
                )
                log.info(out)

            if len_failed_test > 0:
                log.info("Detailed Summary of Failed test %s:", mount_type)
                log.info("------------------------------------")
                out, _ = self.client.exec_command(
                    sudo=True,
                    cmd="cd /root/xfstests-dev && grep -r '' results/generic/*.out.bad",
                )
                log.info(out)

        except Exception as e:
            log.error("Failed to run xfstests: {}".format(e))
            return 1
        return len_failed_test

    def cleanup(self, mount_info):
        """
        Clean up the mounted directories and xfstests setup
        Args:
            mount_info: Dictionary containing mount information
        Sample mount_info:
        {
            "test_mount": "/mnt/test",
            "scratch_mount": "/mnt/scratch",
            "test_dev": "test_dev_a",
            "scratch_dev": "scratch_dev_b",
            "FSTYP": "ceph",  # or "ceph-fuse"
            "fs_name": "cephfs"  # Name of the CephFS
        }
        """
        try:
            self.client.exec_command(
                sudo=True,
                cmd="umount -f {}".format(mount_info["test_mount"]),
                check_ec=False,
            )
            self.client.exec_command(
                sudo=True,
                cmd="umount -f {}".format(mount_info["scratch_mount"]),
                check_ec=False,
            )
            self.client.exec_command(
                sudo=True,
                cmd="rm -rf {} {} /root/xfstests-dev".format(
                    mount_info["test_mount"], mount_info["scratch_mount"]
                ),
            )
            self.client.exec_command(sudo=True, cmd="userdel -r fsgqa")
            self.client.exec_command(sudo=True, cmd="userdel -r fsgqa2")
        except Exception as e:
            log.error("Failed to clean up: {}".format(e))
            return 1
        return 0
