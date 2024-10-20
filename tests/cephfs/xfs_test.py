import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from ceph.ceph import SocketTimeoutException

log = Log(__name__)

"""
Testing description:

Running XFS tests on the client node and run the tests

Steps to Reproduce:

1. Install required packages for xfstests
2. Mount the kernel clients
3. Run the xfstests on the clients
4. Verify the results of the tests
"""


def run(ceph_cluster, **kw):
    try:
        log.info("Running CephFS tests for ceph kernel xfstests")
        # Initialize the utility class for CephFS
        fs_util = FsUtils(ceph_cluster)
        # Get the client nodes
        clients = ceph_cluster.get_ceph_objects("client")
        config = kw.get("config")
        # Authenticate the clients
        fs_util.auth_list(clients)
        build = config.get("build", config.get("rhbuild"))
        # Prepare the clients
        fs_util.prepare_clients(clients, build)
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        # check if rhel version is 8 or 9
        rhel_version, _ = client1.exec_command(
            sudo=True, cmd="cat /etc/os-release | grep VERSION_ID"
        )
        log.info("Installing epel-release depending on the RHEL version")
        if rhel_version.startswith('VERSION_ID="8'):
            client1.exec_command(
                sudo=True,
                cmd="dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm",
            )
        elif rhel_version.startswith('VERSION_ID="9'):
            client1.exec_command(
                sudo=True,
                cmd="dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm",
            )

        commands = [
            "yum install -y acl attr automake bc dbench dump e2fsprogs fio gawk gcc gdbm-devel"
            " git indent kernel-devel libacl-devel libaio-devel libcap-devel libtool libuuid-devel"
            " lvm2 make psmisc python3 quota sed sqlite udftools xfsprogs",
            "dnf install -y glib2-devel readline-devel ncurses-devel e2fsprogs-devel gcc-c++ autoconf",
            "git clone https://github.com/axboe/liburing.git",
            "cd liburing && ./configure && make && make install && cd ..",
            "yum install -y xfsdump xfsprogs-devel",
            "yum install -y exfatprogs autoconf",
            "git clone https://git.kernel.org/pub/scm/linux/kernel/git/jaegeuk/f2fs-tools.git",
            "cd f2fs-tools && ./configure && make && make install && cd ..",
            "git clone https://github.com/markfasheh/ocfs2-tools.git",
            "cd ocfs2-tools && ./autogen.sh && ./configure && make && make install && cd ..",
            "yum install -y libacl-devel bc libtool",
            "git clone git://git.kernel.org/pub/scm/fs/xfs/xfstests-dev.git",
            "cd xfstests-dev && make && sudo make install",
        ]
        log.info("Installing required packages for xfstests")
        for command in commands:
            client1.exec_command(sudo=True, cmd=command, check_ec=False)
        rand = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(5)
        )
        # Define mount directories
        log.info("Mounting only kernel clients for xfstests")
        test_mount_point = f"/mnt/cephfs_kernel_{rand}_test"
        scratch_mount_point = f"/mnt/cephfs_kernel_{rand}_scratch"
        # Mount CephFS
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount([client1], test_mount_point, ",".join(mon_node_ips))
        fs_util.kernel_mount([client1], scratch_mount_point, ",".join(mon_node_ips))
        # create subdirectory in the mounted directory
        client1.exec_command(sudo=True, cmd=f"mkdir {test_mount_point}/aa")
        client1.exec_command(sudo=True, cmd=f"mkdir {scratch_mount_point}/bb")
        # get admin key
        admin_key, _ = client1.exec_command(
            sudo=True, cmd="ceph auth get-key client.admin"
        )
        xfs_config_context = f"""
        export TEST_DIR={test_mount_point}
        export SCRATCH_MNT={scratch_mount_point}
        export FSTYP=ceph
        export TEST_DEV={mon_node_ips[0]}:/aa
        export SCRATCH_DEV={mon_node_ips[1]}:/bb

        COMMON_OPTIONS="name=admin"
        COMMON_OPTIONS+=",secret={admin_key}"

        TEST_FS_MOUNT_OPTS="-o ${{COMMON_OPTIONS}}"
        MOUNT_OPTIONS="-o ${{COMMON_OPTIONS}}"

        export TEST_FS_MOUNT_OPTS
        export MOUNT_OPTIONS
        """
        # write to file as local.config.
        client1.exec_command(
            sudo=True,
            cmd=f"echo '{xfs_config_context}' > /root/xfstests-dev/local.config",
        )
        # create exclude file
        exclude_file = "generic/003"
        client1.exec_command(
            sudo=True, cmd=f"echo '{exclude_file}' > /root/xfstests-dev/ceph.exclude"
        )
        # run the tests
        client1.exec_command(
            sudo=True,
            cmd="cd /root/xfstests-dev && ./check -d -T -g quick -e ceph.exclude",
            check_ec=False,
            long_running=True,
            timeout=7200
        )
        log.info("XFS tests completed successfully")
        return 0
    except Exception as e:
        dmesg, _ = clients[0].exec_command(sudo=True, cmd="dmesg")
        log.error(dmesg)
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    except SocketTimeoutException as ste:
        dmesg , _ = clients[0].exec_command(sudo=True, cmd="dmesg")
        log.error(dmesg)
        log.error(ste)
        return 1
    finally:
        uninstall_commands = [
            "dnf remove -y acl attr automake bc dbench dump e2fsprogs fio gawk gcc gdbm-devel git"
            " indent kernel-devel libacl-devel libaio-devel libcap-devel libtool libuuid-devel lvm2"
            " make psmisc python3 quota sed sqlite udftools xfsprogs",
            "dnf remove -y glib2-devel readline-devel ncurses-devel e2fsprogs-devel gcc-c++ autoconf",
            "dnf remove -y xfsdump xfsprogs-devel",
            "dnf remove -y exfatprogs autoconf",
            "dnf remove -y libuuid-devel libtool libselinux1-dev",
            "dnf remove -y libacl-devel bc libtool",
            "dnf remove -y epel-release",
        ]
        log.info("Uninstalling required packages for xfstests")
        for command in uninstall_commands:
            client1.exec_command(sudo=True, cmd=command, check_ec=False)
        remove_commands = [
            "cd /root && rm -rf liburing",
            "cd /root && rm -rf f2fs-tools",
            "cd /root && rm -rf ocfs2-tools",
            "cd /root && rm -rf xfstests-dev",
        ]
        log.info("Removing the directories created for xfstests)")
        for command in remove_commands:
            client1.exec_command(sudo=True, cmd=command, check_ec=False)
