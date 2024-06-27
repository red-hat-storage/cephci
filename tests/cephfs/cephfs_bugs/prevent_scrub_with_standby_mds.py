import random
import string
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

"""
Testing description:
Verify that the standby mds is not allowed to run scrub
Steps to Reproduce:
1. create directories in the mounted client
2. set max_mds to 1
3. set allow_standby_replay
4. wait the standby mds comes up
5. get the standby mds
6. run scrub with standby mds
7. it should not let the standby mds run the scrub
8. If it returns 0, then the test case fails
"""


def run(ceph_cluster, **kw):
    try:
        tc = "62537"
        log.info(f"Running CephFS tests for ceph tracker - {tc}")
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
        # Check if jq package is installed, install if not
        jq_check = client1.exec_command(
            sudo=True, cmd="rpm -qa | grep jq", check_ec=False
        )
        if "jq" not in jq_check:
            client1.exec_command(sudo=True, cmd="yum install -y jq")
        # Generate random string for directory names
        rand = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(5)
        )
        # Define mount directories
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse_{rand}"
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel_{rand}"
        # Mount CephFS using ceph-fuse and kernel
        fs_util.fuse_mount([client1], fuse_mounting_dir_1)
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount([client1], kernel_mounting_dir_1, ",".join(mon_node_ips))
        # create directories in the mounted directory
        num_dir = 100
        for i in range(num_dir):
            client1.exec_command(
                sudo=True, cmd=f"mkdir {fuse_mounting_dir_1}/dir_fuse_{i}"
            )
            client1.exec_command(
                sudo=True, cmd=f"mkdir {kernel_mounting_dir_1}/dir_kernel_{i}"
            )
        # set max_mad to 1
        client1.exec_command(sudo=True, cmd="ceph fs set cephfs max_mds 1")
        # set allow standby mds to true
        client1.exec_command(
            sudo=True, cmd="ceph fs set cephfs allow_standby_replay true"
        )
        # wait until standby mds is created
        standby_mds_cmd = (
            "ceph fs status cephfs -f json-pretty | jq -r '.mdsmap[] "
            '| select(.state == "standby-replay") | .name\''
        )
        standby_mds, _ = client1.exec_command(sudo=True, cmd=standby_mds_cmd)
        standby_mds = standby_mds.rstrip()
        # run scrub with standby mds
        log.info(f"standby mds:{standby_mds}")
        scrub_cmd = f"ceph tell mds.{standby_mds} scrub start /"
        scrub_output, _ = client1.exec_command(sudo=True, cmd=scrub_cmd, check_ec=False)
        log.info(f"scrub result:{scrub_output}")
        if '"return_code": 0' in scrub_output:
            log.error("With standby mds, scrub should not run")
            log.error("https://bugzilla.redhat.com/show_bug.cgi?id=2294478")
            log.error("https://tracker.ceph.com/issues/62537")
            log.error("this case is expected to fail due to open BZ 2294478")
            return 1
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        # Cleanup
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", kernel_clients=[clients[0]], mounting_dir=kernel_mounting_dir_1
        )
