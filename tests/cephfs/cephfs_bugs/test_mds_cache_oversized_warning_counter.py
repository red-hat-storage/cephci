import json
import random
import string
import time
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

"""
Testing description:

Testing the MDS cache counters are added
when the cache memory limit is reached and
the mds_health_cache_threshold is set to 1.000001.

Steps to Reproduce:
1. Create a CephFS filesystem.
2. Mount the CephFS filesystem using ceph-fuse.
3. Set the cache memory limit to 1K.
4. Set the mds_health_cache_threshold to 1.000001.
5. Set the max_mds to 1.
6. Set the allow_standby_replay to true.
7. Set the standby_count_wanted to 1.
8. Create a file in the mounted directory.
10. Check the health of the MDS.
11. Check the number of MDS cache counters.
"""


def run(ceph_cluster, **kw):
    try:
        tc = "CEPH-83594160"
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
        # Generate random string for directory names
        rand = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(5)
        )
        # Define mount directories
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse_{rand}"
        # Mount CephFS using ceph-fuse and kernel
        fs_util.fuse_mount([client1], fuse_mounting_dir_1)
        # cache limit 2 to 1K
        fs_util.config_set_runtime(client1, "mds", "mds_cache_memory_limit", 1024)
        # mds_health_cache_threshold to 1.000001
        fs_util.config_set_runtime(
            client1, "mds", "mds_health_cache_threshold", 1.000001
        )
        # set max_mad to 1
        client1.exec_command(sudo=True, cmd="ceph fs set cephfs max_mds 1")
        # stand_replay true
        client1.exec_command(
            sudo=True, cmd="ceph fs set cephfs allow_standby_replay true"
        )
        # standby replay 1 mds
        client1.exec_command(sudo=True, cmd="ceph fs set cephfs standby_count_wanted 1")
        # wait untirl standby mds is created
        client1.exec_command(
            sudo=True,
            cmd=f"python3 -c 'import time; [open(f\"/mnt/{fuse_mounting_dir_1}/test"
            f'file_{{i}}", "w") for i in range(400)]; time.sleep(1000)\' &',
        )
        mds_list = fs_util.get_active_mdss(client1, "cephfs")
        count_list = []
        for _ in range(10):
            cache_status, _ = client1.exec_command(
                sudo=True, cmd=f"ceph tell mds.{mds_list[0]} cache status"
            )
            ceph_status, _ = ceph_status = client1.exec_command(
                sudo=True, cmd="ceph -s -f json-pretty"
            )
            ceph_status = json.loads(ceph_status[0])
            count = ceph_status["health"]["checks"]["MDS_CACHE_OVERSIZED"]["summary"][
                "count"
            ]
            log.info(f"Number of count {count}")
            count_list.append(count)
            time.sleep(3)
        if count_list[0] > 1:
            return 0
        log.info(f"Adding counts for cache status has failed. it shows {count_list[0]}")
        return 1

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        # Cleanup
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
