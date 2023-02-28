import json
import random
import string
import time
import traceback

from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.io.fs_io import fs_io
from utility.log import Log

log = Log(__name__)

# This is used for tier 3 test cases of cephfs TC-11238
# Need conf file with 4 clients and filled up cluster with 20%
# Pre-requisite
# fill up cluster with 20% and 4 clients
# Purpose of this test case
# While creating 50k directories and 50k small files in cephfs mount.
# check if standby MDS goes to active under the circumstance.


def run(ceph_cluster, **kw):
    try:
        fs_util = FsUtils(ceph_cluster)

        config = kw.get("config")

        clients = ceph_cluster.get_ceph_objects("client")

        build = config.get("build", config.get("rhbuild"))

        nums = config.get("num_of_file_dir")

        fs_util.prepare_clients(clients, build)

        fs_util.auth_list(clients)

        log.info("checking Pre-requisites")

        client1 = clients[0]
        client2 = clients[1]
        client3 = clients[2]
        client4 = clients[3]

        cephfs = {
            "fill_data": 20,
            "io_tool": "smallfile",
            "mount": "fuse",
            "filesystem": "cephfs",
            "mount_dir": "/mnt/mycephfs1",
        }
        fs_io(client=clients[0], fs_config=cephfs, fs_util=fs_util)

        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        kernel_mounting_dir_2 = f"/mnt/cephfs_kernel{mounting_dir}_2/"

        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fuse_mounting_dir_2 = f"/mnt/cephfs_fuse{mounting_dir}_2/"

        mon_node_ips = fs_util.get_mon_node_ips()

        default_fs = "cephfs"

        fs_util.fuse_mount(
            [clients[0]],
            fuse_mounting_dir_1,
            extra_params=f" --client_fs {default_fs}",
        )
        fs_util.fuse_mount(
            [clients[1]],
            fuse_mounting_dir_2,
            extra_params=f" --client_fs {default_fs}",
        )

        fs_util.kernel_mount(
            [clients[2]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={default_fs}",
        )
        fs_util.kernel_mount(
            [clients[3]],
            kernel_mounting_dir_2,
            ",".join(mon_node_ips),
            extra_params=f",fs={default_fs}",
        )

        for client in clients:
            client.exec_command(sudo=True, cmd=f"ceph fs set {default_fs} max_mds 1")
            client.exec_command(
                sudo=True, cmd=f"ceph fs set {default_fs} standby_count_wanted 1"
            )
        time.sleep(10)

        result, rc = client1.exec_command(
            sudo=True, cmd=f"ceph fs status {default_fs} --format json-pretty"
        )
        result_json = json.loads(result)
        num_active = 0
        num_standby = 0

        for elem in result_json["mdsmap"]:
            log.info(print(elem))
            if elem["state"] == "active":
                num_active += 1
            elif elem["state"] == "standby":
                num_standby += 1
        if num_active == 1 and num_standby == 1:
            log.info(
                f"Number of max_mds and standby MDS is {num_active} and {num_standby}"
            )

        log.info("Waiting for mds set up for the test")
        log.info("2 MDS will be ready as 1 active and 1 standby")
        with parallel() as p:
            p.spawn(create_dir, nums, client1, fuse_mounting_dir_1)
            p.spawn(create_dir, nums, client2, fuse_mounting_dir_2)
            p.spawn(create_dir, nums, client3, kernel_mounting_dir_1)
            p.spawn(create_dir, nums, client4, kernel_mounting_dir_2)
            p.spawn(
                client1.exec_command,
                sudo=True,
                cmd=f"ceph fs set {default_fs} max_mds 2",
            )

        result, rc = client1.exec_command(
            sudo=True, cmd=f"ceph fs status {default_fs} --format json-pretty"
        )
        result_json = json.loads(result)
        num_active = 0
        num_standby = 0

        for elem in result_json["mdsmap"]:
            log.info(print(elem))
            if elem["state"] == "active":
                num_active += 1
            elif elem["state"] == "standby":
                num_standby += 1
        if num_active == 2:
            log.info(f"Number of active MDS is {num_active} ")
        else:
            log.error("Stadby MDS did not move to Active")
            return 1

        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[1]], mounting_dir=fuse_mounting_dir_2
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[2]], mounting_dir=kernel_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[3]], mounting_dir=kernel_mounting_dir_2
        )


def create_dir(num, client, dir):
    for i in range(num):
        ran = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        client.exec_command(sudo=True, cmd=f"mkdir {dir}_{ran}_{i}")
        client.exec_command(
            sudo=True, cmd=f"dd if=/dev/urandom of={ran}__{i} bs=1K count=1"
        )
