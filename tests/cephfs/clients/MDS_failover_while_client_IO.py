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
"""Pre-requisite
1.Make sure cluster is up and configured with
2.Single CephFS, 4 MDS (2 active and 2 standby), required 4 clients(2 FUSE and 2 kernel Clients)
3.Use different IO tool on each client (fio/iozone/Bonnie++/dd etc)
4.Have a data verification enabled.(read and write data checksum should match, eg: in fio "meta" param)
5.Ensure cluster is filled with 20% data
6.Ensure clients are having enough permission to access the data
7.Configure cluster and make sure PG's in acitve + clean state.
8.Cluster configuration should succeed
Steps
1.Create some 50k directories and 50k files in CephFS mount and start client IO on directories.
2.Directory and File creation should succeed.
3.Client IO should continue without any issues.
4.Perform failover of both active MDS one by one with "ls, ll, rm -rf" operations in parallel on the CephFS mount"""


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
        log.info("Setting number of active to 2 and standby to 2")
        for client in clients:
            client.exec_command(sudo=True, cmd=f"ceph fs set {default_fs} max_mds 2")
            client.exec_command(
                sudo=True, cmd=f"ceph fs set {default_fs} standby_count_wanted 2"
            )
        time.sleep(10)
        result, rc = client1.exec_command(
            sudo=True, cmd=f"ceph fs status {default_fs} --format json-pretty"
        )
        result_json = json.loads(result)

        num_active = 0
        num_standby = 0

        active_list = []
        standby_list = []

        for elem in result_json["mdsmap"]:
            if elem["state"] == "active":
                num_active += 1
                active_list.append(elem["name"])
            elif elem["state"] == "standby":
                num_standby += 1
                standby_list.append(elem["name"])
        if num_active == 2 and num_standby == 2:
            log.info(
                f"Number of max_mds and standby MDS is {num_active} and {num_standby}"
            )

        log.info("Waiting for mds set up for the test")
        log.info(
            f"4 MDS will be ready as {num_active} active and {num_standby} standby"
        )

        with parallel() as p:
            p.spawn(create_dir, nums, client1, fuse_mounting_dir_1)
            p.spawn(create_dir, nums, client2, fuse_mounting_dir_2)
            p.spawn(create_dir, nums, client3, kernel_mounting_dir_1)
            p.spawn(create_dir, nums, client4, kernel_mounting_dir_2)

        result, rc = client1.exec_command(
            sudo=True, cmd=f"ceph fs status {default_fs} --format json-pretty"
        )
        result_json = json.loads(result)

        mds_name_list = []

        dir_list = [
            fuse_mounting_dir_1,
            fuse_mounting_dir_2,
            kernel_mounting_dir_1,
            kernel_mounting_dir_2,
        ]
        client_map = {k: v for k, v in zip(clients, dir_list)}

        cmd_list = ["ls", "rm -rf", "ls -al"]

        for elem in result_json["mdsmap"]:
            if elem["state"] == "active":
                mds_name_list.append(elem["name"])
        with parallel() as p:
            for mds in mds_name_list:
                p.spawn(client1.exec_command, sudo=True, cmd=f"ceph mds fail {mds}")
            for client in clients:
                for k in range(3):
                    if cmd_list[k] == "ls -al":
                        client.exec_command(cmd=f"cd {client_map[client]}")
                        client.exec_command(cmd="ls -al")

                    elif cmd_list[k] == "rm -rf":
                        log.info("IT IS RM - RF!!!")
                        client.exec_command(sudo=True, cmd=f"cd {client_map[client]}")
                        rc, ec = client.exec_command(
                            sudo=True, cmd="rm -rf", check_ec=False
                        )
                        if rc:
                            log.info("Cannot rm -rf in mount dir , which it normal")
                    else:
                        client.exec_command(
                            sudo=True, cmd=f"{cmd_list[k]} {client_map[client]}"
                        )
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up")
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
