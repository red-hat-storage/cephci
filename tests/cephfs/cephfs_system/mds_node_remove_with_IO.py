import json
import random
import string
import time
import traceback

from ceph.ceph import CommandFailed
from ceph.ceph_admin import CephAdmin
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.io.fs_io import fs_io
from utility.log import Log
from utility.retry import retry

log = Log(__name__)

"""
Pre-requisite:
Make sure cluster is up and configured with Single CephFS,
3 MDS (1 active and 2 standby),
required 3 clients. Configure cluster and make sure PG's in active + clean state.
Configure 2 clients with Fuse client and
another 1 client with kernel client.
Make sure MDS is having Stand-by rank set
Wait for cluster to get filled upto 20%
Steps:
Identify the faulty MDS node it can be either active
or stand-by MDS node and remove it from cluster using Ansible or manual way.
"""


@retry(CommandFailed, tries=5, delay=60)
def check_nodes(admin, target_node, check_node_cmd):
    out2, _ = admin.installer.exec_command(sudo=True, cmd=check_node_cmd)
    if str(out2).strip() == "No daemons reported":
        raise CommandFailed(f"{target_node} daemons are not removed")


def run(ceph_cluster, **kw):
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        log.info("checking Pre-requisites")
        client1 = clients[0]
        admin = CephAdmin(cluster=ceph_cluster, **config)
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
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}/"
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}/"
        mon_node_ips = fs_util.get_mon_node_ips()
        default_fs = "cephfs"
        fs_util.kernel_mount(
            [clients[0]],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={default_fs}",
        )
        fs_util.fuse_mount(
            [clients[0]],
            fuse_mounting_dir_1,
            extra_params=f" --client_fs {default_fs}",
        )
        fs_util.run_ios(client1, fuse_mounting_dir_1, ["dd"])
        fs_util.run_ios(client1, kernel_mounting_dir_1, ["smallfile"])
        out1, _ = admin.installer.exec_command(
            sudo=True, cmd="cephadm shell ceph orch host ls --format json-pretty"
        )
        output1 = json.loads(out1)
        candidate_host = []
        for host in output1:
            if "osd" not in host["labels"] and "mds" in host["labels"]:
                candidate_host.append(host["hostname"])
        print("Candidate host for removing / Adding MDS node")
        print(candidate_host)
        target_node = candidate_host[-1]
        drain_node_cmd = f"cephadm shell ceph orch host drain {target_node}"
        remove_node_cmd = f"cephadm shell ceph orch host rm {target_node}"
        check_node_cmd = f"cephadm shell ceph orch ps {target_node}"
        admin.installer.exec_command(sudo=True, cmd=drain_node_cmd)
        time.sleep(20)
        admin.installer.exec_command(sudo=True, cmd=remove_node_cmd)
        time.sleep(20)
        check_node_cmd(admin, target_node, check_node_cmd)
        add_node_cmd = f"cephadm shell ceph orch host add {target_node} --labels mds"
        admin.installer.exec_command(sudo=True, cmd=add_node_cmd)
        time.sleep(10)
        check_ps_cmd = f"cephadm shell ceph orch ps {target_node} --format json-pretty"
        out3, ec3 = admin.installer.exec_command(sudo=True, cmd=check_ps_cmd)
        output3 = json.loads(out3)
        if output3[0]["hostname"] == target_node and output3[0]["daemon_type"] == "mds":
            log.info("The Target mds node added")
        else:
            raise CommandFailed("Added node is not added properly")
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
            "umount", fuse_clients=[clients[0]], mounting_dir=kernel_mounting_dir_1
        )
