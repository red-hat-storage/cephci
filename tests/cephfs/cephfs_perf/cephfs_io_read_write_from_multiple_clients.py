import json
import re
import timeit
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utils import FsUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Mount CephFS on multiple clients and run
    Stress IO from all clients on same directory
    """
    try:
        start = timeit.default_timer()
        tc = "11221"
        log.info("Running cephfs %s test case" % tc)
        fs_util = FsUtils(ceph_cluster)
        fs_util_v1 = FsUtilsV1(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        client_info, rc = fs_util.get_clients(build)
        if rc == 0:
            log.info("Got client info")
        else:
            raise CommandFailed("fetching client info failed")
        c1 = 1
        client1, client2, client3, client4 = ([] for _ in range(4))
        client1.append(client_info["fuse_clients"][0])
        client2.append(client_info["fuse_clients"][1])
        client3.append(client_info["kernel_clients"][0])
        client4.append(client_info["kernel_clients"][1])
        rc1 = fs_util_v1.auth_list(client1)
        rc2 = fs_util_v1.auth_list(client2)
        rc3 = fs_util_v1.auth_list(client3)
        rc4 = fs_util_v1.auth_list(client4)
        if rc1 == 0 and rc2 == 0 and rc3 == 0 and rc4 == 0:
            log.info("got auth keys")
        else:
            raise CommandFailed("auth list failed")

        fs_util_v1.fuse_mount(client1, client_info["mounting_dir"])
        fs_util_v1.fuse_mount(client2, client_info["mounting_dir"])
        fs_util_v1.kernel_mount(
            client3, client_info["mounting_dir"], ",".join(client_info["mon_node_ip"])
        )
        fs_util_v1.kernel_mount(
            client4, client_info["mounting_dir"], ",".join(client_info["mon_node_ip"])
        )

        while c1:
            with parallel() as p:
                p.spawn(
                    fs_util.read_write_IO,
                    client1,
                    client_info["mounting_dir"],
                    "g",
                    "write",
                )
                p.spawn(
                    fs_util.read_write_IO,
                    client2,
                    client_info["mounting_dir"],
                    "g",
                    "read",
                )
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info["mounting_dir"],
                    "",
                    0,
                    2,
                    iotype="smallfile",
                )
                p.spawn(
                    fs_util.stress_io,
                    client2,
                    client_info["mounting_dir"],
                    "",
                    0,
                    2,
                    iotype="smallfile",
                )
                p.spawn(fs_util.read_write_IO, client3, client_info["mounting_dir"])
                for op in p:
                    (return_counts, rc) = op
            c1 = ceph_df(ceph_cluster)

        check_health(ceph_cluster)
        log.info("Test completed for CEPH-%s" % tc)
        print("Results:")
        result = fs_util.rc_verify(tc, return_counts)
        print(result)
        print("Script execution time:------")
        stop = timeit.default_timer()
        total_time = stop - start
        (mins, secs) = divmod(total_time, 60)
        (hours, mins) = divmod(mins, 60)
        print("Hours:%d Minutes:%d Seconds:%f" % (hours, mins, secs))
        return 0

    except CommandFailed as e:
        log.info(e)
        log.info(traceback.format_exc())
        log.info("Cleaning up!-----")
        if client3[0].pkg_type != "deb":
            rc = fs_util.client_clean_up(
                client_info["fuse_clients"],
                client_info["kernel_clients"],
                client_info["mounting_dir"],
                "umount",
            )
        else:
            rc = fs_util.client_clean_up(
                client_info["fuse_clients"], "", client_info["mounting_dir"], "umount"
            )
        if rc == 0:
            log.info("Cleaning up successfull")

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1


def ceph_df(ceph_cluster):
    for mnode in ceph_cluster.get_nodes(role="client"):
        if mnode.role == "client":
            m = mnode.role
            log.info("%s" % m)
            out, rc = mnode.exec_command(cmd="sudo ceph df ")
            l = []
            for s in out.split():
                if re.findall(r"-?\d+\.?\d*", s):
                    l.append(s)
            print(l[3])
            cluster_filled_perc = float(l[3])
            if cluster_filled_perc > 30:
                return 0
            return 1


def check_health(ceph_cluster):
    for mnode in ceph_cluster.get_nodes(role="client"):
        if mnode.role == "client":
            m = mnode.role
            log.info("%s" % m)
            out, rc = mnode.exec_command(cmd="sudo ceph -s -f json ")
            out = json.loads(out)
            if out["health"]["status"] == "HEALTH_ERR":
                log.warning("Ceph Health is NOT OK")
                return 1
            else:
                log.info("Health IS OK")
                return 0
