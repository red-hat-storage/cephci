import random
import string
import traceback

from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

"""
ceph --admin-daemon $socketfile client evict [-h|--help] evicts ALL clients.
It is observed that adding "--help|-h" with the "client evict" command
terminates all active client connections and adds to the blocklist.
ceph tell DAEMON_NAME client evict id=ID_NUMBER
Steps to Reproduce:
1. Connect clients to cephfs server
2. ceph --admin-daemon $socketfile client evict [-h|--help] evicts ALL clients
3. verify evcit does not work with --help|-h
4. verify evcit handles id=ID_NUMBER
6. Verify the clients are removed from blocklist while IOs are running
"""


def run(ceph_cluster, **kw):
    try:
        tc = "58619"
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
        mds_nodes = ceph_cluster.get_ceph_objects("mds")
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
        fuse_mounting_dir_2 = f"/mnt/cephfs_fuse_{rand}_2"
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel_{rand}"
        kernel_mounting_dir_2 = f"/mnt/cephfs_kernel_{rand}_2"
        # Mount CephFS using ceph-fuse and kernel
        fs_util.fuse_mount([client1], fuse_mounting_dir_1)
        fs_util.fuse_mount([client1], fuse_mounting_dir_2)
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount([client1], kernel_mounting_dir_1, ",".join(mon_node_ips))
        fs_util.kernel_mount([client1], kernel_mounting_dir_2, ",".join(mon_node_ips))
        # get client ID for rank 0 MDS
        rank_0_mds, _ = client1.exec_command(
            sudo=True,
            cmd="ceph fs status cephfs -f json-pretty |"
            " jq -r '.mdsmap[] | select(.rank == 0 and .state==\"active\") | .name'",
        )
        rank_0_mds = rank_0_mds.rstrip()
        log.info("active rank 0 MDS: %s" % rank_0_mds)
        target_mds = []
        for mds_node in mds_nodes:
            log.info(mds_node.node.hostname)
            if mds_node.node.hostname in rank_0_mds:
                target_mds.append(mds_node)
        target_mds_name = target_mds[0].node.hostname
        client_ips = mds_client_ips(client1, rank_0_mds)
        log.info(f"Active rank 0 MDS:{target_mds_name}")
        admin_daemon = (
            f"cephadm shell ceph --admin-daemon /var/run/c"
            f"eph/ceph-mds.{rank_0_mds}.asok"
        )
        test_cmd_help1 = f"{admin_daemon} client evict -h"
        # evict client -h
        evict_help_out1, _ = target_mds[0].exec_command(
            sudo=True, cmd=test_cmd_help1, check_ec=False
        )
        check_ip_dic1 = mds_client_ips(client1, rank_0_mds)
        log.info(f"After running client evict -h {check_ip_dic1}")
        if len(check_ip_dic1) == 0:
            log.error("This Expected failure")
            log.error("https://bugzilla.redhat.com/show_bug.cgi?id=2160855")
            log.error(
                "[--admon-daemon client evict -h command should not evict all clients]"
            )
            return 1
        test_cmd_help2 = f"{admin_daemon} client evict --help"
        # evict client --help
        evict_help_out2, _ = target_mds[0].exec_command(
            sudo=True, cmd=test_cmd_help2, check_ec=False
        )
        check_ip_dic2 = mds_client_ips(client1, rank_0_mds)
        log.info(f"After running client evict -h {check_ip_dic2}")
        if len(check_ip_dic2) == 0:
            log.error(
                "[--admon-daemon client evict --help command should evict all clients]"
            )
            return 1
        # evict client id=*
        evict_all_cmd = f"{admin_daemon} client evict id=*"
        with parallel() as p:
            # run IOs for all the clients
            p.spawn(fs_util.run_ios(client1, fuse_mounting_dir_1, ["dd", "smallfile"]))
            p.spawn(
                fs_util.run_ios(client1, kernel_mounting_dir_1, ["dd", "smallfile"])
            )
            p.spawn(fs_util.run_ios(client1, fuse_mounting_dir_2, ["dd", "smallfile"]))
            p.spawn(
                fs_util.run_ios(client1, kernel_mounting_dir_2, ["dd", "smallfile"])
            )
            # evict all the client
            p.spawn(target_mds[0].exec_command, sudo=True, cmd=evict_all_cmd)
        check_ip_dic3 = mds_client_ips(client1, rank_0_mds)
        log.info(f"After running client evict id=* {check_ip_dic2}")
        if len(check_ip_dic3) != 0:
            log.error("All the clients are not evicted while IOs")
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
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_2
        )
        fs_util.client_clean_up(
            "umount", kernel_clients=[clients[0]], mounting_dir=kernel_mounting_dir_2
        )
        for ip in client_ips.values():
            client1.exec_command(sudo=True, cmd=f"ceph osd blocklist rm {ip}")


def mds_client_ips(client, mds_name):
    id_ip_cmd = f"ceph tell mds.{mds_name} client ls | jq -r '.[].inst'"
    id_ip, _ = client.exec_command(sudo=True, cmd=id_ip_cmd)
    id_ip_list = id_ip.split("\n")
    id_ip_dict = {}
    for id_ip in id_ip_list:
        if id_ip:
            client_id, client_ip = id_ip.split(" ")
            if client_id.startswith("client."):
                client_id = client_id.split(".")[1]
            if "v1:" in client_ip:
                client_ip = client_ip.split("v1:")[1]
            id_ip_dict[client_id] = client_ip
    return id_ip_dict


def remove_from_blocklist(client, id_ip_dict):
    for ip in id_ip_dict.values():
        client.exec_command(sudo=True, cmd=f"ceph osd blocklist rm {ip}")
    blocklist, _ = client.exec_command(sudo=True, cmd="ceph osd blocklist ls")
    ip_list = []
    for line in blocklist.strip().split("\n"):
        ip = line.split(" ")[0]
        ip_list.append(ip)
    for client_ip in id_ip_dict.values():
        if client_ip in ip_list:
            log.error(f"Client {client_ip} not removed from blocklist")
            return 1
    # all the client IPs are removed from blocklist
    log.info("All the client IPs are removed from blocklist")
    return 0
