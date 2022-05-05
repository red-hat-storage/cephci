import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    CEPH-83574286 - Deploy mds using cephadm and increase & decrease number of mds.
    Pre-requisites :
    1. We need atleast one client node to execute this test case

    Test Case Flow:
    1. Create 1 file systems with --placements
    2. Validate MDS has come up or not
    3. Increase MDS
    4. Validate if the MDS has come up on specific hosts
    5. Decrease the MDS nodes.
    6. Validate if the mds has stopped and started on specific hosts
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))

        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        log.info("checking Pre-requisites")
        if not clients:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        client1 = clients[0]
        host_list = [
            client1.node.hostname.replace("node7", "node2"),
            client1.node.hostname.replace("node7", "node3"),
            client1.node.hostname.replace("node7", "node4"),
            client1.node.hostname.replace("node7", "node5"),
        ]
        hosts = " ".join(host_list[:2])
        fs_name = "cephfs_df_fs"
        client1.exec_command(
            sudo=True,
            cmd=f"ceph fs volume create {fs_name} --placement='2 {hosts}'",
            check_ec=False,
        )

        for host in host_list[:2]:
            if not fs_util.wait_for_mds_deamon(
                client=client1, process_name=fs_name, host=host
            ):
                raise CommandFailed(f"Failed to start MDS on particular nodes {host}")
        log.info("increase the mds for the filesystem")
        hosts = " ".join(host_list)
        client1.exec_command(
            sudo=True,
            cmd=f"ceph orch apply mds {fs_name} --placement='4 {hosts}'",
            check_ec=False,
        )
        for host in host_list:
            if not fs_util.wait_for_mds_deamon(
                client=client1, process_name=fs_name, host=host
            ):
                raise CommandFailed(f"Failed to start MDS on particular nodes {host}")
        log.info("Decrease the mds for the filesystem")
        hosts = " ".join(host_list[2:])
        client1.exec_command(
            sudo=True,
            cmd=f"ceph orch apply mds {fs_name} --placement='2 {hosts}'",
            check_ec=False,
        )
        for host in host_list[2:]:
            if not fs_util.wait_for_mds_deamon(
                client=client1, process_name=fs_name, host=host
            ):
                raise CommandFailed(f"Failed to start MDS on particular nodes {host}")
        for host in host_list[:2]:
            if not fs_util.wait_for_mds_deamon(
                client=client1, process_name=fs_name, host=host, ispresent=False
            ):
                raise CommandFailed(f"Failed to stop MDS on particular nodes {host}")

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        commands = [
            "ceph config set mon mon_allow_pool_delete true",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command)
        fs_util.remove_fs(client1, "cephfs_df_fs")
