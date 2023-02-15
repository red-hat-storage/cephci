import json
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def validate_fs_info(expected_fs, output):
    """
    Validate fs information restriction for clients
    :param expected_fs - only fs expected to show in output
    :param output - output of 'ceph fs ls'
    """
    if len(output) != 1 and output[0]["name"] != expected_fs:
        raise CommandFailed("fs is not matching with authorized FS")
    log.info(f"File systems Information is restricted to {expected_fs} only")


def run(ceph_cluster, **kw):
    """
    Pre-requisites:
    1. Create 3 cephfs volume
       creats fs volume create <vol_name>
       ceph orch apply mds <fs_name> --placement='<no. of mds> <mds_nodes...>'

    Test operation:
    1. Create client1 restricted to first cephfs
       ceph fs authorize <fs_name> client.<client_id> <path-in-cephfs> rw
    2. Create client2 restricted to second cephfs
    3. Create client3 restricted to third cephfs
    4. Get filesystem information using client1
    5. Ensure only first cephfs info is shown
    6. Get filesystem information using client2
    7. Ensure only second cephfs info is shown
    8. Get filesystem information using client3
    9. Ensure only third cephfs info is shown

    Clean-up:
    1. Remove third cephfs
    2. Remove all the cephfs mounts
    3. Remove all the clients
    """
    try:
        tc = "CEPH-83573875"
        log.info(f"Running cephfs {tc} test case")

        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))
        mdss = ceph_cluster.get_ceph_objects("mds")

        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        mds1 = mdss[0].node.hostname
        mds2 = mdss[1].node.hostname
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        mon_node_ip = fs_util.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)
        fs1 = "cephfs"
        fs2 = "cephfs-ec"
        fs3 = "Ceph_fs_new"
        commands = [
            f"ceph fs volume create {fs3}",
            f"ceph orch apply mds {fs3} --placement='2 {mds1} {mds2}'",
        ]
        for command in commands:
            err = clients[0].exec_command(sudo=True, cmd=command, long_running=True)
            if err:
                return 1
            wait_for_process(client=clients[0], process_name=fs3, ispresent=True)
        log.info(f"Creating client authorized to {fs1}")
        fs_util.fs_client_authorize(client1, fs1, "client1", "/", "rw")
        log.info(f"Creating client authorized to {fs2}")
        fs_util.fs_client_authorize(client1, fs2, "client2", "/", "rw")
        log.info(f"Creating client authorized to {fs3}")
        fs_util.fs_client_authorize(client1, fs3, "client3", "/", "rw")
        log.info("Verifying file system information for client1")
        command = (
            "ceph auth get client.client1 -o /etc/ceph/ceph.client.client1.keyring"
        )
        client1.exec_command(sudo=True, cmd=command)
        command = "ceph fs ls -n client.client1 -k /etc/ceph/ceph.client.client1.keyring --format json"
        out, rc = client1.exec_command(sudo=True, cmd=command)
        output = json.loads(out)
        validate_fs_info(fs1, output)
        log.info("Verifying file system information for client2")
        command = (
            "ceph auth get client.client2 -o /etc/ceph/ceph.client.client2.keyring"
        )
        client1.exec_command(sudo=True, cmd=command)
        command = "ceph fs ls -n client.client2 -k /etc/ceph/ceph.client.client2.keyring --format json"
        out, rc = client1.exec_command(sudo=True, cmd=command)
        output = json.loads(out)
        validate_fs_info(fs2, output)
        log.info("Verifying file system information for client3")
        command = (
            "ceph auth get client.client3 -o /etc/ceph/ceph.client.client3.keyring"
        )
        client1.exec_command(sudo=True, cmd=command)
        command = "ceph fs ls -n client.client3 -k /etc/ceph/ceph.client.client3.keyring --format json"
        out, rc = client1.exec_command(sudo=True, cmd=command)
        output = json.loads(out)
        validate_fs_info(fs3, output)
        return 0

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up the system")
        commands = [
            "ceph config set mon mon_allow_pool_delete true",
            f"ceph fs volume rm {fs3} --yes-i-really-mean-it",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command)

        for num in range(1, 4):
            client1.exec_command(sudo=True, cmd=f"ceph auth rm client.client{num}")
