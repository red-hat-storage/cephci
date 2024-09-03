import json
import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)
"""
pre-requisites:
1. remove all the host before the test
2. getting all the availible host list

Test operation:
1. craete a cluster with 2 hosts
2. verify it is created with the hosts
3. reduce the host to 1
4. verify it is created with the hosts
5. increase the nfs cluster host to 3
6. verify it is created with the hosts
"""


def run(ceph_cluster, **kw):
    try:
        tc = "CEPH-83574013"
        log.info(f"Running CephFS tests for {tc}")
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
        fs_util.auth_list([client1])
        fs_util.prepare_clients(clients, build)
        # clean up all the nfs cluster before testing
        clean_up_nfs(client1)
        rand = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        # get 3 candidate hosts
        candidate_host = []
        res0, _ = client1.exec_command(
            sudo=True, cmd="ceph orch host ls --format json-pretty"
        )
        result0 = json.loads(res0)
        for host in result0:
            candidate_host.append(host["hostname"])
        print(candidate_host)
        candidate_1 = candidate_host[-1]
        candidate_2 = candidate_host[-2]
        candidate_3 = candidate_host[-3]
        cluster_name = f"nfs_{rand}"
        # create a nfs cluster with 2 hosts
        log.info(f"Deplying nfs cluster with {candidate_1} {candidate_2}")
        client1.exec_command(
            sudo=True,
            cmd=f'ceph nfs cluster create {cluster_name} "{candidate_1} {candidate_2}"',
        )
        verify_host_nfs(client1, [candidate_1, candidate_2], cluster_name)
        # reduce host to 1
        log.info(f"Deplying nfs cluster with {candidate_1}")
        client1.exec_command(
            sudo=True, cmd=f'ceph orch apply nfs {cluster_name} "{candidate_1}"'
        )
        verify_host_nfs(client1, [candidate_1], cluster_name)
        # increase hosts to 3
        log.info(f"Deplying nfs cluster with {candidate_1} {candidate_2} {candidate_3}")
        client1.exec_command(
            sudo=True,
            cmd=f'ceph orch apply nfs {cluster_name} "{candidate_1} {candidate_2} {candidate_3}" ',
        )
        verify_host_nfs(client1, [candidate_1, candidate_2, candidate_3], cluster_name)
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        clean_up_nfs(client1)


@retry(CommandFailed, tries=5, delay=30)
def verify_host_nfs(client, target_nfs_hosts, nfs_id):
    res, ec = client.exec_command(
        sudo=True, cmd=f"ceph nfs cluster info {nfs_id} --format json-pretty"
    )
    results = json.loads(res)
    current_host = []
    for host in results[nfs_id]["backend"]:
        current_host.append(host["hostname"])
    temp1 = sorted(current_host)
    temp2 = sorted(target_nfs_hosts)
    print(f"Current hosts for nfs: {temp1}")
    print(f"Target hosts for nfs: {temp2}")
    if temp1 != temp2:
        raise CommandFailed("Target nfs hosts and current nfs hosts are not same")
    return 0


def clean_up_nfs(client):
    ress, ecc = client.exec_command(sudo=True, cmd="ceph nfs cluster ls")
    ress2 = [s for s in ress.split("\n") if s.strip()]
    print(ress2)
    if len(ress2) > 0:
        for nfs in ress2:
            client.exec_command(sudo=True, cmd=f"ceph nfs cluster rm {nfs}")
