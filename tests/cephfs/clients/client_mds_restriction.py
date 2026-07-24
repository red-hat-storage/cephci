import json
import secrets
import string
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def verify_mount_failure_on_root(
    fs_util,
    client,
    kernel_mount_dir,
    fuse_mount_dir,
    client_name,
    mon_node_ip,
    **kwargs,
):
    """
    Verify kernel & fuse mount on root directory fails for client.
    :param client:
    :param kernel_mount_dir:
    :param kernel_mount_dir:
    :param fuse_mount_dir:
    :param client_name:
    :param mon_node_ip:
    **kwargs:
        extra_params : we can include extra parameters for mount options such as fs_name
    """
    kernel_fs_para = f",fs={kwargs.get('fs_name', 'cephfs')}"
    fuse_fs_para = f" --client_fs {kwargs.get('fs_name', 'cephfs')}"
    try:
        fs_util.kernel_mount(
            client,
            kernel_mount_dir,
            mon_node_ip,
            new_client_hostname=client_name,
            extra_params=kernel_fs_para,
        )
    except AssertionError as e:
        log.info(e)
        log.info(f"Permissions set for {client_name} is working for kernel mount")
    except CommandFailed as e:
        log.info(e)
        log.info("Error is expected as the client does not have permissions")
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    else:
        log.error(f"Permissions set for {client_name} is not working for kernel mount")
        return 1
    try:
        fs_util.fuse_mount(
            client,
            fuse_mount_dir,
            new_client_hostname=client_name,
            extra_params=fuse_fs_para,
        )
    except AssertionError as e:
        log.info(e)
        log.info(f"Permissions set for {client_name} is working for fuse mount")
    except CommandFailed as e:
        log.info(e)
        log.info("Error is expected as the client does not have permissions")
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    else:
        log.error(f"Permissions set for {client_name} is not working for fuse mount")
        return 1


def run(ceph_cluster, **kw):
    """
    Pre-requisites:
    1. Create 2 cephfs volume
       creats fs volume create <vol_name>

    Test operation:
    1. Create client1 restricted to first cephfs
       ceph fs authorize <fs_name> client.<client_id> <path-in-cephfs> rw
    2. Create client2 restricted to second cephfs
    3. Mount first cephfs with client1
    4. Verify mounting second cephfs with client1 fails
    5. Mount second cephfs with client2
    6. Verify mounting first cephfs with client2 fails

    Clean-up:
    1. Remove all the cephfs mounts
    2. Remove all the created clients
    """
    try:
        tc = "CEPH-83573869"
        log.info(f"Running cephfs {tc} test case")

        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))

        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        mon_node_ip = fs_util.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)
        mount_points = []
        fs1 = "cephfs"
        fs2 = "cephfs-ec"
        for fs in [fs1, fs2]:
            result, rc = clients[0].exec_command(
                sudo=True, cmd=f"ceph fs status {fs} --format json-pretty"
            )
            result_json = json.loads(result)
            log.info(result_json)

        mds_nodes = ceph_cluster.get_ceph_objects("mds")
        mds_names_hostname = []
        for mds in mds_nodes:
            mds_names_hostname.append(mds.node.hostname)
        if len(mds_names_hostname) > 2:
            hosts_list1 = mds_names_hostname[-2:]
        else:
            hosts_list1 = mds_names_hostname

        mds_hosts_1 = " ".join(hosts_list1)
        log.info(f"MDS host list 1 {mds_hosts_1}")
        clients[0].exec_command(
            sudo=True,
            cmd=f"ceph orch apply mds {fs2} --placement='2 {mds_hosts_1}'",
        )
        time.sleep(60)
        fs_util.wait_for_mds_process(clients[0], fs2)
        log.info(f"Creating client authorized to {fs1}")
        fs_util.fs_client_authorize(client1, fs1, "client1", "/", "rw")
        log.info(f"Creating client authorized to {fs2}")
        fs_util.fs_client_authorize(client1, fs2, "client2", "/", "rw")
        for client in clients:
            command = (
                "ceph auth get client.client1 -o /etc/ceph/ceph.client.client1.keyring"
            )
            client.exec_command(sudo=True, cmd=command)
        kernel_mount_dir = "/mnt/kernel" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fuse_mount_dir = "/mnt/fuse" + "".join(
            secrets.choice(string.ascii_lowercase + string.digits) for i in range(5)
        )
        mount_points.extend([kernel_mount_dir, fuse_mount_dir])
        log.info(f"Mounting {fs1} with client1")
        fs_util.kernel_mount(
            [client1],
            kernel_mount_dir,
            mon_node_ip,
            new_client_hostname="client1",
            extra_params=f",fs={fs1}",
        )
        fs_util.fuse_mount(
            [client1],
            fuse_mount_dir,
            new_client_hostname="client1",
            extra_params=f" --client_fs {fs1}",
        )
        log.info(f"Verifying mount failure for client1 for {fs2}")
        rc = verify_mount_failure_on_root(
            fs_util,
            [client1],
            kernel_mount_dir + "_dir1",
            fuse_mount_dir + "_dir1",
            "client1",
            mon_node_ip,
            fs_name=f"{fs2}",
        )
        if rc == 1:
            log.error(f"Mount success on {fs2} with client1")
            return 1
        kernel_mount_dir = "/mnt/kernel" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fuse_mount_dir = "/mnt/fuse" + "".join(
            secrets.choice(string.ascii_lowercase + string.digits) for i in range(5)
        )
        mount_points.extend([kernel_mount_dir, fuse_mount_dir])
        log.info(f"Mounting {fs2} with client2")
        fs_util.kernel_mount(
            [client1],
            kernel_mount_dir,
            mon_node_ip,
            new_client_hostname="client2",
            extra_params=f",fs={fs2}",
        )
        fs_util.fuse_mount(
            [client1],
            fuse_mount_dir,
            new_client_hostname="client2",
            extra_params=f" --client_fs {fs2}",
        )
        rc = verify_mount_failure_on_root(
            fs_util,
            [client1],
            kernel_mount_dir + "_dir2",
            fuse_mount_dir + "_dir2",
            "client2",
            mon_node_ip,
            fs_name=f"{fs1}",
        )
        log.info(f"Verifying mount failure for client2 for {fs1}")
        if rc == 1:
            log.error(f"Mount success on {fs1} with client2")
            return 1
        return 0

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up the system")
        for client in clients:
            for mount_point in mount_points:
                client.exec_command(
                    sudo=True, cmd=f"umount {mount_point}", check_ec=False
                )
        for num in range(1, 4):
            client1.exec_command(
                sudo=True, cmd=f"ceph auth rm client.client{num}", check_ec=False
            )
