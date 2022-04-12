import json
import secrets
import string
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Pre-requisites :
    1. Create cephfs volume
       creats fs volume create <vol_name>
    2. Create cephfs subvolume
       creats fs subvolume create <vol_name> <subvolume_name>

    Operations:
    1. Mount Cephfs on kernel client with recover_session=no
    2. Block the client node on which cephfs is mounted
    3. Verify mount is inaccessible

    Clean-up:
    1. Remove all the data in Cephfs file system
    2. Unblock the client node
    3. Remove all cephfs mounts
    """
    try:
        tc = "CEPH-83573676"
        log.info(f"Running cephfs {tc} test case")
        fs_util = FsUtils(ceph_cluster)
        config = kw["config"]
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        rhbuild = config.get("rhbuild")
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client1 = clients[0]
        client2 = clients[1]
        mon_node_ip = fs_util.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)
        mount_dir = "/mnt/" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        if "4." in rhbuild:
            fs_name = "cephfs_new"
        else:
            fs_name = "cephfs"
        commands = [
            f"ceph fs subvolume create {fs_name} sub1",
            f"mkdir {mount_dir}",
            f"mount -t ceph {mon_node_ip}:/ {mount_dir} -o name=admin,recover_session=no",
            f"ls {mount_dir}",
        ]
        output = None
        for command in commands:
            output, rc = client1.exec_command(sudo=True, cmd=command)
        if "volumes" in output:
            log.info("Cephfs mount is accessible")
        else:
            log.error("Cephfs mount is not accessible")
            return 1
        log.info("Creating Directories")
        commands = [
            f"mkdir {mount_dir}/volumes/dir",
            f"for n in {{1..5}}; do dd if=/dev/urandom of={mount_dir}/volumes/dir/file$( printf %03d "
            "$n"
            " )"
            " bs=1M count=1000; done",
            "ceph tell mds.0 client ls --format json",
        ]
        for command in commands:
            out, rc = client1.exec_command(sudo=True, cmd=command)
        output = json.loads(out)
        for item in output:
            client_metadata = item["client_metadata"]
            kernel_client = 0
            if "kernel_version" in client_metadata.keys():
                kernel_client = 1
            if (
                client1.node.shortname == item["client_metadata"]["hostname"]
                and kernel_client == 1
            ):
                client_id = item["id"]
        log.info("Blocking the Cephfs client")
        command = f"ceph tell mds.0 client evict id={client_id}"
        out, rc = client1.exec_command(sudo=True, cmd=command)
        time.sleep(5)
        try:
            log.info("Verifying mount is inaccessible")
            out, rc = client1.exec_command(sudo=True, cmd=f"ls {mount_dir}")
        except CommandFailed as e:
            log.info(e)
            log.info("Mount point is inaccessible as expected")
            return 0
        else:
            output = out
            if "volumes" in output:
                log.error("Mount point is accessible")
                return 1

    except CommandFailed as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up")
        mount_dir_2 = "/mnt/" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        client2.exec_command(sudo=True, cmd=f"mkdir {mount_dir_2}")
        command = f"mount -t ceph {mon_node_ip}:/ {mount_dir_2} -o name=admin"
        client2.exec_command(sudo=True, cmd=command)
        client2.exec_command(sudo=True, cmd=f"rm -rf {mount_dir_2}/*")
        client2.exec_command(sudo=True, cmd=f"umount {mount_dir_2}")
        ip, rc = client1.exec_command(
            sudo=True, cmd="ifconfig eth0 | grep 'inet ' | awk '{{print $2}}'"
        )
        log.info("Unblocking the Cephfs client")
        if "4." in rhbuild:
            out, rc = client1.exec_command(
                sudo=True, cmd=f"ceph osd blacklist ls | grep {ip}"
            )
            out = out.split()
            blocked_client = out[0]
            client1.exec_command(
                sudo=True, cmd=f"ceph osd blacklist rm {blocked_client}"
            )
        else:
            out, rc = client1.exec_command(
                sudo=True, cmd=f"ceph osd blocklist ls | grep {ip}"
            )
            blocked_client = out.split()
            client = blocked_client[0]
            log.info(f"client_list - {client}")
            client1.exec_command(sudo=True, cmd=f"ceph osd blocklist rm {client}")
        client1.exec_command(sudo=True, cmd=f"umount {mount_dir}")
