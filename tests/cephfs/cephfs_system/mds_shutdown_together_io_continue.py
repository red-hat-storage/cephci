import random
import string
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from ceph.utils import find_vm_node_by_hostname
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.io.fs_io import fs_io
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
        Pre-requisite:
        1.  Make sure cluster is up and configured with
    Single CephFS, 4 MDS (2 active and 2 standby), required 4 clients(2 FUSE and 2 kernel Clients)
        Steps:
        1. Create some 300 directories and 300 files in CephFS mount and start client IO on directories.
        2. Shutdown all the MDS node together and bring up all the MDS node together
        3. Start Client IO's on CephFS mount
    """
    try:
        tc = "CEPH-11240"
        log.info(f"Running CephFS tests for -{tc}")
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        config = kw.get("config")

        clients = ceph_cluster.get_ceph_objects("client")
        mds_nodes = ceph_cluster.get_ceph_objects("mds")

        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client1 = clients[0]
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
        host_list = [mdsnode.node.hostname for mdsnode in mds_nodes]
        hosts = " ".join(host_list)
        client1.exec_command(
            sudo=True,
            cmd=f"ceph orch apply mds {fs_name} --placement='3 {hosts}'",
        )
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        retry_ceph_health = retry(CommandFailed, tries=5, delay=60)(
            fs_util.get_ceph_health_status
        )
        retry_ceph_health(clients[0])
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [client1],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={fs_name}",
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fs_util.fuse_mount(
            [client1], fuse_mounting_dir_1, extra_params=f" --client_fs {fs_name}"
        )
        cephfs = {
            "fill_data": 20,
            "io_tool": "smallfile",
            "mount": "fuse",
            "filesystem": "cephfs",
            "mount_dir": f"{fuse_mounting_dir_1}",
        }
        fs_io(client=clients[0], fs_config=cephfs, fs_util=fs_util)
        # create a test dir in mounted dir
        test_dir = f"{fuse_mounting_dir_1}test_dir"
        client1.exec_command(sudo=True, cmd=f"mkdir {test_dir}")

        def create_io_dir():
            try:
                for i in range(1, 300):
                    rand = "".join(
                        random.choice(string.ascii_lowercase + string.digits)
                        for _ in list(range(3))
                    )
                    directory_name = f"{fuse_mounting_dir_1}directory_{i}_{rand}"
                    client1.exec_command(sudo=True, cmd=f"mkdir {directory_name}")
                    file_name = f"{directory_name}/file_{i}.txt"
                    client1.exec_command(sudo=True, cmd=f"touch {file_name}_{rand}")
                return 0
            except Exception as e:
                log.error(e)
                pass

        with parallel() as p:
            p.spawn(create_io_dir)
            for i in range(len(mds_nodes)):
                if i == 1:
                    before = client1.exec_command(
                        sudo=True, cmd=f" ls -l {fuse_mounting_dir_1} | grep -c '^d'"
                    )
                    log.info(type(before))
                    before1 = int(before[0].replace("\n", ""))
                    log.info(before1)
                target_node = find_vm_node_by_hostname(
                    ceph_cluster, mds_nodes[i].node.hostname
                )
                target_node.shutdown(wait=True)
            for mds in mds_nodes:
                target_node = find_vm_node_by_hostname(ceph_cluster, mds.node.hostname)
                target_node.power_on()
        num_files_after = f"ls -l {fuse_mounting_dir_1} | grep -c '^d'"
        out2 = client1.exec_command(sudo=True, cmd=num_files_after)
        after1 = int(out2[0].replace("\n", ""))
        log.info(f"before : {before1} , After : {after1}")
        if before1 <= after1:
            return 0
        else:
            return 1
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=kernel_mounting_dir_1
        )
        fs_util.client_clean_up(
            "umount", fuse_clients=[clients[0]], mounting_dir=fuse_mounting_dir_1
        )
