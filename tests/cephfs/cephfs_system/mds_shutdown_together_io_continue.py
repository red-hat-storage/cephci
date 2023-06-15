import random
import string
import traceback

from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.io.fs_io import fs_io
from utility.log import Log

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
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")

        clients = ceph_cluster.get_ceph_objects("client")
        mds_nodes = ceph_cluster.get_ceph_objects("mds")

        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        osp_cred = config.get("osp_cred")
        if config.get("cloud-type") == "openstack":
            os_cred = osp_cred.get("globals").get("openstack-credentials")
            params = {}
            params["username"] = os_cred["username"]
            params["password"] = os_cred["password"]
            params["auth_url"] = os_cred["auth-url"]
            params["auth_version"] = os_cred["auth-version"]
            params["tenant_name"] = os_cred["tenant-name"]
            params["service_region"] = os_cred["service-region"]
            params["domain_name"] = os_cred["domain"]
            params["tenant_domain_id"] = os_cred["tenant-domain-id"]
            params["cloud_type"] = "openstack"
        elif config.get("cloud-type") == "ibmc":
            pass
        else:
            pass
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            [client1],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
        )
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        fs_util.fuse_mount([client1], fuse_mounting_dir_1)
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
                fs_util.node_power_off(node=mds_nodes[i].node, sleep_time=40, **params)
            for mds in mds_nodes:
                fs_util.node_power_on(node=mds.node, sleep_time=40, **params)
        num_files_after = f"ls -l {fuse_mounting_dir_1} | grep -c '^d'"
        out2 = client1.exec_command(sudo=True, cmd=num_files_after)
        after1 = int(out2[0].replace("\n", ""))
        if before1 < after1:
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
