import json
import random
import string
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.io.fs_io import fs_io
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Steps:
    1. Create some 5k directories and 5k files in CephFS mount and start client IO on directories.
    2. Shutdown the active MDS, standby MDS should take over the existing cluster
    3. Perform above step for each active MDS in the cluster one after another,
    4. make sure cluster is in healthy state before performing next node shutdown
    """
    try:
        tc = "CEPH-11241"
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
            for i in range(1, 300):
                directory_name = f"{fuse_mounting_dir_1}directory_{i}"
                client1.exec_command(sudo=True, cmd=f"mkdir {directory_name}")
                file_name = f"{directory_name}/file_{i}.txt"
                client1.exec_command(sudo=True, cmd=f"touch {file_name}")

        # Creating 5k dirs and files in parallel
        with parallel() as p:
            p.spawn(create_io_dir)
        find_mdsmap = "ceph fs status cephfs -f json"
        out1 = client1.exec_command(sudo=True, cmd=find_mdsmap)
        output1 = json.loads(out1[0])
        mdsmap = output1["mdsmap"]
        # first get all the mds nodes
        bringup_mds = []
        for mds in mdsmap:
            bringup_mds.append(mds["name"])
        up_mds = []
        standby_mds = []
        for mds in mdsmap:
            if mds["state"] == "active" and mds["name"].startswith("cephfs."):
                up_mds.append(mds["name"])
            if mds["state"] == "standby" and mds["name"].startswith("cephfs."):
                standby_mds.append(mds["name"])
        first_shut = up_mds[0].split(".")[1]
        log.info(f"first_shut_node={first_shut}")
        bringup_mds = []
        for mds in mds_nodes:
            print(mds.node.hostname)
            if mds.node.hostname == first_shut:
                bringup_mds.append(mds)
                fs_util.node_power_off(node=mds.node, sleep_time=150, **params)
        # updating mds
        out2 = client1.exec_command(sudo=True, cmd=find_mdsmap)
        output2 = json.loads(out2[0])
        mdsmap2 = output2["mdsmap"]
        up_mds2 = []
        standby_mds2 = []
        for mds in mdsmap2:
            if mds["state"] == "active":
                up_mds2.append(mds["name"])
            if mds["state"] == "standby":
                standby_mds2.append(mds["name"])
        if len(standby_mds2) == 0:
            log.info("No more standby mds")
        if standby_mds[0] not in up_mds2:
            log.info(up_mds2)
            for mds in bringup_mds:
                fs_util.node_power_on(node=mds.node, sleep_time=150, **params)
            raise CommandFailed(
                f"Standby mds {standby_mds[0]} is not promoted to active"
            )
        else:
            log.info(f"Standby mds {standby_mds[0]} is promoted to active")

        out3 = client1.exec_command(sudo=True, cmd=find_mdsmap)
        output3 = json.loads(out3[0])
        mdsmap3 = output3["mdsmap"]
        for mds in mdsmap3:
            if mds["state"] == "active":
                for mdss in mds_nodes:
                    log.info(mds["name"])
                    log.info(mdss.node.hostname)
                    if mds["name"].split(".")[1] == mdss.node.hostname:
                        log.info(f'shutting down {mds["name"]}')
                        fs_util.node_power_off(node=mdss.node, sleep_time=150, **params)
                        break
        out4 = client1.exec_command(sudo=True, cmd=find_mdsmap)
        output4 = json.loads(out4[0])
        mdsmap4 = output4["mdsmap"]
        log.info(f"mdsmap4={mdsmap4}")
        for bring in mds_nodes:
            fs_util.node_power_on(node=bring.node, sleep_time=150, **params)
        # check if the mds is restarted
        out5 = client1.exec_command(sudo=True, cmd=find_mdsmap)
        output5 = json.loads(out5[0])
        mdsmap5 = output5["mdsmap"]
        for mds in mdsmap5:
            if mds["state"] == "failed":
                raise CommandFailed(f"mds {mds['name']} is not in active state")
        return 0
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
