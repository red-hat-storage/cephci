import random
import string
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Steps:
    1. set the mds conf journal values
    2. verify the mds conf journal values
    """
    try:
        tc = "CEPH-11331"
        log.info(f"Running CephFS tests for -{tc}")
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        fs_util.auth_list([client1])
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(5))
        )
        client1.exec_command(sudo=True, cmd="yum install -y --nogpgcheck ceph-fuse")
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse{mounting_dir}_1/"
        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ip = fs_util.get_mon_node_ips()
        mon_node_ip = ",".join(mon_node_ip)
        fs_util.kernel_mount(
            [client1],
            kernel_mounting_dir_1,
            mon_node_ip,
            extra_params=",fs=cephfs",
        )
        fs_util.fuse_mount([client1], fuse_mounting_dir_1)
        log.info("Setting the client config values")
        conf_set = "ceph config set mds"
        conf_get = "ceph config get mds"
        conf_default = [
            ["journaler_write_head_interval", ""],
            ["journaler_prefetch_periods", ""],
            ["journaler_prezero_periods", ""],
        ]
        for conf in conf_default:
            output = client1.exec_command(sudo=True, cmd=f"{conf_get} {conf[0]}")
            conf[1] = output[0].strip()
        conf_target = [
            ("journaler_write_head_interval", "16"),
            ("journaler_prefetch_periods", "12"),
            ("journaler_prezero_periods", "6"),
        ]
        with parallel() as p:
            p.spawn(fs_util.run_ios, client1, fuse_mounting_dir_1, ["dd", "smallfile"])
            p.spawn(
                fs_util.run_ios, client1, kernel_mounting_dir_1, ["dd", "smallfile"]
            )
        for target in conf_target:
            client_conf = target[0]
            value = target[1]
            client1.exec_command(sudo=True, cmd=f"{conf_set} {client_conf} {value}")
        with parallel() as p:
            p.spawn(fs_util.run_ios, client1, fuse_mounting_dir_1, ["dd", "smallfile"])
            p.spawn(
                fs_util.run_ios, client1, kernel_mounting_dir_1, ["dd", "smallfile"]
            )
        for target in conf_target:
            client_conf = target[0]
            output = client1.exec_command(
                sudo=True, cmd=f"ceph config get mds {client_conf}"
            )
            if output[0].rstrip() != target[1]:
                raise CommandFailed(f"Failed to set {client_conf} to {value}")

        fs_util.get_ceph_health_status(client1, status=["HEALTH_WARN", "HEALTH_OK"])
        log.info("Successfully set all the client config values")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Resetting the client config values to default")
        for default in conf_default:
            client_conf = default[0]
            value = default[1]
            client1.exec_command(sudo=True, cmd=f"{conf_set} {client_conf} {value}")
        log.info("Successfully reset all the client config values to default")
        log.info("Unmounting the clients")
        fs_util.client_clean_up(client1, [fuse_mounting_dir_1, kernel_mounting_dir_1])
        log.info("Successfully unmounted the clients")
