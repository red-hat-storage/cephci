import os
import random
import string
import traceback
from datetime import datetime

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        log.info(f"MetaData Information {log.metadata} in {__name__}")
        fs_util = FsUtils(ceph_cluster)
        log_dir = os.path.dirname(log.logger.handlers[0].baseFilename)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        fs_name = config.get("fs_name", "cephfs")
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        fs_details = FsUtils.get_fs_info(clients[0], fs_name)
        mdss = ceph_cluster.get_ceph_objects("mds")
        if len(mdss) < 3:
            log.error("This test requires atleast 3 mds nodes with role in config file")
            return 1
        mds1 = mdss[0].node.hostname
        mds2 = mdss[1].node.hostname
        mds3 = mdss[2].node.hostname
        if not fs_details:
            if not config.get("erasure"):
                fs_util.create_fs(
                    client=clients[0],
                    vol_name=fs_name,
                    placement=f"3 {mds1} {mds2} {mds3}",
                )
            else:
                clients[0].exec_command(
                    sudo=True, cmd=f"ceph osd pool create {fs_name}-data-ec 64 erasure"
                )
                clients[0].exec_command(
                    sudo=True, cmd=f"ceph osd pool create {fs_name}-metadata-ec 64"
                )
                clients[0].exec_command(
                    sudo=True,
                    cmd=f"ceph osd pool set {fs_name}-data-ec allow_ec_overwrites true",
                )
                clients[0].exec_command(
                    sudo=True,
                    cmd=f"ceph fs new {fs_name} {fs_name}-metadata-ec {fs_name}-data-ec --force",
                )
                clients[0].exec_command(
                    sudo=True,
                    cmd=f"ceph orch apply mds {fs_name} --placement='3 {mds1} {mds2} {mds3}'",
                )
        if config.get("max_mds"):
            clients[0].exec_command(
                sudo=True,
                cmd=f"ceph orch apply mds {fs_name} --placement='3 {mds1} {mds2} {mds3}'",
            )
            clients[0].exec_command(
                sudo=True, cmd=f"ceph fs set {fs_name} max_mds {config.get('max_mds')}"
            )
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        fuse_mounting_dir = f"/mnt/cephfs_fuse{mounting_dir}/"
        fs_util.fuse_mount(
            clients, fuse_mounting_dir, extra_params=f" --client_fs {fs_name}"
        )

        kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            clients,
            kernel_mounting_dir,
            ",".join(mon_node_ips),
            extra_params=f",fs={fs_name}",
        )
        run_fio(
            [kernel_mounting_dir, fuse_mounting_dir],
            config,
            clients[0],
            log_dir,
            fs_util,
            fs_name,
        )
        return 0

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up!-----")
        rc = fs_util.client_clean_up(
            [],
            clients,
            kernel_mounting_dir,
            "umount",
        )
        if rc != 0:
            raise CommandFailed("fuse clients cleanup failed")
        log.info("Fuse clients cleaned up successfully")

        rc = fs_util.client_clean_up(
            clients,
            [],
            fuse_mounting_dir,
            "umount",
        )
        if rc != 0:
            raise CommandFailed("kernel clients cleanup failed")
        log.info("kernel clients cleaned up successfully")


def run_fio(mount_points, config, client, log_dir, fs_util, fs_name):
    for mount in mount_points:
        start_time = datetime.now()
        fio_filenames = fs_util.generate_all_combinations(
            client,
            config.get("fio_config").get("global_params").get("ioengine"),
            mount,
            config.get("fio_config").get("workload_params").get("random_rw").get("rw"),
            config.get("fio_config").get("global_params").get("size", ["1G"]),
            config.get("fio_config")
            .get("workload_params")
            .get("random_rw")
            .get("iodepth", ["1"]),
            config.get("fio_config")
            .get("workload_params")
            .get("random_rw")
            .get("numjobs", ["4"]),
        )
        for file in fio_filenames:
            client.exec_command(
                sudo=True,
                cmd=f"fio {file} --output-format=json "
                f"--output={file}_{client.node.hostname}_{mount.replace('/', '')}_{fs_name}.json",
                long_running=True,
                timeout="notimeout",
            )
            end_time = datetime.now()
            exec_time = (end_time - start_time).total_seconds()
            log.info(
                f"Iteration Details \n"
                f"Export name created : {mount}\n"
                f"Time Taken for the Iteration : {exec_time}\n"
            )
            os.makedirs(f"{log_dir}/{client.node.hostname}", exist_ok=True)
            client.download_file(
                src=f"/root/{file}_{client.node.hostname}_{mount.replace('/', '')}_{fs_name}.json",
                dst=f"{log_dir}/{client.node.hostname}/{file}_{client.node.hostname}"
                f"_{mount.replace('/', '')}.json",
                sudo=True,
            )
