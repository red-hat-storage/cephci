import csv
import os
import random
import string
import traceback
from datetime import datetime

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.utils import get_storage_stats

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Create Subvolumegroup and subvolume inside each subvolume group and write 2 gb data, Repeat till we reach except
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        log.info("checking Pre-requisites")
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        default_fs = "cephfs"
        test_run_details = []
        csv_columns = ["subvolumegroup", "Subvolume", "Execution_time"]
        csv_file = f"/tmp/scale_{__name__}.csv"
        csvfile = open(csv_file, "w")
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        # get_storage_stats(clients[0])
        cluster_stats = get_storage_stats(client=clients[0])
        cluster_used_percent = (
            cluster_stats["total_used_bytes"] / float(cluster_stats["total_bytes"])
        ) * 100
        cluster_to_fill = 80 - cluster_used_percent
        if cluster_to_fill <= 0:
            log.info(
                f"cluster is already filled with {config.get('cephfs').get('fill_data')}"
            )
        else:
            for i in range(1, int(cluster_to_fill)):
                try:
                    log.info(f"Iteration {i}")
                    stats = get_storage_stats(
                        clients[0],
                        pool_name=fs_util.get_fs_info(clients[0], default_fs)[
                            "data_pool_name"
                        ],
                    )
                    log.info(
                        f"Cluster filled till now : {int(stats['percent_used'] * 100)}"
                    )
                    if int(stats["percent_used"] * 100) >= 95:
                        break
                    start_time = datetime.now()
                    fs_util.create_subvolumegroup(
                        clients[0],
                        vol_name=default_fs,
                        group_name=f"subvolumegroup_{i}",
                    )
                    fs_util.create_subvolume(
                        clients[0],
                        vol_name=default_fs,
                        group_name=f"subvolumegroup_{i}",
                        subvol_name=f"subvolume_{i}",
                    )
                    mounting_dir = "".join(
                        random.choice(string.ascii_lowercase + string.digits)
                        for _ in list(range(10))
                    )
                    subvol_path, rc = clients[0].exec_command(
                        sudo=True,
                        cmd=f"ceph fs subvolume getpath {default_fs}  subvolume_{i} subvolumegroup_{i}",
                    )
                    mon_node_ips = fs_util.get_mon_node_ips()
                    kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
                    fs_util.kernel_mount(
                        [clients[0]],
                        kernel_mounting_dir_1,
                        ",".join(mon_node_ips),
                        sub_dir=f"{subvol_path.strip()}",
                    )
                    clients[0].exec_command(
                        sudo=True,
                        cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 1 "
                        f"--file-size 1024 "
                        f"--files 1024 --top "
                        f"{kernel_mounting_dir_1}",
                        long_running=True,
                    )

                    end_time = datetime.now()
                    exec_time = (end_time - start_time).total_seconds()
                    log.info(
                        f"Iteration Details \n"
                        f"subvolumegroup name created : subvolumegroup_{i}\n"
                        f"Subvolume Created : subvolume_{i}\n"
                        f"Time Taken for the Iteration : {exec_time}\n"
                    )
                    writer.writerow(
                        {
                            "subvolumegroup": f"subvolumegroup_{i}",
                            "Subvolume": f"subvolume_{i}",
                            "Execution_time": exec_time,
                        }
                    )
                    csvfile.flush()
                    os.fsync(csvfile.fileno())
                    test_run_details.append(
                        {
                            "subvolumegroup": f"subvolumegroup_{i}",
                            "Subvolume": f"subvolume_{i}",
                            "Execution_time": exec_time,
                        }
                    )
                    cmd = f"umount {kernel_mounting_dir_1} -l"
                    clients[0].exec_command(sudo=True, cmd=cmd)
                    total_iterations = i
                except CommandFailed as err:
                    log.error(f"Error: {err}")
                    if "No space left on device" not in str(err):
                        raise CommandFailed(err)

            log.info("Cluster is Full")
            log.info(f"Total Iterations {i}")

    except Exception as e:
        log.error(e)
        log.error(test_run_details)
        [
            print(
                "%s          %s          %s\n"
                % (item["subvolumegroup"], item["Subvolume"], item["Execution_time"])
            )
            for item in test_run_details
        ]
        log.info(traceback.format_exc())
        return 1
    finally:
        log.info(test_run_details)
        [
            print(
                "%s          %s          %s\n"
                % (item["subvolumegroup"], item["Subvolume"], item["Execution_time"])
            )
            for item in test_run_details
        ]
        status, rc = clients[0].exec_command(
            sudo=True,
            cmd="ceph -s",
        )
        df, rc = clients[0].exec_command(
            sudo=True,
            cmd="ceph df",
        )
        csvfile.write(status)
        csvfile.write(df)
        csvfile.flush()
        csvfile.close()
        for i in range(1, total_iterations):
            fs_util.remove_subvolume(
                clients[0],
                vol_name=default_fs,
                group_name=f"subvolumegroup_{i}",
                subvol_name=f"subvolume_{i}",
            )
            fs_util.remove_subvolumegroup(
                clients[0],
                vol_name=default_fs,
                group_name=f"subvolumegroup_{i}",
                validate=False,
                check_ec=False,
            )
        return 0
