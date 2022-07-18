import _thread as thread
import csv
import os
import random
import string
import sys
import threading
import traceback
from datetime import datetime

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

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
        i = 20
        csv_columns = ["subvolumegroup", "Subvolume", "Execution_time"]
        csv_file = f"/tmp/scale_{__name__}.csv"
        csvfile = open(csv_file, "w")
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        while True:
            log.info(f"Iteration {i}")
            start_time = datetime.now()
            fs_util.create_subvolumegroup(
                clients[0], vol_name=default_fs, group_name=f"subvolumegroup_{i}"
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
            exit_after_fun(fs_util.run_ios, 300, clients[0], kernel_mounting_dir_1)
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
            i = i + 1
            if i == 50:
                break

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
        return 0


def quit_function(fn_name):
    # print to stderr, unbuffered in Python 2.
    print("{0} took too long".format(fn_name), file=sys.stderr)
    sys.stderr.flush()  # Python 3 stderr is likely buffered.
    thread.interrupt_main()  # raises KeyboardInterrupt


def exit_after_fun(fn_name, s, *args, **kwargs):
    """
    Exit the function after s seconds
    """
    timer = threading.Timer(s, quit_function, args=[fn_name.__name__])
    timer.start()
    try:
        result = fn_name(*args, **kwargs)
    finally:
        timer.cancel()
    return result
