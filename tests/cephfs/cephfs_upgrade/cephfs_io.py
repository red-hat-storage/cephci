import traceback
from datetime import datetime, timedelta

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    This will use this file to start the IOs on the Client system.
    This is mainly intended to run continous IOs on any File system.
    Backend IO generators used :
    1. smallfile
    2. DD
    Prerequistes:
    1. Client machine with root access.
    2. Directory path where the filesystem is mounted
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")

        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        timeout = config.get("timeout", 1800)
        log.info("checking Pre-requisites")
        if not clients:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        client1 = clients[0]
        stats = {"total_iterations": 0, "smallfile": 0, "dd": 0}
        out, rc = client1.exec_command(
            sudo=True, cmd="mount -t fuse.ceph-fuse | awk {'print $3'}", check_ec=False
        )
        mount_points = out.rstrip("\n")
        fuse_mount_points = mount_points.split("\n")
        out, rc = client1.exec_command(
            sudo=True, cmd="mount -t ceph | awk {'print $3'}", check_ec=False
        )
        mount_points = out.rstrip("\n")
        kernel_mount_points = mount_points.split("\n")
        total_mounts = fuse_mount_points + kernel_mount_points
        client1.exec_command(
            sudo=True, cmd=f"mkdir {total_mounts[0]}/run_ios", check_ec=False
        )
        log.info("Successfully created run_ios folder in mount directory")
        run_start_time = datetime.now()
        if timeout:
            stop = datetime.now() + timedelta(seconds=timeout)
        else:
            stop = 0
        while True:
            if stop and datetime.now() > stop:
                log.info("Timed out *************************")
                break
            function_called = fs_util.run_ios(client1, f"{total_mounts[0]}/run_ios")
            stats[function_called.__name__] += 1
            stats["total_iterations"] += 1
        return 0
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        run_end_time = datetime.now()
        duration = divmod((run_end_time - run_start_time).total_seconds(), 60)
        log.info(
            "---------------------------------------------------------------------"
        )
        log.info("Test Summary")
        log.info(
            "---------------------------------------------------------------------"
        )
        log.info(f"Total Duration: {int(duration[0])} mins, {int(duration[1])} secs")
        log.info(f"Total Iterations: {stats['total_iterations']}")
        log.info(f"Total no of smallfile executions: {stats['smallfile']}")
        log.info(f"Total no of DD executions: {stats['dd']}")
        log.info(
            "---------------------------------------------------------------------"
        )
