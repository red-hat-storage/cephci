import json
import secrets
import string
import threading
import time
import traceback
from datetime import datetime

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        """
        CEPH-83600107 - Adjust mds_dir_max_commit_size to lower size and validate the MDS States
        Steps:
        1. Create a filesystem  cephfs_max_commit_size
        2. set max_mds to 1
        3. set mds mds_dir_max_commit_size 1 default value is 10
        4. mount the FS and run smallfile ios
        5. monitor MDS states
        """
        tc = "CEPH-83600107"
        log.info("Running cephfs %s test case" % (tc))
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        client1 = clients[0]
        fs_name = "cephfs_max_commit_size"
        fs_util.create_fs(client1, fs_name)
        mds_nodes = ceph_cluster.get_nodes("mds")
        host_list = [node.hostname for node in mds_nodes]
        hosts = " ".join(host_list)
        client1.exec_command(
            sudo=True,
            cmd=f"ceph orch apply mds {fs_name} --placement='3 {hosts}'",
            check_ec=False,
        )
        client1.exec_command(sudo=True, cmd=f"ceph fs set {fs_name} max_mds 1")
        default_commit_size, rc = client1.exec_command(
            sudo=True, cmd="ceph config get mds mds_dir_max_commit_size"
        )
        log.info(f"default value of mds_dir_max_commit_size is {default_commit_size}")
        log.info("Setting the mds_dir_max_commit_size to 1")
        client1.exec_command(
            sudo=True, cmd="ceph config set mds mds_dir_max_commit_size 1"
        )
        fuse_mount_dir = "/mnt/fuse_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        fs_util.fuse_mount(
            [client1],
            fuse_mount_dir,
            new_client_hostname="admin",
            extra_params=f" --client_fs {fs_name}",
        )

        # Start monitoring MDS state in a separate thread
        monitor_thread = threading.Thread(
            target=monitor_mds_state, args=(client1, fs_name)
        )
        monitor_thread.start()  # This starts monitoring in the background

        # Execute smallfile workload
        client1.exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create "
            f"--threads 1 --file-size 1 --files 1048576 --top {fuse_mount_dir}",
            timeout=10000,
        )

        # Perform cleanup after workload
        client1.exec_command(
            sudo=True, cmd=f"rm -rf {fuse_mount_dir}", check_ec=False, timeout=10000
        )

        # Ensure the monitor thread ends after the rm -rf operation
        monitor_thread.do_run = False  # Signal to stop monitoring
        monitor_thread.join()  # Wait for the monitoring thread to finish
        log.info(
            "Validating the FS still has 1 active MDS after the end of the test. Waiting for 5 min"
        )
        retry_mds_status = retry(CommandFailed, tries=3, delay=30)(
            fs_util.get_mds_status
        )
        retry_mds_status(client1, 1, vol_name=fs_name)
        return 0

    except CommandFailed as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        client1.exec_command(sudo=True, cmd=f"umount {fuse_mount_dir}", check_ec=False)
        client1.exec_command(
            sudo=True,
            cmd=f"ceph config set mds mds_dir_max_commit_size {default_commit_size}",
        )
        client1.exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        fs_util.remove_fs(client1, fs_name)


def monitor_mds_state(client1, fs_name, interval=5):
    """
    Monitors the MDS states and logs changes with a datetime stamp.
    Runs in a separate thread and stops when signaled.

    Parameters:
    client1: Object to execute commands on the Ceph cluster.
    interval (int): Time interval in seconds between health checks.
    """
    thread = threading.currentThread()
    previous_mds_states = {}

    while getattr(thread, "do_run", True):
        try:
            # Run command to get the current Ceph MDS state in JSON format
            out, rc = client1.exec_command(
                sudo=True, cmd=f"ceph fs status {fs_name} -f json"
            )
            current_data = json.loads(out)

            current_mds_states = {}

            # Extract current MDS states (name and state)
            for mds in current_data["mdsmap"]:
                name = mds["name"]
                state = mds["state"]
                current_mds_states[name] = state

            log.info(f"Current MDS states: {current_mds_states}")

            # Check for changes in the MDS states
            for mds_name, current_state in current_mds_states.items():
                previous_state = previous_mds_states.get(mds_name)

                if previous_state is None:
                    previous_mds_states[mds_name] = current_state
                elif current_state != previous_state:
                    log.info(f"{datetime.now()} - MDS state changed!\n")
                    log.info(f"MDS: {mds_name}\n")
                    log.info(f"Previous state: {previous_state}\n")
                    log.info(f"Current state: {current_state}\n")
                    previous_mds_states[mds_name] = current_state

                    log.info(f"MDS {mds_name} changed state at {datetime.now()}")

        except Exception as e:
            log.error(f"Exception occurred: {e}")

        time.sleep(interval)
