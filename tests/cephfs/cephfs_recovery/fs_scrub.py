import json
import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered :
    CEPH-11267 - Run CephFS IO while PG's in scrubbing, deep-scrubbing and degraded state

    Test Steps :
    1. We need atleast one client node to execute this test case
    2. create fs volume create cephfs if the volume is not there
    3. write data on to the fs
    4. Start the scrub
    5. Pause the scrub
    6. Resume the scrub
    7. Abort the scrub

    Clean Up:
    1. Del all the snapshots created
    2. Del Subvolumes
    3. Del SubvolumeGroups
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        log.info("checking Pre-requisites")
        if len(clients) < 1:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        default_fs = "cephfs-scrub"
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1, fs_name=default_fs)
        if not fs_details:
            fs_util.create_fs(client1, default_fs)
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )

        kernel_mounting_dir_1 = f"/mnt/cephfs_kernel{mounting_dir}_1/"
        mon_node_ips = fs_util.get_mon_node_ips()
        retry_mount = retry(CommandFailed, tries=3, delay=30)(fs_util.kernel_mount)

        retry_mount([client1], kernel_mounting_dir_1, ",".join(mon_node_ips))

        retry_mount(
            [client1],
            kernel_mounting_dir_1,
            ",".join(mon_node_ips),
            extra_params=f",fs={default_fs}",
        )

        client1.exec_command(
            sudo=True,
            cmd=f"mkdir -p {kernel_mounting_dir_1}/{mounting_dir};python3 /home/cephuser/smallfile/smallfile_cli.py "
            f"--operation create --threads 10 --file-size 1 "
            f"--files 10000 --files-per-dir 100000 --dirs-per-dir 2 --top "
            f"{kernel_mounting_dir_1}/{mounting_dir}",
            long_running=True,
        )
        result, rc = clients[0].exec_command(
            sudo=True, cmd=f"ceph fs status {default_fs} --format json-pretty"
        )

        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph tell mds.{default_fs}:0 scrub start / recursive"
        )
        status = json.loads(out)
        scrub_tag = status["scrub_tag"]
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph tell mds.{default_fs}:0 scrub pause --format json"
        )
        status = json.loads(out)
        if status["return_code"] != 0:
            log.error(
                f"Return code for pause is expected to be 0 but actual output {status}"
            )
            return 1
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph tell mds.{default_fs}:0 scrub status --format json"
        )
        status = json.loads(out)
        if "PAUSED" not in status["status"]:
            log.error(f"Expected status is PAUSED but actual output {status}")
            return 1
        if status["scrubs"][scrub_tag]["tag"] != scrub_tag:
            log.error(f"Expected scrub tag is {scrub_tag} but actual output {status}")
            return 1
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph tell mds.{default_fs}:0 scrub resume --format json"
        )
        status = json.loads(out)
        if status["return_code"] != 0:
            log.error(
                f"Return code for pause is expected to be 0 but actual output {status}"
            )
            return 1
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph tell mds.{default_fs}:0 scrub status --format json"
        )
        status = json.loads(out)
        if status["scrubs"][scrub_tag]["tag"] != scrub_tag:
            log.error(f"Expected scrub tag is {scrub_tag} but actual output {status}")
            return 1
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph tell mds.{default_fs}:0 scrub abort --format json"
        )
        # log.info(json.loads(out))
        status = json.loads(out)
        if status["return_code"] != 0:
            log.error(
                f"Return code for pause is expected to be 0 but actual output {status}"
            )
            return 1
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph tell mds.{default_fs}:0 scrub status --format json"
        )
        status = json.loads(out)
        if status["scrubs"][scrub_tag]["tag"] != scrub_tag:
            log.error(f"Expected scrub tag is {scrub_tag} but actual output {status}")
            return 1
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph tell mds.{default_fs}:0 scrub resume --format json"
        )
        status = json.loads(out)
        if status["return_code"] != 0:
            log.error(
                f"Return code for pause is expected to be 0 but actual output {status}"
            )
            return 1
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph tell mds.{default_fs}:0 scrub status --format json"
        )
        status = json.loads(out)
        if status["scrubs"][scrub_tag]["tag"] != scrub_tag:
            log.error(f"Expected scrub tag is {scrub_tag} but actual output {status}")
            return 1
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        log.info("Clean Up in progess")
        clients[0].exec_command(
            sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
        )
        fs_util.remove_fs(clients[0], vol_name=default_fs, validate=False)
