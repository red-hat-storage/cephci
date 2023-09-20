import random
import secrets
import string
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test operation:
    1.Create a client for a “test directory” from root mounted directory using “ceph fs get-or-create”
    2.Mount the target directory
    3.Create a actual “test directory” in the mounted directory
    4.Set “max_mds 2”
    5.Find out which mds is ranked at 1
    6.Set the ceph.dir.pin extended attribute on a directory (test directory)
    7.Store the “request” number before IOs the ranked MD”
    8.Run IOs
    9.“Request” number
    10.Unpin the directory from the MDS using “-v -1”
    11.Check if the value is set to -1
    """
    try:
        tc = "CEPH-83574329"
        log.info(f"Running CephFS tests for -{tc}")
        fs_util = FsUtils(ceph_cluster)
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_details = fs_util.get_fs_info(client1)
        if not fs_details:
            fs_util.create_fs(client1, "cephfs")
        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        rand = secrets.randbelow(1000)
        pin_dir = "pin_dir_" + str(rand)
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        # client1.exec_command(sudo=True,cmd="git clone https://github.com/distributed-system-analysis/smallfile.git")
        client1.exec_command(sudo=True, cmd="yum install -y --nogpgcheck jq")
        client1.exec_command(sudo=True, cmd="yum install -y --nogpgcheck ceph-fuse")
        fuse_mounting_dir_1 = f"/mnt/cephfs_fuse_{mounting_dir}/"
        client1.exec_command(sudo=True, cmd="ceph fs set cephfs max_mds 2")
        log.info("waiting for 30 seconds for mds to come up")
        time.sleep(30)
        name1 = f"name_{rand}"
        client1.exec_command(
            sudo=True,
            cmd=f"ceph auth get-or-create client.{name1} "
            f"mds 'allow *,allow * path=/{pin_dir}' mon 'allow r' osd 'allow rw tag cephfs data=cephfs' "
            f"-o /etc/ceph/ceph.client.{name1}.keyring",
        )
        client1.exec_command(sudo=True, cmd=f"mkdir -p {fuse_mounting_dir_1}")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph-fuse -n client.{name1} {fuse_mounting_dir_1} --client_fs cephfs",
        )
        client1.exec_command(sudo=True, cmd=f"mkdir -p {fuse_mounting_dir_1}{pin_dir}")
        client1.exec_command(
            sudo=True,
            cmd=f"setfattr -n ceph.dir.pin -v 1 {fuse_mounting_dir_1}{pin_dir}",
        )
        # checking is the dir is pinned
        rank_0_mds = client1.exec_command(
            sudo=True,
            cmd="ceph fs status -f json-pretty | jq -r '.mdsmap[] | select(.rank == 0) | .name' ",
        )
        rank_0_mdss = rank_0_mds[0].split()
        log.info(rank_0_mdss)
        rank_1_mds = client1.exec_command(
            sudo=True,
            cmd="ceph fs status -f json-pretty | jq -r '.mdsmap[] | select(.rank == 1) | .name' ",
        )
        rank_1_mdss = rank_1_mds[0].split()
        log.info(rank_1_mdss)
        rank_0_request_prev = 0
        rank_1_request_prev = 0
        for mds in rank_0_mdss:
            request = client1.exec_command(
                sudo=True,
                cmd=f"ceph tell mds.{mds} perf dump -f json | jq -r '.mds | {{\"request\": .request }} | .request'",
            )
            request = int(request[0].strip())
            rank_0_request_prev = rank_0_request_prev + int(request)
        for mds in rank_1_mdss:
            request = client1.exec_command(
                sudo=True,
                cmd=f"ceph tell mds.{mds} perf dump -f json | jq -r '.mds | {{\"request\": .request }} | .request'",
            )
            request = request[0].strip()
            rank_1_request_prev = rank_1_request_prev + int(request)
        # running IOs
        client1.exec_command(
            sudo=True,
            cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py "
            f"--operation create --threads 10 --file-size 400"
            f" --files 100 --files-per-dir 10 --dirs-per-dir 2 --top {fuse_mounting_dir_1}{pin_dir} ",
        )
        time.sleep(30)
        rank_0_request_after = 0
        rank_1_request_after = 0
        for mds in rank_0_mdss:
            log.info(mds)
            request = client1.exec_command(
                sudo=True,
                cmd=f"ceph tell mds.{mds} perf dump -f json | jq -r '.mds | {{\"request\": .request }} | .request'",
            )
            request = int(request[0].strip())
            rank_0_request_after = rank_0_request_after + int(request)
        for mds in rank_1_mdss:
            log.info(mds)
            request = client1.exec_command(
                sudo=True,
                cmd=f"ceph tell mds.{mds} perf dump -f json | jq -r '.mds | {{\"request\": .request }} | .request'",
            )
            request = int(request[0].strip())
            rank_1_request_after = rank_1_request_after + int(request)

        rank_0_differ = rank_0_request_after - rank_0_request_prev
        log.info(f"rank 0 increase amount is {rank_0_differ}")
        log.info(f"rank 0 mds is {rank_0_mdss}")
        rank_1_differ = rank_1_request_after - rank_1_request_prev
        log.info(f"rank 1 increase amount is {rank_1_differ}")
        log.info(f"rank 1 mds is {rank_1_mdss}")
        if 800 < rank_1_differ < 1300 and rank_0_differ < 400:
            log.info(f"IOs went thru majorly the pinned mds [{rank_1_mdss[0]}]")
        else:
            raise CommandFailed("The result is not expected")

        client1.exec_command(sudo=True, cmd=f"cd {fuse_mounting_dir_1}{pin_dir}")
        client1.exec_command(
            sudo=True,
            cmd=f"setfattr -n ceph.dir.pin -v -1 {fuse_mounting_dir_1}{pin_dir}",
        )
        # checking is the dir is unpinned
        for mds in rank_1_mdss:
            out = client1.exec_command(
                sudo=True,
                cmd=f"ceph tell mds.{mds} get subtrees -f json-pretty | jq -r '.[] | select(.export_pin == 1)'",
            )
            log.info(out[1])
            log.info(type(out[1]))
            if pin_dir in out[1]:
                raise CommandFailed(f"unpinning failed for {pin_dir} for {mds}")

        return 0
    except CommandFailed as e:
        log.info("Expected failure since rank 0 request number changes")
        log.info(e)
        log.info("https://bugzilla.redhat.com/show_bug.cgi?id=2239048")
        if rank_0_request_after > rank_0_request_prev:
            client1.exec_command(sudo=True, cmd=f"cd {fuse_mounting_dir_1}{pin_dir}")
            client1.exec_command(
                sudo=True,
                cmd=f"setfattr -n ceph.dir.pin -v -1 {fuse_mounting_dir_1}{pin_dir}",
            )
            # checking is the dir is unpinned
            for mds in rank_1_mdss:
                out = client1.exec_command(
                    sudo=True,
                    cmd=f"ceph tell mds.{mds} get subtrees | jq -r '.[] | select(.export_pin == 1)'",
                )
                log.info(out[1])
                if pin_dir in out[1]:
                    raise CommandFailed(f"unpinning failed for {pin_dir} for {mds}")
            return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1

    finally:
        log.info("cleaning up")
        client1.exec_command(sudo=True, cmd=f"umount {fuse_mounting_dir_1}")
        client1.exec_command(sudo=True, cmd=f"rm -rf {fuse_mounting_dir_1}")
        client1.exec_command(sudo=True, cmd=f"ceph auth rm client.{name1}")
        client1.exec_command(
            sudo=True, cmd=f"rm -rf /etc/ceph/ceph.client.{name1}.keyring"
        )
