import timeit
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utils import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    try:
        start = timeit.default_timer()
        bz = "1798719"
        log.info("Running cephfs test for bug %s" % bz)
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        client_info, rc = fs_util.get_clients(build)
        if rc == 0:
            log.info("Got client info")
        else:
            raise CommandFailed("fetching client info failed")
        client1 = []
        client1.append(client_info["kernel_clients"][0])
        mon_node_ip = client_info["mon_node_ip"]
        mounting_dir = client_info["mounting_dir"]
        user_name = "qwertyuiopasdfghjklzxcvbnm1234567890123"
        p_flag = "rw"
        log.info("Creating user with more than 37 letters")
        for client in client1:
            client.exec_command(
                cmd="sudo ceph auth get-or-create client.%s "
                "mon 'allow r' mds "
                "'allow %s' osd 'allow rw' "
                "-o /etc/ceph/ceph.client.%s.keyring" % (user_name, p_flag, user_name)
            )
            log.info("Creating mounting dir:")
            client.exec_command(cmd="sudo mkdir %s" % (mounting_dir))
            out, rc = client.exec_command(
                cmd="sudo ceph auth get-key client.%s" % (user_name)
            )
            secret_key = out.rstrip("\n")
            key_file = client.remote_file(
                sudo=True, file_name="/etc/ceph/%s.secret" % (user_name), file_mode="w"
            )
            key_file.write(secret_key)
            key_file.flush()
            op, rc = client.exec_command(
                cmd="sudo mount -t ceph %s,%s,%s:/ "
                "%s -o name=%s,secretfile=/etc/ceph/%s.secret"
                % (
                    mon_node_ip[0],
                    mon_node_ip[1],
                    mon_node_ip[2],
                    mounting_dir,
                    user_name,
                    user_name,
                )
            )
            out, rc = client.exec_command(cmd="mount")
            mount_output = out.split()
            log.info("Checking if kernel mount is passed or failed:")
            assert mounting_dir.rstrip("/") in mount_output
            log.info("mount is passed")
            log.info("Execution of Test for bug %s ended:" % (bz))
            print("Script execution time:------")
            stop = timeit.default_timer()
            total_time = stop - start
            mins, secs = divmod(total_time, 60)
            hours, mins = divmod(mins, 60)
            print("Hours:%d Minutes:%d Seconds:%f" % (hours, mins, secs))
            return 0

    except CommandFailed as e:
        log.info(e)
        log.info(traceback.format_exc())
        log.info("Cleaning up!-----")
        if client1[0].pkg_type != "deb":
            rc = fs_util.client_clean_up(
                client_info["kernel_clients"], client_info["mounting_dir"], "umount"
            )
            if rc == 0:
                log.info("Cleaning up successfull")

            else:
                return 1

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
