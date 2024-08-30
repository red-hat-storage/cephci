import json
import secrets
import string
import traceback
from json import JSONDecodeError

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.cephfs_volume_management import wait_for_process
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83573999 - Try resetting the config to replace with the default values and
                    verify if user defined changes are cleared with default values.
    Pre-requisites:
    1. Create cephfs volume
       creats fs volume create <vol_name>
    2. Create nfs cluster
       ceph nfs cluster create <nfs_name> <nfs_server>
    Test operation:
    1. Create nfs cluster with same name
       ceph nfs cluster create <nfs_name> <nfs_server>
    2. Create Export file with all the details
    3. Create cephfs nfs export using apply with abpve file created
       ceph nfs export apply cephfs-nfs -i export_file_1.conf

    4. Mount nfs mount with cephfs export
       mount -t nfs -o port=2049 <nfs_server>:<nfs_export> <nfs_mounting_dir>
    5. Run IOs and all should pass
    6. Set the nfs cluster conf using configuration file
        ceph nfs cluster config set cephfs-nfs -i nfs_log.conf
    7. Get the config and veify the set values
         ceph nfs cluster config get cephfs-nfs
    8. Reset the config using reset command
        ceph nfs cluster config reset cephfs-nfs
    9. Verify set values have been removed

    Clean-up:
    1. Remove data in cephfs
    2. Unmount the export
    3. Remove cephfs nfs export
    4. Remove all nfs mounts
    5. Remove nfs cluster
    6. Remove all the configs created as part of test cases.
    """
    try:
        tc = "CEPH-83573999"
        log.info(f"Running cephfs {tc} test case")

        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_server = nfs_servers[0].node.hostname
        nfs_name = "cephfs-nfs"
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        nfs_mounting_dir = "/mnt/nfs_" + "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for i in range(5)
        )
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph nfs cluster create {nfs_name} {nfs_server}"
        )
        if not wait_for_process(client=client1, process_name=nfs_name, ispresent=True):
            raise CommandFailed("Cluster has not been created")
        try:
            out, rc = client1.exec_command(sudo=True, cmd="ceph nfs cluster ls -f json")
            output = json.loads(out)
        except JSONDecodeError:
            output = json.dumps([out])
        if nfs_name in output:
            log.info("ceph nfs cluster created successfully")
        else:
            raise CommandFailed("Failed to create nfs cluster")
        nfs_export_name = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        export_id = 100
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util.create_fs(client1, fs_name)
        out, _ = clients[0].exec_command(
            cmd=f"ceph auth get-key client.{clients[0].node.hostname}", sudo=True
        )
        log.info("Output : %s" % out)
        export_fstr = f"""EXPORT {{
          Export_Id = {export_id};
          Transports = TCP;
          Path = /;
          Pseudo = {nfs_export_name};
          Protocols = 4;
          Access_Type = RW;
          Attr_Expiration_Time = 0;
          Squash = None;
          FSAL {{
            Name = CEPH;
            Filesystem = {fs_name};
            User_Id = nfs.cephfs-nfs.{export_id};
          }}
        }}"""

        # Push the export_fstr to a file
        filename = "export_file.conf"
        export_file = clients[0].remote_file(
            sudo=True,
            file_name=filename,
            file_mode="w",
        )
        export_file.write(export_fstr)
        export_file.flush()
        clients[0].exec_command(
            cmd=f"ceph nfs export apply {nfs_name} -i {filename}", sudo=True
        )
        out, rc = clients[0].exec_command(
            cmd=f"ceph nfs export get {nfs_name} {nfs_export_name}", sudo=True
        )
        log.info(out)
        fs_util.cephfs_nfs_mount(client1, nfs_server, nfs_export_name, nfs_mounting_dir)
        commands = [
            f"mkdir {nfs_mounting_dir}/dir1",
            f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 10 --file-size 4 --files"
            f" 1000 --files-per-dir 10 --dirs-per-dir 2 --top {nfs_mounting_dir}/dir1",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command, long_running=True)
        nfs_filename = "nfs_cluster.conf"
        nfs_cluster_conf = clients[0].remote_file(
            sudo=True,
            file_name=nfs_filename,
            file_mode="w",
        )
        nfs_cluster_fstr = """LOG {
    COMPONENTS {
        ALL = FULL_DEBUG;
    }
}        """
        nfs_cluster_conf.write(nfs_cluster_fstr)
        nfs_cluster_conf.flush()
        clients[0].exec_command(
            cmd=f"ceph nfs cluster config set {nfs_name} -i {nfs_filename}", sudo=True
        )
        out, rc = clients[0].exec_command(
            cmd=f"ceph nfs cluster config get {nfs_name}", sudo=True
        )
        log.info(out)
        if out.strip() != nfs_cluster_fstr.strip():
            log.error(
                f"NFS configs not set properly expected: {nfs_cluster_conf} but \n it has {out}"
            )
            return 1
        clients[0].exec_command(
            cmd=f"ceph nfs cluster config reset {nfs_name}", sudo=True
        )
        out, rc = clients[0].exec_command(
            cmd=f"ceph nfs cluster config get {nfs_name}", sudo=True
        )
        if out.strip() == nfs_cluster_fstr.strip():
            log.error(
                f"NFS configs reset properly expected: {nfs_cluster_conf} but \n it has {out}"
            )
            return 1
        log.info(out)

        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning up the system")
        commands = [
            f"rm -rf {nfs_mounting_dir}/*",
            f"umount {nfs_mounting_dir}",
            f"ceph nfs export delete {nfs_name} {nfs_export_name}",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command)
        client1.exec_command(
            sudo=True, cmd=f"rm -rf {nfs_mounting_dir}/", check_ec=False
        )
        client1.exec_command(sudo=True, cmd=f"rm -rf {filename}", check_ec=False)
        client1.exec_command(sudo=True, cmd=f"rm -rf {nfs_filename}", check_ec=False)
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs cluster delete {nfs_name}",
            check_ec=False,
        )
