import json
import random
import secrets
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83575092 - Create a 2 Node - CephFS NFS HA Cluster from the Cephadm shell and
                    check if the cluster has come up as expected and validate all the cluster information
    Procedure
    Scenario 1 - Create default NFS HA Deployment.
    1. Log into the Cephadm shell:
        Example
        [root@host01 ~]# cephadm shell
    2. Create the NFS cluster with the --ingress flag:
        Syntax
        ceph nfs cluster create CLUSTER-ID [PLACEMENT] [--port PORT_NUMBER] [--ingress --virtual-ip IP_ADDRESS]
    3. Validate the Service is UP
    4. Mount using the Virtual IP.
    5. Create Export and write IO.
    6. Cleanup
    """
    try:
        tc = "CEPH-83575092"
        log.info(f"Running cephfs {tc} test case")
        config = kw["config"]
        build = config.get("build", config.get("rhbuild"))
        test_data = kw.get("test_data")
        fs_util_v1 = FsUtilsV1(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtilsV1.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        clients = ceph_cluster.get_ceph_objects("client")
        client1 = clients[0]
        fs_util_v1.prepare_clients(clients, build)
        fs_util_v1.auth_list(clients)
        nfs_name = "cephfs-nfs"
        virtual_ip = "10.8.128.100"
        subnet = "21"

        log.info("checking Pre-requisites")
        if not clients:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1

        client1.exec_command(sudo=True, cmd="ceph mgr module enable nfs")
        client1 = clients[0]
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_name = "cephnfs"
        client1.exec_command(
            sudo=True,
            cmd=f'ceph nfs cluster create {nfs_name} "2 {nfs_servers[0].node.hostname} '
            f'{nfs_servers[1].node.hostname} {nfs_servers[2].node.hostname}" '
            f"--ingress --virtual-ip {virtual_ip}/{subnet}",
        )

        log.info("validate the services hace started on nfs")
        fs_util_v1.validate_services(client1, f"nfs.{nfs_name}")
        fs_util_v1.validate_services(client1, f"ingress.nfs.{nfs_name}")

        nfs_export_name = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        export_path = "/"
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util_v1.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util_v1.create_fs(client1, fs_name)
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export create cephfs {nfs_name} "
            f"{nfs_export_name} {fs_name} path={export_path}",
        )
        out, rc = client1.exec_command(sudo=True, cmd=f"ceph nfs export ls {nfs_name}")

        if nfs_export_name not in out:
            raise CommandFailed("Failed to create nfs export")

        log.info("ceph nfs export created successfully")
        out, rc = client1.exec_command(
            sudo=True, cmd=f"ceph nfs export get {nfs_name} {nfs_export_name}"
        )
        json.loads(out)
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        nfs_mounting_dir = f"/mnt/cephfs_nfs{mounting_dir}_1/"
        client1.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}")
        command = f"mount -t nfs -o port=2049 {virtual_ip}:{nfs_export_name} {nfs_mounting_dir}"
        client1.exec_command(sudo=True, cmd=command, check_ec=False)
        dir_name = "smallfile_dir"
        client1.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}{dir_name}")
        smallfile(clients[0], nfs_mounting_dir, dir_name)

        log.info("Test completed successfully")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning Up")
        client1.exec_command(sudo=True, cmd=f"rm -rf {nfs_mounting_dir}*")
        log.info("Unmount NFS export")
        client1.exec_command(
            sudo=True, cmd=f"umount -l {nfs_mounting_dir}", check_ec=False
        )
        log.info("Removing the Export")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export delete {nfs_name} {nfs_export_name}",
            check_ec=False,
        )
        log.info("Removing NFS Cluster")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs cluster rm {nfs_name}",
            check_ec=False,
        )


def smallfile(client, mounting_dir, dir_name):
    client.exec_command(
        sudo=True,
        cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 8 --file-size 10240 "
        f"--files 10 --top {mounting_dir}{dir_name}",
    )
