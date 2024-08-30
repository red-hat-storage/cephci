import json
import random
import secrets
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83575095 - Upgrade a stand-alone NFS Cluster to HA and verify all the
                    exports are intact and functioning as before    Procedure
    1. Log into the Cephadm shell:
        Example
        [root@host01 ~]# cephadm shell
    2. Create the standalone NFS cluster
    3. Validate the Service is UP
    4. Write IO on export.
    5. Convert the standalone NFS cluster to NFS-HA with ingress
    6. Re-Mount the existing export using the Virtual IP.
    7. write IO.
    8. Cleanup
    """
    try:
        tc = "CEPH-83575095"
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
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        virtual_ip = "10.8.128.100"
        subnet = "21"
        nfs_name = "cephnfs"
        port = "7007"
        nfs_conf = {
            "service_type": "nfs",
            "service_id": nfs_name,
            "placement": {
                "hosts": [nfs_servers[0].node.hostname, nfs_servers[1].node.hostname],
                "count": 1,
            },
            "spec": {"port": port},
        }
        ingress_conf = {
            "service_type": "ingress",
            "service_id": f"nfs.{nfs_name}",
            "placement": {
                "hosts": [nfs_servers[0].node.hostname, nfs_servers[1].node.hostname],
                "count": 2,
            },
            "spec": {
                "backend_service": f"nfs.{nfs_name}",
                "frontend_port": "3049",
                "monitor_port": "8000",
                "virtual_ip": f"{virtual_ip}/{subnet}",
            },
        }

        log.info("checking Pre-requisites")
        if not clients:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1

        client1.exec_command(sudo=True, cmd="ceph mgr module enable nfs")
        client1.exec_command(sudo=True, cmd=f"echo '{nfs_conf}' > nfs.yaml")
        client1.exec_command(sudo=True, cmd="ceph orch apply -i nfs.yaml")
        log.info("validate the NFS services")
        fs_util_v1.validate_services(client1, f"nfs.{nfs_name}")

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

        command = f"mount -t nfs -o port={port} {nfs_servers[0].node.hostname}:{nfs_export_name} {nfs_mounting_dir}"
        client1.exec_command(sudo=True, cmd=command, check_ec=False)

        dir_name = "smallfile_dir"
        client1.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}{dir_name}")
        smallfile(clients[0], nfs_mounting_dir, dir_name)

        client1.exec_command(sudo=True, cmd=f"echo '{ingress_conf}' > ingress.yaml")
        client1.exec_command(sudo=True, cmd="ceph orch apply -i ingress.yaml")
        log.info("validate the Ingress services")
        fs_util_v1.validate_services(client1, f"ingress.nfs.{nfs_name}")

        log.info("Unmount and remount using Virtual IP")
        client1.exec_command(
            sudo=True, cmd=f"umount -l {nfs_mounting_dir}", check_ec=False
        )
        command = f"mount -t nfs -o port={port} {virtual_ip}:{nfs_export_name} {nfs_mounting_dir}"
        client1.exec_command(sudo=True, cmd=command, check_ec=False)
        log.info(
            f"Validate if the {dir_name} exists, delete the dir and rerun IOs on new mount path"
        )
        validate_nfs_ha_server(client1, nfs_mounting_dir, dir_name)

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
            sudo=True, cmd=f"ceph nfs cluster rm {nfs_name}", check_ec=False
        )


def smallfile(client, mounting_dir, dir_name):
    client.exec_command(
        sudo=True,
        cmd=f"python3 /home/cephuser/smallfile/smallfile_cli.py --operation create --threads 8 --file-size 10240 "
        f"--files 10 --top {mounting_dir}{dir_name}",
    )


@retry(CommandFailed, tries=5, delay=5)
def validate_nfs_ha_server(client, nfs_mounting_dir, dir_name):
    try:
        client.exec_command(sudo=True, cmd=f"ls -l {nfs_mounting_dir}{dir_name}")
        client.exec_command(sudo=True, cmd=f"rm -rf {nfs_mounting_dir}{dir_name}")
        client.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}{dir_name}")
        smallfile(client, nfs_mounting_dir, dir_name)
    except CommandFailed:
        raise CommandFailed("Failed to upgrade a standalone Server to NFS-HA Server")
