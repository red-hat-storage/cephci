import random
import re
import secrets
import string
import time
import traceback

from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83575085 - Create an export on the NFS cluster and mount it using all
                    supported NFS versions(4,1,4.2) using the backend servers as well as the ingress
                    and perform IO as usual.
    Test operation:
        1. Create a nfs cluster
        2. Create a nfs export
        3. Create 2 exports
        4. Mount 1st export with nfs version 4.1 and run IOs
        5. Mount 2nd export with nfs version 4.2 and run IOs
        6. Perform HA Failover.
        7. Mount the exports using NFS backend servers and write IOs
    Clean up :
        1. Remove the data written to the mounts.
        2. Remove the NFS dirs
        3. Unmount the nfs exports
        4. Remove the exports
        5. Remove the Cluster.
    """
    try:
        tc = "CEPH-83575085"
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
        fs_util_v1.prepare_clients(clients, build)
        fs_util_v1.auth_list(clients)
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_name = "cephfs-nfs"
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        client1 = clients[0]
        fs_details = fs_util_v1.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util_v1.create_fs(client1, fs_name)
        virtual_ip = "10.8.128.100"
        subnet = "21"
        port = "2049"
        export_path = "/"
        nfs_v41 = "4.1"
        nfs_v42 = "4.2"
        nfs_v4 = "4"

        log.info("checking Pre-requisites")
        if not clients:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1
        log.info("Enable NFS mgr module")
        client1.exec_command(sudo=True, cmd="ceph mgr module enable nfs")

        log.info("Create NFS Cluster with Ingress")
        client1.exec_command(
            sudo=True,
            cmd=f'ceph nfs cluster create {nfs_name} "2 {nfs_servers[0].node.hostname} '
            f'{nfs_servers[1].node.hostname} {nfs_servers[2].node.hostname}" '
            f"--ingress --virtual-ip {virtual_ip}/{subnet}",
        )
        log.info("validate the services have started on nfs")
        fs_util_v1.validate_services(client1, f"nfs.{nfs_name}")
        fs_util_v1.validate_services(client1, f"ingress.nfs.{nfs_name}")

        log.info("Create NFS Exports")
        nfs_export_name_1 = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        nfs_export_name_2 = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        nfs_export_name_3 = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        mount_dir = []
        for nfs_export in [nfs_export_name_1, nfs_export_name_2, nfs_export_name_3]:
            fs_util_v1.create_nfs_export(
                client1, nfs_name, nfs_export, fs_name, path=export_path
            )
            log.info(nfs_export)

        log.info("Mount the exports using Virtual IP")
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        nfs_mounting_dir1 = f"/mnt/cephfs_nfs{mounting_dir}_1/"
        nfs_mounting_dir2 = f"/mnt/cephfs_nfs{mounting_dir}_2/"
        nfs_mounting_dir3 = f"/mnt/cephfs_nfs{mounting_dir}_3/"
        fs_util_v1.cephfs_nfs_mount_ingress(
            client1, nfs_v41, virtual_ip, nfs_export_name_1, nfs_mounting_dir1, port
        )
        fs_util_v1.cephfs_nfs_mount_ingress(
            client1, nfs_v42, virtual_ip, nfs_export_name_2, nfs_mounting_dir2, port
        )
        fs_util_v1.cephfs_nfs_mount_ingress(
            client1, nfs_v42, virtual_ip, nfs_export_name_3, nfs_mounting_dir3, port
        )
        mount_dir.append(nfs_mounting_dir1)
        mount_dir.append(nfs_mounting_dir2)
        mount_dir.append(nfs_mounting_dir3)

        log.info("Validate the nfs protocol version from clients after mounting")
        nfs_exports = [nfs_export_name_1, nfs_export_name_2, nfs_export_name_3]
        valid_versions = [nfs_v41, nfs_v42, nfs_v4]
        check_mounted_exports(client1, nfs_exports, valid_versions)

        log.info("Perform Failover while the IOs are running")
        backend_server = fs_util_v1.get_active_nfs_server(
            client1, nfs_name, ceph_cluster
        )
        log.info(backend_server)
        for server in backend_server:
            log.info(f"Active backend servers {server.node.hostname}")
        dir_name = "smallfile_dir"
        for i, mount in enumerate(mount_dir):
            client1.exec_command(sudo=True, cmd=f"mkdir -p {mount}{dir_name}_{i}")
        with parallel() as p:
            p.spawn(fs_util_v1.write_io, client1, mount_dir)
            for i in backend_server:
                fs_util_v1.reboot_node(i, timeout=300)
                time.sleep(120)
        backend_server_after_failover = fs_util_v1.get_active_nfs_server(
            client1, nfs_name, ceph_cluster
        )
        for server in backend_server_after_failover:
            log.info(f"Active backend servers after reboots {server.node.hostname}")

        log.info(
            "Mount the nfs share with 4.1 and 4.2 and 4 on all the NFS backend servers"
        )
        nfs_backend_dir1 = f"/mnt/nfs_{mounting_dir}_bk_1/"
        nfs_backend_dir2 = f"/mnt/nfs_{mounting_dir}_bk_2/"
        nfs_backend_dir3 = f"/mnt/nfs_{mounting_dir}_bk_3/"
        backend_server1 = backend_server_after_failover[0].node.hostname
        backend_server2 = backend_server_after_failover[1].node.hostname
        fs_util_v1.cephfs_nfs_mount_ingress(
            client1,
            nfs_v41,
            backend_server1,
            nfs_export_name_1,
            nfs_backend_dir1,
            f"1{port}",
        )
        fs_util_v1.cephfs_nfs_mount_ingress(
            client1,
            nfs_v42,
            backend_server2,
            nfs_export_name_2,
            nfs_backend_dir2,
            f"1{port}",
        )
        fs_util_v1.cephfs_nfs_mount_ingress(
            client1,
            nfs_v4,
            backend_server2,
            nfs_export_name_3,
            nfs_backend_dir3,
            f"1{port}",
        )

        log.info("Validate the nfs protocol version from clients after mounting")
        nfs_exports = [nfs_export_name_1, nfs_export_name_2, nfs_export_name_3]
        valid_versions = [nfs_v41, nfs_v42, nfs_v4]
        check_mounted_exports(client1, nfs_exports, valid_versions)

        log.info("Test completed successfully")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleaning Up")
        directories_to_remove = [
            nfs_mounting_dir1,
            nfs_mounting_dir2,
            nfs_mounting_dir3,
            nfs_backend_dir1,
            nfs_backend_dir2,
            nfs_backend_dir3,
        ]
        for directory in directories_to_remove:
            client1.exec_command(sudo=True, cmd=f"rm -rf {directory}*")

        exports_to_remove = [nfs_export_name_1, nfs_export_name_2, nfs_export_name_3]
        for export_name in exports_to_remove:
            log.info(f"Removing the Export: {export_name}")
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export delete {nfs_name} {export_name}",
                check_ec=False,
            )

        for directory in directories_to_remove:
            log.info(f"Unmounting: {directory}")
            client1.exec_command(
                sudo=True, cmd=f"umount -l {directory}", check_ec=False
            )
            client1.exec_command(sudo=True, cmd=f"rm -rf {directory}")

        log.info("Removing NFS Cluster")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs cluster rm {nfs_name}",
            check_ec=False,
        )


def check_mounted_exports(client, nfs_exports, valid_versions):
    for nfs_export in nfs_exports:
        output = client.exec_command(
            sudo=True, cmd=f"mount | grep {nfs_export}", check_ec=False
        )
        lines = output[0].split("\n")
        for line in lines:
            if nfs_export in line:
                match = re.search(r"vers=([0-9.]+)", line)
                if match:
                    vers_value = match.group(1)
                    if vers_value in valid_versions:
                        print(
                            f"Protocol version (vers) for {nfs_export}: {vers_value} - Valid"
                        )
                    else:
                        print(
                            f"Protocol version (vers) for {nfs_export}: {vers_value} - Invalid"
                        )
