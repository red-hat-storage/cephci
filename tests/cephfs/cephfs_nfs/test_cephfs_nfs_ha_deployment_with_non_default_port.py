import json
import random
import secrets
import string
import time
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83575097 - Create an NFS HA service with 2 or more daemons on a non default port
        and test if the ingress / haproxy /keepalived daemons are running and the failover happens as expected.
    Procedure
    Scenario 1 - Create default NFS HA Deployment.
    1. Log into the Cephadm shell:
        Example
        [root@host01 ~]# cephadm shell
    2. Create the NFS cluster with the --ingress flag using a non default port:
        Syntax
        ceph nfs cluster create CLUSTER-ID [PLACEMENT] [--port PORT_NUMBER] [--ingress --virtual-ip IP_ADDRESS]
    3. Validate the Service is UP
    4. Mount using the Virtual IP.
    5. Create Export and write IO.
    6. Perform a failover and validate the IOs
    7. Cleanup
    """
    try:
        tc = "CEPH-83575097"
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
        non_default_port = "5005"

        log.info("checking Pre-requisites")
        if not clients:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1

        client1.exec_command(sudo=True, cmd="ceph mgr module enable nfs")
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        client1.exec_command(
            sudo=True,
            cmd=f'ceph nfs cluster create {nfs_name} "2 {nfs_servers[0].node.hostname} '
            f'{nfs_servers[1].node.hostname} {nfs_servers[2].node.hostname}" '
            f"--ingress --virtual-ip {virtual_ip}/{subnet} --port {non_default_port}",
        )

        log.info("validate the services have started on nfs")
        fs_util_v1.validate_services(client1, f"nfs.{nfs_name}")
        fs_util_v1.validate_services(client1, f"ingress.nfs.{nfs_name}")
        nfs_ls = fs_util_v1.get_daemon_status(client1, "nfs")
        haproxy_ls = fs_util_v1.get_daemon_status(client1, "haproxy")
        keepalived_ls = fs_util_v1.get_daemon_status(client1, "keepalived")

        log.info(f"NFS Services deployed are as follows {nfs_ls}")
        log.info(f"haproxy Services deployed are as follows {haproxy_ls}")
        log.info(f"keepalived Services deployed are as follows {keepalived_ls}")

        log.info("validate the ports")
        port_validation = fs_util_v1.validate_ports(
            client1, f"ingress.nfs.{nfs_name}", port={non_default_port}
        )
        log.info(port_validation)
        nfs_export_name = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        nfs_export_name_2 = "/export_" + "".join(
            secrets.choice(string.digits) for i in range(3)
        )
        export_path = "/"
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        fs_details = fs_util_v1.get_fs_info(client1, fs_name)

        if not fs_details:
            fs_util_v1.create_fs(client1, fs_name)
        mount_dir = []
        for nfs_export in [nfs_export_name, nfs_export_name_2]:
            client1.exec_command(
                sudo=True,
                cmd=f"ceph nfs export create cephfs {nfs_name} "
                f"{nfs_export} {fs_name} path={export_path}",
            )
            out, rc = client1.exec_command(
                sudo=True, cmd=f"ceph nfs export ls {nfs_name}"
            )

            if nfs_export_name not in out:
                raise CommandFailed("Failed to create nfs export")

            log.info("ceph nfs export created successfully")
            out, rc = client1.exec_command(
                sudo=True, cmd=f"ceph nfs export get {nfs_name} {nfs_export}"
            )
            json.loads(out)
            mounting_dir = "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in list(range(10))
            )
            nfs_mounting_dir = f"/mnt/cephfs_nfs{mounting_dir}_1/"
            client1.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mounting_dir}")
            command = f"mount -t nfs -o port={non_default_port} {virtual_ip}:{nfs_export} {nfs_mounting_dir}"
            client1.exec_command(sudo=True, cmd=command, check_ec=False)
            mount_dir.append(nfs_mounting_dir)
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
        backend_server = fs_util_v1.get_active_nfs_server(
            client1, nfs_name, ceph_cluster
        )
        for server in backend_server:
            log.info(f"Active backend servers after reboots {server.node.hostname}")
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
        log.info("Removing the Export1")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export delete {nfs_name} {nfs_export_name}",
            check_ec=False,
        )
        log.info("Removing the Export2")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs export delete {nfs_name} {nfs_export_name_2}",
            check_ec=False,
        )
        log.info("Removing NFS Cluster")
        client1.exec_command(
            sudo=True,
            cmd=f"ceph nfs cluster rm {nfs_name}",
            check_ec=False,
        )
