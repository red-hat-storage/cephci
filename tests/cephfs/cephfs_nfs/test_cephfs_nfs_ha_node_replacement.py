import copy
import json
import random
import secrets
import string
import time
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utilsV1 import FsUtils as FsUtilsV1
from utility.baremetal_power_ops import IPMIPowerControl
from utility.log import Log
from utility.utils import get_cephci_config

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83575096 - Validate failover when primary NFS HA node permanantly goes down,
        - Ensure there is no DL/DU after failover.
    Procedure
    Create default NFS HA Deployment and Reboot the node while IOs are going
    1. Log into the Cephadm shell:
        Example
        [root@host01 ~]# cephadm shell
    2. Create the NFS cluster with the --ingress flag:
        Syntax
        ceph nfs cluster create CLUSTER-ID [PLACEMENT] [--port PORT_NUMBER] [--ingress --virtual-ip IP_ADDRESS]
    3. Validate the Service is UP
    4. Mount using the Virtual IP.
    5. Get active Nfs node
    6. Create 2 Exports start writing IOs and reboot active nfs nodes in parallel with time gap 2 min
    7. Validate the active nfs host changes to different host
    8. power off one of the active node and validate if the services deploy on other backup node in the cluster
    9. Re-apply the nfs by adding new node
    10.Power off second active node and validate if the services get deployed on the newly added node
    11.Power on node which is powered off in step 8 and validate nfs services deployment will not happen
    12.Cleanup
    13.Power on all the nodes and remove nfs cluster
    """
    try:
        tc = "CEPH-83575096"
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
        cephci_config = get_cephci_config()
        baremetal_creds = cephci_config.get("baremetal", None)
        if not baremetal_creds:
            raise CommandFailed(
                "This test requires baremetal credientials to perform power operations"
            )
        baremetal_username = baremetal_creds.get("username")
        baremetal_password = baremetal_creds.get("password")
        if not baremetal_username or not baremetal_password:
            raise CommandFailed(
                "The 'baremetal' section in the configuration should include both 'username' and 'password' fields."
            )
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

        log.info("validate the services have started on nfs")
        fs_util_v1.validate_services(client1, f"nfs.{nfs_name}")
        fs_util_v1.validate_services(client1, f"ingress.nfs.{nfs_name}")
        haproxy_ls = fs_util_v1.get_daemon_status(client1, "haproxy")
        keepalived_ls = fs_util_v1.get_daemon_status(client1, "keepalived")
        haproxy_ls_hostnames = [entry["hostname"] for entry in haproxy_ls]
        keepalived_ls_hostnames = [entry["hostname"] for entry in keepalived_ls]

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
            command = f"mount -t nfs -o port=2049 {virtual_ip}:{nfs_export} {nfs_mounting_dir}"
            client1.exec_command(sudo=True, cmd=command, check_ec=False)
            mount_dir.append(nfs_mounting_dir)
        active_nfs = fs_util_v1.get_active_nfs(nfs_servers, virtual_ip)
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
            p.spawn(
                fs_util_v1.write_io, client1, mount_dir, config.get("io_runtime", 900)
            )
            active_nfs_ipmi = IPMIPowerControl(
                host=f"{backend_server[0].node.hostname}.ipmi.ceph.redhat.com",
                username=baremetal_username,
                password=baremetal_password,
            )
            log.info(f"Powering Off {backend_server[0].node.hostname}")
            active_nfs_ipmi.power_down()
            time.sleep(config.get("shutdown_time", 300))
            fs_util_v1.wait_for_host_offline(client1, backend_server[0])
        interm_list = copy.deepcopy(nfs_servers)
        index_to_remove = None
        for i, obj in enumerate(interm_list):
            if obj.node.hostname == backend_server[0].node.hostname:
                index_to_remove = i
                break

        # Remove the object if it was found
        if index_to_remove is not None:
            interm_list.pop(index_to_remove)
        else:
            print("Object not found in interm_list")
        active_nfs_after = fs_util_v1.get_active_nfs(interm_list, virtual_ip)
        for i in interm_list:
            fs_util_v1.wait_for_service_to_be_in_running(client1, i)
        haproxy_ls_after = fs_util_v1.get_daemon_status(client1, "haproxy")
        keepalived_ls_after = fs_util_v1.get_daemon_status(client1, "keepalived")
        haproxy_ls_after_hostnames = [entry["hostname"] for entry in haproxy_ls_after]
        keepalived_ls_after_hostnames = [
            entry["hostname"] for entry in keepalived_ls_after
        ]
        if active_nfs.node.hostname == active_nfs_after.node.hostname:
            raise CommandFailed(
                f"The active node did not change even after power off."
                f"\nPrevious active node: {active_nfs.node.hostname}"
                f"\nCurrent active node: {active_nfs_after.node.hostname}"
            )
        if (
            haproxy_ls_after_hostnames == haproxy_ls_hostnames
            or keepalived_ls_after_hostnames == keepalived_ls_hostnames
        ):
            raise CommandFailed(
                f"The ha proxy node did not change even after power off ."
                f"\nPrevious haproxy nodes: {haproxy_ls_hostnames}"
                f"\nCurrent haproxy nodes: {haproxy_ls_after_hostnames}"
                f"\nPrevious keepalived nodes: {keepalived_ls_hostnames}"
                f"\nCurrent keepalived nodes: {keepalived_ls_after_hostnames}"
            )
        log.info(
            f"The active node changed after Power off."
            f"\nPrevious active node: {active_nfs.node.hostname}"
            f"\nCurrent active node: {active_nfs_after.node.hostname}"
        )
        orch_formatted_string = " ".join(
            [f"{node.node.hostname}" for node in interm_list]
        )
        log.info("Update with New node")
        nfs_fstr = """
service_type: ingress
service_id: nfs.cephnfs
placement:
  count: 2
  hosts:
{hosts}
spec:
  backend_service: nfs.cephnfs
  frontend_port: 2049
  monitor_port: 9000
  virtual_ip: 10.8.128.100/21
""".format(
            hosts="".join([f"   - {node.node.hostname}\n" for node in interm_list])
        )

        filename = "nfs.yaml"
        nfs_file = clients[0].remote_file(
            sudo=True,
            file_name=filename,
            file_mode="w",
        )
        nfs_file.write(nfs_fstr)
        nfs_file.flush()
        client1.exec_command(sudo=True, cmd=f"ceph orch apply -i {filename}")
        client1.exec_command(
            sudo=True,
            cmd=f'ceph orch apply nfs {nfs_name} --placement "2 {orch_formatted_string}"',
        )

        active_nfs_ipmi = IPMIPowerControl(
            host=f"{backend_server[1].node.hostname}.ipmi.ceph.redhat.com",
            username=baremetal_username,
            password=baremetal_password,
        )
        log.info(f"Powering Off {backend_server[1].node.hostname}")
        active_nfs_ipmi.power_down()
        time.sleep(config.get("shutdown_time", 300))
        fs_util_v1.wait_for_host_offline(client1, backend_server[1])
        index_to_remove = None
        log.info(interm_list)
        for i, obj in enumerate(interm_list):
            log.info(f"Node to be removed {backend_server[1].node.hostname}")
            log.info(obj.node.hostname)
            if obj.node.hostname == backend_server[1].node.hostname:
                index_to_remove = i
                break
            log.info(f" index of the node {i}")
        # Remove the object if it was found
        if index_to_remove is not None:
            interm_list.pop(index_to_remove)
        else:
            log.info("Object not found in interm_list")
        for i in interm_list:
            fs_util_v1.wait_for_service_to_be_in_running(client1, i)
        haproxy_ls_after_step_2 = fs_util_v1.get_daemon_status(client1, "haproxy")
        keepalived_ls_after_step_2 = fs_util_v1.get_daemon_status(client1, "keepalived")
        haproxy_ls_after_hostnames_step_2 = [
            entry["hostname"] for entry in haproxy_ls_after_step_2
        ]
        keepalived_ls_after_hostnames_step_2 = [
            entry["hostname"] for entry in keepalived_ls_after_step_2
        ]
        if (
            haproxy_ls_after_hostnames_step_2 == haproxy_ls_after_hostnames
            or keepalived_ls_after_hostnames == keepalived_ls_after_hostnames_step_2
        ):
            raise CommandFailed(
                f"The ha proxy node did not change even after power off ."
                f"\nPrevious haproxy nodes: {haproxy_ls_after_hostnames}"
                f"\nCurrent haproxy nodes: {haproxy_ls_after_hostnames_step_2}"
                f"\nPrevious keepalived nodes: {keepalived_ls_after_hostnames}"
                f"\nCurrent keepalived nodes: {keepalived_ls_after_hostnames_step_2}"
                f"\nHA Proxy services not started BZ : 2240258"
            )
        if nfs_servers[3].node.hostname not in haproxy_ls_after_hostnames_step_2:
            raise CommandFailed("HA Proxy services not started BZ : 2240258")
        if nfs_servers[3].node.hostname not in keepalived_ls_after_hostnames_step_2:
            raise CommandFailed("Keepalived services not started BZ : 2240258")
        log.info(
            f"The active node changed after Power off."
            f"\nPrevious active node: {active_nfs.node.hostname}"
            f"\nCurrent active node: {active_nfs_after.node.hostname}"
        )
        active_nfs_ipmi = IPMIPowerControl(
            host=f"{backend_server[0].node.hostname}.ipmi.ceph.redhat.com",
            username=baremetal_username,
            password=baremetal_password,
        )
        active_nfs_ipmi.power_up()
        fs_util_v1.wait_for_host_online(client1, backend_server[0])
        ps_ls = client1.exec_command(
            sudo=True, cmd=f"ceph orch ps {backend_server[0].node.hostname} -f json"
        )
        if "nfs" in ps_ls:
            raise CommandFailed(
                f"Still Holding the nfs services on the removed server {backend_server[0].node.hostname}"
            )

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
        client1.exec_command(
            sudo=True, cmd=f"rm -rf {nfs_mounting_dir}*", check_ec=False
        )
        log.info("Unmount NFS export")
        client1.exec_command(
            sudo=True, cmd=f"umount -l {nfs_mounting_dir}", check_ec=False
        )
        client1.exec_command(
            sudo=True, cmd=f"rm -rf {nfs_mounting_dir}", check_ec=False
        )
        log.info("Removing the Exports")
        for i in [nfs_export_name, nfs_export_name_2]:
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
        log.info("Powering on the server")
        active_nfs_ipmi = IPMIPowerControl(
            host=f"{backend_server[1].node.hostname}.ipmi.ceph.redhat.com",
            username=baremetal_username,
            password=baremetal_password,
        )
        active_nfs_ipmi.power_up()
        active_nfs_ipmi = IPMIPowerControl(
            host=f"{backend_server[0].node.hostname}.ipmi.ceph.redhat.com",
            username=baremetal_username,
            password=baremetal_password,
        )
        active_nfs_ipmi.power_up()
        fs_util_v1.wait_for_host_online(client1, backend_server[1])
        fs_util_v1.wait_for_host_online(client1, backend_server[0])
