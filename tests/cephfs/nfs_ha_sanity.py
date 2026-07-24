import json
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.retry import retry

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test Cases Covered:
    Validating basic commands for configuring NFS HA
    Pre-requisites :
    1. We need atleast one client node to execute this test case

    Test Case Flow:
    1. Create nfs cluster with placement
    2.
    """
    try:
        fs_util = FsUtils(ceph_cluster)
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        installer_node = ceph_cluster.get_ceph_objects("installer")
        # virtual_ip = config.get("virtual_ip", "10.0.209.126/30")
        build = config.get("build", config.get("rhbuild"))
        osp_cred = config.get("osp_cred")
        if config.get("cloud-type") == "openstack":
            os_cred = osp_cred.get("globals").get("openstack-credentials")
            params = {}
            params["username"] = os_cred["username"]
            params["password"] = os_cred["password"]
            params["auth_url"] = os_cred["auth-url"]
            params["auth_version"] = os_cred["auth-version"]
            params["tenant_name"] = os_cred["tenant-name"]
            params["service_region"] = os_cred["service-region"]
            params["domain_name"] = os_cred["domain"]
            params["tenant_domain_id"] = os_cred["tenant-domain-id"]
            params["cloud_type"] = "openstack"
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        floating_ip = fs_util.get_floating_ip(installer_node[0].node, **params)
        # virtual_ip = config.get("virtual_ip", "10.0.209.126/30")
        log.info("checking Pre-requisites")
        if not clients:
            log.info(
                f"This test requires minimum 1 client nodes.This has only {len(clients)} clients"
            )
            return 1

        client1 = clients[0]
        nfs_servers = ceph_cluster.get_ceph_objects("nfs")
        nfs_name = "cephnfs"
        client1.exec_command(
            sudo=True,
            cmd=f'ceph nfs cluster create {nfs_name} "1 {nfs_servers[0].node.hostname} {nfs_servers[1].node.hostname}" '
            f"--ingress --virtual-ip {floating_ip.ip_address}/30",
        )

        log.info("validate the services hace started on nfs")
        validate_services(client1, f"nfs.{nfs_name}")
        validate_services(client1, f"ingress.nfs.{nfs_name}")
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        commands = [
            f"ceph nfs cluster rm {nfs_name}",
        ]
        for command in commands:
            client1.exec_command(sudo=True, cmd=command)
        fs_util.remove_floating_ip(floating_ip, **params)


@retry(CommandFailed, tries=10, delay=60)
def validate_services(client, service_name):
    out, rc = client.exec_command(
        sudo=True, cmd=f"ceph orch ls --service_name={service_name} --format json"
    )
    service_ls = json.loads(out)
    log.info(service_ls)
    if service_ls[0]["status"]["running"] != service_ls[0]["status"]["size"]:
        raise CommandFailed(f"All {service_name} are Not UP")
