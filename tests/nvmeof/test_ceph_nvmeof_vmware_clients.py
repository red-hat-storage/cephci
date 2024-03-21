"""
Test module that configures esx server for a nvmeof GW subsystem
 and validates its connection by matching namespaces IDs
 This Test module will be re-written or enhanced based NVMeOF 7.1 test requirements for vmware.,

"""
import json
import re
from copy import deepcopy

import paramiko

from ceph.ceph import Ceph
from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.common import fetch_method
from ceph.nvmegw_cli.common import NVMeCLI
from ceph.utils import get_node_by_id
from utility.log import Log

LOG = Log(__name__)


def execute_ssh_command(ssh_client, command):
    stdin, stdout, stderr = ssh_client.exec_command(command)
    return stdout.read().decode("utf-8")


def configure_esx(gateway, config, nguid_list):
    """Configures VMware ESX hosts and validates subsystem connectivity"""
    esx_ip = config.get("ip")
    esx_passwd = config.get("root_password")
    nqn = config.get("sub_nqn")
    port = config.get("sub_port")

    ssh_esx = paramiko.SSHClient()
    ssh_esx.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_esx.connect(esx_ip, username="root", password=esx_passwd)

    command = "esxcli nvme adapter list | grep nvmetcp"
    output = execute_ssh_command(ssh_esx, command)
    LOG.info(output)
    pattern = re.compile(r"(\w+)\s+.+?\s+TCP\s+nvmetcp")
    match = pattern.search(output)
    adapter = match.group(1)

    commands = [
        f"esxcli nvme fabrics discover -a {adapter} -i {gateway.ip_address} -p {port} -c -N 4",
        f"esxcli nvme fabrics connect -a {adapter} -i {gateway.ip_address} -p {port} -s {nqn}",
        "esxcli nvme controller list",
    ]
    for cmd in commands:
        LOG.info(f"{cmd}")
        output = execute_ssh_command(ssh_esx, cmd)
        LOG.info(output)

    command = "esxcli nvme namespace list"
    output = execute_ssh_command(ssh_esx, command)
    LOG.info(output)
    names = output.strip().split("\n")
    namespace_data = [name.split()[0] for name in names[3:]]
    namespaces = [item.replace("eui.", "") for item in namespace_data]
    LOG.info(namespaces)
    LOG.info(nguid_list)

    if not namespaces:
        raise Exception("no namesapces found on esx server. verify connection")

    command = f"esxcli nvme fabrics disconnect -a {adapter} -s {nqn}"
    execute_ssh_command(ssh_esx, command)
    command = "esxcli nvme controller list"
    output = execute_ssh_command(ssh_esx, command)
    LOG.info(output)

    validate_id = [s1 == s2.upper() for s1, s2 in zip(nguid_list, namespaces)]
    for index, equal in enumerate(validate_id, start=1):
        if equal:
            LOG.info(f"Esx and subsystem namespace IDs match for namespace {index}")
        else:
            raise Exception(
                f"Esx and subsystem namespace IDs does not match for namespace {index}. Failing test."
            )


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """
    Return the status of the Ceph NVMEof test execution.

    - Configures VMware clients for subsystem created
    - Validates namespaces on Esx host and GW subsystem

    Args:
        ceph_cluster: Ceph cluster object
        kwargs:     Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:
          - test:
              abort-on-fail: true
              config:
                gw_node: node5
                vmware_clients:
                  - esx_host: argo029
                    ip: 10.8.128.229
                    root_password: VMware1!
                    sub_nqn: nqn.2016-06.io.spdk:cnode1
                    sub_port: 4420
              desc: test to configure VMware esx server and validate connection
              destroy-cluster: false
              module: test_ceph_nvmeof_vmware_clients1.py
              name: Configure VMware clients
              polarion-id:
    """

    config = deepcopy(kwargs["config"])
    LOG.info(config)
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    overrides = kwargs.get("test_data", {}).get("custom-config")
    nguid_list = []

    for key, value in dict(item.split("=") for item in overrides).items():
        if key == "nvmeof_cli_image":
            NVMeCLI.CEPH_NVMECLI_IMAGE = value
            break
    nvme_cli = NVMeCLI(gw_node, config.get("port", 5500))

    try:
        LOG.info("Get subsystems from ceph nvmeof GW.")
        command = "get_subsystems"
        func = fetch_method(nvme_cli, command)
        get_subsystems = func()
        LOG.info(func)

        subsystems_list = get_subsystems[1]
        LOG.info(subsystems_list)
        subsystems_list = subsystems_list.replace("\n", "").strip()
        json_start = subsystems_list.find("[    {")
        json_end = subsystems_list.rfind("}]") + 2
        subsystem_json = subsystems_list[json_start:json_end]
        subsystem_data = json.loads(subsystem_json)
        nguid_list = [
            namespace["nguid"]
            for item in subsystem_data
            for namespace in item.get("namespaces", [])
        ]
        if not nguid_list:
            raise Exception("no namespaces on GW node")

        if config.get("vmware_clients"):
            for vmware_client in config["vmware_clients"]:
                configure_esx(gw_node, vmware_client, nguid_list)

        instance = CephAdmin(cluster=ceph_cluster, **config)
        health, _ = instance.installer.exec_command(
            cmd="cephadm shell ceph -s", sudo=True
        )
        LOG.info(health)

    except Exception as be:  # noqa
        LOG.error(be, exc_info=True)
        return 1
    return 0
