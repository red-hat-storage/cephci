"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway
 with supported entities like subsystems , etc.,

"""
import json

import paramiko

from ceph.ceph import Ceph
from ceph.ceph_admin import CephAdmin
from ceph.nvmeof.gateway import Gateway, configure_spdk, fetch_gateway_log
from ceph.utils import get_node_by_id
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id

LOG = Log(__name__)


def configure_subsystems(rbd, pool, gw, config):
    """Configure Ceph-NVMEoF Subsystems."""
    sub_nqn = config["nqn"]
    max_ns = config.get("max_ns", 32)
    gw.create_subsystem(sub_nqn, config["serial"], max_ns)
    gw.create_listener(sub_nqn, config["listener_port"])
    gw.add_host(sub_nqn, config["allow_host"])
    if config.get("bdevs"):
        name = generate_unique_id(length=4)
        count = config["bdevs"].get("count", 1)
        size = config["bdevs"].get("size", "1G")
        bdev_size = config["bdevs"].get("bdev_size", "4096")
        # Create image on ceph cluster
        for num in range(count):
            rbd.create_image(pool, f"{name}-image{num}", size)

        # Create block device and add namespace in gateway
        for num in range(count):
            gw.create_block_device(
                f"{name}-bdev{num}", f"{name}-image{num}", pool, bdev_size
            )
            gw.add_namespace(sub_nqn, f"{name}-bdev{num}")

    subsystems = gw.get_subsystems()
    return subsystems


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
    commands = [
        "esxcli nvme adapter list",
        f"esxcli nvme fabrics discover -a vmhba64 -i {gateway.node.ip_address} -p {port} -c -N 4",
        f"esxcli nvme fabrics connect -a vmhba64 -i {gateway.node.ip_address} -p {port} -s {nqn}",
        "esxcli nvme controller list",
        "esxcli nvme namespace list",
    ]
    for cmd in commands:
        LOG.info(f"{cmd}")
        output = execute_ssh_command(ssh_esx, cmd)
        LOG.info(output)

    command = "esxcli nvme namespace list"
    output = execute_ssh_command(ssh_esx, command)
    names = output.strip().split("\n")
    namespace_data = [name.split()[0] for name in names[3:]]
    namespaces = [item.replace("eui.", "") for item in namespace_data]
    LOG.info(namespaces)
    LOG.info(nguid_list)
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

    - Configure SPDK and install with control interface.
    - Configures NVMeOF GW for VMware clients
    - Configures VMware clients for subsystem created
    - Validates namespaces on Esx host and subsystem

    Args:
        ceph_cluster: Ceph cluster object
        kwargs:     Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:
        - test:
            name: Configure NvmeOF GW for VMware clients
            desc: test to setup NVMeOF GW node for VMware clients
            config:
                gw_node: node6
                rbd_pool: rbd
                do_not_create_image: true
                rep-pool-only: true
                rep_pool_config:
                  pool: rbd
                  install: true                             # Run SPDK with all pre-requisites
                subsystems:                                 # Configure subsystems with all sub-entities
                  - nqn: nqn.2016-06.io.spdk:cnode1
                    serial: 1
                    max_ns: 256
                    bdevs:
                      count: 1
                      size: 500M
                      bdev_size: 512                         #VMware Esx host can only recognize bdev of 512 block size
                    listener_port: 5001
                    allow_host: "*"
                vmware_clients:
                  - esx_host: argo010
                    ip: 10.8.128.210
                    root_password: VMware1!
                    sub_nqn: nqn.2016-06.io.spdk:cnode1
                    sub_port: 5001
    """
    LOG.info("Starting Ceph Ceph NVMEoF deployment.")

    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
    nguid_list = []
    get_subsystem = []

    try:
        gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
        gateway = Gateway(gw_node)

        if config.get("install"):
            configure_spdk(gw_node, rbd_pool)

        if config.get("subsystems"):
            for subsystem in config["subsystems"]:
                get_subsystem = configure_subsystems(
                    rbd_obj, rbd_pool, gateway, subsystem
                )
            subsystems_list = get_subsystem[1]
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

        if config.get("vmware_clients"):
            for vmware_client in config["vmware_clients"]:
                configure_esx(gateway, vmware_client, nguid_list)

        instance = CephAdmin(cluster=ceph_cluster, **config)
        health, _ = instance.installer.exec_command(
            cmd="cephadm shell ceph -s", sudo=True
        )
        LOG.info(health)

        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
        fetch_gateway_log(gw_node)

    return 1
