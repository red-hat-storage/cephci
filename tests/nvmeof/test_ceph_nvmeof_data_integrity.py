import json
import time
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.ceph_admin.orch import Orch
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_gateway import create_gateway
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    delete_nvme_service,
    deploy_nvme_service,
    nvme_gw_cli_version_adapter,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id

LOG = Log(__name__)


def configure_subsystems(ceph_cluster, rbd, pool, nvmegwcli, config):
    """Configure Ceph-NVMEoF Subsystems."""
    sub_args = {"subsystem": config["nqn"]}
    nvmegwcli.subsystem.add(
        **{
            "args": {
                **sub_args,
                **{
                    "max-namespaces": config.get("max_ns", 32),
                    **(
                        {"no-group-append": config.get("no-group-append", True)}
                        if ceph_cluster.rhcs_version >= "8.0"
                        else {}
                    ),
                },
            }
        }
    )

    listener_cfg = {
        "host-name": nvmegwcli.fetch_gateway_hostname(),
        "traddr": nvmegwcli.node.ip_address,
        "trsvcid": config["listener_port"],
    }
    nvmegwcli.listener.add(**{"args": {**listener_cfg, **sub_args}})

    # All host to subsystem
    if config.get("allow_host"):
        nvmegwcli.host.add(
            **{"args": {**sub_args, **{"host": repr(config["allow_host"])}}}
        )

    if config.get("hosts"):
        for host in config["hosts"]:
            initiator_node = get_node_by_id(ceph_cluster, host)
            initiator = NVMeInitiator(initiator_node)
            host_nqn = initiator.initiator_nqn()
            nvmegwcli.host.add(**{"args": {**sub_args, **{"host": host_nqn}}})

    if config.get("bdevs"):
        name = generate_unique_id(length=4)
        with parallel() as p:
            count = config["bdevs"].get("count", 1)
            size = config["bdevs"].get("size", "1G")
            # Create image
            for num in range(count):
                p.spawn(rbd.create_image, pool, f"{name}-image{num}", size)
        namespace_args = {**sub_args, **{"rbd-pool": pool}}
        with parallel() as p:
            # Create namespace in gateway
            for num in range(count):
                ns_args = deepcopy(namespace_args)
                ns_args.update({"rbd-image": f"{name}-image{num}"})
                ns_args = {"args": ns_args}
                p.spawn(nvmegwcli.namespace.add, **ns_args)


def create_filesystem_and_mount(rbd_obj, devicepath, mount_point):
    """Create a filesystem and mount it.

    Args:
        rbd_obj: rbdcfg object
        devicepath: Device path to create filesystem and mount
        mount_point: Mount point for the filesystem
         mount_point: Mount point for the filesystem
    """
    # Create a filesystem
    cmd_mkfs = f"mkfs.ext4 {devicepath}"
    rbd_obj.exec_cmd(cmd=cmd_mkfs)
    # Create a directory to mount the filesystem
    rbd_obj.exec_cmd(cmd=f"mkdir -p {mount_point}")
    # Mount the filesystem
    cmd_mount = f"mount {devicepath} {mount_point}"
    rbd_obj.exec_cmd(cmd=cmd_mount)


def initiators(ceph_cluster, gateway, config):
    """Run IOs from NVMe Initiators.

    - Discover NVMe targets
    - Connect to subsystem
    - List targets and Run FIO on target devices.

    Args:
        ceph_cluster: Ceph cluster
        gateway: Ceph-NVMeoF Gateway.
        config: Initiator config

    Example::
        config:
            subnqn: nqn.2016-06.io.spdk:cnode2
            listener_port: 5002
            node: node7
    """
    client = get_node_by_id(ceph_cluster, config["node"])
    initiator = NVMeInitiator(client)
    cmd_args = {
        "transport": "tcp",
        "traddr": gateway.node.ip_address,
    }
    json_format = {"output-format": "json"}

    # Discover the subsystems
    disc_port = {"trsvcid": 8009}
    _disc_cmd = {**cmd_args, **disc_port, **json_format}
    sub_nqns, _ = initiator.discover(**_disc_cmd)
    LOG.debug(sub_nqns)
    for nqn in json.loads(sub_nqns)["records"]:
        if nqn["trsvcid"] == str(config["listener_port"]):
            cmd_args["nqn"] = nqn["subnqn"]
            break
    else:
        raise Exception(f"Subsystem not found -- {cmd_args}")

    # Connect to the subsystem
    conn_port = {"trsvcid": config["listener_port"]}
    _conn_cmd = {**cmd_args, **conn_port}
    LOG.debug(initiator.connect(**_conn_cmd))

    # List NVMe targets
    targets = initiator.list_spdk_drives()
    if not targets:
        raise Exception(f"NVMe Targets not found on {client.hostname}")
    LOG.debug(targets)

    # Get distro version of an initiator
    rhel_version = initiator.distro_version()
    return targets, rhel_version


def disconnect_initiator(ceph_cluster, node):
    """Disconnect Initiator."""
    node = get_node_by_id(ceph_cluster, node)
    initiator = NVMeInitiator(node)
    initiator.disconnect_all()


def teardown(ceph_cluster, rbd_obj, nvmegwcli, config):
    """Cleanup the ceph-nvme gw entities.

    Args:
        ceph_cluster: Ceph Cluster
        rbd_obj: RBD object
        config: test config
    """
    # Delete the multiple Initiators across multiple gateways
    if "initiators" in config["cleanup"]:
        initiator_cfg = config["initiators"]
        disconnect_initiator(ceph_cluster, initiator_cfg["node"])

    # Delete the multiple subsystems across multiple gateways
    if "subsystems" in config["cleanup"]:
        config_sub_node = config["subsystems"]
        if not isinstance(config_sub_node, list):
            config_sub_node = [config_sub_node]
        for sub_cfg in config_sub_node:
            node = config["gw_node"] if "node" not in sub_cfg else sub_cfg["node"]
            LOG.info(f"Deleting subsystem {sub_cfg['nqn']} on gateway {node}")
            nvmegwcli.subsystem.delete(
                **{"args": {"subsystem": sub_cfg["nqn"], "force": True}}
            )

    # Delete the gateway
    if "gateway" in config["cleanup"]:
        delete_nvme_service(ceph_cluster, config)

    # Delete the pool
    if "pool" in config["cleanup"]:
        rbd_obj.clean_up(pools=[config["rbd_pool"]])


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof test execution.

    - Configure SPDK and install with control interface.
    - Configures Initiators and Run FIO and test data integrity on NVMe targets.
    - Cleanup the ceph-nvme gw entities.

    Args:

        ceph_cluster: Ceph cluster object
        kwargs: Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    """
    LOG.info("Starting Ceph Ceph NVMEoF deployment.")

    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
    rbd_obj.ceph_client = get_node_by_id(ceph_cluster, config["initiators"]["node"])

    nvmegwcli = None
    custom_config = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)

    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    ceph = Orch(ceph_cluster, **{})

    if config.get("cleanup-only"):
        teardown(ceph_cluster, rbd_obj, nvmegwcli, config)
        return 0

    try:
        if config.get("install"):
            deploy_nvme_service(ceph_cluster, config)

        nvmegwcli = create_gateway(
            nvme_gw_cli_version_adapter(ceph_cluster),
            gw_node,
            mtls=config.get("mtls"),
            shell=getattr(ceph, "shell"),
            port=config.get("gw_port", 5500),
            gw_group=config.get("gw_group"),
        )

        if config.get("subsystems"):
            with parallel() as p:
                for subsys_args in config["subsystems"]:
                    p.spawn(
                        configure_subsystems,
                        ceph_cluster,
                        rbd_obj,
                        rbd_pool,
                        nvmegwcli,
                        subsys_args,
                    )

        if config.get("initiators"):
            paths, rhel_version = initiators(
                ceph_cluster, nvmegwcli, config["initiators"]
            )
            LOG.info(f"Targets discovered: {paths}")

            if not paths:
                raise RuntimeError("No paths")
            # verifying data integrity on NVMe targets
            for path in paths:
                nvme = generate_unique_id(length=4)
                mount_point = f"/mnt/{nvme}"  # Change this to the desired mount point
                create_filesystem_and_mount(rbd_obj, path, mount_point)
                # FIO command to run on NVMe targets
                fio_command = (
                    f"fio --ioengine=sync --refill_buffers=1 "
                    f"--log_avg_msec=1000 --size=1g --bs=128k "
                    f"--filename={mount_point}/test.txt"
                )

                # Run FIO for Writing the files
                LOG.info("Running FIO for write operations.")
                cmd_write = f"{fio_command} --name=writeiops --rw=randwrite"
                rbd_obj.exec_cmd(cmd=cmd_write, sudo=True)
                # Calculate md5sum after FIO write
                md5sum_write = rbd_obj.exec_cmd(
                    cmd=f"md5sum {mount_point}/test.txt", sudo=True
                )
                # unmount the device
                rbd_obj.exec_cmd(cmd=f"umount {mount_point}", sudo=True)
                # mount the NVMe target
                rbd_obj.exec_cmd(cmd=f"mount {path} {mount_point}", sudo=True)
                # Run FIO for reading the files
                LOG.info("Running FIO for read operations.")
                cmd_read = f"sudo {fio_command} --name=readiops --rw=randread"
                rbd_obj.exec_cmd(cmd=cmd_read, sudo=True)
                # Calculate md5sum after FIO read
                md5sum_read = rbd_obj.exec_cmd(
                    cmd=f"md5sum {mount_point}/test.txt", sudo=True
                )

                # Compare md5sum values
                if md5sum_write == md5sum_read:
                    LOG.info(
                        f"Data integrity verified successfully for target /dev/{path}."
                    )
                else:
                    LOG.error(
                        f"Data integrity verification failed for target /dev/{path}."
                    )
                    return 1
                # copy the file to the local system
                rbd_obj.exec_cmd(
                    cmd=f"cp {mount_point}/test.txt /tmp/copy.txt", sudo=True
                )
                # Rename the file
                rbd_obj.exec_cmd(
                    cmd=f"mv {mount_point}/test.txt {mount_point}/test1.txt", sudo=True
                )
                # Copy the file back to the NVMe target
                rbd_obj.exec_cmd(
                    cmd=f"cp /tmp/copy.txt {mount_point}/test1.txt", sudo=True
                )
                # Calculate md5sum after copying the file back to the NVMe target
                md5sum_copy = rbd_obj.exec_cmd(
                    cmd=f"md5sum {mount_point}/test1.txt", sudo=True
                )
                # Compare md5sum values
                if md5sum_write == md5sum_copy:
                    LOG.info(
                        f"Data integrity verified successfully for copy and compare on /dev/{path}."
                    )
                else:
                    LOG.error(
                        f"Data integrity verification failed for copy and compare on /dev/{path}."
                    )
                    return 1
                # reboot the client node
                initiator = get_node_by_id(ceph_cluster, config["initiators"]["node"])
                initiator.exec_command(sudo=True, cmd="reboot", check_ec=False)
                # Sleep before and after reconnect
                time.sleep(60)
                initiator.reconnect()
                time.sleep(10)
                # Connect to Initiator
                if config.get("initiators"):
                    paths, rhel_version = initiators(
                        ceph_cluster, nvmegwcli, config["initiators"]
                    )
                    LOG.info(f"Targets discovered: {paths}")

                path = paths[0]
                # mount the NVMe target
                rbd_obj.exec_cmd(cmd=f"mount {path} {mount_point}", sudo=True)
                # Calculate md5sum after reboot
                md5sum_reboot = rbd_obj.exec_cmd(
                    cmd=f"md5sum {mount_point}/test1.txt", sudo=True
                )
                # Compare md5sum values
                if md5sum_write == md5sum_reboot:
                    LOG.info(
                        f"Data integrity verified successfully after reboot on /dev/{path}."
                    )
                else:
                    LOG.error(
                        f"Data integrity verification failed after reboot on /dev/{path}."
                    )
                    return 1

        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(ceph_cluster, rbd_obj, nvmegwcli, config)

    return 1
