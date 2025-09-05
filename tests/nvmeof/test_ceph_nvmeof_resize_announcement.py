"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway
 with supported entities like subsystems , etc.,

"""

import json
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.nvmegw_cli import NVMeGWCLI
from ceph.nvmeof.initiator import Initiator
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.nvme_utils import delete_nvme_service, deploy_nvme_service
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import run_fio

LOG = Log(__name__)


def configure_subsystems(ceph_cluster, rbd, pool, nvmegwcli, subsys_config, config):
    """Configure Ceph-NVMEoF Subsystems."""
    sub_args = {"subsystem": subsys_config["nqn"]}
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
        "trsvcid": subsys_config["listener_port"],
    }
    nvmegwcli.listener.add(**{"args": {**listener_cfg, **sub_args}})
    if subsys_config.get("allow_host"):
        nvmegwcli.host.add(
            **{"args": {**sub_args, **{"host": repr(subsys_config["allow_host"])}}}
        )

    if subsys_config.get("hosts"):
        for host in subsys_config["hosts"]:
            initiator_node = get_node_by_id(ceph_cluster, host)
            initiator = Initiator(initiator_node)
            host_nqn = initiator.initiator_nqn()
            nvmegwcli.host.add(**{"args": {**sub_args, **{"host": host_nqn}}})

    if subsys_config.get("bdevs"):
        with parallel() as p:
            count = subsys_config["bdevs"].get("count", 1)
            size = subsys_config["bdevs"].get("size", "1G")
            # Create image
            for num in range(count):
                image_name = f"nvme-image-{num}"
                p.spawn(rbd.create_image, pool, image_name, size)
        namespace_args = {**sub_args, **{"rbd-pool": pool}}
        with parallel() as p:
            # Create namespace in gateway
            for num in range(count):
                ns_args = deepcopy(namespace_args)
                ns_args.update({"rbd-image": image_name})
                ns_args = {"args": ns_args}
                p.spawn(nvmegwcli.namespace.add, **ns_args)


def disconnect_initiator(ceph_cluster, node, subnqn):
    """Disconnect Initiator."""
    node = get_node_by_id(ceph_cluster, node)
    initiator = Initiator(node)
    initiator.disconnect(**{"nqn": subnqn})


def disconnect_all_initiator(ceph_cluster, nodes):
    """Disconnect all connections on Initiator."""
    for node in nodes:
        node = get_node_by_id(ceph_cluster, node)
        initiator = Initiator(node)
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
        for initiator_cfg in config["initiators"]:
            disconnect_initiator(
                ceph_cluster, initiator_cfg["node"], initiator_cfg["nqn"]
            )

    if "disconnect_all" in config["cleanup"]:
        nodes = config.get("disconnect_all")
        if not nodes:
            nodes = [i["node"] for i in config["initiators"]]

        disconnect_all_initiator(ceph_cluster, nodes)

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


def trigger_fio(ceph_cluster, config, io_size):
    client = get_node_by_id(ceph_cluster, config["initiators"][0]["node"])
    initiator = Initiator(client)

    try:
        # List NVMe targets
        targets = initiator.list_spdk_drives()
        if not targets:
            raise Exception(f"NVMe Targets not found on {client.hostname}")
        LOG.debug(targets)

        rhel_version = initiator.distro_version()
        if rhel_version == "9.5":
            paths = [target["DevicePath"] for target in targets]
        elif rhel_version == "9.6":
            paths = [
                f"/dev/{ns['NameSpace']}"
                for device in targets
                for subsys in device.get("Subsystems", [])
                for ns in subsys.get("Namespaces", [])
            ]

            try:
                _io_args = {
                    "device_name": paths[0],
                    "client_node": client,
                    "long_running": True,
                    "run_time": "50",
                    "size": io_size,
                    "iodepth": 4,
                    "time_based": True,
                }
                run_fio(**_io_args)
                LOG.info(
                    f"FIO completed successfully on {paths[0]} with size {io_size}"
                )
                return {"status": "success"}
            except Exception as fio_err:
                LOG.error(f"Error running FIO on target {paths[0]}: {fio_err}")
                return {"status": "failed", "reason": str(fio_err)}

    except Exception as err:
        LOG.error(f"Error in trigger_fio: {err}")
        return {"status": "failed", "reason": str(err)}


def execute_namespace_resize_announcement_test(ceph_cluster, config, nvmegwcli):
    """Execute NVMeoF namespace resize and announcement test."""
    LOG.info("Running CEPH-83627178: NVMeoF namespace resize and announcement")
    installer_node = ceph_cluster.get_ceph_objects("installer")
    pool_name = config["rbd_pool"]
    subsys = config["subsystems"][0]["nqn"]
    image_name = "nvme-image-1"
    nsid = 1

    # Disable auto-resize for namespace
    nvmegwcli.namespace.set_auto_resize(
        **{"args": {"nsid": nsid, "subsystem": subsys, "auto-resize-enabled": "no"}}
    )
    LOG.info(f"Disabled auto-resize for nsid {1} on subsystem {subsys}")

    # Refresh the rbd image to 25G
    cmd = f"rbd resize --size 25G {pool_name}/{image_name}"
    installer_node[0].exec_command(cmd=cmd, sudo=True)
    LOG.info(f"Resized RBD image {pool_name}/{image_name} to 25G")

    # Namespace size should remain unchanged (20 GB).
    _, namespace_response = nvmegwcli.namespace.list(
        **{
            "base_cmd_args": {"format": "json"},
            "args": {"nsid": nsid, "subsystem": subsys},
        }
    )
    ns_size = json.loads(namespace_response)["namespaces"][0]["size"]
    LOG.info(f"Namespace {nsid} size before ns refresh: {ns_size}G")

    # -- IO beyond 20 GB should fail..Add your IO test logic here --
    fio_result = trigger_fio(ceph_cluster, config, io_size="22G")
    if fio_result["status"] == "failed":
        LOG.warning(f"Expected FIO failure observed: {fio_result['reason']}")
    else:
        LOG.error("FIO unexpectedly succeeded on IO beyond 20G")

    # Manually refresh namespace size
    LOG.info(f"Manually refreshing namespace {1}, size should now be 25G")
    nvmegwcli.namespace.refresh_size(**{"args": {"nsid": 1, "subsystem": subsys}})
    _, namespace_response = nvmegwcli.namespace.list(
        **{
            "base_cmd_args": {"format": "json"},
            "args": {"nsid": nsid, "subsystem": subsys},
        }
    )
    ns_size = json.loads(namespace_response)["namespaces"][0]["size"]
    LOG.info(f"Namespace {nsid} size after refresh: {ns_size}G")

    # IO across full 25 GB should succeed
    fio_result = trigger_fio(ceph_cluster, config, io_size="25G")
    if fio_result["status"] == "success":
        LOG.info("FIO succeeded as expected on IO across 25G")

    # Enable auto-resize
    nvmegwcli.namespace.set_auto_resize(
        **{"args": {"nsid": nsid, "subsystem": subsys, "auto-resize-enabled": "yes"}}
    )
    LOG.info(f"Enabled auto-resize on namespace {nsid}")

    # Resize RBD image to 30G (auto-resize should reflect automatically)
    cmd = f"rbd resize --size 30G {pool_name}/{image_name}"
    installer_node[0].exec_command(cmd=cmd, sudo=True)
    LOG.info(f"Resized RBD image {pool_name}/{image_name} to 30G")

    _, namespace_response = nvmegwcli.namespace.list(
        **{
            "base_cmd_args": {"format": "json"},
            "args": {"nsid": nsid, "subsystem": subsys},
        }
    )
    ns_size = json.loads(namespace_response)["namespaces"][0]["size"]
    LOG.info(f"Namespace {nsid} size without refresh: {ns_size}G")

    # IO across full 30 GB should succeed
    fio_result = trigger_fio(ceph_cluster, config, io_size="30G")
    if fio_result["status"] == "success":
        LOG.info("FIO succeeded as expected on IO across 30G")


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof test execution.

    - Configures Initiators, run FIO on NVMe targets, verify QoS.

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

    overrides = kwargs.get("test_data", {}).get("custom-config")
    for key, value in dict(item.split("=") for item in overrides).items():
        if key == "nvmeof_cli_image":
            NVMeGWCLI.NVMEOF_CLI_IMAGE = value
            break

    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    gw_port = config.get("gw_port", 5500)
    nvmegwcli = NVMeGWCLI(gw_node, gw_port)

    try:
        if config.get("install"):
            deploy_nvme_service(ceph_cluster, config)

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
                        config,
                    )

        LOG.info("Successfully configured all subsystems.")
        execute_namespace_resize_announcement_test(config, nvmegwcli)

        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(ceph_cluster, rbd_obj, nvmegwcli, config)

    return 1
