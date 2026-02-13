import json
import pdb
from copy import deepcopy

from ceph.nvmeof.initiators.linux import Initiator
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.constants import (
    DEFAULT_LISTENER_PORT,
    DEFAULT_NVME_METADATA_POOL,
)
from tests.nvmeof.workflows.inband_auth import create_dhchap_key
from tests.nvmeof.workflows.initiator import NVMeInitiator
from utility.log import Log
from utility.utils import generate_unique_id, log_json_dump

LOG = Log(__name__)


def validate_subsystems(nvme_service, subsystem_config):
    """
    Validate that all the subsystems present in subsystem_config are correctly configured.
    Args:
        nvme_service: NvmeService instance
        subsystem_config: List of subsystem configurations to validate
    """
    # Use the first gateway's nvmegwcli for subsystem validation
    if not nvme_service.gateways:
        raise ValueError("No gateways available for subsystem validation")

    gateway = nvme_service.gateways[0]
    args = {"base_cmd_args": {"format": "json"}}
    subsystem_list = gateway.subsystem.list(**args)
    subsystem_list = (
        json.loads(subsystem_list[0])["subsystems"] if subsystem_list else []
    )
    if not subsystem_list:
        raise ValueError("No subsystems found after configuration")

    if len(subsystem_list) != len(subsystem_config):
        raise ValueError(
            f"Mismatch in number of configured subsystems: "
            f"expected {len(subsystem_config)}, found {len(subsystem_list)}"
        )

    for i, sub_cfg in enumerate(subsystem_config):
        nqn = sub_cfg.get("nqn") or sub_cfg.get("subnqn")
        if nqn in subsystem_list[i].get("nqn", subsystem_list[i].get("subnqn")):
            continue
        raise ValueError(
            f"Subsystem {sub_cfg.get('nqn') or sub_cfg.get('subnqn')} not found in configured subsystems"
        )


def configure_subsystems(nvme_service):
    """
    Configure subsystems, hosts, and namespaces for this gateway group.
    This is done once per group, not per gateway.
    Args:
        nvme_service: NvmeService instance
        exec_parallel: Whether to execute subsystem configuration in parallel
        (default: False, sequential execution)
    """

    # Configure subsystem
    def configure_subsystem(nvme_service, sub_cfg):
        nqn = sub_cfg.get("nqn") or sub_cfg.get("subnqn")
        if not nqn:
            raise ValueError("Subsystem NQN not provided in subsystem_config")

        # Use the first gateway's nvmegwcli for subsystem configuration
        if not nvme_service.gateways:
            raise ValueError("No gateways available for subsystem configuration")

        gateway = nvme_service.gateways[0]

        # Configure subsystem using nvmegwcli
        sub_args = {"subsystem": nqn}
        if sub_cfg.get("serial"):
            sub_args["serial-number"] = sub_cfg.get("serial")

        # Configure inband authentication if specified
        if sub_cfg.get("inband_auth"):
            sub_cfg["gw_group"] = nvme_service.group

            # Uncomment the below lines for debugging
            gateway.gateway.set_log_level(**{"args": {"level": "DEBUG"}})
            gateway.loglevel.set(**{"args": {"level": "DEBUG"}})

            # if sub_cfg.get("inband_auth"):
            sub_cfg.update(
                {"initiators": create_dhchap_key(sub_cfg, nvme_service.ceph_cluster)}
            )
            sub_args["dhchap-key"] = sub_cfg["dhchap-key"]

        # Add Subsystem
        release = nvme_service.ceph_cluster.rhcs_version
        if release >= "8.0":
            no_group_append = sub_cfg.get("no-group-append", True)
        else:
            no_group_append = sub_cfg.get("no-group-append", False)
        gateway.subsystem.add(
            **{
                "args": {
                    **sub_args,
                    **{
                        "max-namespaces": sub_cfg.get("max_ns", 32),
                        "enable-ha": sub_cfg.get("enable_ha", False),
                        "no-group-append": no_group_append,
                    },
                }
            }
        )

    subsystem_config = nvme_service.config.get("subsystems", [])
    for sub_cfg in subsystem_config:
        with parallel() as p:
            p.spawn(configure_subsystem, nvme_service, sub_cfg)

    # Validate subsystems
    validate_subsystems(nvme_service, subsystem_config)
    pdb.set_trace()
    initiators = []
    for sub_cfg in subsystem_config:
        if sub_cfg.get("initiators", []):
            initiators.append(sub_cfg["initiators"])
    return initiators


def validate_hosts(gateway, expected_hosts, nqn):
    """
    Validate that all the expected hosts are correctly configured in the gateway.
    Args:
        gateway: NVMeGateway instance
        expected_hosts: List of expected host NQNs to validate
    """
    args = {"base_cmd_args": {"format": "json"}, "args": {"subsystem": nqn}}
    out, _ = gateway.host.list(**args)
    allow_any_host = json.loads(out)["allow_any_host"]

    if isinstance(expected_hosts, bool):
        if expected_hosts != allow_any_host:
            raise ValueError("Open host access '*' not found in configured hosts")
    else:
        host_list = json.loads(out) if out else []
        configured_host_nqns = [host["nqn"] for host in host_list["hosts"]]
        for expected_host in expected_hosts:
            if expected_host not in configured_host_nqns:
                raise ValueError(
                    f"Expected host {expected_host} not found in configured hosts"
                )


def configure_hosts(gateway, config: dict, ceph_cluster=None):
    """
    Configure hosts for this specific gateway.
    This is called per gateway since each gateway needs its own hosts.
    Args:
        gateway: NVMeGateway instance
        config: Test configuration.
    """
    # Configure hosts if specified
    subsystem_config = config.get("subsystems", [])
    for sub_cfg in subsystem_config:
        # Configure hosts if specified
        nqn = sub_cfg.get("nqn") or sub_cfg.get("subnqn")
        sub_args = {"subsystem": nqn}
        if sub_cfg.get("allow_host"):
            gateway.host.add(
                **{"args": {**sub_args, **{"host": repr(sub_cfg["allow_host"])}}}
            )
            if sub_cfg["allow_host"] == "*":
                validate_hosts(gateway, True, nqn)
            else:
                validate_hosts(gateway, [sub_cfg["allow_host"]], nqn)
        if sub_cfg.get("hosts"):
            hosts = sub_cfg["hosts"]
            if not isinstance(hosts, list):
                hosts = [hosts]
            initiators = []
            for host in hosts:
                node_id = host.get("node") if isinstance(host, dict) else host
                initiator_node = get_node_by_id(ceph_cluster, node_id)
                initiator = NVMeInitiator(initiator_node)
                # # Generate key for host NQN
                sub_cfg.pop("dhchap-key", None)
                if host.get("inband_auth"):
                    # initiators.append(create_dhchap_key(sub_cfg, ceph_cluster, update_host_key=True))
                    create_dhchap_key(sub_cfg, ceph_cluster, update_host_key=True)
                    sub_cfg["dhchap-key"] = host["dhchap-key"]
                    sub_args["dhchap-key"] = host["dhchap-key"]
                initiator_nqn = initiator.initiator_nqn()
                gateway.host.add(**{"args": {**sub_args, "host": initiator_nqn}})

            validate_hosts(gateway, [initiator_nqn], nqn)
            return initiators


def validate_namespaces(gateway, expected_namespaces, nqn):
    """
    Validate that all the expected namespaces are correctly configured in the gateway.
    Args:
        gateway: NVMeGateway instance
        expected_namespaces: List of expected namespace names to validate
    """
    args = {"base_cmd_args": {"format": "json"}, "args": {"subsystem": nqn}}
    out, _ = gateway.namespace.list(**args)
    namespace_list = json.loads(out)["namespaces"] if out else []
    configured_ns_names = [ns["rbd_image_name"] for ns in namespace_list]

    for expected_ns in expected_namespaces:
        if expected_ns not in configured_ns_names:
            raise ValueError(
                f"Expected namespace {expected_ns} not found in configured namespaces"
            )


def configure_namespaces(gateway, config, opt_args={}, rbd_obj=None):
    """
    Configure namespaces for this specific gateway.
    This is called per gateway since each gateway needs its own namespaces.
    Args:
        gateway: NVMeGateway instance
        config: test config
        opt_args: Optional arguments to pass to namespace creation in key value form.
    """
    # Configure namespaces if specified
    subsystem_config = config.get("subsystems", [])
    for sub_cfg in subsystem_config:
        nqn = sub_cfg.get("nqn") or sub_cfg.get("subnqn")
        if not nqn:
            raise ValueError("Subsystem NQN not provided in subsystem_config")

        # Configure namespaces if specified
        sub_args = {"subsystem": nqn}
        if sub_cfg.get("bdevs"):
            bdev_configs = sub_cfg["bdevs"]
            if isinstance(bdev_configs, dict):
                bdev_configs = [bdev_configs]

            expected_namespaces = []
            for bdev_cfg in bdev_configs:
                name = generate_unique_id(length=4)

                namespace_args = {
                    **sub_args,
                    **{
                        "rbd-pool": config.get("rbd_pool", "rbd"),
                        **opt_args,
                    },
                }

                if bdev_cfg.get("pool"):
                    namespace_args.update({"rbd-pool": bdev_cfg["pool"]})

                # consider adding option to create pool and image if it doesn't exist
                # and also ns_create_image is false
                if bdev_cfg.get("ns_create_image"):
                    namespace_args.update(
                        {
                            "size": bdev_cfg.get("size", "1G"),
                            "rbd-create-image": bdev_cfg.get("ns_create_image", True),
                        }
                    )
                else:
                    with parallel() as p:
                        for num in range(bdev_cfg["count"]):
                            if rbd_obj:
                                pool = bdev_cfg.get(
                                    "pool", config.get("rbd_pool", "rbd")
                                )
                                p.spawn(
                                    rbd_obj.initial_rbd_config,
                                    pool=pool,
                                    image=f"{name}-image{num}",
                                    size=bdev_cfg.get("size", "1G"),
                                )
                            else:
                                raise ValueError(
                                    "RBD object not provided for pre-creating RBD image"
                                )

                lb_groups = namespace_args.pop("lb_groups", None)
                ceph_cluster = namespace_args.pop("ceph_cluster", None)
                with parallel() as p:
                    for num in range(bdev_cfg["count"]):
                        ns_args = deepcopy(namespace_args)
                        rbd_image = f"{name}-image{num}"
                        ns_args["rbd-image"] = rbd_image
                        ns_args = {"args": ns_args}
                        if lb_groups:
                            if isinstance(lb_groups, dict):
                                if bdev_cfg.get("lb_group"):
                                    lbgid = lb_groups[
                                        get_node_by_id(
                                            ceph_cluster,
                                            bdev_cfg["lb_group"],
                                        ).hostname
                                    ]
                                    ns_args.update({"load-balancing-group": lbgid})
                            elif opt_args.get("lb_groups") == "sequential":
                                lbgid = num
                        expected_namespaces.append(rbd_image)
                        p.spawn(gateway.namespace.add, **ns_args)
            validate_namespaces(gateway, expected_namespaces, nqn)


def validate_listeners(gateway, expected_listeners, nqn):
    """
    Validate that all the expected listeners are correctly configured in the gateway.
    Args:
        gateway: NVMeGateway instance
        expected_listeners: List of expected listener configurations to validate
        nqn: The NQN of the subsystem being validated
    """
    args = {"base_cmd_args": {"format": "json"}, "args": {"subsystem": nqn}}
    out, _ = gateway.listener.list(**args)
    listener_list = json.loads(out)["listeners"] if out else []

    for expected_listener in expected_listeners:
        match_found = False
        for listener in listener_list:
            if (
                listener.get("traddr") == expected_listener.get("traddr")
                and str(listener.get("trsvcid"))
                == str(expected_listener.get("trsvcid"))
                and listener.get("host_name") == expected_listener.get("host-name")
            ):
                match_found = True
                break
        if not match_found:
            raise ValueError(
                f"Expected listener {expected_listener} not found in configured listeners"
            )


def configure_listeners(gateways, config: dict, listeners=None):
    """
    Configure listeners for this specific gateway.
    This is called per gateway since each gateway needs its own listeners.
    Args:
        gateway: NVMeGateway instance
        config: Test configuration.
    """
    # Configure listeners if specified
    subsystem_config = config.get("subsystems", [])

    for sub_cfg in subsystem_config:
        expected_listeners = []
        nqn = sub_cfg.get("nqn") or sub_cfg.get("subnqn")
        if not listeners:
            listeners = sub_cfg.get("listeners", [])
        if listeners:
            if not isinstance(listeners, list):
                listeners = [listeners]

            for listener in listeners:
                gateway = [
                    gateway for gateway in gateways if listener in gateway.node.hostname
                ][0]
                listener_config = {
                    "args": {
                        "subsystem": nqn,
                        "traddr": getattr(gateway.node, "ip_address", None),
                        "trsvcid": sub_cfg.get("listener_port", DEFAULT_LISTENER_PORT),
                        "host-name": getattr(
                            gateway.node, "hostname", str(gateway.node)
                        ),
                    }
                }
                gateway.listener.add(**listener_config)
                expected_listeners.append(listener_config["args"])
            validate_listeners(gateway, expected_listeners, nqn)

        else:
            for gateway in gateways:
                listener_config = {
                    "args": {
                        "subsystem": sub_cfg.get("nqn") or sub_cfg.get("subnqn"),
                        "traddr": getattr(gateway.node, "ip_address", None),
                        "trsvcid": sub_cfg.get("listener_port", DEFAULT_LISTENER_PORT),
                        "host-name": getattr(
                            gateway.node, "hostname", str(gateway.node)
                        ),
                    }
                }
                gateway.listener.add(**listener_config)
                validate_listeners(gateway, expected_listeners, nqn)


def configure_gw_entities(nvme_service, rbd_obj=None, cluster=None):
    """
    Configure gateway entities for the NVMe service.
    This includes:
    - Configuring subsystems
    - Configuring hosts
    - Configuring namespaces
    - Configuring listeners
    Args:
        nvme_service: NvmeService instance
        exec_parallel: Whether to execute configuration in parallel
                       (default: False, sequential execution)
    """
    subsystem_config = nvme_service.config.get("subsystems", [])
    if subsystem_config:
        configure_subsystems(nvme_service)
        configure_listeners(nvme_service.gateways, nvme_service.config)
        configure_hosts(
            nvme_service.gateways[0], nvme_service.config, ceph_cluster=cluster
        )
        configure_namespaces(
            nvme_service.gateways[0], nvme_service.config, rbd_obj=rbd_obj
        )


def disconnect_initiators(nvme_service, node=None):
    """
    Disconnect all initiators from the NVMe service gateways.
    Args:
        nvme_service: NvmeService instance
    """
    if node:
        initiator = Initiator(node)
        initiator.disconnect_all()
        return
    for initiator_cfg in nvme_service.config.get("initiators", []):
        node = get_node_by_id(nvme_service.ceph_cluster, initiator_cfg["node"])
        initiator = Initiator(node)
        initiator.disconnect_all()


def teardown(nvme_service, rbd_obj, cleanup_config=None):
    """
    Cleanup NVMeoF gateways, initiators, and pools for the given config.
    Handles both single and multiple gateway groups.
    Args:
        nvme_service: NvmeService instance
        rbd_obj: RBD object for pool cleanup
    """
    pdb.set_trace()
    rc = 0
    # Disconnect initiators
    if "initiators" in nvme_service.config.get("cleanup", []):
        disconnect_initiators(nvme_service)

    # Delete the multiple subsystems across multiple gateways
    if "subsystems" in nvme_service.config["cleanup"]:
        config_sub_node = nvme_service.config["subsystems"]
        if not isinstance(config_sub_node, list):
            config_sub_node = [config_sub_node]
        for sub_cfg in config_sub_node:
            node = (
                nvme_service.config["gw_node"]
                if "node" not in sub_cfg
                else sub_cfg["node"]
            )
            gateway = nvme_service.gateways[0]
            out, err = gateway.subsystem.delete(
                **{"args": {"subsystem": sub_cfg["nqn"], "force": True}}
            )
            if "success" not in out.lower():
                LOG.warning(
                    f"Failed to delete subsystem {sub_cfg['nqn']}: {out} with error {err}"
                )
                rc = 1

    # Delete gateways
    if "gateway" in nvme_service.config.get("cleanup", []):
        rc = nvme_service.delete_nvme_service()
        if rc != 0:
            LOG.warning("Failed to delete NVMe gateways")

    # Delete the pool
    if "pool" in nvme_service.config["cleanup"]:
        subsystem_config = nvme_service.config.get("subsystems", [])
        pools_to_delete = set()
        for sub_cfg in subsystem_config:
            if sub_cfg.get("bdevs"):
                bdev_configs = sub_cfg["bdevs"]
                if isinstance(bdev_configs, dict):
                    bdev_configs = [bdev_configs]
                for bdev_cfg in bdev_configs:
                    if bdev_cfg.get("pool"):
                        pools_to_delete.add(bdev_cfg["pool"])
                    else:
                        pools_to_delete.add(nvme_service.rbd_pool)
        pools_to_delete.add(nvme_service.rbd_pool)
        if DEFAULT_NVME_METADATA_POOL not in nvme_service.nvme_metadata_pool:
            pools_to_delete.add(nvme_service.nvme_metadata_pool)
        rbd_obj.clean_up(pools=list(pools_to_delete))
    return rc


def fetch_namespaces(gateway, failed_ana_grp_ids=[], get_list=False):
    """Fetch all namespaces for failed gateways.

    Args:
        gateway: Operational gateway
        failed_ana_grp_ids: Failed or to-be failed gateway ids
    Returns:
        list of namespaces
    """
    args = {"base_cmd_args": {"format": "json"}}
    subsystems, _ = gateway.subsystem.list(**args)
    subsystems = json.loads(subsystems)

    namespaces = []
    all_ns = []
    for subsystem in subsystems["subsystems"]:
        sub_name = subsystem["nqn"]
        cmd_args = {"args": {"subsystem": subsystem["nqn"]}}
        nspaces, _ = gateway.namespace.list(**{**args, **cmd_args})
        nspaces = json.loads(nspaces)["namespaces"]
        all_ns.extend(nspaces)

        if failed_ana_grp_ids:
            for ns in nspaces:
                if ns["load_balancing_group"] in failed_ana_grp_ids:
                    # <subsystem>|<nsid>|<pool_name>|<image>
                    ns_info = f"nsid-{ns['nsid']}|{ns['rbd_pool_name']}|{ns['rbd_image_name']}"
                    if get_list:
                        namespaces.append({"list": ns, "info": f"{sub_name}|{ns_info}"})
                    else:
                        namespaces.append(f"{sub_name}|{ns_info}")
    if not failed_ana_grp_ids:
        LOG.info(f"All namespaces : {log_json_dump(all_ns)}")
        return all_ns

    LOG.info(
        f"Namespaces found for ANA-grp-id [{failed_ana_grp_ids}]: {log_json_dump(namespaces)}"
    )
    return namespaces
