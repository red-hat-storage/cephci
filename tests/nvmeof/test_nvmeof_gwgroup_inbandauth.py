from ceph.ceph import Ceph
from ceph.nvmegw_cli import NVMeGWCLI
from ceph.nvmeof.initiator import Initiator
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.cephadm import test_nvmeof
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.nvme_utils import apply_nvme_sdk_cli_support
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id

LOG = Log(__name__)


def configure_listeners(ha, nodes, gw_group, config):
    """Configure Listeners on subsystem."""
    lb_group_ids = {}
    nqn = (
        f"{config['subnqn']}.{gw_group}"
        if gw_group and not config.get("no-group-append", False)
        else config["subnqn"]
    )
    for node in nodes:
        nvmegwcli = ha.check_gateway(node)
        hostname = nvmegwcli.fetch_gateway_hostname()
        nvmegwcli.listener.add(
            **{
                "args": {
                    "subsystem": nqn,
                    "traddr": nvmegwcli.node.ip_address,
                    "trsvcid": config["listener_port"],
                    "host-name": hostname,
                }
            }
        )
        lb_group_ids.update({hostname: nvmegwcli.ana_group_id})
    return lb_group_ids


def configure_subsystems(pool, auth_mode, ha, gw_group, subsys_config):
    """Configure Ceph-NVMEoF Subsystems."""
    nqn = (
        f"{subsys_config['subnqn']}.{gw_group}"
        if gw_group and not subsys_config.get("no-group-append", False)
        else subsys_config["subnqn"]
    )
    sub_args = {"subsystem": subsys_config["subnqn"]}
    ceph_cluster = subsys_config["ceph_cluster"]
    nvmegwcli = ha.gateways[0]
    subsys_config["auth_mode"] = auth_mode

    # Uncomment the below lines for debugging
    nvmegwcli.gateway.set_log_level(**{"args": {"level": "DEBUG"}})
    nvmegwcli.loglevel.set(**{"args": {"level": "DEBUG"}})

    if subsys_config.get("inband_auth"):
        ha.create_dhchap_key(subsys_config)
        sub_args["dhchap-key"] = subsys_config["dhchap-key"]

    # Add Subsystem
    nvmegwcli.subsystem.add(
        **{
            "args": {
                **sub_args,
                **{
                    "max-namespaces": subsys_config.get("max_ns", 32),
                    "enable-ha": subsys_config.get("enable_ha", False),
                    "no-group-append": subsys_config.get("no-group-append", False),
                },
            }
        }
    )

    sub_args["subsystem"] = nqn

    # Add Listeners
    listeners = subsys_config.get("listeners", [nvmegwcli.node.hostname])
    lb_groups = configure_listeners(ha, listeners, gw_group, subsys_config)

    # Add Host access
    if subsys_config.get("allow_host"):
        nvmegwcli.host.add(
            **{"args": {**sub_args, **{"host": repr(subsys_config["allow_host"])}}}
        )

    if subsys_config.get("hosts"):
        for host in subsys_config["hosts"]:
            initiator_node = get_node_by_id(ceph_cluster, host.get("node"))
            initiator = Initiator(initiator_node)

            # unidirectional inband authentication
            if not subsys_config.get("inband_auth") and host.get("inband_auth"):
                ha.create_dhchap_key(subsys_config)
                sub_args["dhchap-key"] = subsys_config["dhchap-key"]
            nvmegwcli.host.add(**{"args": {**sub_args, **{"host": initiator.nqn()}}})

    # Add Namespaces
    if subsys_config.get("bdevs"):
        bdev_configs = (
            [subsys_config["bdevs"]]
            if isinstance(subsys_config["bdevs"], dict)
            else subsys_config["bdevs"]
        )
        with parallel() as p:
            for bdev_cfg in bdev_configs:
                name = generate_unique_id(length=4)
                for num in range(bdev_cfg["count"]):
                    sub_args.pop("dhchap-key", None)
                    namespace_args = {
                        **sub_args,
                        **{
                            "rbd-pool": pool,
                            "rbd-create-image": True,
                            "size": bdev_cfg["size"],
                            "rbd-image": f"{name}-image{num}",
                        },
                    }
                    if bdev_cfg.get("lb_group"):
                        lbgid = lb_groups[
                            get_node_by_id(ceph_cluster, bdev_cfg["lb_group"]).hostname
                        ]
                        namespace_args["load-balancing-group"] = lbgid
                    p.spawn(nvmegwcli.namespace.add, **{"args": namespace_args})

    # Change key for subsystem
    subsys_update_key = subsys_config.get("update_dhchap_key", False)
    if subsys_update_key:
        ha.create_dhchap_key(subsys_config)
        sub_args["dhchap-key"] = subsys_config["dhchap-key"]
        nvmegwcli.subsystem.change_key(
            **{
                "args": {
                    **sub_args,
                }
            }
        )

    # Change key for hosts
    for host in subsys_config["hosts"]:
        host_update_key = host.get("update_dhchap_key", False)
        initiator_node = get_node_by_id(ceph_cluster, host.get("node"))
        initiator = Initiator(initiator_node)
        if subsys_update_key or host_update_key:
            if not subsys_update_key:
                ha.create_dhchap_key(subsys_config, update_host_key=True)
                sub_args["dhchap-key"] = subsys_config["dhchap-key"]
            nvmegwcli.host.change_key(
                **{"args": {**sub_args, **{"host": initiator.nqn()}}}
            )


def disconnect_initiator(ceph_cluster, node):
    """Disconnect Initiator."""
    node = get_node_by_id(ceph_cluster, node)
    initiator = Initiator(node)
    initiator.disconnect_all()


def delete_nvme_service(ceph_cluster, config):
    """Delete the NVMe gateway service.

    Args:
        ceph_cluster: Ceph cluster object
        config: Test case config

    Test case config should have below important params,
    - rbd_pool
    - gw_nodes
    - gw_group      # optional, as per release
    - mtls          # optional
    """
    gw_groups = config.get("gw_groups", [{"gw_group": config.get("gw_group", "")}])

    for gwgroup_config in gw_groups:
        gw_group = gwgroup_config["gw_group"]
        config["rbd_pool"] = gwgroup_config.get("rbd_pool", config["rbd_pool"])
        service_name = f"nvmeof.{config['rbd_pool']}"
        service_name = f"{service_name}.{gw_group}" if gw_group else service_name
        cfg = {
            "no_cluster_state": False,
            "config": {
                "command": "remove",
                "service": "nvmeof",
                "args": {
                    "service_name": service_name,
                    "verify": True,
                },
            },
        }
        test_nvmeof.run(ceph_cluster, **cfg)


def teardown(ceph_cluster, rbd_obj, config):
    """Cleanup the ceph-nvme gw entities.

    Args:
        ceph_cluster: Ceph Cluster
        rbd_obj: RBD object
        config: test config
    """
    # Delete the gateway
    if "gateway" in config["cleanup"]:
        delete_nvme_service(ceph_cluster, config)

    # Disconnect Initiators
    for gwgroup_config in config["gw_groups"]:
        if "initiators" in config["cleanup"]:
            for initiator_cfg in gwgroup_config["initiators"]:
                disconnect_initiator(ceph_cluster, initiator_cfg["node"])

    # Delete the pool
    if "pool" in config["cleanup"]:
        for gwgroup_config in config["gw_groups"]:
            gwgroup_config["rbd_pool"] = gwgroup_config.get(
                "rbd_pool", config["rbd_pool"]
            )
            rbd_obj.clean_up(pools=[gwgroup_config["rbd_pool"]])


def deploy_nvme_service_with_encryption_key(ceph_cluster, config):
    _cfg = apply_nvme_sdk_cli_support(ceph_cluster, config)
    _cfg["config"]["specs"][0]["encryption"] = True
    test_nvmeof.run(ceph_cluster, **_cfg)


def run(ceph_cluster: Ceph, **kwargs) -> int:
    LOG.info("Starting Ceph NVMEoF deployment.")
    config = kwargs["config"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    overrides = kwargs.get("test_data", {}).get("custom-config")
    for key, value in dict(item.split("=") for item in overrides).items():
        if key == "nvmeof_cli_image":
            NVMeGWCLI.NVMEOF_CLI_IMAGE = value
            break

    try:
        for gwgroup_config in config["gw_groups"]:
            gwgroup_config.update(
                {
                    "osp_cred": config["osp_cred"],
                    "rbd_pool": gwgroup_config.get("rbd_pool", config["rbd_pool"]),
                }
            )

            # Deploy NVMeOf service
            if config.get("install"):
                gwgroup_config["spec_deployment"] = True
                deploy_nvme_service_with_encryption_key(ceph_cluster, gwgroup_config)

                ha = HighAvailability(
                    ceph_cluster, gwgroup_config["gw_nodes"], **gwgroup_config
                )

            # Configure subsystems and run HA
            if gwgroup_config.get("subsystems"):
                with parallel() as p:
                    for subsys_args in gwgroup_config["subsystems"]:
                        subsys_args.update({"ceph_cluster": ceph_cluster})
                        p.spawn(
                            configure_subsystems,
                            config["rbd_pool"],
                            gwgroup_config.get("inband_auth_mode"),
                            ha,
                            gwgroup_config.get("gw_group"),
                            subsys_args,
                        )
            if gwgroup_config.get("fault-injection-methods") or config.get(
                "fault-injection-methods"
            ):
                ha.run()

            if "initiators" in config["cleanup"] and gwgroup_config.get("initiators"):
                for initiator_cfg in gwgroup_config["initiators"]:
                    disconnect_initiator(ceph_cluster, initiator_cfg["node"])

        return 0

    except Exception as err:
        LOG.error(err)
        return 1
    finally:
        if config.get("cleanup"):
            teardown(ceph_cluster, rbd_obj, config)
