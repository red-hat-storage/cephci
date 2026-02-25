from ceph.ceph import Ceph
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.gateway_entities import (
    configure_hosts,
    configure_listeners,
    configure_namespaces,
    configure_subsystems,
    disconnect_initiators,
    teardown,
)
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.inband_auth import change_host_key, change_subsystem_key
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    fetch_lb_groups,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


def configure_gw_entities_with_encryption(gwgroup_config, ceph_cluster, nvme_service):
    """
    Docstring for configure_gw_entities_with_encryption
    """
    hosts = gwgroup_config.get("hosts", [])
    initiators = []
    if gwgroup_config.get("subsystems"):
        initiators.extend(configure_subsystems(nvme_service))
        # configure_hosts will be called for all subsystems in the loop below
        # to ensure consistent handling
        listeners = [nvme_service.gateways[0].node.id]
        for cfg in gwgroup_config["subsystems"]:
            if cfg.get("listeners"):
                listeners.extend(cfg["listeners"])
            if cfg.get("hosts"):
                hosts.extend(cfg["hosts"])
        listeners = list(set(listeners))
        hosts = list({tuple(sorted(h.items())): h for h in hosts}.values())
        configure_listeners(nvme_service.gateways, gwgroup_config, listeners=listeners)
        lb_groups = fetch_lb_groups(nvme_service.gateways, listeners)
        opt_args = {"ceph_cluster": ceph_cluster, "lb_groups": lb_groups}
        configure_namespaces(nvme_service.gateways[0], gwgroup_config, opt_args)
        # configure_hosts returns initiators it creates (important for unidirectional auth)
        # Pass initiators list so it appends to it and returns it
        initiators = configure_hosts(
            nvme_service.gateways[0],
            gwgroup_config,
            ceph_cluster=ceph_cluster,
            initiators=initiators,
        )

        for cfg in gwgroup_config["subsystems"]:
            # Ensure auth_mode is set from gwgroup level if not already set in subsystem config
            if "auth_mode" not in cfg and gwgroup_config.get("inband_auth_mode"):
                cfg["auth_mode"] = gwgroup_config.get("inband_auth_mode")
                LOG.info(
                    f"Setting auth_mode={cfg['auth_mode']} for subsystem {cfg.get('nqn') or cfg.get('subnqn')}"
                )

            # Ensure subnqn is set in cfg (needed for change_subsystem_key and change_host_key)
            if "subnqn" not in cfg:
                cfg["subnqn"] = cfg.get("nqn") or cfg.get("subnqn")

            if cfg.get("update_dhchap_key"):
                change_subsystem_key(
                    nvme_service.gateways[0], cfg, ceph_cluster, initiators=initiators
                )
                for host in cfg.get("hosts", []):
                    if host.get("update_dhchap_key"):
                        # change_host_key expects subsys_config with subnqn and hosts list
                        host_subsys_config = {
                            "subnqn": cfg.get("subnqn") or cfg.get("nqn"),
                            "hosts": [host],
                            "auth_mode": cfg.get("auth_mode"),
                        }
                        # Copy any other relevant config
                        if cfg.get("dhchap-key"):
                            host_subsys_config["dhchap-key"] = cfg["dhchap-key"]
                        change_host_key(
                            ceph_cluster,
                            nvme_service.gateways[0],
                            host_subsys_config,
                            initiators=initiators,
                        )

    return initiators


def test_ceph_83595512(ceph_cluster, gwgroup_config, nvme_service, ha):
    # Configure all entities with encryption
    initiators = configure_gw_entities_with_encryption(
        gwgroup_config, ceph_cluster, nvme_service
    )

    # Pass pre-configured initiators to HA object
    ha.config["pre_configured_initiators"] = initiators
    ha.clients = initiators
    ha.nvme_service = nvme_service

    # configure initiators
    if gwgroup_config.get("fault-injection-methods"):
        gw_group = gwgroup_config.get("gw_group")
        updated_initiators = []

        for initiator in gwgroup_config.get("initiators"):
            sub_count = initiator["subsystems"]

            for i in range(1, sub_count):
                new_initiator = dict(initiator)
                new_initiator["nqn"] = f"nqn.2016-06.io.spdk:cnode{i}.{gw_group}"
                updated_initiators.append(new_initiator)

        ha.config["initiators"] = updated_initiators
        ha.run()


testcases = {
    "CEPH-83595512": test_ceph_83595512,
}


def run(ceph_cluster: Ceph, **kwargs) -> int:
    LOG.info("Starting Ceph NVMEoF deployment.")
    config = kwargs["config"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    overrides = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=overrides)

    nvme_services = []
    try:
        for gwgroup_config in config["gw_groups"]:
            initiators = []
            LOG.info(f"Configuring NVMeOf Gateway Group: {gwgroup_config['gw_group']}")
            gwgroup_config.update(
                {
                    "osp_cred": config["osp_cred"],
                    "rbd_pool": gwgroup_config.get("rbd_pool", config["rbd_pool"]),
                }
            )

            # Deploy NVMeOf service
            if config.get("install"):
                gwgroup_config["spec_deployment"] = True

                ha = HighAvailability(
                    ceph_cluster, gwgroup_config["gw_nodes"], **gwgroup_config
                )
                nvme_service = NVMeService(gwgroup_config, ceph_cluster)
                gwgroup_config["spec_deployment"] = True
                if config.get("install"):
                    LOG.info("deploy nvme service")
                    nvme_service.deploy()

                nvme_service.init_gateways()
                nvme_services.append(nvme_service)
                ha.gateways = nvme_service.gateways

            # Configure subsystems and run HA
            if config.get("test_case"):
                test_case_run = testcases[config["test_case"]]
                test_case_run(ceph_cluster, gwgroup_config, nvme_service, ha)

            else:
                # Configure all entities with encryption
                initiators = configure_gw_entities_with_encryption(
                    gwgroup_config, ceph_cluster, nvme_service
                )
                # Pass pre-configured initiators to HA object
                ha.config["pre_configured_initiators"] = initiators
                ha.clients = initiators
                ha.nvme_service = (
                    nvme_service  # Pass the NVMeService instance to HA for later use
                )
                if gwgroup_config.get("fault-injection-methods") or config.get(
                    "fault-injection-methods"
                ):
                    ha.run()

                if "initiators" in config["cleanup"] and gwgroup_config.get(
                    "initiators"
                ):
                    for initiator_cfg in gwgroup_config["initiators"]:
                        node_id = initiator_cfg["node"]
                        node = get_node_by_id(ceph_cluster, node_id)
                        if not node:
                            LOG.warning(
                                f"Node {node_id} not found in cluster, skipping disconnect"
                            )
                            continue
                        disconnect_initiators(nvme_service, node=node)

        return 0

    except Exception as err:
        LOG.error(err)
        return 1
    finally:
        if config.get("cleanup"):
            for nvme_service in nvme_services:
                nvme_service.config.update({"cleanup": config["cleanup"]})
                teardown(nvme_service, rbd_obj)
