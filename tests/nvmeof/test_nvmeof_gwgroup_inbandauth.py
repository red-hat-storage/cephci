import pdb

from ceph.ceph import Ceph
from tests.nvmeof.workflows.gateway_entities import (
    configure_hosts,
    configure_listeners,
    configure_namespaces,
    configure_subsystems,
    disconnect_initiators,
    teardown,
)
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.inband_auth import (
    change_host_key,
    change_subsystem_key,
    create_dhchap_key,
)
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
    subsys_args = {
        "no-group-append": gwgroup_config.get("no_group_append", False),
        "inband_auth": gwgroup_config.get("inband_auth", False),
        "ceph_cluster": ceph_cluster,
        "max_ns": gwgroup_config.get("max-namespaces", False),
        "listeners": gwgroup_config.get("listeners"),
        "bdevs": gwgroup_config.get("bdevs"),
        "listener_port": gwgroup_config.get("listener_port"),
        "gw_group": gwgroup_config.get("gw_group"),
    }

    hosts = gwgroup_config.get("hosts", [])
    initiators = []
    if gwgroup_config.get("subsystems"):
        initiators.extend(configure_subsystems(nvme_service))
        initiators.extend(
            configure_hosts(
                nvme_service.gateways[0],
                gwgroup_config,
                ceph_cluster=nvme_service.ceph_cluster,
            )
        )
        listeners = [nvme_service.gateways[0].node.id]
        for cfg in gwgroup_config["subsystems"]:
            if cfg.get("listeners"):
                listeners.extend(cfg["listeners"])
        listeners = list(set(listeners))
        configure_listeners(nvme_service.gateways, gwgroup_config, listeners=listeners)
        lb_groups = fetch_lb_groups(nvme_service.gateways, listeners)
        opt_args = {"ceph_cluster": ceph_cluster, "lb_groups": lb_groups}
        configure_namespaces(nvme_service.gateways[0], gwgroup_config, opt_args)
        for i in range(1, len(gwgroup_config["subsystems"])):
            subsys_args.update({"subnqn": f"nqn.2016-06.io.spdk:cnode{i}"})

            # Determine target hosts for this subsystem based on range
            for host in hosts:
                range_start, range_end = host.get("subsystem_range", [0, 0])
                if range_start <= i <= range_end:
                    subsys_args.update({"hosts": [host]})

                initiators.extend(
                    change_subsystem_key(
                        nvme_service.gateways[0], subsys_args, ceph_cluster
                    )
                )
                initiators.extend(
                    change_host_key(ceph_cluster, nvme_service.gateways[0], subsys_args)
                )
    return initiators


def test_ceph_83595512(ceph_cluster, gwgroup_config, nvme_service, ha):
    # Configure all entities with encryption
    initiators = configure_gw_entities_with_encryption(
        gwgroup_config, ceph_cluster, nvme_service
    )
    ha.clients.append(initiators)
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

    try:
        nvme_services = []
        for gwgroup_config in config["gw_groups"]:
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
                ha.clients.append(initiators)
                # if gwgroup_config.get("subsystems"):
                #     configure_subsystems(nvme_service)
                #     configure_hosts(
                #         nvme_service.gateways[0],
                #         gwgroup_config,
                #         ceph_cluster=nvme_service.ceph_cluster,
                #     )
                #     listeners = [nvme_service.gateways[0].node.id]
                #     for cfg in gwgroup_config["subsystems"]:
                #         if cfg.get("listeners"):
                #             listeners.extend(cfg["listeners"])
                #     listeners = list(set(listeners))
                #     configure_listeners(
                #         nvme_service.gateways, gwgroup_config, listeners=listeners
                #     )
                #     lb_groups = fetch_lb_groups(nvme_service.gateways, listeners)
                #     opt_args = {"ceph_cluster": ceph_cluster, "lb_groups": lb_groups}
                #     configure_namespaces(
                #         nvme_service.gateways[0], gwgroup_config, opt_args
                #     )
                #     for i in range(1, len(gwgroup_config["subsystems"])):
                #         subsys_args = {
                #             "no-group-append": gwgroup_config.get(
                #                 "no_group_append", False
                #             ),
                #             "inband_auth": gwgroup_config.get("inband_auth", False),
                #             "ceph_cluster": ceph_cluster,
                #             "max_ns": gwgroup_config.get("max-namespaces", False),
                #             "listeners": gwgroup_config.get("listeners"),
                #             "bdevs": gwgroup_config.get("bdevs"),
                #             "listener_port": gwgroup_config.get("listener_port"),
                #             "gw_group": gwgroup_config.get("gw_group"),
                #             "subnqn": f"nqn.2016-06.io.spdk:cnode{i}",
                #         }

                #         # Determine target hosts for this subsystem based on range
                #         for host in gwgroup_config.get("hosts", []):
                #             range_start, range_end = host.get("subsystem_range", [0, 0])
                #             if range_start <= i <= range_end:
                #                 subsys_args.update({"hosts": [host]})
                #             change_subsystem_key(
                #                 nvme_service.gateways[0], subsys_args, ceph_cluster
                #             )
                #             change_host_key(
                #                 ceph_cluster, nvme_service.gateways[0], subsys_args
                #             )
                if gwgroup_config.get("fault-injection-methods") or config.get(
                    "fault-injection-methods"
                ):
                    ha.run()

                if "initiators" in config["cleanup"] and gwgroup_config.get(
                    "initiators"
                ):
                    for initiator_cfg in gwgroup_config["initiators"]:
                        disconnect_initiators(ceph_cluster, initiator_cfg["node"])

        return 0

    except Exception as err:
        LOG.error(err)
        return 1
    finally:
        if config.get("cleanup"):
            for nvme_service in nvme_services:
                nvme_service.config.update({"cleanup": config["cleanup"]})
                pdb.set_trace()
                teardown(nvme_service, rbd_obj)
