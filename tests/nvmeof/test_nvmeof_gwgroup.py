from ceph.ceph import Ceph
from ceph.parallel import parallel
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


def run_gateway_group_operations(
    ceph_cluster, gwgroup_config, config, rbd_obj, nvme_service
):
    try:
        gwgroup_config["osp_cred"] = config["osp_cred"]
        gwgroup_config["rbd_pool"] = config["rbd_pool"]
        # if we have different RBD pools per GWgroup
        config["rbd_pool"] = gwgroup_config.get("rbd_pool", config["rbd_pool"])
        config["rep_pool_config"]["pool"] = gwgroup_config.get(
            "rbd_pool", config["rbd_pool"]
        )

        ha = HighAvailability(
            ceph_cluster, gwgroup_config["gw_nodes"], **gwgroup_config
        )
        ha.initialize_gateways()

        # Configure subsystems in GWgroups
        if gwgroup_config.get("subsystems"):
            with parallel() as p:
                p.spawn(
                    configure_gw_entities,
                    nvme_service,
                    rbd_obj=rbd_obj,
                )

        # HA failover and failback
        if gwgroup_config.get("fault-injection-methods") or config.get(
            "fault-injection-methods"
        ):
            ha.run(FEWR_NAMESPACES=True)

    except Exception as err:
        LOG.error(f"Error in gateway group {gwgroup_config['gw_nodes']}: {err}")
        raise err


def run(ceph_cluster: Ceph, **kwargs) -> int:
    LOG.info("Starting Ceph NVMEoF deployment.")
    config = kwargs["config"]

    overrides = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=overrides)

    try:
        cleanup_dict = {}
        if config.get("parallel"):
            with parallel() as p:
                for gwgroup_config in config["gw_groups"]:
                    clean_up = ["subsystems", "initiators", "pool", "gateway"]
                    gwgroup_config.update({"cleanup": clean_up})
                    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
                    # Deploy NVMeOf services
                    if config.get("install"):
                        gwgroup_config["rbd_pool"] = gwgroup_config.get(
                            "rbd_pool", config["rbd_pool"]
                        )
                        nvme_service = NVMeService(gwgroup_config, ceph_cluster)
                        LOG.info("Deploy NVMe service")
                        nvme_service.deploy()
                        LOG.info("Initialize gateways")
                        nvme_service.init_gateways()
                    p.spawn(
                        run_gateway_group_operations,
                        ceph_cluster,
                        gwgroup_config,
                        config,
                        rbd_obj,
                        nvme_service,
                    )
                    cleanup_dict.update({nvme_service: rbd_obj})
        else:
            for gwgroup_config in config["gw_groups"]:
                clean_up = ["subsystems", "initiators", "pool", "gateway"]
                gwgroup_config.update({"cleanup": clean_up})
                rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
                # Deploy NVMeOf services
                if config.get("install"):
                    gwgroup_config["rbd_pool"] = gwgroup_config.get(
                        "rbd_pool", config["rbd_pool"]
                    )
                    nvme_service = NVMeService(gwgroup_config, ceph_cluster)
                    LOG.info("Deploy NVMe service")
                    nvme_service.deploy()
                    LOG.info("Initialize gateways")
                    nvme_service.init_gateways()
                run_gateway_group_operations(
                    ceph_cluster, gwgroup_config, config, rbd_obj, nvme_service
                )
                cleanup_dict.update({nvme_service: rbd_obj})

        return 0

    except Exception as err:
        LOG.error(err)
        return 1
    finally:
        if config.get("cleanup"):
            for nvme_service, rbd_obj in cleanup_dict.items():
                teardown(nvme_service, rbd_obj)
            LOG.info("Cleanup completed.")
