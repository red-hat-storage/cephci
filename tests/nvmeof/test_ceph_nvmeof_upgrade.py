"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway HA
 with supported entities like subsystems , etc.,

"""

from concurrent.futures import ThreadPoolExecutor
from distutils.version import LooseVersion
from json import loads

from ceph.ceph import Ceph
from ceph.ceph_admin.orch import Orch
from ceph.nvmegw_cli import NVMeGWCLI
from ceph.parallel import parallel
from cephci.utils.configs import get_configs, get_registry_credentials
from cli.utilities.containers import Registry
from tests.nvmeof.test_ceph_nvmeof_high_availability import (
    configure_subsystems,
    teardown,
)
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.nvme_utils import deploy_nvme_service
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


class UpgradeFailure(Exception):
    pass


def upgrade_prerequisites(cluster, orch, **upg_cfg):
    """Run all Pre-requisites.

     - Identify the registry from build and login into registry.
     - Todo: Support CDN Build upgrade.

    Args:
        cluster: Ceph cluster
        orch: Ceph Orch object
        args: Pre-requisites args

    ::Example
        cfg:
          cdn: True | False     # Boolean
          ibm_build: True | False # Boolean
          overrides: List of container images
          release: Ceph Version

    """
    cdn = upg_cfg.get("cdn", False)
    ibm_build = upg_cfg.get("ibm_build", False)
    overrides = upg_cfg.get("overrides")
    release = upg_cfg.get("release")
    registry = None

    if ibm_build:
        get_configs()
        if not cdn:
            registry = get_registry_credentials("stage", "ibm")
        else:
            registry = get_registry_credentials("cdn", "ibm")
    if registry:
        for node in cluster.get_nodes(ignore="client"):
            Registry(node).login(
                registry["registry"], registry["username"], registry["password"]
            )
            args = {
                "registry-url": registry["registry"],
                "registry-username": registry["username"],
                "registry-password": registry["password"],
            }
            orch.registry_login(node=node, args=args)

    # Set Repo based on release
    if not cdn:
        orch.set_tool_repo()

        if overrides and not cdn:
            override_dict = dict(item.split("=") for item in overrides)
            supported_overrides = [
                "grafana",
                "keepalived",
                "haproxy",
                "prometheus",
                "node_exporter",
                "alertmanager",
                "promtail",
                "snmp_gateway",
                "loki",
            ]
            if release >= LooseVersion("7.0"):
                supported_overrides += [
                    "nvmeof",
                ]

            for image in supported_overrides:
                image_key = f"{image}_image"
                if override_dict.get(image_key):
                    cmd = f"ceph config set mgr mgr/cephadm/container_image_{image}"
                    cmd += f" {override_dict[image_key]}"
                    orch.shell(args=[cmd])
    else:
        # Todo: Support CDN repo builds in future
        orch.set_cdn_tool_repo(release)


def fetch_nvme_versions(gateways):
    """Fetch Gateway Versions"""
    info = dict()
    for gateway in gateways:
        _, gw = gateway.gateway.info(**{"base_cmd_args": {"format": "json"}})
        gw = loads(gw)
        node_info = {
            "version": gw["version"],
            "spdk-version": gw["spdk_version"],
        }
        info[gateway.hostname] = node_info

    return info


def compare_nvme_versions(pre_upgrade, post_upgrade):
    """Compare the NVMe and SPDK versions before and after the upgrade.

    Args:
        pre_upgrade: Pre Upgrade Versions
        post_upgrade: Post Upgrade Versions
    """

    def lv(metric):
        return LooseVersion(metric)

    for node, versions in post_upgrade.items():
        if lv(versions["version"]) < lv(pre_upgrade[node]["version"]):
            raise UpgradeFailure("NVMe Gateway Version is Lower or Same.")
        if lv(versions["spdk-version"]) < lv(pre_upgrade[node]["spdk-version"]):
            raise UpgradeFailure("SPDK Version is Lower")
        LOG.info(
            f"[ {node} ] Upgraded NVMe Version - {post_upgrade[node]},  Previous Version - {pre_upgrade[node]}"
        )


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof HA test execution.

    - Configure Gateways
    - Configures Initiators and Run FIO on NVMe targets.
    - Perform failover and failback.
    - Validate the IO continuation prior and after to failover and failback

    Args:
        ceph_cluster: Ceph cluster object
        kwargs: Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:

        # Execute the nvmeof GW test
            - test:
                name: Ceph NVMeoF deployment
                desc: Configure NVMEoF gateways and initiators
                config:
                    gw_nodes:
                     - node6
                    rbd_pool: rbd
                    do_not_create_image: true
                    rep-pool-only: true
                    cleanup-only: true                          # only for cleanup
                    rep_pool_config:
                      pool: rbd
                    install: true                               # Run SPDK with all pre-requisites
                    subsystems:                                 # Configure subsystems with all sub-entities
                      - nqn: nqn.2016-06.io.spdk:cnode3
                        serial: 3
                        bdevs:
                          count: 1
                          size: 100G
                        listener_port: 5002
                        allow_host: "*"
                    initiators:                             # Configure Initiators with all pre-req
                      - nqn: connect-all
                        listener_port: 4420
                        node: node10
                    upgrade:
                      cdn: True or False
                      release: 7.1                  # if cdn, provide release
    """
    LOG.info("Upgrade Ceph cluster with NVMEoF Gateways")
    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
    ibm_build = config.get("ibm_build", False)
    orch = Orch(cluster=ceph_cluster, **config)
    container_image = config.get("container_image")
    upgrade = config["upgrade"]
    overrides = kwargs.get("test_data", {}).get("custom-config")

    # Todo: Fix the CDN NVMe CLI image issue with framework support.
    for key, value in dict(item.split("=") for item in overrides).items():
        if key == "nvmeof_cli_image":
            NVMeGWCLI.NVMEOF_CLI_IMAGE = value
            break
    io_tasks = []
    executor = ThreadPoolExecutor()

    try:
        if config.get("install"):
            deploy_nvme_service(ceph_cluster, config)

        ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
        nvmegwcli = ha.gateways[0]

        # Configure Subsystem
        if config.get("subsystems"):
            with parallel() as p:
                for subsys_args in config["subsystems"]:
                    subsys_args["ceph_cluster"] = ceph_cluster
                    p.spawn(configure_subsystems, rbd_pool, ha, subsys_args)

        pre_upg_versions = fetch_nvme_versions(ha.gateways)

        # Prepare and Run FIO on NVMe devices
        namespaces = ha.fetch_namespaces(nvmegwcli)
        initiators = config["initiators"]
        ha.prepare_io_execution(initiators)
        ha.compare_client_namespace([i["uuid"] for i in namespaces])
        for initiator in ha.clients:
            io_tasks.append(executor.submit(initiator.start_fio))

        # Setup regisgtry
        upg_cfg = {
            "release": upgrade.get("release") or config.get("rhbuild"),
            "cdn": upgrade["cdn"],
            "overrides": overrides,
            "ibm_build": ibm_build,
        }

        upgrade_prerequisites(ceph_cluster, orch, **upg_cfg)

        # Install cephadm
        orch.install()

        # Check service versions vs available and target containers
        orch.upgrade_check(image=container_image)

        # Start Upgrade
        config.update({"args": {"image": "latest"}})
        orch.start_upgrade(config)

        # Monitor upgrade status, till completion
        orch.monitor_upgrade_status()

        # Validate upgraded versions of NVMe Gateways
        post_upg_versions = fetch_nvme_versions(ha.gateways)
        compare_nvme_versions(pre_upg_versions, post_upg_versions)

        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if io_tasks:
            LOG.info("Waiting for completion of IOs.")
            executor.shutdown(wait=True, cancel_futures=True)
        if config.get("cleanup"):
            teardown(ceph_cluster, rbd_obj, config)

    return 1
