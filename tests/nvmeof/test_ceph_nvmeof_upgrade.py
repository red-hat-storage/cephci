"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway HA
 with supported entities like subsystems , etc.,
"""

import re
import time
from concurrent.futures import ThreadPoolExecutor
from json import loads

from looseversion import LooseVersion

from ceph.ceph import Ceph
from ceph.ceph_admin.orch import Orch
from ceph.utils import get_node_by_id
from cephci.utils.configs import get_configs, get_registry_credentials
from cli.utilities.containers import Registry
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
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
        if overrides and not cdn:
            override_dict = overrides
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
        gw, _ = gateway.gateway.info(**{"base_cmd_args": {"format": "json"}})
        gw = loads(gw)
        # We need to extract the SPDK version from the version string
        # "SPDK v24.09 git sha1 316ca90fec66c0d524bfef41bb8996e0b11fb655",
        # "SPDK v25.05 git sha1 c19d07f46274d31201c0c4db3775a655d68f5f38",
        # "24.01.1",
        # "SPDK v24.09"
        spdk_version = None
        pattern = re.compile(r"(?:v)?(\d+\.\d+(?:\.\d+)?)")
        match = pattern.search(gw["version"])
        if match:
            spdk_version = match.group(1)
        else:
            spdk_version = gw["spdk_version"]
        node_info = {
            "version": gw["version"],
            "spdk-version": spdk_version,
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
    ctm = config["manifest"]

    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    ibm_build = True if ctm.product == "ibm" else False

    orch = Orch(cluster=ceph_cluster, **config)
    container_image = config.get("container_image")

    upgrade = config["upgrade"]
    overrides = kwargs.get("test_data", {}).get("custom_config_dict")

    io_tasks = []
    executor = ThreadPoolExecutor()

    try:
        if config.get("install"):
            custom_config = kwargs.get("test_data", {}).get("custom-config")
            LOG.info("Check and set NVMe CLI image")
            check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)
            nvme_service = NVMeService(config, ceph_cluster)
            LOG.info("Deploy NVMe service")
            nvme_service.deploy()
            LOG.info("Initialize gateways")
            nvme_service.init_gateways()

        # ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
        nvmegwcli = nvme_service.gateways[0]

        # Configure Subsystem
        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd_obj, cluster=ceph_cluster)

        pre_upg_versions = fetch_nvme_versions(nvme_service.gateways)

        for initiator in config["initiators"]:
            client = get_node_by_id(ceph_cluster, initiator["node"])
            initiator_obj = NVMeInitiator(client)
            initiator_obj.connect_targets(nvmegwcli, initiator)
            paths = initiator_obj.list_devices()
            i = initiator_obj.start_fio(paths=paths, **initiator)
            LOG.info(f"FIO Result: {i}")

        # Setup regisgtry
        upg_cfg = {
            "release": upgrade.get("release") or config.get("rhbuild"),
            "cdn": upgrade["cdn"],
            "overrides": overrides,
            "ibm_build": ibm_build,
        }

        LOG.info("Upgrade prerequisites")
        upgrade_prerequisites(ceph_cluster, orch, **upg_cfg)

        # Install cephadm
        orch.install()

        # Check service versions vs available and target containers
        orch.upgrade_check(image=container_image)

        LOG.info("Start Upgrade")
        # Start Upgrade
        config.update({"args": {"image": "latest"}})
        orch.start_upgrade(config)

        LOG.info("Monitor upgrade status, till completion")
        # Monitor upgrade status, till completion
        orch.monitor_upgrade_status()

        # Post upgrade nvmeof daemons are taking time to start, so wait for 60 seconds
        time.sleep(60)
        LOG.info("Validate upgraded versions of NVMe Gateways")
        # Validate upgraded versions of NVMe Gateways
        post_upg_versions = fetch_nvme_versions(nvme_service.gateways)
        compare_nvme_versions(pre_upg_versions, post_upg_versions)

        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if io_tasks:
            LOG.info("Waiting for completion of IOs.")
            executor.shutdown(wait=True, cancel_futures=True)
        if config.get("cleanup"):
            teardown(nvme_service, rbd_obj)

    return 1
