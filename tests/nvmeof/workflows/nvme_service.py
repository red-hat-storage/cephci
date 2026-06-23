"""
NVMe Service, Gateway Group, and Gateway classes for NVMeoF workflows.
"""

import json

from looseversion import LooseVersion

from ceph.ceph_admin.orch import Orch
from ceph.utils import get_nodes_by_ids
from tests.cephadm import test_nvmeof
from tests.nvmeof.workflows.constants import DEFAULT_NVME_METADATA_POOL, DEFAULT_PORT
from tests.nvmeof.workflows.nvme_gateway import create_gateway
from tests.nvmeof.workflows.nvme_utils import (
    check_and_enable_nvmeof_module,
    nvme_gw_cli_version_adapter,
    setup_firewalld,
)
from utility.log import Log
from utility.utils import get_ceph_version_from_cluster

LOG = Log(__name__)


class NVMeService:
    def __init__(
        self,
        config,
        ceph_cluster,
    ):
        self.config = config
        self.group = self.config.get("gw_group", None)
        self.mtls = config.get("mtls", False)
        self.inband_auth_mode = config.get("inband_auth_mode", None)
        self.ceph_cluster = ceph_cluster
        self.clients = self.ceph_cluster.get_nodes(role="client")
        if not self.clients:
            raise ValueError("No client nodes found in the cluster")
        self.ceph_version = self._get_ceph_version()
        self.nvme_metadata_pool = self._determine_nvme_metadata_pool()
        self.rbd_pool = config.get("rbd_pool")
        if not self.rbd_pool:
            raise ValueError("Please provide RBD pool name via rbd_pool")
        gw_nodes = config.get("gw_nodes", None) or config.get("gw_node", None)
        if not gw_nodes:
            raise ValueError("Please provide gateway nodes via gw_nodes or gw_node")

        if not isinstance(gw_nodes, list):
            gw_nodes = [gw_nodes]

        self.gw_nodes = get_nodes_by_ids(self.ceph_cluster, gw_nodes)
        self.is_spec_or_mtls = self.mtls or self.config.get("spec_deployment", False)
        if self.inband_auth_mode:
            self.is_spec_or_mtls = True

    def _get_ceph_version(self):
        return get_ceph_version_from_cluster(self.clients[0])

    def _determine_nvme_metadata_pool(self):
        """
        Determine the NVMe metadata pool name based on ceph_version.
        If ceph_version >= 20.2.1, use DEFAULT_NVME_METADATA_POOL (.nvmeof).
        If ceph_version < 20.2.1, use config['nvme_metadata_pool'].
        """
        if LooseVersion(self.ceph_version) >= LooseVersion("20.2.1"):
            # print the nvmeof metadata pool
            LOG.info(f"Using NVMeoF metadata pool: {DEFAULT_NVME_METADATA_POOL}")
            return DEFAULT_NVME_METADATA_POOL
        else:
            LOG.info(
                f"Using NVMe metadata pool: {self.config.get('nvme_metadata_pool')}"
            )
            if not self.config.get("nvme_metadata_pool"):
                raise ValueError("Please provide RBD pool name via nvme_metadata_pool")
            return self.config.get("nvme_metadata_pool")

    def delete_nvme_service(self):
        """Delete the NVMe gateway service."""
        ceph_cluster = self.ceph_cluster

        service_name = self.service_name
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
        rc = test_nvmeof.run(ceph_cluster, **cfg)
        return rc

    def _create_spec_deployment_config(self):
        """Create spec-based deployment configuration."""
        release = self.ceph_cluster.rhcs_version
        spec = {
            "service_type": "nvmeof",
            "service_id": self.nvme_metadata_pool,
            "mtls": self.mtls,
            "placement": self._get_placement_config(self.config, self.gw_nodes),
            "spec": {
                "pool": self.nvme_metadata_pool,
                "enable_auth": self.config.get("mtls", False),
            },
        }

        # Delete pool key from spec if ceph_version >= 20.2.1
        if LooseVersion(self.ceph_version) >= LooseVersion("20.2.1"):
            spec["spec"].pop("pool")

        # Add encryption if specified
        if self.inband_auth_mode:
            spec["encryption"] = True

        # Add group if specified
        if self.group:
            spec["spec"]["group"] = self.group

        if self.is_spec_or_mtls:
            cfg = {
                "no_cluster_state": False,
                "config": {
                    "command": "apply_spec",
                    "service": "nvmeof",
                    "validate-spec-services": self.config.get(
                        "validate-spec-services", True
                    ),
                    "specs": [spec],
                },
            }
            # Handle version-specific logic
            if release <= "7.1":
                return cfg
            elif release >= "8":
                if not self.group:
                    raise ValueError("Gateway group not provided for RHCS 8+")

                if self.is_spec_or_mtls:
                    cfg["config"]["specs"][0][
                        "service_id"
                    ] = f"{self.nvme_metadata_pool}.{self.group}"
                    cfg["config"]["specs"][0]["spec"]["group"] = self.group
                else:
                    if LooseVersion(self.ceph_version) >= LooseVersion("20.2.1"):
                        cfg["config"]["args"].update({"group": self.group})
                    else:
                        cfg["config"]["pos_args"].append(self.group)

                # Add rebalance period if specified
                if self.config.get("rebalance_period", False):
                    rebalance_sec = self.config.get("rebalance_period_sec", 0)
                    cfg["config"]["specs"][0]["spec"][
                        "rebalance_period_sec"
                    ] = rebalance_sec

                return cfg
        else:
            pos_args = [self.nvme_metadata_pool]
            # group name is optional in 7.x so ignore it in that case
            if self.group is not None:
                pos_args.append(self.group)
            cfg = {
                "no_cluster_state": False,
                "config": {
                    "command": "apply",
                    "service": "nvmeof",
                    "args": {
                        "placement": self._get_placement_config(
                            self.config, self.gw_nodes
                        )
                    },
                    "pos_args": pos_args,
                },
            }

            if LooseVersion(self.ceph_version) >= LooseVersion("20.2.1"):
                cfg["config"]["args"].update({"group": self.group})
                # Delete pos_args key from cfg
                cfg["config"].pop("pos_args")

        return cfg

    def _get_placement_config(self, config, gw_nodes):
        """Get placement configuration based on config options."""
        placement = {"nodes": [i.hostname for i in gw_nodes]}

        # Add label-based placement if specified
        if config.get("label"):
            placement["label"] = config["label"]

        # Add limit if specified
        if config.get("limit"):
            placement["limit"] = config["limit"]

        # Add separator if specified
        if config.get("sep"):
            placement["sep"] = config["sep"]

        return placement

    def deploy(self):
        """
        Deploy NVMe gateways using orchestrator, then fetch and update daemon and service names for each gateway node.
        """
        # Open up firewall ports if running.
        setup_firewalld(self.gw_nodes)
        # Enable ceph mgr module enable nvmeof if not enabled
        check_and_enable_nvmeof_module(
            ceph_cluster=self.ceph_cluster, ceph_version=self.ceph_version
        )
        deploy_config = self._create_spec_deployment_config()
        if deploy_config:
            test_nvmeof.run(self.ceph_cluster, **deploy_config)

        # Once the service is deployed, get the service name and service id and store it
        ceph = Orch(self.ceph_cluster, **{})
        cmd = "ceph orch ls nvmeof --format json"
        out, _ = ceph.shell(args=[cmd])
        services = json.loads(out)
        self.service_name = None
        self.service_id = None
        for service in services:
            # If we have multiple services in single cluster then we need to filter the service by group
            # so that we will get the correct service name and service id for the group.
            # when we take services[0]["service_name"] only first service name will be returned
            # so we need to filter the service by group.
            if "nvmeof" in service["service_name"]:
                if self.group:
                    if self.group in service["service_name"]:
                        service_name = service["service_name"]
                        service_id = service["service_id"]
                        LOG.info(
                            f"Service name: {service_name}, Service id: {service_id}"
                        )
                        self.service_name = service_name
                        self.service_id = service_id
                        break
                else:
                    service_name = service["service_name"]
                    service_id = service["service_id"]
                    LOG.info(f"Service name: {service_name}, Service id: {service_id}")
                    self.service_name = service_name
                    self.service_id = service_id
                    break

    def init_gateways(self):
        """
        Initialize NVMeGateway objects for each ceph_node in the group.
        """
        self.gateways = []
        port = getattr(self, "port", DEFAULT_PORT)

        ceph = Orch(self.ceph_cluster, **{})

        for node in self.gw_nodes:
            self.gateways.append(
                create_gateway(
                    nvme_gw_cli_version_adapter(self.ceph_cluster),
                    node,
                    mtls=self.mtls,
                    shell=getattr(ceph, "shell"),
                    port=port,
                    gw_group=self.group,
                )
            )
