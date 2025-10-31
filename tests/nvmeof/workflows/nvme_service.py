"""
NVMe Service, Gateway Group, and Gateway classes for NVMeoF workflows.
"""

from ceph.ceph_admin.orch import Orch
from ceph.utils import get_nodes_by_ids
from tests.cephadm import test_nvmeof
from tests.nvmeof.workflows.constants import DEFAULT_NVME_METADATA_POOL, DEFAULT_PORT
from tests.nvmeof.workflows.nvme_gateway import create_gateway
from tests.nvmeof.workflows.nvme_utils import nvme_gw_cli_version_adapter
from utility.utils import get_ceph_version_from_cluster


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

    def _determine_nvme_metadata_pool(self):
        """
        Determine the NVMe metadata pool name based on ceph_version.
        If ceph_version < 20.0, use config['nvme_metadata_pool'].
        If ceph_version >= 20.0, use DEFAULT_NVME_RBD_POOL.
        """
        current_ceph_version = get_ceph_version_from_cluster(self.clients[0])
        if current_ceph_version.startswith("20.0"):
            return DEFAULT_NVME_METADATA_POOL
        else:
            if not self.config.get("nvme_metadata_pool"):
                raise ValueError("Please provide RBD pool name via nvme_metadata_pool")
            return self.config.get("nvme_metadata_pool")

    def delete_nvme_service(self):
        """Delete the NVMe gateway service."""
        ceph_cluster = self.ceph_cluster

        gw_group = self.group
        pool = self.nvme_metadata_pool
        service_name = f"nvmeof.{pool}"
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
                    cfg["config"]["pos_args"].append(self.group)

                # Add rebalance period if specified
                if self.config.get("rebalance_period", False):
                    rebalance_sec = self.config.get("rebalance_period_sec", 0)
                    cfg["config"]["specs"][0]["spec"][
                        "rebalance_period_sec"
                    ] = rebalance_sec

                return cfg
        else:
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
                    "pos_args": [self.nvme_metadata_pool, self.group],
                },
            }

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
        deploy_config = self._create_spec_deployment_config()
        if deploy_config:
            test_nvmeof.run(self.ceph_cluster, **deploy_config)

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
