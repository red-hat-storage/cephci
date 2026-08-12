"""
NVMe Service, Gateway Group, and Gateway classes for NVMeoF workflows.
"""

import json
import time

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
        # nvmeof_spec / cnc_spec require orch apply_spec so custom keys reach the service YAML
        self.is_spec_or_mtls = (
            self.mtls
            or self.config.get("spec_deployment", False)
            or bool(self.config.get("nvmeof_spec"))
            or bool(self.config.get("cnc_spec"))
        )
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

        # Add encryption if specified (TLS pre-shared key generated on installer)
        if self.inband_auth_mode:
            spec["encryption"] = True

        # Add support for enable_encryption and encryption_key_path params
        # Refer https://ibm-ceph.atlassian.net/browse/IBMCEPH-16168
        if self.config.get("enable_encryption", False):
            spec["enable_encryption"] = True
        if self.config.get("encryption_key_path", False):
            spec["encryption_key_path"] = True

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
                self._merge_nvmeof_spec_keys(cfg["config"]["specs"][0]["spec"])
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

                self._merge_nvmeof_spec_keys(cfg["config"]["specs"][0]["spec"])
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

    def _merge_nvmeof_spec_keys(self, spec_dict):
        """Merge suite ``nvmeof_spec`` / ``cnc_spec`` into the orch service spec.

        Product keys such as ``cnc_enable``, ``cnc_rate_limiter_bytes``,
        ``cnc_chunk_blocks``, and ``cnc_parallel_chunks`` are passed through
        unchanged so Jinja renders them into the cephadm YAML.
        """
        extras = {}
        for key in ("nvmeof_spec", "cnc_spec"):
            value = self.config.get(key)
            if isinstance(value, dict):
                extras.update(value)
        if extras:
            LOG.info(f"Merging custom nvmeof orch spec keys: {extras}")
            spec_dict.update(extras)

    def apply_nvmeof_spec(self, nvmeof_spec=None, redeploy=True, wait_sec=60):
        """Apply (or re-apply) the NVMe-oF orch service spec.

        Args:
            nvmeof_spec: Optional dict merged into config nvmeof_spec before apply
            redeploy: Whether to ``ceph orch redeploy`` after apply
            wait_sec: Sleep after redeploy for daemons to come up
        """
        if nvmeof_spec:
            merged = dict(self.config.get("nvmeof_spec") or {})
            merged.update(nvmeof_spec)
            self.config["nvmeof_spec"] = merged
        self.is_spec_or_mtls = True
        deploy_config = self._create_spec_deployment_config()
        if not deploy_config:
            raise RuntimeError("Failed to build nvmeof apply_spec config")
        LOG.info(f"Applying nvmeof orch spec: {deploy_config}")
        test_nvmeof.run(self.ceph_cluster, **deploy_config)
        # Refresh service_name / service_id after apply
        ceph = Orch(self.ceph_cluster, **{})
        out, _ = ceph.shell(args=["ceph orch ls nvmeof --format json"])
        services = json.loads(out)
        for service in services:
            if "nvmeof" not in service["service_name"]:
                continue
            if self.group and self.group not in service["service_name"]:
                continue
            self.service_name = service["service_name"]
            self.service_id = service["service_id"]
            break
        if redeploy:
            self.redeploy(wait_sec=wait_sec)

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

    def redeploy(self, wait_sec=30):
        """Redeploy the NVMe-oF orchestrator service after spec apply."""
        if not self.service_name:
            raise RuntimeError("NVMe-oF service name not set; deploy the service first")
        orch = Orch(self.ceph_cluster, **{})
        cmd = f"ceph orch redeploy {self.service_name}"
        LOG.info("Redeploying NVMe-oF service: %s", cmd)
        orch.shell(args=[cmd])
        if wait_sec:
            time.sleep(wait_sec)

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
