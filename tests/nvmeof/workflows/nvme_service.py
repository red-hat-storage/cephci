"""
NVMe Service, Gateway Group, and Gateway classes for NVMeoF workflows.
"""

import json
import time

from looseversion import LooseVersion

from ceph.ceph_admin.orch import Orch
from ceph.nvmeof.cli.v1 import NVMeGWCLI
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
        self.tls_psk = config.get("tls_psk", False)
        self.inband_auth_mode = config.get("inband_auth_mode", None)
        self.ceph_cluster = ceph_cluster

        # Optional explicit CLI version override: "v1" forces NVMeGWCLI (podman-run),
        # "v2" forces NVMeGWCLIV2 (ceph nvmeof), None = auto-detect from cluster version.
        self.nvmeof_cli_version = config.get("nvmeof_cli_version", None)
        nvmeof_cli_image = config.get("nvmeof_cli_image", None)
        if nvmeof_cli_image:
            LOG.info("NVMeService: setting NVMeGWCLI image to %s", nvmeof_cli_image)
            NVMeGWCLI.NVMEOF_CLI_IMAGE = nvmeof_cli_image
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
        if self.inband_auth_mode or self.tls_psk:
            self.is_spec_or_mtls = True
        self.service_name = None
        self.service_id = None

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
        """Delete the NVMe gateway service.

        Safe to call even when deploy() failed mid-way:
        - If self.service_name was never set, _discover_service_name() is called
          first to find any running nvmeof service for this group on the cluster.
        - If no service exists at all, returns 0 (nothing to clean up).
        """
        ceph_cluster = self.ceph_cluster

        # If deploy failed before _discover_service_name ran, try to find it now
        if not self.service_name:
            try:
                self._discover_service_name()
            except Exception as exc:
                LOG.warning(
                    "delete_nvme_service: could not discover service name: %s", exc
                )

        service_name = self.service_name
        if not service_name:
            LOG.warning(
                "delete_nvme_service: no service_name known — "
                "nothing to remove (deploy may have failed before service was created)"
            )
            return 0

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
                # enable_auth is mTLS for nvme CLI ↔ gateway.
                # enable_encryption is TLS-PSK for initiator IO in transit.
                # The two are independent and may both be true.
                "enable_auth": self.config.get("mtls", False),
            },
        }

        # Delete pool key from spec if ceph_version >= 20.2.1
        if LooseVersion(self.ceph_version) >= LooseVersion("20.2.1"):
            spec["spec"].pop("pool")

        # Add encryption if specified (TLS pre-shared key generated on installer)
        if self.inband_auth_mode:
            spec["encryption"] = True

        # TLS-PSK: enable_encryption + encryption_key_path in spec["spec"].
        # enable_auth is left as the mtls flag set it. A TLS-PSK spec is:
        #   enable_auth: <mtls>
        #   enable_encryption: True
        #   encryption_key_path: /root/encryption.key   (or custom path from config)
        # Key is generated with:
        #   openssl req -newkey rsa:512 -noenc -noout -keyout <path> -batch 2>/dev/null
        # and distributed to every GW node before deploy() calls apply_spec.
        if self.tls_psk:
            spec["spec"]["enable_encryption"] = True
            spec["spec"]["encryption_key_path"] = self.config.get(
                "encryption_key_path", "/root/encryption.key"
            )

        # Add support for standalone enable_encryption and encryption_key_path params
        # (non-TLS-PSK BYOK path) — Refer https://ibm-ceph.atlassian.net/browse/IBMCEPH-16168
        if not self.tls_psk:
            if self.config.get("enable_encryption", False):
                spec["spec"]["enable_encryption"] = True
            if self.config.get("encryption_key_path"):
                spec["spec"]["encryption_key_path"] = self.config["encryption_key_path"]

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
                    pool_id = self.nvme_metadata_pool.lstrip(".")
                    cfg["config"]["specs"][0]["service_id"] = f"{pool_id}.{self.group}"
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

                # Add state-update tuning parameters if specified
                if "state_update_interval_sec" in self.config:
                    cfg["config"]["specs"][0]["spec"]["state_update_interval_sec"] = (
                        self.config["state_update_interval_sec"]
                    )
                if "state_update_notify" in self.config:
                    cfg["config"]["specs"][0]["spec"]["state_update_notify"] = (
                        self.config["state_update_notify"]
                    )

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

    def apply_nvmeof_spec(
        self, nvmeof_spec=None, redeploy=True, wait_sec=60, generate_key=False
    ):
        """Apply (or re-apply) the NVMe-oF orch service spec.

        When ``tls_psk`` is True the spec includes ``enable_encryption: true``
        and ``encryption_key_path``. ``enable_auth`` stays at the ``mtls``
        value. mTLS and TLS-PSK may both be enabled::

            enable_auth: <mtls>
            enable_encryption: True
            encryption_key_path: <config["encryption_key_path"] or /root/encryption.key>

        The RSA key must already exist on every GW node at ``encryption_key_path``
        before this is called.  If the caller has not yet generated the key (e.g.
        during an initial apply from outside ``deploy()``), pass
        ``generate_key=True`` and this method will run
        ``_generate_and_distribute_encryption_key()`` first.

        Args:
            nvmeof_spec: Optional dict merged into config nvmeof_spec before apply.
            redeploy (bool): Whether to ``ceph orch redeploy`` after apply.
            wait_sec (int): Sleep after redeploy for daemons to come up.
            generate_key (bool): When True and tls_psk is enabled, regenerate +
                distribute the RSA key to all GW nodes before applying the spec.
                Default False (callers like tlspsk.py manage key distribution
                themselves).
        """
        if nvmeof_spec:
            merged = dict(self.config.get("nvmeof_spec") or {})
            merged.update(nvmeof_spec)
            self.config["nvmeof_spec"] = merged
        self.is_spec_or_mtls = True

        # Optionally generate + distribute the RSA key before apply_spec
        if self.tls_psk and generate_key:
            self._generate_and_distribute_encryption_key()

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

    def _discover_service_name(self):
        """Discover NVMeoF orchestrator service name/id when not deployed by this test."""
        if self.service_name:
            return

        ceph = Orch(self.ceph_cluster, **{})
        cmd = "ceph orch ls nvmeof --format json"
        out, _ = ceph.shell(args=[cmd])
        services = json.loads(out)
        for service in services:
            if "nvmeof" not in service["service_name"]:
                continue
            if self.group:
                if not service["service_name"].endswith(f".{self.group}"):
                    continue
            self.service_name = service["service_name"]
            self.service_id = service["service_id"]
            LOG.info(
                "Discovered NVMeoF service name: %s, service id: %s",
                self.service_name,
                self.service_id,
            )
            return

    def _generate_and_distribute_encryption_key(self):
        """Generate an RSA TLS-PSK encryption key and distribute it to all GW nodes.

        Runs on the first GW node::

            openssl req -newkey rsa:512 -noenc -noout \\
                -keyout <encryption_key_path> -batch 2>/dev/null

        Then writes the resulting PEM to every other GW node at the same path
        with mode 0600.

        Only called when ``self.tls_psk`` is True.  Safe to call multiple times
        (idempotent — each call regenerates a fresh key).
        """
        key_path = self.config.get("encryption_key_path", "/root/encryption.key")
        primary_node = self.gw_nodes[0]

        LOG.info(
            "Generating TLS-PSK RSA encryption key on %s at %s",
            primary_node.hostname,
            key_path,
        )
        primary_node.exec_command(
            cmd=(
                f"openssl req -newkey rsa:512 -noenc -noout "
                f"-keyout {key_path} -batch 2>/dev/null"
            ),
            sudo=True,
        )
        out, _ = primary_node.exec_command(cmd=f"cat {key_path}", sudo=True)
        key_pem = out.strip()
        if "PRIVATE KEY" not in key_pem:
            raise RuntimeError(
                f"TLS-PSK key generation failed on {primary_node.hostname}: "
                f"unexpected content: {key_pem[:80]!r}"
            )

        # Distribute to all remaining GW nodes
        for node in self.gw_nodes[1:]:
            LOG.info(
                "Distributing TLS-PSK encryption key to %s at %s",
                node.hostname,
                key_path,
            )
            node.exec_command(
                cmd=(
                    f"cat > {key_path} << 'TLSPSK_EOF'\n"
                    f"{key_pem}\n"
                    f"TLSPSK_EOF\n"
                    f"chmod 600 {key_path}"
                ),
                sudo=True,
            )

        LOG.info(
            "TLS-PSK encryption key ready on %d GW node(s) at %s",
            len(self.gw_nodes),
            key_path,
        )
        return key_pem

    def deploy(self):
        """
        Deploy NVMe gateways using orchestrator, then fetch and update daemon and service names for each gateway node.

        When ``tls_psk`` is True, generates an RSA encryption key on every GW
        node **before** applying the orch spec so that the key file exists at
        ``encryption_key_path`` when cephadm reads it.
        """
        # Open up firewall ports if running.
        setup_firewalld(self.gw_nodes)
        # Enable ceph mgr module enable nvmeof if not enabled
        check_and_enable_nvmeof_module(
            ceph_cluster=self.ceph_cluster, ceph_version=self.ceph_version
        )

        # Generate + distribute the RSA encryption key to all GW nodes BEFORE
        # apply_spec so the file exists at encryption_key_path when cephadm reads it.
        if self.tls_psk:
            self._generate_and_distribute_encryption_key()

        deploy_config = self._create_spec_deployment_config()
        if deploy_config:
            test_nvmeof.run(self.ceph_cluster, **deploy_config)

        self._discover_service_name()

    def redeploy(self, wait_sec=30):
        """Redeploy the NVMe-oF orchestrator service after spec apply.

        When gateways are already initialised, also waits until each gateway
        reports ``gateway_initialization_over``. Subsystem CLI calls issued
        during the redeploy fail with "is going down" or connection refused.
        """
        if not self.service_name:
            raise RuntimeError("NVMe-oF service name not set; deploy the service first")
        orch = Orch(self.ceph_cluster, **{})
        cmd = f"ceph orch redeploy {self.service_name}"
        LOG.info("Redeploying NVMe-oF service: %s", cmd)
        orch.shell(args=[cmd])
        if wait_sec:
            time.sleep(wait_sec)
        # orch "running" is not enough for subsystem CLI calls. A redeploy
        # returns "gateway is going down" / connection refused until
        # gateway_initialization_over is true.
        if getattr(self, "gateways", None):
            self.wait_for_gateways_ready()

    def wait_for_gateways_ready(self, timeout=300, interval=10):
        """Poll each gateway until gateway_initialization_over is True.

        Must be called after init_gateways() so that self.gateways is populated.

        Args:
            timeout (int): Maximum seconds to wait per gateway (default 300).
            interval (int): Poll interval in seconds (default 10).

        Raises:
            RuntimeError: If any gateway does not become ready within timeout.
        """
        if not getattr(self, "gateways", None):
            raise RuntimeError("Gateways not initialised; call init_gateways() first")

        for gw in self.gateways:
            hostname = gw.node.hostname
            deadline = time.time() + timeout
            LOG.info(
                "Waiting for gateway %s to be ready (timeout=%ds)", hostname, timeout
            )
            while time.time() < deadline:
                try:
                    out, _ = gw.gateway.info(**{"base_cmd_args": {"format": "json"}})
                    info = json.loads(out)
                    if info.get("gateway_initialization_over"):
                        LOG.info("Gateway %s is ready", hostname)
                        break
                except Exception as exc:
                    LOG.debug("gateway info on %s raised %s — retrying", hostname, exc)
                time.sleep(interval)
            else:
                raise RuntimeError(
                    f"Gateway {hostname} did not become ready within {timeout}s"
                )

    def register_dashboard_nvmeof_gateways(self):
        """Register running NVMe-oF daemons with dashboard so ``ceph nvmeof`` works.

        Bootstrap with skip-dashboard leaves ``ceph dashboard nvmeof-gateway-list``
        empty; the CLI then fails with "Gateway group does not exist".
        """
        orch = Orch(self.ceph_cluster, **{})
        out, _ = orch.shell(args=["ceph", "dashboard", "nvmeof-gateway-list"])
        try:
            listed = json.loads(out)
        except (TypeError, json.JSONDecodeError):
            listed = {}
        if listed.get("gateways"):
            LOG.info("Dashboard already has NVMe-oF gateways registered")
            return

        out, _ = orch.shell(
            args=["ceph", "orch", "ps", "--daemon_type", "nvmeof", "--format", "json"]
        )
        daemons = json.loads(out)
        installer = getattr(orch.installer, "node", orch.installer)
        port = getattr(self, "port", DEFAULT_PORT)
        url_file = "/tmp/nvmeof-dashboard-gw.url"

        for daemon in daemons:
            service_name = daemon.get("service_name") or ""
            daemon_name = daemon.get("daemon_name") or daemon.get("daemon_id") or ""
            hostname = daemon.get("hostname") or ""
            if (
                self.group
                and self.group not in service_name
                and self.group not in daemon_name
            ):
                continue
            node = None
            for gw in self.gw_nodes:
                names = {
                    gw.hostname,
                    getattr(gw, "shortname", ""),
                    getattr(gw, "id", ""),
                }
                if hostname in names or any(n and n in hostname for n in names if n):
                    node = gw
                    break
            if not node:
                LOG.warning(
                    "No gateway node match to register with dashboard: %s", hostname
                )
                continue
            url = f"{node.ip_address}:{port}"
            installer.exec_command(
                sudo=True, cmd=f"printf '%s\\n' '{url}' > {url_file}"
            )
            # Dashboard looks up orch.services.get(name); name must be the
            # nvmeof service name (e.g. nvmeof.nvmeof.gw_group1), not hostname.
            dash_name = service_name or hostname
            args = ["ceph", "dashboard", "nvmeof-gateway-add", dash_name]
            if self.group:
                args.append(self.group)
            if daemon_name:
                args.append(daemon_name)
            args.extend(["-i", url_file])
            LOG.info(
                "Registering dashboard NVMe-oF gateway %s (%s) -> %s",
                dash_name,
                hostname,
                url,
            )
            orch.shell(args=args, base_cmd_args={"mount": "/tmp:/tmp"})

        out, _ = orch.shell(args=["ceph", "dashboard", "nvmeof-gateway-list"])
        LOG.info("Dashboard NVMe-oF gateway list: %s", out)

    def init_gateways(self, timeout=300, interval=10):
        """
        Initialize NVMeGateway objects for each ceph_node in the group.

        Retries create_gateway() until the gateway responds (handles the window
        where the daemon is still starting after a redeploy).

        Args:
            timeout (int): Max seconds to wait per node (default 300).
            interval (int): Retry interval in seconds (default 10).
        """
        self.register_dashboard_nvmeof_gateways()
        self.gateways = []
        port = getattr(self, "port", DEFAULT_PORT)
        ceph = Orch(self.ceph_cluster, **{})
        version = nvme_gw_cli_version_adapter(
            self.ceph_cluster, override=self.nvmeof_cli_version
        )

        for node in self.gw_nodes:
            deadline = time.time() + timeout
            LOG.info("Initialising gateway %s (timeout=%ds)", node.hostname, timeout)
            while True:
                try:
                    gw = create_gateway(
                        version,
                        node,
                        mtls=self.mtls,
                        shell=getattr(ceph, "shell"),
                        port=port,
                        gw_group=self.group,
                    )
                    self._sync_gateway_identity(gw)
                    self.gateways.append(gw)
                    LOG.info("Gateway %s initialised", node.hostname)
                    break
                except Exception as exc:
                    if time.time() >= deadline:
                        raise RuntimeError(
                            f"Gateway {node.hostname} did not become reachable "
                            f"within {timeout}s"
                        ) from exc
                    LOG.debug(
                        "Gateway %s not ready yet (%s) — retrying in %ds",
                        node.hostname,
                        exc,
                        interval,
                    )
                    time.sleep(interval)
        self._discover_service_name()

    def _sync_gateway_identity(self, gw):
        """Overlay orch / nvme-gw identity when dashboard returns the first GW in the group."""
        orch = Orch(self.ceph_cluster, **{})
        out, _ = orch.shell(
            args=["ceph", "orch", "ps", "--daemon_type", "nvmeof", "--format", "json"]
        )
        daemons = json.loads(out) if isinstance(out, str) else out
        if isinstance(daemons, str):
            start, end = daemons.find("["), daemons.rfind("]")
            daemons = json.loads(daemons[start : end + 1]) if start != -1 else []
        hostname = gw.node.hostname
        for daemon in daemons:
            dhost = daemon.get("hostname") or ""
            if hostname == dhost or hostname in dhost or dhost in hostname:
                daemon_name = daemon.get("daemon_name")
                if daemon_name:
                    gw.daemon_name = daemon_name
                    gw.gw_id = (
                        daemon_name
                        if str(daemon_name).startswith("client.")
                        else f"client.{daemon_name}"
                    )
                break
        try:
            from cli.ceph.nvme_gw import NvmeGw

            nvme_gw = NvmeGw(orch.installer, "ceph")
            show = nvme_gw.show(self.nvme_metadata_pool, self.group, format="json")
            text = show if isinstance(show, str) else str(show)
            start, end = text.find("{"), text.rfind("}")
            data = json.loads(text[start : end + 1]) if start != -1 else {}
            for item in data.get("Created Gateways:", data.get("gateways", [])):
                gid = item.get("gw-id") or ""
                if gw.gw_id == gid or hostname in gid:
                    if item.get("anagrp-id") is not None:
                        gw.ana_group_id = item["anagrp-id"]
                    break
        except Exception as exc:
            LOG.debug("Could not sync ANA group from nvme-gw show: %s", exc)
