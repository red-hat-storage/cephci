"""Deploy NFS Multi-Active services via orchestrator spec."""

import json
from time import sleep

import yaml

from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.lib.multi_active.constants import log_section
from utility.log import Log

log = Log(__name__)
from tests.nfs.lib.multi_active.validation import NfsMultiActiveValidation
from tests.nfs.nfs_operations import create_nfs_via_file_and_verify


class NfsMultiActiveDeploy:
    """Deploy NFS Multi-Active services via orchestrator spec."""

    def __init__(self, ceph_cluster, nfs_name, vip, client=None):
        self.ceph_cluster = ceph_cluster
        self.nfs_name = nfs_name
        self.vip = vip
        self.installer = ceph_cluster.get_nodes("installer")[0]
        if client is None:
            clients = ceph_cluster.get_nodes("client")
            if not clients:
                raise ConfigError(
                    "At least one client node is required to enable NFS mgr module"
                )
            client = clients[0]
        self.client = client
        self.validator = NfsMultiActiveValidation(ceph_cluster, self.client)

    def deploy(
        self,
        specs,
        nfs_nodes,
        deploy_timeout=300,
        expected_ingress_hosts=None,
        ingress_wait_targets=None,
        cluster_validations=None,
        colocation_validation=False,
        skip_mgr_enable=False,
        ingress_poll_interval=5,
    ):
        log_section(log, "DEPLOY NFS MULTI-ACTIVE VIA SPEC")
        wait_targets = ingress_wait_targets or [(self.nfs_name, self.vip)]
        log.info(
            "Ingress VIP(s): %s",
            ", ".join(f"{name}={vip}" for name, vip in wait_targets),
        )

        if skip_mgr_enable:
            log.info("Skipping NFS mgr module enable (already initialized this suite)")
        else:
            Ceph(self.client).mgr.module.enable(module="nfs", force=True)
            sleep(3)

        spec_yaml = yaml.dump_all(
            specs, sort_keys=False, default_flow_style=False, indent=2
        )
        log_section(log, "ORCHESTRATOR SPEC TO APPLY")
        log.info("%s", spec_yaml)

        if not create_nfs_via_file_and_verify(
            self.installer,
            specs,
            deploy_timeout,
            nfs_nodes=nfs_nodes,
            cluster_nodes=self.ceph_cluster.node_list,
        ):
            raise OperationFailedError(
                "Failed to deploy NFS Multi-Active services via spec"
            )

        self._wait_all_ingress(
            wait_targets, deploy_timeout, interval=ingress_poll_interval
        )

        if cluster_validations:
            for nfs_spec, ingress_spec, exp_ingress_hosts in cluster_validations:
                nfs_name = nfs_spec["service_id"]
                cluster_info = self.validator.get_cluster_info(nfs_name)
                if colocation_validation:
                    self.validator.assert_colocation_cluster_info(
                        cluster_info, nfs_spec, ingress_spec
                    )
                    self.validator.assert_colocation_ports_listening(
                        cluster_info, nfs_spec
                    )
                else:
                    self.validator.assert_cluster_info(
                        cluster_info, nfs_spec, ingress_spec
                    )
                self.validator.assert_orch_ps(
                    nfs_spec,
                    ingress_spec,
                    expected_ingress_hosts=exp_ingress_hosts,
                    timeout=deploy_timeout,
                )
        else:
            nfs_spec = self._find_spec(specs, "nfs")
            ingress_spec = self._find_spec(specs, "ingress")
            cluster_info = self.validator.get_cluster_info(self.nfs_name)
            if colocation_validation:
                self.validator.assert_colocation_cluster_info(
                    cluster_info, nfs_spec, ingress_spec
                )
                self.validator.assert_colocation_ports_listening(cluster_info, nfs_spec)
            else:
                self.validator.assert_cluster_info(cluster_info, nfs_spec, ingress_spec)
            self.validator.assert_orch_ps(
                nfs_spec,
                ingress_spec,
                expected_ingress_hosts=expected_ingress_hosts,
                timeout=deploy_timeout,
            )

        log.info("NFS Multi-Active services deployed successfully via spec")

    def reapply_specs(
        self,
        specs,
        nfs_nodes,
        deploy_timeout=300,
        ingress_poll_interval=5,
        timings=None,
        timings_trigger_key=None,
    ):
        """Re-apply orchestrator specs after label swap (no mgr module re-enable)."""
        log_section(log, "RE-APPLY NFS MULTI-ACTIVE SPECS")
        spec_yaml = yaml.dump_all(
            specs, sort_keys=False, default_flow_style=False, indent=2
        )
        log.info("%s", spec_yaml)
        if not create_nfs_via_file_and_verify(
            self.installer,
            specs,
            deploy_timeout,
            nfs_nodes=nfs_nodes,
            cluster_nodes=self.ceph_cluster.node_list,
            timings=timings,
            timings_key=timings_trigger_key,
        ):
            raise OperationFailedError(
                "Failed to re-apply NFS Multi-Active services via spec"
            )
        self._wait_ingress(deploy_timeout, interval=ingress_poll_interval)
        log.info("NFS Multi-Active specs re-applied successfully")

    def _wait_ingress(self, deploy_timeout, interval=5):
        self._wait_ingress_for(
            self.nfs_name, self.vip, deploy_timeout, interval=interval
        )

    def _wait_all_ingress(self, wait_targets, deploy_timeout, interval=5):
        for nfs_name, vip in wait_targets:
            self._wait_ingress_for(nfs_name, vip, deploy_timeout, interval=interval)

    def _wait_ingress_for(self, nfs_name, vip, deploy_timeout, interval=5):
        service_name = f"ingress.nfs.{nfs_name}"

        for w in WaitUntil(timeout=deploy_timeout, interval=interval):
            services = json.loads(
                CephAdm(self.installer).ceph.orch.ls(
                    format="json", service_type="ingress"
                )
            )
            ingress = [
                svc for svc in services if svc.get("service_name") == service_name
            ]
            if not ingress:
                continue

            status = ingress[0].get("status", {})
            if status.get("running") != status.get("size"):
                continue

            if ingress[0].get("spec", {}).get("virtual_ip") != vip:
                raise OperationFailedError(
                    f"Ingress VIP mismatch for {service_name}: expected {vip!r}, "
                    f"got {ingress[0].get('spec', {}).get('virtual_ip')!r}"
                )
            log.info("Ingress service %s is running with VIP %s", service_name, vip)
            return

        raise OperationFailedError(
            f"Ingress service {service_name} not running within {deploy_timeout}s"
        )

    @staticmethod
    def _find_spec(specs, service_type):
        for spec in specs:
            if spec.get("service_type") == service_type:
                return spec
        raise OperationFailedError(f"No {service_type!r} spec found in {specs!r}")
