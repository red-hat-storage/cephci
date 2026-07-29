"""NFS Multi-Active failover via placement label swap."""

import json
from datetime import datetime

from ceph.waiter import WaitUntil
from cli.exceptions import OperationFailedError
from cli.utilities.utils import reboot_node
from tests.nfs.lib.multi_active.config import NfsMultiActiveConfig
from tests.nfs.lib.multi_active.constants import log_section
from tests.nfs.lib.multi_active.validation import NfsMultiActiveValidation
from utility.log import Log

log = Log(__name__)


class NfsMultiActiveFailover:
    """Simulate baremetal-safe failover by updating placement labels."""

    @staticmethod
    def _format_daemon_wait_snapshot(daemons, max_events=3):
        return [
            {
                "name": daemon.get("daemon_name") or daemon.get("name"),
                "hostname": daemon.get("hostname"),
                "status_desc": daemon.get("status_desc"),
                "daemon_type": daemon.get("daemon_type"),
                "events": (daemon.get("events") or [])[-max_events:],
            }
            for daemon in daemons
        ]

    @classmethod
    def _raise_daemon_wait_timeout(
        cls,
        service_name,
        expected_count,
        exclude_host,
        timeout,
        last_daemons,
        running_daemons,
        extra=None,
    ):
        running_hosts = [d.get("hostname") for d in running_daemons]
        snapshot = cls._format_daemon_wait_snapshot(last_daemons)
        log.error(
            "Timed out after %ss waiting for %d daemon(s) on %s (exclude %r); "
            "running=%d on %s\nLast orch ps snapshot:\n%s",
            timeout,
            expected_count,
            service_name,
            exclude_host,
            len(running_daemons),
            running_hosts,
            json.dumps(snapshot, indent=2),
        )
        if extra:
            log.error(extra)
        raise OperationFailedError(
            f"Timed out after {timeout}s waiting for {expected_count} daemon(s) "
            f"on {service_name} (exclude {exclude_host!r}); "
            f"running={len(running_daemons)} on {running_hosts}"
        )

    @staticmethod
    def swap_label(client, remove_host, add_host, label):
        client.exec_command(
            sudo=True,
            cmd=f"ceph orch host label rm {remove_host} {label}",
        )
        client.exec_command(
            sudo=True,
            cmd=f"ceph orch host label add {add_host} {label}",
        )
        log.info(
            "Swapped label %r from %s to %s",
            label,
            remove_host,
            add_host,
        )

    @staticmethod
    def wait_nfs_daemons(
        validator,
        nfs_name,
        expected_count,
        exclude_host=None,
        timeout=300,
        timings=None,
    ):
        """Wait until NFS daemons are running and excluded host is drained."""
        nfs_service = f"nfs.{nfs_name}"
        log_section(log, "WAIT NFS DAEMON FAILOVER")
        last_daemons = []
        for _ in WaitUntil(timeout=timeout, interval=10):
            daemons = validator.get_orch_ps_daemons(nfs_service)
            last_daemons = daemons
            running = NfsMultiActiveValidation.running_daemons(daemons)
            if exclude_host and any(
                daemon.get("hostname") == exclude_host for daemon in running
            ):
                continue
            if len(running) == expected_count:
                hosts = [daemon.get("hostname") for daemon in running]
                log.info(
                    "%d NFS daemon(s) running on %s (excluded %r)",
                    expected_count,
                    hosts,
                    exclude_host,
                )
                if timings is not None:
                    timings["nfs_ready"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                return running
        NfsMultiActiveFailover._raise_daemon_wait_timeout(
            nfs_service,
            expected_count,
            exclude_host,
            timeout,
            last_daemons,
            NfsMultiActiveValidation.running_daemons(last_daemons),
        )

    @staticmethod
    def get_ganesha_host_pid(validator, nfs_name, hostname, nfs_host_node):
        """Return the host PID of ganesha.nfsd for the NFS daemon on *hostname*."""
        nfs_service = f"nfs.{nfs_name}"
        daemons = validator.get_orch_ps_daemons(nfs_service)
        running = NfsMultiActiveValidation.running_daemons(daemons)
        target = next(
            (daemon for daemon in running if daemon.get("hostname") == hostname),
            None,
        )
        if target is None:
            raise OperationFailedError(
                f"No running NFS daemon for {nfs_service} on {hostname!r}"
            )
        cmd = "ps -ef | grep '[g]anesha.nfsd' | grep -v grep | awk '{print $2}'"
        out, err = nfs_host_node.exec_command(cmd=cmd, sudo=True, check_ec=False)
        combined = f"{out or ''}{err or ''}".strip()
        pid = (out or "").strip().split("\n")[0].strip()
        if not pid.isdigit():
            raise OperationFailedError(
                f"Could not resolve ganesha PID on {hostname!r}: {combined or 'no output'}"
            )
        log.info(
            "Ganesha PID on %s (%s): %s",
            hostname,
            target.get("daemon_name"),
            pid,
        )
        return int(pid)

    @staticmethod
    def crash_ganesha_process(nfs_host_node, pid, hostname):
        """Send SIGKILL to the ganesha process on *hostname*."""
        nfs_host_node.exec_command(
            sudo=True,
            cmd=f"kill -9 {pid}",
        )
        log.info("Sent SIGKILL to ganesha PID %s on %s", pid, hostname)

    @staticmethod
    def wait_nfs_daemon_on_host(
        validator, nfs_name, hostname, timeout=300, timings=None
    ):
        """Wait until an NFS daemon is running again on *hostname*."""
        nfs_service = f"nfs.{nfs_name}"
        log_section(log, f"WAIT NFS DAEMON RECOVERY ON {hostname}")
        last_daemons = []
        for _ in WaitUntil(timeout=timeout, interval=10):
            daemons = validator.get_orch_ps_daemons(nfs_service)
            last_daemons = daemons
            running = [
                daemon
                for daemon in NfsMultiActiveValidation.running_daemons(daemons)
                if daemon.get("hostname") == hostname
            ]
            if running:
                log.info(
                    "NFS daemon running on %s: %s",
                    hostname,
                    running[0].get("daemon_name"),
                )
                if timings is not None:
                    timings["nfs_daemon_recovered_at"] = datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                return running[0]
        NfsMultiActiveFailover._raise_daemon_wait_timeout(
            nfs_service,
            1,
            None,
            timeout,
            last_daemons,
            [
                daemon
                for daemon in NfsMultiActiveValidation.running_daemons(last_daemons)
                if daemon.get("hostname") == hostname
            ],
            extra=f"Expected NFS daemon to recover on {hostname!r}",
        )

    @staticmethod
    def _node_by_hostname(ceph_cluster, hostname):
        for node in ceph_cluster.node_list:
            if getattr(node, "hostname", None) == hostname:
                return node
        raise OperationFailedError(f"Node {hostname!r} not found in cluster node list")

    @classmethod
    def get_active_ingress_host(
        cls, ceph_cluster, vip, ingress_hosts, exclude_hosts=None
    ):
        """Return ingress node currently hosting the VIP (keepalived master).

        The VIP is detected by checking ``ip addr`` output on each ingress host.
        """
        vip_addr = vip.split("/")[0].strip()
        exclude_hosts = set(exclude_hosts or [])
        log_section(log, "IDENTIFY ACTIVE INGRESS (VIP HOLDER)")
        for hostname in ingress_hosts:
            if hostname in exclude_hosts:
                log.info("Skipping ingress host %s (excluded)", hostname)
                continue
            node = cls._node_by_hostname(ceph_cluster, hostname)
            out, _ = node.exec_command(
                sudo=True,
                cmd=f"ip -o addr show | grep -F '{vip_addr}'",
                check_ec=False,
            )
            if out and vip_addr in out:
                log.info(
                    "Active ingress host %s holds VIP %s (keepalived)",
                    hostname,
                    vip,
                )
                return hostname
            log.info("VIP %s not present on ingress host %s", vip, hostname)
        raise OperationFailedError(
            f"VIP {vip!r} not found on any ingress host in {ingress_hosts} "
            f"(exclude={exclude_hosts})"
        )

    @classmethod
    def get_standby_ingress_host(
        cls, ceph_cluster, vip, ingress_hosts, exclude_hosts=None
    ):
        """Return ingress pool member that is not the current VIP holder."""
        vip_holder = cls.get_active_ingress_host(
            ceph_cluster, vip, ingress_hosts, exclude_hosts=exclude_hosts
        )
        excluded = set(exclude_hosts or [])
        excluded.add(vip_holder)
        for hostname in ingress_hosts:
            if hostname not in excluded:
                log.info(
                    "Standby ingress host %s (VIP %s held by %s)",
                    hostname,
                    vip,
                    vip_holder,
                )
                return hostname
        raise OperationFailedError(
            f"No standby ingress host in {ingress_hosts} "
            f"(VIP holder={vip_holder!r}, exclude={excluded})"
        )

    @staticmethod
    def wait_ingress_daemons(
        validator,
        nfs_name,
        expected_count,
        exclude_host=None,
        timeout=300,
        timings=None,
    ):
        """Wait until ingress haproxy/keepalived daemons are stable."""
        ingress_service = f"ingress.nfs.{nfs_name}"
        log_section(log, "WAIT INGRESS DAEMON FAILOVER")
        last_daemons = []
        for _ in WaitUntil(timeout=timeout, interval=10):
            daemons = validator.get_orch_ps_daemons(ingress_service)
            last_daemons = daemons
            running = NfsMultiActiveValidation.running_daemons(daemons)
            haproxy = [
                d
                for d in running
                if NfsMultiActiveValidation.daemon_component(d) == "haproxy"
            ]
            keepalived = [
                d
                for d in running
                if NfsMultiActiveValidation.daemon_component(d) == "keepalived"
            ]
            if exclude_host and any(
                d.get("hostname") == exclude_host for d in haproxy + keepalived
            ):
                continue
            if len(haproxy) == expected_count and len(keepalived) == expected_count:
                hosts = [d.get("hostname") for d in haproxy]
                log.info(
                    "%d ingress daemon pair(s) running on %s (excluded %r)",
                    expected_count,
                    hosts,
                    exclude_host,
                )
                if timings is not None:
                    timings["ingress_ready"] = datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                return haproxy
        running = NfsMultiActiveValidation.running_daemons(last_daemons)
        haproxy = [
            d
            for d in running
            if NfsMultiActiveValidation.daemon_component(d) == "haproxy"
        ]
        keepalived = [
            d
            for d in running
            if NfsMultiActiveValidation.daemon_component(d) == "keepalived"
        ]
        NfsMultiActiveFailover._raise_daemon_wait_timeout(
            ingress_service,
            expected_count,
            exclude_host,
            timeout,
            last_daemons,
            running,
            extra=(
                f"Ingress component counts: haproxy={len(haproxy)}, "
                f"keepalived={len(keepalived)} (expected {expected_count} each)"
            ),
        )

    def failover_nfs_by_label(
        self,
        client,
        deploy,
        nfs_spec,
        nfs_nodes,
        remove_host,
        add_host,
        label,
        nfs_count,
        deploy_timeout=300,
        timings=None,
        timings_trigger_key="nfs_failover_triggered_at",
        post_apply_callback=None,
    ):
        """Move NFS backend off remove_host onto add_host via label swap."""
        log_section(log, "NFS MULTI-ACTIVE FAILOVER — nfs")
        log.info(
            "Failing over NFS label %r: %s -> %s",
            label,
            remove_host,
            add_host,
        )
        self.swap_label(client, remove_host, add_host, label)
        if timings is not None and timings_trigger_key:
            timings[timings_trigger_key] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            timings["failover_triggered_at"] = timings[timings_trigger_key]
        if post_apply_callback:
            post_apply_callback()
        return self.wait_nfs_daemons(
            deploy.validator,
            nfs_spec["service_id"],
            nfs_count,
            exclude_host=remove_host,
            timeout=deploy_timeout,
            timings=timings,
        )

    def failover_ingress_by_label(
        self,
        client,
        deploy,
        ingress_spec,
        nfs_service_id,
        nfs_nodes,
        remove_host,
        add_host,
        label,
        expected_ingress_count,
        deploy_timeout=300,
        timings=None,
        timings_trigger_key="ingress_failover_triggered_at",
        post_apply_callback=None,
    ):
        """Move ingress off remove_host onto add_host via label swap."""
        log_section(log, "NFS MULTI-ACTIVE FAILOVER — ingress")
        log.info(
            "Failing over ingress label %r: %s -> %s",
            label,
            remove_host,
            add_host,
        )
        self.swap_label(client, remove_host, add_host, label)
        if timings is not None and timings_trigger_key:
            timings[timings_trigger_key] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            timings["failover_triggered_at"] = timings[timings_trigger_key]
        if post_apply_callback:
            post_apply_callback()
        return self.wait_ingress_daemons(
            deploy.validator,
            nfs_service_id,
            expected_ingress_count,
            exclude_host=remove_host,
            timeout=deploy_timeout,
            timings=timings,
        )

    def failover_nfs_roundtrip(
        self,
        client,
        deploy,
        nfs_spec,
        nfs_nodes,
        original_host,
        spare_host,
        label,
        nfs_count,
        deploy_timeout=300,
        timings=None,
        post_apply_callback=None,
    ):
        """Label-drain NFS from original_host to spare and back."""
        log_section(log, "NFS MULTI-ACTIVE FAILOVER — nfs roundtrip")
        self.failover_nfs_by_label(
            client,
            deploy,
            nfs_spec,
            nfs_nodes,
            original_host,
            spare_host,
            label,
            nfs_count,
            deploy_timeout=deploy_timeout,
            timings=timings,
            timings_trigger_key="nfs_failover_out_triggered_at",
            post_apply_callback=post_apply_callback,
        )
        return self.failover_nfs_by_label(
            client,
            deploy,
            nfs_spec,
            nfs_nodes,
            spare_host,
            original_host,
            label,
            nfs_count,
            deploy_timeout=deploy_timeout,
            timings=timings,
            timings_trigger_key="nfs_failover_return_triggered_at",
            post_apply_callback=post_apply_callback,
        )

    def failover_ingress_roundtrip(
        self,
        client,
        deploy,
        ingress_spec,
        nfs_service_id,
        nfs_nodes,
        original_host,
        spare_host,
        label,
        expected_ingress_count,
        deploy_timeout=300,
        timings=None,
        post_apply_callback=None,
    ):
        """Label-drain ingress from original_host to spare and back."""
        log_section(log, "NFS MULTI-ACTIVE FAILOVER — ingress roundtrip")
        self.failover_ingress_by_label(
            client,
            deploy,
            ingress_spec,
            nfs_service_id,
            nfs_nodes,
            original_host,
            spare_host,
            label,
            expected_ingress_count,
            deploy_timeout=deploy_timeout,
            timings=timings,
            timings_trigger_key="ingress_failover_out_triggered_at",
            post_apply_callback=post_apply_callback,
        )
        return self.failover_ingress_by_label(
            client,
            deploy,
            ingress_spec,
            nfs_service_id,
            nfs_nodes,
            spare_host,
            original_host,
            label,
            expected_ingress_count,
            deploy_timeout=deploy_timeout,
            timings=timings,
            timings_trigger_key="ingress_failover_return_triggered_at",
            post_apply_callback=post_apply_callback,
        )

    def reboot_and_wait_nfs(
        self,
        ceph_cluster,
        validator,
        nfs_name,
        hostname,
        nfs_count,
        deploy_timeout=300,
        timings=None,
    ):
        """Reboot an NFS backend node and wait for daemons to recover."""
        log_section(log, f"NFS MULTI-ACTIVE FAILOVER — reboot nfs backend {hostname}")
        node = self._node_by_hostname(ceph_cluster, hostname)
        if not reboot_node(node):
            raise OperationFailedError(
                f"Node {hostname!r} did not reconnect after reboot"
            )
        return self.wait_nfs_daemons(
            validator,
            nfs_name,
            nfs_count,
            exclude_host=None,
            timeout=deploy_timeout,
            timings=timings,
        )

    def reboot_and_wait_ingress(
        self,
        ceph_cluster,
        validator,
        nfs_name,
        hostname,
        vip,
        ingress_hosts,
        expected_ingress_count,
        deploy_timeout=300,
        timings=None,
    ):
        """Reboot an ingress node and wait for VIP + ingress daemons to recover."""
        log_section(log, f"NFS MULTI-ACTIVE FAILOVER — reboot ingress {hostname}")
        node = self._node_by_hostname(ceph_cluster, hostname)
        if not reboot_node(node):
            raise OperationFailedError(
                f"Node {hostname!r} did not reconnect after reboot"
            )
        self.get_active_ingress_host(
            ceph_cluster, vip, ingress_hosts, exclude_hosts=set()
        )
        return self.wait_ingress_daemons(
            validator,
            nfs_name,
            expected_ingress_count,
            exclude_host=None,
            timeout=deploy_timeout,
            timings=timings,
        )

    def restore_baseline_placement(
        self,
        client,
        deploy,
        nfs_nodes,
        nfs_spec,
        ingress_spec,
        nfs_service_id,
        placement,
        labels,
        spare_host,
        nfs_count,
        ingress_hosts,
        deploy_nodes,
        deploy_timeout=300,
        timings=None,
    ):
        """Reset placement labels after a label-drain phase."""
        log_section(log, "RESTORE BASELINE PLACEMENT")
        NfsMultiActiveConfig.apply_placement_labels(
            client,
            nfs_nodes,
            placement["nfs"],
            placement["ingress"],
            labels,
        )
        for spare_label in labels.values():
            client.exec_command(
                sudo=True,
                cmd=f"ceph orch host label rm {spare_host} {spare_label}",
                check_ec=False,
            )
        self.wait_nfs_daemons(
            deploy.validator,
            nfs_service_id,
            nfs_count,
            exclude_host=None,
            timeout=deploy_timeout,
            timings=timings,
        )
        self.wait_ingress_daemons(
            deploy.validator,
            nfs_service_id,
            len(ingress_hosts),
            exclude_host=None,
            timeout=deploy_timeout,
            timings=timings,
        )
        log.info("Baseline placement restored for %s", nfs_service_id)
