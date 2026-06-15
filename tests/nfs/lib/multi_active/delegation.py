"""NFS Multi-Active helpers for NFSv4 delegation tests."""

import json
import time

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from tests.nfs.lib.multi_active.constants import (
    DEFAULT_TC3_DELEGATION_HOLD_READY_SEC,
    DEFAULT_TC3_DELEGATION_HOLD_SEC,
    DEFAULT_TC3_REDEPLOY_WAIT,
    DEFAULT_TC3_SERVICE_WAIT_TIMEOUT,
    EXPORT_NAME,
)
from tests.nfs.nfs_delegation_operations import (
    LOG_TEMPLATE_BACKUP_PATH,
    _expect_export_delegation,
    clamp_delegation_log_settle_seconds,
    enable_ganesha_debug_logging,
    ensure_ceph_conf_and_admin_keyring_on_hosts,
    hold_delegation_open,
    parse_nfs4_open_delegation_types,
    read_ganesha_delegation_tailf_capture,
    redeploy_nfs_clusters,
    restore_ganesha_template,
    start_ganesha_delegation_tailf_follow,
    stop_ganesha_delegation_tailf_follow,
    truncate_ganesha_container_log,
    update_export_delegation,
    validate_delegation_ganesha_window,
    wait_for_delegation_path,
    write_delegation_file,
)
from utility.log import Log

log = Log(__name__)


class NfsMultiActiveDelegation:
    """Bridge multi-active sessions to nfs_delegation_operations helpers."""

    @staticmethod
    def _installer(ceph_cluster):
        installers = ceph_cluster.get_nodes("installer")
        if not installers:
            raise OperationFailedError("Delegation tests require an installer node")
        return installers[0]

    @staticmethod
    def _redeploy_clusters(
        cephadm, cluster_ids, installer, redeploy_wait, service_wait_timeout
    ):
        redeploy_nfs_clusters(
            cephadm,
            cluster_ids,
            installer,
            redeploy_wait,
            service_wait_timeout,
        )

    @staticmethod
    def _export_delegation(
        nfs_cmd_host, nfs_service_id, export_name, mode, *, update=False
    ):
        if update:
            update_export_delegation(nfs_cmd_host, nfs_service_id, export_name, mode)
        _expect_export_delegation(nfs_cmd_host, nfs_service_id, export_name, mode)

    @staticmethod
    def prepare_nfs_hosts(ceph_cluster, nfs_nodes):
        ensure_ceph_conf_and_admin_keyring_on_hosts(
            NfsMultiActiveDelegation._installer(ceph_cluster), nfs_nodes
        )

    @staticmethod
    def enable_ganesha_debug_logging_for_clusters(
        nfs_cmd_host,
        ceph_cluster,
        cluster_ids,
        redeploy_wait=DEFAULT_TC3_REDEPLOY_WAIT,
        service_wait_timeout=DEFAULT_TC3_SERVICE_WAIT_TIMEOUT,
    ):
        """Enable Ganesha FULL_DEBUG logging and redeploy the given NFS clusters."""
        installer = NfsMultiActiveDelegation._installer(ceph_cluster)
        cephadm = CephAdm(installer).ceph
        logging_backup = enable_ganesha_debug_logging(nfs_cmd_host)
        log.info(
            "Enabled Ganesha FULL_DEBUG logging; redeploying %s",
            ", ".join(cluster_ids),
        )
        NfsMultiActiveDelegation._redeploy_clusters(
            cephadm, cluster_ids, installer, redeploy_wait, service_wait_timeout
        )
        return logging_backup, cephadm, installer

    @staticmethod
    def enable_rw_export(nfs_cmd_host, nfs_service_id, export_name=EXPORT_NAME):
        NfsMultiActiveDelegation._export_delegation(
            nfs_cmd_host, nfs_service_id, export_name, "rw", update=True
        )
        log.info(
            "Export %s on %s has delegations=rw",
            export_name,
            nfs_service_id,
        )

    @staticmethod
    def assert_export_delegation(nfs_cmd_host, nfs_service_id, export_name, mode="rw"):
        NfsMultiActiveDelegation._export_delegation(
            nfs_cmd_host, nfs_service_id, export_name, mode
        )

    @staticmethod
    def restore_ganesha_debug_logging(
        nfs_cmd_host,
        cephadm,
        installer,
        cluster_ids,
        logging_template_backup,
        redeploy_wait=DEFAULT_TC3_REDEPLOY_WAIT,
        service_wait_timeout=DEFAULT_TC3_SERVICE_WAIT_TIMEOUT,
    ):
        if not logging_template_backup:
            return
        restore_ganesha_template(
            nfs_cmd_host, logging_template_backup, LOG_TEMPLATE_BACKUP_PATH
        )
        NfsMultiActiveDelegation._redeploy_clusters(
            cephadm, cluster_ids, installer, redeploy_wait, service_wait_timeout
        )

    @staticmethod
    def assert_cluster_services_running(client, nfs_service_id, label):
        """Return when nfs and ingress daemons for *nfs_service_id* are running."""
        for service in (f"nfs.{nfs_service_id}", f"ingress.nfs.{nfs_service_id}"):
            raw, _ = client.exec_command(
                sudo=True,
                cmd=f"ceph orch ps --service_name {service} --format json",
                check_ec=False,
            )
            daemons = json.loads(raw) if raw else []
            running = [d for d in daemons if d.get("status_desc") == "running"]
            if not running:
                raise OperationFailedError(
                    f"{label}: no running daemons for {service} ({daemons!r})"
                )
            log.info(
                "%s: %d running daemon(s) for %s",
                label,
                len(running),
                service,
            )

    @staticmethod
    def hold_rw_delegation_on_mount(
        client,
        mount_point,
        rel_path,
        hold_seconds=DEFAULT_TC3_DELEGATION_HOLD_SEC,
        hold_ready_seconds=DEFAULT_TC3_DELEGATION_HOLD_READY_SEC,
    ):
        """Seed a file on *mount_point* and keep it OPEN to hold an RW delegation."""
        parent = "/".join(rel_path.split("/")[:-1])
        if parent:
            client.exec_command(
                sudo=True,
                cmd=f"mkdir -p {mount_point}/{parent}",
            )
        path = f"{mount_point}/{rel_path}"
        seed = f"hold-seed-{getattr(client, 'hostname', 'client')}\n"
        write_delegation_file(client, path, seed)
        wait_for_delegation_path(client, path, "delegation hold seed")
        write_delegation_file(client, path, "hold-prime\n", append=True)
        hold_delegation_open(client, path, "r+b", int(hold_seconds))
        if hold_ready_seconds > 0:
            time.sleep(int(hold_ready_seconds))
        log.info("RW delegation hold active on %s for %ss", path, hold_seconds)
        return path, seed

    @staticmethod
    def assert_delegation_access_after_failover(client_a, client_b, path, seed_marker):
        """Verify both clients can read a held file via the existing VIP mount."""
        for client, label in ((client_a, "A"), (client_b, "B")):
            out, _ = client.exec_command(
                sudo=True,
                cmd=f"cat {path}",
                check_ec=False,
            )
            if int(getattr(client, "exit_status", 1)) != 0:
                raise OperationFailedError(
                    f"Client {label} cannot read {path} after failover "
                    f"(ec={getattr(client, 'exit_status', '?')})"
                )
            if seed_marker not in (out or ""):
                raise OperationFailedError(
                    f"Client {label} readback missing seed on {path} after failover: "
                    f"{out!r}"
                )
            log.info(
                "Client %s read delegated file after failover (%d bytes)",
                label,
                len(out or ""),
            )

    @staticmethod
    def _resolve_nfs_node(nfs_nodes, hostname):
        for node in nfs_nodes:
            if node.hostname == hostname:
                return node
        raise OperationFailedError(
            f"No NFS node matching backend hostname {hostname!r}"
        )

    @staticmethod
    def _ganesha_container_on_backend(installer, nfs_service_id, nfs_host_node):
        cephadm = CephAdm(installer).ceph
        raw = cephadm.orch.ps(service_name=f"nfs.{nfs_service_id}", format="json")
        daemons = json.loads(raw) if raw else []
        hostname = nfs_host_node.hostname
        for daemon in daemons:
            if daemon.get("hostname") == hostname and daemon.get("container_id"):
                return daemon["container_id"]
        raise OperationFailedError(
            f"No Ganesha container for nfs.{nfs_service_id} on {hostname}"
        )

    @staticmethod
    def _run_rw_delegation_client_io(client, testfile):
        """Trigger RW delegation OPEN paths (nfs_verify_delegation_rw_none_scenarios)."""
        client.exec_command(
            sudo=True,
            cmd=f"bash -c 'echo rw-deleg > {testfile} && sync'",
        )
        client.exec_command(
            sudo=True,
            cmd=f"bash -c 'echo append >> {testfile} && sync'",
        )
        client.exec_command(sudo=True, cmd=f"cat {testfile}")

    @staticmethod
    def assert_rw_delegation_ganesha_logs(
        client,
        installer,
        nfs_service_id,
        nfs_nodes,
        backend_hostname,
        mount_point,
        config,
        tag,
    ):
        """Validate export-level RW delegation via filtered Ganesha log capture."""
        settle_seconds = clamp_delegation_log_settle_seconds(config)
        nfs_node = NfsMultiActiveDelegation._resolve_nfs_node(
            nfs_nodes, backend_hostname
        )
        container_id = NfsMultiActiveDelegation._ganesha_container_on_backend(
            installer, nfs_service_id, nfs_node
        )
        testfile = f"{mount_point}/deleg_ganesha_io_{tag}.txt"

        truncate_ganesha_container_log(nfs_node, container_id)
        start_ganesha_delegation_tailf_follow(nfs_node, container_id)
        try:
            NfsMultiActiveDelegation._run_rw_delegation_client_io(client, testfile)
            log.info(
                "Waiting %ss after IO on %s for delegation lines in ganesha.log",
                settle_seconds,
                tag,
            )
            time.sleep(settle_seconds)
        finally:
            stop_ganesha_delegation_tailf_follow(nfs_node, container_id)

        hits = read_ganesha_delegation_tailf_capture(nfs_node, container_id)
        if not hits:
            raise OperationFailedError(
                f"Empty ganesha.log tail capture for {tag} (export delegations=rw)"
            )
        log.info(
            "Delegation log for %s: %d tail -f capture lines",
            tag,
            len(hits),
        )
        for line in hits[-25:]:
            log.debug("%s", line)

        window_text = "\n".join(hits)
        ok, reason = validate_delegation_ganesha_window("rw", window_text)
        if not ok:
            raise OperationFailedError(
                f"Ganesha log validation failed for {tag} (delegations=rw): {reason}; "
                f"{len(hits)} capture lines"
            )
        log.info(
            "Passed %s export-level RW delegation logs; nfs4_op_open types=%s",
            tag,
            parse_nfs4_open_delegation_types(window_text),
        )
