"""Validate NFS-Ganesha TSM configuration, listen port, and peer connectivity."""

import json

from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from tests.nfs.lib.common_lib import (
    assert_port_listening,
    get_nfs_container_id,
    read_ganesha_conf,
    scrape_nfs_logs,
)
from tests.nfs.lib.tsm.constants import (
    TSM_DISABLE_ABSENT,
    TSM_DISABLE_PEER_PRESENT,
    TSM_DISABLE_PRESENT,
    TSM_FIRST_BOOT_PRESENT,
    TSM_PRIMARY_SELECTION_FAIL_ABSENT,
    TSM_PRIMARY_SELECTION_FAIL_PRESENT,
)
from utility.log import Log

log = Log(__name__)


class NfsTsmValidation:
    """Post-deploy TSM checks against orch daemons and ganesha.conf."""

    def __init__(
        self,
        ceph_cluster,
        client,
        enable_tsm_key,
        tsm_port_key,
        tsm_port,
        timeout=30,
        poll_interval=5,
    ):
        self.ceph_cluster = ceph_cluster
        self.client = client
        self.installer = ceph_cluster.get_nodes("installer")[0]
        self.enable_tsm_key = enable_tsm_key
        self.tsm_port_key = tsm_port_key
        self.tsm_port = tsm_port
        self.timeout = timeout
        self.poll_interval = poll_interval

    def get_cluster_info(self, nfs_name):
        """Return cluster info dict, or None on failure."""
        info = Ceph(self.client).nfs.cluster.info(nfs_name)
        if not info or nfs_name not in info:
            log.error("Cluster %r not found in nfs cluster info: %r", nfs_name, info)
            return None
        log.info("NFS cluster info for %s:\n%s", nfs_name, json.dumps(info, indent=2))
        return info[nfs_name]

    def assert_ganesha_tsm_conf(
        self,
        nfs_name,
        nfs_nodes,
        expect_enabled=True,
        expected_tsm_port=None,
        per_node_port=False,
    ):
        """Verify enable_TSM / Tsm_Port in ganesha.conf on each NFS node.

        ``expect_enabled``:
          True  — keys present with enable_TSM=true and matching Tsm_Port
          False — keys present with enable_TSM=false and matching Tsm_Port
          None  — both TSM keys must be absent from NFS_CORE_PARAM

        When ``per_node_port`` is True (or ``expected_tsm_port`` is None and
        multiple daemons may use distinct ports), each node's ``Tsm_Port`` is
        taken from that node's ganesha.conf instead of a single global value.

        Returns list of per-node results on success, or None on failure.
        """
        log.info("GANESHA.CONF TSM VALIDATION")
        conf_by_host = read_ganesha_conf(nfs_nodes, nfs_name=nfs_name)
        if conf_by_host is None:
            return None
        use_per_node = per_node_port or (
            expected_tsm_port is None and len(nfs_nodes) > 1
        )
        global_expected = str(
            self.tsm_port if expected_tsm_port is None else expected_tsm_port
        )
        results = []

        for node in nfs_nodes:
            conf = conf_by_host.get(node.hostname)
            if not conf:
                log.error("No ganesha.conf for %s", node.hostname)
                return None

            core = conf.get("NFS_CORE_PARAM", {})
            has_enabled = self.enable_tsm_key in core
            has_port = self.tsm_port_key in core
            enabled = str(core.get(self.enable_tsm_key, "")).lower()
            tsm_port = str(core.get(self.tsm_port_key, ""))
            peers = [
                p.strip()
                for p in conf.get("CEPH_NODES_LIST", {})
                .get("Ceph_Nodes", "")
                .split(",")
                if p.strip()
            ]
            log.info(
                "%s: %s=%s %s=%s peers=%s",
                node.hostname,
                self.enable_tsm_key,
                enabled if has_enabled else "<absent>",
                self.tsm_port_key,
                tsm_port if has_port else "<absent>",
                peers,
            )

            if expect_enabled is None:
                if has_enabled or has_port:
                    log.error(
                        "%s: expected %s/%s absent, got %s=%r %s=%r",
                        node.hostname,
                        self.enable_tsm_key,
                        self.tsm_port_key,
                        self.enable_tsm_key,
                        enabled,
                        self.tsm_port_key,
                        tsm_port,
                    )
                    return None
            else:
                expected_enabled = "true" if expect_enabled else "false"
                if enabled != expected_enabled:
                    log.error(
                        "%s: expected %s=%s, got %r",
                        node.hostname,
                        self.enable_tsm_key,
                        expected_enabled,
                        enabled,
                    )
                    return None
                if not has_port or not tsm_port.isdigit():
                    log.error(
                        "%s: missing or invalid %s=%r",
                        node.hostname,
                        self.tsm_port_key,
                        tsm_port,
                    )
                    return None
                if not use_per_node and tsm_port != global_expected:
                    log.error(
                        "%s: expected %s=%s, got %r",
                        node.hostname,
                        self.tsm_port_key,
                        global_expected,
                        tsm_port,
                    )
                    return None

            results.append(
                {
                    "hostname": node.hostname,
                    "node": node,
                    "enabled": enabled == "true",
                    "tsm_port": tsm_port,
                    "peers": peers,
                    "conf": conf,
                }
            )
        return results

    def assert_tsm_peer_connections(
        self, nodes, tsm_port=None, conf=None, timeout=None, min_peers=1
    ):
        """Wait until each node has ESTAB TCP sessions to peer IPs on TSM port.

        When ``conf`` is a per-node result list from ``assert_ganesha_tsm_conf``,
        each node is checked for ESTAB to peer IPs (multi-daemon may use distinct
        local TSM ports).

        Returns 0 on success, 1 on failure.
        """
        timeout = self.timeout if timeout is None else timeout
        port_by_host = {}
        if conf:
            port_by_host = {e["hostname"]: int(e["tsm_port"]) for e in conf}
        default_port = int(tsm_port if tsm_port is not None else self.tsm_port)
        ips = {node.hostname: node.ip_address for node in nodes}

        for node in nodes:
            local_port = port_by_host.get(node.hostname, default_port)
            peers = [ip for host, ip in ips.items() if host != node.hostname and ip]
            if len(peers) < min_peers:
                log.error(
                    "%s: need >= %s peers, got %s", node.hostname, min_peers, peers
                )
                return 1

            for _ in WaitUntil(timeout=timeout, interval=self.poll_interval):
                out, _ = node.exec_command(
                    sudo=True,
                    cmd="ss -tn state established",
                    check_ec=False,
                )
                text = out or ""
                if conf:
                    ok = sum(ip in text for ip in peers) >= min_peers
                else:
                    ok = sum(ip in text for ip in peers) >= min_peers and (
                        f":{local_port}" in text
                    )
                if ok:
                    log.info(
                        "%s: TSM ESTAB peers ok (port=%s) -> %s",
                        node.hostname,
                        local_port,
                        peers,
                    )
                    break
            else:
                log.error(
                    "%s: timed out waiting for TSM ESTAB to %s (local port %s)",
                    node.hostname,
                    peers,
                    local_port,
                )
                return 1
        return 0

    def _assert_boot_path(self, node, nfs_name, present, absent, since, label):
        """Require present markers and forbid absent markers. Returns 0 or 1.

        ``present`` as a tuple/list of plain strings: each must match (INFO-level
        first-boot). A single OR-regex string: any match is enough (recovery).
        ``absent`` is typically one OR-regex; any hit fails the path.
        """
        if isinstance(present, (tuple, list)):
            for pat in present:
                if (
                    scrape_nfs_logs(
                        [node],
                        pat,
                        nfs_name=nfs_name,
                        require_match=True,
                        since=since,
                    )
                    is None
                ):
                    return 1
        else:
            present_hits = scrape_nfs_logs(
                [node], present, nfs_name=nfs_name, since=since
            )
            if present_hits is None or not present_hits.get(node.hostname):
                return 1

        if isinstance(absent, (tuple, list)):
            for pat in absent:
                if (
                    scrape_nfs_logs(
                        [node],
                        pat,
                        nfs_name=nfs_name,
                        require_absent=True,
                        since=since,
                    )
                    is None
                ):
                    return 1
        else:
            absent_hits = scrape_nfs_logs(
                [node], absent, nfs_name=nfs_name, since=since
            )
            if absent_hits is None or absent_hits.get(node.hostname):
                return 1

        log.info("%s: TSM %s path ok", node.hostname, label)
        return 0

    def assert_tsm_disable_logs(self, nfs_name, node, peers=None, since=None):
        """Return 0 if disable markers match on recovering node (and peers if set)."""
        log.info("TSM DISABLE-PATH LOG VALIDATION (%s on %s)", nfs_name, node.hostname)
        if self._assert_boot_path(
            node, nfs_name, TSM_DISABLE_PRESENT, TSM_DISABLE_ABSENT, since, "disable"
        ):
            return 1
        if peers and TSM_DISABLE_PEER_PRESENT:
            peer_list = peers if isinstance(peers, (list, tuple)) else [peers]
            for pat in TSM_DISABLE_PEER_PRESENT:
                if (
                    scrape_nfs_logs(
                        peer_list,
                        pat,
                        nfs_name=nfs_name,
                        require_match=True,
                        since=since,
                    )
                    is None
                ):
                    return 1
        return 0

    def assert_tsm_primary_selection_fail_logs(self, nfs_name, node, since=None):
        """Return 0 if all-peers-down disable markers match (no partial primary)."""
        log.info(
            "TSM PRIMARY-SELECTION-FAIL LOG VALIDATION (%s on %s)",
            nfs_name,
            node.hostname,
        )
        return self._assert_boot_path(
            node,
            nfs_name,
            TSM_PRIMARY_SELECTION_FAIL_PRESENT,
            TSM_PRIMARY_SELECTION_FAIL_ABSENT,
            since,
            "primary-selection-fail",
        )

    def assert_tsm_boot_logs(self, nfs_name, nfs_nodes, since=None):
        """All NFS nodes must show first-boot INFO markers after initial bring-up."""
        log.info("TSM BOOT-PATH LOG VALIDATION (%s)", nfs_name)
        for node in nfs_nodes:
            if self._assert_boot_path(
                node, nfs_name, TSM_FIRST_BOOT_PRESENT, (), since, "first-boot"
            ):
                log.error("%s: missing first-boot TSM logs", node.hostname)
                return 1
        log.info("TSM boot roles: all first_boot=%s", [n.hostname for n in nfs_nodes])
        return 0

    def assert_tsm_ready(
        self,
        nfs_name,
        nfs_nodes,
        tsm_port=None,
        expect_enabled=True,
        timeout=None,
        check_peers=True,
        log_patterns=None,
        log_since=None,
        check_boot_logs=True,
    ):
        """Composite ready check: containers → conf → port → boot logs → peers.

        Polls until NFS containers are up on the given nodes (skips hosts
        without a container yet), then validates TSM conf, listen port,
        first-boot logs, and optional peer ESTAB.

        Returns 0 on success, 1 on failure.
        """
        log.info("ASSERT TSM READY (%s)", nfs_name)
        port = self.tsm_port if tsm_port is None else tsm_port
        per_node = len(nfs_nodes) > 1
        timeout = self.timeout if timeout is None else timeout
        conf = None
        active = []
        for _ in WaitUntil(timeout=timeout, interval=self.poll_interval):
            active = [
                n for n in nfs_nodes if get_nfs_container_id(n, nfs_name=nfs_name)
            ]
            if not active:
                log.warning("NFS containers not up yet for %s", nfs_name)
                continue
            conf = self.assert_ganesha_tsm_conf(
                nfs_name,
                active,
                expect_enabled=expect_enabled,
                expected_tsm_port=port if not per_node else None,
                per_node_port=per_node,
            )
            if conf is None:
                log.warning("TSM conf not ready yet")
                continue
            if expect_enabled:
                port_ok = 0
                for entry in conf:
                    if assert_port_listening(
                        [entry["node"]], int(entry["tsm_port"]), label="TSM port"
                    ):
                        port_ok = 1
                        break
                if port_ok:
                    log.warning("TSM port not listening yet")
                    continue
                if check_boot_logs:
                    if self.assert_tsm_boot_logs(nfs_name, active, since=log_since):
                        log.warning("TSM boot logs not ready yet")
                        continue
                elif log_patterns:
                    if (
                        scrape_nfs_logs(
                            active,
                            log_patterns,
                            nfs_name=nfs_name,
                            require_match=True,
                            since=log_since,
                        )
                        is None
                    ):
                        log.warning("TSM log patterns not ready yet")
                        continue
            break
        else:
            log.error(
                "TSM ready check did not succeed within %ss for nfs.%s",
                timeout,
                nfs_name,
            )
            return 1

        if expect_enabled and check_peers and conf and len(conf) >= 2:
            if self.assert_tsm_peer_connections(
                [e["node"] for e in conf], tsm_port=port, conf=conf
            ):
                return 1
        return 0
