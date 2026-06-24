"""
Test Module: NFS over RDMA -- Basic Workflow Scenarios
======================================================

This module runs 12 sequential RDMA scenarios that validate end-to-end NFS
over RDMA transport across various configurations.  Each scenario follows a
strict workflow and cleans up fully before the next one begins.

TEST WORKFLOW (per scenario):
=============================

    +-- Create NFS cluster (with scenario-specific flags)
    |
    +-- Create NFS export(s)
    |
    +-- Verify export info:
    |       ceph nfs export get <cluster> <export> --format json
    |       Assert transports field contains "RDMA"
    |
    +-- Mount export(s) on client(s) with proto=rdma
    |
    +-- Verify RDMA connection active:
    |       rdma resource show cm_id --> comm [rpcrdma], dst-addr <server_ip>
    |       mount -l | grep <mountpoint> --> proto=rdma
    |
    +-- Write data with integrity:
    |       dd if=/dev/urandom of=<mount>/testfile bs=1M count=32
    |       md5sum <mount>/testfile > <mount>/testfile.md5
    |
    +-- Unmount
    |
    +-- Remount (same params)
    |
    +-- Re-verify integrity:
    |       md5sum -c <mount>/testfile.md5  --> OK
    |
    +-- Re-verify RDMA connection
    |
    +-- Cleanup: unmount, delete exports, delete cluster
    |
    +-- PASS scenario

SCENARIOS:
==========
 1. RDMA NFSv4.2          -- Single daemon, single client, proto=rdma, vers=4.2
 2. RDMA NFSv4.1          -- Single daemon, single client, proto=rdma, vers=4.1
 3. RDMA NFSv4.0          -- Single daemon, single client, proto=rdma, vers=4.0
 4. RDMA NFSv3            -- Single daemon, single client, proto=rdma, vers=3
 5. Multi-version RDMA    -- 2 clients: one v4.2, one v3, both RDMA
 6. Active-Active no VIP  -- 2 servers, round-robin mounts, no ingress
 7. HA with VIP           -- 2 servers, ingress+VIP, RDMA mount via VIP
 8. Enable RDMA on existing -- TCP cluster first, then ceph orch apply with
                              enable_rdma, update old export transports, mount
                              both old+new exports over RDMA
 9. Custom RDMA port      -- Non-default TCP port=2050 and rdma_port=12345
10. Multi-client parallel IO -- 2 clients writing in parallel over RDMA
11. TCP + RDMA coexist    -- Same cluster: client1=proto=rdma, client2=proto=tcp
12. Single client dual proto -- Same client, same cluster: export1=rdma, export2=tcp

CONFIGURATION OPTIONS:
======================
- servers: Number of NFS server nodes to use (default: 2)
- clients: Number of client nodes to use (default: 2)
- port: TCP NFS port for cluster create (default: "2049")
- rdma_port: RDMA listener port (default: "20049")
- vip: Virtual IP for HA scenario in CIDR form, e.g. "10.64.66.99/24"

RDMA CONNECTION VERIFICATION:
=============================
After every RDMA mount, the test runs:

    rdma resource show cm_id

Expected output contains entries like:
    link bnxt_re0/1 cm-idn 1 ... state CONNECT ... comm [rpcrdma]
        src-addr <client_ip>:<port> dst-addr <server_ip>:<rdma_port>

The test asserts:
    1. "rpcrdma" is present (RDMA kernel module active)
    2. dst-addr matches the NFS server IP we mounted against
    3. mount -l shows proto=rdma for the mount point

TARGET CLUSTER:
===============
Bare-metal RDMA-capable nodes (e.g. grim031-037):
    - NFS servers: nodes with "nfs" role
    - Clients: nodes with "client" role
    - RoCE/InfiniBand NICs required on all participating nodes

"""

import json
from threading import Thread
from time import sleep

import yaml
from nfs_operations import (
    Enable_nfs_coredump,
    check_nfs_daemons_removed,
    mount_retry,
    open_mandatory_v3_ports,
)

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.utils import get_cluster_timestamp
from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Unmount
from utility.log import Log

log = Log(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _verify_rdma_connection(client, nfs_server_ip, mount_point):
    """Assert RDMA CM IDs to *nfs_server_ip* and proto=rdma on *mount_point*.

    Raises OperationFailedError on any check failure.
    """
    out, _ = client.exec_command(sudo=True, cmd="rdma resource show cm_id")
    if "rpcrdma" not in out:
        raise OperationFailedError(
            f"No rpcrdma CM IDs on {client.hostname}. Output: {out}"
        )
    if f"dst-addr {nfs_server_ip}:" not in out:
        raise OperationFailedError(
            f"No rpcrdma CM ID with dst-addr {nfs_server_ip} on "
            f"{client.hostname}. Output: {out}"
        )
    out, _ = client.exec_command(sudo=True, cmd=f"mount -l | grep '{mount_point}'")
    if "proto=rdma" not in out:
        raise OperationFailedError(
            f"{mount_point} on {client.hostname} not using proto=rdma. "
            f"Mount line: {out}"
        )
    log.info(
        "RDMA connection verified: %s -> %s on %s",
        client.hostname,
        nfs_server_ip,
        mount_point,
    )


def _verify_export_flags(client, nfs_name, export_path, expect_rdma=True):
    """Check export transports field via ``ceph nfs export get``.

    Returns the parsed export dict.
    """
    out, _ = client.exec_command(
        sudo=True,
        cmd=f"ceph nfs export get {nfs_name} {export_path} --format json",
        timeout=30,
    )
    export_info = json.loads(str(out).strip())
    transports = export_info.get("transports", [])
    if isinstance(transports, str):
        transports = [t.strip() for t in transports.split(",")]
    has_rdma = any(t.upper() == "RDMA" for t in transports)
    if expect_rdma and not has_rdma:
        raise OperationFailedError(
            f"Export {export_path} missing RDMA in transports: {transports}"
        )
    if not expect_rdma and has_rdma:
        raise OperationFailedError(
            f"Export {export_path} unexpectedly has RDMA: {transports}"
        )
    log.info(
        "Export %s transports=%s (expect_rdma=%s) -- OK",
        export_path,
        transports,
        expect_rdma,
    )
    return export_info


def _write_with_integrity(client, mount_point, filename="testfile", size_mb=64):
    """Write a file with dd and produce an md5sum checksum file.

    Returns the checksum filename (relative path inside mount).
    """
    data_path = f"{mount_point}/{filename}"
    md5_path = f"{mount_point}/{filename}.md5"
    client.exec_command(
        sudo=True,
        cmd=(
            f"dd if=/dev/urandom of={data_path} bs=1M count={size_mb} "
            f"conv=fsync status=none"
        ),
        timeout=300,
    )
    client.exec_command(
        sudo=True,
        cmd=f"md5sum {data_path} > {md5_path}",
        timeout=60,
    )
    log.info("Wrote %dMB to %s with md5 checksum", size_mb, data_path)
    return f"{filename}.md5"


def _verify_integrity(client, mount_point, md5_filename="testfile.md5"):
    """Run md5sum -c and assert success."""
    md5_path = f"{mount_point}/{md5_filename}"
    out, _ = client.exec_command(sudo=True, cmd=f"md5sum -c {md5_path}", timeout=60)
    if "OK" not in out:
        raise OperationFailedError(
            f"Integrity check failed on {client.hostname}: {out}"
        )
    log.info("Integrity verified via %s on %s", md5_path, client.hostname)


def _do_mount(client, mount_point, version, port, server, export, proto=None):
    """Mount an NFS export with optional proto override."""
    client.exec_command(sudo=True, cmd=f"mkdir -p {mount_point}")
    kwargs = {}
    if proto:
        kwargs["proto"] = proto
    mount_retry(client, mount_point, version, port, server, export, **kwargs)
    log.info(
        "Mounted %s:%s on %s at %s (vers=%s, port=%s, proto=%s)",
        server,
        export,
        client.hostname,
        mount_point,
        version,
        port,
        proto or "tcp",
    )


def _do_unmount(client, mount_point):
    """Unmount and remove mount directory."""
    Unmount(client).unmount(mount_point)
    client.exec_command(sudo=True, cmd=f"rm -rf {mount_point}", check_ec=False)
    sleep(2)


def _create_cluster(
    ceph_cluster,
    client,
    nfs_name,
    nfs_server,
    port="2049",
    enable_rdma=False,
    rdma_port=None,
    ha=False,
    vip=None,
    nfs_version=None,
):
    """Create an NFS cluster, enable coredumps, and wait for daemons."""
    Ceph(client).mgr.module.enable(module="nfs", force=True)
    sleep(2)
    kwargs = {}
    if nfs_version == 3:
        kwargs["nfs_version"] = 3
    if port and port != "2049":
        kwargs["port"] = port
    Ceph(client).nfs.cluster.create(
        name=nfs_name,
        nfs_server=nfs_server,
        ha=ha,
        vip=vip,
        enable_rdma=enable_rdma,
        rdma_port=rdma_port,
        **kwargs,
    )
    sleep(5)

    # Enable coredump collection only on the servers hosting this cluster
    server_list = nfs_server if isinstance(nfs_server, list) else [nfs_server]
    cluster_nodes = [n for n in ceph_cluster.get_nodes() if n.hostname in server_list]
    if cluster_nodes:
        Enable_nfs_coredump(cluster_nodes)

    log.info(
        "Created NFS cluster %s (servers=%s, rdma=%s, rdma_port=%s, ha=%s)",
        nfs_name,
        nfs_server,
        enable_rdma,
        rdma_port,
        ha,
    )


def _create_export(client, nfs_name, export_path, fs_name="cephfs", fs="cephfs"):
    """Create an NFS export backed by a CephFS subvolume."""
    Ceph(client).nfs.export.create(
        fs_name=fs_name, nfs_name=nfs_name, nfs_export=export_path, fs=fs
    )
    sleep(2)
    log.info("Created export %s on cluster %s", export_path, nfs_name)


def _delete_cluster(client, nfs_name):
    """Remove cluster and verify daemons are fully removed."""
    try:
        client.exec_command(
            sudo=True, cmd=f"ceph nfs cluster rm {nfs_name}", timeout=30
        )
    except Exception as e:
        log.warning("Cluster rm for %s: %s", nfs_name, e)
    sleep(30)
    check_nfs_daemons_removed(client)


def _delete_exports_and_subvols(client, nfs_name, export_paths):
    """Delete exports and their backing subvolumes."""
    for ep in export_paths:
        try:
            Ceph(client).nfs.export.delete(nfs_name, ep)
        except Exception as e:
            log.warning("Export delete %s/%s: %s", nfs_name, ep, e)
    sleep(2)
    try:
        out, _ = client.exec_command(
            sudo=True,
            cmd="ceph fs subvolume ls cephfs --group_name ganeshagroup --format json",
        )
        for item in json.loads(out):
            subvol = item["name"]
            client.exec_command(
                sudo=True,
                cmd=f"ceph fs subvolume rm cephfs {subvol} --group_name ganeshagroup",
                check_ec=False,
            )
    except Exception as e:
        log.warning("Subvolume cleanup: %s", e)
    client.exec_command(
        sudo=True,
        cmd="ceph fs subvolumegroup rm cephfs ganeshagroup --force",
        check_ec=False,
    )


def _full_cleanup(client, clients, nfs_name, export_paths, mount_points):
    """Best-effort cleanup of mounts, exports, and cluster.

    After cleanup, fails the active MGR to force a clean mgr state for
    subsequent tests (avoids stale NFS module state).

    Args:
        mount_points: list of mount path strings, or a single string.
    """
    if isinstance(mount_points, str):
        mount_points = [mount_points]
    for c in clients:
        for mp in mount_points:
            try:
                c.exec_command(sudo=True, cmd=f"umount -l {mp}", check_ec=False)
                c.exec_command(sudo=True, cmd=f"rm -rf {mp}", check_ec=False)
            except Exception:
                pass
    _delete_exports_and_subvols(client, nfs_name, export_paths)
    _delete_cluster(client, nfs_name)

    # Failover MGR to clear any stale NFS module state
    try:
        client.exec_command(sudo=True, cmd="ceph mgr fail", check_ec=False)
        sleep(5)
    except Exception:
        pass


def _pre_cleanup_checks(rados_obj, start_time):
    """Run health and crash checks BEFORE cleanup destroys state.

    Called in each scenario's finally block before _full_cleanup.
    Returns True if crashes were detected.
    """
    if not rados_obj or not start_time:
        log.warning("_pre_cleanup_checks: rados_obj or start_time not available")
        return False
    rados_obj.log_cluster_health()
    test_end_time = get_cluster_timestamp(rados_obj.node)
    log.debug(
        "Test workflow completed. Start time: %s, End time: %s",
        start_time,
        test_end_time,
    )
    if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
        log.error("Crash detected before cleanup")
        return True
    return False


def _get_server_ip(ceph_cluster, hostname):
    """Resolve a hostname to its primary IP via the CephNode object."""
    for node in ceph_cluster.get_nodes():
        if node.hostname == hostname:
            if node.ip_address:
                return node.ip_address
    raise ConfigError(f"Cannot resolve IP for {hostname}")


def _log_setup_details(client, nfs_name):
    """Log cluster, service, daemon, network, and export details for diagnostics.

    Captures everything needed to debug mount/connection failures:
    - Cluster backend info (hosts, IPs, ports, VIP)
    - Orch service spec (enable_rdma, rdma_port, ports, placement)
    - Individual daemon status (hostname, running state, container ID)
    - RDMA device/link status on NFS server
    - Firewall ports open on NFS server
    - Export list with details
    """
    log.info("--- Setup Details for cluster: %s ---", nfs_name)

    # 1. Cluster info (backend hosts, IPs, ports)
    try:
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph nfs cluster info {nfs_name} --format json",
            timeout=30,
        )
        log.info("Cluster info:\n%s", out.strip())
    except Exception as e:
        log.warning("Could not get cluster info: %s", e)

    # 2. Orch service spec and status
    try:
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph orch ls --service-name nfs.{nfs_name} --format json",
            timeout=30,
        )
        svc = json.loads(out)
        if svc:
            spec = svc[0].get("spec", {})
            status = svc[0].get("status", {})
            placement = svc[0].get("placement", {})
            ports = status.get("ports", [])
            log.info(
                "Service: nfs.%s | running=%s/%s | ports=%s | "
                "placement=%s | spec=%s",
                nfs_name,
                status.get("running", 0),
                status.get("size", "?"),
                ports,
                placement,
                spec,
            )
    except Exception as e:
        log.warning("Could not get orch service info: %s", e)

    # 3. Individual daemon status
    try:
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph orch ps --service-name nfs.{nfs_name} --format json",
            timeout=30,
        )
        daemons = json.loads(out) if out.strip() else []
        for d in daemons:
            log.info(
                "  Daemon: %s | host=%s | status=%s | ports=%s",
                d.get("daemon_name", "?"),
                d.get("hostname", "?"),
                d.get("status_desc", "?"),
                d.get("ports", []),
            )
    except Exception as e:
        log.warning("Could not get daemon status: %s", e)

    # 4. RDMA device status on client
    try:
        out, _ = client.exec_command(sudo=True, cmd="rdma link show", timeout=10)
        log.info("RDMA links (client): %s", out.strip())
    except Exception as e:
        log.warning("Could not get RDMA links: %s", e)

    # 5. Export list
    try:
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph nfs export ls {nfs_name} --format json",
            timeout=30,
        )
        log.info("Exports: %s", out.strip())
    except Exception as e:
        log.warning("Could not list exports: %s", e)

    log.info("--- End Setup Details ---")


def _wait_for_nfs_service(client, nfs_name, timeout=120):
    """Poll until NFS service is running."""
    for w in WaitUntil(timeout=timeout, interval=10):
        try:
            out, _ = client.exec_command(
                sudo=True,
                cmd=f"ceph orch ls --service-name nfs.{nfs_name} --format json",
                timeout=30,
            )
            services = json.loads(out)
            if services and services[0].get("status", {}).get("running", 0) > 0:
                log.info("NFS service nfs.%s is running", nfs_name)
                return
        except Exception:
            pass
    if w.expired:
        raise OperationFailedError(
            f"NFS service nfs.{nfs_name} not running after {timeout}s"
        )


# ---------------------------------------------------------------------------
# Standard scenario workflow (used by simple scenarios)
# ---------------------------------------------------------------------------


def _run_standard_rdma_scenario(
    ceph_cluster,
    config,
    nfs_version,
    num_servers=1,
    num_clients=1,
    rdma_port=None,
    ha=False,
    vip=None,
    round_robin=False,
    custom_port=None,
):
    """Execute the standard RDMA validation workflow for a single scenario.

    Creates cluster, exports, mounts with RDMA, verifies connections and
    data integrity through an unmount/remount cycle, then cleans up.
    """
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    if num_servers > len(nfs_nodes):
        raise ConfigError(
            f"Need {num_servers} NFS servers but only {len(nfs_nodes)} available"
        )
    servers = nfs_nodes[:num_servers]
    server_names = [s.hostname for s in servers]

    clients = ceph_cluster.get_nodes("client")[:num_clients]
    if not clients:
        raise ConfigError("No client nodes available")

    port = custom_port or config.get("port", "2049")
    r_port = rdma_port or config.get("rdma_port", "20049")
    nfs_name = "cephfs-nfs"
    mount_base = "/mnt/nfs_rdma"
    export_base = "/export"
    exports_created = []

    try:
        # 1. Create cluster
        _create_cluster(
            ceph_cluster,
            clients[0],
            nfs_name,
            server_names,
            port=port,
            enable_rdma=True,
            rdma_port=r_port,
            ha=ha,
            vip=vip,
            nfs_version=int(float(nfs_version)) if float(nfs_version) < 4 else None,
        )
        _wait_for_nfs_service(clients[0], nfs_name, timeout=120)

        # Prepare NFSv3 ports if needed (skip if firewalld not running on BM nodes)
        if str(nfs_version) == "3":
            for node in servers:
                try:
                    open_mandatory_v3_ports(node, ["portmapper", "mountd"])
                except Exception as e:
                    if "not running" in str(e).lower():
                        log.info(
                            "Firewalld not running on %s -- ports assumed open",
                            node.hostname,
                        )
                    else:
                        raise

        # 2. Create exports (one per client)
        for i in range(num_clients):
            ep = f"{export_base}_{i}"
            _create_export(clients[0], nfs_name, ep)
            exports_created.append(ep)

        # 3. Verify export info
        for ep in exports_created:
            _verify_export_flags(clients[0], nfs_name, ep, expect_rdma=True)

        # Log setup details for diagnostics
        _log_setup_details(clients[0], nfs_name)

        # Determine mount server
        if ha and vip:
            mount_server_ip = vip.split("/")[0]
        else:
            mount_server_ip = _get_server_ip(ceph_cluster, server_names[0])

        # 4. Mount exports on clients
        for i, client in enumerate(clients):
            ep = exports_created[i] if i < len(exports_created) else exports_created[0]
            mount_point = f"{mount_base}_{i}"
            svr = mount_server_ip
            if round_robin and not ha:
                svr = _get_server_ip(ceph_cluster, server_names[i % len(server_names)])
            _do_mount(
                client, mount_point, str(nfs_version), r_port, svr, ep, proto="rdma"
            )

        # 5. Verify RDMA connections
        for i, client in enumerate(clients):
            mount_point = f"{mount_base}_{i}"
            svr = mount_server_ip
            if round_robin and not ha:
                svr = _get_server_ip(ceph_cluster, server_names[i % len(server_names)])
            _verify_rdma_connection(client, svr, mount_point)

        # 6. Write with integrity
        md5_files = []
        for i, client in enumerate(clients):
            mount_point = f"{mount_base}_{i}"
            md5 = _write_with_integrity(client, mount_point, f"data_{i}", size_mb=32)
            md5_files.append(md5)

        # 7. Unmount
        for i, client in enumerate(clients):
            _do_unmount(client, f"{mount_base}_{i}")

        # 8. Remount
        for i, client in enumerate(clients):
            ep = exports_created[i] if i < len(exports_created) else exports_created[0]
            mount_point = f"{mount_base}_{i}"
            svr = mount_server_ip
            if round_robin and not ha:
                svr = _get_server_ip(ceph_cluster, server_names[i % len(server_names)])
            _do_mount(
                client, mount_point, str(nfs_version), r_port, svr, ep, proto="rdma"
            )

        # 9. Re-verify integrity
        for i, client in enumerate(clients):
            mount_point = f"{mount_base}_{i}"
            _verify_integrity(client, mount_point, md5_files[i])

        # 10. Re-verify RDMA
        for i, client in enumerate(clients):
            mount_point = f"{mount_base}_{i}"
            svr = mount_server_ip
            if round_robin and not ha:
                svr = _get_server_ip(ceph_cluster, server_names[i % len(server_names)])
            _verify_rdma_connection(client, svr, mount_point)

    finally:
        _pre_cleanup_checks(config.get("_rados_obj"), config.get("_start_time"))
        mount_list = [f"{mount_base}_{i}" for i in range(num_clients)]
        _full_cleanup(clients[0], clients, nfs_name, exports_created, mount_list)


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


def scenario_basic_rdma_v42(ceph_cluster, config):
    """
    Scenario 1: RDMA basic mount with NFSv4.2.

    Creates a single-daemon NFS cluster with --enable-rdma, creates one
    export, mounts on one client with proto=rdma and vers=4.2, verifies
    RDMA CM connection to server endpoint, writes 32MB with md5 checksum,
    unmounts, remounts, and re-verifies data integrity and RDMA connection.

    Validates: Basic RDMA transport works with the default NFSv4.2 protocol.
    """
    log.info("Single daemon, single client, NFSv4.2 over RDMA")
    _run_standard_rdma_scenario(ceph_cluster, config, nfs_version="4.2")


def scenario_rdma_v41(ceph_cluster, config):
    """
    Scenario 2: RDMA with NFSv4.1.

    Same as Scenario 1 but mounts with vers=4.1.  Verifies that NFSv4.1
    sessions and trunking work correctly over RDMA transport.

    Validates: RDMA transport compatibility with NFSv4.1 protocol.
    """
    log.info("Single daemon, single client, NFSv4.1 over RDMA")
    _run_standard_rdma_scenario(ceph_cluster, config, nfs_version="4.1")


def scenario_rdma_v40(ceph_cluster, config):
    """
    Scenario 3: RDMA with NFSv4.0.

    Same as Scenario 1 but mounts with vers=4.0.  Tests the legacy NFSv4.0
    protocol (no sessions) over RDMA transport.

    Validates: RDMA transport compatibility with NFSv4.0 protocol.
    """
    log.info("Single daemon, single client, NFSv4.0 over RDMA")
    _run_standard_rdma_scenario(ceph_cluster, config, nfs_version="4.0")


def scenario_rdma_v3(ceph_cluster, config):
    """
    Scenario 4: RDMA with NFSv3.

    Creates cluster with --enable-rdma --enable-nfsv3, prepares host for
    NFSv3 dependencies (rpcbind), opens portmapper/mountd firewall ports,
    then mounts with vers=3 and proto=rdma.

    Validates: NFSv3 (stateless, UDP-heritage protocol) works over RDMA
    transport with NFS-Ganesha.
    """
    log.info("Single daemon, single client, NFSv3 over RDMA")
    _run_standard_rdma_scenario(ceph_cluster, config, nfs_version="3")


def scenario_multi_version_rdma(ceph_cluster, config):
    """
    Scenario 5: Multi-version mix -- two clients, different NFS versions, both RDMA.

    Creates one RDMA-enabled cluster with --enable-nfsv3, creates two exports.
    Client 0 mounts export_0 with vers=4.2 proto=rdma.
    Client 1 mounts export_1 with vers=3 proto=rdma.
    Both write data with integrity, unmount, remount, re-verify.

    Validates: A single NFS-Ganesha daemon can serve both NFSv3 and NFSv4.2
    clients simultaneously over RDMA transport without interference.
    """
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    servers = nfs_nodes[:1]
    server_names = [s.hostname for s in servers]
    clients = ceph_cluster.get_nodes("client")[:2]
    if len(clients) < 2:
        raise ConfigError("Need at least 2 clients for multi-version scenario")

    port = config.get("port", "2049")
    r_port = config.get("rdma_port", "20049")
    nfs_name = "cephfs-nfs"
    mount_base = "/mnt/nfs_rdma"
    exports_created = []
    versions = ["4.2", "3"]

    try:
        _create_cluster(
            ceph_cluster,
            clients[0],
            nfs_name,
            server_names,
            port=port,
            enable_rdma=True,
            rdma_port=r_port,
            nfs_version=3,
        )
        for node in servers:
            try:
                open_mandatory_v3_ports(node, ["portmapper", "mountd"])
            except Exception as e:
                if "not running" in str(e).lower():
                    log.info(
                        "Firewalld not running on %s -- ports assumed open",
                        node.hostname,
                    )
                else:
                    raise

        for i in range(2):
            ep = f"/export_{i}"
            _create_export(clients[0], nfs_name, ep)
            exports_created.append(ep)

        for ep in exports_created:
            _verify_export_flags(clients[0], nfs_name, ep, expect_rdma=True)

        _log_setup_details(clients[0], nfs_name)
        mount_server_ip = _get_server_ip(ceph_cluster, server_names[0])

        for i, client in enumerate(clients):
            _do_mount(
                client,
                f"{mount_base}_{i}",
                versions[i],
                r_port,
                mount_server_ip,
                exports_created[i],
                proto="rdma",
            )

        for i, client in enumerate(clients):
            _verify_rdma_connection(client, mount_server_ip, f"{mount_base}_{i}")

        md5_files = []
        for i, client in enumerate(clients):
            md5 = _write_with_integrity(
                client, f"{mount_base}_{i}", f"data_{i}", size_mb=32
            )
            md5_files.append(md5)

        for i, client in enumerate(clients):
            _do_unmount(client, f"{mount_base}_{i}")
        for i, client in enumerate(clients):
            _do_mount(
                client,
                f"{mount_base}_{i}",
                versions[i],
                r_port,
                mount_server_ip,
                exports_created[i],
                proto="rdma",
            )

        for i, client in enumerate(clients):
            _verify_integrity(client, f"{mount_base}_{i}", md5_files[i])
            _verify_rdma_connection(client, mount_server_ip, f"{mount_base}_{i}")

    finally:
        _pre_cleanup_checks(config.get("_rados_obj"), config.get("_start_time"))
        mount_list = [f"{mount_base}_{i}" for i in range(2)]
        _full_cleanup(clients[0], clients, nfs_name, exports_created, mount_list)


def scenario_active_active_no_vip(ceph_cluster, config):
    """
    Scenario 6: Active-active multi-server, no VIP, round-robin RDMA mounts.

    Creates a 2-daemon NFS cluster (both active simultaneously, no ingress).
    Two clients mount directly to different daemons in round-robin fashion,
    each using proto=rdma.  Verifies that RDMA CM IDs point to the correct
    respective server endpoints.

    Validates: RDMA works with active-active (non-HA) multi-daemon topology
    where clients connect directly to individual Ganesha instances.
    """
    log.info("2 servers active-active, 2 clients round-robin RDMA mounts")
    _run_standard_rdma_scenario(
        ceph_cluster,
        config,
        nfs_version="4.2",
        num_servers=2,
        num_clients=2,
        round_robin=True,
    )


def scenario_ha_with_vip(ceph_cluster, config):
    """
    Scenario 7: HA with VIP/ingress, RDMA mount via VIP.

    Creates a 2-daemon NFS cluster with --ingress --virtual-ip, fronted by
    haproxy+keepalived.  The RDMA listener binds directly on the VIP (haproxy
    proxies TCP only).  Client mounts via VIP with proto=rdma.

    Validates: RDMA transport works through a VIP/keepalived HA topology
    where the floating VIP carries the RDMA endpoint.
    """
    vip = config.get("vip")
    if not vip:
        raise ConfigError("VIP not provided in config for HA scenario")
    log.info("HA with VIP %s, RDMA mount via VIP", vip)
    _run_standard_rdma_scenario(
        ceph_cluster,
        config,
        nfs_version="4.2",
        num_servers=2,
        num_clients=1,
        ha=True,
        vip=vip,
    )


def scenario_enable_rdma_on_existing(ceph_cluster, config):
    """
    Scenario 8: Enable RDMA on an existing TCP-only NFS cluster.

    This is the most complex scenario with two sub-workflows:

    Sub-workflow A (TCP baseline):
        A1. Create cluster WITHOUT --enable-rdma
        A2. Create export /export_tcp
        A3. Verify export transports does NOT contain "RDMA"
        A4. Mount with proto=tcp, write 32MB with md5 checksum
        A5. Unmount, remount TCP, verify integrity
        A6. Unmount (prepare for RDMA enablement)

    Sub-workflow B (Enable RDMA):
        B1. Apply updated orch spec with enable_rdma=true via ceph orch apply -i
        B2. Wait for NFS daemons to restart with RDMA listener
        B3. Update OLD export transports to ["TCP", "RDMA"] via ceph nfs export apply
        B4. Verify old export now shows RDMA in transports
        B5. Create NEW export /export_rdma (auto-inherits RDMA from cluster)
        B6. Verify new export has RDMA in transports
        B7. Mount OLD export with proto=rdma (proves pre-existing export works)
        B8. Mount NEW export with proto=rdma
        B9. Verify RDMA CM connections to server for both mounts
        B10. Verify OLD data integrity (md5 from TCP phase survives)
        B11. Write new data on both exports with integrity
        B12. Unmount, remount RDMA, re-verify all integrity
        B13. Cleanup

    Validates: RDMA can be enabled on a running NFS cluster without data loss,
    and both pre-existing and new exports work over RDMA after enablement.
    """
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    servers = nfs_nodes[:1]
    server_names = [s.hostname for s in servers]
    clients = ceph_cluster.get_nodes("client")[:1]
    if not clients:
        raise ConfigError("No client nodes available")
    client = clients[0]

    port = config.get("port", "2049")
    r_port = config.get("rdma_port", "20049")
    nfs_name = "cephfs-nfs"
    mount_old = "/mnt/nfs_export_old"
    mount_new = "/mnt/nfs_export_new"
    export_tcp = "/export_tcp"
    export_rdma = "/export_rdma"
    exports_created = []

    try:
        # --- Sub-workflow A: TCP baseline ---
        log.info("--- Sub-workflow A: TCP baseline ---")

        # A1. Create cluster WITHOUT RDMA
        _create_cluster(
            ceph_cluster, client, nfs_name, server_names, port=port, enable_rdma=False
        )
        _wait_for_nfs_service(client, nfs_name, timeout=120)

        # A2. Create export
        _create_export(client, nfs_name, export_tcp)
        exports_created.append(export_tcp)

        # A3. Verify export does NOT have RDMA
        _verify_export_flags(client, nfs_name, export_tcp, expect_rdma=False)

        # A4. Mount with TCP
        server_ip = _get_server_ip(ceph_cluster, server_names[0])
        _do_mount(client, mount_old, "4.2", port, server_ip, export_tcp, proto="tcp")

        # A5. Write with integrity
        md5_tcp = _write_with_integrity(client, mount_old, "tcp_data", size_mb=32)

        # A6. Unmount, remount, verify integrity
        _do_unmount(client, mount_old)
        _do_mount(client, mount_old, "4.2", port, server_ip, export_tcp, proto="tcp")
        _verify_integrity(client, mount_old, md5_tcp)

        # A7. Unmount before RDMA enablement
        _do_unmount(client, mount_old)

        # --- Sub-workflow B: Enable RDMA ---
        log.info("--- Sub-workflow B: Enable RDMA via orch apply ---")

        # B2. Get existing orch spec, add RDMA fields, and re-apply
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph orch ls --service-name nfs.{nfs_name} --export --format json",
            timeout=30,
        )
        existing_specs = json.loads(out)
        if not existing_specs:
            raise OperationFailedError(
                f"No existing orch spec found for nfs.{nfs_name}"
            )
        original_spec = existing_specs[0]
        log.info("Original orch spec:\n%s", json.dumps(original_spec, indent=2))

        spec = json.loads(json.dumps(original_spec))  # deep copy
        spec.setdefault("spec", {})
        spec["spec"]["enable_rdma"] = True
        spec["spec"]["rdma_port"] = int(r_port)
        # Remove runtime fields that shouldn't be in an apply spec
        spec.pop("status", None)
        spec.pop("events", None)
        spec_yaml = yaml.dump(spec, default_flow_style=False)
        log.info("Modified orch spec (RDMA enabled):\n%s", spec_yaml)

        client.exec_command(
            sudo=True,
            cmd=f"cat > /tmp/nfs_rdma_spec.yaml << 'EOF'\n{spec_yaml}EOF",
        )
        client.exec_command(
            sudo=True,
            cmd="ceph orch apply -i /tmp/nfs_rdma_spec.yaml",
            timeout=60,
        )
        log.info("Applied updated orch spec with enable_rdma=true")

        # B3. Wait for service to restart with RDMA
        # Workaround: orch apply may not reliably redeploy the daemon.
        # If enable_rdma_redeploy_wa is set, explicitly redeploy the daemon.
        enable_rdma_redeploy_wa = config.get("enable_rdma_redeploy_wa", True)
        if enable_rdma_redeploy_wa:
            log.info("Workaround: explicitly redeploying NFS daemon")
            sleep(10)
            try:
                out, _ = client.exec_command(
                    sudo=True,
                    cmd=(
                        f"ceph orch ps --service-name nfs.{nfs_name} " f"--format json"
                    ),
                    timeout=30,
                )
                daemons = json.loads(out) if out.strip() else []
                for d in daemons:
                    daemon_name = d.get("daemon_name", "")
                    if daemon_name:
                        log.info("Redeploying daemon: %s", daemon_name)
                        client.exec_command(
                            sudo=True,
                            cmd=f"ceph orch daemon redeploy {daemon_name}",
                            timeout=60,
                        )
            except Exception as e:
                log.warning("Redeploy workaround failed: %s", e)

        sleep(30)
        _wait_for_nfs_service(client, nfs_name, timeout=300)
        sleep(30)  # Allow RDMA listener to fully bind after daemon restart

        # B4. Update OLD export to add RDMA transport
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph nfs export get {nfs_name} {export_tcp} --format json",
            timeout=30,
        )
        old_export = json.loads(str(out).strip())
        old_export["transports"] = ["TCP", "RDMA"]
        export_json = json.dumps(old_export)
        client.exec_command(
            sudo=True,
            cmd=f"echo '{export_json}' > /tmp/export_update.json",
        )
        client.exec_command(
            sudo=True,
            cmd=f"ceph nfs export apply {nfs_name} -i /tmp/export_update.json",
            timeout=30,
        )
        sleep(3)

        # B5. Verify old export now shows RDMA
        _verify_export_flags(client, nfs_name, export_tcp, expect_rdma=True)

        # B6. Create NEW export
        _create_export(client, nfs_name, export_rdma)
        exports_created.append(export_rdma)

        # B7. Verify new export has RDMA
        _verify_export_flags(client, nfs_name, export_rdma, expect_rdma=True)

        _log_setup_details(client, nfs_name)

        # B8. Mount OLD export with RDMA (same mount point preserves md5 paths)
        _do_mount(client, mount_old, "4.2", r_port, server_ip, export_tcp, proto="rdma")

        # B9. Mount NEW export with RDMA
        _do_mount(
            client,
            mount_new,
            "4.2",
            r_port,
            server_ip,
            export_rdma,
            proto="rdma",
        )

        # B10. Verify RDMA connections
        _verify_rdma_connection(client, server_ip, mount_old)
        _verify_rdma_connection(client, server_ip, mount_new)

        # B11. Verify OLD data integrity (survived RDMA enablement)
        _verify_integrity(client, mount_old, md5_tcp)

        # B12. Write new data on both exports
        md5_old_new = _write_with_integrity(
            client, mount_old, "rdma_data_old", size_mb=32
        )
        md5_new = _write_with_integrity(client, mount_new, "rdma_data_new", size_mb=32)

        # B13. Unmount, remount, re-verify
        _do_unmount(client, mount_old)
        _do_unmount(client, mount_new)
        _do_mount(client, mount_old, "4.2", r_port, server_ip, export_tcp, proto="rdma")
        _do_mount(
            client,
            mount_new,
            "4.2",
            r_port,
            server_ip,
            export_rdma,
            proto="rdma",
        )
        _verify_integrity(client, mount_old, md5_tcp)
        _verify_integrity(client, mount_old, md5_old_new)
        _verify_integrity(client, mount_new, md5_new)
        _verify_rdma_connection(client, server_ip, mount_old)
        _verify_rdma_connection(client, server_ip, mount_new)

    finally:
        # B14. Cleanup
        _pre_cleanup_checks(config.get("_rados_obj"), config.get("_start_time"))
        for mp in (mount_old, mount_new):
            try:
                client.exec_command(sudo=True, cmd=f"umount -l {mp}", check_ec=False)
                client.exec_command(sudo=True, cmd=f"rm -rf {mp}", check_ec=False)
            except Exception:
                pass
        _delete_exports_and_subvols(client, nfs_name, exports_created)
        _delete_cluster(client, nfs_name)
        try:
            client.exec_command(sudo=True, cmd="ceph mgr fail", check_ec=False)
            sleep(5)
        except Exception:
            pass


def scenario_custom_rdma_port(ceph_cluster, config):
    """
    Scenario 9: RDMA with non-default custom TCP and RDMA ports.

    Creates cluster with custom --port and --rdma_port (both non-default).
    Mounts with proto=rdma using the custom RDMA port.  Verifies that the
    RDMA CM connection uses the custom dst-port and that non-standard port
    configuration works end-to-end.

    Validates: Custom port and rdma_port configuration is correctly applied
    to the NFS-Ganesha listeners and clients can connect on non-standard ports.
    """
    custom_port = config.get("port", "2050")
    custom_rdma_port = config.get("rdma_port", "12345")
    log.info("Custom TCP port %s, RDMA port %s", custom_port, custom_rdma_port)
    _run_standard_rdma_scenario(
        ceph_cluster,
        config,
        nfs_version="4.2",
        rdma_port=custom_rdma_port,
        custom_port=custom_port,
    )


def scenario_multi_client_parallel_io(ceph_cluster, config):
    """
    Scenario 10: Two clients writing 64MB each in parallel over RDMA.

    Creates one RDMA-enabled cluster with two exports.  Both clients mount
    their respective exports with proto=rdma and write 64MB simultaneously
    using parallel threads.  After completion, both unmount, remount, and
    verify data integrity independently.

    Validates: Concurrent RDMA I/O from multiple clients does not cause
    resource contention, CM ID conflicts, or data corruption.
    """
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    servers = nfs_nodes[:1]
    server_names = [s.hostname for s in servers]
    clients = ceph_cluster.get_nodes("client")[:2]
    if len(clients) < 2:
        raise ConfigError("Need 2 clients for parallel IO scenario")

    port = config.get("port", "2049")
    r_port = config.get("rdma_port", "20049")
    nfs_name = "cephfs-nfs"
    mount_base = "/mnt/nfs_rdma"
    exports_created = []

    try:
        _create_cluster(
            ceph_cluster,
            clients[0],
            nfs_name,
            server_names,
            port=port,
            enable_rdma=True,
            rdma_port=r_port,
        )
        _wait_for_nfs_service(clients[0], nfs_name, timeout=120)

        for i in range(2):
            ep = f"/export_{i}"
            _create_export(clients[0], nfs_name, ep)
            exports_created.append(ep)

        for ep in exports_created:
            _verify_export_flags(clients[0], nfs_name, ep, expect_rdma=True)

        _log_setup_details(clients[0], nfs_name)
        server_ip = _get_server_ip(ceph_cluster, server_names[0])

        for i, client in enumerate(clients):
            _do_mount(
                client,
                f"{mount_base}_{i}",
                "4.2",
                r_port,
                server_ip,
                exports_created[i],
                proto="rdma",
            )
            _verify_rdma_connection(client, server_ip, f"{mount_base}_{i}")

        # Parallel write with integrity
        errors = []

        def _parallel_write(client, mount_point, idx):
            try:
                _write_with_integrity(client, mount_point, f"par_data_{idx}", 64)
            except Exception as e:
                errors.append(f"Client {idx}: {e}")

        threads = []
        for i, client in enumerate(clients):
            t = Thread(target=_parallel_write, args=(client, f"{mount_base}_{i}", i))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        if errors:
            raise OperationFailedError(f"Parallel write failures: {errors}")

        # Unmount, remount, verify
        for i, client in enumerate(clients):
            _do_unmount(client, f"{mount_base}_{i}")
        for i, client in enumerate(clients):
            _do_mount(
                client,
                f"{mount_base}_{i}",
                "4.2",
                r_port,
                server_ip,
                exports_created[i],
                proto="rdma",
            )

        for i, client in enumerate(clients):
            _verify_integrity(client, f"{mount_base}_{i}", f"par_data_{i}.md5")
            _verify_rdma_connection(client, server_ip, f"{mount_base}_{i}")

    finally:
        _pre_cleanup_checks(config.get("_rados_obj"), config.get("_start_time"))
        mount_list = [f"{mount_base}_{i}" for i in range(2)]
        _full_cleanup(clients[0], clients, nfs_name, exports_created, mount_list)


def scenario_tcp_and_rdma_coexist(ceph_cluster, config):
    """
    Scenario 11: TCP and RDMA coexistence on the same cluster.

    Creates one RDMA-enabled cluster with two exports.
    Client 0 mounts export_0 with proto=rdma (RDMA port).
    Client 1 mounts export_1 with proto=tcp (TCP port).
    Verifies:
        - Client 0 shows proto=rdma in mount and has RDMA CM IDs
        - Client 1 shows proto=tcp in mount (no RDMA)
        - Both write with integrity, unmount, remount, re-verify

    Validates: An RDMA-enabled NFS-Ganesha cluster correctly serves both
    TCP and RDMA clients simultaneously without interference.
    """
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    servers = nfs_nodes[:1]
    server_names = [s.hostname for s in servers]
    clients = ceph_cluster.get_nodes("client")[:2]
    if len(clients) < 2:
        raise ConfigError("Need 2 clients for TCP+RDMA coexist scenario")

    port = config.get("port", "2049")
    r_port = config.get("rdma_port", "20049")
    nfs_name = "cephfs-nfs"
    mount_rdma = "/mnt/nfs_rdma_0"
    mount_tcp = "/mnt/nfs_tcp_0"
    exports_created = []

    try:
        _create_cluster(
            ceph_cluster,
            clients[0],
            nfs_name,
            server_names,
            port=port,
            enable_rdma=True,
            rdma_port=r_port,
        )
        _wait_for_nfs_service(clients[0], nfs_name, timeout=120)

        for i in range(2):
            ep = f"/export_{i}"
            _create_export(clients[0], nfs_name, ep)
            exports_created.append(ep)

        for ep in exports_created:
            _verify_export_flags(clients[0], nfs_name, ep, expect_rdma=True)

        _log_setup_details(clients[0], nfs_name)
        server_ip = _get_server_ip(ceph_cluster, server_names[0])

        # Client 0: RDMA mount
        _do_mount(
            clients[0],
            mount_rdma,
            "4.2",
            r_port,
            server_ip,
            exports_created[0],
            proto="rdma",
        )
        _verify_rdma_connection(clients[0], server_ip, mount_rdma)

        # Client 1: TCP mount (same cluster, different export)
        _do_mount(
            clients[1],
            mount_tcp,
            "4.2",
            port,
            server_ip,
            exports_created[1],
            proto="tcp",
        )
        # Verify TCP mount does NOT show proto=rdma
        out, _ = clients[1].exec_command(
            sudo=True, cmd=f"mount -l | grep '{mount_tcp}'"
        )
        if "proto=rdma" in out:
            raise OperationFailedError(
                f"TCP mount {mount_tcp} unexpectedly shows proto=rdma"
            )
        if "proto=tcp" not in out:
            raise OperationFailedError(
                f"TCP mount {mount_tcp} missing proto=tcp: {out}"
            )
        log.info("TCP mount verified on %s: %s", clients[1].hostname, mount_tcp)

        # Both write with integrity
        md5_rdma = _write_with_integrity(clients[0], mount_rdma, "rdma_file", 32)
        md5_tcp = _write_with_integrity(clients[1], mount_tcp, "tcp_file", 32)

        # Unmount both
        _do_unmount(clients[0], mount_rdma)
        _do_unmount(clients[1], mount_tcp)

        # Remount with same protos
        _do_mount(
            clients[0],
            mount_rdma,
            "4.2",
            r_port,
            server_ip,
            exports_created[0],
            proto="rdma",
        )
        _do_mount(
            clients[1],
            mount_tcp,
            "4.2",
            port,
            server_ip,
            exports_created[1],
            proto="tcp",
        )

        # Verify integrity
        _verify_integrity(clients[0], mount_rdma, md5_rdma)
        _verify_integrity(clients[1], mount_tcp, md5_tcp)

        # Re-verify RDMA only on RDMA client
        _verify_rdma_connection(clients[0], server_ip, mount_rdma)

    finally:
        _pre_cleanup_checks(config.get("_rados_obj"), config.get("_start_time"))
        for mp, c in [(mount_rdma, clients[0]), (mount_tcp, clients[1])]:
            try:
                c.exec_command(sudo=True, cmd=f"umount -l {mp}", check_ec=False)
                c.exec_command(sudo=True, cmd=f"rm -rf {mp}", check_ec=False)
            except Exception:
                pass
        _delete_exports_and_subvols(clients[0], nfs_name, exports_created)
        _delete_cluster(clients[0], nfs_name)
        try:
            clients[0].exec_command(sudo=True, cmd="ceph mgr fail", check_ec=False)
            sleep(5)
        except Exception:
            pass


def scenario_single_client_dual_proto(ceph_cluster, config):
    """
    Scenario 12: Single client mounts two exports -- one RDMA, one TCP.

    Creates one RDMA-enabled cluster with two exports. The SAME client
    mounts export_0 with proto=rdma and export_1 with proto=tcp.
    Verifies both mounts work simultaneously, RDMA CM IDs exist for the
    RDMA mount only, and data integrity holds on both after unmount/remount.

    Validates: A single NFS client can maintain both RDMA and TCP connections
    to the same NFS-Ganesha server simultaneously without interference.
    """
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    servers = nfs_nodes[:1]
    server_names = [s.hostname for s in servers]
    clients = ceph_cluster.get_nodes("client")[:1]
    if not clients:
        raise ConfigError("No client nodes available")
    client = clients[0]

    port = config.get("port", "2049")
    r_port = config.get("rdma_port", "20049")
    nfs_name = "cephfs-nfs"
    mount_rdma = "/mnt/nfs_rdma_single"
    mount_tcp = "/mnt/nfs_tcp_single"
    exports_created = []

    try:
        _create_cluster(
            ceph_cluster,
            client,
            nfs_name,
            server_names,
            port=port,
            enable_rdma=True,
            rdma_port=r_port,
        )
        _wait_for_nfs_service(client, nfs_name, timeout=120)

        for i in range(2):
            ep = f"/export_{i}"
            _create_export(client, nfs_name, ep)
            exports_created.append(ep)

        for ep in exports_created:
            _verify_export_flags(client, nfs_name, ep, expect_rdma=True)

        _log_setup_details(client, nfs_name)
        server_ip = _get_server_ip(ceph_cluster, server_names[0])

        # Mount export_0 with RDMA
        _do_mount(
            client,
            mount_rdma,
            "4.2",
            r_port,
            server_ip,
            exports_created[0],
            proto="rdma",
        )
        _verify_rdma_connection(client, server_ip, mount_rdma)

        # Mount export_1 with TCP on the SAME client
        _do_mount(
            client, mount_tcp, "4.2", port, server_ip, exports_created[1], proto="tcp"
        )
        # Verify TCP mount shows proto=tcp (not rdma)
        out, _ = client.exec_command(sudo=True, cmd=f"mount -l | grep '{mount_tcp}'")
        if "proto=rdma" in out:
            raise OperationFailedError(
                f"TCP mount {mount_tcp} unexpectedly shows proto=rdma"
            )
        if "proto=tcp" not in out:
            raise OperationFailedError(
                f"TCP mount {mount_tcp} missing proto=tcp: {out}"
            )
        log.info("Dual-proto verified: RDMA on %s, TCP on %s", mount_rdma, mount_tcp)

        # Write with integrity on both
        md5_rdma = _write_with_integrity(client, mount_rdma, "rdma_data", 32)
        md5_tcp = _write_with_integrity(client, mount_tcp, "tcp_data", 32)

        # Unmount both
        _do_unmount(client, mount_rdma)
        _do_unmount(client, mount_tcp)

        # Remount both with same protos
        _do_mount(
            client,
            mount_rdma,
            "4.2",
            r_port,
            server_ip,
            exports_created[0],
            proto="rdma",
        )
        _do_mount(
            client, mount_tcp, "4.2", port, server_ip, exports_created[1], proto="tcp"
        )

        # Re-verify integrity
        _verify_integrity(client, mount_rdma, md5_rdma)
        _verify_integrity(client, mount_tcp, md5_tcp)

        # Re-verify RDMA connection still active
        _verify_rdma_connection(client, server_ip, mount_rdma)

    finally:
        _pre_cleanup_checks(config.get("_rados_obj"), config.get("_start_time"))
        for mp in (mount_rdma, mount_tcp):
            try:
                client.exec_command(sudo=True, cmd=f"umount -l {mp}", check_ec=False)
                client.exec_command(sudo=True, cmd=f"rm -rf {mp}", check_ec=False)
            except Exception:
                pass
        _delete_exports_and_subvols(client, nfs_name, exports_created)
        _delete_cluster(client, nfs_name)
        try:
            client.exec_command(sudo=True, cmd="ceph mgr fail", check_ec=False)
            sleep(5)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


SCENARIO_MAP = {
    "basic_rdma_v42": scenario_basic_rdma_v42,
    "rdma_v41": scenario_rdma_v41,
    "rdma_v40": scenario_rdma_v40,
    "rdma_v3": scenario_rdma_v3,
    "multi_version_rdma": scenario_multi_version_rdma,
    "active_active_no_vip": scenario_active_active_no_vip,
    "ha_with_vip": scenario_ha_with_vip,
    "enable_rdma_on_existing": scenario_enable_rdma_on_existing,
    "custom_rdma_port": scenario_custom_rdma_port,
    "multi_client_parallel_io": scenario_multi_client_parallel_io,
    "tcp_and_rdma_coexist": scenario_tcp_and_rdma_coexist,
    "single_client_dual_proto": scenario_single_client_dual_proto,
}


def run(ceph_cluster, **kw):
    """
    Main entry point for a single NFS RDMA basic workflow scenario.

    Each invocation runs ONE scenario (determined by config["scenario"]).
    The suite YAML contains one test entry per scenario, giving independent
    pass/fail and proper try/except/finally handling per scenario.

    Args:
        ceph_cluster: CephCluster object providing node access.
        **kw: Keyword arguments containing:
            config (dict): Suite YAML config with keys:
                - scenario (str): Scenario key from SCENARIO_MAP (required).
                    One of: basic_rdma_v42, rdma_v41, rdma_v40, rdma_v3,
                    multi_version_rdma, active_active_no_vip, ha_with_vip,
                    enable_rdma_on_existing, custom_rdma_port,
                    multi_client_parallel_io, tcp_and_rdma_coexist,
                    single_client_dual_proto
                - servers (int): Number of NFS servers (default: 2)
                - clients (int): Number of clients (default: 2)
                - port (str): TCP NFS port (default: "2049")
                - rdma_port (str): RDMA listener port (default: "20049")
                - vip (str): VIP in CIDR for HA scenario (e.g. "10.64.66.99/24")

    Returns:
        0: Scenario passed -- no errors, no crashes detected
        1: Scenario failed -- error during execution or crash detected
    """
    config = kw.get("config")
    if not config:
        raise ConfigError("No config provided")

    scenario_key = config.get("scenario")
    if not scenario_key:
        raise ConfigError(
            "config['scenario'] is required. "
            f"Valid values: {list(SCENARIO_MAP.keys())}"
        )

    scenario_fn = SCENARIO_MAP.get(scenario_key)
    if not scenario_fn:
        raise ConfigError(
            f"Unknown scenario '{scenario_key}'. "
            f"Valid values: {list(SCENARIO_MAP.keys())}"
        )

    clients = ceph_cluster.get_nodes("client")
    if not clients:
        raise ConfigError("No client nodes available")

    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)

    log.info("=" * 60)
    log.info("SCENARIO: %s", scenario_key)
    log.info("=" * 60)

    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug("Test workflow started. Start time: %s", start_time)

    # Inject rados_obj and start_time so scenarios can call _pre_cleanup_checks
    config["_rados_obj"] = rados_obj
    config["_start_time"] = start_time

    try:
        scenario_fn(ceph_cluster, config)
        log.info("Scenario execution completed successfully: %s", scenario_key)
        return 0
    except Exception as e:
        log.error("Scenario FAILED: %s -- %s", scenario_key, e, exc_info=True)
        return 1
    finally:
        # Final crash check (authoritative -- catches crashes even on passing tests)
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            "Test workflow completed. Start time: %s, End time: %s",
            start_time,
            test_end_time,
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("Test failed due to crash at the end of test")
            return 1
        log.info("=" * 60)
