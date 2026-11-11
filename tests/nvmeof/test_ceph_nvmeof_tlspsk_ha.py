#!/usr/bin/env python3
"""
TS-07: TLS-PSK + HA (ANA Failover/Failback)

Tests that TLS-PSK authentication works correctly during NVMe-oF High Availability
failover and failback scenarios using ANA (Asymmetric Namespace Access).

Validates:
- Background FIO continues during ANA failover
- TLS enforced on surviving GW after failover
- PSK rotation while GW is degraded works
- Plain (non-TLS) connect refused on surviving GW
"""

import logging
import time

from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.daemon import Daemon
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.pool_workflows import PoolFunctions
from tests.nvmeof.workflows.fio import FIO
from tests.nvmeof.workflows.gateway_entities import (
    configure_hosts,
    configure_namespaces,
    configure_subsystems,
)
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.tlspsk import (
    add_host_with_psk,
    add_secure_listener,
    connect_with_tls,
    disconnect_nqn,
    get_host_nqn,
    rotate_psk,
    setup_initiator_tls_psk,
)
from utility.log import Log

log = Log(__name__)


def _get_gw_nodes(ceph_cluster):
    """Return all NVMe-oF gateway nodes."""
    return ceph_cluster.get_nodes(role="nvmeof-gw") or ceph_cluster.get_nodes(
        role="gateway"
    )


def _get_initiator_node(ceph_cluster):
    """Return the initiator/client node."""
    return ceph_cluster.get_nodes(role="client")[0]


def _get_subsystem_nqn(config, gw_nodes):
    """Build unique subsystem NQN for this test."""
    return config.get("subsystem_nqn", f"nqn.2016-06.io.spdk:cnode1.tlspsk_ha")


def _get_pool_name(config):
    return config.get("pool", "rbd")


def _get_image_name(config):
    return config.get("image", "tlspsk_ha_img")


def _get_image_size(config):
    return config.get("size", "10G")


def _get_trsvcid(config):
    return config.get("trsvcid", "4420")


def _setup_tlspsk_subsystem(
    ceph_cluster,
    config,
    gw_nodes,
    initiator_node,
    pool,
    image,
    size,
    trsvcid,
    nqn,
):
    """
    Complete TLS-PSK subsystem setup per IBM doc:
    1. Initiator: install ktls-utils, configure tlshd, gen-tls-key, load nvme-tcp, insert key
    2. GW: subsystem add --secure_listeners
    3. GW: namespace add
    4. GW: listener add --secure (per GW)
    5. GW: host add --psk <KEY_DATA>
    Returns dict with psk_info, hostnqn, traddrs
    """
    log.info("=== Phase 1: Initiator TLS-PSK setup ===")
    psk_info = setup_initiator_tls_psk(
        initiator_node,
        nqn,
        gw_nodes,
        trsvcid,
    )
    hostnqn = psk_info["hostnqn"]
    key_data = psk_info["key_data"]
    log.info(f"Initiator host NQN: {hostnqn}, PSK generated")

    log.info("=== Phase 2: Create secure subsystem ===")
    subsystem_config = {
        "subsystem": {
            "nqn": nqn,
            "secure_listener": True,
            "allow_any_host": False,
            "serial": "tlspsk-ha-serial",
            "model": "tlspsk-ha-model",
        }
    }
    configure_subsystems(gw_nodes[0], subsystem_config)

    log.info("=== Phase 3: Add namespace ===")
    namespace_config = {
        "namespace": {
            "nqn": nqn,
            "pool": pool,
            "image": image,
            "size": size,
            "create_image": True,
        }
    }
    configure_namespaces(gw_nodes[0], namespace_config)

    log.info("=== Phase 4: Add secure listeners on all GWs ===")
    traddrs = {}
    for gw in gw_nodes:
        gw_ip = gw.ip_address
        traddrs[gw.node_id] = gw_ip
        add_secure_listener(gw, nqn, gw_ip, trsvcid)
        log.info(f"Added secure listener on {gw.node_id} ({gw_ip}:{trsvcid})")

    log.info("=== Phase 5: Add host with PSK ===")
    add_host_with_psk(gw_nodes[0], nqn, hostnqn, key_data)
    log.info(f"Host {hostnqn} added with PSK")

    return {
        "psk_info": psk_info,
        "hostnqn": hostnqn,
        "traddrs": traddrs,
        "nqn": nqn,
    }


def _connect_all_paths(initiator_node, traddrs, trsvcid, nqn):
    """Connect initiator to all GW paths with TLS."""
    for gw_id, traddr in traddrs.items():
        connect_with_tls(initiator_node, traddr, trsvcid, nqn)
        log.info(f"Connected to {gw_id} ({traddr}:{trsvcid}) with TLS")


def _disconnect_all_paths(initiator_node, traddrs, nqn):
    """Disconnect initiator from all GW paths."""
    for gw_id, traddr in traddrs.items():
        try:
            disconnect_nqn(initiator_node, nqn)
            log.info(f"Disconnected from {gw_id} ({traddr})")
        except Exception as e:
            log.warning(f"Disconnect from {gw_id} failed (may already be down): {e}")


def _start_background_fio(initiator_node, nqn, runtime=300):
    """Start background FIO on all connected NVMe devices."""
    devices = NVMeInitiator(initiator_node).list_devices(nqn)
    if not devices:
        raise RuntimeError("No NVMe devices found for FIO")
    fio = FIO(initiator_node)
    job = fio.run_fio(
        devices=devices,
        rw="randwrite",
        bs="4k",
        numjobs=4,
        iodepth=32,
        runtime=runtime,
        time_based=True,
        direct=1,
        ioengine="libaio",
        group_reporting=True,
        name="tlspsk_ha_fio",
    )
    log.info(f"Background FIO started: {job}")
    return fio, job


def _check_fio_errors(fio, job):
    """Check if FIO job has any errors."""
    if fio and job:
        result = fio.get_fio_result(job)
        if result and result.get("jobs"):
            for j in result["jobs"]:
                if j.get("error", 0) != 0:
                    return False, f"FIO error: {j['error']}"
    return True, "FIO completed without errors"


def _plain_connect_must_fail(initiator_node, traddr, trsvcid, nqn):
    """Verify plain (non-TLS) connect is refused."""
    initiator = NVMeInitiator(initiator_node)
    try:
        # Doc-correct flags: --transport tcp --traddr --trsvcid --nqn (NO --tls)
        initiator.connect(
            transport="tcp",
            traddr=traddr,
            trsvcid=trsvcid,
            nqn=nqn,
        )
        # If connect succeeded, disconnect and fail
        initiator.disconnect(nqn=nqn)
        return False, f"Plain connect unexpectedly succeeded on {traddr}"
    except Exception as e:
        log.info(f"Plain connect correctly refused on {traddr}: {e}")
        return True, "Plain connect refused as expected"


def _stop_gw_daemon(gw_node):
    """Stop the nvmeof gateway daemon on a node."""
    daemon = Daemon(gw_node)
    daemon.stop("nvmeof")
    log.info(f"Stopped nvmeof daemon on {gw_node.node_id}")


def _start_gw_daemon(gw_node):
    """Start the nvmeof gateway daemon on a node."""
    daemon = Daemon(gw_node)
    daemon.start("nvmeof")
    log.info(f"Started nvmeof daemon on {gw_node.node_id}")


def _wait_for_gw_daemons_ready(gw_nodes, timeout=120):
    """Wait for all nvmeof daemons to be running."""
    start = time.time()
    while time.time() - start < timeout:
        all_running = True
        for gw in gw_nodes:
            out, _, _ = gw.exec_command(
                cmd="ceph orch ps --daemon_type nvmeof --format json"
            )
            import json

            try:
                daemons = json.loads(out)
                for d in daemons:
                    if d.get("status") != "running":
                        all_running = False
                        break
            except Exception:
                all_running = False
                break
        if all_running:
            log.info("All GW daemons are running")
            return True
        time.sleep(5)
    raise TimeoutError("GW daemons did not become ready in time")


def run(ceph_cluster, **config):
    """
    TS-07: TLS-PSK + HA Failover/Failback

    Test flow:
    1. Deploy NVMe-oF service with TLS-PSK (enable_encryption: True)
    2. Full doc TLS-PSK setup (initiator + GW subsystem/namespace/listener/host --psk)
    3. Connect initiator to all GWs with TLS
    4. Start background FIO
    5. Stop one GW daemon (simulate failure)
    6. Verify ANA failover: FIO continues on surviving GW, TLS still enforced
    7. Rotate PSK while GW is down
    8. Restart failed GW
    9. Verify failback: FIO continues, TLS enforced on all GWs
    10. Verify plain connect refused on all GWs
    """
    log.info("Starting TS-07: TLS-PSK + HA Failover/Failback")

    # --- 1. Deploy NVMe-oF service with TLS-PSK ---
    gw_nodes = _get_gw_nodes(ceph_cluster)
    if len(gw_nodes) < 2:
        log.error("Need at least 2 GW nodes for HA test")
        return 1

    initiator_node = _get_initiator_node(ceph_cluster)
    nqn = _get_subsystem_nqn(config, gw_nodes)
    pool = _get_pool_name(config)
    image = _get_image_name(config)
    size = _get_image_size(config)
    trsvcid = _get_trsvcid(config)

    # Deploy NVMe-oF service with TLS-PSK enabled (enable_encryption: True)
    nvme_service = NVMeService(config, ceph_cluster)
    # Ensure tls_psk config is set for spec generation
    config["tls_psk"] = True
    config["enable_encryption"] = True
    deploy_config = nvme_service._create_spec_deployment_config()
    from tests.nvmeof import test_nvmeof

    test_nvmeof.run(ceph_cluster, **deploy_config)
    log.info("NVMe-oF service deployed with TLS-PSK enabled")

    # --- 2. Full TLS-PSK setup per IBM doc ---
    setup_result = _setup_tlspsk_subsystem(
        ceph_cluster,
        config,
        gw_nodes,
        initiator_node,
        pool,
        image,
        size,
        trsvcid,
        nqn,
    )
    hostnqn = setup_result["hostnqn"]
    traddrs = setup_result["traddrs"]
    psk_info = setup_result["psk_info"]

    # --- 3. Connect initiator to all GWs with TLS ---
    _connect_all_paths(initiator_node, traddrs, trsvcid, nqn)

    # --- 4. Start background FIO ---
    fio, job = _start_background_fio(initiator_node, nqn, runtime=300)

    # --- 5. Stop one GW (simulate failure) ---
    primary_gw = gw_nodes[0]
    secondary_gw = gw_nodes[1]
    _stop_gw_daemon(primary_gw)

    # --- 6. Verify ANA failover ---
    ha = HighAvailability(ceph_cluster, config)
    # Wait for ANA failover to complete
    time.sleep(10)
    ha.check_gateway_availability(initiator_node, nqn)
    log.info("ANA failover completed, I/O continuing on surviving GW")

    # Verify TLS still enforced on surviving GW
    surviving_traddr = traddrs[secondary_gw.node_id]
    ok, msg = _plain_connect_must_fail(initiator_node, surviving_traddr, trsvcid, nqn)
    if not ok:
        log.error(msg)
        fio.stop_fio(job)
        return 1
    log.info("TLS enforced on surviving GW after failover")

    # Verify FIO still running without errors
    time.sleep(15)
    ok, msg = _check_fio_errors(fio, job)
    if not ok:
        log.error(f"FIO errors during failover: {msg}")
        fio.stop_fio(job)
        return 1
    log.info("FIO continuing without errors during failover")

    # --- 7. Rotate PSK while primary GW is down ---
    log.info("Rotating PSK while primary GW is down")
    rotate_psk(
        initiator_node,
        secondary_gw,  # Use surviving GW for rotation
        nqn,
        hostnqn,
        [secondary_gw],  # Only surviving GW
        trsvcid=trsvcid,
    )
    log.info("PSK rotation completed on surviving GW")

    # Reconnect with new PSK on surviving GW
    _disconnect_all_paths(initiator_node, {secondary_gw.node_id: surviving_traddr}, nqn)
    connect_with_tls(initiator_node, surviving_traddr, trsvcid, nqn)
    log.info("Reconnected to surviving GW with new PSK")

    # Verify plain connect still refused after rotation
    ok, msg = _plain_connect_must_fail(initiator_node, surviving_traddr, trsvcid, nqn)
    if not ok:
        log.error(msg)
        fio.stop_fio(job)
        return 1

    # --- 8. Restart failed GW ---
    _start_gw_daemon(primary_gw)
    _wait_for_gw_daemons_ready(gw_nodes)

    # The restarted GW should have the new PSK from OMAP replay
    # Reconnect to primary GW with new PSK
    primary_traddr = traddrs[primary_gw.node_id]
    connect_with_tls(initiator_node, primary_traddr, trsvcid, nqn)
    log.info("Reconnected to restarted primary GW with new PSK")

    # --- 9. Verify failback and TLS on all GWs ---
    ha.check_gateway_availability(initiator_node, nqn)
    log.info("Failback completed, I/O on all GWs")

    for gw_id, traddr in traddrs.items():
        ok, msg = _plain_connect_must_fail(initiator_node, traddr, trsvcid, nqn)
        if not ok:
            log.error(f"TLS not enforced on {gw_id} after failback: {msg}")
            fio.stop_fio(job)
            return 1
    log.info("TLS enforced on all GWs after failback")

    # Final FIO check
    time.sleep(10)
    ok, msg = _check_fio_errors(fio, job)
    if not ok:
        log.error(f"FIO errors after failback: {msg}")
        fio.stop_fio(job)
        return 1

    # --- 10. Cleanup ---
    fio.stop_fio(job)
    _disconnect_all_paths(initiator_node, traddrs, nqn)
    log.info("TS-07 PASSED: TLS-PSK + HA failover/failback works correctly")
    return 0
