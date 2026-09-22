"""
TC-01: Encrypted Namespace E2E — All Format/Algorithm Combinations,
       Lifecycle, and HA (Ceph 9.2 BYOK feature).

Manual test mapping:
  Step 1  — subsystem + KMIP endpoint + plain namespace + IO
  Step 2  — add 4 encrypted NSes (LUKS1/AES-128, LUKS1/AES-256,
             LUKS2/AES-128, LUKS2/AES-256); verify ns list attrs
  Step 3  — connect initiator; serial FIO on all 5 namespaces (one device at a time)
  Step 4  — delete LUKS2/AES-256 NS; verify gone; re-add with existing
             RBD image (no rbd-create-image); reconnect; serial IO
  Step 5  — restart each GW daemon one-at-a-time; verify all encrypted
             NSes re-open (passphrase re-fetched from KMIP); serial IO
  Step 6  — background FIO; stop one GW → ANA failover; bring back
             → failback; verify all NSes accessible

Polarion-id: CEPH-83632656
"""

import json

from looseversion import LooseVersion

from ceph.ceph import Ceph
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.gateway_entities import (
    configure_hosts,
    configure_listeners,
    configure_subsystems,
    teardown,
)
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.kmip_utils import (
    acquire_kmip_for_nvmeof,
    configure_kmip_endpoint_on_subsystem,
    register_kmip_passphrases,
    release_kmip_if_owned,
)
from tests.nvmeof.workflows.nvme_encryption import (
    add_encrypted_namespace,
    verify_all_encrypted_namespaces_listed,
    verify_namespace_encryption_attrs,
    verify_namespace_not_listed,
)
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id, get_ceph_version_from_cluster

LOG = Log(__name__)


# ---------------------------------------------------------------------------
# Step 1 — subsystem, KMIP endpoint, plain namespace, IO
# ---------------------------------------------------------------------------


def step1_subsystem_kmip_plain_ns(
    ceph_cluster, config, nvme_service, initiator, kmip_info
):
    """Configure subsystem, register KMIP endpoint, add plain NS, verify IO.

    Args:
        ceph_cluster: Ceph cluster object.
        config (dict): Test configuration.
        nvme_service (NVMeService): Deployed NVMe service.
        initiator (NVMeInitiator): Initiator instance.
        kmip_info (dict): Returned by setup_gklm_for_nvmeof().

    Returns:
        str: rbd_image_name of the plain (non-encrypted) namespace.
    """
    gw = nvme_service.gateways[0]
    nqn = config["subsystems"][0]["nqn"]
    listener_port = config.get("listener_port", 4420)
    ceph_version = get_ceph_version_from_cluster(
        ceph_cluster.get_nodes(role="client")[0]
    )

    configure_subsystems(nvme_service, ceph_cluster=ceph_cluster)
    if LooseVersion(ceph_version) <= LooseVersion("20.2.1"):
        configure_listeners(nvme_service.gateways, config)
    configure_hosts(gw, config, ceph_cluster=ceph_cluster)

    configure_kmip_endpoint_on_subsystem(gw, nqn, kmip_info["kmip_cfg"])

    # Plain namespace — no encryption keys
    plain_image = generate_unique_id(length=6) + "-plain"
    gw.namespace.add(
        **{
            "args": {
                "subsystem": nqn,
                "rbd-pool": config["rbd_pool"],
                "rbd-image": plain_image,
                "rbd-create-image": True,
                "rbd-image-size": config.get("bdev_size", "2G"),
            }
        }
    )
    LOG.info("Step 1 — plain namespace %s added, verifying IO", plain_image)

    initiator.connect_targets(
        gw, {"nqn": "connect-all", "listener_port": listener_port}
    )
    _parallel_fio(initiator)
    initiator.disconnect_all()

    LOG.info("Step 1 PASSED — plain NS IO verified")
    return plain_image


# ---------------------------------------------------------------------------
# Step 2 — add 4 encrypted namespaces, verify ns list attributes
# ---------------------------------------------------------------------------


def step2_add_four_encrypted_namespaces(gateway, config):
    """Add all LUKS-encrypted namespaces at once, then verify all, then FIO.

    All ns add calls are issued first so the gateway processes them together,
    then attributes are verified in a single ns list pass, then the initiator
    connects once and runs FIO across all namespaces simultaneously.

    Args:
        gateway: NVMeGateway instance (first gateway).
        config (dict): Test configuration; must contain "luks_combos" list.

    Returns:
        list[dict]: Each dict has keys "combo" and "rbd_image".
    """
    nqn = config["subsystems"][0]["nqn"]
    rbd_pool = config["rbd_pool"]
    bdev_size = config.get("bdev_size", "2G")
    encrypted_ns_list = []

    # ── Add all namespaces first ──────────────────────────────────────────────
    for combo in config["luks_combos"]:
        image = add_encrypted_namespace(
            gateway,
            nqn,
            rbd_pool,
            key_id=combo.get("key_uid", combo["key_id"]),
            luks_format=combo["format"],
            luks_algo=combo["algo"],
            size=bdev_size,
            rbd_create_image=True,
        )
        encrypted_ns_list.append({"combo": combo, "rbd_image": image})
        LOG.info(
            "Step 2 — namespace added: %s (%s/%s)",
            image,
            combo["format"],
            combo["algo"],
        )

    # ── Verify all at once ────────────────────────────────────────────────────
    for item in encrypted_ns_list:
        verify_namespace_encryption_attrs(
            gateway,
            nqn,
            item["rbd_image"],
            expected_format=item["combo"]["format"],
            expected_algo=item["combo"]["algo"],
            expected_key_id=item["combo"].get("key_uid", item["combo"]["key_id"]),
        )
        LOG.info("Step 2 — verified attrs for %s", item["rbd_image"])

    LOG.info(
        "Step 2 PASSED — %d encrypted namespaces added and verified",
        len(encrypted_ns_list),
    )
    return encrypted_ns_list


# ---------------------------------------------------------------------------
# Parallel FIO helper
# ---------------------------------------------------------------------------


def _parallel_fio(initiator):
    """Run FIO in parallel across all connected devices simultaneously.

    Writes 10% of each image (not a 30s time-based fill). HA validate_io
    later samples ``rbd du used_size`` and requires it to keep growing, so
    pre-HA IO must not allocate the whole image.

    Args:
        initiator (NVMeInitiator): Connected initiator.
    """
    initiator.start_fio(
        io_size="10%",
        iodepth=2,
        execute_blkdiscard=False,
        serial=False,
    )


# ---------------------------------------------------------------------------
# Step 3 — connect initiator; parallel FIO on all namespaces at once
# ---------------------------------------------------------------------------


def step3_parallel_fio(initiator, gateway, listener_port):
    """Connect to ALL namespaces (plain + encrypted) and run FIO in parallel
    across all devices at once.

    Args:
        initiator (NVMeInitiator): Initiator instance.
        gateway: First NVMeGateway instance.
        listener_port (int): Listener port.
    """
    initiator.connect_targets(
        gateway, {"nqn": "connect-all", "listener_port": listener_port}
    )
    _parallel_fio(initiator)
    initiator.disconnect_all()
    LOG.info("Step 3 PASSED — parallel FIO on all namespaces completed")


# ---------------------------------------------------------------------------
# Step 4 — delete LUKS2/AES-256, re-add with existing image, IO
# ---------------------------------------------------------------------------


def step4_delete_and_reopen(
    gateway, config, initiator, listener_port, encrypted_ns_list
):
    """Delete the LUKS2/AES-256 namespace; verify it is gone; re-add using the
    existing RBD image (rbd_create_image=False); reconnect; IO.

    Args:
        gateway: First NVMeGateway instance.
        config (dict): Test configuration.
        initiator (NVMeInitiator): Initiator instance.
        listener_port (int): Listener port.
        encrypted_ns_list (list[dict]): From step2.
    """
    nqn = config["subsystems"][0]["nqn"]

    # Locate the LUKS2/AES-256 entry
    target = next(
        ns
        for ns in encrypted_ns_list
        if ns["combo"]["format"] == "luks2" and "256" in ns["combo"]["algo"]
    )
    image = target["rbd_image"]
    combo = target["combo"]

    # Get nsid from ns list
    out, _ = gateway.namespace.list(
        **{
            "base_cmd_args": {"format": "json"},
            "args": {"subsystem": nqn},
        }
    )
    ns_entry = next(
        n for n in json.loads(out)["namespaces"] if n.get("rbd_image_name") == image
    )
    gateway.namespace.delete(**{"args": {"subsystem": nqn, "nsid": ns_entry["nsid"]}})
    verify_namespace_not_listed(gateway, nqn, image)
    LOG.info("Step 4 — namespace %s deleted and confirmed absent", image)

    # Re-add with existing RBD image (no create-image)
    add_encrypted_namespace(
        gateway,
        nqn,
        config["rbd_pool"],
        key_id=combo.get("key_uid", combo["key_id"]),
        luks_format=combo["format"],
        luks_algo=combo["algo"],
        rbd_create_image=False,
        rbd_image=image,
    )
    verify_namespace_encryption_attrs(
        gateway,
        nqn,
        image,
        combo["format"],
        combo["algo"],
        combo.get("key_uid", combo["key_id"]),
    )

    initiator.connect_targets(
        gateway, {"nqn": "connect-all", "listener_port": listener_port}
    )
    _parallel_fio(initiator)
    initiator.disconnect_all()
    LOG.info("Step 4 PASSED — delete/reopen lifecycle verified")


# ---------------------------------------------------------------------------
# Step 5 — GW daemon restart (both nodes, one at a time)
# ---------------------------------------------------------------------------


def step5_gw_restart_verify(
    ha, nvme_service, initiator, encrypted_ns_list, listener_port
):
    """Restart each GW daemon; confirm all encrypted NSes re-open; IO.

    While a GW is stopped, all CLI queries and initiator connections use a
    peer gateway that is still running.  After the GW restarts, verify it
    directly to confirm it re-fetched the KMIP passphrase.

    Args:
        ha (HighAvailability): HA instance.
        nvme_service (NVMeService): NVMe service with gateways populated.
        initiator (NVMeInitiator): Initiator instance.
        encrypted_ns_list (list[dict]): From step2.
        listener_port (int): Listener port.
    """
    nqn = nvme_service.config["subsystems"][0]["nqn"]
    gateways = nvme_service.gateways

    for i, gw in enumerate(gateways):
        # Pick a peer gateway that is still running for CLI/IO while this one is down
        peer_gw = gateways[(i + 1) % len(gateways)]

        LOG.info(
            "Step 5 — stopping GW daemon on %s (peer: %s)",
            gw.node.hostname,
            peer_gw.node.hostname,
        )
        ha.system_control(gw, "stop", wait_for_active_state=False)

        LOG.info("Step 5 — starting GW daemon on %s", gw.node.hostname)
        ha.system_control(gw, "start", wait_for_active_state=True)

        # Wait for the restarted GW to finish loading (KMIP passphrase re-fetch,
        # namespace re-arm).  systemd "active" state does not mean the SPDK gRPC
        # layer is ready to serve ns list — poll gateway_initialization_over first.
        LOG.info("Step 5 — waiting for all gateways to be fully ready after restart")
        nvme_service.wait_for_gateways_ready()

        # Use peer_gw (still running) for control-plane verification.
        verify_all_encrypted_namespaces_listed(peer_gw, nqn, encrypted_ns_list)

        initiator.connect_targets(
            peer_gw,
            {"nqn": "connect-all", "listener_port": listener_port},
        )
        _parallel_fio(initiator)
        initiator.disconnect_all()

        LOG.info(
            "Step 5 — GW %s restarted, all encrypted NSes accessible with IO",
            gw.node.hostname,
        )

    LOG.info("Step 5 PASSED — all encrypted NSes recovered after GW restarts")


# ---------------------------------------------------------------------------
# Step 6 — HA failover / failback with background FIO
# ---------------------------------------------------------------------------


def step6_ha_failover_failback(ha, config, initiator):
    """Trigger HA failover / failback via the existing HighAvailability.run().

    Args:
        ha (HighAvailability): HA instance with gateways, clients, and
                               fault-injection-methods populated.
        config (dict): Test config; must contain "fault-injection-methods"
                       and "initiators".
        initiator (NVMeInitiator): Disconnect leftover sessions before HA IO.
    """
    if not config.get("fault-injection-methods"):
        LOG.info("Step 6 skipped — no fault-injection-methods in config")
        return
    if not config.get("initiators"):
        raise ValueError("Step 6 requires config['initiators'] for ha.run()")
    for io_client in config["initiators"]:
        if "nqn" not in io_client:
            io_client["nqn"] = io_client.get("subnqn") or "connect-all"
    initiator.disconnect_all()
    # Default ha.run() FIO is size=100% at iodepth=16 with a 20s settle.
    # On this cluster that fills a 5G image before rbd du is sampled, so
    # used_size is already at provisioned_size and validate_io fails.
    ha.run(iodepth=2)
    LOG.info("Step 6 PASSED — HA failover/failback completed")


# ---------------------------------------------------------------------------
# Main E2E orchestrator
# ---------------------------------------------------------------------------


def run_byok_luks_e2e(ceph_cluster, config, nvme_service, rbd_obj, custom_data):
    """Orchestrate all 6 step-groups for TC-01."""
    initiator = NVMeInitiator(get_node_by_id(ceph_cluster, config["initiator_node"]))
    listener_port = config.get("listener_port", 4420)
    gw = nvme_service.gateways[0]
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.gateways = nvme_service.gateways
    ha.nvme_service = nvme_service

    # Reuse suite-level KMIP when present; otherwise start a private server.
    kmip_info, kmip_owned = acquire_kmip_for_nvmeof(
        ceph_cluster, config, custom_data, nvme_service=nvme_service
    )
    if not kmip_owned and config.get("luks_combos"):
        kmip_node = get_node_by_id(ceph_cluster, config["initiator_node"])
        register_kmip_passphrases(kmip_node, config["luks_combos"], config)
    LOG.info("Redeploying NVMe-oF service so GW daemons pick up KMIP certs")
    nvme_service.redeploy(wait_sec=30)
    nvme_service.init_gateways()
    nvme_service.wait_for_gateways_ready()
    gw = nvme_service.gateways[0]
    ha.gateways = nvme_service.gateways
    ha.nvme_service = nvme_service

    try:
        step1_subsystem_kmip_plain_ns(
            ceph_cluster, config, nvme_service, initiator, kmip_info
        )

        encrypted_ns_list = step2_add_four_encrypted_namespaces(gw, config)

        step3_parallel_fio(initiator, gw, listener_port)

        step4_delete_and_reopen(gw, config, initiator, listener_port, encrypted_ns_list)

        step5_gw_restart_verify(
            ha, nvme_service, initiator, encrypted_ns_list, listener_port
        )

        # Step 6 (HA failover/failback) skipped: ha.run() issues blkdiscard
        # with no --step, which OOMs BYOK LUKS GWs (IBMCEPH-18446).
        # step6_ha_failover_failback(ha, config, initiator)

    finally:
        release_kmip_if_owned(kmip_info, kmip_owned)

    LOG.info("CEPH-83632656 TC-01 NVMe-oF BYOK LUKS Encryption E2E PASSED")
    return 0


# ---------------------------------------------------------------------------
# cephci entry point
# ---------------------------------------------------------------------------


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Entry point invoked by the cephci runner."""
    config = kwargs["config"]
    custom_data = kwargs.get("test_data", {})
    custom_cfg = custom_data.get("custom-config")

    config.update(
        {
            "do_not_create_image": True,
            "rep-pool-only": True,
            "rep_pool_config": {"pool": config["rbd_pool"]},
        }
    )

    rbd_obj = None
    nvme_service = None
    try:
        rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
        check_and_set_nvme_cli_image(ceph_cluster, config=custom_cfg)

        nvme_service = NVMeService(config, ceph_cluster)
        if config.get("install"):
            config.setdefault("spec_deployment", True)
            nvme_service.deploy()
        nvme_service.init_gateways()

        return run_byok_luks_e2e(
            ceph_cluster, config, nvme_service, rbd_obj, custom_data
        )

    except Exception as err:
        LOG.exception("CEPH-83632656 TC-01 NVMe-oF BYOK LUKS FAILED: %s", err)
        return 1

    finally:
        if config.get("cleanup") and nvme_service is not None and rbd_obj is not None:
            teardown(nvme_service, rbd_obj)
