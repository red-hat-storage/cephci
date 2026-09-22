"""
TC-03: Multiple KMIP Servers and KMIP Failover (Ceph 9.2 BYOK).

Objective:
  Validate that the NVMe-oF gateway can retrieve encryption keys from an
  alternate KMIP server configured on the subsystem when the primary KMIP
  server is unavailable.

Setup (performed inside this test):
  - KMIP-1 (port 5696) started on the initiator node (node10); key
    "key-id-failover" registered with a random passphrase.
  - KMIP-2 (port 5696) started on a DIFFERENT node (node1 — installer) so
    that both containers can use --network host without a port conflict.
    The same passphrase is loaded into KMIP-2 so that the gateway receives
    identical key material regardless of which server it queries.
  - Both KMIP server endpoints are registered on the NVMe-oF subsystem.
  - One encrypted namespace backed by a single LUKS2/AES-256 RBD image.
  - Two gateway nodes, one RHEL initiator.

Sub-scenarios (steps):
  1  — Both KMIP servers up: create encrypted namespace, FIO.  Verify normal IO.
  2  — Fail KMIP-1 (podman stop): delete + re-add namespace to force key
       re-fetch.  Verify gateway opens the image using KMIP-2.  IO succeeds.
  3  — Restore KMIP-1; Fail KMIP-2 (podman stop): restart the GW daemon to
       force passphrase re-fetch from scratch.  Verify namespace re-opens
       using KMIP-1.  IO succeeds.
  4  — Restore both servers: restart GW daemon for clean state, FIO to
       confirm.

Cleanup:
  - Delete namespace, remove RBD image.
  - Stop both KMIP containers, remove cert dirs from GW nodes.

Polarion-id: CEPH-83632974
"""

import json
import time

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
    _DUMMY_KMIP_PORT,
    _DUMMY_KMIP_PORT_2,
    _KMIP_CONTAINER_2_NAME,
    _KMIP_CONTAINER_NAME,
    acquire_kmip_for_nvmeof,
    configure_kmip_endpoint_on_subsystem,
    copy_kmip_certs_to_gw_nodes,
    extract_passphrase_map,
    register_kmip_passphrases,
    release_kmip_if_owned,
    setup_second_kmip_server,
    start_kmip_container,
    stop_kmip_container,
)
from tests.nvmeof.workflows.nvme_encryption import (
    add_encrypted_namespace,
    delete_and_readd_namespace,
    verify_gw_subsystem_healthy,
    verify_namespace_encryption_attrs,
)
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id, get_ceph_version_from_cluster

LOG = Log(__name__)

# LUKS format / algo used for the single encrypted namespace in this test
_LUKS_FORMAT = "luks2"
_LUKS_ALGO = "aes256"


# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------


def _setup_kmip_pair(ceph_cluster, config, custom_data, nvme_service):
    """Start KMIP-1, register the failover key, then start KMIP-2 with the
    same passphrase.

    Also copies both servers' certs to every GW node and registers both
    endpoints on the subsystem NQN.

    KMIP-1 runs on ``initiator_node`` (default: node10).
    KMIP-2 runs on ``kmip_node_2`` (default: node1 — the installer node).
    Using separate hosts eliminates the port-5696 conflict that occurs when
    both containers share ``--network host`` on the same machine.

    Args:
        ceph_cluster: Ceph cluster object.
        config (dict): Test configuration.
        custom_data (dict): kwargs["test_data"].
        nvme_service (NVMeService): Deployed (but not yet redeployed) service.

    Returns:
        dict: ``kmip_pair`` with keys:
            kmip1_info   — full kmip_info dict from setup_kmip_for_nvmeof()
            kmip2_cfg    — kmip_cfg dict for the second server
            key_entry    — single-element list entry with key_id / key_uid /
                           passphrase_file filled in
            failover_key_id   — str: key_id name
            failover_key_uid  — str: key UID returned by KMIP-1
            kmip_node    — node running KMIP-1 (initiator_node)
            kmip_node_2  — node running KMIP-2 (kmip_node_2 config key)
    """
    # ── KMIP-1: reuse suite server when present; otherwise start a private one.
    # luks_combos is empty — the failover key is registered manually below.
    kmip1_info, kmip1_owned = acquire_kmip_for_nvmeof(
        ceph_cluster, config, custom_data, nvme_service=nvme_service
    )

    kmip_node = get_node_by_id(ceph_cluster, config["initiator_node"])

    # ── Register the failover key on KMIP-1 ──────────────────────────────────
    failover_key_id = config.get("failover_key_id", "key-id-failover")
    key_entries = [{"key_id": failover_key_id}]
    register_kmip_passphrases(kmip_node, key_entries, config)
    key_entry = key_entries[0]  # now has key_uid + passphrase_file

    # ── Extract the raw passphrase so KMIP-2 can be seeded identically ───────
    passphrase_map = extract_passphrase_map(kmip1_info["kmip_cfg"], key_entries)
    uid_map = {entry["key_id"]: entry["key_uid"] for entry in key_entries}

    # ── KMIP-2: second server on a *different* node to avoid port conflict ────
    # Using the same node with --network host means both containers would race
    # for port 5696; putting KMIP-2 on a separate node eliminates that entirely.
    kmip2_node_id = config.get("kmip_node_2", "node1")
    kmip_node_2 = get_node_by_id(ceph_cluster, kmip2_node_id)
    LOG.info("KMIP-2 will run on %s (node id: %s)", kmip_node_2.hostname, kmip2_node_id)

    # KMIP-2 MUST use the same server_name as KMIP-1.  The GW identifies a
    # KMIP server by name; registering a second name on the same subsystem
    # returns EINVAL "no other server is allowed".  Using the same name adds
    # a second endpoint (different IP/port) under the existing server entry.
    kmip2_server_name = kmip1_info["kmip_cfg"]["server_name"]
    LOG.info("KMIP-2 will use server_name=%s (same as KMIP-1)", kmip2_server_name)
    kmip2_cfg = setup_second_kmip_server(
        kmip_node_2,
        config,
        shared_passphrase_map=passphrase_map,
        server_name=kmip2_server_name,
        uid_map=uid_map,
    )

    # ── Distribute KMIP-2 certs to every GW node ─────────────────────────────
    copy_kmip_certs_to_gw_nodes(ceph_cluster, kmip2_cfg, nvme_service=nvme_service)
    LOG.info("Both KMIP servers are running; certs distributed to all GW nodes.")

    return {
        "kmip1_info": kmip1_info,
        "kmip2_cfg": kmip2_cfg,
        "key_entry": key_entry,
        "failover_key_id": failover_key_id,
        "failover_key_uid": key_entry["key_uid"],
        "kmip_node": kmip_node,
        "kmip_node_2": kmip_node_2,
        "kmip1_owned": kmip1_owned,
    }


def _setup_subsystem_and_namespace(ceph_cluster, config, nvme_service, kmip_pair):
    """Configure the subsystem, register both KMIP endpoints, and create the
    encrypted namespace.

    Args:
        ceph_cluster: Ceph cluster object.
        config (dict): Test configuration.
        nvme_service (NVMeService): Service with gateways initialised.
        kmip_pair (dict): From :func:`_setup_kmip_pair`.

    Returns:
        tuple: ``(gw, nqn, rbd_image, key_uid)``
            gw        — first NVMeGateway
            nqn       — subsystem NQN string
            rbd_image — created/named RBD image backing the namespace
            key_uid   — KMIP UID string used for the namespace
    """
    gw = nvme_service.gateways[0]
    nqn = config["subsystems"][0]["nqn"]
    key_uid = kmip_pair["failover_key_uid"]
    rbd_pool = config["rbd_pool"]
    bdev_size = config.get("bdev_size", "2G")

    ceph_version = get_ceph_version_from_cluster(
        ceph_cluster.get_nodes(role="client")[0]
    )

    # Subsystem + listeners + hosts
    configure_subsystems(nvme_service, ceph_cluster=ceph_cluster)
    if LooseVersion(ceph_version) <= LooseVersion("20.2.1"):
        configure_listeners(nvme_service.gateways, config)
    configure_hosts(gw, config, ceph_cluster=ceph_cluster)

    # Register KMIP-1 endpoint
    LOG.info("Registering KMIP-1 endpoint on subsystem %s", nqn)
    configure_kmip_endpoint_on_subsystem(gw, nqn, kmip_pair["kmip1_info"]["kmip_cfg"])

    # Register KMIP-2 endpoint
    LOG.info("Registering KMIP-2 endpoint on subsystem %s", nqn)
    configure_kmip_endpoint_on_subsystem(gw, nqn, kmip_pair["kmip2_cfg"])

    # Create the encrypted namespace (GW creates the RBD image).
    # Pass the pre-generated name explicitly so add_encrypted_namespace uses it
    # and the returned name always matches what we verify/delete later.
    uid = generate_unique_id(length=6)
    rbd_image = f"{uid}-failover"
    LOG.info(
        "Creating encrypted namespace: image=%s key_uid=%s format=%s algo=%s",
        rbd_image,
        key_uid,
        _LUKS_FORMAT,
        _LUKS_ALGO,
    )
    rbd_image = add_encrypted_namespace(
        gw,
        nqn,
        rbd_pool,
        key_id=key_uid,
        luks_format=_LUKS_FORMAT,
        luks_algo=_LUKS_ALGO,
        size=bdev_size,
        rbd_create_image=True,
        rbd_image=rbd_image,
    )

    # Verify attrs
    verify_namespace_encryption_attrs(
        gw, nqn, rbd_image, _LUKS_FORMAT, _LUKS_ALGO, key_uid
    )

    LOG.info("Setup complete: namespace %s created with two KMIP endpoints.", rbd_image)
    return gw, nqn, rbd_image, key_uid


# ---------------------------------------------------------------------------
# Step functions
# ---------------------------------------------------------------------------


def step1_both_servers_up_io(initiator, gw, listener_port):
    """Step 1 — both KMIP servers up; baseline FIO verify.

    Connects the initiator and runs FIO to establish the baseline IO path.

    Args:
        initiator (NVMeInitiator): Initiator instance.
        gw: First NVMeGateway.
        listener_port (int): NVMe listener port.
    """
    LOG.info("Step 1 — connecting initiator with both KMIP servers available")
    initiator.connect_targets(
        gw, {"nqn": "connect-all", "listener_port": listener_port}
    )
    initiator.start_fio(io_size="10%", execute_blkdiscard=False)
    initiator.disconnect_all()
    LOG.info("Step 1 PASSED — baseline IO succeeded with both KMIP servers up")


def step2_fail_kmip1_verify_kmip2_failover(
    initiator, gw, nqn, config, kmip_pair, listener_port
):
    """Step 2 — stop KMIP-1; verify the GW falls back to KMIP-2.

    Actions:
      1. Stop the KMIP-1 container (``podman stop``).
      2. Delete the namespace and re-add it with the same RBD image — this
         forces the GW to perform a fresh passphrase fetch from KMIP.
      3. Connect initiator and run FIO.
      4. Verify the namespace is still listed with the correct key attributes
         (confirming the passphrase came from KMIP-2).
      5. Verify the GW subsystem is healthy.

    Args:
        initiator (NVMeInitiator): Initiator instance.
        gw: First NVMeGateway.
        nqn (str): Subsystem NQN.
        config (dict): Test config.
        kmip_pair (dict): From :func:`_setup_kmip_pair`.
        listener_port (int): NVMe listener port.

    Returns:
        str: ``rbd_image`` name (needed by later steps to re-verify attrs).
    """
    kmip_node = kmip_pair["kmip_node"]
    rbd_image = config["_rbd_image"]
    key_uid = kmip_pair["failover_key_uid"]

    LOG.info("Step 2 — stopping KMIP-1 container to simulate primary failure")
    stop_kmip_container(kmip_node, _KMIP_CONTAINER_NAME)

    # Give the OS a moment to mark the port closed
    time.sleep(3)

    LOG.info(
        "Step 2 — delete + re-add namespace %s to force passphrase re-fetch",
        rbd_image,
    )
    delete_and_readd_namespace(
        gw,
        nqn,
        rbd_pool=config["rbd_pool"],
        rbd_image=rbd_image,
        key_id=key_uid,
        luks_format=_LUKS_FORMAT,
        luks_algo=_LUKS_ALGO,
    )

    # Verify the namespace opened (GW used KMIP-2)
    verify_namespace_encryption_attrs(
        gw, nqn, rbd_image, _LUKS_FORMAT, _LUKS_ALGO, key_uid
    )
    verify_gw_subsystem_healthy(gw, nqn)

    LOG.info("Step 2 — connecting initiator; IO must succeed via KMIP-2")
    initiator.connect_targets(
        gw, {"nqn": "connect-all", "listener_port": listener_port}
    )
    initiator.start_fio(io_size="100%", execute_blkdiscard=False)
    initiator.disconnect_all()

    LOG.info(
        "Step 2 PASSED — namespace opened and IO succeeded with KMIP-1 offline "
        "(gateway used KMIP-2)"
    )
    return rbd_image


def step3_restore_kmip1_fail_kmip2_gw_restart(
    ha, nvme_service, initiator, gw, nqn, config, kmip_pair, listener_port
):
    """Step 3 — restore KMIP-1; stop KMIP-2; restart GW daemon; verify IO.

    Actions:
      1. Start KMIP-1 container again.
      2. Stop KMIP-2 container.
      3. Restart each GW daemon (``systemctl stop`` then ``start``) to force a
         full passphrase re-fetch from KMIP servers.
      4. Verify namespace is still listed with the expected key attributes.
      5. Connect initiator, run FIO.

    This proves the GW re-acquires the passphrase from KMIP-1 after restart,
    even though KMIP-2 is now unavailable.

    Args:
        ha (HighAvailability): HA instance.
        nvme_service (NVMeService): NVMe service with gateways.
        initiator (NVMeInitiator): Initiator instance.
        gw: Current first NVMeGateway.
        nqn (str): Subsystem NQN.
        config (dict): Test config.
        kmip_pair (dict): From :func:`_setup_kmip_pair`.
        listener_port (int): NVMe listener port.
    """
    kmip_node = kmip_pair["kmip_node"]
    kmip_node_2 = kmip_pair["kmip_node_2"]
    rbd_image = config["_rbd_image"]
    key_uid = kmip_pair["failover_key_uid"]

    LOG.info("Step 3 — restoring KMIP-1")
    start_kmip_container(kmip_node, _KMIP_CONTAINER_NAME, _DUMMY_KMIP_PORT)

    LOG.info("Step 3 — stopping KMIP-2 to simulate secondary failure")
    stop_kmip_container(kmip_node_2, _KMIP_CONTAINER_2_NAME)
    time.sleep(3)

    # Restart each GW daemon one at a time; then reconnect and run FIO.
    gateways = nvme_service.gateways
    for i, each_gw in enumerate(gateways):
        peer_gw = gateways[(i + 1) % len(gateways)]
        LOG.info(
            "Step 3 — restarting GW daemon on %s (peer: %s)",
            each_gw.node.hostname,
            peer_gw.node.hostname,
        )
        ha.system_control(each_gw, "stop", wait_for_active_state=False)
        ha.system_control(each_gw, "start", wait_for_active_state=True)
        nvme_service.wait_for_gateways_ready()

        # Verify via the peer while the just-restarted GW is warming up
        verify_namespace_encryption_attrs(
            peer_gw, nqn, rbd_image, _LUKS_FORMAT, _LUKS_ALGO, key_uid
        )
        verify_gw_subsystem_healthy(peer_gw, nqn)
        LOG.info(
            "Step 3 — GW %s restarted; namespace re-verified via peer.",
            each_gw.node.hostname,
        )

    initiator.connect_targets(
        gw, {"nqn": "connect-all", "listener_port": listener_port}
    )
    initiator.start_fio(io_size="100%", execute_blkdiscard=False)
    initiator.disconnect_all()

    LOG.info("Step 3 PASSED — namespace re-opened after GW restart with KMIP-2 offline")


def step4_restore_both_clean_state(
    ha, nvme_service, initiator, gw, nqn, config, kmip_pair, listener_port
):
    """Step 4 — restore both servers; verify clean final state.

    Actions:
      1. Start KMIP-2 container again.
      2. Restart all GW daemons to confirm no residual failure state.
      3. Verify namespace listed, FIO.

    Args:
        ha (HighAvailability): HA instance.
        nvme_service (NVMeService): NVMe service with gateways.
        initiator (NVMeInitiator): Initiator instance.
        gw: Current first NVMeGateway.
        nqn (str): Subsystem NQN.
        config (dict): Test config.
        kmip_pair (dict): From :func:`_setup_kmip_pair`.
        listener_port (int): NVMe listener port.
    """
    kmip_node_2 = kmip_pair["kmip_node_2"]
    rbd_image = config["_rbd_image"]
    key_uid = kmip_pair["failover_key_uid"]
    port2 = config.get("kmip_port_2", _DUMMY_KMIP_PORT_2)

    LOG.info("Step 4 — restoring KMIP-2 to bring both servers back online")
    start_kmip_container(kmip_node_2, _KMIP_CONTAINER_2_NAME, port2)

    # One final GW restart to confirm clean startup with both servers available.
    gateways = nvme_service.gateways
    for i, each_gw in enumerate(gateways):
        peer_gw = gateways[(i + 1) % len(gateways)]
        LOG.info(
            "Step 4 — restarting GW %s for final clean state check",
            each_gw.node.hostname,
        )
        ha.system_control(each_gw, "stop", wait_for_active_state=False)
        ha.system_control(each_gw, "start", wait_for_active_state=True)
        nvme_service.wait_for_gateways_ready()

        verify_namespace_encryption_attrs(
            peer_gw, nqn, rbd_image, _LUKS_FORMAT, _LUKS_ALGO, key_uid
        )
        verify_gw_subsystem_healthy(peer_gw, nqn)

    initiator.connect_targets(
        gw, {"nqn": "connect-all", "listener_port": listener_port}
    )
    initiator.start_fio(io_size="100%", execute_blkdiscard=False)
    initiator.disconnect_all()

    LOG.info(
        "Step 4 PASSED — clean final state confirmed; both KMIP servers back online"
    )


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


def teardown_kmip_failover(ceph_cluster, config, rbd_image, gw, nqn, kmip_pair):
    """Remove the namespace, RBD image, and both KMIP containers.

    Args:
        ceph_cluster: Ceph cluster object.
        config (dict): Test config.
        rbd_image (str): Image name to remove.
        gw: NVMeGateway for namespace delete.
        nqn (str): Subsystem NQN.
        kmip_pair (dict): From :func:`_setup_kmip_pair`.
    """
    pool = config["rbd_pool"]
    kmip_node_2 = kmip_pair["kmip_node_2"]

    # ── Delete namespace ──────────────────────────────────────────────────────
    if rbd_image:
        try:
            out, _ = gw.namespace.list(
                **{
                    "base_cmd_args": {"format": "json"},
                    "args": {"subsystem": nqn},
                }
            )
            ns_list = json.loads(out).get("namespaces", [])
            ns = next(
                (n for n in ns_list if n.get("rbd_image_name") == rbd_image), None
            )
            if ns:
                gw.namespace.delete(**{"args": {"subsystem": nqn, "nsid": ns["nsid"]}})
                LOG.info("TC-03 cleanup: deleted namespace for %s", rbd_image)
        except Exception as exc:
            LOG.warning("TC-03 cleanup: could not delete namespace: %s", exc)

        # ── Remove RBD image ──────────────────────────────────────────────────
        initiator_node = get_node_by_id(ceph_cluster, config["initiator_node"])
        try:
            initiator_node.exec_command(cmd=f"rbd rm {pool}/{rbd_image}", sudo=True)
            LOG.info("TC-03 cleanup: removed RBD image %s/%s", pool, rbd_image)
        except Exception as exc:
            LOG.warning("TC-03 cleanup: could not remove RBD image: %s", exc)

    # ── KMIP-1: tear down only if this test started it; otherwise restart
    # the suite-level server in case Step 2 stopped it. ───────────────────────
    try:
        if kmip_pair.get("kmip1_owned", True):
            release_kmip_if_owned(kmip_pair["kmip1_info"], True)
        else:
            port = kmip_pair["kmip1_info"]["kmip_cfg"].get("port", _DUMMY_KMIP_PORT)
            start_kmip_container(kmip_pair["kmip_node"], _KMIP_CONTAINER_NAME, port)
            LOG.info("TC-03 cleanup: restarted shared KMIP-1")
    except Exception as exc:
        LOG.warning("TC-03 cleanup: KMIP-1 teardown/restore error: %s", exc)

    # ── Teardown KMIP-2: stop container + remove cert dir ─────────────────────
    try:
        kmip2_cfg = kmip_pair["kmip2_cfg"]
        kmip_node_2.exec_command(
            cmd=(
                f"podman rm -f {kmip2_cfg['container_name']} 2>/dev/null || true ; "
                f"rm -rf {kmip2_cfg['dummy_cert_dir']}"
            ),
            sudo=True,
        )
        LOG.info("TC-03 cleanup: KMIP-2 container stopped and cert dir removed.")
    except Exception as exc:
        LOG.warning("TC-03 cleanup: KMIP-2 teardown error: %s", exc)

    LOG.info("TC-03 cleanup complete.")


# ---------------------------------------------------------------------------
# Main E2E orchestrator
# ---------------------------------------------------------------------------


def run_byok_kmip_failover_e2e(
    ceph_cluster, config, nvme_service, rbd_obj, custom_data
):
    """Orchestrate all 4 steps of the TC-03 KMIP failover test.

    Args:
        ceph_cluster: Ceph cluster object.
        config (dict): Test configuration.
        nvme_service (NVMeService): Deployed NVMe service (gateways ready).
        rbd_obj: RBD object from ``initial_rbd_config()``.
        custom_data (dict): kwargs["test_data"] from the cephci runner.

    Returns:
        int: 0 on success, 1 on failure.
    """
    initiator = NVMeInitiator(get_node_by_id(ceph_cluster, config["initiator_node"]))
    listener_port = config.get("listener_port", 4420)
    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.gateways = nvme_service.gateways

    # ── Phase A: start KMIP pair + copy certs ────────────────────────────────
    LOG.info("TC-03 Phase A — setting up dual KMIP servers")
    kmip_pair = _setup_kmip_pair(ceph_cluster, config, custom_data, nvme_service)

    # Redeploy so the GW daemons pick up both cert directories
    LOG.info("TC-03 — redeploying NVMe-oF service so GW daemons pick up KMIP certs")
    nvme_service.redeploy()
    nvme_service.init_gateways()
    gw = nvme_service.gateways[0]
    ha.gateways = nvme_service.gateways

    # ── Phase B: subsystem + both endpoints + namespace ──────────────────────
    LOG.info("TC-03 Phase B — configuring subsystem, endpoints, and namespace")
    gw, nqn, rbd_image, key_uid = _setup_subsystem_and_namespace(
        ceph_cluster, config, nvme_service, kmip_pair
    )
    # Stash rbd_image in config so step helpers can reference it without
    # threading it through every call.
    config["_rbd_image"] = rbd_image

    try:
        # ── Step 1: both servers up — baseline IO ─────────────────────────────
        step1_both_servers_up_io(initiator, gw, listener_port)

        # ── Step 2: fail KMIP-1 — GW should fall back to KMIP-2 ──────────────
        step2_fail_kmip1_verify_kmip2_failover(
            initiator, gw, nqn, config, kmip_pair, listener_port
        )

        # ── Step 3: restore KMIP-1, fail KMIP-2 — GW restart uses KMIP-1 ─────
        step3_restore_kmip1_fail_kmip2_gw_restart(
            ha, nvme_service, initiator, gw, nqn, config, kmip_pair, listener_port
        )

        # ── Step 4: restore both — clean state ───────────────────────────────
        step4_restore_both_clean_state(
            ha, nvme_service, initiator, gw, nqn, config, kmip_pair, listener_port
        )

    finally:
        teardown_kmip_failover(ceph_cluster, config, rbd_image, gw, nqn, kmip_pair)

    LOG.info("CEPH-83632974 TC-03 NVMe-oF BYOK KMIP Failover — all steps PASSED")
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
            nvme_service.redeploy()
        nvme_service.init_gateways()

        return run_byok_kmip_failover_e2e(
            ceph_cluster, config, nvme_service, rbd_obj, custom_data
        )

    except Exception as err:
        LOG.exception("CEPH-83632974 TC-03 NVMe-oF BYOK KMIP Failover FAILED: %s", err)
        return 1

    finally:
        if config.get("cleanup") and nvme_service is not None and rbd_obj is not None:
            teardown(nvme_service, rbd_obj)
