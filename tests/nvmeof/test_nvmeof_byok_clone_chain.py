"""
TC-02: Multi-Level Encryption — Clone Chain (Ceph 9.2 BYOK).

Objective:
  Validate that the NVMe-oF gateway correctly opens an encrypted RBD clone of
  a LUKS-formatted parent. The clone keeps nested encryption (clone layer
  outer, parent layer inner), so the gateway must be given both key-ids,
  child-first then parent-last.

Setup (performed inside this test):
  - KMIP server pre-loaded with two keys:
      key-id-parent          → used to LUKS2-format the parent RBD image
      key-id-child-clone     → used to LUKS2-format the clone
  - Parent RBD image : created + LUKS2-formatted with key-id-parent
  - Clone            : cloned from a parent snap, LUKS2-formatted with
                       key-id-child-clone (left unflattened)

Sub-scenarios:
  1  — Add namespace for the nested-LUKS clone with child+parent key-ids.
       Connect + FIO with data-integrity verify.
  2  — Negative: attempt namespace add for the clone with a wrong key-id.
       Confirm error; no orphan namespace; GW still healthy.
  3  — Negative: attempt namespace add with rbd-create-image=true and two
       encryption formats/key-ids. Confirm the CLI rejects it (multi-key
       and image creation are mutually exclusive).

Cleanup: delete namespaces, then delete clone RBD images and parent snapshots.

Polarion-id: CEPH-83632659
"""

import json

from looseversion import LooseVersion

from ceph.ceph import Ceph, CommandFailed
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.gateway_entities import (
    configure_hosts,
    configure_listeners,
    configure_subsystems,
    teardown,
)
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.kmip_utils import (
    acquire_kmip_for_nvmeof,
    configure_kmip_endpoint_on_subsystem,
    register_kmip_passphrases,
    release_kmip_if_owned,
)
from tests.nvmeof.workflows.nvme_encryption import (
    add_encrypted_namespace_chain,
    cleanup_rbd_clone_chain,
    create_rbd_luks_clone,
    create_rbd_luks_image,
    verify_gw_subsystem_healthy,
    verify_no_namespace_for_image,
)
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id, get_ceph_version_from_cluster

LOG = Log(__name__)


# ---------------------------------------------------------------------------
# Setup: KMIP keys + RBD chain creation
# ---------------------------------------------------------------------------


def setup_clone_chain(ceph_cluster, config, kmip_info):
    """Register chain keys on the KMIP server and build the RBD image hierarchy.

    Creates two images on the client node:
      parent          — LUKS2 with key-id-parent
      clone_direct    — clone of parent snapshot, LUKS2 with key-id-child-clone

    Args:
        ceph_cluster: Ceph cluster object.
        config (dict): Test configuration; must contain ``chain_keys`` list and
            ``rbd_pool`` / ``bdev_size``.
        kmip_info (dict): kmip_info from ``setup_kmip_for_nvmeof()``; the KMIP
            server container must already be running on the initiator node.

    Returns:
        dict: Chain context with keys:
            parent_image        — str
            clone_direct_image  — str
            snap_specs          — list[str]  (for cleanup)
            key_map             — dict mapping key_id → key_uid
    """
    pool = config["rbd_pool"]
    size = config.get("bdev_size", "2G")
    chain_cfg = config["chain_keys"]
    uid = generate_unique_id(length=6)

    # Image names are unique per run
    parent_image = f"{uid}-parent"
    clone_direct_image = f"{uid}-clone-direct"

    initiator_node = get_node_by_id(ceph_cluster, config["initiator_node"])

    use_dummy = kmip_info.get("use_dummy", True)
    if use_dummy:
        # Register all chain keys on the running dummy KMIP server and
        # write passphrase files so rbd encryption format can consume them.
        LOG.info("Registering chain passphrases on dummy KMIP server")
        register_kmip_passphrases(initiator_node, chain_cfg, config)
    else:
        # Real GKLM: passphrases are already registered; passphrase_file must
        # be supplied externally.  We just log and continue.
        LOG.info(
            "Real GKLM backend: passphrase files must be present on the initiator node"
        )

    # Build a key_id → {key_uid, passphrase_file} lookup
    key_map = {e["key_id"]: e for e in chain_cfg}

    def _pp_file(key_id):
        return key_map[key_id]["passphrase_file"]

    # ── Parent image ──────────────────────────────────────────────────────────
    parent_key_id = config["parent_key_id"]
    LOG.info("Creating parent LUKS2 image: %s/%s", pool, parent_image)
    create_rbd_luks_image(
        initiator_node, pool, parent_image, size, _pp_file(parent_key_id)
    )

    # ── Clone (snapshot of parent) ────────────────────────────────────────────
    child_clone_key_id = config["child_clone_key_id"]
    snap1_name = f"{uid}-snap1"
    LOG.info("Creating clone via snapshot %s@%s", parent_image, snap1_name)
    snap1_spec, _clone_direct_spec = create_rbd_luks_clone(
        initiator_node,
        pool,
        parent_image,
        snap1_name,
        clone_direct_image,
        _pp_file(child_clone_key_id),
        parent_passphrase_file=_pp_file(parent_key_id),
        size=size,
    )

    LOG.info(
        "Clone chain built: parent=%s clone=%s",
        parent_image,
        clone_direct_image,
    )

    return {
        "parent_image": parent_image,
        "clone_direct_image": clone_direct_image,
        "snap_specs": [snap1_spec],
        "key_map": key_map,
    }


# ---------------------------------------------------------------------------
# Sub-scenario helpers
# ---------------------------------------------------------------------------


def _run_chain_key_scenario(
    label, gateway, nqn, pool, image, key_uids, luks_formats, initiator, listener_port
):
    """Add an unflattened nested-LUKS clone namespace and verify IO.

    ``ns add`` must receive both key-ids in child-first, parent-last order.

    Returns:
        tuple[bool, str]: (True, "PASSED …") always — raises on any error.
    """
    LOG.info(
        "%s — adding namespace for %s with key_uids=%s formats=%s",
        label,
        image,
        key_uids,
        luks_formats,
    )
    add_encrypted_namespace_chain(
        gateway,
        nqn,
        pool,
        image,
        key_ids=key_uids,
        luks_formats=luks_formats,
    )

    initiator.connect_targets(
        gateway, {"nqn": "connect-all", "listener_port": listener_port}
    )
    initiator.start_fio(io_size="100%", execute_blkdiscard=False, verify="crc32c")
    initiator.disconnect_all()

    msg = f"{label} PASSED — two-key chain IO verified for nested LUKS clone {image}"
    LOG.info(msg)
    return True, msg


def scenario1_clone_chain_io(gateway, config, initiator, listener_port, chain_ctx):
    """Add namespace for the nested-LUKS direct clone with child+parent keys; verify IO.

    Args:
        gateway: NVMeGateway instance.
        config (dict): Test configuration.
        initiator (NVMeInitiator): Initiator instance.
        listener_port (int): NVMe listener port.
        chain_ctx (dict): From ``setup_clone_chain()``.

    Returns:
        tuple[bool, str]: (passed, message) — raises on failure.
    """
    nqn = config["subsystems"][0]["nqn"]
    pool = config["rbd_pool"]
    key_map = chain_ctx["key_map"]
    image = chain_ctx["clone_direct_image"]

    child_uid = key_map[config["child_clone_key_id"]]["key_uid"]
    parent_uid = key_map[config["parent_key_id"]]["key_uid"]

    return _run_chain_key_scenario(
        "Scenario 1",
        gateway,
        nqn,
        pool,
        image,
        key_uids=[child_uid, parent_uid],
        luks_formats=["luks2", "luks2"],
        initiator=initiator,
        listener_port=listener_port,
    )


def scenario3_missing_parent_key_rejected(gateway, config, chain_ctx):
    """Negative: namespace add for the clone with a wrong (nonexistent) key-id must fail.

    Uses a synthetic KMIP UID that was never registered.  The GW cannot fetch
    the passphrase, so the namespace add is rejected.

    Confirms:
      - The GW returns an error (via a raised exception or error text in output).
      - No orphan namespace is left in ns list.
      - The subsystem is still healthy afterwards.

    Maps to manual test step 3.

    Args:
        gateway: NVMeGateway instance.
        config (dict): Test configuration.
        chain_ctx (dict): From ``setup_clone_chain()``.
    """
    nqn = config["subsystems"][0]["nqn"]
    pool = config["rbd_pool"]
    image = chain_ctx["clone_direct_image"]

    # Use a key-id that was never registered on the KMIP server.
    wrong_key_uid = "99999"

    LOG.info(
        "Scenario 3 (negative) — attempting ns add with nonexistent key %s "
        "for clone image %s",
        wrong_key_uid,
        image,
    )

    error_detected = False
    try:
        out, err = gateway.namespace.add(
            **{
                "args": {
                    "subsystem": nqn,
                    "rbd-pool": pool,
                    "rbd-image": image,
                    "encryption-format": "luks2",
                    "key-id": wrong_key_uid,
                }
            }
        )
        # If no exception, check the output text for an error indicator
        combined = f"{out} {err}".lower()
        if any(
            kw in combined
            for kw in ("error", "fail", "invalid", "not found", "passphrase", "fetch")
        ):
            error_detected = True
            LOG.info(
                "Scenario 3 — expected error in CLI output: out=%s err=%s", out, err
            )
        else:
            raise AssertionError(
                f"Scenario 3: expected namespace add with wrong key to fail "
                f"but it succeeded. out={out!r} err={err!r}"
            )
    except CommandFailed as exc:
        error_detected = True
        LOG.info("Scenario 3 — expected CommandFailed exception: %s", exc)

    assert (
        error_detected
    ), "Scenario 3: namespace add with nonexistent key did not produce any error"

    # No orphan namespace must remain
    verify_no_namespace_for_image(gateway, nqn, image)

    # GW must still be operational
    verify_gw_subsystem_healthy(gateway, nqn)

    LOG.info(
        "Scenario 3 PASSED — wrong key-id rejected; no orphan namespace; GW healthy"
    )


def scenario4_create_image_with_chain_rejected(gateway, config, chain_ctx):
    """Negative: rbd-create-image=True with multi-key chain must be rejected by CLI.

    Per spec, image creation (rbd-create-image) is only supported for a single
    encryption format and a single key-id.  Passing comma-separated multi-key
    lists together with create-image must be rejected.

    Maps to manual test step 4.

    Args:
        gateway: NVMeGateway instance.
        config (dict): Test configuration.
        chain_ctx (dict): From ``setup_clone_chain()``.
    """
    nqn = config["subsystems"][0]["nqn"]
    pool = config["rbd_pool"]
    key_map = chain_ctx["key_map"]

    parent_uid = key_map[config["parent_key_id"]]["key_uid"]
    child_uid = key_map[config["child_clone_key_id"]]["key_uid"]

    # Use a fresh image name that does NOT exist — we expect rejection before
    # any image is touched.
    fake_image = generate_unique_id(length=6) + "-chain-create-reject"

    LOG.info(
        "Scenario 4 (negative) — attempting ns add with rbd-create-image=True "
        "and two-key chain [%s, %s] for non-existing image %s",
        parent_uid,
        child_uid,
        fake_image,
    )

    error_detected = False
    try:
        out, err = gateway.namespace.add(
            **{
                "args": {
                    "subsystem": nqn,
                    "rbd-pool": pool,
                    "rbd-image": fake_image,
                    "rbd-create-image": True,
                    "rbd-image-size": config.get("bdev_size", "2G"),
                    "encryption-format": "luks2,luks2",
                    "key-id": f"{parent_uid},{child_uid}",
                }
            }
        )
        combined = f"{out} {err}".lower()
        if any(
            kw in combined
            for kw in ("error", "fail", "invalid", "not supported", "incompatible")
        ):
            error_detected = True
            LOG.info(
                "Scenario 4 — expected error in CLI output: out=%s err=%s", out, err
            )
        else:
            raise AssertionError(
                f"Scenario 4: expected ns add with rbd-create-image + multi-key "
                f"to be rejected, but it succeeded. out={out!r} err={err!r}"
            )
    except CommandFailed as exc:
        error_detected = True
        LOG.info("Scenario 4 — expected CommandFailed exception: %s", exc)

    assert (
        error_detected
    ), "Scenario 4: rbd-create-image with multi-key chain was not rejected"

    # No namespace for the fake image must have been created
    verify_no_namespace_for_image(gateway, nqn, fake_image)

    # GW still healthy
    verify_gw_subsystem_healthy(gateway, nqn)

    LOG.info(
        "Scenario 4 PASSED — rbd-create-image + multi-key chain correctly rejected; "
        "GW healthy"
    )


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


def teardown_clone_chain(ceph_cluster, config, chain_ctx, gateway):
    """Remove NVMe namespaces and RBD clone images created by TC-02.

    Args:
        ceph_cluster: Ceph cluster object.
        config (dict): Test configuration.
        chain_ctx (dict): From ``setup_clone_chain()``.
        gateway: NVMeGateway instance for namespace delete calls.
    """
    nqn = config["subsystems"][0]["nqn"]
    pool = config["rbd_pool"]

    # Delete all namespaces that were created for the chain images
    for image_key in ("clone_direct_image",):
        image = chain_ctx.get(image_key)
        if not image:
            continue
        try:
            out, _ = gateway.namespace.list(
                **{
                    "base_cmd_args": {"format": "json"},
                    "args": {"subsystem": nqn},
                }
            )
            ns_list = json.loads(out).get("namespaces", [])
            ns = next((n for n in ns_list if n.get("rbd_image_name") == image), None)
            if ns:
                gateway.namespace.delete(
                    **{"args": {"subsystem": nqn, "nsid": ns["nsid"]}}
                )
                LOG.info("Deleted namespace for image %s (nsid=%s)", image, ns["nsid"])
        except Exception as exc:
            LOG.warning("Could not delete namespace for %s: %s", image, exc)

    # Remove clone RBD images then unprotect/remove parent snapshots
    initiator_node = get_node_by_id(ceph_cluster, config["initiator_node"])
    cleanup_rbd_clone_chain(
        initiator_node,
        pool,
        [chain_ctx["clone_direct_image"]],
        chain_ctx["snap_specs"],
    )

    # Remove the parent image
    parent = chain_ctx.get("parent_image")
    if parent:
        try:
            initiator_node.exec_command(cmd=f"rbd rm {pool}/{parent}", sudo=True)
            LOG.info("Removed parent image %s/%s", pool, parent)
        except Exception as exc:
            LOG.warning("Could not remove parent image %s: %s", parent, exc)

    LOG.info("TC-02 RBD chain cleanup complete.")


# ---------------------------------------------------------------------------
# Main E2E orchestrator
# ---------------------------------------------------------------------------


def run_byok_clone_chain_e2e(ceph_cluster, config, nvme_service, rbd_obj, custom_data):
    """Orchestrate all 3 sub-scenarios for TC-02.

    Args:
        ceph_cluster: Ceph cluster object.
        config (dict): Test configuration.
        nvme_service (NVMeService): Deployed NVMe service.
        rbd_obj: RBD object from ``initial_rbd_config()``.
        custom_data (dict): kwargs["test_data"] from the cephci runner.
    """
    initiator = NVMeInitiator(get_node_by_id(ceph_cluster, config["initiator_node"]))
    listener_port = config.get("listener_port", 4420)
    gw = nvme_service.gateways[0]

    ceph_version = get_ceph_version_from_cluster(
        ceph_cluster.get_nodes(role="client")[0]
    )

    # ── KMIP setup ────────────────────────────────────────────────────────────
    # The dummy KMIP server is started via the normal path; the clone-chain
    # keys will be registered on top with register_kmip_passphrases().
    # config["luks_combos"] is intentionally empty for TC-02 (no luks_combos
    # needed); the KMIP server is still started so the endpoint can be
    # registered on the subsystem.
    kmip_info, kmip_owned = acquire_kmip_for_nvmeof(
        ceph_cluster, config, custom_data, nvme_service=nvme_service
    )

    LOG.info("Redeploying NVMe-oF service so GW daemons pick up KMIP certs")
    nvme_service.redeploy()
    nvme_service.init_gateways()
    gw = nvme_service.gateways[0]

    chain_ctx = None
    results = {}
    try:
        # ── Subsystem + KMIP endpoint ─────────────────────────────────────────
        configure_subsystems(nvme_service, ceph_cluster=ceph_cluster)
        if LooseVersion(ceph_version) <= LooseVersion("20.2.1"):
            configure_listeners(nvme_service.gateways, config)
        configure_hosts(gw, config, ceph_cluster=ceph_cluster)

        configure_kmip_endpoint_on_subsystem(
            gw, config["subsystems"][0]["nqn"], kmip_info["kmip_cfg"]
        )

        # ── Build RBD clone chain + register keys ────────────────────────────
        chain_ctx = setup_clone_chain(ceph_cluster, config, kmip_info)

        # ── Sub-scenario 1: clone, two-key chain, IO ─────────────────────────
        s1_passed, s1_msg = scenario1_clone_chain_io(
            gw, config, initiator, listener_port, chain_ctx
        )
        results["scenario1"] = (s1_passed, s1_msg)

        # ── Remove the Scenario 1 namespace before the negative scenarios ─────
        # Scenario 3 calls verify_no_namespace_for_image(clone_direct_image) after
        # an expected failure, so the namespace added by scenario 1 must be gone
        # first.
        nqn = config["subsystems"][0]["nqn"]
        image = chain_ctx.get("clone_direct_image")
        if image:
            try:
                out, _ = gw.namespace.list(
                    **{"base_cmd_args": {"format": "json"}, "args": {"subsystem": nqn}}
                )
                ns_list = json.loads(out).get("namespaces", [])
                ns = next(
                    (n for n in ns_list if n.get("rbd_image_name") == image), None
                )
                if ns:
                    gw.namespace.delete(
                        **{"args": {"subsystem": nqn, "nsid": ns["nsid"]}}
                    )
                    LOG.info(
                        "Pre-scenario3 cleanup: deleted namespace for %s (nsid=%s)",
                        image,
                        ns["nsid"],
                    )
            except Exception as exc:
                LOG.warning(
                    "Pre-scenario3 cleanup: could not delete namespace for %s: %s",
                    image,
                    exc,
                )

        # ── Sub-scenario 3: negative — partial key list rejected ──────────────
        # Must pass (raises on failure)
        scenario3_missing_parent_key_rejected(gw, config, chain_ctx)
        results["scenario3"] = (True, "Scenario 3 PASSED — partial key rejected")

        # ── Sub-scenario 4: negative — create-image + multi-key rejected ──────
        # Must pass (raises on failure)
        scenario4_create_image_with_chain_rejected(gw, config, chain_ctx)
        results["scenario4"] = (
            True,
            "Scenario 4 PASSED — create-image + multi-key rejected",
        )

    finally:
        if chain_ctx is not None:
            teardown_clone_chain(ceph_cluster, config, chain_ctx, gw)
        release_kmip_if_owned(kmip_info, kmip_owned)

    # ── Summary ───────────────────────────────────────────────────────────────
    LOG.info("TC-02 sub-scenario results:")
    for name, (passed, msg) in results.items():
        status = "PASSED" if passed else "FAILED"
        LOG.info("  %-12s %s  %s", name, status, msg)

    LOG.info("CEPH-83632659 TC-02 NVMe-oF BYOK Clone Chain — all sub-scenarios PASSED")
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

        return run_byok_clone_chain_e2e(
            ceph_cluster, config, nvme_service, rbd_obj, custom_data
        )

    except Exception as err:
        LOG.exception("CEPH-83632659 TC-02 NVMe-oF BYOK Clone Chain FAILED: %s", err)
        return 1

    finally:
        if config.get("cleanup") and nvme_service is not None and rbd_obj is not None:
            teardown(nvme_service, rbd_obj)
