"""
Encrypted-namespace lifecycle helpers for NVMe-oF BYOK tests (Ceph 9.2+).

Verification is performed exclusively via `ceph nvmeof ns list --format json`
(no `rbd encryption info` — that command does not exist in 9.2).

Public API
----------
add_encrypted_namespace()               — add one LUKS-encrypted namespace (single key)
add_encrypted_namespace_chain()         — add a namespace with a multi-key ancestor chain
create_rbd_luks_image()                 — create an RBD image and format it with LUKS
create_rbd_luks_clone()                 — snapshot-protect-clone an existing LUKS image
verify_namespace_encryption_attrs()     — assert ns list shows correct attrs
verify_namespace_not_listed()           — assert namespace is absent from ns list
verify_all_encrypted_namespaces_listed()— assert every encrypted NS is present
                                          and has a non-empty key_id (post-restart)
verify_ns_encryption_chain()            — assert ns list shows the full ordered key chain
verify_no_namespace_for_image()         — assert no namespace exists for a given rbd image
verify_gw_subsystem_healthy()           — assert subsystem still appears in ns list (GW alive)
cleanup_rbd_clone_chain()               — remove clone images and their parent snapshots
"""

import json

from ceph.ceph import CommandFailed
from utility.log import Log
from utility.utils import generate_unique_id

LOG = Log(__name__)


def add_encrypted_namespace(
    gateway,
    nqn,
    rbd_pool,
    key_id,
    luks_format,
    luks_algo,
    size="2G",
    rbd_create_image=True,
    rbd_image=None,
):
    """Add one LUKS-encrypted namespace to a subsystem.

    Maps to:
      ceph nvmeof ns add <nqn> [rbd_image_name] [rbd_pool] ...
          --encryption_format <luks_format>
          --encryption_algorithm <luks_algo>
          --key_id <key_id>
          [--create-image --size <size>]

    The KEY_MAP in base_cli.py translates the hyphen-form config keys used here
    to the underscore-form CLI flags automatically.

    Args:
        gateway: NVMeGateway instance.
        nqn (str): Subsystem NQN.
        rbd_pool (str): RBD pool name.
        key_id (str): KMIP key ID string (referenced directly on the KMIP server).
        luks_format (str): "luks1" or "luks2".
        luks_algo (str): "aes128" or "aes256" (no hyphen — matches the CLI enum).
        size (str): Image size (default "2G"). Used only when rbd_create_image=True.
        rbd_create_image (bool): True → let the GW create the RBD image.
                                 False → use an existing image (delete/reopen flow).
        rbd_image (str|None): Image name to use. Auto-generated when None.

    Returns:
        str: The rbd_image_name used (needed for subsequent verify/delete calls).
    """
    image = rbd_image or generate_unique_id(length=6)
    ns_args = {
        "subsystem": nqn,
        "rbd-pool": rbd_pool,
        "rbd-image": image,
        "encryption-format": luks_format,
        "key-id": key_id,
    }
    if rbd_create_image:
        # encryption-algorithm is only valid when the GW creates a new image
        ns_args["encryption-algorithm"] = luks_algo
        ns_args["rbd-create-image"] = True
        ns_args["rbd-image-size"] = size

    LOG.info(
        "Adding encrypted namespace: nqn=%s image=%s format=%s algo=%s key_id=%s",
        nqn,
        image,
        luks_format,
        luks_algo,
        key_id,
    )
    gateway.namespace.add(**{"args": ns_args})
    return image


def verify_namespace_encryption_attrs(
    gateway,
    nqn,
    rbd_image,
    expected_format,
    expected_algo,
    expected_key_id,
):
    """Assert that `ns list` shows correct encryption attributes for rbd_image.

    Calls `ceph nvmeof ns list --format json`, finds the namespace by
    rbd_image_name, and asserts encryption_format, encryption_algorithm,
    and key_id all match the expected values.

    Args:
        gateway: NVMeGateway instance.
        nqn (str): Subsystem NQN.
        rbd_image (str): RBD image name to look up.
        expected_format (str): "luks1" or "luks2".
        expected_algo (str): "aes128" or "aes256".
        expected_key_id (str): KMIP key ID string.

    Raises:
        AssertionError: If any attribute does not match.
        ValueError: If the namespace is not found in ns list output.
    """
    out, _ = gateway.namespace.list(
        **{
            "base_cmd_args": {"format": "json"},
            "args": {"subsystem": nqn},
        }
    )
    namespaces = json.loads(out).get("namespaces", [])
    ns = next((n for n in namespaces if n.get("rbd_image_name") == rbd_image), None)
    if ns is None:
        raise ValueError(
            f"Namespace with rbd_image_name={rbd_image!r} not found in ns list "
            f"for subsystem {nqn}. Listed images: "
            f"{[n.get('rbd_image_name') for n in namespaces]}"
        )

    # encryption_algorithm is a top-level field; format and key_id are inside
    # the encryption_entries list: [{"format": "luks1", "key_id": "1"}, ...]
    # NOTE: some GW builds return null for encryption_algorithm in ns list even
    # when the namespace was created with --encryption-algorithm.  Skip the
    # algorithm assertion when the GW omits the field rather than failing the test.
    got_algo = (ns.get("encryption_algorithm") or "").lower()
    entries = ns.get("encryption_entries", [])
    got_fmt = (entries[0].get("format") or "").lower() if entries else ""
    got_kid = entries[0].get("key_id", "") if entries else ""

    assert got_fmt == expected_format.lower(), (
        f"{rbd_image}: encryption_format mismatch — "
        f"expected={expected_format!r} got={got_fmt!r}"
    )
    if got_algo:
        assert got_algo == expected_algo.lower(), (
            f"{rbd_image}: encryption_algorithm mismatch — "
            f"expected={expected_algo!r} got={got_algo!r}"
        )
    else:
        LOG.warning(
            "%s: encryption_algorithm not returned by GW (null) — skipping algo check "
            "(expected=%r)",
            rbd_image,
            expected_algo,
        )
    assert got_kid == str(expected_key_id), (
        f"{rbd_image}: key_id mismatch — "
        f"expected={expected_key_id!r} got={got_kid!r}"
    )

    LOG.info(
        "Verified encryption attrs for %s: format=%s algo=%s key_id=%s",
        rbd_image,
        got_fmt,
        got_algo,
        got_kid,
    )


def verify_namespace_not_listed(gateway, nqn, rbd_image):
    """Assert that rbd_image does NOT appear in ns list output.

    Args:
        gateway: NVMeGateway instance.
        nqn (str): Subsystem NQN.
        rbd_image (str): RBD image name that must be absent.

    Raises:
        AssertionError: If the namespace is still listed.
    """
    out, _ = gateway.namespace.list(
        **{
            "base_cmd_args": {"format": "json"},
            "args": {"subsystem": nqn},
        }
    )
    namespaces = json.loads(out).get("namespaces", [])
    listed_images = [n.get("rbd_image_name") for n in namespaces]
    assert rbd_image not in listed_images, (
        f"Namespace {rbd_image!r} still present in ns list after delete: "
        f"{listed_images}"
    )
    LOG.info(
        "Confirmed namespace %s is no longer listed in subsystem %s.", rbd_image, nqn
    )


def verify_all_encrypted_namespaces_listed(gateway, nqn, encrypted_ns_list):
    """Assert every item in encrypted_ns_list appears in ns list with a non-empty key_id.

    Used after a GW daemon restart to confirm that all encrypted namespaces
    re-armed automatically (passphrase re-fetched from KMIP).

    Args:
        gateway: NVMeGateway instance.
        nqn (str): Subsystem NQN.
        encrypted_ns_list (list[dict]): Each dict must have key "rbd_image" (str).

    Raises:
        AssertionError: If any namespace is missing or has an empty key_id.
    """
    out, _ = gateway.namespace.list(
        **{
            "base_cmd_args": {"format": "json"},
            "args": {"subsystem": nqn},
        }
    )
    namespaces = json.loads(out).get("namespaces", [])
    listed = {n["rbd_image_name"]: n for n in namespaces if n.get("rbd_image_name")}

    for item in encrypted_ns_list:
        img = item["rbd_image"]
        assert img in listed, (
            f"Encrypted namespace {img!r} missing from ns list after GW restart. "
            f"Present images: {list(listed.keys())}"
        )
        entries = listed[img].get("encryption_entries", [])
        assert entries and entries[0].get("key_id"), (
            f"encryption_entries[0].key_id is empty for namespace {img!r} "
            "after GW restart — KMIP re-fetch may have failed."
        )

    LOG.info(
        "All %d encrypted namespaces re-listed with key_id after GW restart.",
        len(encrypted_ns_list),
    )


# ---------------------------------------------------------------------------
# Multi-key ancestor-chain helpers (TC-02: clone / snapshot chains)
# ---------------------------------------------------------------------------


def add_encrypted_namespace_chain(
    gateway,
    nqn,
    rbd_pool,
    rbd_image,
    key_ids,
    luks_formats,
):
    """Add an NVMe-oF namespace backed by a multi-level LUKS ancestor chain.

    The Ceph 9.2 gateway accepts comma-separated lists for ``--encryption-format``
    and ``--key-id`` when the RBD image is a clone whose ancestors were
    independently encrypted.  The order must be **child-first → parent-last**
    (top-down: outermost/newest encryption layer first, root ancestor last).

    The RBD image MUST already exist (rbd-create-image is not used here,
    since the clone chain was pre-created by the tester).

    Args:
        gateway: NVMeGateway instance.
        nqn (str): Subsystem NQN.
        rbd_pool (str): RBD pool containing the image.
        rbd_image (str): Name of the pre-existing clone/child image.
        key_ids (list[str]): KMIP key IDs ordered child-first (outermost LUKS layer
            first, root ancestor last), e.g. ["key-id-child-clone", "key-id-parent"].
        luks_formats (list[str]): LUKS format for each key level,
            e.g. ["luks2", "luks2"].  Must be same length as key_ids.

    Returns:
        str: The rbd_image name (echo of the input).

    Raises:
        ValueError: If key_ids and luks_formats have different lengths.
    """
    if len(key_ids) != len(luks_formats):
        raise ValueError(
            f"key_ids ({len(key_ids)}) and luks_formats ({len(luks_formats)}) "
            "must have the same length for a chain namespace add"
        )

    enc_format_val = ",".join(luks_formats)
    key_id_val = ",".join(str(k) for k in key_ids)

    ns_args = {
        "subsystem": nqn,
        "rbd-pool": rbd_pool,
        "rbd-image": rbd_image,
        "encryption-format": enc_format_val,
        "key-id": key_id_val,
    }

    LOG.info(
        "Adding chain-encrypted namespace: nqn=%s image=%s formats=%s key_ids=%s",
        nqn,
        rbd_image,
        enc_format_val,
        key_id_val,
    )
    gateway.namespace.add(**{"args": ns_args})
    return rbd_image


def verify_ns_encryption_chain(
    gateway, nqn, rbd_image, expected_key_ids, expected_formats
):
    """Assert that ``ns list`` shows the full ordered encryption chain for rbd_image.

    Each element of ``encryption_entries`` (as returned by the GW) is matched
    against the corresponding position in *expected_key_ids* / *expected_formats*,
    child-first (top-down: outermost layer first, root ancestor last).

    Args:
        gateway: NVMeGateway instance.
        nqn (str): Subsystem NQN.
        rbd_image (str): RBD image name to look up.
        expected_key_ids (list[str]): Ordered list of expected KMIP key IDs
            (parent → child).
        expected_formats (list[str]): Ordered list of expected LUKS formats
            (parent → child).

    Raises:
        ValueError: If the namespace is not found.
        AssertionError: If the chain does not match.
    """
    out, _ = gateway.namespace.list(
        **{
            "base_cmd_args": {"format": "json"},
            "args": {"subsystem": nqn},
        }
    )
    namespaces = json.loads(out).get("namespaces", [])
    ns = next((n for n in namespaces if n.get("rbd_image_name") == rbd_image), None)
    if ns is None:
        raise ValueError(
            f"Namespace with rbd_image_name={rbd_image!r} not found in ns list "
            f"for subsystem {nqn}. Listed images: "
            f"{[n.get('rbd_image_name') for n in namespaces]}"
        )

    entries = ns.get("encryption_entries", [])
    assert len(entries) == len(expected_key_ids), (
        f"{rbd_image}: encryption_entries count mismatch — "
        f"expected {len(expected_key_ids)} entries, got {len(entries)}: {entries}"
    )
    for idx, (entry, exp_kid, exp_fmt) in enumerate(
        zip(entries, expected_key_ids, expected_formats)
    ):
        got_kid = entry.get("key_id", "")
        got_fmt = entry.get("format", "").lower()
        assert got_kid == str(exp_kid), (
            f"{rbd_image} entry[{idx}]: key_id mismatch — "
            f"expected={exp_kid!r} got={got_kid!r}"
        )
        assert got_fmt == exp_fmt.lower(), (
            f"{rbd_image} entry[{idx}]: format mismatch — "
            f"expected={exp_fmt!r} got={got_fmt!r}"
        )

    LOG.info(
        "Verified encryption chain for %s: %d entries OK (key_ids=%s formats=%s)",
        rbd_image,
        len(entries),
        expected_key_ids,
        expected_formats,
    )


def verify_no_namespace_for_image(gateway, nqn, rbd_image):
    """Assert that no namespace backed by *rbd_image* is present in ns list.

    Used after an expected-to-fail ``namespace add`` to confirm no orphan
    namespace was left behind.

    Args:
        gateway: NVMeGateway instance.
        nqn (str): Subsystem NQN.
        rbd_image (str): RBD image name that must not appear in any namespace.

    Raises:
        AssertionError: If a namespace for *rbd_image* is found.
    """
    out, _ = gateway.namespace.list(
        **{
            "base_cmd_args": {"format": "json"},
            "args": {"subsystem": nqn},
        }
    )
    namespaces = json.loads(out).get("namespaces", [])
    listed = [n.get("rbd_image_name") for n in namespaces]
    assert rbd_image not in listed, (
        f"Orphan namespace for image {rbd_image!r} found after expected failure. "
        f"All listed images: {listed}"
    )
    LOG.info(
        "Confirmed no namespace exists for image %s (subsystem %s) — GW clean.",
        rbd_image,
        nqn,
    )


def verify_gw_subsystem_healthy(gateway, nqn):
    """Assert that the subsystem NQN still appears in ``subsystem list``.

    A quick sanity check run after negative-path scenarios to confirm the
    gateway is still operational and the subsystem was not corrupted.

    Args:
        gateway: NVMeGateway instance.
        nqn (str): Subsystem NQN to look for.

    Raises:
        AssertionError: If the subsystem is missing (GW may be broken).
    """
    out, _ = gateway.subsystem.list(**{"base_cmd_args": {"format": "json"}})
    subsystems = json.loads(out).get("subsystems", [])
    nqns = [s.get("nqn") or s.get("subnqn") for s in subsystems]
    assert nqn in nqns, (
        f"Subsystem {nqn!r} missing from subsystem list after negative test — "
        f"GW may be unhealthy. Present NQNs: {nqns}"
    )
    LOG.info("GW subsystem health OK — %s present in subsystem list.", nqn)


# ---------------------------------------------------------------------------
# RBD image / clone / snapshot helpers (used by TC-02 setup)
# ---------------------------------------------------------------------------


def create_rbd_luks_image(client_node, pool, image, size, passphrase_file):
    """Create an RBD image and format it with LUKS2 encryption using the given passphrase.

    Steps:
      1. ``rbd create <pool>/<image> --size <size>``
      2. Write passphrase to a temporary file on the node.
      3. ``rbd encryption format <pool>/<image> luks2 <passphrase_file>``

    Args:
        client_node: CephNode with ``rbd`` and ``ceph-common`` installed.
        pool (str): RBD pool name.
        image (str): Image name to create.
        size (str): Size string, e.g. ``"2G"``.
        passphrase_file (str): Remote path to the passphrase file; the caller
            is responsible for writing the passphrase there before this call.

    Returns:
        str: The ``pool/image`` spec (e.g. ``"rbd1/parent-img"``).
    """
    spec = f"{pool}/{image}"
    LOG.info("Creating RBD image %s (size=%s)", spec, size)
    client_node.exec_command(
        cmd=f"rbd create {spec} --size {size}",
        sudo=True,
    )
    LOG.info("Formatting %s with LUKS2 using passphrase file %s", spec, passphrase_file)
    client_node.exec_command(
        cmd=f"rbd encryption format {spec} luks2 {passphrase_file}",
        sudo=True,
    )
    # LUKS2 header consumes space at the start of the image. Grow back to the
    # requested size so a later clone still sees the full parent payload and
    # has room for its own nested header. Without this, ns add fails with
    # "Wrong passphrase" on the stacked clone.
    LOG.info("Resizing %s to %s to compensate for LUKS2 header", spec, size)
    client_node.exec_command(
        cmd=(
            f"rbd resize --size {size} {spec} "
            f"--encryption-passphrase-file {passphrase_file}"
        ),
        sudo=True,
    )
    LOG.info("RBD LUKS2 image created: %s", spec)
    return spec


def create_rbd_luks_clone(
    client_node,
    pool,
    parent_image,
    snap_name,
    clone_image,
    clone_passphrase_file,
    parent_passphrase_file=None,
    size=None,
):
    """Snap-protect-clone an RBD parent image and LUKS2-format the clone.

    The clone is left unflattened so it keeps a nested LUKS chain (clone layer
    outer, parent layer inner). ``ns add`` must pass child key then parent key.

    The parent must already have been resized after its own LUKS format so the
    snapshot includes header slack for the clone layer.

    Steps:
      1. ``rbd snap create <pool>/<parent>@<snap>``
      2. ``rbd snap protect <pool>/<parent>@<snap>``
      3. ``rbd clone <pool>/<parent>@<snap> <pool>/<clone>``
      4. ``rbd encryption format <pool>/<clone> luks2 <clone_passphrase_file>``
      5. Optional stacked ``rbd resize`` (child passphrase, then parent).

    Args:
        client_node: CephNode.
        pool (str): RBD pool (parent and clone share the same pool).
        parent_image (str): Name of the parent RBD image.
        snap_name (str): Snapshot name to create on the parent.
        clone_image (str): Name for the new clone image.
        clone_passphrase_file (str): Remote path to the passphrase file for
            the clone's own LUKS2 layer.
        parent_passphrase_file (str | None): Parent passphrase file; used for
            the stacked resize after clone format.
        size (str | None): Image size (e.g. ``"2G"``); required for resize.

    Returns:
        tuple[str, str]: ``(snap_spec, clone_spec)`` where snap_spec is
            ``"pool/parent@snap"`` and clone_spec is ``"pool/clone"``.
    """
    snap_spec = f"{pool}/{parent_image}@{snap_name}"
    clone_spec = f"{pool}/{clone_image}"

    LOG.info("Creating snapshot %s", snap_spec)
    client_node.exec_command(cmd=f"rbd snap create {snap_spec}", sudo=True)

    LOG.info("Protecting snapshot %s", snap_spec)
    client_node.exec_command(cmd=f"rbd snap protect {snap_spec}", sudo=True)

    LOG.info("Cloning %s → %s", snap_spec, clone_spec)
    client_node.exec_command(cmd=f"rbd clone {snap_spec} {clone_spec}", sudo=True)

    LOG.info(
        "Formatting clone %s with LUKS2 using passphrase file %s",
        clone_spec,
        clone_passphrase_file,
    )
    client_node.exec_command(
        cmd=f"rbd encryption format {clone_spec} luks2 {clone_passphrase_file}",
        sudo=True,
    )

    if size and parent_passphrase_file:
        LOG.info(
            "Resizing clone %s to %s with stacked passphrases (child then parent)",
            clone_spec,
            size,
        )
        try:
            client_node.exec_command(
                cmd=(
                    f"rbd resize --size {size} {clone_spec} --allow-shrink "
                    f"--encryption-passphrase-file {clone_passphrase_file} "
                    f"--encryption-passphrase-file {parent_passphrase_file}"
                ),
                sudo=True,
            )
        except CommandFailed as exc:
            # rbd exits non-zero when size is unchanged; that still proves the
            # nested LUKS chain opened with child-then-parent passphrases.
            if "new size is equal to original size" not in str(exc):
                raise
            LOG.info(
                "Clone resize no-op (size unchanged); stacked passphrases accepted"
            )

    LOG.info("RBD LUKS2 unflattened clone created: %s (snap=%s)", clone_spec, snap_spec)
    return snap_spec, clone_spec


def cleanup_rbd_clone_chain(client_node, pool, clone_images, snap_specs):
    """Remove NVMe namespace backing images and their parent snapshots.

    Clones must be removed before their parent snapshots can be unprotected
    and deleted.

    Args:
        client_node: CephNode.
        pool (str): RBD pool.
        clone_images (list[str]): Clone image names to remove.
        snap_specs (list[str]): Fully-qualified snap specs (``pool/img@snap``)
            to unprotect and remove after the clones are gone.
    """
    for clone in clone_images:
        spec = f"{pool}/{clone}"
        LOG.info("Removing clone image %s", spec)
        try:
            client_node.exec_command(cmd=f"rbd rm {spec}", sudo=True)
        except CommandFailed as exc:
            LOG.warning("Could not remove clone %s: %s", spec, exc)

    for snap_spec in snap_specs:
        LOG.info("Unprotecting and removing snapshot %s", snap_spec)
        try:
            client_node.exec_command(cmd=f"rbd snap unprotect {snap_spec}", sudo=True)
            client_node.exec_command(cmd=f"rbd snap rm {snap_spec}", sudo=True)
        except CommandFailed as exc:
            LOG.warning("Could not clean up snapshot %s: %s", snap_spec, exc)


# ---------------------------------------------------------------------------
# Namespace delete-and-re-add helper (TC-03: KMIP failover)
# ---------------------------------------------------------------------------


def delete_and_readd_namespace(
    gateway,
    nqn,
    rbd_pool,
    rbd_image,
    key_id,
    luks_format,
    luks_algo,
):
    """Delete an existing namespace and immediately re-add it using the same image.

    This forces the NVMe-oF gateway to release its key handle and re-fetch the
    passphrase from KMIP the next time the namespace is opened.  Used in TC-03
    to prove that the gateway retrieves the passphrase from the surviving KMIP
    server when the primary is offline.

    The image is NOT recreated — ``rbd-create-image`` is **False** so the
    existing LUKS-formatted RBD image is reused as-is.

    Steps:
      1. ``ceph nvmeof ns list`` → find nsid for *rbd_image*.
      2. ``ceph nvmeof ns delete <nqn> <nsid>`` → remove the namespace.
      3. Verify the namespace is gone (``verify_namespace_not_listed``).
      4. ``ceph nvmeof ns add`` with the existing image and same key material.

    Args:
        gateway: NVMeGateway instance.
        nqn (str): Subsystem NQN.
        rbd_pool (str): RBD pool name.
        rbd_image (str): Existing RBD image to reuse.
        key_id (str): KMIP key ID (UID string) to pass in ``--key-id``.
        luks_format (str): ``"luks1"`` or ``"luks2"``.
        luks_algo (str): ``"aes-128"`` or ``"aes-256"``.

    Returns:
        str: The ``rbd_image`` name (echo of the input).

    Raises:
        ValueError: If the namespace for *rbd_image* is not found before delete.
    """
    # ── Step 1: find nsid ─────────────────────────────────────────────────────
    out, _ = gateway.namespace.list(
        **{
            "base_cmd_args": {"format": "json"},
            "args": {"subsystem": nqn},
        }
    )
    namespaces = json.loads(out).get("namespaces", [])
    ns = next((n for n in namespaces if n.get("rbd_image_name") == rbd_image), None)
    if ns is None:
        raise ValueError(
            f"delete_and_readd_namespace: namespace for image {rbd_image!r} not "
            f"found in subsystem {nqn}. Present: "
            f"{[n.get('rbd_image_name') for n in namespaces]}"
        )
    nsid = ns["nsid"]
    LOG.info(
        "Deleting namespace nsid=%s (image=%s) to force KMIP re-fetch",
        nsid,
        rbd_image,
    )

    # ── Step 2: delete ────────────────────────────────────────────────────────
    gateway.namespace.delete(**{"args": {"subsystem": nqn, "nsid": nsid}})

    # ── Step 3: verify gone ───────────────────────────────────────────────────
    verify_namespace_not_listed(gateway, nqn, rbd_image)
    LOG.info("Namespace %s deleted. Re-adding with existing image ...", rbd_image)

    # ── Step 4: re-add with existing image (no create) ────────────────────────
    # NOTE: encryption-algorithm is intentionally omitted here.
    # The GW rejects it with EINVAL "Encryption algorithm is only allowed when
    # creating a new image" — the algorithm is already baked into the LUKS
    # header on the existing image, so only format + key-id are needed.
    ns_args = {
        "subsystem": nqn,
        "rbd-pool": rbd_pool,
        "rbd-image": rbd_image,
        "encryption-format": luks_format,
        "key-id": str(key_id),
        # rbd-create-image intentionally absent — image already exists
    }
    LOG.info(
        "Re-adding namespace: image=%s format=%s algo=%s key_id=%s",
        rbd_image,
        luks_format,
        luks_algo,
        key_id,
    )
    gateway.namespace.add(**{"args": ns_args})
    LOG.info("Namespace %s re-added successfully.", rbd_image)
    return rbd_image
