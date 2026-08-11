"""Reusable RBD block-IO profile helpers for multi-bs FIO coverage.

Suites can supply ``block_io_profiles`` as a list of dicts::

    block_io_profiles:
      - name: source-4k
        bs: 4K
        offset: "0"
        size: 256M
        pattern: "0xAA"
        role: source
        iodepth: 32

Supported keys per profile: ``name``, ``bs``, ``offset``, ``size``,
``pattern``, ``role`` (``source``|``target``), optional ``iodepth``,
``rate``, and ``timeout``.
"""

from ceph.rbd.workflows.rbd import run_rbd_fio
from utility.log import Log

log = Log(__name__)

DEFAULT_BLOCK_IO_PROFILES = [
    # Source-backed regions (written before snapshot / import)
    {
        "name": "source-4k",
        "bs": "4K",
        "offset": "0",
        "size": "256M",
        "pattern": "0xAA",
        "role": "source",
    },
    {
        "name": "source-64k",
        "bs": "64K",
        "offset": "512M",
        "size": "256M",
        "pattern": "0xBB",
        "role": "source",
    },
    {
        "name": "source-1m",
        "bs": "1M",
        "offset": "2G",
        "size": "512M",
        "pattern": "0xCC",
        "role": "source",
    },
    {
        "name": "source-4m",
        "bs": "4M",
        "offset": "4G",
        "size": "1G",
        "pattern": "0xDD",
        "role": "source",
    },
    # Destination-only writes after prepare (must not change source snap)
    {
        "name": "target-4k",
        "bs": "4K",
        "offset": "6G",
        "size": "64M",
        "pattern": "0x11",
        "role": "target",
    },
    {
        "name": "target-64k",
        "bs": "64K",
        "offset": "6272M",
        "size": "64M",
        "pattern": "0x22",
        "role": "target",
    },
    {
        "name": "target-1m",
        "bs": "1M",
        "offset": "7G",
        "size": "128M",
        "pattern": "0x33",
        "role": "target",
    },
    {
        "name": "target-4m",
        "bs": "4M",
        "offset": "8G",
        "size": "256M",
        "pattern": "0x44",
        "role": "target",
    },
]


def get_block_io_profiles(config):
    """Return source/target block-IO profiles for multi-bs coverage.

    Args:
        config: Test configuration dict. May contain ``block_io_profiles``.

    Returns:
        list: Profile dicts covering 4K/64K/1M/4M by default when suite
        does not supply ``block_io_profiles``.
    """
    profiles = config.get("block_io_profiles")
    if profiles:
        return profiles
    return list(DEFAULT_BLOCK_IO_PROFILES)


def filter_profiles_by_role(profiles, role):
    """Filter block IO profiles by role.

    Args:
        profiles: List of profile dicts.
        role: ``source`` or ``target``.

    Returns:
        list: Profiles whose ``role`` matches (default role is ``source``).
    """
    return [profile for profile in profiles if profile.get("role", "source") == role]


def run_profile_fio(client, pool, image, profile, rw, name_prefix=""):
    """Run fio for a single block-IO profile using the RBD ioengine.

    Args:
        client: CephNode where fio runs.
        pool: RBD pool name.
        image: RBD image name.
        profile: Profile dict with ``bs``, ``offset``, ``size``, ``pattern``.
            Optional ``verify`` overrides fio verify mode (default ``crc32c``;
            use ``pattern`` for sparse-hole zero checks).
        rw: fio mode (``write`` or ``read``).
        name_prefix: Optional prefix for the fio job name.

    Returns:
        int: fio command return code (0 on success).
    """
    name = f"{name_prefix}{profile['name']}" if name_prefix else profile["name"]
    # Sparse holes were never written with crc32c headers; verify raw zeros.
    verify = profile.get("verify")
    if not verify:
        if profile.get("role") == "hole" or str(profile.get("pattern", "")).lower() in (
            "0x00",
            "0",
            "00",
        ):
            verify = "pattern"
        else:
            verify = "crc32c"
    log.info(
        f"FIO {rw} profile={profile['name']} bs={profile['bs']} "
        f"offset={profile['offset']} size={profile['size']} "
        f"pattern={profile['pattern']} verify={verify}"
    )
    return run_rbd_fio(
        client=client,
        pool=pool,
        image=image,
        rw=rw,
        offset=profile["offset"],
        size=profile["size"],
        pattern=profile["pattern"],
        bs=profile.get("bs", "4M"),
        name=name,
        iodepth=profile.get("iodepth", 16),
        rate=profile.get("rate"),
        timeout=profile.get("timeout", 3600),
        verify=verify,
    )


DEFAULT_SPARSE_IO_PROFILES = [
    # Written regions at beginning / middle / end of a 10G sparse image
    {
        "name": "sparse-begin",
        "bs": "4M",
        "offset": "0",
        "size": "256M",
        "pattern": "0xAA",
        "role": "written",
    },
    {
        "name": "sparse-middle",
        "bs": "4M",
        "offset": "4G",
        "size": "256M",
        "pattern": "0xBB",
        "role": "written",
    },
    {
        "name": "sparse-end",
        "bs": "4M",
        "offset": "9G",
        "size": "256M",
        "pattern": "0xCC",
        "role": "written",
    },
    # Unwritten holes that must read as zeroes and stay sparsely allocated.
    # verify=pattern: raw zeros have no crc32c magic headers.
    {
        "name": "sparse-hole-a",
        "bs": "4M",
        "offset": "1G",
        "size": "256M",
        "pattern": "0x00",
        "role": "hole",
        "verify": "pattern",
    },
    {
        "name": "sparse-hole-b",
        "bs": "4M",
        "offset": "6G",
        "size": "256M",
        "pattern": "0x00",
        "role": "hole",
        "verify": "pattern",
    },
]


def get_sparse_io_profiles(config):
    """Return sparse written/hole IO profiles for sparse native-import tests.

    Args:
        config: Test configuration dict. May contain ``sparse_io_profiles``.

    Returns:
        list: Profile dicts with roles ``written`` and ``hole``.
    """
    profiles = config.get("sparse_io_profiles")
    if profiles:
        return profiles
    return list(DEFAULT_SPARSE_IO_PROFILES)


def get_rbd_du_exact(client, image_spec):
    """Run ``rbd du --exact --format json`` and return usage for an image.

    Args:
        client: CephNode client.
        image_spec: Image or snap spec (``pool/image`` or ``pool/image@snap``).

    Returns:
        dict: ``provisioned_size`` and ``used_size`` in bytes.

    Raises:
        ValueError: If usage cannot be parsed from command output.
    """
    import json

    from ceph.rbd.utils import exec_cmd

    out = exec_cmd(
        node=client,
        cmd=f"rbd du --exact --format json {image_spec}",
        output=True,
    )
    data = json.loads(out)
    images = data.get("images") or []
    if not images:
        raise ValueError(f"rbd du returned no images for {image_spec}: {data}")

    # Prefer the matching image/snap entry; fall back to first / totals
    entry = images[0]
    for image in images:
        name = image.get("name", "")
        snap = image.get("snapshot")
        spec_tail = image_spec.split("/")[-1]
        if "@" in spec_tail:
            img_name, snap_name = spec_tail.split("@", 1)
            if name == img_name and snap == snap_name:
                entry = image
                break
        elif name == spec_tail and not snap:
            entry = image
            break

    provisioned = int(
        entry.get("provisioned_size", data.get("total_provisioned_size", 0))
    )
    used = int(entry.get("used_size", data.get("total_used_size", 0)))
    log.info(
        f"rbd du --exact {image_spec}: provisioned={provisioned} "
        f"used={used} ({used * 100.0 / provisioned:.2f}% used)"
        if provisioned
        else f"rbd du --exact {image_spec}: provisioned={provisioned} used={used}"
    )
    return {"provisioned_size": provisioned, "used_size": used}


def get_rbd_diff_allocated_bytes(client, image_spec):
    """Sum allocated extent lengths from ``rbd diff --format json``.

    Useful when ``rbd du --exact`` still reports ``used_size=0`` on a
    migration target that has already copied initialized extents.

    Args:
        client: CephNode client.
        image_spec: Image or snap spec.

    Returns:
        int: Total bytes marked as existing in the diff output.
    """
    import json

    from ceph.rbd.utils import exec_cmd

    out = exec_cmd(
        node=client,
        cmd=f"rbd diff --format json {image_spec}",
        output=True,
        check_ec=False,
    )
    try:
        extents = json.loads(out) if out else []
    except Exception as error:
        log.warning(f"Unable to parse rbd diff for {image_spec}: {error}")
        return 0

    allocated = 0
    for extent in extents or []:
        if extent.get("exists", True):
            allocated += int(extent.get("length", 0))
    log.info(f"rbd diff {image_spec}: allocated_bytes={allocated}")
    return allocated


def get_effective_used_size(client, image_spec, usage=None):
    """Return usage dict, falling back to ``rbd diff`` when du used is 0.

    Args:
        client: CephNode client.
        image_spec: Image or snap spec.
        usage: Optional existing ``get_rbd_du_exact`` result.

    Returns:
        dict: ``provisioned_size`` and ``used_size`` (possibly from diff).
    """
    usage = dict(usage or get_rbd_du_exact(client, image_spec))
    if usage.get("used_size", 0) > 0:
        return usage

    diff_used = get_rbd_diff_allocated_bytes(client, image_spec)
    if diff_used > 0:
        log.info(
            f"{image_spec}: rbd du used=0; using rbd diff allocated "
            f"size {diff_used} for sparseness comparison"
        )
        usage["used_size"] = diff_used
    return usage


def assert_image_is_sparse(usage, max_used_ratio=0.5):
    """Assert used size is substantially smaller than provisioned size.

    Args:
        usage: Dict from ``get_rbd_du_exact``.
        max_used_ratio: Maximum allowed used/provisioned ratio (default 0.5).

    Returns:
        0 if sparse enough, 1 otherwise.
    """
    provisioned = usage["provisioned_size"]
    used = usage["used_size"]
    if provisioned <= 0:
        log.error(f"Invalid provisioned size from rbd du: {usage}")
        return 1
    ratio = used / float(provisioned)
    if ratio >= max_used_ratio:
        log.error(
            f"Image is not sparse enough: used={used} provisioned={provisioned} "
            f"ratio={ratio:.3f} (max allowed {max_used_ratio})"
        )
        return 1
    log.info(
        f"Confirmed sparseness: used={used} provisioned={provisioned} "
        f"ratio={ratio:.3f}"
    )
    return 0


def assert_used_size_close(
    source_usage, dest_usage, max_inflation_ratio=1.5, min_ratio=0.5
):
    """Assert destination used size stays close to source and is not full size.

    Args:
        source_usage: Dict from ``get_rbd_du_exact`` on source.
        dest_usage: Dict from ``get_rbd_du_exact`` on destination.
        max_inflation_ratio: Max dest_used / source_used allowed (default 1.5).
        min_ratio: Min dest_used / source_used allowed. Use ``0`` to only
            enforce the anti-inflation / not-full-size checks (useful right
            after migration execute when ``rbd du`` may still report 0).

    Returns:
        0 if destination used size is acceptable, 1 otherwise.
    """
    src_used = source_usage["used_size"]
    dst_used = dest_usage["used_size"]
    dst_prov = dest_usage["provisioned_size"]

    if dst_prov and dst_used >= dst_prov * 0.9:
        log.error(
            f"Destination used size inflated near full provisioned size: "
            f"used={dst_used} provisioned={dst_prov}"
        )
        return 1

    if src_used <= 0:
        log.error(f"Source used size is unexpectedly zero: {source_usage}")
        return 1

    if dst_used <= 0:
        if min_ratio and min_ratio > 0:
            log.error(f"Destination used size is zero while source used={src_used}")
            return 1
        log.info(
            f"Destination used size is 0 after this stage; source_used={src_used}. "
            f"Skipping min-ratio check (min_ratio={min_ratio}); "
            f"confirmed not inflated vs provisioned={dst_prov}"
        )
        return 0

    inflation = dst_used / float(src_used)
    if inflation > max_inflation_ratio:
        log.error(
            f"Destination used size inflated vs source: "
            f"source_used={src_used} dest_used={dst_used} "
            f"inflation={inflation:.3f} (max {max_inflation_ratio})"
        )
        return 1
    if min_ratio and inflation < min_ratio:
        log.error(
            f"Destination used size too small vs source: "
            f"source_used={src_used} dest_used={dst_used} "
            f"ratio={inflation:.3f} (min {min_ratio})"
        )
        return 1

    log.info(
        f"Destination used size close to source: source_used={src_used} "
        f"dest_used={dst_used} ratio={inflation:.3f}"
    )
    return 0
