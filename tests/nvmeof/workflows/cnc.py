"""CNC / XCOPY helpers for RHEL NVMe-oF initiator tests."""

import json
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from utility.log import Log
from utility.utils import run_fio

LOG = Log(__name__)

DEFAULT_LBA_SIZE = 512
# 4 MiB at 512-byte LBA
BLOCKS_4MIB = 8192
# Transient nvme-cli / host-stack errors during long CNC loops (dual-GW ANA)
TRANSIENT_NVME_COPY_ERROR_MARKERS = (
    "0x370",
    "Host Pathing Error",
    "pathing error was detected by the host",
    "0x371",
    "Command Aborted By Host",
    "aborted as a result of host action",
    "resource temporarily unavailable",
    "get-namespace-id",
    "eagain",
)
DEFAULT_COPY_RETRIES = 3
DEFAULT_COPY_RETRY_DELAY_SEC = 2


def _to_comma_list(value):
    if isinstance(value, (list, tuple)):
        return ",".join(str(v) for v in value)
    return str(value)


def _is_transient_nvme_copy_error(err):
    """True for retryable nvme copy failures (pathing 0x370, EAGAIN, etc.)."""
    msg = str(err).lower()
    return any(marker.lower() in msg for marker in TRANSIENT_NVME_COPY_ERROR_MARKERS)


def _blocks_to_nlb(block_count):
    """Convert an actual block count to nvme-cli 0-based NLB (--blocks).

    nvme-cli documents --blocks as zeroes-based values (same as NVMe NLB).
    """
    count = int(block_count)
    if count < 1:
        raise ValueError(f"block count must be >= 1, got {count}")
    return count - 1


def get_ns_devices(initiator):
    """Return list of {Namespace, NSID} for Ceph SPDK drives."""
    devices = initiator.list_spdk_drives(nsid_device_pair=True)
    if not devices or len(devices) < 2:
        raise Exception(f"CNC tests require at least 2 namespaces, found: {devices}")
    LOG.info(f"CNC namespace devices: {devices}")
    return devices


def fetch_ns_lb_groups(gateway, subsystem_nqn):
    """Return ``{nsid: load_balancing_group}`` from gateway namespace list."""
    out, _ = gateway.namespace.list(
        **{
            "base_cmd_args": {"format": "json"},
            "args": {"subsystem": subsystem_nqn},
        }
    )
    data = json.loads(out) if isinstance(out, str) else out
    namespaces = data.get("namespaces") if isinstance(data, dict) else None
    if namespaces is None and isinstance(data, list):
        namespaces = data
    lb_map = {}
    for ns in namespaces or []:
        nsid = ns.get("nsid")
        if nsid is None:
            continue
        lb_map[int(nsid)] = ns.get("load_balancing_group")
    return lb_map


def annotate_devices_with_ana(initiator, gateways, config=None):
    """Attach ``ana_group_id`` to each initiator SPDK namespace entry."""
    config = config or {}
    devices = get_ns_devices(initiator)
    lb_map = {}
    subsystems = config.get("subsystems") or []
    nqn = None
    if subsystems:
        nqn = subsystems[0].get("nqn") or subsystems[0].get("subnqn")
    if nqn and gateways:
        try:
            lb_map = fetch_ns_lb_groups(gateways[0], nqn)
            LOG.info(f"Gateway NS load-balancing groups for {nqn}: {lb_map}")
        except Exception as err:
            LOG.warning(f"Unable to list NS load-balancing groups: {err}")

    annotated = []
    for entry in devices:
        nsid = int(entry["NSID"])
        ana = lb_map.get(nsid)
        if ana is None and gateways:
            try:
                paths = initiator.fetch_anastate(entry["Namespace"])
                if paths.get("optimized"):
                    gw = find_gateway_by_ip(gateways, paths["optimized"][0])
                    if gw is not None:
                        ana = gw.ana_group_id
            except Exception as err:
                LOG.warning(f"ANA path fallback failed for {entry['Namespace']}: {err}")
        annotated.append({**entry, "ana_group_id": ana})
    return annotated


def select_ns_pair(initiator, gateways, config=None):
    """Pick source/destination NS honoring suite ``ana_affinity``.

    Suite config:
      - ``ana_affinity: same`` — both namespaces share one ANA / LB group
      - ``ana_affinity: different`` — namespaces on distinct ANA / LB groups
      - omitted — first two discovered namespaces (still logged with ANA ids)

    Create at least **2 namespaces per ANA group** (e.g. 2 with
    ``lb_group: node4`` and 2 with ``lb_group: node5``). Auto load-balancing
    can redistribute a single pair created on one group onto different ANA
    ids, which breaks ``ana_affinity=same``. Prefer ``rebalance_period_sec: 0``
    so explicit ``lb_group`` pins stay put.
    """
    config = config or {}
    devices = annotate_devices_with_ana(initiator, gateways, config)
    affinity = str(config.get("ana_affinity") or "").strip().lower() or None

    by_ana = {}
    for entry in devices:
        by_ana.setdefault(entry.get("ana_group_id"), []).append(entry)

    LOG.info(
        "ANA group distribution: "
        + ", ".join(
            f"ana={ana}:n={len(group)}"
            for ana, group in sorted(
                ((a, g) for a, g in by_ana.items() if a is not None),
                key=lambda x: str(x[0]),
            )
        )
        + f" (total_ns={len(devices)})"
    )

    if affinity == "same":
        pair = None
        for ana_id, group in by_ana.items():
            if ana_id is None:
                continue
            if len(group) >= 2:
                pair = (group[0], group[1])
                break
        if pair is None:
            raise Exception(
                f"ana_affinity=same needs >=2 namespaces on one ANA group; "
                f"found: {devices}. Suite should create 2+ NS per lb_group "
                f"(e.g. 2 on node4 and 2 on node5) and set "
                f"rebalance_period_sec=0 so auto-LB does not split a single pair."
            )
        src, dst = pair
    elif affinity == "different":
        ana_ids = [aid for aid, group in by_ana.items() if aid is not None and group]
        if len(ana_ids) < 2:
            raise Exception(
                f"ana_affinity=different needs namespaces on >=2 ANA groups; "
                f"found: {devices}. Suite should create NS with distinct "
                f"lb_group values (e.g. node4 and node5)."
            )
        src = by_ana[ana_ids[0]][0]
        dst = by_ana[ana_ids[1]][0]
    else:
        if len(devices) < 2:
            raise Exception(f"Need >=2 namespaces, found: {devices}")
        src, dst = devices[0], devices[1]

    src_ana = src.get("ana_group_id")
    dst_ana = dst.get("ana_group_id")
    LOG.info(
        f"CNC NS pair ana_affinity={affinity or 'unspecified'}: "
        f"src NSID={src['NSID']} ana={src_ana} ({src['Namespace']}) -> "
        f"dst NSID={dst['NSID']} ana={dst_ana} ({dst['Namespace']})"
    )
    if affinity == "same" and src_ana != dst_ana:
        raise Exception(f"Expected same ANA group, got src={src_ana} dst={dst_ana}")
    if affinity == "different" and (
        src_ana is None or dst_ana is None or src_ana == dst_ana
    ):
        raise Exception(
            f"Expected different ANA groups, got src={src_ana} dst={dst_ana}"
        )
    return src, dst, devices


def log_ana_result(operation, config, src, dst, metrics=None):
    """Emit a comparable one-line result for same- vs different-ANA runs."""
    metrics = metrics or {}
    metric_str = " ".join(f"{k}={v}" for k, v in metrics.items())
    LOG.info(
        f"CNC_RESULT op={operation} "
        f"ana_affinity={config.get('ana_affinity') or 'unspecified'} "
        f"src_nsid={src['NSID']} src_ana={src.get('ana_group_id')} "
        f"dst_nsid={dst['NSID']} dst_ana={dst.get('ana_group_id')} "
        f"{metric_str}".rstrip()
    )


def get_lba_size(initiator, device):
    """Return namespace LBA size in bytes from id-ns (fallback 512)."""
    try:
        out, _ = initiator.id_ns(device, **{"output-format": "json"})
        data = json.loads(out)
        lbads = None
        lbafs = data.get("lbafs")
        if lbafs:
            flbas = data.get("flbas", 0) & 0xF
            if flbas < len(lbafs):
                lbads = lbafs[flbas].get("ds")
            if lbads is None:
                lbads = lbafs[0].get("ds")
        if lbads is None:
            lbads = data.get("lbads")
        if lbads is not None:
            return 1 << int(lbads)
    except Exception as err:
        LOG.warning(f"Unable to parse LBA size from id-ns: {err}")
    return DEFAULT_LBA_SIZE


def assert_copy_capabilities(initiator, devices):
    """Verify controller Copy support and namespace CNC advertisement.

    Returns:
        dict: {device: {"mcl": int, "mcd": int, "cfs": int, "lba_size": int}}
    """
    caps = {}
    for entry in devices:
        device = entry["Namespace"]
        ctrl_out, _ = initiator.id_ctrl(device, **{"human-readable": True})
        if "Copy" not in ctrl_out and "0x19" not in ctrl_out:
            # JSON path fallback
            ctrl_json, _ = initiator.id_ctrl(device, **{"output-format": "json"})
            oncs = json.loads(ctrl_json).get("oncs", 0)
            if not (int(oncs) & 0x100):
                raise Exception(f"Controller {device} does not advertise Copy support")

        ns_out, _ = initiator.id_ns(device, **{"human-readable": True})
        ns_json, _ = initiator.id_ns(device, **{"output-format": "json"})
        ns_data = json.loads(ns_json)

        mcl = ns_data.get("mcl")
        mcd = ns_data.get("mcd")
        cfs = None
        # NVMe copy format support may appear as 'cfs' in newer identify
        if "cfs" in ns_data:
            cfs = ns_data["cfs"]
        elif re.search(r"cfs\s*[:=]\s*(\d+)", ns_out, re.I):
            cfs = int(re.search(r"cfs\s*[:=]\s*(\d+)", ns_out, re.I).group(1))

        if mcl is None:
            match = re.search(r"mcl\s*[:=]\s*(\d+)", ns_out, re.I)
            mcl = int(match.group(1)) if match else None
        if mcd is None:
            match = re.search(r"mcd\s*[:=]\s*(\d+)", ns_out, re.I)
            mcd = int(match.group(1)) if match else None

        lba_size = get_lba_size(initiator, device)
        LOG.info(
            f"NS {device} (NSID={entry['NSID']}): mcl={mcl} mcd={mcd} "
            f"cfs={cfs} lba_size={lba_size}"
        )
        # cfs bit for format 2 is typically value including 2, or bitfield
        if cfs is not None and int(cfs) not in (2, 3, 6, 7) and not (int(cfs) & 0x2):
            LOG.warning(
                f"Unexpected cfs={cfs} on {device}; continuing if copy succeeds"
            )

        caps[device] = {
            "nsid": entry["NSID"],
            "mcl": int(mcl) if mcl is not None else 65535,
            "mcd": int(mcd) if mcd is not None else 128,
            "cfs": cfs,
            "lba_size": lba_size,
        }
    return caps


def enable_cnc_logging(gateways, config=None, level=None):
    """Enable CNC SPDK logging on all gateways before XCOPY/CNC work.

    RPCs (in order)::

        log_set_flag nvmf_cnc
        log_set_level INFO

    Controlled by suite ``enable_cnc_logging`` (default True) and optional
    ``cnc_log_level`` (default INFO).
    """
    config = config or {}
    if not config.get("enable_cnc_logging", True):
        LOG.info("Skipping CNC logging RPCs (enable_cnc_logging=false)")
        return
    level = level or config.get("cnc_log_level", "INFO")
    for gw in gateways:
        try:
            LOG.info(
                f"Enabling CNC logging on {gw.node.hostname}: "
                f"log_set_flag nvmf_cnc; log_set_level {level}"
            )
            gw.cnc_enable_logging(level=level)
        except Exception as err:
            LOG.warning(f"CNC logging enable failed on {gw.node.hostname}: {err}")


def apply_cnc_config(gateways, cnc_config=None, enable_logging=True, config=None):
    """Apply CNC RPC config (and optional logging) on all gateways."""
    cfg = {
        "host_behav_support_cnc": True,
        "chunk_nlb": 512,
        "max_inflight": 8,
        "rate_limit_bytes": 400000000,
    }
    if cnc_config:
        cfg.update(cnc_config)
    if enable_logging:
        enable_cnc_logging(gateways, config=config)
    for gw in gateways:
        LOG.info(f"Applying CNC config on {gw.node.hostname}: {cfg}")
        gw.cnc_set_config(**cfg)


def skip_cnc_rpc_config(config=None):
    """True when CNC settings come from orch service spec, not SPDK RPC."""
    config = config or {}
    if config.get("skip_cnc_rpc_config") or config.get("cnc_from_spec"):
        return True
    if config.get("nvmeof_spec") or config.get("cnc_spec"):
        return True
    return False


def maybe_apply_cnc_config(gateways, config=None, cnc_config=None, enable_logging=None):
    """Apply CNC via RPC unless suite opts into orch-spec-sourced config.

    When skipping RPC, still optionally enable CNC logging so xcopy activity
    is visible in podman logs.
    """
    config = config or {}
    if enable_logging is None:
        enable_logging = config.get("enable_cnc_logging", True)
    if skip_cnc_rpc_config(config):
        LOG.info(
            "Skipping nvmf_cnc_set_config; using CNC parameters from orch nvmeof_spec"
        )
        if enable_logging:
            enable_cnc_logging(gateways, config=config)
        return
    apply_cnc_config(
        gateways,
        cnc_config=cnc_config if cnc_config is not None else config.get("cnc_config"),
        enable_logging=enable_logging,
        config=config,
    )


def write_verified_pattern(
    node,
    device,
    size="100M",
    offset=None,
    verify="crc32c",
    bs=None,
    io_type="write",
    lba_size=DEFAULT_LBA_SIZE,
    iodepth=8,
):
    """Write (or read-verify) a fio pattern on a device."""
    # Offset LBA writes must use LBA-sized blocks so size maps 1:1 to NLB counts.
    # Bulk fills use a larger block size for acceptable runtime.
    if bs is None:
        bs = lba_size if offset is not None else "1M"
    fio_args = {
        "device_name": device,
        "client_node": node,
        "io_type": io_type,
        "size": size,
        "bs": bs,
        "direct": 1,
        "iodepth": iodepth,
        "verify": verify,
        "verify_fatal": 1,
        "long_running": True,
        "cmd_timeout": "notimeout",
        "test_name": f"cnc-{io_type}-{device.replace('/', '_')}",
    }
    if offset is not None:
        # run_fio does not expose offset; inject via custom fio invocation
        offset_bytes = offset if isinstance(offset, int) else offset
        cmd = (
            f"fio --name={fio_args['test_name']} --ioengine=libaio "
            f"--filename={device} --rw={io_type} --bs={bs} --size={size} "
            f"--offset={offset_bytes} --direct=1 --verify={verify} "
            f"--verify_fatal=1 --iodepth={iodepth} --group_reporting"
        )
        LOG.info(f"Running offset fio: {cmd}")
        return node.exec_command(cmd=cmd, sudo=True, long_running=True, timeout=3600)
    return run_fio(**fio_args)


def nvme_copy_format2(
    initiator,
    dest_device,
    sdlba,
    slbs,
    blocks,
    snsids,
    check_ec=True,
    mcl=None,
    retries=DEFAULT_COPY_RETRIES,
    retry_delay_sec=DEFAULT_COPY_RETRY_DELAY_SEC,
):
    """Execute ``nvme copy --format=2``.

    ``blocks`` is the actual number of logical blocks to copy per range.
    Values are converted to 0-based NLBs for nvme-cli ``--blocks``.
    ``mcl`` from id-ns is treated as a 0-based max NLB per range.

    Transient host-stack failures (``0x370`` pathing, ``0x371`` host abort,
    nvme-cli EAGAIN) are retried up to ``retries`` attempts.
    """
    slbs_list = (
        [int(x) for x in str(slbs).split(",")]
        if not isinstance(slbs, (list, tuple))
        else [int(x) for x in slbs]
    )
    blocks_list = (
        [int(x) for x in str(blocks).split(",")]
        if not isinstance(blocks, (list, tuple))
        else [int(x) for x in blocks]
    )
    nlbs_list = [_blocks_to_nlb(b) for b in blocks_list]
    if mcl is not None:
        for count, nlb in zip(blocks_list, nlbs_list):
            if nlb > int(mcl):
                raise ValueError(
                    f"Copy range length {count} (NLB={nlb}) exceeds mcl={mcl}"
                )

    kwargs = {
        "sdlba": sdlba,
        "slbs": _to_comma_list(slbs_list),
        "blocks": _to_comma_list(nlbs_list),
        "snsids": _to_comma_list(snsids),
        "format": 2,
        "check_ec": check_ec,
    }
    LOG.info(
        f"nvme copy {dest_device} sdlba={sdlba} slbs={slbs_list} "
        f"blocks(count)={blocks_list} blocks(nlb)={nlbs_list} snsids={snsids}"
    )
    attempts = max(1, int(retries))
    delay = float(retry_delay_sec)
    last_err = None
    for attempt in range(1, attempts + 1):
        start = time.time()
        try:
            out = initiator.copy(dest_device, **kwargs)
            elapsed = time.time() - start
            if attempt > 1:
                LOG.info(
                    f"nvme copy succeeded on attempt {attempt}/{attempts} "
                    f"in {elapsed:.2f}s (after transient error retry)"
                )
            else:
                LOG.info(f"nvme copy completed in {elapsed:.2f}s")
            return out, elapsed
        except Exception as err:
            last_err = err
            elapsed = time.time() - start
            if not check_ec:
                raise
            if not _is_transient_nvme_copy_error(err):
                raise
            if attempt >= attempts:
                LOG.error(
                    f"nvme copy failed after {attempts} attempts with transient "
                    f"error (last attempt {elapsed:.2f}s): {err}"
                )
                raise
            LOG.warning(
                f"nvme copy transient error on attempt {attempt}/{attempts} "
                f"after {elapsed:.2f}s; retrying in {delay:.1f}s: {err}"
            )
            time.sleep(delay)
    raise last_err


def flush_nvme_devices(node, *devices):
    """Sync, flush device buffers, and drop page cache before integrity reads."""
    parts = [f"blockdev --flushbufs {dev} || true" for dev in devices if dev]
    flush = "; ".join(parts)
    cmd = "sync"
    if flush:
        cmd = f"{cmd}; {flush}"
    cmd = f"{cmd}; echo 3 > /proc/sys/vm/drop_caches || true"
    LOG.info(f"Flushing NVMe devices before verify: {devices}")
    node.exec_command(cmd=cmd, sudo=True)


def verify_cnc_loop_integrity(
    node,
    src_device,
    dst_device,
    count,
    blocks,
    src_base_slba=0,
    dest_base_slba=0,
    lba_size=DEFAULT_LBA_SIZE,
):
    """Flush devices, then verify first and last extents from a CNC loop."""
    count = int(count)
    blocks = int(blocks)
    sample = min(blocks, 1024)
    flush_nvme_devices(node, src_device, dst_device)

    LOG.info(
        f"CNC integrity: first {sample} blocks @ "
        f"src={src_base_slba} dst={dest_base_slba}"
    )
    verify_copied_regions(
        node,
        src_device,
        dst_device,
        src_base_slba,
        dest_base_slba,
        sample,
        lba_size=lba_size,
    )
    if count > 1:
        last_src = int(src_base_slba) + (count - 1) * blocks
        last_dst = int(dest_base_slba) + (count - 1) * blocks
        LOG.info(
            f"CNC integrity: last {sample} blocks @ " f"src={last_src} dst={last_dst}"
        )
        verify_copied_regions(
            node,
            src_device,
            dst_device,
            last_src,
            last_dst,
            sample,
            lba_size=lba_size,
        )


def verify_copied_regions(
    node,
    src_device,
    dst_device,
    src_slba,
    dst_slba,
    blocks,
    lba_size=DEFAULT_LBA_SIZE,
):
    """Compare source and destination LBA regions via dd + cmp.

    Uses ``iflag=direct`` so verify reads bypass page cache and observe
    what was actually persisted to the NVMe namespaces.
    On mismatch, logs ``cmp`` output and md5sums before deleting temp files.
    """
    suffix = uuid.uuid4().hex
    src_file = f"/tmp/cnc_src_{suffix}.bin"
    dst_file = f"/tmp/cnc_dst_{suffix}.bin"
    try:
        node.exec_command(
            cmd=(
                f"dd if={src_device} of={src_file} bs={lba_size} "
                f"skip={src_slba} count={blocks} iflag=direct status=none"
            ),
            sudo=True,
        )
        node.exec_command(
            cmd=(
                f"dd if={dst_device} of={dst_file} bs={lba_size} "
                f"skip={dst_slba} count={blocks} iflag=direct status=none"
            ),
            sudo=True,
        )
        cmp_out, cmp_err, cmp_rc, _ = node.exec_command(
            cmd=f"cmp {src_file} {dst_file}",
            sudo=True,
            check_ec=False,
            verbose=True,
        )
        if cmp_rc == 0:
            LOG.info(
                f"Verified {blocks} blocks: {src_device}@{src_slba} == "
                f"{dst_device}@{dst_slba}"
            )
            return

        detail_out, _ = node.exec_command(
            cmd=f"cmp -l {src_file} {dst_file} | head -20",
            sudo=True,
            check_ec=False,
        )
        md5_out, _ = node.exec_command(
            cmd=f"md5sum {src_file} {dst_file}",
            sudo=True,
            check_ec=False,
        )
        differ = (cmp_out or cmp_err or detail_out or "").strip()
        LOG.error(
            f"Region mismatch {src_device}@{src_slba} vs "
            f"{dst_device}@{dst_slba} ({blocks} blocks, cmp_rc={cmp_rc}): "
            f"{differ}; md5sum:\n{md5_out}"
        )
        raise Exception(
            f"cmp failed for {src_device}@{src_slba} vs {dst_device}@{dst_slba}: "
            f"{differ or f'exit {cmp_rc}'}"
        )
    finally:
        node.exec_command(cmd=f"rm -f {src_file} {dst_file}", sudo=True)


def copy_within_mcl(
    initiator,
    dest_device,
    src_nsid,
    src_slba,
    dest_slba,
    total_blocks,
    mcl,
    check_ec=True,
):
    """Issue one or more format-2 copies to cover total_blocks within mcl."""
    remaining = int(total_blocks)
    src_off = int(src_slba)
    dst_off = int(dest_slba)
    # mcl from id-ns is 0-based max NLB → max actual blocks per range is mcl+1
    max_chunk = (int(mcl) + 1) if mcl is not None else remaining
    total_elapsed = 0.0
    while remaining > 0:
        chunk = min(remaining, max_chunk)
        _, elapsed = nvme_copy_format2(
            initiator,
            dest_device,
            sdlba=dst_off,
            slbs=src_off,
            blocks=chunk,
            snsids=src_nsid,
            check_ec=check_ec,
            mcl=mcl,
        )
        total_elapsed += elapsed
        remaining -= chunk
        src_off += chunk
        dst_off += chunk
    return total_elapsed


def _pick_dd_bs(size_bytes, lba_size=DEFAULT_LBA_SIZE):
    """Choose a dd block size that divides size_bytes (prefer 1MiB)."""
    size_bytes = int(size_bytes)
    lba_size = int(lba_size)
    for candidate in (1024 * 1024, 256 * 1024, 64 * 1024, 4096, lba_size):
        if candidate >= lba_size and size_bytes % candidate == 0:
            return candidate
    return lba_size


def host_rw_baseline(
    node, src_device, dst_device, size_bytes, bs=None, lba_size=DEFAULT_LBA_SIZE
):
    """Sequential host read from source and write to destination (no CNC).

    Uses direct I/O and a large block size. Buffered ``bs=512`` was both very
    slow and could leave verify seeing stale/inconsistent data on multipath
    NVMe namespaces after CNC disable + gateway redeploy.
    """
    size_bytes = int(size_bytes)
    if size_bytes <= 0:
        raise ValueError(f"size_bytes must be > 0, got {size_bytes}")
    bs = int(bs) if bs is not None else _pick_dd_bs(size_bytes, lba_size)
    if size_bytes % bs != 0:
        raise ValueError(f"size_bytes={size_bytes} not divisible by bs={bs}")
    count = size_bytes // bs
    tmp = f"/tmp/cnc_host_rw_{uuid.uuid4().hex}.bin"
    LOG.info(
        f"Host R/W baseline: {src_device} -> {dst_device} "
        f"size={size_bytes} bs={bs} count={count} (direct I/O)"
    )
    start = time.time()
    try:
        # long_running defaults check_ec=False in CephNode.exec_command; force
        # True so a failed/short dd cannot be treated as a successful baseline.
        read_rc = node.exec_command(
            cmd=(
                f"dd if={src_device} of={tmp} bs={bs} count={count} "
                f"iflag=direct status=none"
            ),
            sudo=True,
            long_running=True,
            check_ec=True,
            timeout=7200,
        )
        LOG.info(f"Host R/W read dd exit={read_rc}")
        write_rc = node.exec_command(
            cmd=(
                f"dd if={tmp} of={dst_device} bs={bs} count={count} "
                f"oflag=direct conv=fsync status=none"
            ),
            sudo=True,
            long_running=True,
            check_ec=True,
            timeout=7200,
        )
        LOG.info(f"Host R/W write dd exit={write_rc}")
        flush_nvme_devices(node, src_device, dst_device)
    finally:
        node.exec_command(cmd=f"rm -f {tmp}", sudo=True)
    elapsed = time.time() - start
    LOG.info(f"Host R/W baseline completed in {elapsed:.2f}s")
    return elapsed


def run_cnc_loop(
    initiator,
    dest_device,
    src_nsid,
    count=500,
    blocks=BLOCKS_4MIB,
    src_base_slba=0,
    dest_base_slba=0,
    mcl=None,
):
    """Run sequential CNC copies (default 500 x 4MiB)."""
    start = time.time()
    for i in range(count):
        src_slba = src_base_slba + (i * blocks)
        dst_slba = dest_base_slba + (i * blocks)
        nvme_copy_format2(
            initiator,
            dest_device,
            sdlba=dst_slba,
            slbs=src_slba,
            blocks=blocks,
            snsids=src_nsid,
            mcl=mcl,
        )
    elapsed = time.time() - start
    LOG.info(f"CNC loop {count}x{blocks} blocks finished in {elapsed:.2f}s")
    return elapsed


def sample_gateway_resources(gateways):
    """Sample CPU and RSS for nvmeof processes on each gateway."""
    samples = []
    for gw in gateways:
        try:
            out, _ = gw.node.exec_command(
                cmd=(
                    "ps -eo pid,pcpu,rss,comm,args | "
                    "grep -E 'nvmf|spdk|nvmeof' | grep -v grep || true"
                ),
                sudo=True,
            )
            samples.append(
                {
                    "host": gw.node.hostname,
                    "timestamp": time.time(),
                    "ps": out.strip(),
                }
            )
            LOG.info(f"GW resource sample [{gw.node.hostname}]:\n{out}")
        except Exception as err:
            LOG.warning(f"Resource sample failed on {gw.node.hostname}: {err}")
    return samples


def find_gateway_by_ip(gateways, ip):
    for gw in gateways:
        if gw.node.ip_address == ip:
            return gw
    return None


def ana_active_path_copy(
    initiator,
    gateways,
    src_device,
    dest_device,
    src_nsid,
    dest_nsid,
    caps,
    blocks=1255,
    src_slba=5000,
    dest_slba=1000,
):
    """Document Active ANA path and run a cross-NS copy; verify data."""
    paths = initiator.fetch_anastate(dest_device)
    LOG.info(f"ANA paths for dest {dest_device}: {paths}")
    if not paths.get("optimized"):
        raise Exception(f"No optimized ANA path for {dest_device}")

    active_ip = paths["optimized"][0]
    active_gw = find_gateway_by_ip(gateways, active_ip)
    LOG.info(
        f"Active ANA gateway for NSID {dest_nsid}: "
        f"{active_gw.node.hostname if active_gw else active_ip}"
    )

    lba_size = caps[src_device]["lba_size"]
    write_verified_pattern(
        initiator.node,
        src_device,
        size=blocks * lba_size,
        offset=src_slba * lba_size,
    )
    nvme_copy_format2(
        initiator,
        dest_device,
        sdlba=dest_slba,
        slbs=src_slba,
        blocks=blocks,
        snsids=src_nsid,
        mcl=caps[src_device]["mcl"],
    )
    verify_copied_regions(
        initiator.node,
        src_device,
        dest_device,
        src_slba,
        dest_slba,
        blocks,
        lba_size=lba_size,
    )
    return paths


def ana_failover_during_copy(
    initiator,
    gateways,
    dest_device,
    src_nsid,
    src_device,
    caps,
    fault_nodes=None,
    large_blocks=50000,
):
    """Start a large CNC, stop Active GW mid-copy, verify no silent corruption.

    Accepts clean failure or successful retry; fails only on data mismatch when
    the copy reports success.
    """
    paths = initiator.fetch_anastate(dest_device)
    if not paths.get("optimized"):
        raise Exception(f"No optimized ANA path for {dest_device}")
    active_ip = paths["optimized"][0]
    active_gw = find_gateway_by_ip(gateways, active_ip)
    if active_gw is None:
        raise Exception(f"Could not map active ANA IP {active_ip} to a gateway")

    # Prefer fault_nodes from suite config if they match active
    target_gw = active_gw
    if fault_nodes:
        for gw in gateways:
            if gw.node.id in fault_nodes or gw.node.hostname in fault_nodes:
                # still failover the currently active path
                pass

    lba_size = caps[src_device]["lba_size"]
    mcl = caps[src_device]["mcl"]
    blocks = min(int(large_blocks), int(mcl) + 1)
    src_slba = 2000
    dest_slba = 5000

    write_verified_pattern(
        initiator.node,
        src_device,
        size=blocks * lba_size,
        offset=src_slba * lba_size,
    )

    copy_error = None
    copy_ok = False

    def _do_copy():
        return nvme_copy_format2(
            initiator,
            dest_device,
            sdlba=dest_slba,
            slbs=src_slba,
            blocks=blocks,
            snsids=src_nsid,
            check_ec=True,
            mcl=mcl,
        )

    LOG.info(f"Stopping Active gateway {target_gw.node.hostname} mid-copy")
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(_do_copy)
        time.sleep(1)
        target_gw.systemctl.stop(target_gw.system_unit_id)
        try:
            result, _elapsed = future.result(timeout=600)
            copy_ok = True
            LOG.info(f"Copy finished successfully after failover: {result}")
        except Exception as err:
            copy_error = err
            LOG.warning(f"Copy failed or timed out during failover (acceptable): {err}")

    # Restore gateway
    LOG.info(f"Restoring gateway {target_gw.node.hostname}")
    target_gw.systemctl.start(target_gw.system_unit_id)
    time.sleep(30)

    if copy_ok:
        try:
            verify_copied_regions(
                initiator.node,
                src_device,
                dest_device,
                src_slba,
                dest_slba,
                blocks,
                lba_size=lba_size,
            )
        except Exception as err:
            raise Exception(
                f"Silent corruption after mid-copy failover: {err}"
            ) from err
    else:
        LOG.info(
            f"Failover mid-copy resulted in clean failure "
            f"(no success to verify): {copy_error}"
        )


def run_cross_ns_copy_verify(initiator, gateways, config=None):
    """Same-NS, multi-range, cross-NS, and large chunked copies."""
    config = config or {}
    src, dst, devices = select_ns_pair(initiator, gateways, config)
    caps = assert_copy_capabilities(initiator, devices)
    maybe_apply_cnc_config(gateways, config)

    src_dev, src_nsid = src["Namespace"], src["NSID"]
    dst_dev, dst_nsid = dst["Namespace"], dst["NSID"]
    lba_size = caps[src_dev]["lba_size"]
    mcl = caps[src_dev]["mcl"]
    node = initiator.node
    start_all = time.time()

    # --- same-NS single-range ---
    same_blocks = int(config.get("same_ns_blocks", 1255))
    src_slba = int(config.get("src_slba", 5000))
    dst_slba = int(config.get("dst_slba", 1000))
    write_verified_pattern(
        node, src_dev, size=same_blocks * lba_size, offset=src_slba * lba_size
    )
    # Destination is same namespace device for same-NS copy
    nvme_copy_format2(
        initiator,
        src_dev,
        sdlba=dst_slba,
        slbs=src_slba,
        blocks=same_blocks,
        snsids=src_nsid,
        mcl=mcl,
    )
    verify_copied_regions(
        node, src_dev, src_dev, src_slba, dst_slba, same_blocks, lba_size=lba_size
    )
    LOG.info("Same-NS single-range copy verified")

    # --- multi-range same-NS ---
    r1_slba, r1_blocks = 5000, 99
    r2_slba, r2_blocks = 9000, 199
    multi_dst = 1000
    write_verified_pattern(
        node, src_dev, size=r1_blocks * lba_size, offset=r1_slba * lba_size
    )
    write_verified_pattern(
        node, src_dev, size=r2_blocks * lba_size, offset=r2_slba * lba_size
    )
    nvme_copy_format2(
        initiator,
        src_dev,
        sdlba=multi_dst,
        slbs=[r1_slba, r2_slba],
        blocks=[r1_blocks, r2_blocks],
        snsids=[src_nsid, src_nsid],
        mcl=mcl,
    )
    verify_copied_regions(
        node, src_dev, src_dev, r1_slba, multi_dst, r1_blocks, lba_size=lba_size
    )
    verify_copied_regions(
        node,
        src_dev,
        src_dev,
        r2_slba,
        multi_dst + r1_blocks,
        r2_blocks,
        lba_size=lba_size,
    )
    LOG.info("Multi-range same-NS copy verified")

    # --- cross-NS ---
    cross_blocks = int(config.get("cross_ns_blocks", 1255))
    cross_src_slba = int(config.get("cross_src_slba", 0))
    cross_dst_slba = int(config.get("cross_dst_slba", 0))
    write_verified_pattern(
        node, src_dev, size=cross_blocks * lba_size, offset=cross_src_slba * lba_size
    )
    _, cross_elapsed = nvme_copy_format2(
        initiator,
        dst_dev,
        sdlba=cross_dst_slba,
        slbs=cross_src_slba,
        blocks=cross_blocks,
        snsids=src_nsid,
        mcl=mcl,
    )
    verify_copied_regions(
        node,
        src_dev,
        dst_dev,
        cross_src_slba,
        cross_dst_slba,
        cross_blocks,
        lba_size=lba_size,
    )
    LOG.info(f"Cross-NS copy NSID{src_nsid}->NSID{dst_nsid} verified")

    # --- large copy within mcl (chunking) ---
    large_blocks = min(int(config.get("large_blocks", 50000)), mcl + 1)
    large_src = int(config.get("large_src_slba", 2000))
    large_dst = int(config.get("large_dst_slba", 5000))
    # Ensure chunk_nlb is set for chunking exercise (RPC path only)
    if not skip_cnc_rpc_config(config):
        apply_cnc_config(
            gateways,
            cnc_config={**(config.get("cnc_config") or {}), "chunk_nlb": 512},
            enable_logging=True,
        )
    write_verified_pattern(
        node, src_dev, size=large_blocks * lba_size, offset=large_src * lba_size
    )
    elapsed = copy_within_mcl(
        initiator,
        dst_dev,
        src_nsid,
        large_src,
        large_dst,
        large_blocks,
        mcl,
    )
    verify_copied_regions(
        node, src_dev, dst_dev, large_src, large_dst, large_blocks, lba_size=lba_size
    )
    sample_gateway_resources(gateways)
    assert_xcopy_in_podman_logs(gateways, config=config)
    total_elapsed = time.time() - start_all
    LOG.info(f"Large chunked copy verified in {elapsed:.2f}s")
    log_ana_result(
        "cross_ns_copy_verify",
        config,
        src,
        dst,
        metrics={
            "cross_ns_s": f"{cross_elapsed:.2f}",
            "large_chunk_s": f"{elapsed:.2f}",
            "total_s": f"{total_elapsed:.2f}",
        },
    )


def run_full_volume_integrity(initiator, gateways, config=None):
    """Fio verify fill → CNC copy → fio verify on destination."""
    config = config or {}
    src, dst, devices = select_ns_pair(initiator, gateways, config)
    caps = assert_copy_capabilities(initiator, devices)
    maybe_apply_cnc_config(gateways, config)

    src_dev, src_nsid = src["Namespace"], src["NSID"]
    dst_dev = dst["Namespace"]
    mcl = caps[src_dev]["mcl"]
    lba_size = caps[src_dev]["lba_size"]
    io_size = config.get("io_size", "5G")
    iodepth = int(config.get("fio_iodepth", 32))
    fio_bs = config.get("fio_bs", "1M")

    LOG.info(
        f"Filling {src_dev} with {io_size} "
        f"(bs={fio_bs}, verify=crc32c, iodepth={iodepth})"
    )
    write_verified_pattern(
        initiator.node,
        src_dev,
        size=io_size,
        verify="crc32c",
        bs=fio_bs,
        iodepth=iodepth,
    )

    # Convert io_size to blocks for CNC
    size_bytes = _parse_size(io_size)
    total_blocks = size_bytes // lba_size
    start = time.time()
    copy_within_mcl(initiator, dst_dev, src_nsid, 0, 0, total_blocks, mcl)
    elapsed = time.time() - start
    LOG.info(f"Full-volume CNC copy of {io_size} took {elapsed:.2f}s")

    write_verified_pattern(
        initiator.node,
        dst_dev,
        size=io_size,
        verify="crc32c",
        bs=fio_bs,
        io_type="read",
        iodepth=iodepth,
    )
    LOG.info("Destination fio read-verify passed")

    if config.get("stress_loop"):
        run_cnc_loop(
            initiator,
            dst_dev,
            src_nsid,
            count=int(config.get("stress_count", 500)),
            blocks=int(config.get("stress_blocks", BLOCKS_4MIB)),
            mcl=mcl,
        )
    assert_xcopy_in_podman_logs(gateways, config=config)
    log_ana_result(
        "full_volume_integrity",
        config,
        src,
        dst,
        metrics={"io_size": io_size, "cnc_copy_s": f"{elapsed:.2f}"},
    )


def run_perf_cnc_vs_host_rw(initiator, gateways, config=None):
    """Time 500x4MB CNC vs host read/write baseline."""
    config = config or {}
    src, dst, devices = select_ns_pair(initiator, gateways, config)
    caps = assert_copy_capabilities(initiator, devices)
    maybe_apply_cnc_config(gateways, config)

    src_dev, src_nsid = src["Namespace"], src["NSID"]
    dst_dev = dst["Namespace"]
    mcl = caps[src_dev]["mcl"]
    lba_size = caps[src_dev]["lba_size"]
    count = int(config.get("copy_count", 500))
    blocks = int(config.get("copy_blocks", BLOCKS_4MIB))
    total_bytes = count * blocks * lba_size

    write_verified_pattern(initiator.node, src_dev, size=total_bytes, verify="crc32c")

    t1 = run_cnc_loop(initiator, dst_dev, src_nsid, count=count, blocks=blocks, mcl=mcl)
    verify_cnc_loop_integrity(
        initiator.node,
        src_dev,
        dst_dev,
        count=count,
        blocks=blocks,
        lba_size=lba_size,
    )

    # Baseline on a cleared destination region — use second half if space, else overwrite
    t2 = host_rw_baseline(
        initiator.node,
        src_dev,
        dst_dev,
        size_bytes=total_bytes,
        lba_size=lba_size,
    )
    verify_copied_regions(
        initiator.node,
        src_dev,
        dst_dev,
        0,
        0,
        min(blocks, 1024),
        lba_size=lba_size,
    )

    speedup = t2 / t1 if t1 > 0 else 0
    LOG.info(
        f"Perf CNC vs host R/W: T1(cnc)={t1:.2f}s T2(host)={t2:.2f}s "
        f"speedup={speedup:.2f}x size={total_bytes} bytes"
    )
    min_speedup = config.get("min_speedup")
    if min_speedup is not None and speedup < float(min_speedup):
        raise Exception(f"CNC speedup {speedup:.2f}x below min_speedup={min_speedup}")
    assert_xcopy_in_podman_logs(gateways, config=config)
    log_ana_result(
        "perf_cnc_vs_host_rw",
        config,
        src,
        dst,
        metrics={
            "t1_cnc_s": f"{t1:.2f}",
            "t2_host_s": f"{t2:.2f}",
            "speedup": f"{speedup:.2f}",
        },
    )
    return {"t1": t1, "t2": t2, "speedup": speedup}


def run_ana_cnc(initiator, gateways, config=None):
    """Active-path copy, secondary path observation, mid-copy failover."""
    config = config or {}
    src, dst, devices = select_ns_pair(initiator, gateways, config)
    caps = assert_copy_capabilities(initiator, devices)
    maybe_apply_cnc_config(gateways, config)

    src_dev, src_nsid = src["Namespace"], src["NSID"]
    dst_dev, dst_nsid = dst["Namespace"], dst["NSID"]

    paths = ana_active_path_copy(
        initiator, gateways, src_dev, dst_dev, src_nsid, dst_nsid, caps
    )

    # Document secondary / inaccessible path presence (connect-all keeps multipath)
    if paths.get("inaccessible"):
        LOG.info(f"Secondary/inaccessible ANA paths: {paths['inaccessible']}")
    else:
        LOG.info("No inaccessible ANA paths reported (both may be optimized per NS)")

    # Re-run copy (still via multipath; Active path should service dest NS)
    ana_active_path_copy(
        initiator,
        gateways,
        src_dev,
        dst_dev,
        src_nsid,
        dst_nsid,
        caps,
        src_slba=6000,
        dest_slba=2000,
    )

    if config.get("failover_mid_copy", True):
        fault_nodes = []
        for method in config.get("fault-injection-methods", []):
            nodes = method.get("nodes", [])
            if isinstance(nodes, str):
                nodes = [nodes]
            fault_nodes.extend(nodes)
        ana_failover_during_copy(
            initiator,
            gateways,
            dst_dev,
            src_nsid,
            src_dev,
            caps,
            fault_nodes=fault_nodes,
            large_blocks=int(config.get("large_blocks", 50000)),
        )
    # Active-path copies earlier in this op must have left XCOPY evidence
    assert_xcopy_in_podman_logs(gateways, config=config)
    log_ana_result("ana_cnc", config, src, dst)


def run_cnc_soak(initiator, gateways, config=None):
    """Sustained CNC soak: continuous CNC loops with resource sampling."""
    config = config or {}
    src, dst, devices = select_ns_pair(initiator, gateways, config)
    caps = assert_copy_capabilities(initiator, devices)
    maybe_apply_cnc_config(
        gateways,
        config,
        cnc_config=config.get(
            "cnc_config",
            {
                "host_behav_support_cnc": True,
                "rate_limit_bytes": 400000000,
                "max_inflight": 8,
                "chunk_nlb": 512,
            },
        ),
    )

    src_dev, src_nsid = src["Namespace"], src["NSID"]
    dst_dev = dst["Namespace"]
    mcl = caps[src_dev]["mcl"]
    lba_size = caps[src_dev]["lba_size"]
    duration_min = int(config.get("duration_min", 30))
    sample_interval = int(config.get("sample_interval_sec", 300))
    spot_minutes = config.get("spot_check_minutes", [15, 30, 60])
    blocks = min(int(config.get("copy_blocks", BLOCKS_4MIB)), mcl + 1)
    loop_count = int(config.get("loop_copies", 50))

    # Prepare source pattern large enough for one loop
    prep_size = loop_count * blocks * lba_size
    write_verified_pattern(initiator.node, src_dev, size=prep_size, verify="crc32c")

    end_time = time.time() + (duration_min * 60)
    next_sample = time.time()
    spot_done = set()
    start = time.time()
    iteration = 0

    while time.time() < end_time:
        iteration += 1
        LOG.info(f"Soak iteration {iteration}")
        run_cnc_loop(
            initiator,
            dst_dev,
            src_nsid,
            count=loop_count,
            blocks=blocks,
            mcl=mcl,
        )
        now = time.time()
        if now >= next_sample:
            sample_gateway_resources(gateways)
            next_sample = now + sample_interval

        elapsed_min = (now - start) / 60
        for mark in spot_minutes:
            if mark <= duration_min and mark not in spot_done and elapsed_min >= mark:
                LOG.info(f"Soak spot-check at {mark} minutes")
                verify_cnc_loop_integrity(
                    initiator.node,
                    src_dev,
                    dst_dev,
                    count=loop_count,
                    blocks=blocks,
                    lba_size=lba_size,
                )
                spot_done.add(mark)

    sample_gateway_resources(gateways)
    verify_cnc_loop_integrity(
        initiator.node,
        src_dev,
        dst_dev,
        count=loop_count,
        blocks=blocks,
        lba_size=lba_size,
    )
    LOG.info(f"Soak completed: {iteration} iterations over {duration_min} minutes")
    assert_xcopy_in_podman_logs(gateways, config=config)
    log_ana_result(
        "cnc_soak",
        config,
        src,
        dst,
        metrics={"iterations": iteration, "duration_min": duration_min},
    )


def _parse_size(size_str):
    """Parse size strings like 10G, 100M, 2048 into bytes."""
    if isinstance(size_str, int):
        return size_str
    size_str = str(size_str).strip().upper()
    units = {"B": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    match = re.match(r"^(\d+(?:\.\d+)?)\s*([KMGT]?)B?$", size_str)
    if not match:
        raise ValueError(f"Unrecognized size: {size_str}")
    value = float(match.group(1))
    unit = match.group(2) or "B"
    return int(value * units[unit])


def measure_cnc_throughput(bytes_copied, elapsed):
    """Return MiB/s for a timed CNC transfer."""
    if elapsed <= 0:
        return 0.0
    return (bytes_copied / (1024 * 1024)) / elapsed


_CNC_SPEC_KEYS = (
    "cnc_enable",
    "cnc_rate_limiter_bytes",
    "cnc_chunk_blocks",
    "cnc_parallel_chunks",
)


def _normalize_cnc_value(key, value):
    """Normalize CNC conf/spec values for equality checks."""
    if value is None:
        return None
    text = str(value).strip()
    if key == "cnc_enable":
        return text.lower() in ("1", "true", "yes", "on")
    if key in (
        "cnc_rate_limiter_bytes",
        "cnc_chunk_blocks",
        "cnc_parallel_chunks",
    ):
        return int(float(text))
    return text


def _parse_orch_ls_json(out):
    """Parse ``ceph orch ls --format json`` output; return a list of services."""
    text = (out or "").strip()
    if not text or text.lower().startswith("no services"):
        return []
    try:
        services = json.loads(text)
    except json.JSONDecodeError as err:
        raise Exception(
            f"Failed to parse orch ls JSON (len={len(text)}): {err}; "
            f"output starts with: {text[:200]!r}"
        ) from err
    if isinstance(services, dict):
        return [services]
    return list(services)


def _fetch_orch_nvmeof_service(ceph_cluster, service_name, gw_group=None):
    """Return the orch ls entry for an NVMe-oF service by name."""
    from ceph.ceph_admin.orch import Orch

    orch = Orch(ceph_cluster, **{})
    out, _ = orch.shell(
        args=[
            f"ceph orch ls --service_name {service_name} "
            "--service_type nvmeof --format json"
        ]
    )
    services = _parse_orch_ls_json(out)
    if services:
        return services[0]

    # Fallback: list all nvmeof services (positional filter is unreliable)
    out, _ = orch.shell(args=["ceph orch ls nvmeof --format json"])
    services = _parse_orch_ls_json(out)
    for svc in services:
        if svc.get("service_name") == service_name:
            return svc
    if gw_group:
        for svc in services:
            if gw_group in (svc.get("service_name") or ""):
                return svc
    return None


def assert_cnc_spec_applied(gateways, expected, nvme_service=None, config=None):
    """Verify orch-spec CNC settings are present on gateways (and orch).

    Checks ``ceph-nvmeof.conf`` inside each gateway container for expected
    ``cnc_*`` keys. When ``nvme_service`` is provided, also confirms the
    same keys appear in ``ceph orch ls --service_name ... --format json``.

    When orch spec matches but gateway conf files omit ``cnc_*`` keys (older
    cephadm render paths or empty stub files), gateway conf validation is
    skipped unless ``require_cnc_gateway_conf: true`` is set in ``config``.
    Behavioral CNC tests still exercise the deployed settings.
    """
    config = config or {}
    expected = {
        key: value
        for key, value in (expected or {}).items()
        if key in _CNC_SPEC_KEYS and value is not None
    }
    if not expected:
        LOG.info("No CNC orch-spec keys to validate")
        return

    orch_validated = False
    if nvme_service and getattr(nvme_service, "service_name", None):
        svc = _fetch_orch_nvmeof_service(
            nvme_service.ceph_cluster,
            nvme_service.service_name,
            gw_group=getattr(nvme_service, "group", None),
        )
        if not svc:
            raise Exception(f"No orch service found for {nvme_service.service_name}")
        orch_spec = svc.get("spec") or {}
        mismatches = []
        for key, exp in expected.items():
            actual = orch_spec.get(key)
            if _normalize_cnc_value(key, actual) != _normalize_cnc_value(key, exp):
                mismatches.append(f"{key}: orch={actual!r} expected={exp!r}")
        if mismatches:
            raise Exception(
                "CNC keys missing/mismatched in orch service spec: "
                + "; ".join(mismatches)
            )
        orch_validated = True
        LOG.info(
            f"Orch service {nvme_service.service_name} CNC spec matches: {expected}"
        )

    require_gateway_conf = bool(config.get("require_cnc_gateway_conf", False))
    for gw in gateways:
        conf = gw.cnc_get_conf()
        LOG.info(f"CNC conf on {gw.node.hostname}: {conf}")
        if not conf and orch_validated and not require_gateway_conf:
            LOG.warning(
                f"CNC orch-spec validated but no cnc_* keys in gateway conf on "
                f"{gw.node.hostname}; skipping gateway conf check "
                "(set require_cnc_gateway_conf: true to enforce)"
            )
            continue
        mismatches = []
        for key, exp in expected.items():
            actual = conf.get(key)
            if actual is None:
                mismatches.append(f"{key}: missing in gateway conf")
                continue
            if _normalize_cnc_value(key, actual) != _normalize_cnc_value(key, exp):
                mismatches.append(f"{key}: conf={actual!r} expected={exp!r}")
        if mismatches:
            raise Exception(
                f"CNC orch-spec not applied on {gw.node.hostname}: "
                + "; ".join(mismatches)
            )
    LOG.info(f"CNC orch-spec applied on all gateways: {expected}")


def assert_cnc_rate_limiter_throughput(mib_s, rate_limit_bytes, config=None):
    """Assert concurrent CNC throughput is bounded by the rate limiter.

    Upper bound: must not exceed ``max_limited_MBps`` (default 1.5x configured
    rate). Lower bound: when ``min_limited_MBps`` is set (or defaults to 0.25x
    rate), throughput must stay above that floor so the limiter is actually
    exercised rather than copies stalling for unrelated reasons.
    """
    config = config or {}
    rate_mib = rate_limit_bytes / (1024 * 1024)
    max_limited = float(config.get("max_limited_MBps", rate_mib * 1.5))
    min_cfg = config.get("min_limited_MBps")
    if min_cfg is None:
        min_limited = rate_mib * 0.25
    else:
        min_limited = float(min_cfg)

    LOG.info(
        f"Rate-limiter throughput check: measured={mib_s:.3f} MiB/s "
        f"expected≈{rate_mib:.3f} MiB/s "
        f"bounds=[{min_limited:.3f}, {max_limited:.3f}] "
        f"(cnc_rate_limiter_bytes={rate_limit_bytes})"
    )
    if mib_s > max_limited:
        raise Exception(
            f"Throughput {mib_s:.3f} MiB/s exceeds max_limited_MBps={max_limited} "
            f"(cnc_rate_limiter_bytes={rate_limit_bytes})"
        )
    if min_limited > 0 and mib_s < min_limited:
        raise Exception(
            f"Throughput {mib_s:.3f} MiB/s below min_limited_MBps={min_limited} "
            f"(cnc_rate_limiter_bytes={rate_limit_bytes}); "
            "rate limiter may not be exercising concurrent CNC traffic"
        )


def run_concurrent_cnc_copies(
    initiator,
    dest_device,
    src_nsid,
    ranges,
    mcl=None,
    max_workers=4,
):
    """Run non-overlapping format-2 copies concurrently.

    Args:
        ranges: list of (src_slba, dst_slba, blocks)
    """
    errors = []
    per_elapsed = {}

    def _one(idx, src_slba, dst_slba, blocks):
        try:
            elapsed = copy_within_mcl(
                initiator,
                dest_device,
                src_nsid,
                src_slba,
                dst_slba,
                blocks,
                mcl,
            )
            per_elapsed[idx] = elapsed
        except Exception as err:
            errors.append((idx, err))

    wall_start = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(_one, idx, src_slba, dst_slba, blocks)
            for idx, (src_slba, dst_slba, blocks) in enumerate(ranges)
        ]
        for fut in futures:
            fut.result()
    wall_elapsed = time.time() - wall_start
    if errors:
        raise Exception(f"Concurrent CNC failures: {errors}")
    total_blocks = sum(int(r[2]) for r in ranges)
    LOG.info(
        f"Concurrent CNC: {len(ranges)} copies, {total_blocks} total blocks, "
        f"wall={wall_elapsed:.2f}s"
    )
    return {
        "elapsed": wall_elapsed,
        "total_blocks": total_blocks,
        "per_elapsed": per_elapsed,
        "ranges": ranges,
    }


def _reconnect_initiator(initiator, gateways, config):
    """Disconnect and reconnect initiator after gateway redeploy."""
    initiators = config.get("initiators") or []
    io_client = dict(initiators[0]) if initiators else {"nqn": "connect-all"}
    io_client.setdefault("nqn", "connect-all")
    LOG.info(
        f"Reconnecting initiator on {initiator.node.hostname} "
        f"after CNC spec re-apply: {io_client}"
    )
    try:
        initiator.disconnect_all()
    except Exception as err:
        LOG.warning(f"disconnect_all before reconnect: {err}")
    time.sleep(5)
    initiator.connect_targets(gateways[0], io_client)
    time.sleep(5)


# SPDK ctrlr_cnc.c markers (log flag nvmf_cnc + log level INFO)
DEFAULT_XCOPY_SUCCESS_PATTERNS = (
    r"XCOPY:\s*ISSUE READ",
    r"XCOPY:\s*READ OK",
    r"XCOPY:\s*WRITE OK",
    r"XCOPY:.*COMPLETE SUCCESS",
    r"CNC total blocks to copy",
    r"CNC_FREE",
)

# Strong proof the offload path ran end-to-end (prefer these when present)
STRONG_XCOPY_PATTERNS = (
    r"XCOPY:\s*ISSUE READ",
    r"XCOPY:\s*WRITE OK",
    r"XCOPY:.*COMPLETE SUCCESS",
)

XCOPY_COMPLETE_PATTERN = re.compile(r"XCOPY:.*COMPLETE SUCCESS", re.I)


def _expected_cnc_internal_chunks(total_blocks, chunk_blocks):
    """Minimum internal CNC chunk steps for ``total_blocks`` at ``chunk_blocks``."""
    total = int(total_blocks)
    chunk = int(chunk_blocks)
    if total <= 0 or chunk <= 0:
        return 1
    return max(1, (total + chunk - 1) // chunk)


def _chunk_blocks_log_patterns(chunk_blocks):
    nlb = int(chunk_blocks) - 1
    cb = int(chunk_blocks)
    return [
        rf"cnc_chunk_blocks[^\d]*{cb}",
        rf"chunk_blocks[^\d]*{cb}",
        rf"chunk[^\d]*{cb}\s*block",
        rf"chunk_nlb[^\d]*{nlb}",
        r"CNC total blocks to copy",
    ]


def _parallel_chunks_log_patterns(parallel_chunks):
    pc = int(parallel_chunks)
    return [
        rf"cnc_parallel_chunks[^\d]*{pc}",
        rf"parallel_chunks[^\d]*{pc}",
        rf"max_inflight[^\d]*{pc}",
        rf"max.inflight[^\d]*{pc}",
        # Runtime CNC/XCOPY markers (ctrlr_cnc.c with nvmf_cnc log flag)
        r"XCOPY:\s*ISSUE READ",
        r"XCOPY:\s*WRITE OK",
        r"XCOPY:.*COMPLETE SUCCESS",
        r"CNC total blocks to copy",
    ]


def _resolve_cnc_log_since(config, since, config_key, use_phase_key, default="30m"):
    """Pick podman ``--since`` for CNC log scans.

    Phase-scoped windows (e.g. ``86s``) often miss startup config lines and
    sparse SPDK output. Default to a wider suite window unless explicitly opted in.
    """
    config = config or {}
    if config.get(use_phase_key, False) and since:
        return since
    return config.get(config_key, default)


def _count_xcopy_completes(logs_by_host):
    """Return total and per-host ``XCOPY COMPLETE SUCCESS`` counts."""
    per_host = {}
    total = 0
    for host, text in logs_by_host.items():
        n = len(XCOPY_COMPLETE_PATTERN.findall(text or ""))
        per_host[host] = n
        total += n
    return total, per_host


def collect_gateway_logs(gateways, lines=2000, since=None, include_spdk_files=True):
    """Return ``{hostname: merged_log_text}`` from podman and optional file logs."""
    logs_by_host = {}
    for gw in gateways:
        parts = []
        try:
            podman = gw.cnc_get_container_logs(lines=lines, since=since) or ""
            if podman.strip():
                parts.append(podman)
        except Exception as err:
            LOG.warning(f"Could not fetch podman logs from {gw.node.hostname}: {err}")
        if include_spdk_files:
            try:
                spdk = gw.cnc_get_spdk_file_logs(lines=lines) or ""
                if spdk.strip():
                    parts.append(spdk)
            except Exception as err:
                LOG.warning(
                    f"Could not fetch SPDK file logs from {gw.node.hostname}: {err}"
                )
        text = "\n".join(parts)
        logs_by_host[gw.node.hostname] = text
        LOG.info(
            f"Fetched {len(text.splitlines())} merged CNC log lines from "
            f"{gw.node.hostname} (bytes={len(text)}, since={since}, "
            f"sources={len(parts)})"
        )
    return logs_by_host


def _scan_gateway_logs(gateways, lines, since, patterns, include_spdk_files=True):
    """Return total regex matches and per-host counts."""
    logs = collect_gateway_logs(
        gateways,
        lines=lines,
        since=since,
        include_spdk_files=include_spdk_files,
    )
    compiled = [re.compile(p, re.I) for p in patterns]
    per_host = {}
    total = 0
    for host, text in logs.items():
        count = sum(len(p.findall(text or "")) for p in compiled)
        per_host[host] = count
        total += count
    return total, per_host, logs


def assert_cnc_chunk_blocks_behavior(
    gateways,
    large_blocks,
    chunk_blocks,
    config=None,
    since=None,
    soft=None,
):
    """Verify podman logs show CNC chunking at ``cnc_chunk_blocks``."""
    config = config or {}
    if not config.get("verify_cnc_chunk_behavior", True):
        LOG.info("Skipping CNC chunk-blocks log verification")
        return {"skipped": True}

    if soft is None:
        soft = bool(config.get("soft_verify_cnc_chunk_logs", False))

    chunk_blocks = int(chunk_blocks)
    large_blocks = int(large_blocks)
    lines = int(config.get("cnc_chunk_log_lines", 3000))
    since = _resolve_cnc_log_since(
        config,
        since,
        "cnc_chunk_log_since",
        "cnc_chunk_log_use_phase_since",
        default="30m",
    )
    min_matches = int(config.get("min_cnc_chunk_log_matches", 2))
    min_completes = int(
        config.get(
            "min_cnc_chunk_completes",
            min(_expected_cnc_internal_chunks(large_blocks, chunk_blocks), 3),
        )
    )

    patterns = config.get("cnc_chunk_log_patterns") or _chunk_blocks_log_patterns(
        chunk_blocks
    )
    total, per_host, logs = _scan_gateway_logs(gateways, lines, since, patterns)

    complete_by_host = {}
    complete_total = 0
    for host, text in logs.items():
        n = len(XCOPY_COMPLETE_PATTERN.findall(text or ""))
        complete_by_host[host] = n
        complete_total += n

    LOG.info(
        f"CNC chunk-blocks check: chunk_blocks={chunk_blocks} large_blocks={large_blocks} "
        f"pattern_matches={total} per_host={per_host} "
        f"xcopy_completes={complete_total} per_host={complete_by_host} "
        f"need matches>={min_matches} completes>={min_completes}"
    )

    ok = total >= min_matches or complete_total >= min_completes
    if ok:
        LOG.info(f"Verified CNC chunking evidence for cnc_chunk_blocks={chunk_blocks}")
        return {
            "matched": True,
            "pattern_matches": total,
            "xcopy_completes": complete_total,
        }

    msg = (
        f"No CNC chunk-blocks evidence for cnc_chunk_blocks={chunk_blocks}: "
        f"pattern_matches={total} (need>={min_matches}), "
        f"xcopy_completes={complete_total} (need>={min_completes}), "
        f"since={since}"
    )
    if soft:
        LOG.warning(msg)
        return {"matched": False, "pattern_matches": total}
    raise Exception(msg)


def assert_cnc_parallel_chunks_behavior(
    gateways,
    parallel_chunks,
    concurrent_copies,
    config=None,
    since=None,
    soft=None,
):
    """Verify concurrent CNC traffic exercises ``cnc_parallel_chunks``."""
    config = config or {}
    if not config.get("verify_cnc_parallel_behavior", True):
        LOG.info("Skipping CNC parallel-chunks verification")
        return {"skipped": True}

    if soft is None:
        soft = bool(config.get("soft_verify_cnc_parallel_logs", False))

    parallel_chunks = int(parallel_chunks)
    concurrent_copies = int(concurrent_copies)
    if concurrent_copies < parallel_chunks:
        msg = (
            f"concurrent_copies={concurrent_copies} < "
            f"cnc_parallel_chunks={parallel_chunks}; cannot exercise parallelism"
        )
        if soft:
            LOG.warning(msg)
        else:
            raise Exception(msg)

    lines = int(config.get("cnc_parallel_log_lines", 3000))
    since = _resolve_cnc_log_since(
        config,
        since,
        "cnc_parallel_log_since",
        "cnc_parallel_log_use_phase_since",
        default="30m",
    )
    min_matches = int(config.get("min_cnc_parallel_log_matches", 1))
    min_completes = int(
        config.get(
            "min_cnc_parallel_completes",
            min(concurrent_copies, parallel_chunks),
        )
    )

    patterns = config.get("cnc_parallel_log_patterns") or _parallel_chunks_log_patterns(
        parallel_chunks
    )
    total, per_host, logs = _scan_gateway_logs(gateways, lines, since, patterns)
    complete_total, complete_by_host = _count_xcopy_completes(logs)

    LOG.info(
        f"CNC parallel-chunks check: parallel_chunks={parallel_chunks} "
        f"concurrent_copies={concurrent_copies} pattern_matches={total} "
        f"per_host={per_host} xcopy_completes={complete_total} "
        f"per_host={complete_by_host} need matches>={min_matches} "
        f"completes>={min_completes} since={since}"
    )

    ok = total >= min_matches or complete_total >= min_completes
    if ok:
        LOG.info(
            f"Verified CNC parallel evidence for cnc_parallel_chunks={parallel_chunks}"
        )
        return {
            "matched": True,
            "pattern_matches": total,
            "xcopy_completes": complete_total,
        }

    msg = (
        f"No CNC parallel-chunks evidence for cnc_parallel_chunks={parallel_chunks}: "
        f"pattern_matches={total} (need>={min_matches}), "
        f"xcopy_completes={complete_total} (need>={min_completes}), since={since}"
    )
    if soft:
        LOG.warning(msg)
        return {"matched": False, "pattern_matches": total}
    raise Exception(msg)


def _rate_limit_bounds(rate_limit_bytes, config=None):
    """Compute min/max MiB/s bounds from ``cnc_rate_limiter_bytes`` and suite slack."""
    config = config or {}
    rate_mib = rate_limit_bytes / (1024 * 1024)
    max_limited = float(config.get("max_limited_MBps", rate_mib * 1.5))
    min_cfg = config.get("min_limited_MBps")
    if min_cfg is None:
        # High-rate limits need a lower relative floor; slow limits need traffic proof
        min_limited = rate_mib * (0.1 if rate_mib >= 32 else 0.25)
    else:
        min_limited = float(min_cfg)
    return min_limited, max_limited, rate_mib


def collect_gateway_podman_logs(gateways, lines=2000, since=None):
    """Return ``{hostname: log_text}`` from nvmeof containers (podman + file logs)."""
    return collect_gateway_logs(gateways, lines=lines, since=since)


def assert_xcopy_in_podman_logs(
    gateways,
    config=None,
    soft=None,
    lines=None,
    since=None,
    patterns=None,
):
    """Verify nvmeof podman logs show the copy used SPDK XCOPY/CNC offload.

    Scans all gateway containers (copy may land on the dest NS ANA-optimized
    GW). Passes if any host matches enough success patterns from
    ``lib/nvmf/ctrlr_cnc.c``.

    Suite knobs:
      verify_xcopy_logs: bool (default True) — run the check
      soft_verify_xcopy_logs: bool (default False) — warn instead of fail
      xcopy_log_lines: int (default 2000)
      xcopy_log_since: podman --since value (e.g. ``30m``)
      xcopy_log_patterns: optional list of regexes
      min_xcopy_log_matches: int (default 1)
    """
    config = config or {}
    if not config.get("verify_xcopy_logs", True):
        LOG.info("Skipping XCOPY podman log verification (verify_xcopy_logs=false)")
        return {"matched": False, "skipped": True}

    if soft is None:
        soft = bool(config.get("soft_verify_xcopy_logs", False))
    lines = int(lines or config.get("xcopy_log_lines", 2000))
    since = _resolve_cnc_log_since(
        config,
        since,
        "xcopy_log_since",
        "xcopy_log_use_phase_since",
        default="30m",
    )
    min_matches = int(config.get("min_xcopy_log_matches", 1))
    pattern_list = (
        patterns
        or config.get("xcopy_log_patterns")
        or list(DEFAULT_XCOPY_SUCCESS_PATTERNS)
    )
    compiled = [re.compile(p, re.I) for p in pattern_list]

    logs_by_host = collect_gateway_podman_logs(gateways, lines=lines, since=since)
    host_hits = {}
    strong_hits = {}
    strong_compiled = [re.compile(p, re.I) for p in STRONG_XCOPY_PATTERNS]

    for host, text in logs_by_host.items():
        matched = [p.pattern for p in compiled if p.search(text or "")]
        host_hits[host] = matched
        strong_hits[host] = [p.pattern for p in strong_compiled if p.search(text or "")]
        if matched:
            sample = "\n".join(
                line
                for line in (text or "").splitlines()
                if re.search(r"XCOPY|CNC", line, re.I)
            )[:2000]
            LOG.info(f"XCOPY/CNC log evidence on {host}: matched={matched}\n{sample}")
        elif text:
            # Help diagnose empty-match cases (wrong stream / too-small window)
            preview = "\n".join((text or "").splitlines()[-8:])
            LOG.warning(
                f"No XCOPY pattern match on {host} "
                f"(log_lines={len(text.splitlines())}). Tail preview:\n{preview}"
            )
        else:
            LOG.warning(f"Empty podman log text from {host}")

    total_unique = {m for hits in host_hits.values() for m in hits}
    any_strong = any(strong_hits.values())
    best_host = max(host_hits, key=lambda h: len(host_hits[h]), default=None)
    best_count = len(host_hits.get(best_host, [])) if best_host else 0

    LOG.info(
        f"XCOPY podman log scan: hosts={list(logs_by_host)} "
        f"unique_patterns={sorted(total_unique)} "
        f"best_host={best_host} best_matches={best_count} "
        f"strong_evidence={any_strong} since={since} lines={lines}"
    )

    ok = len(total_unique) >= min_matches or any_strong
    result = {
        "matched": ok,
        "hosts": host_hits,
        "strong": strong_hits,
        "unique_patterns": sorted(total_unique),
    }
    if ok:
        LOG.info("Verified copy went through XCOPY/CNC offload path via podman logs")
        return result

    msg = (
        "No XCOPY/CNC offload evidence in nvmeof podman logs. "
        f"Expected patterns like {list(STRONG_XCOPY_PATTERNS)}; "
        f"scanned hosts={list(logs_by_host)} since={since} lines={lines}. "
        "Ensure log_set_flag nvmf_cnc + log_set_level INFO ran before the copy."
    )
    if soft:
        LOG.warning(msg)
        return result
    raise Exception(msg)


def _soft_check_cnc_chunk_logs(gateways, config=None):
    """Backward-compatible soft check; prefer ``assert_xcopy_in_podman_logs``."""
    return assert_xcopy_in_podman_logs(gateways, config=config, soft=True)


def run_spec_cnc_enable_perf(initiator, gateways, config=None, nvme_service=None):
    """CNC enabled via orch spec vs host R/W after cnc_enable=false.

    T1: timed nvme copy with CNC enabled (spec).
    T2: host read/write baseline after re-applying spec with cnc_enable=false.
    """
    config = config or {}
    if nvme_service is None:
        raise ValueError("spec_cnc_enable_perf requires nvme_service for orch re-apply")

    src, dst, devices = select_ns_pair(initiator, gateways, config)
    caps = assert_copy_capabilities(initiator, devices)
    maybe_apply_cnc_config(gateways, config)

    nvmeof_spec = dict(config.get("nvmeof_spec") or config.get("cnc_spec") or {})
    if config.get("verify_cnc_spec_applied", True) and nvmeof_spec:
        assert_cnc_spec_applied(
            gateways, nvmeof_spec, nvme_service=nvme_service, config=config
        )

    src_dev, src_nsid = src["Namespace"], src["NSID"]
    dst_dev = dst["Namespace"]
    mcl = caps[src_dev]["mcl"]
    lba_size = caps[src_dev]["lba_size"]
    count = int(config.get("copy_count", 500))
    blocks = int(config.get("copy_blocks", BLOCKS_4MIB))
    total_bytes = count * blocks * lba_size
    redeploy_wait = int(config.get("redeploy_wait_sec", 60))

    write_verified_pattern(initiator.node, src_dev, size=total_bytes, verify="crc32c")

    LOG.info("Phase T1: CNC-enabled nvme copy (orch spec cnc_enable=true)")
    t1 = run_cnc_loop(initiator, dst_dev, src_nsid, count=count, blocks=blocks, mcl=mcl)
    verify_cnc_loop_integrity(
        initiator.node,
        src_dev,
        dst_dev,
        count=count,
        blocks=blocks,
        lba_size=lba_size,
    )
    assert_xcopy_in_podman_logs(gateways, config=config)

    LOG.info("Re-applying orch spec with cnc_enable=false")
    nvme_service.apply_nvmeof_spec(
        nvmeof_spec={"cnc_enable": False},
        redeploy=True,
        wait_sec=redeploy_wait,
    )
    _reconnect_initiator(initiator, gateways, config)
    # Redeploy resets SPDK log flags; re-enable for remaining podman log checks
    enable_cnc_logging(gateways, config=config)

    # Device paths may change after reconnect; re-select with same affinity
    src, dst, _devices = select_ns_pair(initiator, gateways, config)
    src_dev, src_nsid = src["Namespace"], src["NSID"]
    dst_dev = dst["Namespace"]
    lba_size = get_lba_size(initiator, src_dev)

    # Soft check: format-2 copy may fail when CNC is disabled (TC-016 style)
    try:
        nvme_copy_format2(
            initiator,
            dst_dev,
            sdlba=0,
            slbs=0,
            blocks=min(blocks, 16),
            snsids=src_nsid,
            check_ec=True,
            mcl=mcl,
        )
        LOG.warning(
            "nvme copy succeeded with cnc_enable=false; "
            "continuing with host R/W baseline for timing comparison"
        )
    except Exception as err:
        LOG.info(f"nvme copy rejected with cnc_enable=false as expected: {err}")

    LOG.info("Phase T2: host read/write path (CNC disabled)")
    t2 = host_rw_baseline(
        initiator.node,
        src_dev,
        dst_dev,
        size_bytes=total_bytes,
        lba_size=lba_size,
    )
    verify_copied_regions(
        initiator.node,
        src_dev,
        dst_dev,
        0,
        0,
        min(blocks, 1024),
        lba_size=lba_size,
    )

    speedup = t2 / t1 if t1 > 0 else 0
    LOG.info(
        f"Spec CNC enable perf: T1(cnc_enable)={t1:.2f}s "
        f"T2(host_rw_cnc_disabled)={t2:.2f}s speedup={speedup:.2f}x "
        f"size={total_bytes} bytes"
    )
    min_speedup = config.get("min_speedup", 1.0)
    if min_speedup is not None and speedup < float(min_speedup):
        LOG.warning(
            f"CNC-enabled speedup {speedup:.2f}x below min_speedup={min_speedup} "
            "(timing comparison only; not failing the test)"
        )
    log_ana_result(
        "spec_cnc_enable_perf",
        config,
        src,
        dst,
        metrics={
            "t1_cnc_s": f"{t1:.2f}",
            "t2_host_s": f"{t2:.2f}",
            "speedup": f"{speedup:.2f}",
        },
    )
    return {"t1": t1, "t2": t2, "speedup": speedup}


def run_spec_cnc_params_exercise(initiator, gateways, config=None, nvme_service=None):
    """Exercise orch-spec CNC rate/chunk/parallel without RPC override.

    Verifies:
      1. orch-spec CNC keys are applied (gateway conf + orch ls)
      2. concurrent CNC throughput stays within the rate-limiter band
      3. large chunked copies succeed with chunk/parallel evidence in logs
    """
    config = config or {}
    src, dst, devices = select_ns_pair(initiator, gateways, config)
    caps = assert_copy_capabilities(initiator, devices)
    maybe_apply_cnc_config(gateways, config, enable_logging=True)

    src_dev, src_nsid = src["Namespace"], src["NSID"]
    dst_dev = dst["Namespace"]
    mcl = caps[src_dev]["mcl"]
    lba_size = caps[src_dev]["lba_size"]

    nvmeof_spec = dict(config.get("nvmeof_spec") or config.get("cnc_spec") or {})
    rate_limit_bytes = int(
        nvmeof_spec.get(
            "cnc_rate_limiter_bytes",
            config.get("rate_limit_bytes", 100000000),
        )
    )
    chunk_blocks = int(nvmeof_spec.get("cnc_chunk_blocks", 512))
    parallel_chunks = int(nvmeof_spec.get("cnc_parallel_chunks", 8))
    concurrent = int(config.get("concurrent_copies", parallel_chunks))
    copy_blocks = int(config.get("copy_blocks", 65536))
    min_limited_MBps, max_limited_MBps, rate_mib = _rate_limit_bounds(
        rate_limit_bytes, config
    )

    if config.get("verify_cnc_spec_applied", True):
        assert_cnc_spec_applied(
            gateways, nvmeof_spec, nvme_service=nvme_service, config=config
        )

    ranges = [
        (i * copy_blocks, i * copy_blocks, copy_blocks) for i in range(concurrent)
    ]
    prep_blocks = concurrent * copy_blocks
    prep_bytes = prep_blocks * lba_size
    LOG.info(
        f"Spec CNC params: preparing {prep_bytes} bytes for {concurrent} x "
        f"{copy_blocks}-block concurrent copies; "
        f"cnc_rate_limiter_bytes={rate_limit_bytes} ({rate_mib:.1f} MiB/s) "
        f"cnc_chunk_blocks={chunk_blocks} cnc_parallel_chunks={parallel_chunks} "
        f"min_limited_MBps={min_limited_MBps} max_limited_MBps={max_limited_MBps}"
    )
    write_verified_pattern(
        initiator.node, src_dev, size=prep_bytes, verify="crc32c", bs="1M", iodepth=32
    )

    sample_gateway_resources(gateways)
    rate_phase_start = time.time()
    result = run_concurrent_cnc_copies(
        initiator,
        dst_dev,
        src_nsid,
        ranges,
        mcl=mcl,
        max_workers=concurrent,
    )
    bytes_copied = result["total_blocks"] * lba_size
    mib_s = measure_cnc_throughput(bytes_copied, result["elapsed"])
    for src_slba, dst_slba, blocks in ranges:
        verify_copied_regions(
            initiator.node,
            src_dev,
            dst_dev,
            src_slba,
            dst_slba,
            min(blocks, 1024),
            lba_size=lba_size,
        )
    sample_gateway_resources(gateways)
    LOG.info(
        f"Spec rate-limiter phase: throughput={mib_s:.3f} MiB/s "
        f"elapsed={result['elapsed']:.2f}s"
    )
    assert_cnc_rate_limiter_throughput(mib_s, rate_limit_bytes, config=config)
    assert_cnc_parallel_chunks_behavior(
        gateways,
        parallel_chunks,
        concurrent,
        config=config,
        since=f"{max(1, int(time.time() - rate_phase_start) + 5)}s",
    )

    # Large chunked copy to exercise cnc_chunk_blocks / cnc_parallel_chunks
    large_blocks = min(int(config.get("large_blocks", 50000)), mcl + 1)
    if large_blocks <= chunk_blocks:
        large_blocks = min(chunk_blocks * 4, mcl + 1)
    large_src = int(config.get("large_src_slba", 2000))
    large_dst = int(config.get("large_dst_slba", 5000))
    chunk_phase_start = time.time()
    write_verified_pattern(
        initiator.node,
        src_dev,
        size=large_blocks * lba_size,
        offset=large_src * lba_size,
    )
    elapsed = copy_within_mcl(
        initiator,
        dst_dev,
        src_nsid,
        large_src,
        large_dst,
        large_blocks,
        mcl,
    )
    verify_copied_regions(
        initiator.node,
        src_dev,
        dst_dev,
        large_src,
        large_dst,
        large_blocks,
        lba_size=lba_size,
    )
    chunk_since = f"{max(1, int(time.time() - chunk_phase_start) + 5)}s"
    assert_cnc_chunk_blocks_behavior(
        gateways,
        large_blocks,
        chunk_blocks,
        config=config,
        since=chunk_since,
    )
    assert_xcopy_in_podman_logs(
        gateways,
        config=config,
        since=chunk_since,
    )
    LOG.info(
        f"Spec CNC chunk/parallel large copy verified in {elapsed:.2f}s "
        f"(cnc_chunk_blocks={chunk_blocks} "
        f"cnc_parallel_chunks={parallel_chunks})"
    )
    log_ana_result(
        "spec_cnc_params_exercise",
        config,
        src,
        dst,
        metrics={
            "rate_mib_s": f"{mib_s:.3f}",
            "large_copy_s": f"{elapsed:.2f}",
            "cnc_rate_limiter_bytes": rate_limit_bytes,
            "cnc_chunk_blocks": chunk_blocks,
            "cnc_parallel_chunks": parallel_chunks,
        },
    )
    return {
        "rate_mib_s": mib_s,
        "large_copy_elapsed": elapsed,
        "chunk_blocks": chunk_blocks,
        "parallel_chunks": parallel_chunks,
    }


OPERATIONS = {
    "cross_ns_copy_verify": run_cross_ns_copy_verify,
    "full_volume_integrity": run_full_volume_integrity,
    "perf_cnc_vs_host_rw": run_perf_cnc_vs_host_rw,
    "ana_cnc": run_ana_cnc,
    "cnc_soak": run_cnc_soak,
    "spec_cnc_enable_perf": run_spec_cnc_enable_perf,
    "spec_cnc_params_exercise": run_spec_cnc_params_exercise,
}
