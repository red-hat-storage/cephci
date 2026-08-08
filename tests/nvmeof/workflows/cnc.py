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


def _to_comma_list(value):
    if isinstance(value, (list, tuple)):
        return ",".join(str(v) for v in value)
    return str(value)


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


def apply_cnc_config(gateways, cnc_config=None, enable_logging=True):
    """Apply CNC RPC config (and optional logging) on all gateways."""
    cfg = {
        "host_behav_support_cnc": True,
        "chunk_nlb": 512,
        "max_inflight": 8,
        "rate_limit_bytes": 400000000,
    }
    if cnc_config:
        cfg.update(cnc_config)
    for gw in gateways:
        LOG.info(f"Applying CNC config on {gw.node.hostname}: {cfg}")
        if enable_logging:
            try:
                gw.cnc_enable_logging()
            except Exception as err:
                LOG.warning(f"CNC logging enable failed on {gw.node.hostname}: {err}")
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

    When skipping RPC, still optionally enable CNC debug logging so chunk
    activity can be soft-checked in container logs.
    """
    config = config or {}
    if enable_logging is None:
        enable_logging = config.get("enable_cnc_logging", True)
    if skip_cnc_rpc_config(config):
        LOG.info(
            "Skipping nvmf_cnc_set_config; using CNC parameters from orch nvmeof_spec"
        )
        if enable_logging:
            for gw in gateways:
                try:
                    gw.cnc_enable_logging()
                except Exception as err:
                    LOG.warning(
                        f"CNC logging enable failed on {gw.node.hostname}: {err}"
                    )
        return
    apply_cnc_config(
        gateways,
        cnc_config=cnc_config if cnc_config is not None else config.get("cnc_config"),
        enable_logging=enable_logging,
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
):
    """Execute ``nvme copy --format=2``.

    ``blocks`` is the actual number of logical blocks to copy per range.
    Values are converted to 0-based NLBs for nvme-cli ``--blocks``.
    ``mcl`` from id-ns is treated as a 0-based max NLB per range.
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
    start = time.time()
    out = initiator.copy(dest_device, **kwargs)
    elapsed = time.time() - start
    LOG.info(f"nvme copy completed in {elapsed:.2f}s")
    return out, elapsed


def verify_copied_regions(
    node,
    src_device,
    dst_device,
    src_slba,
    dst_slba,
    blocks,
    lba_size=DEFAULT_LBA_SIZE,
):
    """Compare source and destination LBA regions via dd + cmp."""
    suffix = uuid.uuid4().hex
    src_file = f"/tmp/cnc_src_{suffix}.bin"
    dst_file = f"/tmp/cnc_dst_{suffix}.bin"
    try:
        node.exec_command(
            cmd=(
                f"dd if={src_device} of={src_file} bs={lba_size} "
                f"skip={src_slba} count={blocks} status=none"
            ),
            sudo=True,
        )
        node.exec_command(
            cmd=(
                f"dd if={dst_device} of={dst_file} bs={lba_size} "
                f"skip={dst_slba} count={blocks} status=none"
            ),
            sudo=True,
        )
        node.exec_command(cmd=f"cmp {src_file} {dst_file}", sudo=True)
        LOG.info(
            f"Verified {blocks} blocks: {src_device}@{src_slba} == "
            f"{dst_device}@{dst_slba}"
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


def host_rw_baseline(node, src_device, dst_device, size_bytes, bs=DEFAULT_LBA_SIZE):
    """Sequential host read from source and write to destination (no CNC)."""
    tmp = f"/tmp/cnc_host_rw_{int(time.time())}.bin"
    start = time.time()
    try:
        node.exec_command(
            cmd=f"dd if={src_device} of={tmp} bs={bs} count={size_bytes // bs} status=none",
            sudo=True,
            long_running=True,
            timeout=7200,
        )
        node.exec_command(
            cmd=f"dd if={tmp} of={dst_device} bs={bs} count={size_bytes // bs} status=none "
            f"conv=fsync",
            sudo=True,
            long_running=True,
            timeout=7200,
        )
    finally:
        node.exec_command(cmd=f"rm -f {tmp}", sudo=True)
    return time.time() - start


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
    devices = get_ns_devices(initiator)
    caps = assert_copy_capabilities(initiator, devices)
    apply_cnc_config(
        gateways,
        cnc_config=config.get("cnc_config"),
        enable_logging=config.get("enable_cnc_logging", True),
    )

    src = devices[0]
    dst = devices[1]
    src_dev, src_nsid = src["Namespace"], src["NSID"]
    dst_dev, dst_nsid = dst["Namespace"], dst["NSID"]
    lba_size = caps[src_dev]["lba_size"]
    mcl = caps[src_dev]["mcl"]
    node = initiator.node

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
    nvme_copy_format2(
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
    # Ensure chunk_nlb is set for chunking exercise
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
    for gw in gateways:
        try:
            logs = gw.cnc_get_container_logs(lines=100)
            if "cnc" in logs.lower() or "chunk" in logs.lower():
                LOG.info(f"CNC/chunk references found in {gw.node.hostname} logs")
            else:
                LOG.warning(
                    f"No obvious CNC/chunk log lines on {gw.node.hostname} "
                    "(build may use different log tags)"
                )
        except Exception as err:
            LOG.warning(f"Could not fetch CNC logs: {err}")
    LOG.info(f"Large chunked copy verified in {elapsed:.2f}s")


def run_full_volume_integrity(initiator, gateways, config=None):
    """Fio verify fill → CNC copy → fio verify on destination."""
    config = config or {}
    devices = get_ns_devices(initiator)
    caps = assert_copy_capabilities(initiator, devices)
    apply_cnc_config(gateways, cnc_config=config.get("cnc_config"))

    src = devices[0]
    dst = devices[1]
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


def run_perf_cnc_vs_host_rw(initiator, gateways, config=None):
    """Time 500x4MB CNC vs host read/write baseline."""
    config = config or {}
    devices = get_ns_devices(initiator)
    caps = assert_copy_capabilities(initiator, devices)
    apply_cnc_config(gateways, cnc_config=config.get("cnc_config"))

    src = devices[0]
    dst = devices[1]
    src_dev, src_nsid = src["Namespace"], src["NSID"]
    dst_dev = dst["Namespace"]
    mcl = caps[src_dev]["mcl"]
    lba_size = caps[src_dev]["lba_size"]
    count = int(config.get("copy_count", 500))
    blocks = int(config.get("copy_blocks", BLOCKS_4MIB))
    total_bytes = count * blocks * lba_size

    write_verified_pattern(initiator.node, src_dev, size=total_bytes, verify="crc32c")

    t1 = run_cnc_loop(initiator, dst_dev, src_nsid, count=count, blocks=blocks, mcl=mcl)
    verify_copied_regions(
        initiator.node,
        src_dev,
        dst_dev,
        0,
        0,
        min(blocks, 1024),
        lba_size=lba_size,
    )

    # Baseline on a cleared destination region — use second half if space, else overwrite
    t2 = host_rw_baseline(
        initiator.node, src_dev, dst_dev, size_bytes=total_bytes, bs=lba_size
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
    return {"t1": t1, "t2": t2, "speedup": speedup}


def run_ana_cnc(initiator, gateways, config=None):
    """Active-path copy, secondary path observation, mid-copy failover."""
    config = config or {}
    devices = get_ns_devices(initiator)
    caps = assert_copy_capabilities(initiator, devices)
    apply_cnc_config(gateways, cnc_config=config.get("cnc_config"))

    src = devices[0]
    dst = devices[1]
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


def run_cnc_soak(initiator, gateways, config=None):
    """Sustained CNC soak: continuous CNC loops with resource sampling."""
    config = config or {}
    devices = get_ns_devices(initiator)
    caps = assert_copy_capabilities(initiator, devices)
    apply_cnc_config(
        gateways,
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

    src = devices[0]
    dst = devices[1]
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
                verify_copied_regions(
                    initiator.node,
                    src_dev,
                    dst_dev,
                    0,
                    0,
                    min(blocks, 1024),
                    lba_size=lba_size,
                )
                spot_done.add(mark)

    sample_gateway_resources(gateways)
    verify_copied_regions(
        initiator.node,
        src_dev,
        dst_dev,
        0,
        0,
        min(blocks, 1024),
        lba_size=lba_size,
    )
    LOG.info(f"Soak completed: {iteration} iterations over {duration_min} minutes")


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


def _soft_check_cnc_chunk_logs(gateways):
    for gw in gateways:
        try:
            logs = gw.cnc_get_container_logs(lines=100)
            if "cnc" in logs.lower() or "chunk" in logs.lower():
                LOG.info(f"CNC/chunk references found in {gw.node.hostname} logs")
            else:
                LOG.warning(
                    f"No obvious CNC/chunk log lines on {gw.node.hostname} "
                    "(build may use different log tags)"
                )
        except Exception as err:
            LOG.warning(f"Could not fetch CNC logs: {err}")


def run_spec_cnc_enable_perf(initiator, gateways, config=None, nvme_service=None):
    """CNC enabled via orch spec vs host R/W after cnc_enable=false.

    T1: timed nvme copy with CNC enabled (spec).
    T2: host read/write baseline after re-applying spec with cnc_enable=false.
    """
    config = config or {}
    if nvme_service is None:
        raise ValueError("spec_cnc_enable_perf requires nvme_service for orch re-apply")

    devices = get_ns_devices(initiator)
    caps = assert_copy_capabilities(initiator, devices)
    maybe_apply_cnc_config(gateways, config)

    src = devices[0]
    dst = devices[1]
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
    verify_copied_regions(
        initiator.node,
        src_dev,
        dst_dev,
        0,
        0,
        min(blocks, 1024),
        lba_size=lba_size,
    )

    LOG.info("Re-applying orch spec with cnc_enable=false")
    nvme_service.apply_nvmeof_spec(
        nvmeof_spec={"cnc_enable": False},
        redeploy=True,
        wait_sec=redeploy_wait,
    )
    _reconnect_initiator(initiator, gateways, config)

    # Device paths may change after reconnect
    devices = get_ns_devices(initiator)
    src = devices[0]
    dst = devices[1]
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
        initiator.node, src_dev, dst_dev, size_bytes=total_bytes, bs=lba_size
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
        raise Exception(
            f"CNC-enabled speedup {speedup:.2f}x below min_speedup={min_speedup}"
        )
    return {"t1": t1, "t2": t2, "speedup": speedup}


def run_spec_cnc_params_exercise(initiator, gateways, config=None, nvme_service=None):
    """Exercise orch-spec CNC rate/chunk/parallel without RPC override.

    Verifies rate limiter via concurrent CNC throughput bound, and large
    chunked copies succeed (soft log check for chunk activity).
    """
    config = config or {}
    devices = get_ns_devices(initiator)
    caps = assert_copy_capabilities(initiator, devices)
    maybe_apply_cnc_config(gateways, config, enable_logging=True)

    src = devices[0]
    dst = devices[1]
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
    concurrent = int(config.get("concurrent_copies", 4))
    copy_blocks = int(config.get("copy_blocks", 65536))
    # Cap expected MiB/s slightly above configured rate (bytes/s → MiB/s) with slack
    rate_mib = rate_limit_bytes / (1024 * 1024)
    max_limited_MBps = float(config.get("max_limited_MBps", rate_mib * 1.5))

    ranges = [
        (i * copy_blocks, i * copy_blocks, copy_blocks) for i in range(concurrent)
    ]
    prep_blocks = concurrent * copy_blocks
    prep_bytes = prep_blocks * lba_size
    LOG.info(
        f"Spec CNC params: preparing {prep_bytes} bytes for {concurrent} x "
        f"{copy_blocks}-block concurrent copies; "
        f"cnc_rate_limiter_bytes={rate_limit_bytes} "
        f"max_limited_MBps={max_limited_MBps}"
    )
    write_verified_pattern(
        initiator.node, src_dev, size=prep_bytes, verify="crc32c", bs="1M", iodepth=32
    )

    sample_gateway_resources(gateways)
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
    if mib_s > max_limited_MBps:
        raise Exception(
            f"Throughput {mib_s:.3f} MiB/s exceeds max_limited_MBps={max_limited_MBps} "
            f"(cnc_rate_limiter_bytes={rate_limit_bytes} from orch spec)"
        )

    # Large chunked copy to exercise cnc_chunk_blocks / cnc_parallel_chunks
    large_blocks = min(int(config.get("large_blocks", 50000)), mcl + 1)
    large_src = int(config.get("large_src_slba", 2000))
    large_dst = int(config.get("large_dst_slba", 5000))
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
    _soft_check_cnc_chunk_logs(gateways)
    LOG.info(
        f"Spec CNC chunk/parallel large copy verified in {elapsed:.2f}s "
        f"(cnc_chunk_blocks={nvmeof_spec.get('cnc_chunk_blocks')} "
        f"cnc_parallel_chunks={nvmeof_spec.get('cnc_parallel_chunks')})"
    )
    return {"rate_mib_s": mib_s, "large_copy_elapsed": elapsed}


OPERATIONS = {
    "cross_ns_copy_verify": run_cross_ns_copy_verify,
    "full_volume_integrity": run_full_volume_integrity,
    "perf_cnc_vs_host_rw": run_perf_cnc_vs_host_rw,
    "ana_cnc": run_ana_cnc,
    "cnc_soak": run_cnc_soak,
    "spec_cnc_enable_perf": run_spec_cnc_enable_perf,
    "spec_cnc_params_exercise": run_spec_cnc_params_exercise,
}
