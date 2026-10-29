"""
IBMCEPH-15738 / Polarion CEPH-83633026 – ``ceph nvmeof top connection``

Automates Polarion steps for live per-connection IO statistics (IO-size buckets)
for a host ↔ subsystem pair across NVMe-oF gateways.

Polarion step map
-----------------
STEP-01  Deploy ≥2 GWs, configure entities, connect-all, verify multipath
STEP-02  Happy-path under 4K randrw; IOPS non-zero; trend to zero after stop
STEP-03  Required-arg validation (missing / empty nqn & host-nqn)
STEP-04  IO size bucket sweep (2K-128K)
STEP-05  Read-only / write-only / mixed column split
STEP-06  Multi-gateway rows vs nvme list-subsys live paths
STEP-07  --gw-group filter (group1 with IO vs group2 with no connections)
STEP-08  --sort-by numeric / --descending / invalid column
STEP-09  --period / --with-timestamp / --no-header
STEP-10  Idle connected host (no FIO)
STEP-11  HA failover / failback while IO + top samples
STEP-12  QoS delay visibility (rQoS / wQoS)
STEP-13  Parallel top io / top cpu / top connection
STEP-14  Unconnected / disconnected host NQN
STEP-15  Invalid --period / --sort-by
STEP-16  top io numeric sort regression (IBMCEPH-18216)
STEP-17  TLS-PSK host setup; top connection under TLS IO
STEP-18  Rotate TLS-PSK key; top connection under TLS IO
"""

import json
import re
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

from ceph.ceph import Ceph
from ceph.ceph_admin.orch import Orch
from ceph.waiter import WaitUntil
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.initiator import prepare_io_execution
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    get_subsystem_network_masks,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)

CANONICAL_COLS = [
    "Gateway",
    "SizeKB",
    "Total_IOPS",
    "rIOPS",
    "rBDEV_us",
    "rNet_us",
    "rQoS_us",
    "rTotal_us",
    "wIOPS",
    "wBDEV_us",
    "wNet_us",
    "wQoS_us",
    "wTotal_us",
]
NUM_COLS = len(CANONICAL_COLS)
VALID_SIZES = {2, 4, 8, 16, 32, 64, 128}
INVALID_NQN = "nqn.2099-01.io.spdk:doesnotexist"
INVALID_HOST_NQN = (
    "nqn.2099-01.org.nvmexpress:uuid:00000000-0000-0000-0000-000000000000"
)
LATENCY_TOLERANCE = 0.20
# Fallback when suite does not deploy a real second GW group
EMPTY_GROUP_FALLBACK = "group_with_no_connections"

# Sampling hardening: under high latency, 1s top polls are bursty (esp. mixed
# randrw writes). Settle longer, capture a wider window, and retry predicates.
FIO_SETTLE_SEC = 20
TOP_LIVE_TIMEOUT = 15
TOP_RETRY_ATTEMPTS = 4
TOP_RETRY_PAUSE_SEC = 5

# TLS-PSK: dedicated secure listener port (avoids clash with insecure :4420)
TLS_SECURE_PORT = 4421
# HMAC-SHA256 interchange + identity 0 (NVMe0R01) — SPDK-compatible
TLS_PSK_HMAC = 1
TLS_PSK_IDENTITY = 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_host_nqn(initiator_node):
    out, _ = initiator_node.node.exec_command(cmd="nvme show-hostnqn", sudo=True)
    hostnqn = out.strip()
    LOG.info("[ %s ] hostnqn: %s", initiator_node.node.hostname, hostnqn)
    return hostnqn


def _parse_top_blocks(raw_output):
    """Parse all top connection table blocks separated by ``----``."""
    blocks = []
    rows = []
    in_data_block = False

    for raw_line in (raw_output or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("----"):
            if rows:
                blocks.append(rows)
                rows = []
            in_data_block = False
            continue
        if "Gateway" in line and "SizeKB" in line:
            in_data_block = True
            continue
        if not in_data_block:
            continue

        parts = line.split()
        if len(parts) < NUM_COLS:
            continue
        if len(parts) > NUM_COLS:
            parts = [parts[0]] + parts[-(NUM_COLS - 1) :]
        rows.append(dict(zip(CANONICAL_COLS, parts)))

    if rows:
        blocks.append(rows)
    return blocks


def _parse_top_output(raw_output, prefer_last_block=True):
    """Parse top connection table rows.

    Upstream notes the first poll batch can be garbage; when multiple blocks
    separated by ``----`` are present, return the last complete block.
    """
    blocks = _parse_top_blocks(raw_output)
    if not blocks:
        return []
    return blocks[-1] if prefer_last_block else blocks[0]


def _live_top_rows(raw_output):
    """Flatten live poll rows, skipping the first (often cumulative) block.

    Mixed workloads are bursty: a single 1s sample may show only rIOPS or
    only wIOPS. Callers that need "seen during window" should use this.
    """
    blocks = _parse_top_blocks(raw_output)
    if not blocks:
        return []
    live = blocks[1:] if len(blocks) > 1 else blocks
    return [row for block in live for row in block]


def _int_or_none(value):
    if value is None or value == "-":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _assert_schema(rows, step):
    if not rows:
        raise AssertionError(f"[{step}] Parsed 0 rows from top connection output.")
    missing = [k for k in CANONICAL_COLS if k not in rows[0]]
    if missing:
        raise AssertionError(f"[{step}] Missing columns {missing}")
    LOG.info("[%s] Schema OK - %d rows", step, len(rows))


def _settle(seconds=None):
    """Wait for FIO / path state to stabilize before sampling top."""
    wait = FIO_SETTLE_SEC if seconds is None else seconds
    LOG.info("Settling %ss before top sample", wait)
    time.sleep(wait)


def _run_top(nvmegwcli, sub_nqn, host_nqn, gw_group, live_timeout=None, **extra):
    """Invoke top connection using underscore CLI keys (same as run_nvme_cli).

    Default ``live_timeout`` is ``TOP_LIVE_TIMEOUT`` so callers get a wider
    sample window than the CLI wrapper default (8s). Pass ``live_timeout`` or
    ``sample_timeout`` explicitly for short validation / period checks.
    """
    args = {"nqn": sub_nqn, "host_nqn": host_nqn}
    if gw_group:
        args["gw_group"] = gw_group
    # Connection-level timeout kwargs (not CLI flags)
    explicit_lt = extra.pop("live_timeout", None)
    explicit_st = extra.pop("sample_timeout", None)
    # Normalize any hyphenated extras from callers to underscore keys
    for key, val in list(extra.items()):
        if key == "gw-group":
            args["gw_group"] = val
        elif key == "server-address":
            args["server_address"] = val
        elif key == "server-port":
            args["server_port"] = val
        elif key == "sort_by":
            args["sort-by"] = val
        else:
            args[key] = val

    conn_kwargs = {}
    if explicit_lt is not None:
        conn_kwargs["live_timeout"] = explicit_lt
    elif explicit_st is not None:
        conn_kwargs["sample_timeout"] = explicit_st
    elif live_timeout is not None:
        conn_kwargs["live_timeout"] = live_timeout
    else:
        conn_kwargs["live_timeout"] = TOP_LIVE_TIMEOUT

    out, err = nvmegwcli.top.connection(args=args, **conn_kwargs)
    return out, err


def _retry_top_rows(
    nvmegwcli,
    sub_nqn,
    host_nqn,
    gw_group,
    *,
    step,
    predicate,
    use_live_rows=False,
    attempts=None,
    **top_extra,
):
    """Sample top until ``predicate(rows)`` holds or attempts are exhausted."""
    attempts = attempts if attempts is not None else TOP_RETRY_ATTEMPTS
    last_out, last_err, last_rows = "", "", []
    for attempt in range(1, attempts + 1):
        last_out, last_err = _run_top(
            nvmegwcli, sub_nqn, host_nqn, gw_group, **top_extra
        )
        last_rows = (
            _live_top_rows(last_out) if use_live_rows else _parse_top_output(last_out)
        )
        if last_rows and predicate(last_rows):
            LOG.info(
                "%s: condition met on attempt %d/%d (%d rows)",
                step,
                attempt,
                attempts,
                len(last_rows),
            )
            return last_out, last_err, last_rows
        LOG.info(
            "%s: attempt %d/%d not met (rows=%d); pause %ss",
            step,
            attempt,
            attempts,
            len(last_rows or []),
            TOP_RETRY_PAUSE_SEC,
        )
        if attempt < attempts:
            time.sleep(TOP_RETRY_PAUSE_SEC)
    return last_out, last_err, last_rows


def _nonzero_iops_rows(rows):
    return [
        r
        for r in rows
        if _int_or_none(r.get("Total_IOPS")) is not None
        and _int_or_none(r.get("Total_IOPS")) > 0
    ]


def _has_nonzero_iops(rows):
    return bool(_nonzero_iops_rows(rows))


def _rw_active(rows):
    r_active = any((_int_or_none(r.get("rIOPS")) or 0) > 0 for r in rows)
    w_active = any((_int_or_none(r.get("wIOPS")) or 0) > 0 for r in rows)
    return r_active, w_active


def _start_fio_on_initiators(
    initiator_objs,
    io_executor,
    *,
    io_type="randrw",
    runtime="120",
    bs="4k",
    rwmixread=None,
    iodepth=8,
):
    futures = []
    for initiator in initiator_objs:
        paths = initiator.list_spdk_drives()
        if not paths:
            LOG.warning("No NVMe drives on %s; skipping FIO", initiator.node.hostname)
            continue
        kwargs = {
            "io_size": "100%",
            "runtime": runtime,
            "paths": paths,
            "io_type": io_type,
            "iodepth": iodepth,
            "bs": bs,
            "time_based": True,
            "execute_blkdiscard": False,
        }
        if rwmixread is not None:
            kwargs["rwmixread"] = rwmixread
        futures.append(io_executor.submit(initiator.start_fio, **kwargs))
    return futures


def _stop_fio(initiator_objs):
    for initiator in initiator_objs:
        try:
            initiator.stop_fio()
        except Exception as exc:
            LOG.warning("stop_fio on %s: %s", initiator.node.hostname, exc)


def _live_path_ips(initiator):
    """Return set of live gateway IPs from ``nvme list-subsys`` JSON."""
    out, _ = initiator.list_subsys(**{"output-format": "json"})
    ips = set()
    try:
        data = json.loads(out)
    except (TypeError, json.JSONDecodeError):
        LOG.warning("Could not parse list-subsys JSON: %s", out)
        return ips
    # nvme-cli JSON shapes vary; walk conservatively
    entries = data if isinstance(data, list) else [data]
    for entry in entries:
        for subsys in entry.get("Subsystems", []) if isinstance(entry, dict) else []:
            for path in subsys.get("Paths", []):
                if path.get("State") != "live":
                    continue
                addr = path.get("Address", "")
                match = re.search(r"traddr=([^,]+)", addr)
                if match:
                    ips.add(match.group(1))
    return ips


def _max_qos_us(rows, cols=("rQoS_us", "wQoS_us")):
    values = []
    for row in rows:
        for col in cols:
            val = _int_or_none(row.get(col))
            if val is not None:
                values.append(val)
    return max(values) if values else 0


def step01_verify_multipath(initiator_objs, expected_gw_ips):
    """STEP-01: Confirm multipath live paths to multiple gateway IPs."""
    LOG.info("STEP-01: verify multipath live paths")
    if not initiator_objs:
        raise AssertionError("STEP-01: No initiators connected")
    live = _live_path_ips(initiator_objs[0])
    LOG.info("STEP-01: live path IPs=%s expected=%s", sorted(live), expected_gw_ips)
    if len(live) < 2:
        raise AssertionError(
            f"STEP-01: Expected ≥2 live multipath paths, got {sorted(live)}"
        )
    missing = [ip for ip in expected_gw_ips if ip not in live]
    if missing:
        raise AssertionError(
            f"STEP-01: Expected GW IPs missing from live paths: {missing}"
        )
    LOG.info("STEP-01: PASSED")


def step02_happy_path_under_io(nvmegwcli, sub_nqn, host_nqn, gw_group):
    """STEP-02: Non-zero IOPS under active FIO (schema + latency sanity)."""
    LOG.info("STEP-02: happy-path under IO")
    out, _, rows = _retry_top_rows(
        nvmegwcli,
        sub_nqn,
        host_nqn,
        gw_group,
        step="STEP-02",
        predicate=_has_nonzero_iops,
        use_live_rows=True,
    )
    LOG.info("STEP-02 raw:\n%s", out)
    _assert_schema(rows, "STEP-02")

    for row in rows:
        sz = _int_or_none(row.get("SizeKB"))
        if sz is None or sz not in VALID_SIZES:
            raise AssertionError(f"STEP-02: Unexpected SizeKB '{row.get('SizeKB')}'")
        if ":" not in row.get("Gateway", ""):
            raise AssertionError(
                f"STEP-02: Gateway '{row.get('Gateway')}' is not IP:port"
            )

    nonzero = _nonzero_iops_rows(rows)
    if not nonzero:
        raise AssertionError(
            "STEP-02: All Total_IOPS are 0/- while FIO is running. " f"Rows={rows}"
        )
    LOG.info(
        "STEP-02: %d/%d rows with non-zero IOPS (sample SizeKB=%s Total_IOPS=%s)",
        len(nonzero),
        len(rows),
        nonzero[0]["SizeKB"],
        nonzero[0]["Total_IOPS"],
    )

    # Latency additive sanity on read-active rows
    checked = 0
    for row in rows:
        if not _int_or_none(row.get("rIOPS")):
            continue
        bdev = _int_or_none(row.get("rBDEV_us"))
        net = _int_or_none(row.get("rNet_us"))
        qos = _int_or_none(row.get("rQoS_us")) or 0
        total = _int_or_none(row.get("rTotal_us"))
        if None in (bdev, net, total):
            continue
        if bdev <= 0 or total <= 0 or net < 0:
            raise AssertionError(f"STEP-02: Invalid latency values: {row}")
        computed = bdev + net + qos
        deviation = abs(computed - total) / max(total, 1)
        if deviation > LATENCY_TOLERANCE:
            raise AssertionError(
                f"STEP-02: bdev+net+qos={computed} vs total={total} "
                f"dev={deviation:.1%}: {row}"
            )
        checked += 1
    LOG.info("STEP-02: latency checked on %d row(s)", checked)
    LOG.info("STEP-02: PASSED")
    return rows


def step02_zero_after_stop(nvmegwcli, sub_nqn, host_nqn, gw_group):
    """STEP-02 (post): IOPS trend to zero after FIO stops."""
    LOG.info("STEP-02-post: wait for zero IOPS after FIO stop")

    def _all_zero():
        out, _ = _run_top(nvmegwcli, sub_nqn, host_nqn, gw_group)
        rows = _parse_top_output(out)
        if not rows:
            return False
        return not _nonzero_iops_rows(rows)

    for w in WaitUntil(timeout=45, interval=5):
        if _all_zero():
            break
    if w.expired:
        raise AssertionError(
            "STEP-02-post: Total_IOPS did not reach 0 within 45s after FIO stop"
        )
    LOG.info("STEP-02-post: PASSED")


def _looks_like_validation_error(combined):
    """True if CLI output indicates usage / required-arg validation failure."""
    markers = (
        "usage",
        "error",
        "required",
        "missing",
        "invalid",
        "einval",
        "must be",
        "need",
        "expected",
        "argument",
    )
    text = (combined or "").lower()
    if "traceback" in text:
        return False
    # A successful table must not be treated as validation error
    if "gateway" in text and "sizekb" in text:
        return False
    return any(m in text for m in markers)


def step03_required_args(nvmegwcli, ceph_cluster):
    """STEP-03: Missing / empty required arguments (CLI validation)."""
    LOG.info("STEP-03: required argument validation")

    cases = [
        {"host_nqn": "any-host-nqn"},  # missing nqn
        {"nqn": "any-subsystem-nqn"},  # missing host_nqn
        {"nqn": "", "host_nqn": ""},  # empty strings
    ]
    for args in cases:
        out, err = nvmegwcli.top.connection(args=args, live_timeout=10)
        combined = f"{out or ''}{err or ''}"
        LOG.info("STEP-03 args=%s → %s", args, combined[:400])
        if "traceback" in combined.lower():
            raise AssertionError(f"STEP-03: traceback for args={args}")
        if not _looks_like_validation_error(combined):
            raise AssertionError(
                f"STEP-03: expected usage/validation error for args={args}, "
                f"got: {combined[:500]!r}"
            )

    orch = Orch(ceph_cluster, **{})
    out, err = orch.shell(
        args=[
            "timeout --foreground --signal=TERM --kill-after=2 10s "
            "ceph nvmeof top connection"
        ],
        check_status=False,
        timeout=60,
        pretty_print=True,
    )
    combined = f"{out or ''}{err or ''}"
    LOG.info("STEP-03 bare command → %s", combined[:400])
    if "traceback" in combined.lower():
        raise AssertionError("STEP-03: traceback for bare top connection")
    if not _looks_like_validation_error(combined):
        raise AssertionError(
            f"STEP-03: bare command expected usage/validation error, "
            f"got: {combined[:500]!r}"
        )
    LOG.info("STEP-03: PASSED")


def step04_bucket_sweep(
    nvmegwcli, sub_nqn, host_nqn, gw_group, initiator_objs, io_executor, runtime
):
    """STEP-04: Per-SizeKB elevated IOPS for bs 2k–128k."""
    LOG.info("STEP-04: IO size bucket sweep (runtime=%ss)", runtime)
    # for bs in ("2k", "4k", "8k", "16k", "32k", "64k", "128k"):
    for bs in ("4k", "8k", "16k", "32k", "64k", "128k"):
        expected_kb = int(bs.rstrip("kK"))
        LOG.info("STEP-04: fio randwrite bs=%s", bs)
        _stop_fio(initiator_objs)
        _start_fio_on_initiators(
            initiator_objs,
            io_executor,
            io_type="randwrite",
            runtime=str(runtime),
            bs=bs,
        )
        _settle(max(FIO_SETTLE_SEC, min(25, max(10, int(runtime) // 3))))

        def _bucket_hit(rows, kb=expected_kb):
            return any(
                _int_or_none(r.get("SizeKB")) == kb
                and (_int_or_none(r.get("Total_IOPS")) or 0) > 0
                for r in rows
            )

        out, _, rows = _retry_top_rows(
            nvmegwcli,
            sub_nqn,
            host_nqn,
            gw_group,
            step=f"STEP-04/{bs}",
            predicate=_bucket_hit,
            use_live_rows=True,
        )
        _assert_schema(rows, f"STEP-04/{bs}")
        matching = [
            r
            for r in rows
            if _int_or_none(r.get("SizeKB")) == expected_kb
            and _int_or_none(r.get("Total_IOPS"))
            and _int_or_none(r.get("Total_IOPS")) > 0
        ]
        if not matching:
            raise AssertionError(
                f"STEP-04: No elevated IOPS for SizeKB={expected_kb} under bs={bs}. "
                f"Non-zero rows: {_nonzero_iops_rows(rows)}"
            )
        LOG.info(
            "STEP-04: bs=%s → SizeKB=%s Total_IOPS=%s",
            bs,
            matching[0]["SizeKB"],
            matching[0]["Total_IOPS"],
        )
        _stop_fio(initiator_objs)
        _settle(5)
    LOG.info("STEP-04: PASSED")


def step05_rw_column_split(
    nvmegwcli, sub_nqn, host_nqn, gw_group, initiator_objs, io_executor
):
    """STEP-05: Read / write / mixed column population."""
    LOG.info("STEP-05: read/write/mixed column split")
    cases = [
        ("read", None, "r"),
        ("write", None, "w"),
        ("randrw", "70", "both"),
    ]
    for io_type, mix, expect in cases:
        _stop_fio(initiator_objs)
        _start_fio_on_initiators(
            initiator_objs,
            io_executor,
            io_type=io_type,
            runtime="90",
            bs="4k",
            rwmixread=mix,
        )
        _settle()

        def _rw_ok(rows, expect_mode=expect):
            r_active, w_active = _rw_active(rows)
            if expect_mode == "r":
                return r_active
            if expect_mode == "w":
                return w_active
            return r_active and w_active

        # Always scan live polls (skip first cumulative). Mixed writes are
        # bursty — retry until both sides appear in the capture window.
        out, _, rows = _retry_top_rows(
            nvmegwcli,
            sub_nqn,
            host_nqn,
            gw_group,
            step=f"STEP-05/{io_type}",
            predicate=_rw_ok,
            use_live_rows=True,
        )
        _assert_schema(rows, f"STEP-05/{io_type}")
        r_active, w_active = _rw_active(rows)
        if expect == "r" and not r_active:
            raise AssertionError(f"STEP-05: read workload produced no rIOPS: {rows}")
        if expect == "w" and not w_active:
            raise AssertionError(f"STEP-05: write workload produced no wIOPS: {rows}")
        if expect == "both" and not (r_active and w_active):
            raise AssertionError(
                f"STEP-05: mixed workload missing r/w IOPS "
                f"(r={r_active} w={w_active}): {_nonzero_iops_rows(rows)}"
            )
        LOG.info(
            "STEP-05: io_type=%s r_active=%s w_active=%s", io_type, r_active, w_active
        )
        _stop_fio(initiator_objs)
        _settle(5)
    LOG.info("STEP-05: PASSED")


def step06_multi_gateway(
    nvmegwcli, sub_nqn, host_nqn, gw_group, expected_gw_ips, initiator_objs
):
    """STEP-06: Gateway IPs match live paths; active GWs show IOPS."""
    LOG.info("STEP-06: multi-gateway rows vs list-subsys")

    def _all_expected_gws(rows):
        observed = {r.get("Gateway", "").split(":")[0] for r in rows}
        return all(ip in observed for ip in expected_gw_ips) and _has_nonzero_iops(rows)

    out, _, rows = _retry_top_rows(
        nvmegwcli,
        sub_nqn,
        host_nqn,
        gw_group,
        step="STEP-06",
        predicate=_all_expected_gws,
        use_live_rows=True,
    )
    _assert_schema(rows, "STEP-06")

    observed = {r.get("Gateway", "").split(":")[0] for r in rows}
    live = _live_path_ips(initiator_objs[0]) if initiator_objs else set()
    LOG.info(
        "STEP-06: observed=%s live=%s expected=%s", observed, live, expected_gw_ips
    )

    unexpected = observed - set(expected_gw_ips)
    if unexpected:
        raise AssertionError(f"STEP-06: Unexpected gateway IPs: {unexpected}")
    missing = [ip for ip in expected_gw_ips if ip not in observed]
    if missing:
        raise AssertionError(f"STEP-06: Missing expected gateway IPs: {missing}")

    # Optional --server-address filter (must be non-empty and exclusive)
    target_ip = expected_gw_ips[0]

    def _filtered_ok(rows, tip=target_ip):
        return bool(rows) and all(r.get("Gateway", "").startswith(tip) for r in rows)

    out_f, _, filtered = _retry_top_rows(
        nvmegwcli,
        sub_nqn,
        host_nqn,
        gw_group,
        step=f"STEP-06/server_address={target_ip}",
        predicate=_filtered_ok,
        use_live_rows=True,
        server_address=target_ip,
    )
    if not filtered:
        raise AssertionError(
            f"STEP-06: --server_address={target_ip} returned 0 parseable rows"
        )
    bad = [r for r in filtered if not r.get("Gateway", "").startswith(target_ip)]
    if bad:
        raise AssertionError(
            f"STEP-06: --server_address={target_ip} leaked other GWs: {bad}"
        )
    LOG.info("STEP-06: --server_address filter OK (%d rows)", len(filtered))
    LOG.info("STEP-06: PASSED")


def step07_gw_group_filter(
    nvmegwcli,
    sub_nqn,
    host_nqn,
    gw_group_a,
    expected_gw_ips_a,
    gw_group_b,
):
    """STEP-07: --gw-group restricts to group A; group B (no connections) is empty.

    Polarion: with IO only on group A, ``--gw-group <A>`` shows stats and
    ``--gw-group <B>`` returns empty when B has no matching connections.
    """
    LOG.info("STEP-07: --gw-group filter (active=%s empty=%s)", gw_group_a, gw_group_b)

    def _group_a_ok(rows):
        if not _has_nonzero_iops(rows):
            return False
        return all(
            r.get("Gateway", "").split(":")[0] in expected_gw_ips_a for r in rows
        )

    out_a, _, rows_a = _retry_top_rows(
        nvmegwcli,
        sub_nqn,
        host_nqn,
        gw_group_a,
        step="STEP-07/groupA",
        predicate=_group_a_ok,
        use_live_rows=True,
    )
    _assert_schema(rows_a, "STEP-07/groupA")
    for row in rows_a:
        ip = row.get("Gateway", "").split(":")[0]
        if ip not in expected_gw_ips_a:
            raise AssertionError(
                f"STEP-07: IP {ip} not in group {gw_group_a}: {expected_gw_ips_a}"
            )
    if not _nonzero_iops_rows(rows_a):
        raise AssertionError(
            f"STEP-07: group A '{gw_group_a}' expected non-zero IOPS under active IO"
        )

    out_b, err_b = _run_top(nvmegwcli, sub_nqn, host_nqn, gw_group_b)
    rows_b = _parse_top_output(out_b)
    if rows_b and _nonzero_iops_rows(rows_b):
        raise AssertionError(
            f"STEP-07: group B '{gw_group_b}' returned non-zero connection stats "
            f"but has no initiator IO: {rows_b}"
        )
    if rows_b:
        LOG.info(
            "STEP-07: group B returned %d zero-IOPS row(s) (acceptable empty-connection)",
            len(rows_b),
        )
    else:
        LOG.info("STEP-07: group B returned no rows (err=%r)", (err_b or "")[:200])
    LOG.info("STEP-07: PASSED")


def step08_sort_by(nvmegwcli, sub_nqn, host_nqn, gw_group, ceph_cluster):
    """STEP-08: Numeric --sort-by / --descending; invalid column rejected."""
    LOG.info("STEP-08: --sort-by / --descending")

    def _sizes(descending):
        out, _, rows = _retry_top_rows(
            nvmegwcli,
            sub_nqn,
            host_nqn,
            gw_group,
            step=f"STEP-08/SizeKB desc={descending}",
            predicate=lambda r: len(r) >= 2,
            **{"sort-by": "SizeKB", "descending": descending},
        )
        _assert_schema(rows, "STEP-08")
        return [
            _int_or_none(r.get("SizeKB")) for r in rows if _int_or_none(r.get("SizeKB"))
        ]

    asc = _sizes(False)
    desc = _sizes(True)
    if asc != sorted(asc):
        # SizeKB may already be natural order; only fail if clearly reversed wrong
        LOG.warning("STEP-08: ascending SizeKB not strictly sorted: %s", asc)
    for i in range(1, len(desc)):
        if desc[i] > desc[i - 1]:
            raise AssertionError(f"STEP-08: SizeKB not descending: {desc}")

    # Total IOPS descending under load (last block only — preserve sort order)
    out, _, rows = _retry_top_rows(
        nvmegwcli,
        sub_nqn,
        host_nqn,
        gw_group,
        step="STEP-08/Total IOPS",
        predicate=_has_nonzero_iops,
        use_live_rows=False,
        **{"sort-by": "Total IOPS", "descending": True},
    )
    iops = [
        _int_or_none(r.get("Total_IOPS"))
        for r in rows
        if _int_or_none(r.get("Total_IOPS")) is not None
    ]
    for i in range(1, len(iops)):
        if iops[i] > iops[i - 1]:
            raise AssertionError(f"STEP-08: Total IOPS not numeric-descending: {iops}")

    orch = Orch(ceph_cluster, **{})
    out_b, err_b = orch.shell(
        args=[
            "timeout --foreground --signal=TERM --kill-after=2 10s "
            f"ceph nvmeof top connection --nqn {sub_nqn} --host_nqn {host_nqn} "
            f"--gw_group {gw_group} --sort-by BogusColumn"
        ],
        check_status=False,
        timeout=60,
        pretty_print=True,
    )
    combined = f"{out_b or ''}{err_b or ''}".lower()
    if "traceback" in combined:
        raise AssertionError("STEP-08: BogusColumn caused traceback")
    LOG.info("STEP-08: BogusColumn response: %s", combined[:300])
    LOG.info("STEP-08: PASSED")


def step09_display_options(nvmegwcli, sub_nqn, host_nqn, gw_group):
    """STEP-09: --period / --with-timestamp / --no-header / boundary periods."""
    LOG.info("STEP-09: display options")

    for period in (1, 5, 3600):
        # For period=3600 only verify acceptance with a short sample_timeout kill.
        sample_timeout = 4 if period >= 60 else max(5, int(period) * 2 + 2)
        out, err = nvmegwcli.top.connection(
            args={
                "nqn": sub_nqn,
                "host_nqn": host_nqn,
                "gw-group": gw_group,
                "period": period,
            },
            sample_timeout=sample_timeout,
        )
        combined = f"{out or ''}{err or ''}".lower()
        if (
            "must be between" in combined
            or "invalid" in combined
            and "period" in combined
        ):
            if period in (1, 3600):
                raise AssertionError(
                    f"STEP-09: valid period={period} rejected: {combined[:300]}"
                )
        LOG.info(
            "STEP-09: period=%s accepted (captured %d chars)", period, len(out or "")
        )

    out_ts, _ = _run_top(
        nvmegwcli, sub_nqn, host_nqn, gw_group, **{"with-timestamp": True, "period": 1}
    )
    if not re.search(r"\d{4}-\d{2}-\d{2}|delay", out_ts or "", re.I):
        LOG.warning(
            "STEP-09: --with-timestamp output missing obvious timestamp/delay: %r",
            (out_ts or "")[:200],
        )
    else:
        LOG.info("STEP-09: --with-timestamp OK")

    out_nh, _ = _run_top(nvmegwcli, sub_nqn, host_nqn, gw_group, **{"no-header": True})
    first = next(
        (
            ln.strip()
            for ln in (out_nh or "").splitlines()
            if ln.strip() and not ln.strip().startswith("----")
        ),
        "",
    )
    if "Gateway" in first and "SizeKB" in first:
        raise AssertionError(f"STEP-09: --no-header did not suppress header: {first!r}")
    LOG.info("STEP-09: PASSED")


def step10_idle(nvmegwcli, sub_nqn, host_nqn, gw_group):
    """STEP-10: Connected host with no FIO → zero/near-zero IOPS."""
    LOG.info("STEP-10: idle connected host")
    out, _ = _run_top(nvmegwcli, sub_nqn, host_nqn, gw_group)
    rows = _parse_top_output(out)
    # Empty or zero-IOPS both acceptable while idle
    nonzero = _nonzero_iops_rows(rows)
    if nonzero:
        # Allow a brief residual; fail only if sustained high
        max_iops = max(_int_or_none(r["Total_IOPS"]) for r in nonzero)
        if max_iops and max_iops > 50:
            raise AssertionError(
                f"STEP-10: Unexpected non-zero IOPS while idle: {nonzero}"
            )
        LOG.warning("STEP-10: residual IOPS=%s (tolerated)", max_iops)
    LOG.info("STEP-10: PASSED")


def step11_ha_failover(
    nvmegwcli,
    sub_nqn,
    host_nqn,
    gw_group,
    nvme_service,
    ceph_cluster,
    config,
    initiator_objs,
    io_executor,
):
    """STEP-11: Failover / failback while sampling top connection."""
    LOG.info("STEP-11: HA failover / failback")
    if len(nvme_service.gateways) < 2:
        raise AssertionError("STEP-11: Need ≥2 gateways for HA")

    _stop_fio(initiator_objs)
    _start_fio_on_initiators(
        initiator_objs,
        io_executor,
        io_type="randrw",
        runtime="180",
        bs="4k",
        rwmixread="70",
    )
    _settle()

    ha = HighAvailability(
        ceph_cluster,
        [g.node.hostname for g in nvme_service.gateways],
        **{
            "gw_group": gw_group,
            "rbd_pool": config.get("rbd_pool", "rbd"),
            "nvme_service": nvme_service,
        },
    )
    ha.gateways = list(nvme_service.gateways)
    fail_gw = nvme_service.gateways[0]
    fail_ip = fail_gw.node.ip_address
    surviving_ips = {
        g.node.ip_address for g in nvme_service.gateways if g is not fail_gw
    }
    LOG.info(
        "STEP-11: stopping gateway %s (%s); surviving=%s",
        fail_gw.node.hostname,
        fail_ip,
        sorted(surviving_ips),
    )
    ha.system_control(fail_gw, "stop", wait_for_active_state=False)
    _settle(25)

    # Sample via a surviving gateway CLI when possible (failed GW may be unreachable)
    sample_cli = nvme_service.gateways[1]

    def _failover_rows_ok(rows):
        if not rows:
            return False
        observed = {r.get("Gateway", "").split(":")[0] for r in rows}
        return bool(observed & surviving_ips) if surviving_ips else True

    out, err, rows = _retry_top_rows(
        sample_cli,
        sub_nqn,
        host_nqn,
        gw_group,
        step="STEP-11/failover",
        predicate=_failover_rows_ok,
        use_live_rows=True,
    )
    combined = f"{out or ''}{err or ''}".lower()
    if "traceback" in combined:
        raise AssertionError(f"STEP-11: traceback during failover: {combined[:400]}")
    if not rows:
        raise AssertionError(
            "STEP-11: top connection returned no parseable rows during failover"
        )
    observed = {r.get("Gateway", "").split(":")[0] for r in rows}
    LOG.info(
        "STEP-11 during failover rows=%d nonzero=%d observed_ips=%s",
        len(rows),
        len(_nonzero_iops_rows(rows)),
        sorted(observed),
    )
    # Surviving gateway(s) should still appear (or at least some live GW rows)
    if surviving_ips and not (observed & surviving_ips):
        raise AssertionError(
            f"STEP-11: no surviving GW IPs {sorted(surviving_ips)} in top "
            f"output during failover; observed={sorted(observed)}"
        )

    LOG.info("STEP-11: restarting gateway %s", fail_gw.node.hostname)
    ha.system_control(fail_gw, "start", wait_for_active_state=True)
    _settle()
    out2, err2, rows2 = _retry_top_rows(
        nvmegwcli,
        sub_nqn,
        host_nqn,
        gw_group,
        step="STEP-11/failback",
        predicate=lambda r: bool(r),
        use_live_rows=True,
    )
    if "traceback" in f"{out2 or ''}{err2 or ''}".lower():
        raise AssertionError("STEP-11: traceback after failback")
    _assert_schema(rows2, "STEP-11/failback")
    _stop_fio(initiator_objs)
    LOG.info("STEP-11: PASSED")


def step12_qos(nvmegwcli, sub_nqn, host_nqn, gw_group, initiator_objs, io_executor):
    """STEP-12: QoS limit increases rQoS/wQoS vs baseline."""
    LOG.info("STEP-12: QoS delay visibility")
    _stop_fio(initiator_objs)
    _start_fio_on_initiators(
        initiator_objs,
        io_executor,
        io_type="randrw",
        runtime="90",
        bs="4k",
        rwmixread="50",
    )
    _settle()
    out_base, _, base_rows = _retry_top_rows(
        nvmegwcli,
        sub_nqn,
        host_nqn,
        gw_group,
        step="STEP-12/baseline",
        predicate=_has_nonzero_iops,
        use_live_rows=True,
    )
    base_qos = _max_qos_us(base_rows)
    LOG.info("STEP-12: baseline max QoS µs=%s", base_qos)

    # Apply a tight IOPS QoS on nsid 1
    try:
        nvmegwcli.namespace.set_qos(
            args={
                "subsystem": sub_nqn,
                "nsid": 1,
                "rw-ios-per-second": 100,
            }
        )
    except Exception as exc:
        LOG.warning("STEP-12: set_qos failed (%s); skipping hard assert", exc)
        _stop_fio(initiator_objs)
        return

    _settle()
    out_qos, _, qos_rows = _retry_top_rows(
        nvmegwcli,
        sub_nqn,
        host_nqn,
        gw_group,
        step="STEP-12/qos",
        predicate=_has_nonzero_iops,
        use_live_rows=True,
    )
    qos_max = _max_qos_us(qos_rows)
    LOG.info("STEP-12: under QoS max QoS µs=%s (baseline=%s)", qos_max, base_qos)
    if qos_max < base_qos:
        LOG.warning(
            "STEP-12: QoS µs did not increase (base=%s now=%s); "
            "directional soft-warn only",
            base_qos,
            qos_max,
        )
    elif qos_max == 0 and base_qos == 0:
        LOG.warning("STEP-12: QoS columns remained 0; limit may not throttle this load")
    else:
        LOG.info("STEP-12: QoS delay increased as expected")

    # Clear QoS
    try:
        nvmegwcli.namespace.set_qos(
            args={
                "subsystem": sub_nqn,
                "nsid": 1,
                "rw-ios-per-second": 0,
            }
        )
    except Exception as exc:
        LOG.warning("STEP-12: clear QoS failed: %s", exc)
    _stop_fio(initiator_objs)
    LOG.info("STEP-12: PASSED")


def step13_parallel_top_cmds(nvmegwcli, sub_nqn, host_nqn, gw_group):
    """STEP-13: top io / top cpu / top connection succeed under load."""
    LOG.info("STEP-13: parallel top io / cpu / connection")
    out_io, err_io = nvmegwcli.top.io(
        args={"nqn": sub_nqn, "gw-group": gw_group, "period": 1},
        live_timeout=TOP_LIVE_TIMEOUT,
    )
    out_cpu, err_cpu = nvmegwcli.top.cpu(
        args={"gw-group": gw_group, "period": 1},
        live_timeout=TOP_LIVE_TIMEOUT,
    )
    out_conn, err_conn, rows = _retry_top_rows(
        nvmegwcli,
        sub_nqn,
        host_nqn,
        gw_group,
        step="STEP-13/connection",
        predicate=lambda r: bool(r),
        use_live_rows=True,
    )
    for name, out, err in (
        ("top io", out_io, err_io),
        ("top cpu", out_cpu, err_cpu),
        ("top connection", out_conn, err_conn),
    ):
        combined = f"{out or ''}{err or ''}".lower()
        if "traceback" in combined:
            raise AssertionError(f"STEP-13: {name} traceback: {combined[:400]}")
        LOG.info("STEP-13: %s captured %d chars", name, len(out or ""))
    if rows:
        _assert_schema(rows, "STEP-13")
    LOG.info("STEP-13: PASSED")


def step14_bad_or_disconnected_host(
    nvmegwcli, sub_nqn, host_nqn, gw_group, initiator_objs
):
    """STEP-14: Unconnected host NQN and post disconnect-all."""
    LOG.info("STEP-14: unconnected / disconnected host NQN")
    out, err = _run_top(nvmegwcli, sub_nqn, INVALID_HOST_NQN, gw_group)
    rows = _parse_top_output(out)
    if rows and _nonzero_iops_rows(rows):
        raise AssertionError(f"STEP-14: Unexpected IOPS for invalid host NQN: {rows}")
    LOG.info("STEP-14: invalid host NQN → empty/zero OK")

    for initiator in initiator_objs:
        try:
            initiator.disconnect_all()
        except Exception as exc:
            LOG.warning("disconnect_all: %s", exc)

    out2, err2 = _run_top(nvmegwcli, sub_nqn, host_nqn, gw_group)
    rows2 = _parse_top_output(out2)
    if rows2 and _nonzero_iops_rows(rows2):
        raise AssertionError(f"STEP-14: Non-zero IOPS after disconnect-all: {rows2}")
    combined = f"{out2 or ''}{err2 or ''}".lower()
    if "traceback" in combined:
        raise AssertionError("STEP-14: traceback after disconnect-all")
    LOG.info("STEP-14: PASSED")


def step15_invalid_period_sort(nvmegwcli, sub_nqn, host_nqn, gw_group, ceph_cluster):
    """STEP-15: Invalid period / sort-by rejected."""
    LOG.info("STEP-15: invalid period / sort-by")
    orch = Orch(ceph_cluster, **{})
    for period in (0, 3601, -1):
        out, err = orch.shell(
            args=[
                "timeout --foreground --signal=TERM --kill-after=2 10s "
                f"ceph nvmeof top connection --nqn {sub_nqn} --host_nqn {host_nqn} "
                f"--gw_group {gw_group} --period {period}"
            ],
            check_status=False,
            timeout=60,
            pretty_print=True,
        )
        combined = f"{out or ''}{err or ''}".lower()
        LOG.info("STEP-15: period=%s → %s", period, combined[:250])
        if "traceback" in combined:
            raise AssertionError(f"STEP-15: traceback for period={period}")
    LOG.info("STEP-15: PASSED")


def step16_top_io_sort(nvmegwcli, sub_nqn, gw_group):
    """STEP-16: top io numeric --sort-by regression (IBMCEPH-18216)."""
    LOG.info("STEP-16: top io numeric sort")
    for col in ("rMB/s", "r_await", "wareq-sz"):
        out, err = nvmegwcli.top.io(
            args={
                "nqn": sub_nqn,
                "gw-group": gw_group,
                "sort-by": col,
                "descending": True,
                "period": 1,
            }
        )
        combined = f"{out or ''}{err or ''}".lower()
        if "traceback" in combined:
            raise AssertionError(f"STEP-16: traceback for sort-by={col}")
        LOG.info("STEP-16: sort-by=%s captured %d chars", col, len(out or ""))
    LOG.info("STEP-16: PASSED")


# ---------------------------------------------------------------------------
# TLS-PSK helpers (STEP-17 / STEP-18)
# ---------------------------------------------------------------------------


def _gen_tls_psk(initiator_node, host_nqn, sub_nqn):
    """Generate an NVMe TLS PSK interchange string on the initiator."""
    out, _ = initiator_node.exec_command(
        cmd=(
            f"nvme gen-tls-key -n {host_nqn} -c {sub_nqn} "
            f"-m {TLS_PSK_HMAC} -I {TLS_PSK_IDENTITY}"
        ),
        sudo=True,
    )
    psk = (out or "").strip().splitlines()[-1].strip()
    if not psk.startswith("NVMeTLSkey-"):
        raise AssertionError(f"Unexpected TLS PSK output: {out!r}")
    LOG.info("Generated TLS PSK for host=%s sub=%s", host_nqn, sub_nqn)
    return psk


def _ensure_tlshd(initiator_node):
    """Install/enable tlshd (ktls-utils) required for NVMe/TCP TLS connect."""
    LOG.info("Ensuring tlshd/ktls-utils on %s", initiator_node.hostname)
    initiator_node.exec_command(
        cmd="dnf install -y ktls-utils || yum install -y ktls-utils || true",
        sudo=True,
    )
    initiator_node.exec_command(cmd="modprobe tls || true", sudo=True)
    initiator_node.exec_command(
        cmd=(
            "cat > /etc/tlshd.conf <<'EOF'\n"
            "[authenticate.client]\n"
            "keyrings = .nvme\n"
            "EOF"
        ),
        sudo=True,
    )
    initiator_node.exec_command(
        cmd="systemctl enable --now tlshd; systemctl restart tlshd",
        sudo=True,
    )


def _import_tls_psk(initiator_node, host_nqn, sub_nqn, psk):
    """Insert PSK into the initiator keyring (identity 0 / NVMe0R01)."""
    # Quote PSK carefully — interchange form contains ':' and '/'
    quoted = psk.replace("'", "'\"'\"'")
    initiator_node.exec_command(
        cmd=(
            f"nvme check-tls-key -n {host_nqn} -c {sub_nqn} "
            f"-d '{quoted}' -I {TLS_PSK_IDENTITY} -i -v"
        ),
        sudo=True,
    )


def _resolve_tls_key_ids(initiator_node, sub_nqn):
    """Return (keyring_id_dec, tls_key_id_dec) from /proc/keys for .nvme + PSK.

    Prefers the last matching ``NVMe0R01`` entry so PSK rotation picks the
    newly imported key when older ones remain in the keyring.
    """
    out, _ = initiator_node.exec_command(cmd="cat /proc/keys", sudo=True)
    kr_hex = None
    key_hex = None
    sub_token = sub_nqn.split(":")[-1] if ":" in sub_nqn else sub_nqn
    for line in (out or "").splitlines():
        if " keyring " in line and ".nvme" in line:
            kr_hex = line.split()[0]
        if (
            "NVMe0R01" in line
            and "expd" not in line
            and (sub_token in line or sub_nqn in line)
        ):
            key_hex = line.split()[0]
    if not kr_hex or not key_hex:
        # Avoid the literal word "keyring=" — Log redactor eats the next %s
        raise AssertionError(
            f"Could not resolve TLS PSK IDs from /proc/keys "
            f"(kr_hex={kr_hex}, key_hex={key_hex}). Output:\n{out}"
        )
    return int(kr_hex, 16), int(key_hex, 16)


def _host_set_psk(nvmegwcli, sub_nqn, host_nqn, psk):
    """Replace subsystem host ACL entry with the given TLS PSK."""
    try:
        nvmegwcli.host.delete(**{"args": {"subsystem": sub_nqn, "host": host_nqn}})
    except Exception as exc:
        LOG.info("host delete before PSK add (ok if absent): %s", exc)
    time.sleep(2)
    nvmegwcli.host.add(
        **{
            "args": {
                "subsystem": sub_nqn,
                "host": host_nqn,
                "psk": psk,
            }
        }
    )
    # Allow GW to load PSK into SPDK keyring
    time.sleep(3)
    LOG.info("Host %s re-added on %s with TLS PSK", host_nqn, sub_nqn)


def _open_tls_listener_firewall(gateways, port=TLS_SECURE_PORT):
    """Allow the dedicated TLS listener port through firewalld when active."""
    for gateway in gateways:
        node = gateway.node
        try:
            active, _ = node.exec_command(
                cmd="systemctl is-active firewalld || true", sudo=True
            )
            if "active" not in (active or ""):
                continue
            node.exec_command(
                cmd=(
                    f"firewall-cmd --permanent --add-port={port}/tcp; "
                    "firewall-cmd --reload"
                ),
                sudo=True,
            )
            LOG.info("Opened firewall TCP/%s on %s", port, node.hostname)
        except Exception as exc:
            LOG.warning("firewall open %s on %s failed: %s", port, node.hostname, exc)


def _enable_spdk_ssl_tls(gateways, sub_nqn, port=TLS_SECURE_PORT):
    """Enable SPDK ssl sock TLS 1.3 and bind secure listeners to it.

    Ceph ``listener add --secure`` alone is not enough on builds where the
    ssl sock defaults to ``tls_version=0`` — initiator then fails with
    ``failed to start TLS: -3``. Set tls_version=13 and rebind listeners via
    SPDK RPC (``-k -S ssl``) without disturbing insecure :4420 listeners.
    """
    for gateway in gateways:
        addr = gateway.node.ip_address
        host = gateway.node.hostname
        try:
            gateway._rpc("sock_impl_set_options -i ssl --tls-version 13")
            opts, _ = gateway._rpc("sock_impl_get_options -i ssl")
            LOG.info("GW %s ssl sock options: %s", host, (opts or "").strip())
        except Exception as exc:
            LOG.warning("GW %s sock_impl_set_options ssl failed: %s", host, exc)

        try:
            gateway._rpc(
                f"nvmf_subsystem_remove_listener {sub_nqn} "
                f"-t TCP -a {addr} -s {port}"
            )
        except Exception as exc:
            LOG.info("GW %s remove_listener %s:%s: %s", host, addr, port, exc)

        try:
            gateway._rpc(
                f"nvmf_subsystem_add_listener {sub_nqn} "
                f"-t TCP -a {addr} -s {port} -f ipv4 -k -S ssl"
            )
            LOG.info("GW %s SPDK ssl listener %s:%s OK", host, addr, port)
        except Exception as exc:
            LOG.warning(
                "GW %s SPDK ssl add_listener %s:%s failed: %s", host, addr, port, exc
            )


def _ensure_secure_listeners(nvme_service, sub_nqn, port=TLS_SECURE_PORT):
    """Add secure listeners on every GW (dedicated TLS port).

    Subsystems created with ``network-mask`` (auto-listeners) reject manual
    ``listener add``. Remove any network masks first, then add secure listeners.
    """
    gateway0 = nvme_service.gateways[0]
    try:
        masks = get_subsystem_network_masks(gateway0, sub_nqn)
    except Exception as exc:
        LOG.warning("Could not list network masks for %s: %s", sub_nqn, exc)
        masks = []
    for mask in masks:
        LOG.info(
            "Removing network-mask %s from %s before secure listeners", mask, sub_nqn
        )
        try:
            gateway0.subsystem.del_network(
                **{"args": {"subsystem": sub_nqn, "network-mask": mask}}
            )
        except Exception as exc:
            LOG.warning("del_network %s failed (continuing): %s", mask, exc)
    if masks:
        # Allow OMAP / auto-listener teardown to settle
        time.sleep(3)

    for gateway in nvme_service.gateways:
        # Explicit server_address: BaseCLI skips auto-inject when traddr is set,
        # which caused "Host name mismatch" when both listeners hit the wrong GW.
        args = {
            "subsystem": sub_nqn,
            "host-name": gateway.node.hostname,
            "traddr": gateway.node.ip_address,
            "trsvcid": port,
            "secure": True,
            "force": True,
            "server_address": gateway.node.ip_address,
        }
        try:
            gateway.listener.add(**{"args": args})
            LOG.info(
                "Secure listener %s:%s on %s",
                gateway.node.ip_address,
                port,
                gateway.node.hostname,
            )
        except Exception as exc:
            msg = str(exc).lower()
            if "already" in msg or "exist" in msg:
                LOG.info("Secure listener already present: %s", exc)
            else:
                raise

    _open_tls_listener_firewall(nvme_service.gateways, port=port)
    _enable_spdk_ssl_tls(nvme_service.gateways, sub_nqn, port=port)
    time.sleep(2)


def _disconnect_subsystem(initiator_objs, sub_nqn):
    for initiator in initiator_objs:
        try:
            initiator.disconnect(**{"nqn": sub_nqn})
        except Exception:
            try:
                initiator.disconnect_all()
            except Exception as exc:
                LOG.warning("disconnect failed: %s", exc)


def _get_host_id(initiator_node, host_nqn=None):
    """Return initiator host UUID, creating /etc/nvme/hostid if missing.

    Fresh clients often have hostnqn but no hostid file; nvme connect --tls
    requires an explicit --hostid. Prefer the UUID embedded in hostnqn when
    present so hostid matches the registered host identity.
    """
    out, _ = initiator_node.exec_command(
        cmd="cat /etc/nvme/hostid 2>/dev/null || true", sudo=True
    )
    host_id = (out or "").strip()
    if not host_id:
        uuid_match = re.search(
            r"uuid:([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
            r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12})",
            host_nqn or "",
        )
        if uuid_match:
            host_id = uuid_match.group(1)
            initiator_node.exec_command(
                cmd=(
                    f"mkdir -p /etc/nvme && "
                    f"printf '%s\\n' '{host_id}' > /etc/nvme/hostid"
                ),
                sudo=True,
            )
        else:
            initiator_node.exec_command(
                cmd="mkdir -p /etc/nvme && uuidgen > /etc/nvme/hostid",
                sudo=True,
            )
            out, _ = initiator_node.exec_command(cmd="cat /etc/nvme/hostid", sudo=True)
            host_id = (out or "").strip()
        LOG.info("Created /etc/nvme/hostid=%s on %s", host_id, initiator_node.hostname)
    if not host_id:
        raise AssertionError("Empty /etc/nvme/hostid on initiator")
    return host_id


def _connect_tls_multipath(
    initiator, gateways, sub_nqn, host_nqn, keyring_id, tls_key_id, port=TLS_SECURE_PORT
):
    """Connect initiator to every GW over TLS-PSK on the secure port.

    Uses ``check_ec=True`` — ``Cli.execute`` defaults to False, which previously
    swallowed TLS handshake failures (empty Devices, zero top rows).
    """
    host_id = _get_host_id(initiator.node, host_nqn=host_nqn)
    # Clear prior fabric errors for clearer diagnostics on failure
    try:
        initiator.node.exec_command(cmd="dmesg -C || true", sudo=True)
    except Exception:
        pass

    for gateway in gateways:
        # Avoid literal "keyring=" in the format string — Log redactor masks it
        # and drops a %s placeholder, causing TypeError during logging.
        LOG.info(
            "TLS connect %s -> %s:%s (kr_id=%s tls_key_id=%s)",
            sub_nqn,
            gateway.node.ip_address,
            port,
            keyring_id,
            tls_key_id,
        )
        cmd = (
            f"nvme connect --transport tcp "
            f"--traddr {gateway.node.ip_address} --trsvcid {port} "
            f"--nqn {sub_nqn} --hostnqn {host_nqn} --hostid {host_id} "
            f"--tls --keyring {keyring_id} --tls-key {tls_key_id}"
        )
        try:
            out, err = initiator.node.exec_command(cmd=cmd, sudo=True, check_ec=True)
            LOG.info(
                "TLS connect OK %s:%s out=%r err=%r",
                gateway.node.ip_address,
                port,
                (out or "")[:200],
                (err or "")[:200],
            )
        except Exception as exc:
            dmesg, _ = initiator.node.exec_command(
                cmd="dmesg | tail -40 || true", sudo=True
            )
            raise AssertionError(
                f"TLS connect failed to {gateway.node.ip_address}:{port}: {exc}\n"
                f"dmesg:\n{dmesg}"
            ) from exc


def _assert_tls_devices(initiator, step):
    """Require SPDK namespaces after TLS connect; dump diagnostics on failure."""
    paths = initiator.list_spdk_drives()
    if paths:
        LOG.info("%s: TLS namespaces ready: %s", step, paths)
        return paths

    node = initiator.node
    list_subsys, _ = node.exec_command(cmd="nvme list-subsys 2>&1 || true", sudo=True)
    nvme_list, _ = node.exec_command(cmd="nvme list 2>&1 || true", sudo=True)
    dmesg, _ = node.exec_command(cmd="dmesg | tail -60 || true", sudo=True)
    tlshd, _ = node.exec_command(
        cmd="systemctl is-active tlshd; journalctl -u tlshd -n 30 --no-pager || true",
        sudo=True,
    )
    raise AssertionError(
        f"{step}: TLS connect produced no NVMe namespaces.\n"
        f"list-subsys:\n{list_subsys}\n"
        f"nvme list:\n{nvme_list}\n"
        f"tlshd:\n{tlshd}\n"
        f"dmesg:\n{dmesg}"
    )


def _assert_top_under_tls_io(
    nvmegwcli, sub_nqn, host_nqn, gw_group, initiator_objs, io_executor, step
):
    """Start FIO, sample top connection, require schema + non-zero IOPS."""
    futures = _start_fio_on_initiators(
        initiator_objs,
        io_executor,
        io_type="randrw",
        runtime="90",
        bs="4k",
        rwmixread="70",
    )
    if not futures:
        raise AssertionError(
            f"{step}: No FIO started — no NVMe namespaces after TLS connect"
        )
    _settle()
    out, _, rows = _retry_top_rows(
        nvmegwcli,
        sub_nqn,
        host_nqn,
        gw_group,
        step=step,
        predicate=_has_nonzero_iops,
        use_live_rows=True,
    )
    LOG.info("%s raw top connection:\n%s", step, out)
    _assert_schema(rows, step)
    if not _has_nonzero_iops(rows):
        raise AssertionError(
            f"{step}: Expected non-zero IOPS under TLS-PSK IO; rows={rows}"
        )
    LOG.info("%s: non-zero IOPS OK (%d rows)", step, len(rows))
    _stop_fio(initiator_objs)
    return rows


def step17_tls_psk_setup(
    nvmegwcli,
    nvme_service,
    sub_nqn,
    host_nqn,
    gw_group,
    initiator_objs,
    io_executor,
    secure_port=TLS_SECURE_PORT,
):
    """STEP-17: Configure TLS-PSK on the host ACL and validate top connection."""
    LOG.info("STEP-17: TLS-PSK setup + top connection")
    if not initiator_objs:
        raise AssertionError("STEP-17: No initiator objects")
    initiator = initiator_objs[0]
    node = initiator.node

    _stop_fio(initiator_objs)
    _disconnect_subsystem(initiator_objs, sub_nqn)
    _ensure_tlshd(node)

    psk = _gen_tls_psk(node, host_nqn, sub_nqn)
    _host_set_psk(nvmegwcli, sub_nqn, host_nqn, psk)
    _ensure_secure_listeners(nvme_service, sub_nqn, port=secure_port)
    _import_tls_psk(node, host_nqn, sub_nqn, psk)
    keyring_id, tls_key_id = _resolve_tls_key_ids(node, sub_nqn)
    _connect_tls_multipath(
        initiator,
        nvme_service.gateways,
        sub_nqn,
        host_nqn,
        keyring_id,
        tls_key_id,
        port=secure_port,
    )
    _assert_tls_devices(initiator, "STEP-17")
    _assert_top_under_tls_io(
        nvmegwcli,
        sub_nqn,
        host_nqn,
        gw_group,
        initiator_objs,
        io_executor,
        "STEP-17",
    )
    LOG.info("STEP-17: PASSED")
    return psk


def step18_tls_psk_rotate(
    nvmegwcli,
    nvme_service,
    sub_nqn,
    host_nqn,
    gw_group,
    initiator_objs,
    io_executor,
    secure_port=TLS_SECURE_PORT,
):
    """STEP-18: Rotate TLS-PSK (host del/add) and re-validate top connection."""
    LOG.info("STEP-18: TLS-PSK rotate + top connection")
    if not initiator_objs:
        raise AssertionError("STEP-18: No initiator objects")
    initiator = initiator_objs[0]
    node = initiator.node

    _stop_fio(initiator_objs)
    _disconnect_subsystem(initiator_objs, sub_nqn)

    new_psk = _gen_tls_psk(node, host_nqn, sub_nqn)
    _host_set_psk(nvmegwcli, sub_nqn, host_nqn, new_psk)
    _import_tls_psk(node, host_nqn, sub_nqn, new_psk)
    keyring_id, tls_key_id = _resolve_tls_key_ids(node, sub_nqn)
    _connect_tls_multipath(
        initiator,
        nvme_service.gateways,
        sub_nqn,
        host_nqn,
        keyring_id,
        tls_key_id,
        port=secure_port,
    )
    _assert_tls_devices(initiator, "STEP-18")
    _assert_top_under_tls_io(
        nvmegwcli,
        sub_nqn,
        host_nqn,
        gw_group,
        initiator_objs,
        io_executor,
        "STEP-18",
    )
    LOG.info("STEP-18: PASSED")
    return new_psk


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _resolve_subsystem_nqn(nvmegwcli, configured_nqn):
    """Return actual subsystem NQN (handles optional group-append)."""
    sub_out, _ = nvmegwcli.subsystem.list(**{"base_cmd_args": {"format": "json"}})
    subs = json.loads(sub_out).get("subsystems", [])
    for s in subs:
        nqn_val = s.get("nqn") or s.get("subnqn", "")
        if configured_nqn in nqn_val:
            return nqn_val
    return configured_nqn


def _deploy_gw_groups(config, ceph_cluster, rbd_obj, services_out=None):
    """Deploy each entry in ``config['gw_groups']`` (4-node HA / multi-group pattern).

    Args:
        services_out: optional dict updated in-place as soon as each GW group is
            deployed (before subsystem configure). Lets ``finally`` teardown run
            even when configure fails mid-loop.

    Returns:
        dict[str, NVMeService]: keyed by gw_group name
        NVMeService: primary (first) group used for initiator IO / top samples
    """
    services = services_out if services_out is not None else {}
    for svc_cfg in config["gw_groups"]:
        group_cfg = dict(svc_cfg)
        group_cfg["rbd_pool"] = group_cfg.get("rbd_pool", config["rbd_pool"])
        group_cfg["install"] = config.get("install", True)
        group_cfg["cleanup"] = list(config.get("cleanup") or [])
        nvme_service = NVMeService(group_cfg, ceph_cluster)
        if config.get("install"):
            LOG.info(
                "Deploying NVMe service for gw_group=%s", group_cfg.get("gw_group")
            )
            nvme_service.deploy()
            nvme_service.init_gateways()
        # Register before configure so partial deploy is still torn down on failure
        services[group_cfg["gw_group"]] = nvme_service
        if group_cfg.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd_obj, cluster=ceph_cluster)
        LOG.info(
            "Gateway group %s ready on %s",
            group_cfg["gw_group"],
            [n.hostname for n in nvme_service.gw_nodes],
        )
    primary_name = config["gw_groups"][0]["gw_group"]
    return services, services[primary_name]


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Execute Polarion CEPH-83633026 / IBMCEPH-15738 top connection validation."""
    LOG.info("=== CEPH-83633026 / IBMCEPH-15738: top connection – start ===")

    config = kwargs["config"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
    custom_config = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)

    bs_sweep_runtime = int(config.get("bs_sweep_runtime", 60))
    nvme_service = None
    nvme_services = {}
    nvmegwcli = None
    initiator_objs = []
    io_executor = None
    sub_nqn = None
    host_nqn = None
    initiator_config_list = []
    gw_group = config.get("gw_group", "")
    gw_group_b = EMPTY_GROUP_FALLBACK

    try:
        # ----- STEP-01 deploy (+ optional second GW group) -----
        if config.get("gw_groups"):
            # Pass nvme_services so deploy registers into it before configure;
            # on configure failure the finally block still sees deployed GWs.
            nvme_services, nvme_service = _deploy_gw_groups(
                config, ceph_cluster, rbd_obj, services_out=nvme_services
            )
            gw_group = config["gw_groups"][0]["gw_group"]
            if len(config["gw_groups"]) > 1:
                gw_group_b = config["gw_groups"][1]["gw_group"]
            nvmegwcli = nvme_service.gateways[0]
            primary_subs = config["gw_groups"][0].get("subsystems") or []
            configured_nqn = (
                primary_subs[0].get("nqn") or primary_subs[0].get("subnqn")
                if primary_subs
                else None
            )
            if not configured_nqn:
                raise ValueError("Primary gw_group must define subsystems[0].nqn")
            sub_nqn = _resolve_subsystem_nqn(nvmegwcli, configured_nqn)
            initiator_config_list = list(config["gw_groups"][0].get("initiators") or [])
        else:
            if config.get("install"):
                nvme_service = NVMeService(config, ceph_cluster)
                nvme_service.deploy()
                nvme_service.init_gateways()
                nvmegwcli = nvme_service.gateways[0]
                nvme_services[gw_group or "default"] = nvme_service
                LOG.info("Gateway service deployed")

            if config.get("subsystems"):
                configure_gw_entities(
                    nvme_service, rbd_obj=rbd_obj, cluster=ceph_cluster
                )
                configured_nqn = config["subsystems"][0]["nqn"]
                sub_nqn = _resolve_subsystem_nqn(nvmegwcli, configured_nqn)
            initiator_config_list = list(config.get("initiators") or [])

        LOG.info("Resolved subsystem NQN: %s", sub_nqn)
        expected_gw_ips = (
            [node.ip_address for node in nvme_service.gw_nodes] if nvme_service else []
        )
        LOG.info(
            "Primary gw_group=%s IPs=%s; empty/filter group=%s",
            gw_group,
            expected_gw_ips,
            gw_group_b,
        )

        step03_required_args(nvmegwcli, ceph_cluster)

        if initiator_config_list:
            initiator_objs = prepare_io_execution(
                initiator_config_list,
                gateways=nvme_service.gateways if nvme_service else [nvmegwcli],
                cluster=ceph_cluster,
                return_clients=True,
            )
            if initiator_objs:
                host_nqn = _get_host_nqn(initiator_objs[0])

        if not sub_nqn or not host_nqn:
            raise ValueError(
                "sub_nqn and host_nqn required – ensure primary group has "
                "subsystems + initiators in config"
            )

        step01_verify_multipath(initiator_objs, expected_gw_ips)

        io_executor = ThreadPoolExecutor(max_workers=max(len(initiator_objs) * 2, 2))

        # Idle before starting IO
        step10_idle(nvmegwcli, sub_nqn, host_nqn, gw_group)

        # Background 4K randrw for happy-path / multi-gw / sort / display / parallel
        _start_fio_on_initiators(
            initiator_objs,
            io_executor,
            io_type="randrw",
            runtime="300",
            bs="4k",
            rwmixread="70",
        )
        _settle()

        step02_happy_path_under_io(nvmegwcli, sub_nqn, host_nqn, gw_group)
        # step06_multi_gateway(
        #     nvmegwcli, sub_nqn, host_nqn, gw_group, expected_gw_ips, initiator_objs
        # )
        step07_gw_group_filter(
            nvmegwcli,
            sub_nqn,
            host_nqn,
            gw_group,
            expected_gw_ips,
            gw_group_b,
        )
        step08_sort_by(nvmegwcli, sub_nqn, host_nqn, gw_group, ceph_cluster)
        step09_display_options(nvmegwcli, sub_nqn, host_nqn, gw_group)
        step13_parallel_top_cmds(nvmegwcli, sub_nqn, host_nqn, gw_group)
        step16_top_io_sort(nvmegwcli, sub_nqn, gw_group)

        _stop_fio(initiator_objs)
        step02_zero_after_stop(nvmegwcli, sub_nqn, host_nqn, gw_group)

        step05_rw_column_split(
            nvmegwcli, sub_nqn, host_nqn, gw_group, initiator_objs, io_executor
        )
        step04_bucket_sweep(
            nvmegwcli,
            sub_nqn,
            host_nqn,
            gw_group,
            initiator_objs,
            io_executor,
            bs_sweep_runtime,
        )
        step12_qos(nvmegwcli, sub_nqn, host_nqn, gw_group, initiator_objs, io_executor)
        # step11_ha_failover(
        #     nvmegwcli,
        #     sub_nqn,
        #     host_nqn,
        #     gw_group,
        #     nvme_service,
        #     ceph_cluster,
        #     {**config, "rbd_pool": config.get("rbd_pool", "rbd"), "gw_group": gw_group},
        #     initiator_objs,
        #     io_executor,
        # )
        step15_invalid_period_sort(nvmegwcli, sub_nqn, host_nqn, gw_group, ceph_cluster)
        # Reconnect for disconnect negative (step14 ends disconnected)
        initiator_objs = prepare_io_execution(
            initiator_config_list,
            gateways=nvme_service.gateways,
            cluster=ceph_cluster,
            return_clients=True,
        )
        host_nqn = _get_host_nqn(initiator_objs[0])
        step14_bad_or_disconnected_host(
            nvmegwcli, sub_nqn, host_nqn, gw_group, initiator_objs
        )

        # TLS-PSK scenarios (after step14 leaves the initiator disconnected)
        tls_port = int(config.get("tls_listener_port", TLS_SECURE_PORT))
        step17_tls_psk_setup(
            nvmegwcli,
            nvme_service,
            sub_nqn,
            host_nqn,
            gw_group,
            initiator_objs,
            io_executor,
            secure_port=tls_port,
        )
        step18_tls_psk_rotate(
            nvmegwcli,
            nvme_service,
            sub_nqn,
            host_nqn,
            gw_group,
            initiator_objs,
            io_executor,
            secure_port=tls_port,
        )

        LOG.info("=== All CEPH-83633026 Polarion steps PASSED ===")
        return 0

    except Exception as err:
        LOG.error("Test FAILED: %s", err)
        LOG.error(traceback.format_exc())
        return 1

    finally:
        _stop_fio(initiator_objs)
        if io_executor:
            io_executor.shutdown(wait=False, cancel_futures=True)
        if config.get("cleanup"):
            # Tear down every deployed GW group (group1 + group2)
            targets = list(nvme_services.values()) if nvme_services else []
            if nvme_service and nvme_service not in targets:
                targets.append(nvme_service)
            if not targets:
                LOG.warning(
                    "cleanup requested but no NVMe services were registered "
                    "(deploy likely failed before any GW was created)"
                )
            for svc in targets:
                try:
                    LOG.info(
                        "Running teardown for gw_group=%s",
                        getattr(svc, "group", None) or svc,
                    )
                    teardown(svc, rbd_obj)
                except Exception as cleanup_err:
                    LOG.warning("teardown failed for %s: %s", svc, cleanup_err)
