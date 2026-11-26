"""Helpers for NFS xcopy / COPY offload validation."""

import re
import time
from contextlib import contextmanager

from cli.exceptions import OperationFailedError
from tests.nfs.lib.common_lib import (
    enable_nfs_debug_logs,
    get_nfs_container_id,
    podman_nfs_logs,
    wait_nfs_service_running,
)
from tests.nfs.security.security_utils import ensure_package_installed
from utility.log import Log

log = Log(__name__)

DEFAULT_PCAP_PATH = "/tmp/nfs-xcopy-cp.pcap"
DEFAULT_MAX_COPY_WORKERS = 8
# cephadm custom Ganesha template (NOT the ingress/haproxy key).
GANESHA_CONF_KEY = "mgr/cephadm/services/nfs/ganesha.conf"
GANESHA_DEFAULT_TEMPLATE = (
    "/usr/share/ceph/mgr/cephadm/templates/services/nfs/ganesha.conf.j2"
)
GANESHA_WORK_TEMPLATE = "/tmp/nfs_xcopy_ganesha.conf.j2"
GANESHA_MOUNTED_TEMPLATE = "/var/lib/ceph/ganesha.conf"
RE_NFSV4_BLOCK = re.compile(r"(NFSv4\s*\{)(.*?)(\n\})", re.S)
# Per-hostname cache so ensure_xcopy_pcap_tools is not re-run.
_PCAP_TOOLS_READY = set()
# NFSv4 opcodes (RFC 7530 / 7862) as reported by tshark -e nfs.opcode
NFS_OPCODE_READ = "25"
NFS_OPCODE_WRITE = "38"
NFS_OPCODE_COPY = "60"  # OP_COPY
# Packages tried (in order) when a capture/analysis binary is missing.
PCAP_BINARIES = {
    "tcpdump": ("tcpdump",),
    "tshark": ("wireshark-cli", "wireshark"),
    "capinfos": ("wireshark-cli", "wireshark"),
}
# Capinfos / tshark expectations (Copy_Offload true vs false).
# ON:  packets scale with COPY count (see pcap_on_max_packets); data = KB–low MB;
#      opcode 60 yes; 25/38 == 0
# OFF: packets = tens of thousands; data ≈ 2× file; 25/38 flood yes
PCAP_ON_MAX_PACKETS = 200  # floor for single-COPY / small-file captures
PCAP_ON_BASE_PACKETS = 100  # open/lookup/getattr/close + compound overhead
PCAP_ON_PACKETS_PER_COPY = 12  # ~7 seen per Short_Copy continuation; headroom
PCAP_ON_MAX_DATA_BYTES = 16 * 1024 * 1024  # 16 MiB ("low MB")
PCAP_ON_MAX_RW_OPS = 0  # READ(25)/WRITE(38) must be absent for offload
PCAP_OFF_MIN_PACKETS = 10000
PCAP_OFF_DATA_RATIO_MIN = 1.5
PCAP_OFF_DATA_RATIO_MAX = 2.5
PCAP_OFF_MIN_RW_OPS = 1000
# Wireshark 4.x prints "10 kB" / "11 kB"; older builds use "N bytes".
RE_CAPINFOS_DATA_SIZE = re.compile(
    r"Data size:\s*([\d,.]+)\s*(bytes?|kB|KiB|KB|MB|MiB|GB|GiB)?",
    re.I,
)
RE_CAPINFOS_FILE_SIZE = re.compile(
    r"File size:\s*([\d,.]+)\s*(bytes?|kB|KiB|KB|MB|MiB|GB|GiB)?",
    re.I,
)
RE_CAPINFOS_PACKETS = re.compile(r"Number of packets:\s*([\d,]+)", re.I)
RE_OPCODE_HIST = re.compile(r"^\s*(\d+)\s+(\S+)\s*$")
_CAPINFOS_UNITS = {
    None: 1,
    "": 1,
    "byte": 1,
    "bytes": 1,
    "kb": 1000,
    "kib": 1024,
    "mb": 1000**2,
    "mib": 1024**2,
    "gb": 1000**3,
    "gib": 1024**3,
}

RE_WORKER_START = re.compile(
    r"COPY (SYNC|ASYNC) worker START:\s*total=(\d+)\s+bytes\s+"
    r"src_off=(\d+)\s+dst_off=(\d+)\s+chunk_size=(\d+)",
    re.I,
)
RE_MDCACHE_OK = re.compile(r"COPY mdcache:\s*done OK copied=(\d+)", re.I)
RE_CB_OFFLOAD = re.compile(r"CB_OFFLOAD:\s*sending OK bytes=(\d+)\s+seqid=", re.I)
RE_OFFLOAD_STATE = re.compile(r"create_copy_offload_state", re.I)

# Remote grep ERE fragments (OR'd into one cumulative grep).
# Keep in sync with the RE_* validators above so evidence is easy to spot.
XCOPY_GREP_PATTERNS = (
    r"COPY (SYNC|ASYNC) worker START",
    r"COPY (SYNC|ASYNC) chunk #",
    r"COPY mdcache:.*done OK",
    r"CB_OFFLOAD:.*sending OK",
    r"create_copy_offload_state",
)


def announce(step):
    """Banner so each xcopy step is obvious in cephci logs."""
    log.info("\n========== [xcopy] %s ==========\n", step)


def case_begin(name, tag=None):
    """Single CASE banner (replaces three-line separator blocks)."""
    announce(f"CASE: {name}" + (f" ({tag})" if tag else ""))


def case_pass(name):
    """PASS CASE banner."""
    announce(f"PASS CASE: {name}")


def log_case_summary(results, title="XCOPY CASE SUMMARY"):
    """Print pass/fail/skipped table. Return 1 if any failed or none passed."""
    passed = [n for n, s in results.items() if s == "passed"]
    failed = [n for n, s in results.items() if s == "failed"]
    skipped = [n for n, s in results.items() if s == "skipped"]
    rows = (
        "\n".join(f"  {n:<40} {s.upper()}" for n, s in results.items())
        if results
        else "  (no cases recorded)"
    )
    log.info(
        "\n%s\n%s\n%s\n%s\n%s\n  Total: %s  Passed: %s  Failed: %s  Skipped: %s\n%s",
        "=" * 60,
        title,
        "=" * 60,
        rows,
        "-" * 60,
        len(results),
        len(passed),
        len(failed),
        len(skipped),
        "=" * 60,
    )
    if failed or not results or (skipped and not passed):
        return 1
    return 0


def ensure_background_cp_stopped(client, pid, rc_file=None, tag=None):
    """Best-effort stop leftover ``start_background_cp`` session (KILL + wait)."""
    if not pid:
        return
    from tests.nfs.lib.xcopy.async_helpers import signal_process

    try:
        signal_process(client, int(pid), sig="KILL", escalate=True, tag=tag)
    except Exception as exc:
        log.warning("bg cp kill best-effort failed pid=%s: %s", pid, exc)
    if rc_file:
        try:
            wait_background_cp(client, pid, rc_file, timeout=60)
        except Exception as exc:
            log.warning("bg cp wait after kill failed pid=%s: %s", pid, exc)


@contextmanager
def cephfs_subvol_fuse_mount(client, ctx, fuse_mnt="/mnt/cephfs_xcopy_check"):
    """Mount ceph-fuse on the primary NFS export subvolume."""
    export = primary_nfs_export_pseudo(ctx)
    fs_name = ctx.get("fs_name", "cephfs")
    subvol = export.replace("/", "")
    group = "ganeshagroup"
    client.exec_command(sudo=True, cmd=f"mkdir -p {fuse_mnt}", check_ec=False)
    client.exec_command(
        sudo=True,
        cmd=f"if mountpoint -q {fuse_mnt}; then umount -l {fuse_mnt}; fi",
        check_ec=False,
    )
    getpath_out, _ = client.exec_command(
        sudo=True,
        cmd=(f"ceph fs subvolume getpath {fs_name} {subvol} " f"--group_name {group}"),
        check_ec=False,
    )
    ceph_base = (getpath_out or "").strip()
    if client.exit_status != 0 or not ceph_base:
        raise RuntimeError(
            f"subvolume getpath failed for {group}/{subvol}: {ceph_base!r}"
        )
    err_file = f"/tmp/ceph_fuse_{fuse_mnt.strip('/').replace('/', '_')}.err"
    client.exec_command(
        sudo=True,
        cmd=f"ceph-fuse {fuse_mnt} -n client.admin 2>{err_file}",
        check_ec=False,
        timeout=120,
    )
    if client.exit_status != 0:
        err, _ = client.exec_command(sudo=True, cmd=f"cat {err_file}", check_ec=False)
        raise RuntimeError(f"ceph-fuse unavailable: {(err or '').strip()[:500]}")
    try:
        yield fuse_mnt, f"{fuse_mnt}{ceph_base.rstrip('/')}"
    finally:
        client.exec_command(sudo=True, cmd=f"umount -l {fuse_mnt}", check_ec=False)


def _remote_write(node, path, text):
    """Write ``text`` to ``path`` on ``node``."""
    remote = None
    try:
        try:
            remote = node.remote_file(sudo=True, file_name=path, file_mode="w")
            remote.write(text)
            remote.flush()
        except AttributeError:
            remote = node.remote_file(sudo=True, file_name=path, file_mode="wb")
            remote.write(text.encode("utf-8"))
            remote.flush()
    finally:
        if remote and hasattr(remote, "close"):
            remote.close()


def _upsert_conf_key(block_body, key, value):
    """Replace or append ``key = value;`` inside a Ganesha config block body."""
    line = f"\t{key} = {value};"
    pat = re.compile(rf"(?m)^(\s*){re.escape(key)}\s*=\s*[^;]+;")
    if pat.search(block_body):
        return pat.sub(rf"\g<1>{key} = {value};", block_body, count=1)
    return block_body.rstrip() + f"\n{line}\n"


def _inject_nfsv4_copy_flags(template, flags):
    """Upsert Copy_Offload / Short_Copy / chunk / workers into the NFSv4 block."""
    match = RE_NFSV4_BLOCK.search(template)
    if not match:
        raise OperationFailedError(
            "Ganesha template missing NFSv4 {} block; cannot inject Copy_Offload"
        )
    body = match.group(2)
    for key, value in flags.items():
        if value is None:
            continue
        body = _upsert_conf_key(body, key, value)
    return (
        template[: match.start()]
        + match.group(1)
        + body
        + match.group(3)
        + template[match.end() :]
    )


def _load_ganesha_template(installer, work_file=GANESHA_WORK_TEMPLATE):
    """Load config-key override, else cephadm default ganesha.conf.j2, into work_file."""
    installer.exec_command(
        sudo=True,
        cmd=(
            f"cephadm shell -- ceph config-key get {GANESHA_CONF_KEY} " f"> {work_file}"
        ),
        check_ec=False,
    )
    installer.exec_command(
        sudo=True,
        cmd=(
            f"test -s {work_file} || "
            f"cephadm shell -- cat {GANESHA_DEFAULT_TEMPLATE} > {work_file}"
        ),
    )
    out, _ = installer.exec_command(sudo=True, cmd=f"cat {work_file}", check_ec=False)
    text = out or ""
    if "NFSv4" not in text or "NFS_CORE_PARAM" not in text:
        raise OperationFailedError(
            f"Unable to load a complete Ganesha template into {work_file} "
            f"(config-key {GANESHA_CONF_KEY} or {GANESHA_DEFAULT_TEMPLATE})"
        )
    return text


def _apply_ganesha_template(installer, text, work_file=GANESHA_WORK_TEMPLATE):
    """Write edited template and ``ceph config-key set`` it."""
    _remote_write(installer, work_file, text)
    installer.exec_command(
        sudo=True,
        cmd=(
            f"cephadm shell --mount {work_file}:{GANESHA_MOUNTED_TEMPLATE} "
            f"-- ceph config-key set {GANESHA_CONF_KEY} -i {GANESHA_MOUNTED_TEMPLATE}"
        ),
    )
    verify, _ = installer.exec_command(
        sudo=True,
        cmd=f"cephadm shell -- ceph config-key get {GANESHA_CONF_KEY}",
        check_ec=False,
    )
    return verify or ""


def primary_nfs_export_pseudo(ctx):
    """Return the NFS export pseudo for the primary mount."""
    pairs = ctx.get("mount_export_pairs") or []
    if pairs:
        return pairs[0][1]
    return f"{ctx['nfs_export']}_0"


def _xcopy_mount_export_pairs(ctx):
    """Return [(mount_path, export_path), ...] for the active xcopy layout."""
    if ctx.get("mount_export_pairs"):
        return list(ctx["mount_export_pairs"])
    if ctx.get("need_cross"):
        return [
            (ctx["nfs_mount_a"], f"{ctx['nfs_export']}_0"),
            (ctx["nfs_mount_b"], f"{ctx['nfs_export']}_1"),
        ]
    # Intra / single_export: one shared mount, export index 0.
    return [(ctx["nfs_mount"], primary_nfs_export_pseudo(ctx))]


def mount_xcopy_nfs(ctx, remount=False):
    """Mount xcopy exports (optionally lazy-umount first after redeploy)."""
    from nfs_operations import mount_nfs_export_with_wait

    action = "Remount" if remount else "Mount"
    announce(f"{action} NFS export(s) for xcopy")
    server = ctx["server_ip"]
    version = ctx["version"]
    port = str(ctx["port"])
    pairs = _xcopy_mount_export_pairs(ctx)

    for client in ctx["clients"]:
        for mount, export in pairs:
            if remount:
                client.exec_command(sudo=True, cmd=f"umount -l {mount}", check_ec=False)
            mount_nfs_export_with_wait(
                client,
                server,
                port,
                export,
                mount,
                version,
                installer_node=ctx.get("installer"),
                nfs_name=ctx.get("nfs_name"),
            )
            client.exec_command(
                sudo=True, cmd=f"chown cephuser:cephuser {mount}", check_ec=False
            )
            log.info(
                "%sed %s:%s at %s on %s",
                action,
                server,
                export,
                mount,
                client.hostname,
            )


def remount_xcopy_nfs(ctx, extra_mount_options=None):
    """Lazy-umount and remount after a mid-run Ganesha redeploy."""
    if not extra_mount_options:
        mount_xcopy_nfs(ctx, remount=True)
        return

    client = ctx["client"]
    announce(f"Remount NFS {ctx['nfs_mount_a']} with extra opts on {client.hostname}")
    client.exec_command(
        sudo=True,
        cmd=(
            f"if mountpoint -q {ctx['nfs_mount_a']}; then "
            f"umount -l {ctx['nfs_mount_a']}; fi"
        ),
        check_ec=False,
    )
    export = primary_nfs_export_pseudo(ctx)
    opts = f"vers={ctx['version']},port={ctx['port']},{extra_mount_options}"
    client.exec_command(sudo=True, cmd=f"mkdir -p {ctx['nfs_mount_a']}")
    client.exec_command(
        sudo=True,
        cmd=(
            f"mount -t nfs -o {opts} "
            f"{ctx['server_ip']}:{export} {ctx['nfs_mount_a']}"
        ),
    )


def ensure_copy_offload(
    installer,
    nfs_name,
    enabled=True,
    chunk_size_mb=64,
    short_copy=False,
    max_copy_workers=DEFAULT_MAX_COPY_WORKERS,
    client=None,
    redeploy=True,
    remount_ctx=None,
    work_file=GANESHA_WORK_TEMPLATE,
    conf_file="nfs_xcopy_log.conf",
):
    """Apply xcopy NFSv4 flags via Ganesha template; LOG via cluster config set."""
    announce(
        f"Cephadm: patch Ganesha template + redeploy nfs.{nfs_name} "
        f"(Copy_Offload={enabled}, Short_Copy={short_copy}, "
        f"Max_Copy_Workers={max_copy_workers})"
    )
    flag = "true" if enabled else "false"
    short_flag = "true" if short_copy else "false"
    chunk_bytes = int(chunk_size_mb) * 1024 * 1024
    workers = int(max_copy_workers)
    flags = {
        "Copy_Offload": flag,
        "Short_Copy": short_flag,
        "Copy_Offload_Chunk_Size": str(chunk_bytes),
        "Max_Copy_Workers": str(workers),
    }

    template = _load_ganesha_template(installer, work_file=work_file)
    template = _inject_nfsv4_copy_flags(template, flags)
    verify = _apply_ganesha_template(installer, template, work_file=work_file)
    for key, value in flags.items():
        needle = f"{key} = {value};"
        if needle not in verify:
            raise OperationFailedError(
                f"config-key {GANESHA_CONF_KEY} missing expected '{needle}'"
            )
    log.info(
        "Updated %s with Copy_Offload=%s Short_Copy=%s chunk=%s (%s MiB) "
        "Max_Copy_Workers=%s",
        GANESHA_CONF_KEY,
        flag,
        short_flag,
        chunk_bytes,
        chunk_size_mb,
        workers,
    )

    # Drop RADOS overlay so older NFSv4 cluster-config values cannot shadow
    # the template; LOG is re-applied below via cluster config set.
    cmd_host = client or installer
    cmd_host.exec_command(
        sudo=True,
        cmd=f"ceph nfs cluster config reset {nfs_name}",
        check_ec=False,
    )

    if redeploy:
        installer.exec_command(
            sudo=True,
            cmd=f"ceph orch redeploy nfs.{nfs_name}",
            timeout=120,
        )
        time.sleep(10)
        running = wait_nfs_service_running(cmd_host, nfs_name, timeout=300)
        if not running:
            raise OperationFailedError(
                f"nfs.{nfs_name} did not return to running after redeploy"
            )

    announce(f"Cephadm: set LOG NFS4=FULL_DEBUG via cluster config on nfs.{nfs_name}")
    enable_nfs_debug_logs(cmd_host, nfs_name, "NFS4", conf_file=conf_file)

    # Mid-run only: initial setup mounts once after this helper returns.
    if remount_ctx is not None:
        remount_xcopy_nfs(remount_ctx)


def create_test_file(client, path, size_mb=64):
    """Create a test file of size_mb MiB under path (parent dirs must exist)."""
    announce(f"Create source file {path} ({size_mb} MiB) on {client.hostname}")
    # /dev/zero is far faster than urandom; content uniqueness is not required.
    client.exec_command(sudo=True, cmd=f"mkdir -p $(dirname {path})")
    client.exec_command(
        sudo=True,
        cmd=f"dd if=/dev/zero of={path} bs=1M count={int(size_mb)} status=none",
        timeout=600,
    )
    out, _ = client.exec_command(sudo=True, cmd=f"ls -lh {path}")
    log.info("Source file created: %s", (out or "").strip())


def sparse_file_layout(size_mb=128, island_mb=8):
    """Return (data_ranges, hole_ranges) in MiB for the XCOPY sparse layout."""
    size_mb = int(size_mb)
    island_mb = int(island_mb)
    if size_mb < 64:
        raise ValueError(f"sparse layout needs size_mb >= 64 (got {size_mb})")
    mid = 32
    end = size_mb - (2 * island_mb)
    data = [
        (0, island_mb),
        (mid, island_mb),
        (end, island_mb),
    ]
    holes = [
        (island_mb, mid - island_mb),
        (mid + island_mb, end - (mid + island_mb)),
        (end + island_mb, size_mb - (end + island_mb)),
    ]
    holes = [(off, length) for off, length in holes if length > 0]
    return data, holes


def create_sparse_test_file(client, path, size_mb=128):
    """Create a sparse file via truncate + partial writes (XCOPY-05/06)."""
    size_mb = int(size_mb)
    data_ranges, hole_ranges = sparse_file_layout(size_mb)
    announce(
        f"Create sparse source {path} (logical={size_mb} MiB, "
        f"data={data_ranges}, holes={hole_ranges}) on {client.hostname}"
    )
    client.exec_command(sudo=True, cmd=f"mkdir -p $(dirname {path})")
    client.exec_command(sudo=True, cmd=f"rm -f {path}")
    client.exec_command(sudo=True, cmd=f"truncate -s {size_mb}M {path}")
    for seek, island_mb in data_ranges:
        client.exec_command(
            sudo=True,
            cmd=(
                f"dd if=/dev/urandom of={path} bs=1M count={island_mb} "
                f"seek={seek} conv=notrunc status=none"
            ),
            timeout=600,
        )
    out, _ = client.exec_command(
        sudo=True,
        cmd=(
            f"stat -c 'logical=%s blocks=%b block_size=%B' {path}; "
            f"du -h {path}; du -h --apparent-size {path}; ls -lh {path}"
        ),
    )
    log.info("Sparse source created:\n%s", (out or "").strip())


def get_file_size_bytes(client, path):
    """Return st_size for path, or 0 if missing."""
    out, _ = client.exec_command(
        sudo=True, cmd=f"stat -c %s {path} 2>/dev/null || echo 0", check_ec=False
    )
    try:
        return int((out or "0").strip().splitlines()[-1])
    except ValueError:
        return 0


def ganesha_service_active(node, nfs_name=None, client=None):
    """Return True if the NFS Ganesha container is running on ``node``."""
    return bool(get_nfs_container_id(node, nfs_name=nfs_name, client=client))


def _wait_remote_pid(client, pid_file, retries=30, sleep_sec=0.1, log_file=None):
    """Poll ``pid_file`` until a positive int appears; raise with optional log."""
    pid = 0
    for _ in range(int(retries)):
        time.sleep(float(sleep_sec))
        out, _ = client.exec_command(sudo=True, cmd=f"cat {pid_file}", check_ec=False)
        try:
            pid = int((out or "0").strip().splitlines()[-1])
        except (ValueError, IndexError):
            pid = 0
        if pid > 0:
            return pid
    detail = ""
    if log_file:
        out, _ = client.exec_command(sudo=True, cmd=f"cat {log_file}", check_ec=False)
        detail = f"; log:\n{(out or '').strip() or '<empty>'}"
    raise RuntimeError(f"pid not published in {pid_file}{detail}")


def start_background_cp(client, src, dst, tag="xcopy_bg"):
    """Start background cp (setsid + trap - INT so SIGINT can cancel offload)."""
    rc_file = f"/tmp/{tag}.rc"
    pid_file = f"/tmp/{tag}.pid"
    cp_pid_file = f"/tmp/{tag}.cp_pid"
    log_file = f"/tmp/{tag}.log"
    client.exec_command(
        sudo=True,
        cmd=f"rm -f {dst} {rc_file} {pid_file} {cp_pid_file} {log_file}",
        check_ec=False,
    )
    client.exec_command(
        sudo=True,
        cmd=(
            f"setsid bash -c '"
            f"trap - INT QUIT; "
            f"cp {src} {dst}; "
            f"echo $? > {rc_file}"
            f"' >{log_file} 2>&1 </dev/null & echo $! > {pid_file}"
        ),
    )
    pid = _wait_remote_pid(client, pid_file, retries=30, log_file=log_file)
    cp_pid = 0
    for _ in range(50):
        out, _ = client.exec_command(
            sudo=True,
            cmd=(
                f"pgrep -P {pid} -x cp 2>/dev/null || "
                f"pgrep -P {pid} 2>/dev/null || true"
            ),
            check_ec=False,
        )
        try:
            cp_pid = int((out or "0").strip().splitlines()[0])
        except (ValueError, IndexError):
            cp_pid = 0
        if cp_pid > 0:
            client.exec_command(
                sudo=True, cmd=f"echo {cp_pid} > {cp_pid_file}", check_ec=False
            )
            break
        time.sleep(0.1)
    log.info(
        "Background cp session pid=%s cp_pid=%s src=%s dst=%s",
        pid,
        cp_pid or "<pending>",
        src,
        dst,
    )
    return pid, rc_file


def wait_background_cp(client, pid, rc_file, timeout=900):
    """Wait for background cp; return int exit code (or 255 on timeout)."""
    client.exec_command(
        sudo=True,
        cmd=(
            f"for i in $(seq 1 {int(timeout)}); do "
            f"  if [ -f {rc_file} ]; then exit 0; fi; "
            f"  if ! kill -0 {int(pid)} 2>/dev/null; then "
            f"    sleep 1; "
            f"    if [ -f {rc_file} ]; then exit 0; fi; "
            f"    echo 255 > {rc_file}; exit 0; "
            f"  fi; "
            f"  sleep 1; "
            f"done; "
            f"echo 255 > {rc_file}"
        ),
        timeout=timeout + 60,
    )
    out, _ = client.exec_command(sudo=True, cmd=f"cat {rc_file}", check_ec=False)
    try:
        rc = int((out or "255").strip().splitlines()[-1])
    except ValueError:
        rc = 255
    log.info("Background cp finished: pid=%s rc=%s", pid, rc)
    return rc


def wait_file_min_size(client, path, min_bytes, timeout=120, poll=0.5):
    """Poll until path size >= min_bytes or timeout. Return final size."""
    deadline = time.time() + float(timeout)
    last = 0
    while time.time() < deadline:
        last = get_file_size_bytes(client, path)
        if last >= int(min_bytes):
            log.info("File %s reached %s bytes (want >= %s)", path, last, min_bytes)
            return last
        time.sleep(float(poll))
    log.warning("Timed out waiting for %s >= %s bytes (last=%s)", path, min_bytes, last)
    return last


def _timed_cp(client, src, dst, label="cp"):
    """Run ``cp src dst``; return wall-clock seconds (remote date; rm dst first)."""
    announce(f"{label} {src} -> {dst} on {client.hostname}")
    client.exec_command(sudo=True, cmd=f"rm -f {dst}")
    out, _ = client.exec_command(
        sudo=True,
        cmd=(
            f"start=$(date +%s.%N); cp {src} {dst} && end=$(date +%s.%N) && "
            f'awk -v s="$start" -v e="$end" \'BEGIN {{ printf "%.3f\\n", e - s }}\''
        ),
        timeout=600,
    )
    elapsed = float((out or "0").strip().splitlines()[-1])
    log.info("cp elapsed: %.3fs (%s -> %s)", elapsed, src, dst)
    out, _ = client.exec_command(sudo=True, cmd=f"ls -lh {src} {dst}")
    log.info("After copy:\n%s", (out or "").strip())
    return elapsed


def copy_within_export(client, src, dst):
    """Intra-export cp; return elapsed seconds."""
    return _timed_cp(client, src, dst, label="Intra-export cp")


def copy_across_exports(client, src, dst):
    """Cross-export cp; return elapsed seconds."""
    return _timed_cp(client, src, dst, label="Cross-export cp")


def verify_files_match(client, src, dst):
    """Return True if src and dst have identical content (cmp)."""
    announce(f"Verify data integrity: cmp {src} {dst}")
    client.exec_command(sudo=True, cmd=f"cmp -s {src} {dst}", check_ec=False)
    if client.exit_status != 0:
        log.error(
            "File mismatch: %s vs %s (cmp rc=%s)",
            src,
            dst,
            client.exit_status,
        )
        out, _ = client.exec_command(sudo=True, cmd=f"ls -lh {src} {dst}")
        log.info("ls after cmp fail:\n%s", (out or "").strip())
        return False
    log.info("PASS: files match (%s == %s)", src, dst)
    return True


def verify_files_shasum(client, src, dst):
    """Return True if sha256 digests of src and dst match."""
    announce(f"Verify data integrity: sha256sum {src} {dst}")
    out, _ = client.exec_command(sudo=True, cmd=f"sha256sum {src} {dst}", timeout=600)
    text = (out or "").strip()
    log.info("sha256sum:\n%s", text if text else "<empty>")
    hashes = []
    for line in text.splitlines():
        parts = line.split()
        if parts:
            hashes.append(parts[0].lower())
    if len(hashes) < 2:
        log.error("Could not parse two sha256 digests from sha256sum output")
        return False
    if hashes[0] != hashes[1]:
        log.error(
            "SHA mismatch: %s (%s) vs %s (%s)",
            hashes[0],
            src,
            hashes[1],
            dst,
        )
        return False
    log.info("PASS: sha256 match (%s)", hashes[0])
    return True


def _range_is_zeros(client, path, offset_mb, length_mb):
    """Return True if [offset_mb, offset_mb+length_mb) in path is all zeros."""
    # Must use cmp -n: plain ``cmp - /dev/zero`` returns 1 on EOF even when
    # the stream was all zeros (/dev/zero never ends).
    nbytes = int(length_mb) * 1024 * 1024
    client.exec_command(
        sudo=True,
        cmd=(
            f"dd if={path} bs=1M skip={int(offset_mb)} count={int(length_mb)} "
            f"status=none | cmp -s -n {nbytes} - /dev/zero"
        ),
        check_ec=False,
        timeout=600,
    )
    return client.exit_status == 0


def verify_sparse_holes_preserved(client, src, dst, size_mb=128):
    """Return True if post-COPY hole regions behave correctly (XCOPY-06)."""
    size_mb = int(size_mb)
    _, hole_ranges = sparse_file_layout(size_mb)
    announce(
        f"Verify sparse holes preserved via fill probe: {src} vs {dst} "
        f"(holes_mib={hole_ranges})"
    )
    fill_path = "/tmp/xcopy_hole_fill.bin"
    # One MiB known pattern reused for every hole probe write.
    client.exec_command(
        sudo=True,
        cmd=(f"dd if=/dev/urandom of={fill_path} bs=1M count=1 status=none"),
    )
    try:
        for offset_mb, length_mb in hole_ranges:
            log.info(
                "Hole range probe: offset=%sMiB length=%sMiB",
                offset_mb,
                length_mb,
            )
            if not _range_is_zeros(client, src, offset_mb, length_mb):
                log.error(
                    "src hole range not zeros after copy (offset=%sMiB len=%sMiB)",
                    offset_mb,
                    length_mb,
                )
                return False
            if not _range_is_zeros(client, dst, offset_mb, length_mb):
                log.error(
                    "dst hole range not zeros after copy (offset=%sMiB len=%sMiB)",
                    offset_mb,
                    length_mb,
                )
                return False

            # Fill only the first 1 MiB of the hole on dst (enough to detect sharing).
            probe_mb = 1 if length_mb >= 1 else length_mb
            client.exec_command(
                sudo=True,
                cmd=(
                    f"dd if={fill_path} of={dst} bs=1M seek={offset_mb} "
                    f"count={probe_mb} conv=notrunc status=none"
                ),
                timeout=600,
            )

            # Src must be unchanged (still zeros in the hole).
            if not _range_is_zeros(client, src, offset_mb, probe_mb):
                log.error(
                    "src hole changed after writing fill into dst "
                    "(offset=%sMiB) — hole/copy independence failed",
                    offset_mb,
                )
                return False

            # Dst must now contain the fill pattern in the probed window.
            client.exec_command(
                sudo=True,
                cmd=(
                    f"dd if={dst} bs=1M skip={offset_mb} count={probe_mb} "
                    f"status=none | cmp -s - {fill_path}"
                ),
                check_ec=False,
                timeout=600,
            )
            if client.exit_status != 0:
                log.error(
                    "dst hole range did not keep fill pattern (offset=%sMiB)",
                    offset_mb,
                )
                return False
            log.info(
                "PASS hole range offset=%sMiB: zeros after copy; "
                "dst fill ok; src still zeros",
                offset_mb,
            )

        log.info("PASS: sparse hole fill probes succeeded for all hole ranges")
        return True
    finally:
        client.exec_command(sudo=True, cmd=f"rm -f {fill_path}", check_ec=False)


def ensure_xcopy_pcap_tools(node):
    """Ensure ``tcpdump``, ``tshark``, and ``capinfos`` exist on ``node``."""
    host = getattr(node, "hostname", str(node))
    if host in _PCAP_TOOLS_READY:
        log.info("pcap tools already verified on %s", host)
        return

    announce(f"Ensure pcap tools on {host}")
    for binary, packages in PCAP_BINARIES.items():
        out, _ = node.exec_command(
            sudo=True, cmd=f"command -v {binary}", check_ec=False
        )
        path = (out or "").strip()
        if path:
            log.info("%s already present: %s", binary, path)
            continue
        log.info("%s missing on %s; trying packages %s", binary, host, packages)
        installed = False
        for pkg in packages:
            log.info("Installing package %s for %s", pkg, binary)
            ensure_package_installed(node, pkg)
            out, _ = node.exec_command(
                sudo=True, cmd=f"command -v {binary}", check_ec=False
            )
            path = (out or "").strip()
            if path:
                log.info("%s available after installing %s: %s", binary, pkg, path)
                installed = True
                break
        if not installed:
            raise RuntimeError(f"{binary} not found on {host} after trying {packages}")
    _PCAP_TOOLS_READY.add(host)
    log.info("pcap tools ready on %s", host)


def _tcpdump_log_path(pcap_path):
    """Return side-car log path for a pcap capture."""
    return f"/tmp/tcpdump_{pcap_path.rsplit('/', 1)[-1]}.log"


def _tcpdump_pid_path(pcap_path):
    """Return pidfile path for a pcap capture."""
    return f"{pcap_path}.pid"


def start_xcopy_pcap_capture(
    client, server_host, port="2049", pcap_path=DEFAULT_PCAP_PATH
):
    """Start full-snaplen tcpdump on the client."""
    announce(
        f"Start tcpdump capture on {client.hostname}: "
        f"host {server_host} port {port} -> {pcap_path}"
    )
    ensure_xcopy_pcap_tools(client)

    log_path = _tcpdump_log_path(pcap_path)
    pid_path = _tcpdump_pid_path(pcap_path)
    bpf = f"host {server_host} and tcp port {port}"

    # Stop any prior capture tracked by pidfile (do not use pkill -f).
    out, _ = client.exec_command(sudo=True, cmd=f"cat {pid_path}", check_ec=False)
    old_pid = (out or "").strip()
    if old_pid.isdigit():
        client.exec_command(sudo=True, cmd=f"kill {old_pid}", check_ec=False)
    client.exec_command(sudo=True, cmd=f"rm -f {pcap_path}")
    client.exec_command(sudo=True, cmd=f"rm -f {log_path}")
    client.exec_command(sudo=True, cmd=f"rm -f {pid_path}")

    # Background start must keep & and pidfile write in the same shell.
    client.exec_command(
        sudo=True,
        cmd=(
            f"nohup tcpdump -i any -s 0 -U -w {pcap_path} '{bpf}' "
            f">{log_path} 2>&1 </dev/null & echo $! > {pid_path}"
        ),
    )
    time.sleep(1)

    out, _ = client.exec_command(sudo=True, cmd=f"cat {pid_path}")
    pid = (out or "").strip()
    out, _ = client.exec_command(sudo=True, cmd=f"ps -p {pid} -o comm=", check_ec=False)
    if not pid.isdigit() or "tcpdump" not in str(out or ""):
        out, _ = client.exec_command(sudo=True, cmd=f"cat {log_path}", check_ec=False)
        log.info("tcpdump log:\n%s", (out or "").strip() or "<empty>")
        raise RuntimeError(
            f"tcpdump not running after start for {pcap_path} (pid={pid!r})"
        )
    out, _ = client.exec_command(sudo=True, cmd=f"ps -p {pid} -o pid=,comm=,args=")
    log.info("tcpdump running:\n%s", (out or "").strip())
    log.info("tcpdump pid=%s pcap=%s bpf=%r", pid, pcap_path, bpf)
    return pid


def stop_xcopy_pcap_capture(client, pcap_path, pid):
    """Stop tcpdump capture and confirm the pcap is flushed."""
    announce(f"Stop tcpdump capture pid={pid} pcap={pcap_path}")
    log_path = _tcpdump_log_path(pcap_path)
    pid_path = _tcpdump_pid_path(pcap_path)

    live = pid
    if not live:
        out, _ = client.exec_command(sudo=True, cmd=f"cat {pid_path}", check_ec=False)
        live = (out or "").strip()

    if live and str(live).isdigit():
        log.info("Stopping tcpdump pid=%s", live)
        client.exec_command(sudo=True, cmd=f"kill -TERM {live}", check_ec=False)
        for i in range(5):
            time.sleep(0.5)
            out, _ = client.exec_command(
                sudo=True, cmd=f"ps -p {live} -o comm=", check_ec=False
            )
            if not out or "tcpdump" not in str(out):
                log.info("tcpdump stopped after %.1fs", (i + 1) * 0.5)
                break
        else:
            client.exec_command(sudo=True, cmd=f"kill -KILL {live}", check_ec=False)
    else:
        log.info("No live tcpdump pid for %s", pcap_path)

    client.exec_command(sudo=True, cmd=f"rm -f {pid_path}", check_ec=False)
    out, _ = client.exec_command(sudo=True, cmd=f"ls -lh {pcap_path}")
    log.info("pcap after stop:\n%s", (out or "").strip())
    out, _ = client.exec_command(sudo=True, cmd=f"cat {log_path}", check_ec=False)
    log.info("tcpdump log:\n%s", (out or "").strip() or "<empty>")


def _capinfos_size_to_bytes(value, unit):
    """Convert capinfos size + unit (e.g. 10, 'kB') to integer bytes."""
    scale = _CAPINFOS_UNITS.get((unit or "").lower())
    if scale is None:
        return None
    return int(float(str(value).replace(",", "")) * scale)


def _parse_capinfos(text):
    """Return dict with data_size, file_size, packets from capinfos output."""

    def _size(regex):
        m = regex.search(text or "")
        if not m:
            return None
        return _capinfos_size_to_bytes(m.group(1), m.group(2))

    def _packets():
        m = RE_CAPINFOS_PACKETS.search(text or "")
        if not m:
            return None
        return int(m.group(1).replace(",", ""))

    return {
        "data_size": _size(RE_CAPINFOS_DATA_SIZE),
        "file_size": _size(RE_CAPINFOS_FILE_SIZE),
        "packets": _packets(),
    }


def _parse_opcode_histogram(text):
    """Return {opcode_label: count} from ``uniq -c | sort -nr`` output."""
    counts = {}
    for line in (text or "").splitlines():
        m = RE_OPCODE_HIST.match(line.strip())
        if not m:
            continue
        counts[m.group(2)] = int(m.group(1))
    return counts


def _opcode_count(opcodes, *labels):
    """Sum counts for numeric and/or named opcode labels (case-insensitive names)."""
    want = {str(x).upper() for x in labels}
    total = 0
    for key, count in (opcodes or {}).items():
        if str(key).upper() in want:
            total += int(count)
    return total


def pcap_on_max_packets(file_size_bytes, chunk_size_bytes=None, short_copy=False):
    """ON packet ceiling from expected COPY count (floor at PCAP_ON_MAX_PACKETS)."""
    size = int(file_size_bytes or 0)
    chunk = int(chunk_size_bytes or 0)
    n_copies = (
        max(1, (size + chunk - 1) // chunk)
        if short_copy and size > 0 and chunk > 0
        else 1
    )
    ceiling = max(
        PCAP_ON_MAX_PACKETS,
        PCAP_ON_BASE_PACKETS + PCAP_ON_PACKETS_PER_COPY * n_copies,
    )
    log.info(
        "pcap ON budget: short_copy=%s copies=%s ceiling=%s",
        short_copy,
        n_copies,
        ceiling,
    )
    return ceiling


def _pcap_fail(ok, message):
    if ok:
        return True
    log.error(message)
    return False


def verify_xcopy_pcap(
    client,
    pcap_path,
    file_size_bytes,
    offload_enabled=True,
    chunk_size_bytes=None,
    short_copy=False,
    on_max_packets=None,
    on_max_data_bytes=PCAP_ON_MAX_DATA_BYTES,
    on_max_rw_ops=PCAP_ON_MAX_RW_OPS,
    off_min_packets=PCAP_OFF_MIN_PACKETS,
    off_data_ratio_min=PCAP_OFF_DATA_RATIO_MIN,
    off_data_ratio_max=PCAP_OFF_DATA_RATIO_MAX,
    off_min_rw_ops=PCAP_OFF_MIN_RW_OPS,
):
    """Validate Copy_Offload ON/OFF from client pcap (capinfos + tshark opcodes)."""
    mode = "ON" if offload_enabled else "OFF"
    if on_max_packets is None:
        on_max_packets = (
            pcap_on_max_packets(
                file_size_bytes,
                chunk_size_bytes=chunk_size_bytes,
                short_copy=short_copy,
            )
            if offload_enabled
            else PCAP_ON_MAX_PACKETS
        )
    announce(f"Validate xcopy pcap (Copy_Offload={mode}) ({pcap_path})")
    ensure_xcopy_pcap_tools(client)

    out, _ = client.exec_command(sudo=True, cmd=f"capinfos {pcap_path}")
    caps = _parse_capinfos((out or "").strip())
    opcode_cmd = (
        f"tshark -r {pcap_path} -Y nfs -T fields -e nfs.opcode "
        f"| tr ',' '\\n' | sort | uniq -c | sort -nr"
    )
    out, _ = client.exec_command(sudo=True, cmd=opcode_cmd, timeout=600)
    opcodes = _parse_opcode_histogram((out or "").strip())
    log.info("capinfos=%s opcodes=%s", caps, opcodes)

    packets = caps.get("packets")
    data_size = caps.get("data_size")
    if packets is None or packets == 0:
        log.error("pcap has no packets")
        return False
    if data_size is None:
        log.error("Could not parse capinfos Data size")
        return False

    copy_n = _opcode_count(opcodes, NFS_OPCODE_COPY, "COPY")
    rw_n = _opcode_count(opcodes, NFS_OPCODE_READ, "READ") + _opcode_count(
        opcodes, NFS_OPCODE_WRITE, "WRITE"
    )

    if offload_enabled:
        checks = [
            (packets <= on_max_packets, f"ON packets={packets} > max {on_max_packets}"),
            (
                data_size <= on_max_data_bytes,
                f"ON data_size={data_size} > max {on_max_data_bytes}",
            ),
            (copy_n >= 1, f"ON missing COPY(60); opcodes={opcodes}"),
            (rw_n <= on_max_rw_ops, f"ON READ+WRITE={rw_n} > {on_max_rw_ops}"),
        ]
    else:
        ratio = float(data_size) / float(file_size_bytes) if file_size_bytes else 0.0
        checks = [
            (
                packets >= off_min_packets,
                f"OFF packets={packets} < min {off_min_packets}",
            ),
            (
                off_data_ratio_min <= ratio <= off_data_ratio_max,
                f"OFF data/file ratio {ratio:.3f} not in "
                f"{off_data_ratio_min}..{off_data_ratio_max}",
            ),
            (
                rw_n >= off_min_rw_ops,
                f"OFF READ+WRITE={rw_n} < flood min {off_min_rw_ops}",
            ),
        ]
    for ok, msg in checks:
        if not _pcap_fail(ok, msg):
            return False

    announce(
        f"PASS pcap Copy_Offload={mode}: packets={packets} "
        f"data_size={data_size} COPY={copy_n} READ+WRITE={rw_n}"
    )
    return True


def collect_nfs_logs(
    nfs_nodes, nfs_name=None, since=None, client=None, patterns=XCOPY_GREP_PATTERNS
):
    """Grep podman NFS logs for xcopy strings on the given host(s)."""
    if not isinstance(nfs_nodes, (list, tuple)):
        nfs_nodes = [nfs_nodes]
    ere = "|".join(f"({p})" for p in patterns)
    parts = []
    for node in nfs_nodes:
        text = podman_nfs_logs(
            node, nfs_name, since=since, client=client, grep=ere
        ).rstrip()
        if not text and not get_nfs_container_id(
            node, nfs_name=nfs_name, client=client
        ):
            log.error("No NFS container on %s", node.hostname)
            continue
        log.info(
            "xcopy log hits on %s: %s line(s)\n%s",
            node.hostname,
            len(text.splitlines()) if text else 0,
            text or "<none>",
        )
        if text:
            parts.append(text)
    combined = "\n".join(parts)
    log.info(
        "Combined xcopy logs: %s bytes from %s",
        len(combined),
        [n.hostname for n in nfs_nodes],
    )
    return combined


def _expected_chunks(total, chunk_size):
    """Return list of (chunk_idx, src_off, length, progress) for a full copy."""
    chunks = []
    remaining = total
    src_off = 0
    idx = 0
    while remaining > 0:
        length = min(remaining, chunk_size)
        chunks.append((idx, src_off, length, src_off))
        src_off += length
        remaining -= length
        idx += 1
    log.info(
        "Chunk plan: total=%s chunk_size=%s -> %s chunk(s)",
        total,
        chunk_size,
        len(chunks),
    )
    return chunks


def _log_matched_lines(label, log_text, pattern, limit=5):
    """Log up to ``limit`` matching evidence lines from Ganesha output."""
    hits = [ln for ln in (log_text or "").splitlines() if re.search(pattern, ln, re.I)]
    log.info(
        "%s: %s match(es); showing up to %s:\n%s",
        label,
        len(hits),
        limit,
        "\n".join(hits[:limit]) if hits else "<none>",
    )
    return hits


def _require_count(label, got, expected):
    if got != expected:
        log.error("%s: got %s, expected %s", label, got, expected)
        return False
    return True


def _require_mdcache_sizes(md_hits, expected, label="mdcache"):
    if not _require_count(f"{label} count", len(md_hits), len(expected)):
        return False
    for exp_idx, _, exp_len, _ in expected:
        got = int(md_hits[exp_idx].group(1))
        if got != exp_len:
            log.error("%s #%s copied=%s, expected %s", label, exp_idx, got, exp_len)
            return False
    return True


def _mode_and_chunk_ok(detected, logged_chunk, mode, chunk_size_bytes):
    if mode != "any" and detected.lower() != mode.lower():
        log.error("Expected copy mode %s but logs show %s", mode, detected)
        return False
    if logged_chunk != int(chunk_size_bytes):
        log.error(
            "Logged chunk_size=%s != Copy_Offload_Chunk_Size=%s",
            logged_chunk,
            chunk_size_bytes,
        )
        return False
    return True


def _offload_state_ok(log_text, required):
    hits = _log_matched_lines(
        "create_copy_offload_state", log_text, RE_OFFLOAD_STATE.pattern
    )
    if hits:
        return True
    if required:
        log.error("ASYNC path missing create_copy_offload_state")
        return False
    log.warning("Short_Copy ASYNC: create_copy_offload_state missing (continuing)")
    return True


def validate_xcopy_logs(
    log_text,
    file_size_bytes,
    chunk_size_bytes,
    mode="any",
    short_copy=False,
):
    """Validate xcopy milestones + chunk math (Short_Copy ON/OFF packaging)."""
    announce(
        "Validate xcopy logs "
        f"(Short_Copy={'ON' if short_copy else 'OFF'}, mode={mode})"
    )
    if not log_text or not log_text.strip():
        log.error("Empty Ganesha logs; cannot validate xcopy")
        return False

    starts = list(RE_WORKER_START.finditer(log_text))
    if not starts:
        log.error("No COPY SYNC|ASYNC worker START in logs")
        _log_matched_lines("COPY/xcopy hints", log_text, r"COPY |xcopy|CB_OFFLOAD", 20)
        return False

    expected = _expected_chunks(int(file_size_bytes), int(chunk_size_bytes))
    expected_n = len(expected)

    if short_copy:
        detected = starts[0].group(1).upper()
        logged_chunk = int(starts[0].group(5))
        if not _mode_and_chunk_ok(detected, logged_chunk, mode, chunk_size_bytes):
            return False
        if not _require_count("Short_Copy START", len(starts), expected_n):
            return False
        if detected == "ASYNC" and not _offload_state_ok(log_text, required=False):
            return False

        cb = list(RE_CB_OFFLOAD.finditer(log_text))
        if not _require_count("Short_Copy CB_OFFLOAD", len(cb), expected_n):
            return False
        cb_sizes = []
        for i, hit in enumerate(cb):
            nbytes = int(hit.group(1))
            cb_sizes.append(nbytes)
            if nbytes != expected[i][2]:
                log.error(
                    "Short_Copy CB #%s bytes=%s, expected %s",
                    i,
                    nbytes,
                    expected[i][2],
                )
                return False
        if sum(cb_sizes) != int(file_size_bytes):
            log.error(
                "Short_Copy sum(CB)=%s != file_size=%s",
                sum(cb_sizes),
                file_size_bytes,
            )
            return False
        if not _require_mdcache_sizes(
            list(RE_MDCACHE_OK.finditer(log_text)), expected, "Short_Copy mdcache"
        ):
            return False
        announce(
            f"PASS Short_Copy logs: mode={detected} chunks={expected_n} "
            f"chunk_size={logged_chunk}"
        )
        return True

    # Short_Copy=OFF: prefer START whose total == file_size.
    start = None
    for cand in reversed(starts):
        if int(cand.group(2)) == int(file_size_bytes):
            start = cand
            break
    if start is None:
        start = starts[-1]
        log.warning(
            "No START with total==file_size; using last (total=%s file=%s)",
            start.group(2),
            file_size_bytes,
        )
    detected = start.group(1).upper()
    total = int(start.group(2))
    src_off0 = int(start.group(3))
    dst_off0 = int(start.group(4))
    logged_chunk = int(start.group(5))
    if not _mode_and_chunk_ok(detected, logged_chunk, mode, chunk_size_bytes):
        return False
    if total != int(file_size_bytes):
        log.error("Worker total=%s != file_size=%s", total, file_size_bytes)
        return False
    if src_off0 != 0 or dst_off0 != 0:
        log.error("Expected src_off=dst_off=0 at START, got %s/%s", src_off0, dst_off0)
        return False

    expected = _expected_chunks(total, logged_chunk)
    expected_n = len(expected)

    if detected == "ASYNC":
        if not _offload_state_ok(log_text, required=True):
            return False
        cb = list(RE_CB_OFFLOAD.finditer(log_text))
        if not cb:
            log.error("ASYNC path missing CB_OFFLOAD: sending OK")
            return False
        if not any(int(m.group(1)) == total for m in cb):
            log.error(
                "Short_Copy=OFF: no CB_OFFLOAD with bytes==file_size (%s); saw %s",
                total,
                [int(m.group(1)) for m in cb],
            )
            return False
    elif RE_CB_OFFLOAD.search(log_text):
        log.warning("SYNC path unexpectedly has CB_OFFLOAD (continuing)")

    chunk_re = re.compile(
        rf"COPY {detected} chunk #(\d+):\s*src_off=(\d+)\s+len=(\d+)\s+"
        rf"progress=(\d+)/(\d+)",
        re.I,
    )
    after = log_text[start.start() :]
    chunk_hits = list(chunk_re.finditer(after))
    if not _require_count("chunk lines", len(chunk_hits), expected_n):
        return False
    for exp_idx, exp_off, exp_len, exp_prog in expected:
        hit = chunk_hits[exp_idx]
        got = tuple(int(hit.group(i)) for i in range(1, 6))
        want = (exp_idx, exp_off, exp_len, exp_prog, total)
        if got != want:
            log.error("Chunk #%s mismatch: got %s expected %s", exp_idx, got, want)
            return False
    if not _require_mdcache_sizes(list(RE_MDCACHE_OK.finditer(after)), expected):
        return False

    log.info(
        "xcopy validation summary: mode=%s total=%s chunk_size=%s chunks=%s "
        "short_copy=False",
        detected,
        total,
        logged_chunk,
        expected_n,
    )
    announce(
        f"PASS xcopy logs: mode={detected} total={total} "
        f"chunk_size={logged_chunk} chunks={expected_n}"
    )
    return True


def verify_copy_offload_logs(
    nfs_nodes,
    nfs_name,
    file_size_bytes,
    chunk_size_bytes,
    since=None,
    mode="any",
    short_copy=False,
    client=None,
):
    """Collect podman NFS logs and validate xcopy milestones + chunk math."""
    announce(f"Collect podman NFS logs for {nfs_name} (since={since})")
    text = collect_nfs_logs(nfs_nodes, nfs_name=nfs_name, since=since, client=client)
    ok = validate_xcopy_logs(
        text,
        file_size_bytes=file_size_bytes,
        chunk_size_bytes=chunk_size_bytes,
        mode=mode,
        short_copy=short_copy,
    )
    log.info(
        "verify_copy_offload_logs: %s (%s bytes)", "PASS" if ok else "FAIL", len(text)
    )
    return ok
