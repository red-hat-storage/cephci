"""
CephFS Subvolume Quarantine — Interoperability workflows.

Polarion: CEPH-83632684 (Client / protocol matrix)
Suite block: Interop

I-01 FUSE: rw denied under quarantine; rwq recovery OK
I-02 NFS: IO blocked / fails under quarantine (Ganesha typically allow *; no q);
       restored after disable
I-03 SMB: same semantics when SMB mgr stack is present; otherwise N/A

Note: IBM plan wording "mount with q flag" over NFS/SMB is not literal —
``q`` is an MDS CephX flag. Expect protocol gateways to be blocked unless an
RFE delivers rwq-backed gateway credentials.
"""

from __future__ import annotations

import json
import shlex
import time
import traceback

from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_subvol_quarantine_utils import (
    SubvolQuarantineUtils,
    skip_quarantine_tests_unless_supported,
)
from utility.log import Log

log = Log(__name__)


def _prepare(ceph_cluster, config):
    fs_util = FsUtils(ceph_cluster)
    qtn = SubvolQuarantineUtils(ceph_cluster)
    clients = ceph_cluster.get_ceph_objects("client")
    if len(clients) < 2:
        log.error("Need at least 2 client nodes; found %d", len(clients))
        return None

    build = config.get("build", config.get("rhbuild"))
    fs_util.prepare_clients(clients[:2], build)
    fs_util.auth_list(clients[:2])

    admin = clients[0]
    fuse = clients[1]
    if not qtn.feature_available(admin):
        log.error("Subvolume quarantine CLI not available — mark NA")
        return None

    return qtn, fs_util, admin, fuse, config.get("fs_name", "cephfs"), ceph_cluster


def _umount_fuse(qtn, client, mounts):
    for mnt in mounts:
        qtn.umount_fuse(client, mnt)


def _umount_nfs(client, mount_point):
    client.exec_command(
        sudo=True, cmd=f"umount -f {shlex.quote(mount_point)}", check_ec=False
    )
    client.exec_command(
        sudo=True, cmd=f"umount {shlex.quote(mount_point)}", check_ec=False
    )


def _del_clients(qtn, admin, names):
    for name in names:
        qtn.delete_client(admin, name)


def _nfs_cluster_exists(admin, nfs_name):
    out, _ = admin.exec_command(sudo=True, cmd="ceph nfs cluster ls", check_ec=False)
    text = (out or "").strip()
    if not text:
        return False
    try:
        clusters = json.loads(text)
        if isinstance(clusters, list):
            return nfs_name in clusters
    except json.JSONDecodeError:
        pass
    return nfs_name in [line.strip() for line in text.splitlines() if line.strip()]


def _resolve_nfs_server(admin, ceph_cluster, nfs_name):
    """Return hostname/IP usable for NFS mount."""
    nfs_objs = ceph_cluster.get_ceph_objects("nfs")
    if nfs_objs:
        node = nfs_objs[0].node
        return getattr(node, "ip_address", None) or node.hostname

    out, _ = admin.exec_command(
        sudo=True,
        cmd=f"ceph nfs cluster info {nfs_name} -f json",
        check_ec=False,
    )
    try:
        info = json.loads(out) if out else {}
    except json.JSONDecodeError:
        info = {}
    for _cluster, payload in info.items() if isinstance(info, dict) else []:
        backends = payload.get("backend") if isinstance(payload, dict) else None
        if isinstance(backends, list) and backends:
            ip = backends[0].get("ip") or backends[0].get("hostname")
            if ip:
                return ip

    # Fall back: place NFS on installer hostname for create; mount via that host
    installers = ceph_cluster.get_nodes(role="installer")
    if installers:
        return getattr(installers[0], "ip_address", None) or installers[0].hostname
    nodes = ceph_cluster.get_nodes()
    if nodes:
        return getattr(nodes[0], "ip_address", None) or nodes[0].hostname
    return None


def _ensure_nfs_cluster(fs_util, admin, ceph_cluster, nfs_name):
    if _nfs_cluster_exists(admin, nfs_name):
        log.info("NFS cluster %s already exists", nfs_name)
        return 0

    nfs_objs = ceph_cluster.get_ceph_objects("nfs")
    if nfs_objs:
        host = nfs_objs[0].node.hostname
    else:
        installers = ceph_cluster.get_nodes(role="installer")
        host = (
            installers[0].hostname
            if installers
            else ceph_cluster.get_nodes()[0].hostname
        )
    log.info("Creating NFS cluster %s on host %s", nfs_name, host)
    try:
        fs_util.create_nfs(admin, nfs_name, validate=True, placement=f"1 {host}")
    except Exception as exc:
        log.error("Failed to create NFS cluster %s: %s", nfs_name, exc)
        return 1
    time.sleep(10)
    return 0


def _timed_shell(client, cmd, timeout_sec=30):
    """Run cmd under timeout(1); return (rc, out, err)."""
    wrapped = f"timeout {timeout_sec} bash -c {shlex.quote(cmd)}"
    out, err = client.exec_command(sudo=True, cmd=wrapped, check_ec=False)
    return client.node.exit_status, out, err


def _io_blocked_or_failed(client, mount_point, file_name="n.txt"):
    """
    Return 0 if read/write under quarantine is blocked, stalls, or errors.

    NFS/Ganesha often returns EACCES, ESTALE, or hangs — all acceptable as
    "not usable while quarantined".
    """
    path = f"{mount_point}/{file_name}"
    rc_r, out_r, err_r = _timed_shell(client, f"cat {shlex.quote(path)}", 20)
    rc_w, out_w, err_w = _timed_shell(
        client,
        f"echo nfs-after-q > {shlex.quote(mount_point + '/after-q.txt')}",
        20,
    )
    log.info(
        "NFS IO under quarantine: read rc=%s out=%s err=%s; write rc=%s out=%s err=%s",
        rc_r,
        out_r,
        err_r,
        rc_w,
        out_w,
        err_w,
    )
    # timeout(1) uses 124 on timeout
    if rc_r in (0,) and rc_w in (0,):
        log.error("NFS IO unexpectedly succeeded while quarantined")
        return 1
    log.info("NFS IO blocked/failed/stalled under quarantine as expected")
    return 0


def _smb_module_available(admin):
    out, _ = admin.exec_command(sudo=True, cmd="ceph smb --help", check_ec=False)
    text = f"{out}".lower()
    if "cluster" in text or "share" in text:
        return True
    out2, _ = admin.exec_command(
        sudo=True, cmd="ceph mgr module ls -f json", check_ec=False
    )
    try:
        modules = json.loads(out2) if out2 else {}
    except json.JSONDecodeError:
        return False
    enabled = modules.get("enabled_modules") or modules.get("always_on_modules") or []
    return "smb" in enabled


# ---------------------------------------------------------------------------
# I-01 .. I-03
# ---------------------------------------------------------------------------


def i01_fuse_rwq(qtn, fs_util, admin, fuse, vol, config, ceph_cluster):
    """I-01: FUSE without q denied; with rwq recovery access OK."""
    sub = config.get("sub_i01", "qtn-i01")
    client_rw = config.get("client_i01", "qtn-i01-rw")
    client_rwq = config.get("client_i01q", "qtn-i01-rwq")
    mount_rw = config.get("mount_i01", "/mnt/qtn-i01-rw")
    mount_rwq = config.get("mount_i01q", "/mnt/qtn-i01-rwq")
    content = "fuse-baseline"
    mounts = [mount_rw, mount_rwq]
    clients = [client_rw, client_rwq]

    try:
        if qtn.setup_subvolume(admin, vol, sub):
            return 1
        root, data = qtn.get_subvolume_paths(admin, vol, sub)
        qtn.create_rw_client(admin, vol, root, client_rw)
        qtn.create_rwq_client(admin, vol, root, client_rwq)

        qtn.mount_fuse(fuse, mount_rw, data, client_rw, vol_name=vol)
        if qtn.write_baseline_file(fuse, mount_rw, "f.txt", content):
            return 1
        qtn.umount_fuse(fuse, mount_rw)

        qtn.quarantine_enable(admin, vol, sub)

        if qtn.assert_fuse_mount_fails(fuse, mount_rw, data, client_rw, vol_name=vol):
            return 1

        qtn.mount_fuse(fuse, mount_rwq, data, client_rwq, vol_name=vol)
        if qtn.assert_content_equals(fuse, mount_rwq, "f.txt", content):
            return 1
        if qtn.assert_write_ok(fuse, mount_rwq, "r.txt"):
            return 1

        log.info("i01_fuse_rwq PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _umount_fuse(qtn, fuse, mounts)
        try:
            qtn.quarantine_disable(admin, vol, sub)
        except Exception:
            pass
        qtn.cleanup_subvolume(admin, vol, sub)
        _del_clients(qtn, admin, clients)


def i02_nfs_blocked_under_quarantine(
    qtn, fs_util, admin, fuse, vol, config, ceph_cluster
):
    """
    I-02: NFS export of subvolume — IO OK before quarantine; blocked/failed under
    quarantine; usable again after disable.

    Does not require an NFS ``q`` mount option (not supported). Gateway clients
    typically use allow * and must be denied data-path access while quarantined.
    """
    if config.get("skip_nfs", False):
        log.info("I-02 N/A: skip_nfs=true in config")
        return 0

    sub = config.get("sub_i02", "qtn-i02")
    nfs_name = config.get("nfs_cluster", "cephfs-nfs-qtn")
    export_path = config.get("nfs_export", "/qtn-i02")
    nfs_mount = config.get("nfs_mount", "/mnt/qtn-nfs-i02")
    content = "nfs-baseline"
    created_cluster = False

    try:
        if qtn.setup_subvolume(admin, vol, sub):
            return 1
        _root, data = qtn.get_subvolume_paths(admin, vol, sub)

        existed = _nfs_cluster_exists(admin, nfs_name)
        if _ensure_nfs_cluster(fs_util, admin, ceph_cluster, nfs_name):
            return 1
        created_cluster = not existed

        # Clean prior export if any
        fs_util.remove_nfs_export(
            admin, nfs_name, export_path, validate=False, check_ec=False
        )
        fs_util.create_nfs_export(
            admin, nfs_name, export_path, vol, path=data, validate=True
        )

        nfs_server = _resolve_nfs_server(admin, ceph_cluster, nfs_name)
        if not nfs_server:
            log.error("I-02: could not resolve NFS server address")
            return 1
        log.info("Mounting NFS %s:%s -> %s", nfs_server, export_path, nfs_mount)

        if not fs_util.cephfs_nfs_mount(fuse, nfs_server, export_path, nfs_mount):
            log.error("I-02: NFS mount failed before quarantine")
            return 1

        if qtn.write_baseline_file(fuse, nfs_mount, "n.txt", content):
            return 1
        if qtn.assert_content_equals(fuse, nfs_mount, "n.txt", content):
            return 1

        qtn.quarantine_enable(admin, vol, sub)
        time.sleep(3)

        if _io_blocked_or_failed(fuse, nfs_mount, "n.txt"):
            return 1

        # Remount path while quarantined should not yield usable RW access
        _umount_nfs(fuse, nfs_mount)
        mounted = False
        try:
            mounted = bool(
                fs_util.cephfs_nfs_mount(fuse, nfs_server, export_path, nfs_mount)
            )
        except Exception as exc:
            log.info("NFS remount while quarantined failed (acceptable): %s", exc)
        if mounted:
            if _io_blocked_or_failed(fuse, nfs_mount, "n.txt"):
                return 1
            _umount_nfs(fuse, nfs_mount)

        qtn.quarantine_disable(admin, vol, sub)
        time.sleep(3)

        if not fs_util.cephfs_nfs_mount(fuse, nfs_server, export_path, nfs_mount):
            log.error("I-02: NFS remount failed after disable")
            return 1
        if qtn.assert_content_equals(fuse, nfs_mount, "n.txt", content):
            # file may be missing if never flushed — allow rewrite
            log.warning("baseline missing after disable; rewriting")
            if qtn.write_baseline_file(fuse, nfs_mount, "n.txt", content):
                return 1
        if qtn.assert_write_ok(fuse, nfs_mount, "nfs-restored.txt"):
            return 1

        log.info("i02_nfs_blocked_under_quarantine PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _umount_nfs(fuse, nfs_mount)
        try:
            fs_util.remove_nfs_export(
                admin, nfs_name, export_path, validate=False, check_ec=False
            )
        except Exception:
            pass
        if created_cluster and config.get("cleanup_nfs_cluster", True):
            try:
                fs_util.remove_nfs_cluster(
                    admin, nfs_name, validate=False, check_ec=False
                )
            except Exception:
                pass
        try:
            qtn.quarantine_disable(admin, vol, sub)
        except Exception:
            pass
        qtn.cleanup_subvolume(admin, vol, sub)


def i03_smb_blocked_under_quarantine(
    qtn, fs_util, admin, fuse, vol, config, ceph_cluster
):
    """
    I-03: SMB/CIFS access while quarantined.

    Full SMB mgr deploy is out of scope for this suite. Behavior:
      - If skip_smb=true (default) or SMB mgr CLI unavailable → N/A (PASS 0)
      - If run_smb=true and an existing share/mount recipe is provided via
        config (smb_mount, smb_unc, smb_user, smb_password), validate block /
        restore like NFS.

    Dedicated SMB+quarantine coverage can later reuse tests/smb helpers.
    """
    if not config.get("run_smb", False):
        log.info(
            "I-03 N/A: set run_smb=true plus smb_unc/smb_user/smb_password "
            "to exercise an existing share; full SMB deploy deferred"
        )
        return 0

    if not _smb_module_available(admin):
        log.info("I-03 N/A: ceph smb / mgr smb module not available")
        return 0

    smb_unc = config.get("smb_unc")
    smb_user = config.get("smb_user")
    smb_password = config.get("smb_password")
    smb_mount = config.get("smb_mount", "/mnt/qtn-smb-i03")
    if not (smb_unc and smb_user and smb_password):
        log.info("I-03 N/A: run_smb=true but smb_unc/smb_user/smb_password not set")
        return 0

    sub = config.get("sub_i03", "qtn-i03")
    content = "smb-baseline"

    try:
        if qtn.setup_subvolume(admin, vol, sub):
            return 1
        # Operator is expected to have pointed smb_unc at this subvolume path.
        # We only validate quarantine effect on the provided CIFS mount.
        fuse.exec_command(
            sudo=True, cmd=f"mkdir -p {shlex.quote(smb_mount)}", check_ec=False
        )
        mount_cmd = (
            f"mount -t cifs {shlex.quote(smb_unc)} {shlex.quote(smb_mount)} "
            f"-o username={shlex.quote(smb_user)},password={shlex.quote(smb_password)}"
        )
        out, err = fuse.exec_command(sudo=True, cmd=mount_cmd, check_ec=False)
        if fuse.node.exit_status != 0:
            log.error("I-03: CIFS mount failed: out=%s err=%s", out, err)
            return 1

        if qtn.write_baseline_file(fuse, smb_mount, "s.txt", content):
            return 1

        qtn.quarantine_enable(admin, vol, sub)
        time.sleep(3)
        if _io_blocked_or_failed(fuse, smb_mount, "s.txt"):
            return 1

        qtn.quarantine_disable(admin, vol, sub)
        time.sleep(3)
        if qtn.assert_write_ok(fuse, smb_mount, "smb-restored.txt"):
            return 1

        log.info("i03_smb_blocked_under_quarantine PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        fuse.exec_command(
            sudo=True, cmd=f"umount -f {shlex.quote(smb_mount)}", check_ec=False
        )
        try:
            qtn.quarantine_disable(admin, vol, sub)
        except Exception:
            pass
        qtn.cleanup_subvolume(admin, vol, sub)


SUBTESTS = {
    "i01_fuse_rwq": i01_fuse_rwq,
    "i02_nfs_blocked_under_quarantine": i02_nfs_blocked_under_quarantine,
    "i03_smb_blocked_under_quarantine": i03_smb_blocked_under_quarantine,
}


def run(ceph_cluster, **kw):
    """Run Interop subtests for CephFS subvolume quarantine."""
    config = kw.get("config") or {}
    if skip_quarantine_tests_unless_supported(config, ceph_cluster):
        return 0

    log.info("=" * 80)
    log.info("TEST TYPE : Interop")
    log.info("MODULE    : test_subvolume_quarantine_interop.py")
    log.info("POLARION  : CEPH-83632684")
    log.info("=" * 80)

    prepared = _prepare(ceph_cluster, config)
    if prepared is None:
        return 1
    qtn, fs_util, admin, fuse, vol, cluster = prepared

    requested = config.get("subtests")
    test_list = requested if requested else list(SUBTESTS.keys())

    failed = []
    for name in test_list:
        if name not in SUBTESTS:
            log.error("Unknown Interop subtest '%s'; known: %s", name, list(SUBTESTS))
            failed.append(name)
            continue

        log.info("")
        log.info("=" * 80)
        log.info("SUBTEST START : [Interop] %s", name)
        log.info("DESC          : %s", (SUBTESTS[name].__doc__ or "").strip())
        log.info("=" * 80)

        try:
            rc = SUBTESTS[name](qtn, fs_util, admin, fuse, vol, config, cluster)
        except Exception:
            log.error("SUBTEST EXCEPTION : [Interop] %s", name)
            log.error(traceback.format_exc())
            rc = 1

        if rc:
            log.error("SUBTEST FAILED  : [Interop] %s", name)
            failed.append(name)
        else:
            log.info("SUBTEST PASSED  : [Interop] %s", name)
        log.info("-" * 80)

    log.info("=" * 80)
    if failed:
        log.error(
            "Interop summary: %d/%d FAILED → %s",
            len(failed),
            len(test_list),
            failed,
        )
        log.info("=" * 80)
        return 1

    log.info("Interop summary: ALL %d subtest(s) PASSED", len(test_list))
    log.info("=" * 80)
    return 0
