"""
CephFS Subvolume Quarantine — Functional workflows.

Polarion: CEPH-83632678
Suite block: Functional

Subtests F-01 .. F-18 map to the Functional test plan.
No workarounds for regressions against the MGR contract:
  - getpath blocked while quarantined
  - ls enabled (names only, no quarantine indication)
  - info: non-quarantined full payload with quarantine="disabled";
         quarantined minimal payload with quarantine="enabled"
F-18 (encrypted) is N/A until GKLM/fscrypt lab is available.
"""

import json
import re
import shlex
import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_subvol_quarantine_utils import SubvolQuarantineUtils
from utility.log import Log

log = Log(__name__)

BASELINE_FILE = "testfile.txt"


def _cfg(config, key, default):
    return config.get(key, default)


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

    return qtn, fs_util, admin, fuse, config.get("fs_name", "cephfs")


def _umount_all(qtn, client, mounts):
    for mnt in mounts:
        qtn.umount_fuse(client, mnt)


def _del_clients(qtn, admin, names):
    for name in names:
        qtn.delete_client(admin, name)


def _rm_sv(qtn, admin, vol, sub, group=None):
    qtn.cleanup_subvolume(admin, vol, sub, group_name=group)


def _rm_group(fs_util, admin, vol, group):
    try:
        fs_util.remove_subvolumegroup(
            admin, vol, group, force=True, validate=False, check_ec=False
        )
    except Exception as exc:
        log.warning("remove_subvolumegroup %s: %s", group, exc)


def _mount_root(fuse_client, mount_point, vol_name):
    """Mount FS root with default/admin client keyring on the fuse node."""
    fuse_client.exec_command(sudo=True, cmd=f"mkdir -p {mount_point}")
    rc = fuse_client.exec_command(
        sudo=True,
        cmd=f"ceph-fuse {mount_point} --client_fs {vol_name}",
        long_running=True,
    )
    if rc:
        raise CommandFailed(f"root fuse mount failed: {rc}")


def _snap_schedule_status_entries(admin, path):
    out, err = admin.exec_command(
        sudo=True, cmd=f"ceph fs snap-schedule status {path}", check_ec=False
    )
    if admin.node.exit_status:
        log.error("snap-schedule status failed: %s %s", out, err)
        return []
    entries = []
    for chunk in re.split(r"\n===\n", out.strip()):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            entries.append(json.loads(chunk))
        except json.JSONDecodeError:
            log.warning("skip non-JSON schedule status chunk: %s", chunk[:120])
    return entries


def _max_created_count(entries):
    if not entries:
        return 0
    return max(int(e.get("created_count") or 0) for e in entries)


# ---------------------------------------------------------------------------
# F-01 .. F-18
# ---------------------------------------------------------------------------


def f01_group_quarantine(qtn, fs_util, admin, fuse, vol, config):
    """F-01 Quarantine under subvolume group — enable + info state."""
    group = _cfg(config, "group", "qtn-group")
    sub = _cfg(config, "sub_a", "qtn-sv-a")
    try:
        fs_util.create_subvolumegroup(admin, vol, group)
        if qtn.setup_subvolume(admin, vol, sub, group_name=group):
            return 1
        result = qtn.quarantine_enable(admin, vol, sub, group_name=group)
        log.info("enable result: %s", result)
        if qtn.assert_info_quarantined(admin, vol, sub, group_name=group):
            return 1
        log.info("f01_group_quarantine PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _rm_sv(qtn, admin, vol, sub, group)
        _rm_group(fs_util, admin, vol, group)


def f02_clear_quarantine(qtn, fs_util, admin, fuse, vol, config):
    """F-02 Clear quarantine flag under group — disable + info cleared + mount OK."""
    group = _cfg(config, "group", "qtn-group")
    sub = _cfg(config, "sub_a", "qtn-sv-a")
    client = "qtn-f02-normal"
    mount = "/mnt/qtn-f02"
    try:
        fs_util.create_subvolumegroup(admin, vol, group)
        if qtn.setup_subvolume(admin, vol, sub, group_name=group):
            return 1
        root, data = qtn.get_subvolume_paths(admin, vol, sub, group_name=group)
        qtn.quarantine_enable(admin, vol, sub, group_name=group)

        result = qtn.quarantine_disable(admin, vol, sub, group_name=group)
        log.info("disable result: %s", result)
        if qtn.assert_info_not_quarantined(admin, vol, sub, group_name=group):
            return 1

        qtn.create_rw_client(admin, vol, root, client)
        qtn.mount_fuse(fuse, mount, data, client, vol)
        if qtn.assert_write_ok(fuse, mount, "after-disable.txt"):
            return 1

        log.info("f02_clear_quarantine PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _umount_all(qtn, fuse, [mount])
        _del_clients(qtn, admin, [client])
        _rm_sv(qtn, admin, vol, sub, group)
        _rm_group(fs_util, admin, vol, group)


def f03_reject_mount_without_q(qtn, fs_util, admin, fuse, vol, config):
    """F-03 Reject mount without q — Permission denied (13)."""
    sub = _cfg(config, "sub_a", "qtn-sv-a")
    client = "qtn-f03-normal"
    mount = "/mnt/qtn-f03"
    try:
        if qtn.setup_subvolume(admin, vol, sub):
            return 1
        root, data = qtn.get_subvolume_paths(admin, vol, sub)
        qtn.create_rw_client(admin, vol, root, client)
        qtn.quarantine_enable(admin, vol, sub)
        if qtn.assert_fuse_mount_fails(fuse, mount, data, client, vol):
            return 1
        log.info("f03_reject_mount_without_q PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _umount_all(qtn, fuse, [mount])
        _del_clients(qtn, admin, [client])
        _rm_sv(qtn, admin, vol, sub)


def f04_mount_with_rwq(qtn, fs_util, admin, fuse, vol, config):
    """F-04 Mount with rwq — R/W on quarantined SV succeeds."""
    sub = _cfg(config, "sub_a", "qtn-sv-a")
    client = "qtn-f04-recovery"
    mount = "/mnt/qtn-f04"
    try:
        if qtn.setup_subvolume(admin, vol, sub):
            return 1
        root, data = qtn.get_subvolume_paths(admin, vol, sub)
        qtn.quarantine_enable(admin, vol, sub)
        qtn.create_rwq_client(admin, vol, root, client)
        qtn.mount_fuse(fuse, mount, data, client, vol)
        if qtn.assert_write_ok(fuse, mount, "recovery.txt"):
            return 1
        if qtn.assert_read_ok(fuse, mount, "recovery.txt"):
            return 1
        log.info("f04_mount_with_rwq PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _umount_all(qtn, fuse, [mount])
        _del_clients(qtn, admin, [client])
        _rm_sv(qtn, admin, vol, sub)


def f05_allow_star_denied(qtn, fs_util, admin, fuse, vol, config):
    """F-05 allow * does not grant quarantine access."""
    sub = _cfg(config, "sub_a", "qtn-sv-a")
    client = "qtn-f05-star"
    mount = "/mnt/qtn-f05"
    try:
        if qtn.setup_subvolume(admin, vol, sub):
            return 1
        _, data = qtn.get_subvolume_paths(admin, vol, sub)
        qtn.quarantine_enable(admin, vol, sub)
        qtn.create_star_client(admin, client)
        if qtn.assert_fuse_mount_fails(fuse, mount, data, client, vol):
            return 1
        log.info("f05_allow_star_denied PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _umount_all(qtn, fuse, [mount])
        _del_clients(qtn, admin, [client])
        _rm_sv(qtn, admin, vol, sub)


def f06_isolation_same_group(qtn, fs_util, admin, fuse, vol, config):
    """F-06 Isolation within same group — only SUB_A blocked."""
    group = _cfg(config, "group", "qtn-group")
    sub_a = _cfg(config, "sub_a", "qtn-sv-a")
    sub_b = _cfg(config, "sub_b", "qtn-sv-b")
    client_a, client_b = "qtn-f06-a", "qtn-f06-b"
    mount_a, mount_b = "/mnt/qtn-f06-a", "/mnt/qtn-f06-b"
    try:
        fs_util.create_subvolumegroup(admin, vol, group)
        if qtn.setup_subvolume(admin, vol, sub_a, group_name=group):
            return 1
        if qtn.setup_subvolume(admin, vol, sub_b, group_name=group):
            return 1
        root_a, data_a = qtn.get_subvolume_paths(admin, vol, sub_a, group_name=group)
        root_b, data_b = qtn.get_subvolume_paths(admin, vol, sub_b, group_name=group)

        qtn.create_rw_client(admin, vol, root_a, client_a)
        qtn.create_rw_client(admin, vol, root_b, client_b)
        qtn.mount_fuse(fuse, mount_b, data_b, client_b, vol)
        if qtn.write_baseline_file(fuse, mount_b, "test.txt", "b-ok"):
            return 1

        qtn.quarantine_enable(admin, vol, sub_a, group_name=group)
        if qtn.assert_fuse_mount_fails(fuse, mount_a, data_a, client_a, vol):
            return 1
        if qtn.assert_content_equals(fuse, mount_b, "test.txt", "b-ok"):
            return 1
        if qtn.assert_write_ok(fuse, mount_b, "new.txt"):
            return 1

        log.info("f06_isolation_same_group PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _umount_all(qtn, fuse, [mount_a, mount_b])
        _del_clients(qtn, admin, [client_a, client_b])
        _rm_sv(qtn, admin, vol, sub_a, group)
        _rm_sv(qtn, admin, vol, sub_b, group)
        _rm_group(fs_util, admin, vol, group)


def f07_info_shows_quarantine(qtn, fs_util, admin, fuse, vol, config):
    """
    F-07 Info shows quarantine status (minimal payload); volume info remains valid.

    Quarantined SV info: name / group / quarantine status only (no path/size).
    """
    sub = _cfg(config, "sub_a", "qtn-sv-a")
    try:
        if qtn.setup_subvolume(admin, vol, sub):
            return 1
        if qtn.assert_info_not_quarantined(admin, vol, sub):
            return 1

        qtn.quarantine_enable(admin, vol, sub)

        if qtn.assert_info_quarantined(admin, vol, sub):
            return 1

        out, err = admin.exec_command(
            sudo=True, cmd=f"ceph fs volume info {vol} -f json", check_ec=False
        )
        if admin.node.exit_status:
            log.error("volume info failed: %s %s", out, err)
            return 1
        vinfo = json.loads(out)
        if "pools" not in vinfo:
            log.error("volume info missing pools: %s", vinfo)
            return 1
        log.info("volume info OK (volume itself not quarantined)")

        log.info("f07_info_shows_quarantine PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _rm_sv(qtn, admin, vol, sub)


def f08_snapshot_ops_under_quarantine(qtn, fs_util, admin, fuse, vol, config):
    """F-08 Snapshot create/clone blocked while quarantined; pre-snap exists."""
    sub = _cfg(config, "sub_a", "qtn-sv-a")
    try:
        if qtn.setup_subvolume(admin, vol, sub):
            return 1
        admin.exec_command(
            sudo=True, cmd=f"ceph fs subvolume snapshot create {vol} {sub} snap1"
        )
        qtn.quarantine_enable(admin, vol, sub)

        rc = 0
        rc |= qtn.assert_mgr_op_fails(
            admin,
            f"ceph fs subvolume snapshot create {vol} {sub} snap2",
            "snapshot create while quarantined",
        )
        rc |= qtn.assert_mgr_op_fails(
            admin,
            f"ceph fs subvolume snapshot clone {vol} {sub} snap1 clone-from-snap1",
            "snapshot clone while quarantined",
        )
        if rc:
            return 1

        qtn.quarantine_disable(admin, vol, sub)
        snaps, _ = admin.exec_command(
            sudo=True, cmd=f"ceph fs subvolume snapshot ls {vol} {sub}"
        )
        if "snap1" not in snaps:
            log.error("pre-quarantine snap1 missing after disable: %s", snaps)
            return 1

        log.info("f08_snapshot_ops_under_quarantine PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        admin.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume snapshot rm {vol} {sub} snap1 --force",
            check_ec=False,
        )
        _rm_sv(qtn, admin, vol, sub)


def f09_clone_blocked(qtn, fs_util, admin, fuse, vol, config):
    """F-09 Clone blocked while source quarantined."""
    sub = _cfg(config, "sub_a", "qtn-sv-a")
    clone = f"{sub}-clone"
    try:
        if qtn.setup_subvolume(admin, vol, sub):
            return 1
        admin.exec_command(
            sudo=True, cmd=f"ceph fs subvolume snapshot create {vol} {sub} snap1"
        )
        qtn.quarantine_enable(admin, vol, sub)
        if qtn.assert_mgr_op_fails(
            admin,
            f"ceph fs subvolume snapshot clone {vol} {sub} snap1 {clone}",
            "clone while quarantined",
        ):
            return 1
        log.info("f09_clone_blocked PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        admin.exec_command(
            sudo=True,
            cmd=f"ceph fs clone cancel {vol} {clone}",
            check_ec=False,
        )
        _rm_sv(qtn, admin, vol, clone)
        admin.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume snapshot rm {vol} {sub} snap1 --force",
            check_ec=False,
        )
        _rm_sv(qtn, admin, vol, sub)


def f10_snap_schedule(qtn, fs_util, admin, fuse, vol, config):
    """
    F-10 Snap-schedule under quarantine.

    Schedule on subvolume root path; created_count must not increase while
    quarantined; after disable, schedule creates again.
    """
    sub = _cfg(config, "sub_a", "qtn-sv-a")
    wait_sec = int(_cfg(config, "snap_schedule_wait_sec", 75))
    sched_path = None
    try:
        admin.exec_command(
            sudo=True,
            cmd="ceph config set mgr mgr/snap_schedule/allow_m_granularity true",
            check_ec=False,
        )
        admin.exec_command(
            sudo=True, cmd="ceph mgr module enable snap_schedule", check_ec=False
        )

        if qtn.setup_subvolume(admin, vol, sub):
            return 1
        root, _data = qtn.get_subvolume_paths(admin, vol, sub)
        sched_path = root if root.endswith("/") else root + "/"

        admin.exec_command(
            sudo=True, cmd=f"ceph fs snap-schedule add {sched_path} 1m", check_ec=False
        )
        # EEXIST is fine if schedule already present from a prior attempt
        entries = _snap_schedule_status_entries(admin, sched_path)
        if not entries:
            log.error(
                "snap-schedule not active on %s — cannot validate F-10", sched_path
            )
            return 1

        log.info("Wait for at least one scheduled snap before quarantine")
        time.sleep(wait_sec)
        before = _max_created_count(_snap_schedule_status_entries(admin, sched_path))
        log.info("created_count before quarantine window: %s", before)

        qtn.quarantine_enable(admin, vol, sub)
        time.sleep(wait_sec)
        during = _max_created_count(_snap_schedule_status_entries(admin, sched_path))
        log.info("created_count during quarantine: %s", during)
        if during > before:
            log.error(
                "F-10 FAILED: snap-schedule created new snaps while quarantined "
                "(%s → %s)",
                before,
                during,
            )
            return 1

        qtn.quarantine_disable(admin, vol, sub)
        time.sleep(wait_sec)
        after = _max_created_count(_snap_schedule_status_entries(admin, sched_path))
        log.info("created_count after disable: %s", after)
        if after <= during:
            log.error(
                "F-10 FAILED: snap-schedule did not resume after disable " "(%s → %s)",
                during,
                after,
            )
            return 1

        log.info("f10_snap_schedule PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _rm_sv(qtn, admin, vol, sub)


def f11_subdir_mount_blocked(qtn, fs_util, admin, fuse, vol, config):
    """F-11 Mount subdir under quarantined SV also blocked."""
    sub = _cfg(config, "sub_a", "qtn-sv-a")
    client = "qtn-f11-normal"
    mount = "/mnt/qtn-f11"
    mount_sub = "/mnt/qtn-f11-subdir"
    try:
        if qtn.setup_subvolume(admin, vol, sub):
            return 1
        root, data = qtn.get_subvolume_paths(admin, vol, sub)
        qtn.create_rw_client(admin, vol, root, client)
        qtn.mount_fuse(fuse, mount, data, client, vol)
        fuse.exec_command(sudo=True, cmd=f"mkdir -p {mount}/subdir")
        fuse.exec_command(
            sudo=True, cmd=f"bash -c 'echo x > {mount}/subdir/f.txt'", check_ec=True
        )
        qtn.umount_fuse(fuse, mount)

        qtn.quarantine_enable(admin, vol, sub)
        if qtn.assert_fuse_mount_fails(fuse, mount_sub, f"{data}/subdir", client, vol):
            return 1

        log.info("f11_subdir_mount_blocked PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _umount_all(qtn, fuse, [mount, mount_sub])
        _del_clients(qtn, admin, [client])
        _rm_sv(qtn, admin, vol, sub)


def f12_root_mount_vs_quarantined(qtn, fs_util, admin, fuse, vol, config):
    """F-12 Root mount OK; access into quarantined SV tree blocked."""
    sub = _cfg(config, "sub_a", "qtn-sv-a")
    sub_b = _cfg(config, "sub_b", "qtn-sv-b")
    mount_root = "/mnt/qtn-f12-root"
    try:
        if qtn.setup_subvolume(admin, vol, sub):
            return 1
        if qtn.setup_subvolume(admin, vol, sub_b):
            return 1
        root, data = qtn.get_subvolume_paths(admin, vol, sub)
        _root_b, data_b = qtn.get_subvolume_paths(admin, vol, sub_b)

        _mount_root(fuse, mount_root, vol)
        # write a marker via admin path before quarantine
        fuse.exec_command(
            sudo=True,
            cmd=f"bash -c 'echo pre > {mount_root}{data}/marker.txt'",
            check_ec=False,
        )

        qtn.quarantine_enable(admin, vol, sub)

        log.info("Parent listing may show name but access must fail")
        out, err = fuse.exec_command(
            sudo=True,
            cmd=f"ls {mount_root}{root}",
            check_ec=False,
        )
        if fuse.node.exit_status == 0:
            log.error("expected Permission denied on quarantined SV path ls: %s", out)
            return 1
        log.info("ls quarantined path blocked as expected: %s %s", out, err)

        _, _ = fuse.exec_command(
            sudo=True, cmd=f"cat {mount_root}{data}/marker.txt", check_ec=False
        )
        if fuse.node.exit_status == 0:
            log.error("expected EACCES reading quarantined path via root mount")
            return 1

        # sibling still accessible via root mount
        fuse.exec_command(
            sudo=True,
            cmd=f"bash -c 'echo sib > {mount_root}{data_b}/ok.txt'",
            check_ec=True,
        )

        log.info("f12_root_mount_vs_quarantined PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _umount_all(qtn, fuse, [mount_root])
        _rm_sv(qtn, admin, vol, sub)
        _rm_sv(qtn, admin, vol, sub_b)


def _resolve_active_mgr_node(admin, ceph_cluster):
    """
    Return (mgr_node, daemon_name) for the *active* MGR.

    cephadm logs must be run on the host where that MGR runs — not on a client.
    """
    dump_out, _ = admin.exec_command(
        sudo=True, cmd="ceph mgr dump -f json", check_ec=False
    )
    active_name = ""
    try:
        active_name = json.loads(dump_out).get("active_name", "") or ""
    except json.JSONDecodeError:
        log.warning("could not parse ceph mgr dump: %s", dump_out)
    log.info("Active MGR name from mgr dump: %s", active_name)

    orch_out, _ = admin.exec_command(
        sudo=True, cmd="ceph orch ps --daemon_type mgr -f json", check_ec=False
    )
    try:
        mgrs = json.loads(orch_out) if orch_out else []
    except json.JSONDecodeError:
        mgrs = []

    daemon_name = None
    hostname = None
    for mgr in mgrs:
        did = str(mgr.get("daemon_id", ""))
        dname = str(mgr.get("daemon_name", "") or "")
        if not dname and did:
            dname = f"mgr.{did}"
        status = str(mgr.get("status_desc", "") or mgr.get("status", "")).lower()
        matches_active = bool(
            active_name
            and (
                active_name == did
                or active_name in did
                or active_name in dname
                or did in active_name
            )
        )
        if matches_active or (not active_name and "running" in status):
            daemon_name = dname if dname.startswith("mgr.") else f"mgr.{dname}"
            hostname = mgr.get("hostname")
            if matches_active:
                break

    if not daemon_name and active_name:
        daemon_name = (
            active_name if active_name.startswith("mgr.") else f"mgr.{active_name}"
        )

    log.info("Resolved MGR daemon_name=%s hostname=%s", daemon_name, hostname)

    mgr_node = None
    nodes = list(ceph_cluster.get_nodes())
    if hostname:
        for node in nodes:
            if (
                node.hostname == hostname
                or hostname in node.hostname
                or node.hostname in str(hostname)
            ):
                mgr_node = node
                break

    if not mgr_node and active_name:
        for node in nodes:
            if node.hostname and node.hostname in active_name:
                mgr_node = node
                break

    if not mgr_node:
        for role in ("mgr", "installer", "_admin"):
            role_nodes = ceph_cluster.get_nodes(role=role)
            if role_nodes:
                mgr_node = role_nodes[0]
                log.warning(
                    "Falling back to first node with role=%s: %s",
                    role,
                    mgr_node.hostname,
                )
                break

    return mgr_node, daemon_name


def f13_logs_set_unset(qtn, fs_util, admin, fuse, vol, config):
    """F-13 Active MGR logs contain quarantine enable/disable entries."""
    sub = _cfg(config, "sub_a", "qtn-sv-a")
    try:
        if qtn.setup_subvolume(admin, vol, sub):
            return 1

        qtn.quarantine_enable(admin, vol, sub)
        qtn.quarantine_disable(admin, vol, sub)

        mgr_node, daemon_name = _resolve_active_mgr_node(admin, qtn.ceph_cluster)
        if not mgr_node or not daemon_name:
            log.error(
                "F-13 FAILED: could not resolve active MGR node/daemon "
                "(node=%s daemon=%s)",
                getattr(mgr_node, "hostname", None),
                daemon_name,
            )
            return 1

        log.info(
            "Collecting cephadm logs for %s on active MGR host %s",
            daemon_name,
            mgr_node.hostname,
        )
        # Run on the MGR/installer host — not the client
        mgr_node.exec_command(
            sudo=True,
            cmd=(
                f"cephadm logs --name {shlex.quote(daemon_name)} "
                f"> {shlex.quote('/tmp/qtn-f13-mgr.log')}"
            ),
            check_ec=False,
        )
        logs, err = mgr_node.exec_command(
            sudo=True,
            cmd="tail -n 800 /tmp/qtn-f13-mgr.log",
            check_ec=False,
        )
        if not logs:
            log.warning("empty mgr log output: %s", err)

        hits = [
            line for line in (logs or "").splitlines() if "quarantine" in line.lower()
        ]
        log.info(
            "mgr %s on %s quarantine log hits: %d",
            daemon_name,
            mgr_node.hostname,
            len(hits),
        )
        for line in hits[:30]:
            log.info("MGRLOG: %s", line)

        enable_hit = any(
            "quarantine_enable" in line.lower() or "quarantine enable" in line.lower()
            for line in hits
        )
        disable_hit = any(
            "quarantine_disable" in line.lower() or "quarantine disable" in line.lower()
            for line in hits
        )
        if not hits:
            log.error(
                "F-13 FAILED: no quarantine entries in active MGR (%s) logs on %s",
                daemon_name,
                mgr_node.hostname,
            )
            return 1
        if not enable_hit or not disable_hit:
            log.error(
                "F-13 FAILED: need both enable and disable log lines "
                "(enable_hit=%s disable_hit=%s)",
                enable_hit,
                disable_hit,
            )
            return 1

        log.info("f13_logs_set_unset PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _rm_sv(qtn, admin, vol, sub)


def f14_mgr_ops_matrix(qtn, fs_util, admin, fuse, vol, config):
    """
    F-14 MGR ops matrix while quarantined (updated Dev contract).

    Blocked on quarantined SV: resize, rm, snapshot create/clone/ls, getpath,
    earmark set/get
    Allowed: subvolume ls (names only), info (minimal quarantine status);
    earmark set/get on non-quarantined sibling SV
    """
    sub_a = _cfg(config, "sub_a", "qtn-sv-a")
    sub_b = _cfg(config, "sub_b", "qtn-sv-b")
    try:
        if qtn.setup_subvolume(admin, vol, sub_a):
            return 1
        if qtn.setup_subvolume(admin, vol, sub_b):
            return 1
        admin.exec_command(
            sudo=True, cmd=f"ceph fs subvolume snapshot create {vol} {sub_a} snap1"
        )
        qtn.quarantine_enable(admin, vol, sub_a)

        rc = 0
        log.info("--- Expect FAIL (structural + getpath) ---")
        rc |= qtn.assert_mgr_op_fails(
            admin, f"ceph fs subvolume resize {vol} {sub_a} 10485760", "resize"
        )
        rc |= qtn.assert_mgr_op_fails(
            admin, f"ceph fs subvolume rm {vol} {sub_a}", "rm"
        )
        rc |= qtn.assert_mgr_op_fails(
            admin,
            f"ceph fs subvolume snapshot create {vol} {sub_a} snap-blocked",
            "snapshot create",
        )
        rc |= qtn.assert_mgr_op_fails(
            admin,
            f"ceph fs subvolume snapshot clone {vol} {sub_a} snap1 clone-x",
            "snapshot clone",
        )
        rc |= qtn.assert_mgr_op_fails(
            admin, f"ceph fs subvolume getpath {vol} {sub_a}", "getpath"
        )
        rc |= qtn.assert_mgr_op_fails(
            admin,
            f"ceph fs subvolume snapshot ls {vol} {sub_a}",
            "snapshot ls",
        )
        rc |= qtn.assert_mgr_op_fails(
            admin,
            f"ceph fs subvolume earmark set {vol} {sub_a} --earmark nfs",
            "earmark set",
        )
        rc |= qtn.assert_mgr_op_fails(
            admin,
            f"ceph fs subvolume earmark get {vol} {sub_a}",
            "earmark get",
        )

        log.info("--- Expect SUCCEED (ls / minimal info) ---")
        if qtn.assert_subvolume_ls_complete(admin, vol, [sub_a, sub_b]):
            log.error("F-14 FAILED: subvolume ls must list names while quarantined")
            rc = 1
        if qtn.assert_info_quarantined(admin, vol, sub_a):
            rc = 1
        # sibling remains fully visible via info
        if qtn.assert_info_not_quarantined(admin, vol, sub_b):
            rc = 1

        log.info("--- Earmark on non-quarantined sibling (working CLI) ---")
        rc |= qtn.assert_mgr_op_succeeds(
            admin,
            f"ceph fs subvolume earmark set {vol} {sub_b} --earmark nfs",
            "earmark set on sibling",
        )
        rc |= qtn.assert_mgr_op_succeeds(
            admin,
            f"ceph fs subvolume earmark set {vol} {sub_b} --earmark smb",
            "earmark update on sibling",
        )
        info_b = qtn.get_subvolume_info_json(admin, vol, sub_b)
        if not info_b or info_b.get("earmark") != "smb":
            log.error(
                "F-14 FAILED: sibling info earmark expected smb, got %s",
                info_b.get("earmark") if info_b else None,
            )
            rc = 1
        rc |= qtn.assert_mgr_op_succeeds(
            admin,
            f"ceph fs subvolume earmark get {vol} {sub_b}",
            "earmark get on sibling",
        )

        if rc:
            return 1
        log.info("f14_mgr_ops_matrix PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        qtn.quarantine_disable(admin, vol, sub_a)
        admin.exec_command(
            sudo=True,
            cmd=f"ceph fs subvolume snapshot rm {vol} {sub_a} snap1 --force",
            check_ec=False,
        )
        _rm_sv(qtn, admin, vol, "clone-x")
        _rm_sv(qtn, admin, vol, sub_a)
        _rm_sv(qtn, admin, vol, sub_b)


def f15_symlink_into_quarantined(qtn, fs_util, admin, fuse, vol, config):
    """F-15 Symlink into quarantined path also returns EACCES."""
    sub = _cfg(config, "sub_a", "qtn-sv-a")
    client = "qtn-f15-normal"
    mount_root = "/mnt/qtn-f15-root"
    mount_sv = "/mnt/qtn-f15-sv"
    try:
        if qtn.setup_subvolume(admin, vol, sub):
            return 1
        root, data = qtn.get_subvolume_paths(admin, vol, sub)
        qtn.create_rw_client(admin, vol, root, client)

        _mount_root(fuse, mount_root, vol)
        qtn.mount_fuse(fuse, mount_sv, data, client, vol)
        if qtn.write_baseline_file(fuse, mount_sv, "orig.txt", "data"):
            return 1
        fuse.exec_command(
            sudo=True,
            cmd=(
                f"ln -sfn {mount_root}{data}/orig.txt " f"{mount_root}/outside-link.txt"
            ),
        )

        qtn.quarantine_enable(admin, vol, sub)

        _, _ = fuse.exec_command(
            sudo=True, cmd=f"cat {mount_root}/outside-link.txt", check_ec=False
        )
        if fuse.node.exit_status == 0:
            log.error("expected EACCES via symlink into quarantined tree")
            return 1
        log.info("symlink access blocked as expected")

        if qtn.assert_read_blocked(fuse, mount_sv, "orig.txt"):
            return 1

        log.info("f15_symlink_into_quarantined PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _umount_all(qtn, fuse, [mount_sv, mount_root])
        _del_clients(qtn, admin, [client])
        _rm_sv(qtn, admin, vol, sub)


def f16_multiple_quarantined(qtn, fs_util, admin, fuse, vol, config):
    """F-16 Multiple SVs quarantined — normal blocked; rwq/rwQ recovery OK."""
    sub_a = _cfg(config, "sub_a", "qtn-sv-a")
    sub_b = _cfg(config, "sub_b", "qtn-sv-b")
    ca, cb = "qtn-f16-a", "qtn-f16-b"
    ra, rb = "qtn-f16-rec-a", "qtn-f16-rec-b"
    rq = "qtn-f16-recQ"
    ma, mb = "/mnt/qtn-f16-a", "/mnt/qtn-f16-b"
    mra, mrb = "/mnt/qtn-f16-rec-a", "/mnt/qtn-f16-rec-b"
    mqa, mqb = "/mnt/qtn-f16-recQ-a", "/mnt/qtn-f16-recQ-b"
    try:
        if qtn.setup_subvolume(admin, vol, sub_a):
            return 1
        if qtn.setup_subvolume(admin, vol, sub_b):
            return 1
        root_a, data_a = qtn.get_subvolume_paths(admin, vol, sub_a)
        root_b, data_b = qtn.get_subvolume_paths(admin, vol, sub_b)
        qtn.create_rw_client(admin, vol, root_a, ca)
        qtn.create_rw_client(admin, vol, root_b, cb)

        qtn.mount_fuse(fuse, ma, data_a, ca, vol)
        qtn.mount_fuse(fuse, mb, data_b, cb, vol)
        qtn.write_baseline_file(fuse, ma, BASELINE_FILE, "data-a")
        qtn.write_baseline_file(fuse, mb, BASELINE_FILE, "data-b")
        qtn.umount_fuse(fuse, ma)
        qtn.umount_fuse(fuse, mb)

        qtn.quarantine_enable(admin, vol, sub_a)
        qtn.quarantine_enable(admin, vol, sub_b)

        if qtn.assert_fuse_mount_fails(fuse, ma, data_a, ca, vol):
            return 1
        if qtn.assert_fuse_mount_fails(fuse, mb, data_b, cb, vol):
            return 1

        qtn.create_rwq_client(admin, vol, root_a, ra)
        qtn.create_rwq_client(admin, vol, root_b, rb)
        qtn.mount_fuse(fuse, mra, data_a, ra, vol)
        qtn.mount_fuse(fuse, mrb, data_b, rb, vol)
        if qtn.assert_content_equals(fuse, mra, BASELINE_FILE, "data-a"):
            return 1
        if qtn.assert_content_equals(fuse, mrb, BASELINE_FILE, "data-b"):
            return 1
        qtn.umount_fuse(fuse, mra)
        qtn.umount_fuse(fuse, mrb)

        qtn.create_rwQ_client(admin, vol, rq)
        qtn.mount_fuse(fuse, mqa, data_a, rq, vol)
        qtn.mount_fuse(fuse, mqb, data_b, rq, vol)
        if qtn.assert_read_ok(fuse, mqa, BASELINE_FILE):
            return 1
        if qtn.assert_read_ok(fuse, mqb, BASELINE_FILE):
            return 1

        log.info("f16_multiple_quarantined PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        _umount_all(qtn, fuse, [ma, mb, mra, mrb, mqa, mqb])
        _del_clients(qtn, admin, [ca, cb, ra, rb, rq])
        _rm_sv(qtn, admin, vol, sub_a)
        _rm_sv(qtn, admin, vol, sub_b)


def f17_quarantine_during_heavy_io(qtn, fs_util, admin, fuse, vol, config):
    """F-17 Quarantine during heavy IO — enable succeeds; new IO blocked."""
    sub = _cfg(config, "sub_a", "qtn-sv-a")
    client = "qtn-f17-normal"
    mount = "/mnt/qtn-f17"
    try:
        if qtn.setup_subvolume(admin, vol, sub):
            return 1
        root, data = qtn.get_subvolume_paths(admin, vol, sub)
        qtn.delete_client(admin, client)
        qtn.create_rw_client(admin, vol, root, client)
        qtn.mount_fuse(fuse, mount, data, client, vol)

        log.info("Start background dd IO")
        fuse.exec_command(
            sudo=True,
            cmd=(
                f"bash -c 'dd if=/dev/zero of={mount}/big.img bs=1M count=512 "
                f"oflag=direct >/tmp/qtn-f17-dd.out 2>&1 &'"
            ),
            check_ec=False,
        )
        time.sleep(2)

        result = qtn.quarantine_enable(admin, vol, sub)
        log.info("enable during IO: %s", result)

        time.sleep(3)
        if qtn.assert_write_blocked(fuse, mount, "after.txt"):
            return 1
        # read of big.img may fail with EACCES
        _, _ = fuse.exec_command(
            sudo=True, cmd=f"cat {mount}/big.img >/dev/null", check_ec=False
        )
        if fuse.node.exit_status == 0:
            log.error("expected post-quarantine read of big.img to fail")
            return 1
        log.info("post-quarantine IO blocked as expected")

        log.info("f17_quarantine_during_heavy_io PASSED")
        return 0
    except Exception:
        qtn.log_exception()
        return 1
    finally:
        fuse.exec_command(
            sudo=True, cmd="pkill -f 'dd if=/dev/zero' || true", check_ec=False
        )
        _umount_all(qtn, fuse, [mount])
        _del_clients(qtn, admin, [client])
        _rm_sv(qtn, admin, vol, sub)


def f18_encrypted_na(qtn, fs_util, admin, fuse, vol, config):
    """F-18 Encrypted subvolume + quarantine — N/A pending GKLM/fscrypt lab."""
    log.info(
        "F-18 N/A: encrypted subvolume quarantine requires GKLM/fscrypt lab setup. "
        "Skipping until environment is available."
    )
    return 0


SUBTESTS = {
    "f01_group_quarantine": f01_group_quarantine,
    "f02_clear_quarantine": f02_clear_quarantine,
    "f03_reject_mount_without_q": f03_reject_mount_without_q,
    "f04_mount_with_rwq": f04_mount_with_rwq,
    "f05_allow_star_denied": f05_allow_star_denied,
    "f06_isolation_same_group": f06_isolation_same_group,
    "f07_info_shows_quarantine": f07_info_shows_quarantine,
    "f08_snapshot_ops_under_quarantine": f08_snapshot_ops_under_quarantine,
    "f09_clone_blocked": f09_clone_blocked,
    "f10_snap_schedule": f10_snap_schedule,
    "f11_subdir_mount_blocked": f11_subdir_mount_blocked,
    "f12_root_mount_vs_quarantined": f12_root_mount_vs_quarantined,
    "f13_logs_set_unset": f13_logs_set_unset,
    "f14_mgr_ops_matrix": f14_mgr_ops_matrix,
    "f15_symlink_into_quarantined": f15_symlink_into_quarantined,
    "f16_multiple_quarantined": f16_multiple_quarantined,
    "f17_quarantine_during_heavy_io": f17_quarantine_during_heavy_io,
    "f18_encrypted_na": f18_encrypted_na,
}


def run(ceph_cluster, **kw):
    """Run Functional subtests for CephFS subvolume quarantine."""
    config = kw.get("config") or {}
    log.info("=" * 80)
    log.info("TEST TYPE : Functional")
    log.info("MODULE    : test_subvolume_quarantine_functional.py")
    log.info("POLARION  : CEPH-83632678")
    log.info("=" * 80)

    prepared = _prepare(ceph_cluster, config)
    if prepared is None:
        return 1
    qtn, fs_util, admin, fuse, vol = prepared

    requested = config.get("subtests")
    test_list = requested if requested else list(SUBTESTS.keys())

    failed = []
    for name in test_list:
        if name not in SUBTESTS:
            log.error(
                "Unknown Functional subtest '%s'; known: %s", name, list(SUBTESTS)
            )
            failed.append(name)
            continue

        log.info("")
        log.info("=" * 80)
        log.info("SUBTEST START : [Functional] %s", name)
        log.info("DESC          : %s", (SUBTESTS[name].__doc__ or "").strip())
        log.info("=" * 80)

        try:
            rc = SUBTESTS[name](qtn, fs_util, admin, fuse, vol, config)
        except Exception:
            log.error("SUBTEST EXCEPTION : [Functional] %s", name)
            log.error(traceback.format_exc())
            rc = 1

        if rc:
            log.error("SUBTEST FAILED  : [Functional] %s", name)
            failed.append(name)
        else:
            log.info("SUBTEST PASSED  : [Functional] %s", name)
        log.info("-" * 80)

    log.info("=" * 80)
    if failed:
        log.error(
            "Functional summary: %d/%d FAILED → %s",
            len(failed),
            len(test_list),
            failed,
        )
        log.info("=" * 80)
        return 1

    log.info("Functional summary: ALL %d subtest(s) PASSED", len(test_list))
    log.info("=" * 80)
    return 0
