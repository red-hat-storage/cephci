"""
IBMCEPH-19131 / IBMCEPH-18277 — Objecter per-OSD session strand dispatch (OSDC).

Minimal CephCI coverage:
  - Flag on/off (objecter_dispatch_to_session_strand)
  - Few FIO direct-IO combinations OFF vs ON on fuse (libcephfs)
  - Negative + corner cases

Full formal SC matrix remains lab/QE Share; this is a short CI-friendly gate.
"""

import json
import secrets
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)

FLAG = "objecter_dispatch_to_session_strand"
DIAG = "objecter_session_strands"


def _rand_suffix(n=5):
    return "".join(secrets.choice(string.ascii_lowercase + string.digits) for _ in range(n))


def _ensure_fio(client):
    out, _ = client.exec_command(sudo=True, cmd="which fio", check_ec=False)
    if out and out.strip():
        return
    # Best-effort install (rpm/deb)
    client.exec_command(sudo=True, cmd="yum install -y fio", check_ec=False)
    out, _ = client.exec_command(sudo=True, cmd="which fio", check_ec=False)
    if out and out.strip():
        return
    client.exec_command(sudo=True, cmd="apt-get install -y fio", check_ec=False)
    out, _ = client.exec_command(sudo=True, cmd="which fio", check_ec=False)
    if not (out and out.strip()):
        raise CommandFailed("fio is required but could not be installed")


def _flag_supported(client):
    """Return True if the OSDC config option exists on this build."""
    out, err = client.exec_command(
        sudo=True,
        cmd=f"ceph config get client {FLAG}",
        check_ec=False,
    )
    text = f"{out or ''} {err or ''}"
    if client.exit_status != 0 or "ENOENT" in text or "Error ENOENT" in text:
        return False
    return True


def _set_flag(client, value: str):
    client.exec_command(
        sudo=True,
        cmd=f"ceph config set client {FLAG} {value}",
    )
    got, _ = client.exec_command(sudo=True, cmd=f"ceph config get client {FLAG}")
    got = got.strip()
    log.info(f"{FLAG} -> set={value} get={got}")
    if got != value:
        raise CommandFailed(f"flag mismatch: expected {value}, got {got}")
    return got


def _remount_fuse(fs_util, client, mount_dir, fs_name):
    client.exec_command(sudo=True, cmd=f"umount -l {mount_dir}", check_ec=False)
    fs_util.fuse_mount(
        [client],
        mount_dir,
        new_client_hostname="admin",
        extra_params=f" --client_fs {fs_name}",
    )


def _run_fio_cell(client, filename, rw, bs, jobs, runtime, iodepth=32):
    """Run one short FIO cell; return mean iops from json+ output."""
    out_json = f"/tmp/osdc_fio_{rw}_{bs}_j{jobs}_{_rand_suffix()}.json"
    cmd = (
        f"fio --name=osdc --filename={filename} --ioengine=libaio --direct=1 "
        f"--rw={rw} --bs={bs} --iodepth={iodepth} --numjobs={jobs} "
        f"--time_based --runtime={runtime} --group_reporting "
        f"--thread=1 --norandommap=1 --randrepeat=0 "
        f"--output-format=json+ --output={out_json}"
    )
    client.exec_command(sudo=True, cmd=cmd, timeout=runtime + 180)
    raw, _ = client.exec_command(sudo=True, cmd=f"cat {out_json}")
    client.exec_command(sudo=True, cmd=f"rm -f {out_json}", check_ec=False)
    data = json.loads(raw)
    job = data["jobs"][0]
    side = "write" if "write" in rw else "read"
    iops = float(job[side]["iops"])
    pct = (job[side].get("clat_ns") or {}).get("percentile") or {}
    p99 = pct.get("99.000000")
    return {"rw": rw, "bs": bs, "jobs": f"j{jobs}", "iops": iops, "p99_ns": p99}


def _prealloc(client, path, size="256M"):
    client.exec_command(
        sudo=True,
        cmd=(
            f"fio --name=prealloc --filename={path} --size={size} --bs=1M "
            f"--rw=write --iodepth=8 --direct=1 --numjobs=1 --group_reporting"
        ),
        timeout=600,
    )


def run(ceph_cluster, **kw):
    """
    Minimal OSDC async-dispatch validation (flag OFF vs ON).

    Config (optional):
      runtime: int seconds per FIO cell (default 30)
      iodepth: FIO iodepth (default 32)
      cells: list of {rw, bs, jobs} (default: few combinations)
      max_regression_pct: soft warn if OSDC IOPS worse than this vs BASE (default 50)
      skip_if_unsupported: bool (default True) — pass when option absent on older builds
    """
    try:
        test_data = kw.get("test_data")
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        config = kw.get("config") or {}
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        if not clients:
            log.error("No client nodes available")
            return 1
        client = clients[0]

        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        fs_name = "cephfs" if not erasure else "cephfs-ec"
        if not fs_util.get_fs_info(client, fs_name):
            fs_util.create_fs(client, fs_name)

        _ensure_fio(client)

        skip_if_unsupported = config.get("skip_if_unsupported", True)
        if not _flag_supported(client):
            msg = (
                f"{FLAG} not present on this build — "
                "OSDC feature requires Ceph 10.0+ / PR #68869"
            )
            if skip_if_unsupported:
                log.warning(msg + " ; skipping (pass)")
                return 0
            log.error(msg)
            return 1

        runtime = int(config.get("runtime", 30))
        iodepth = int(config.get("iodepth", 32))
        max_reg = float(config.get("max_regression_pct", 50))
        cells = config.get(
            "cells",
            [
                {"rw": "randread", "bs": "4k", "jobs": 8},
                {"rw": "randwrite", "bs": "4k", "jobs": 8},
                {"rw": "randread", "bs": "64k", "jobs": 8},
                {"rw": "randwrite", "bs": "64k", "jobs": 8},
            ],
        )

        mount_dir = f"/mnt/osdc_fuse_{_rand_suffix()}"
        fs_util.fuse_mount(
            [client],
            mount_dir,
            new_client_hostname="admin",
            extra_params=f" --client_fs {fs_name}",
        )
        work = f"{mount_dir}/osdc_work"
        client.exec_command(sudo=True, cmd=f"mkdir -p {work}")
        fio_file = f"{work}/fio-data.dat"

        # ---------- Negative tests ----------
        log.info("=== Negative: invalid flag value ===")
        client.exec_command(
            sudo=True,
            cmd=f"ceph config set client {FLAG} not_a_bool",
            check_ec=False,
        )
        if client.exit_status == 0:
            _set_flag(client, "false")
            raise CommandFailed("expected invalid flag value to be rejected")
        log.info("invalid value rejected as expected")

        log.info("=== Negative: unknown related option should fail cleanly ===")
        client.exec_command(
            sudo=True,
            cmd="ceph config set client objecter_dispatch_to_session_strand_TYPO true",
            check_ec=False,
        )
        if client.exit_status == 0:
            raise CommandFailed("typo option should not succeed")
        log.info("typo option rejected as expected")

        # ---------- Corner: idempotent set + get ----------
        log.info("=== Corner: set true twice (idempotent) ===")
        _set_flag(client, "true")
        _set_flag(client, "true")
        _set_flag(client, "false")

        # ---------- Baseline OFF ----------
        log.info("=== BASE: flag=false, remount, FIO cells ===")
        _set_flag(client, "false")
        _remount_fuse(fs_util, client, mount_dir, fs_name)
        client.exec_command(sudo=True, cmd=f"mkdir -p {work}")
        _prealloc(client, fio_file, size=config.get("fio_size", "256M"))

        base_results = []
        for c in cells:
            r = _run_fio_cell(
                client,
                fio_file,
                c["rw"],
                c["bs"],
                int(c["jobs"]),
                runtime,
                iodepth=iodepth,
            )
            base_results.append(r)
            log.info(f"BASE cell {r}")

        # ---------- Corner: flip ON while short IO runs ----------
        log.info("=== Corner: flip flag true during background fio ===")
        bg = (
            f"fio --name=bg --filename={fio_file} --ioengine=libaio --direct=1 "
            f"--rw=randread --bs=4k --iodepth=16 --numjobs=4 --time_based "
            f"--runtime={max(runtime, 45)} --group_reporting >/tmp/osdc_bg.out 2>&1 & echo $!"
        )
        pid_out, _ = client.exec_command(sudo=True, cmd=bg)
        _set_flag(client, "true")
        client.exec_command(sudo=True, cmd="sleep 5")
        # wait for bg to finish (best effort)
        client.exec_command(
            sudo=True,
            cmd=f"wait {pid_out.strip()} 2>/dev/null || true",
            check_ec=False,
            timeout=runtime + 120,
        )
        log.info("flag flip under IO completed without command failure")

        # ---------- OSDC ON ----------
        log.info("=== OSDC: flag=true, remount, FIO cells ===")
        _set_flag(client, "true")
        _remount_fuse(fs_util, client, mount_dir, fs_name)
        client.exec_command(sudo=True, cmd=f"mkdir -p {work}")

        osdc_results = []
        for c in cells:
            r = _run_fio_cell(
                client,
                fio_file,
                c["rw"],
                c["bs"],
                int(c["jobs"]),
                runtime,
                iodepth=iodepth,
            )
            osdc_results.append(r)
            log.info(f"OSDC cell {r}")

        # ---------- Compare (informational + soft regression guard) ----------
        log.info("=== Compare BASE vs OSDC ===")
        hard_fail = False
        for b, o in zip(base_results, osdc_results):
            if b["iops"] <= 0 or o["iops"] <= 0:
                log.error(f"non-positive iops: BASE={b} OSDC={o}")
                hard_fail = True
                continue
            delta = (o["iops"] - b["iops"]) / b["iops"] * 100.0
            log.info(
                f"{b['rw']} {b['bs']} {b['jobs']}: "
                f"BASE_iops={b['iops']:.2f} OSDC_iops={o['iops']:.2f} delta={delta:+.2f}%"
            )
            if delta < -max_reg:
                log.error(
                    f"OSDC IOPS regressed by {delta:.1f}% "
                    f"(threshold -{max_reg}%) on {b['rw']} {b['bs']} {b['jobs']}"
                )
                hard_fail = True

        # Best-effort diagnostics visibility (may not be exposed the same on all builds)
        log.info("=== Corner: diagnostics name visibility ===")
        dout, drc = client.exec_command(
            sudo=True,
            cmd=f"ceph --help 2>&1 | grep -i {DIAG} || true",
            check_ec=False,
        )
        log.info(f"diagnostics grep rc-ish: {(dout or '').strip()[:200]}")

        # Restore default-ish off for suite isolation
        _set_flag(client, "false")

        if hard_fail:
            return 1
        log.info("OSDC minimal flag on/off + IO comparison PASSED")
        return 0

    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        try:
            clients = ceph_cluster.get_ceph_objects("client")
            client = clients[0]
            # best-effort cleanup mounts
            client.exec_command(
                sudo=True,
                cmd="bash -c 'umount -l /mnt/osdc_fuse_* 2>/dev/null || true'",
                check_ec=False,
            )
            client.exec_command(
                sudo=True,
                cmd=f"ceph config set client {FLAG} false",
                check_ec=False,
            )
        except Exception:
            pass
