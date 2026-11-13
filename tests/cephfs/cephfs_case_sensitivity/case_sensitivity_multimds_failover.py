"""
IBMCEPH-16942 / IBMCEPH-21205 / tracker #72399 — casefold subvolume under
multi-active MDS.

Regression coverage for MDS journal replay and rejoin failures involving
``alternate_name`` and subtree authority after failover during case-insensitive
metadata workloads.

Polarion: CEPH-83632953

Usecase 1: rename-heavy casefold storm + rotating ``ceph mds fail``.
Usecase 2: ``max_mds`` scale-down/scale-up under load.
Usecase 3: casefold ``rm Foo`` then ``create FOO`` + MDS fail (#72399).
Usecase 4: create → journal flush/trim → unlink → recreate → MDS fail
(empty cached alternate_name vs journaled fullbit; #72399).
"""

import json
import os
import random
import string
import time

from cli.ceph.ceph import Ceph
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.exceptions import FsBaseException, log_and_fail
from tests.cephfs.lib.cephfs_attributes_lib import CephFSAttributeUtilities
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from utility.log import Log

log = Log(__name__)

_ACCEPTABLE_FAILOVER_HEALTH_WARNINGS = (
    "MDS_INSUFFICIENT_STANDBY",
    "insufficient standby MDS daemons available",
    "SLOW_OPS",
)


def _subvolume_is_case_insensitive(subvol_info):
    val = subvol_info.get("casesensitive")
    if val is False:
        return True
    return isinstance(val, str) and val.lower() == "false"


def _remove_subvolume_if_exists(client, fs_name, subvol_name, subvol_group):
    client.exec_command(
        sudo=True,
        cmd="ceph fs subvolume rm {} {} {} --force".format(
            fs_name, subvol_name, subvol_group
        ),
        check_ec=False,
    )


def _mds_placement_hosts(ceph_cluster, count=4):
    mds_nodes = ceph_cluster.get_ceph_objects("mds")
    hostnames = [m.node.hostname for m in mds_nodes]
    if len(hostnames) < 2:
        raise FsBaseException(
            "Multi-MDS casefold failover requires at least 2 MDS nodes, found {}".format(
                len(hostnames)
            )
        )
    use = min(count, len(hostnames))
    return use, " ".join(hostnames[:use]) + " "


def _parse_fs_status_rank(rank):
    if rank is None:
        return None
    if isinstance(rank, int):
        return rank
    rank_str = str(rank).strip()
    if rank_str.endswith("-s"):
        return None
    try:
        return int(rank_str)
    except ValueError:
        return None


def _wait_for_fs_ranks_active(
    client, fs_name, max_mds, timeout=600, interval=10, laggy_fail_sec=180
):
    """
    Wait until all ranks are active.

    If any rank stays in ``replay(laggy)`` for ``laggy_fail_sec`` consecutive
    seconds, return False early — that is the IBMCEPH-21205 / #72399 hang shape
    and waiting the full timeout only delays crash-oracle attribution.
    """
    max_mds = int(max_mds)
    end = time.time() + timeout
    laggy_since = {}
    while time.time() < end:
        out, _ = client.exec_command(
            sudo=True,
            cmd="ceph fs status {} -f json".format(fs_name),
            client_exec=True,
            check_ec=False,
        )
        try:
            status = json.loads(out)
        except json.JSONDecodeError:
            time.sleep(interval)
            continue

        rank_states = {}
        for m in status.get("mdsmap", []):
            rank = _parse_fs_status_rank(m.get("rank"))
            if rank is None:
                continue
            rank_states[rank] = m.get("state", "")

        bad = [
            (r, s)
            for r, s in rank_states.items()
            if r < max_mds and ("active" not in s or "damaged" in s or "failed" in s)
        ]
        if not bad:
            active_count = sum(
                1 for r, s in rank_states.items() if r < max_mds and "active" in s
            )
            if active_count >= max_mds:
                log.info("All %s MDS ranks active for %s", max_mds, fs_name)
                return True

        # Per-rank laggy timers: a single shared timer can false-fail when
        # different ranks are briefly laggy in sequence during normal failover.
        now = time.time()
        laggy_ranks = {
            r for r, s in rank_states.items() if r < max_mds and "laggy" in (s or "")
        }
        for r in list(laggy_since):
            if r not in laggy_ranks:
                del laggy_since[r]
        for r in laggy_ranks:
            laggy_since.setdefault(r, now)
            if now - laggy_since[r] >= laggy_fail_sec:
                log.error(
                    "Rank %s stuck laggy for >=%ss on %s (states=%s)",
                    r,
                    laggy_fail_sec,
                    fs_name,
                    rank_states,
                )
                return False

        log.info("Waiting for ranks active on %s (states=%s)", fs_name, rank_states)
        time.sleep(interval)
    return False


def _fail_on_inactive_ranks(client, attr_util, fs_name, context):
    """
    When ranks never return active, still run crash/damage oracles so an
    alternate_name replay crash is attributed instead of only a generic timeout.
    """
    log.error("Ranks did not return active (%s)", context)
    # Oracles intentionally run even after timeout — return False either way.
    _assert_post_fail_oracles(client, attr_util, fs_name, context)
    return False


def _assert_ok_after_failover(client, context=""):
    health, _ = client.exec_command(sudo=True, cmd="ceph health", check_ec=False)
    health = (health or "").strip()
    if "HEALTH_OK" in health:
        return True

    detail, _ = client.exec_command(sudo=True, cmd="ceph health detail", check_ec=False)
    detail = detail or ""
    for pattern in (
        "OSD_DOWN",
        "OSD_HOST_DOWN",
        "MDS_DAMAGE",
        "MDS_ALL_DOWN",
        "FS_DEGRADED",
        "FS_DAMAGED",
        "PG_UNAVAILABLE",
    ):
        if pattern in detail:
            log.error(
                "Unacceptable cluster health '%s' (%s)\n%s",
                pattern,
                context,
                detail,
            )
            return False

    if any(w in detail for w in _ACCEPTABLE_FAILOVER_HEALTH_WARNINGS):
        log.info("Acceptable HEALTH_WARN during failover (%s): %s", context, health)
        return True

    log.error("Unexpected cluster health after failover (%s): %s", context, detail)
    return False


def _assert_post_fail_oracles(client, attr_util, fs_name, context):
    if not attr_util.assert_no_damaged_mds_ranks(client, fs_name, context):
        return False
    if not attr_util.assert_no_mds_crashes(client, context=context):
        return False
    return _assert_ok_after_failover(client, context)


def _resolve_fail_ranks(config, max_mds):
    fail_ranks = config.get("fail_ranks")
    if fail_ranks:
        return [int(r) for r in fail_ranks]
    return list(range(max_mds))


def _mds_fail_rank(client, fs_name, rank):
    # Qualify with fs_name so multi-FS clusters do not fail the default FS rank.
    out, err, exit_code, _ = client.exec_command(
        sudo=True,
        cmd="ceph mds fail {}:{}".format(fs_name, rank),
        check_ec=False,
        verbose=True,
    )
    if exit_code != 0:
        log.error(
            "ceph mds fail %s:%s failed exit_code=%s stdout=%r stderr=%r",
            fs_name,
            rank,
            exit_code,
            out,
            err,
        )
        return False
    return True


def _flush_mds_journals(client, fs_name, max_mds):
    """Flush journal on every active rank; return False if any flush fails."""
    for rank in range(int(max_mds)):
        out, err, exit_code, _ = client.exec_command(
            sudo=True,
            cmd="ceph tell mds.{}:{} flush journal".format(fs_name, rank),
            check_ec=False,
            verbose=True,
            timeout=600,
        )
        if exit_code != 0:
            log.error(
                "flush journal failed rank=%s exit_code=%s stdout=%r stderr=%r",
                rank,
                exit_code,
                out,
                err,
            )
            return False
    return True


def _fail_all_ranks_and_assert(
    client, attr_util, fs_name, max_mds, post_fail_sleep_sec, oracle_prefix
):
    """
    Fail every active rank after a focused mutation so the auth MDS for the
    work directory is always forced through journal replay.
    """
    for rank in range(int(max_mds)):
        log.info("%s: failing rank %s", oracle_prefix, rank)
        if not _mds_fail_rank(client, fs_name, rank):
            return False
        time.sleep(post_fail_sleep_sec)
        if not _wait_for_fs_ranks_active(client, fs_name, max_mds):
            return _fail_on_inactive_ranks(
                client,
                attr_util,
                fs_name,
                "{}-r{}".format(oracle_prefix, rank),
            )
        if not _assert_post_fail_oracles(
            client, attr_util, fs_name, "{}-r{}".format(oracle_prefix, rank)
        ):
            return False
    return True


def _usecase_multimds_failover(
    ceph_cluster, client, fs_util, attr_util, common_util, config
):
    max_mds = int(config.get("max_mds", 4))
    fail_iterations = int(config.get("fail_iterations", 10))
    fail_ranks = _resolve_fail_ranks(config, max_mds)
    storm_warmup_sec = int(config.get("storm_warmup_sec", 180))
    post_fail_sleep_sec = int(config.get("post_fail_sleep_sec", 5))
    health_wait = int(config.get("health_wait", 300))
    subvol_group = config.get("subvol_group", "cs_mm_repro")
    subvol_name = config.get("subvol_name", "casefold_sv")
    mds_placement_count = int(config.get("mds_placement_count", 4))

    fs_name = "case-sensitivity-mm-failover"
    mounting_dir = "/mnt/cs_mm_failover_{}/".format(
        "".join(random.choice(string.ascii_lowercase) for _ in range(8))
    )
    stop_flag = "/tmp/casefold_storm_{}.stop".format(fs_name.replace("-", "_"))

    log.info(
        "Usecase 1: casefold subvolume + max_mds=%s + %s fail iterations ranks=%s",
        max_mds,
        fail_iterations,
        fail_ranks,
    )

    if common_util.wait_for_healthy_ceph(client, health_wait):
        log.error("Cluster not healthy before Usecase 1")
        return 1, fs_name, mounting_dir, stop_flag, None, None

    attr_util.archive_mds_crashes(client)

    host_count, mds_hosts = _mds_placement_hosts(ceph_cluster, mds_placement_count)
    client.exec_command(
        sudo=True,
        cmd='ceph fs volume create {} --placement="{} {}"'.format(
            fs_name, host_count, mds_hosts
        ),
    )
    client.exec_command(
        sudo=True, cmd="ceph fs set {} max_mds {}".format(fs_name, max_mds)
    )
    fs_util.wait_for_mds_process(client, fs_name)
    fs_util.set_and_validate_mds_standby_replay(client, fs_name, 1)

    ceph = Ceph(client)
    ceph.fs.sub_volume_group.create(fs_name, subvol_group)
    _remove_subvolume_if_exists(client, fs_name, subvol_name, subvol_group)
    ceph.fs.sub_volume.create(
        fs_name,
        subvol_name,
        **{
            "group-name": subvol_group,
            "casesensitive=": "false",
            "normalization=": "nfd",
        },
    )

    subvol_path = ceph.fs.sub_volume.getpath(
        fs_name, subvol_name, **{"group-name": subvol_group}
    ).strip()
    log.info("Casefold subvolume path: %s", subvol_path)

    if not _subvolume_is_case_insensitive(
        ceph.fs.sub_volume.info(fs_name, subvol_name, **{"group-name": subvol_group})
    ):
        log.error("Subvolume casesensitive flag is not false")
        return 1, fs_name, mounting_dir, stop_flag, subvol_group, subvol_name

    client.exec_command(sudo=True, cmd="mkdir -p {}".format(mounting_dir))
    client.exec_command(
        sudo=True,
        cmd="mountpoint -q {} && fusermount -u {} -z || true".format(
            mounting_dir, mounting_dir
        ),
        check_ec=False,
    )
    fs_util.fuse_mount(
        [client],
        mounting_dir,
        extra_params=" -r {} --client_fs {}".format(subvol_path, fs_name),
    )

    probe = os.path.join(mounting_dir.rstrip("/"), ".repro_probe")
    client.exec_command(sudo=True, cmd="echo ok > {!r}".format(probe))
    out, _ = client.exec_command(
        sudo=True, cmd="cat {!r}".format(probe), check_ec=False
    )
    if "ok" not in (out or "").strip():
        log.error("FUSE write probe failed on casefold mount (output=%r)", out)
        return 1, fs_name, mounting_dir, stop_flag, subvol_group, subvol_name

    attr_util.start_casefold_metadata_storm(client, mounting_dir, stop_flag)
    log.info("Metadata storm warmup for %s seconds", storm_warmup_sec)
    time.sleep(storm_warmup_sec)

    for iteration in range(1, fail_iterations + 1):
        rank_to_fail = fail_ranks[(iteration - 1) % len(fail_ranks)]
        log.info(
            "MDS fail iteration %s/%s (rank %s)",
            iteration,
            fail_iterations,
            rank_to_fail,
        )
        if not _mds_fail_rank(client, fs_name, rank_to_fail):
            return 1, fs_name, mounting_dir, stop_flag, subvol_group, subvol_name
        time.sleep(post_fail_sleep_sec)

        if not _wait_for_fs_ranks_active(client, fs_name, max_mds):
            _fail_on_inactive_ranks(
                client, attr_util, fs_name, "iteration-{}".format(iteration)
            )
            return 1, fs_name, mounting_dir, stop_flag, subvol_group, subvol_name

        if not _assert_post_fail_oracles(
            client, attr_util, fs_name, "iteration-{}".format(iteration)
        ):
            return 1, fs_name, mounting_dir, stop_flag, subvol_group, subvol_name

    log.info("Passed Usecase 1: multi-MDS casefold failover without MDS crashes")
    return 0, fs_name, mounting_dir, stop_flag, subvol_group, subvol_name


def _usecase_max_mds_scaleup(client, fs_util, attr_util, fs_name, max_mds):
    log.info("Usecase 2: max_mds scale-down to 1 then scale-up to %s", max_mds)

    if not fs_util.set_and_validate_max_mds(client, fs_name, 1):
        log.error("Failed to set max_mds=1")
        return 1

    fs_util.wait_for_stable_fs(client, "false", 120)
    time.sleep(15)

    if not attr_util.assert_no_damaged_mds_ranks(client, fs_name, "pre-scale-up"):
        return 1

    if not fs_util.set_and_validate_max_mds(client, fs_name, max_mds):
        log.error("Failed to scale max_mds to %s", max_mds)
        return 1

    if not _wait_for_fs_ranks_active(client, fs_name, max_mds, timeout=900):
        _fail_on_inactive_ranks(client, attr_util, fs_name, "post-scale-up")
        return 1

    if not _assert_post_fail_oracles(client, attr_util, fs_name, "post-scale-up"):
        return 1

    log.info("Passed Usecase 2: max_mds scale-up without damaged ranks")
    return 0


def _usecase_rm_foo_create_foo_failover(
    client, attr_util, fs_name, mounting_dir, config
):
    """
    Casefold ``rm Foo`` then ``create FOO``, then MDS fail (#72399 / IBMCEPH-21205).

    Cached null dentry may retain alternate_name ``Foo`` while the fullbit
    carries ``FOO``, hitting ``journal.cc`` alternate_name assert on replay.
    """
    max_mds = int(config.get("max_mds", 4))
    post_fail_sleep_sec = int(config.get("post_fail_sleep_sec", 5))
    iterations = int(config.get("foo_flip_iterations", 5))
    work_dir = os.path.join(mounting_dir.rstrip("/"), "casefold_foo_flip")

    log.info(
        "Usecase 3 (P0): rm Foo / create FOO + MDS fail (%s iterations)",
        iterations,
    )
    attr_util.archive_mds_crashes(client)
    client.exec_command(sudo=True, cmd="mkdir -p {!r}".format(work_dir))

    for iteration in range(1, iterations + 1):
        foo_path = os.path.join(work_dir, "Foo")
        foo_upper = os.path.join(work_dir, "FOO")
        client.exec_command(
            sudo=True,
            cmd="rm -f {f!r} {u!r}; echo flip > {f!r}".format(f=foo_path, u=foo_upper),
        )
        client.exec_command(sudo=True, cmd="rm -f {!r}".format(foo_path))
        client.exec_command(sudo=True, cmd="echo flip2 > {!r}".format(foo_upper))

        log.info(
            "Usecase 3 iteration %s/%s: failing all ranks after Foo→FOO",
            iteration,
            iterations,
        )
        if not _fail_all_ranks_and_assert(
            client,
            attr_util,
            fs_name,
            max_mds,
            post_fail_sleep_sec,
            "foo-flip-{}".format(iteration),
        ):
            return 1

    log.info("Passed Usecase 3: Foo→FOO casefold flip + MDS fail")
    return 0


def _usecase_journal_trim_recreate_failover(
    client, attr_util, fs_name, mounting_dir, config
):
    """
    Create → flush/trim journal → unlink → recreate → MDS fail (#72399 / IBMCEPH-21205).

    After the create event is aged out of the journal, unlink leaves a nullbit
    with empty alternate_name; recreate journals a non-empty alternate_name and
    replay asserts if the cached dentry is not updated.
    """
    max_mds = int(config.get("max_mds", 4))
    post_fail_sleep_sec = int(config.get("post_fail_sleep_sec", 5))
    iterations = int(config.get("trim_recreate_iterations", 3))
    flush_rounds = int(config.get("journal_flush_rounds", 8))
    filler_files = int(config.get("journal_filler_files", 200))
    work_dir = os.path.join(mounting_dir.rstrip("/"), "casefold_trim_recreate")
    prev_segments = None

    log.info(
        "Usecase 4 (P0): journal-trim recreate + MDS fail (%s iterations)",
        iterations,
    )
    attr_util.archive_mds_crashes(client)
    client.exec_command(sudo=True, cmd="mkdir -p {!r}".format(work_dir))

    # Shrink MDS log so flushes age create events out of the journal.
    out, _ = client.exec_command(
        sudo=True,
        cmd="ceph config get mds mds_log_max_segments",
        check_ec=False,
    )
    prev_segments = (out or "").strip() or None
    client.exec_command(
        sudo=True,
        cmd="ceph config set mds mds_log_max_segments 2",
        check_ec=False,
    )

    try:
        for iteration in range(1, iterations + 1):
            target = os.path.join(work_dir, "TrimName_{}".format(iteration))
            target_flip = os.path.join(work_dir, "trimname_{}".format(iteration))
            filler = os.path.join(work_dir, "filler_{}".format(iteration))

            client.exec_command(
                sudo=True,
                cmd="rm -f {t!r} {f!r}; echo orig > {t!r}".format(
                    t=target, f=target_flip
                ),
            )
            if not _flush_mds_journals(client, fs_name, max_mds):
                return 1

            # Generate journal pressure then flush repeatedly so the create
            # event is trimmed before the unlink is journaled alone.
            client.exec_command(
                sudo=True,
                cmd=(
                    "mkdir -p {d!r}; "
                    "for i in $(seq 1 {n}); do "
                    "echo z > {d!r}/f_$i; "
                    "done"
                ).format(d=filler, n=filler_files),
            )
            for _ in range(flush_rounds):
                if not _flush_mds_journals(client, fs_name, max_mds):
                    return 1
                time.sleep(1)

            client.exec_command(sudo=True, cmd="rm -f {!r}".format(target))
            # Do not flush after unlink — keep the nullbit in the journal for
            # recreate + failover (empty alternate_name vs later fullbit).
            client.exec_command(
                sudo=True, cmd="echo recreated > {!r}".format(target_flip)
            )

            log.info(
                "Usecase 4 iteration %s/%s: failing all ranks after trim/recreate",
                iteration,
                iterations,
            )
            if not _fail_all_ranks_and_assert(
                client,
                attr_util,
                fs_name,
                max_mds,
                post_fail_sleep_sec,
                "trim-recreate-{}".format(iteration),
            ):
                return 1
    finally:
        if prev_segments:
            client.exec_command(
                sudo=True,
                cmd="ceph config set mds mds_log_max_segments {}".format(prev_segments),
                check_ec=False,
            )
        else:
            client.exec_command(
                sudo=True,
                cmd="ceph config rm mds mds_log_max_segments",
                check_ec=False,
            )

    log.info("Passed Usecase 4: journal-trim recreate + MDS fail")
    return 0


def run(ceph_cluster, **kw):
    config = kw.get("config") or {}
    fs_name = "case-sensitivity-mm-failover"
    subvol_group = config.get("subvol_group", "cs_mm_repro")
    subvol_name = config.get("subvol_name", "casefold_sv")
    mounting_dir = None
    stop_flag = None
    client = None
    fs_util = None
    attr_util = None

    try:
        build = config.get("build", config.get("rhbuild"))
        max_mds = int(config.get("max_mds", 4))
        run_scaleup = config.get("run_max_mds_scaleup", True)
        run_p0_repros = config.get("run_alternate_name_replay_repros", True)

        fs_util = FsUtils(ceph_cluster, test_data=kw.get("test_data"))
        attr_util = CephFSAttributeUtilities(ceph_cluster)
        common_util = CephFSCommonUtils(ceph_cluster)

        clients = ceph_cluster.get_ceph_objects("client")
        if not clients:
            log.error("Requires at least 1 client node")
            return 1

        client = clients[0]
        fs_util.prepare_clients([client], build)
        fs_util.auth_list([client])

        rc, fs_name, mounting_dir, stop_flag, subvol_group, subvol_name = (
            _usecase_multimds_failover(
                ceph_cluster, client, fs_util, attr_util, common_util, config
            )
        )
        if rc != 0:
            return rc

        # Focused P0 paths need a quiet mount (no racing storm).
        if run_p0_repros:
            attr_util.stop_casefold_metadata_storm(client, stop_flag)
            time.sleep(2)

            rc = _usecase_rm_foo_create_foo_failover(
                client, attr_util, fs_name, mounting_dir, config
            )
            if rc != 0:
                return rc

            rc = _usecase_journal_trim_recreate_failover(
                client, attr_util, fs_name, mounting_dir, config
            )
            if rc != 0:
                return rc

            if run_scaleup:
                attr_util.start_casefold_metadata_storm(client, mounting_dir, stop_flag)
                time.sleep(int(config.get("storm_restart_warmup_sec", 30)))

        if run_scaleup:
            rc = _usecase_max_mds_scaleup(client, fs_util, attr_util, fs_name, max_mds)
            if rc != 0:
                return rc

        log.info(
            "TEST PASSED | max_mds=%s | fail_iterations=%s | ranks=%s | "
            "scaleup=%s | alternate_name_replay=%s",
            max_mds,
            config.get("fail_iterations", 10),
            _resolve_fail_ranks(config, max_mds),
            run_scaleup,
            run_p0_repros,
        )
        return 0

    except FsBaseException as exc:
        log.error("FsBaseException during multi-MDS casefold failover test: %s", exc)
        return log_and_fail(
            "FsBaseException during multi-MDS casefold failover test", exc
        )
    except Exception as exc:
        log.error("Unexpected error: %s", exc)
        return 1

    finally:
        if client and stop_flag and attr_util:
            try:
                attr_util.stop_casefold_metadata_storm(client, stop_flag)
            except Exception as exc:
                log.warning("Storm stop during cleanup: %s", exc)

        if client and mounting_dir:
            try:
                client.exec_command(
                    sudo=True,
                    cmd="fusermount -u {} -z".format(mounting_dir),
                    check_ec=False,
                )
                client.exec_command(
                    sudo=True, cmd="rm -rf {}".format(mounting_dir), check_ec=False
                )
            except Exception as exc:
                log.warning("Unmount cleanup: %s", exc)

        if client and fs_name and subvol_name and subvol_group:
            # Bound cleanup: a stuck MDS (replay/laggy) can hang subvolume rm
            # for the default 3600s and waste the suite (seen on 20.2.2-367).
            # Enable mon_allow_pool_delete like fs_util.remove_fs() so volume
            # removal can reclaim the data/meta pools.
            cleanup_timeout = 120
            try:
                client.exec_command(
                    sudo=True,
                    cmd=(
                        "ceph config set mon mon_allow_pool_delete true || true; "
                        "ceph fs subvolume rm {fs} {sv} {grp} --force || true; "
                        "ceph fs subvolumegroup rm {fs} {grp} --force || true; "
                        "ceph fs volume rm {fs} --yes-i-really-mean-it || true"
                    ).format(fs=fs_name, sv=subvol_name, grp=subvol_group),
                    check_ec=False,
                    timeout=cleanup_timeout,
                )
            except Exception as exc:
                log.warning("FS/subvolume cleanup: %s", exc)
