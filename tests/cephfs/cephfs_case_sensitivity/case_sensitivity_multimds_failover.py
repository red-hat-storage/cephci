"""
IBMCEPH-16942 / tracker #72399 — casefold subvolume under multi-active MDS.

Regression coverage for MDS journal replay and rejoin failures involving
``alternate_name`` and subtree authority after failover during case-insensitive
metadata workloads.
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


def _wait_for_fs_ranks_active(client, fs_name, max_mds, timeout=600, interval=10):
    max_mds = int(max_mds)
    end = time.time() + timeout
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

        log.info("Waiting for ranks active on %s (states=%s)", fs_name, rank_states)
        time.sleep(interval)
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
        out, err, exit_code, _ = client.exec_command(
            sudo=True,
            cmd="ceph mds fail {}".format(rank_to_fail),
            check_ec=False,
            verbose=True,
        )
        if exit_code != 0:
            log.error(
                "ceph mds fail rank %s failed exit_code=%s stdout=%r stderr=%r",
                rank_to_fail,
                exit_code,
                out,
                err,
            )
            return 1, fs_name, mounting_dir, stop_flag, subvol_group, subvol_name
        time.sleep(post_fail_sleep_sec)

        if not _wait_for_fs_ranks_active(client, fs_name, max_mds):
            log.error("Ranks did not return active after fail iteration %s", iteration)
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
        log.error("Ranks not active after max_mds scale-up to %s", max_mds)
        return 1

    if not _assert_post_fail_oracles(client, attr_util, fs_name, "post-scale-up"):
        return 1

    log.info("Passed Usecase 2: max_mds scale-up without damaged ranks")
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

        if run_scaleup:
            rc = _usecase_max_mds_scaleup(client, fs_util, attr_util, fs_name, max_mds)
            if rc != 0:
                return rc

        log.info(
            "TEST PASSED | max_mds=%s | fail_iterations=%s | ranks=%s | scaleup=%s",
            max_mds,
            config.get("fail_iterations", 10),
            _resolve_fail_ranks(config, max_mds),
            run_scaleup,
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

        if client and fs_name and subvol_name and subvol_group and fs_util:
            try:
                Ceph(client).fs.sub_volume.rm(
                    fs_name, subvol_name, group=subvol_group, force=True
                )
                Ceph(client).fs.sub_volume_group.rm(fs_name, subvol_group)
                fs_util.remove_fs(client, fs_name, validate=False)
            except Exception as exc:
                log.warning("FS/subvolume cleanup: %s", exc)
