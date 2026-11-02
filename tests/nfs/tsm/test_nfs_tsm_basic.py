"""NFS TSM deployment workflows.

Each WORKFLOWS entry is a list of deploy steps. Each step:
  1. Build orch NFS spec
  2. Apply + assert_tsm_ready
  3. Enable TSM debug logs

Then: export + mount + IO, coredump check, cleanup (always in finally).

Step flags: expect_deploy_fail, recreate, min_nfs_nodes, second_cluster
"""

import time
import uuid

import yaml

from cli.ceph.ceph import Ceph
from cli.cephadm.cephadm import CephAdm
from cli.utilities.filesys import Mount
from tests.nfs.lib.common_lib import (
    enable_nfs_debug_logs,
    get_nfs_container_id,
    get_node_time,
)
from tests.nfs.lib.multi_active.config import NfsMultiActiveConfig
from tests.nfs.lib.tsm import NfsTsmValidation
from tests.nfs.lib.tsm.helpers import announce, check_coredumps, log_workflow_summary
from tests.nfs.nfs_operations import cleanup_cluster, create_nfs_via_file_and_verify
from utility.log import Log

log = Log(__name__)

TIMEOUT = 300

WORKFLOWS = {
    "basic": [{}],
    "placement_hosts": [{"placement": "hosts"}],
    "placement_count": [{"placement": "count", "nfs_count": 2, "min_nfs_nodes": 3}],
    "custom_ports": [{"nfs_port": 12049, "tsm_port": 36370}],
    "tsm_disabled": [
        {"enable_tsm": False, "expect_enabled": None, "check_peers": False}
    ],
    "tsm_enable_flip": [
        {"enable_tsm": False, "expect_enabled": None, "check_peers": False},
        {"enable_tsm": True, "expect_enabled": True, "check_peers": True},
    ],
    "redeploy_tsm_port": [
        {"tsm_port": 36369},
        {"tsm_port": 36371},
    ],
    "colocation": [
        {
            "second_cluster": {
                "name": "cephfs-nfs-tsmb-b",
                "nfs_port": 2050,
                "tsm_port": 36370,
                "export": "/export_tsmb_b",
                "mount": "/mnt/nfs_tsmb_b",
            }
        }
    ],
    "cleanup_recreate": [{"recreate": True}],
}


def build_spec(
    nfs_name,
    nfs_port,
    tsm_port,
    enable_tsm,
    count,
    hosts,
    placement,
    monitoring_port=None,
):
    """Build NFS orch spec (hosts = pinned list, count = pool with count < len)."""
    if placement == "count":
        if len(hosts) <= count:
            log.error(
                "placement count requires len(hosts) > count (hosts=%s, count=%s)",
                len(hosts),
                count,
            )
            return None
        place = {"count": count, "hosts": list(hosts)}
    else:
        place = {"count": count, "hosts": hosts[:count]}
    spec_body = {
        "port": int(nfs_port),
        "enable_tsm": bool(enable_tsm),
        "tsm_port": int(tsm_port),
    }
    if monitoring_port is not None:
        spec_body["monitoring_port"] = int(monitoring_port)
    return {
        "service_type": "nfs",
        "service_id": nfs_name,
        "placement": place,
        "spec": spec_body,
    }


def nodes_running_nfs(nodes, nfs_name):
    """Return nodes that currently host the NFS container."""
    return [n for n in nodes if get_nfs_container_id(n, nfs_name=nfs_name)]


def deploy_step(cluster, installer, client, nodes, step, nfs_name, tag):
    """Spec → apply → assert_tsm_ready → optionally enable TSM debug.

    Set ``step["enable_debug"]=False`` when the caller enables TSM (and other)
    components in a single combined logging path afterward.

    Returns (coredump_since, active, nfs_port, second) or None / 'expect_deploy_fail'.
    """
    count = step.get("nfs_count", 2)
    nfs_port = step.get("nfs_port", 2049)
    tsm_port = step.get("tsm_port", 36369)
    monitoring_port = step.get("monitoring_port")
    enable_tsm = step.get("enable_tsm", True)
    enable_debug = step.get("enable_debug", True)
    expect_enabled = step.get("expect_enabled", enable_tsm)
    check_peers = step.get("check_peers", count >= 2 and enable_tsm)
    placement = step.get("placement", "hosts")
    hosts = [n.hostname for n in nodes]
    pool = nodes if placement == "count" else nodes[:count]
    second = step.get("second_cluster")

    log_since, coredump_since = get_node_time(pool)

    specs = [
        build_spec(
            nfs_name,
            nfs_port,
            tsm_port,
            enable_tsm,
            count,
            hosts,
            placement,
            monitoring_port=monitoring_port,
        )
    ]
    if specs[0] is None:
        return None
    if second:
        second_spec = build_spec(
            second["name"],
            second["nfs_port"],
            second["tsm_port"],
            True,
            count,
            hosts[:count],
            "hosts",
            monitoring_port=second.get("monitoring_port", monitoring_port),
        )
        if second_spec is None:
            return None
        specs.append(second_spec)

    log.info("[%s] deploy %s", tag, specs)
    if step.get("expect_deploy_fail"):
        remote = f"/tmp/cephci_nfs_neg_{uuid.uuid4().hex}.yaml"
        try:
            fp = installer.remote_file(sudo=True, file_name=remote, file_mode="wb")
            fp.write(
                yaml.dump_all(
                    specs, sort_keys=False, indent=2, default_flow_style=False
                ).encode("utf-8")
            )
            fp.flush()
            fp.close()
            out, _ = installer.exec_command(sudo=True, cmd=f"cat {remote}")
            log.info(
                "[%s] NFS orch spec (expect apply fail):\n%s",
                tag,
                (out or "").strip(),
            )
            CephAdm(installer, mount="/tmp/").ceph.orch.apply(
                input=remote, check_ec=True
            )
            log.error("[%s] orch apply succeeded but expected to fail", tag)
            return None
        except Exception as exc:
            log.info("[%s] orch apply failed as expected: %s", tag, exc)
            return "expect_deploy_fail"
        finally:
            installer.exec_command(sudo=True, cmd=f"rm -f {remote}", check_ec=False)

    if not create_nfs_via_file_and_verify(installer, specs, TIMEOUT, nfs_nodes=pool):
        log.error("[%s] deploy failed", tag)
        return None

    # Ready checks before debug (debug restarts Ganesha).
    ready = [(nfs_name, tsm_port, expect_enabled, check_peers)]
    if second:
        ready.append((second["name"], second["tsm_port"], True, True))
    for svc, port, exp, peers in ready:
        if NfsTsmValidation(
            cluster, client, "enable_TSM", "Tsm_Port", port
        ).assert_tsm_ready(
            svc,
            pool,
            tsm_port=port,
            expect_enabled=exp,
            check_peers=peers,
            log_since=log_since,
        ):
            log.error("[%s] TSM ready check failed for %s", tag, svc)
            return None

    if enable_debug:
        for spec in specs:
            enable_nfs_debug_logs(client, spec["service_id"], "TSM")
    else:
        log.info(
            "[%s] skipping deploy-time TSM debug (caller enables combined logging)", tag
        )

    active = []
    for _ in range(30):
        active = nodes_running_nfs(pool, nfs_name)
        if len(active) >= count:
            break
        time.sleep(2)
    if len(active) < count:
        log.error(
            "[%s] NFS container not seen after deploy (seen=%s, expected=%s)",
            tag,
            [n.hostname for n in active],
            count,
        )
        return None

    return coredump_since, active[:count], nfs_port, second


def run_io(client, nfs_name, export, mount, nfs_port, server, tag, nfs_version="4.2"):
    """Export + mount + write/read. Returns 0 on success, 1 on failure."""
    try:
        Ceph(client).nfs.export.create(
            fs_name="cephfs", nfs_name=nfs_name, nfs_export=export, fs="cephfs"
        )
        NfsMultiActiveConfig.wait_until_export_visible(client, nfs_name, export)
        client.create_dirs(dir_path=mount, sudo=True)
        if Mount(client).nfs(
            mount=mount,
            version=str(nfs_version),
            port=str(nfs_port),
            server=server,
            export=export,
        ):
            log.error("[%s] Failed to mount nfs on %s", tag, client.hostname)
            return 1
        client.exec_command(
            sudo=True,
            cmd=f"bash -c 'echo {tag} > {mount}/{tag}.txt && cat {mount}/{tag}.txt'",
        )
        return 0
    except Exception as exc:
        log.error("[%s] IO failed: %s", tag, exc)
        return 1


def safe_cleanup(client, mount, nfs_name, export, nodes, steps=None):
    """Cleanup NFS cluster(s). Pass steps to also remove any second_cluster."""
    clusters = [(mount, nfs_name, export)]
    for step in steps or []:
        second = step.get("second_cluster")
        if second:
            clusters.append((second["mount"], second["name"], second["export"]))
    for mnt, name, exp in clusters:
        try:
            cleanup_cluster([client], mnt, name, exp, nfs_nodes=nodes)
        except Exception as exc:
            log.warning("[%s] cleanup: %s", name, exc)


def run_workflow(cluster, installer, client, all_nodes, name, steps, nfs_version="4.2"):
    """Deploy step(s) → IO → coredump → cleanup (always)."""
    min_nodes = max(s.get("min_nfs_nodes", 1) for s in steps)
    if len(all_nodes) < min_nodes:
        log.warning("[%s] skip (need %s NFS nodes)", name, min_nodes)
        return "skipped"

    need = max(max(s.get("nfs_count", 2), s.get("min_nfs_nodes", 1)) for s in steps)
    nodes = sorted(all_nodes, key=lambda n: n.hostname)[:need]
    nfs_name = f"cephfs-nfs-tsmb-{name}"
    export = f"/export_tsmb_{name}"
    mount = f"/mnt/nfs_tsmb_{name}"
    recreate = any(s.get("recreate") for s in steps)

    def once(tag):
        coredump_since, _ = get_node_time(nodes)
        active, nfs_port, second = nodes, 2049, None
        for i, step in enumerate(steps):
            result = deploy_step(
                cluster, installer, client, nodes, step, nfs_name, f"{tag}-step{i}"
            )
            if result is None:
                return 1
            if result == "expect_deploy_fail":
                # Negative path still must not hide a crash during the failed apply.
                return check_coredumps(nodes, coredump_since, tag)
            coredump_since, active, nfs_port, second = result

        io_targets = [
            (nfs_name, export, mount, nfs_port, tag),
        ]
        if second:
            io_targets.append(
                (
                    second["name"],
                    second["export"],
                    second["mount"],
                    second["nfs_port"],
                    f"{tag}_b",
                )
            )
        for svc, exp, mnt, port, io_tag in io_targets:
            if run_io(
                client,
                svc,
                exp,
                mnt,
                port,
                active[0].hostname,
                io_tag,
                nfs_version=nfs_version,
            ):
                return 1
        return check_coredumps(active, coredump_since, tag)

    try:
        if once(name):
            log.error("[%s] FAILED", name)
            return "failed"
        if recreate:
            safe_cleanup(client, mount, nfs_name, export, nodes, steps=steps)
            time.sleep(5)
            if once(f"{name}_recreate"):
                log.error("[%s] FAILED on recreate", name)
                return "failed"
        log.info("[%s] PASSED", name)
        return "passed"
    except Exception as exc:
        log.error("[%s] FAILED: %s", name, exc)
        return "failed"
    finally:
        safe_cleanup(client, mount, nfs_name, export, nodes, steps=steps)


def execute_workflows(
    ceph_cluster,
    workflows,
    summary_title="TSM WORKFLOW SUMMARY",
    handlers=None,
    nfs_version="4.2",
):
    """Run workflows; always print summary. Return 0 if none failed."""
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")
    installer = ceph_cluster.get_nodes("installer")[0]
    if not nfs_nodes or not clients:
        log.error("Need ≥1 NFS node and ≥1 client")
        return 1

    handlers = handlers or {}
    results = {}
    for i, (name, steps) in enumerate(workflows.items(), 1):
        log.info(
            "\n==============================\n"
            "Test %s: TSM workflow [%s]\n"
            "==============================",
            i,
            name,
        )
        announce("Do workflow [%s]" % name)
        try:
            runner = handlers.get(name, run_workflow)
            results[name] = runner(
                ceph_cluster,
                installer,
                clients[0],
                nfs_nodes,
                name,
                steps,
                nfs_version=nfs_version,
            )
        except Exception as exc:
            log.error("[%s] FAILED: %s", name, exc)
            results[name] = "failed"

    return log_workflow_summary(results, title=summary_title, name_width=24)


def run(ceph_cluster, **kw):
    config = kw.get("config") or {}
    return execute_workflows(
        ceph_cluster,
        WORKFLOWS,
        "TSM DEPLOYMENT WORKFLOW SUMMARY",
        nfs_version=str(config.get("nfs_version", "4.2")),
    )
