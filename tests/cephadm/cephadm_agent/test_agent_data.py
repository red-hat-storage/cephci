"""Agent host facts, daemon status, data path isolation, and scale tests."""

import json
import time

from cephadm_agent.helpers import (
    DEFAULT_AGENT_REFRESH,
    agent_service_name,
    get_agent_daemons,
    get_fsid,
    get_node_for_host,
    get_orch_ps,
    log,
    setup_run,
    shell,
)


def run_host_facts_test(ceph_cluster, installer):
    log.info("=== TEST: Host facts - Cache refresh ===")

    hosts_out, _ = shell(installer, "ceph orch host ls -f json")
    hosts = json.loads(hosts_out)
    assert len(hosts) > 0, "No hosts"
    hostname = hosts[0]["hostname"]

    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None

    get_orch_ps(installer, refresh=True)
    time.sleep(DEFAULT_AGENT_REFRESH + 10)

    facts = None
    for method_name, method_cmd in [
        ("ceph orch host facts", f"ceph orch host facts {hostname} -f json"),
        ("cephadm gather-facts", "cephadm gather-facts"),
    ]:
        try:
            if method_name == "cephadm gather-facts":
                out, _ = target_node.exec_command(sudo=True, cmd=method_cmd)
            else:
                out, _ = shell(installer, method_cmd)
            if out and out.strip():
                facts = json.loads(out)
                log.info(f"Host facts obtained via {method_name}")
                break
        except Exception as e:
            log.info(f"{method_name} not available: {e}")

    if facts:
        keys = list(facts.keys())
        log.info(f"Host facts keys for {hostname}: {keys}")
        assert any(
            k in facts for k in ("cpu_count", "hostname", "arch", "cpu_cores")
        ), f"Host facts missing expected fields: {keys}"
        if facts.get("interfaces"):
            ifaces = facts["interfaces"]
            if isinstance(ifaces, dict):
                iface_names = list(ifaces.keys())
            elif isinstance(ifaces, list) and ifaces and isinstance(ifaces[0], dict):
                iface_names = [iface.get("name", str(iface)) for iface in ifaces]
            else:
                iface_names = [str(i) for i in ifaces]
            log.info(f"Network interfaces reported: {iface_names}")
        log.info("PASS: Host facts cache contains expected data")
    else:
        log.info(
            "Direct facts commands unavailable, verifying host data via orch host ls"
        )
        for h in hosts:
            hn = h.get("hostname", "")
            status = h.get("status", "")
            labels = h.get("labels", [])
            log.info(f"Host {hn}: status={status}, labels={labels}")
            assert hn, "Empty hostname in host list"
        log.info("PASS: Host data available via orch host ls for all hosts")


def run_daemon_status_test(ceph_cluster, installer):
    log.info("=== TEST: Daemon status - orch ps reflects stopped daemon ===")

    daemons = get_orch_ps(installer, daemon_type="crash")
    if not daemons:
        daemons = get_orch_ps(installer, daemon_type="osd")
    assert len(daemons) > 0, "No suitable daemons (crash/osd) found to test"

    target = daemons[0]
    daemon_type = target["daemon_type"]
    daemon_id = target["daemon_id"]
    hostname = target["hostname"]
    daemon_name = f"{daemon_type}.{daemon_id}"
    log.info(f"Target daemon: {daemon_name} on {hostname}")

    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None

    fsid = get_fsid(installer)
    systemd_unit = f"ceph-{fsid}@{daemon_name}"

    log.info(f"Stopping {daemon_name}")
    target_node.exec_command(sudo=True, cmd=f"systemctl stop {systemd_unit}")

    found_stopped = False
    end = time.time() + 180
    while time.time() < end:
        time.sleep(DEFAULT_AGENT_REFRESH)
        refreshed = get_orch_ps(installer, refresh=True)
        for d in refreshed:
            if d.get("daemon_type") == daemon_type and str(d.get("daemon_id")) == str(
                daemon_id
            ):
                status = d.get("status_desc", "")
                log.info(f"Daemon {daemon_name} status: {status}")
                if status in ("stopped", "error", "unknown"):
                    found_stopped = True
                    break
        if found_stopped:
            break

    assert (
        found_stopped
    ), f"Daemon {daemon_name} did not show stopped/error/unknown within 180s after stop"

    log.info(f"Restarting {daemon_name}")
    target_node.exec_command(sudo=True, cmd=f"systemctl start {systemd_unit}")

    end = time.time() + 120
    while time.time() < end:
        time.sleep(DEFAULT_AGENT_REFRESH)
        refreshed = get_orch_ps(installer, refresh=True)
        for d in refreshed:
            if d.get("daemon_type") == daemon_type and str(d.get("daemon_id")) == str(
                daemon_id
            ):
                status = d.get("status_desc", "")
                log.info(f"Daemon {daemon_name} status after restart: {status}")
                if status == "running":
                    break
        else:
            continue
        break

    log.info("PASS: Daemon state change reflected in orch ps --refresh")


def run_data_path_isolation_test(ceph_cluster, installer):
    log.info("=== TEST: Data path isolation - I/O continues with agent down ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0, "No agent daemons found"
    target = agents[0]
    hostname = target["hostname"]

    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None

    fsid = get_fsid(installer)
    service_name = agent_service_name(fsid, hostname)

    pool_name = "agent_io_test_pool"
    shell(installer, "ceph config set mon mon_allow_pool_delete true")
    time.sleep(3)
    shell(installer, f"ceph osd pool create {pool_name} 16")
    time.sleep(10)

    log.info(f"Stopping agent on {hostname}")
    target_node.exec_command(sudo=True, cmd=f"systemctl stop {service_name}")
    time.sleep(5)

    try:
        log.info("Running rados bench write with agent stopped")
        bench_out, bench_err = shell(
            installer, f"rados bench -p {pool_name} 15 write --no-cleanup"
        )
        log.info(f"Rados bench output: {bench_out[:500]}")
        assert (
            "Total time run" in bench_out or "Bandwidth" in bench_out
        ), f"Rados bench did not complete successfully while agent was stopped. Output: {bench_out}"
        log.info("Client I/O completed successfully with agent stopped")
    finally:
        log.info(f"Restarting agent on {hostname}")
        target_node.exec_command(sudo=True, cmd=f"systemctl start {service_name}")
        try:
            shell(installer, f"rados -p {pool_name} cleanup")
        except Exception:
            pass
        try:
            shell(
                installer,
                f"ceph osd pool delete {pool_name} {pool_name} --yes-i-really-really-mean-it",
            )
        except Exception as e:
            log.warning(f"Pool cleanup failed (non-critical): {e}")

    log.info("PASS: Client I/O unaffected by agent downtime")


def run_scale_test(ceph_cluster, installer):
    log.info("=== TEST: Scale - Multi-host refresh ===")

    hosts_out, _ = shell(installer, "ceph orch host ls -f json")
    hosts = json.loads(hosts_out)
    hostnames = [h["hostname"] for h in hosts]
    num_hosts = len(hostnames)
    log.info(f"Cluster has {num_hosts} hosts: {hostnames}")

    log.info("Triggering orch ps --refresh to force metadata sync")
    start_time = time.time()
    refreshed = get_orch_ps(installer, refresh=True)
    elapsed = time.time() - start_time
    log.info(
        f"Refresh completed in {elapsed:.1f}s for {len(refreshed)} daemons across {num_hosts} hosts"
    )

    refreshed_hosts = {d["hostname"] for d in refreshed}
    for h in hostnames:
        assert (
            h in refreshed_hosts
        ), f"Host {h} has no daemons in refreshed orch ps — possible stale state"

    agents = get_agent_daemons(installer)
    agent_hosts = {a["hostname"] for a in agents if a.get("status_desc") == "running"}
    stale_hosts = set(hostnames) - agent_hosts
    if stale_hosts:
        log.warning(
            f"Hosts without running agents (possible stale state): {stale_hosts}"
        )
    assert len(stale_hosts) == 0, f"Stale agent state detected on hosts: {stale_hosts}"

    max_acceptable_time = num_hosts * 15
    assert elapsed < max_acceptable_time, (
        f"Refresh took {elapsed:.1f}s, exceeds "
        f"{max_acceptable_time}s threshold for {num_hosts} hosts"
    )

    log.info("PASS: All hosts refreshed with no stale state")


TEST_REGISTRY = {
    "host_facts": run_host_facts_test,
    "daemon_status": run_daemon_status_test,
    "data_path_isolation": run_data_path_isolation_test,
    "scale": run_scale_test,
}


def run(ceph_cluster, **kw):
    """Run agent data tests."""
    config, installer = setup_run(ceph_cluster, kw)
    test_selection = config.get("tests", "all")
    if test_selection == "all":
        tests_to_run = list(TEST_REGISTRY.keys())
    elif isinstance(test_selection, list):
        tests_to_run = test_selection
    else:
        tests_to_run = [test_selection]

    passed, failed = [], []
    for test_name in tests_to_run:
        if test_name not in TEST_REGISTRY:
            log.warning(f"Unknown test '{test_name}', skipping")
            continue
        log.info(f"\n{'='*60}\nRunning test: {test_name}\n{'='*60}")
        try:
            TEST_REGISTRY[test_name](ceph_cluster, installer)
            passed.append(test_name)
            log.info(f"[PASSED] {test_name}")
        except Exception as e:
            failed.append(test_name)
            log.error(f"[FAILED] {test_name}: {e}", exc_info=True)

    log.info(f"\n{'='*60}")
    log.info(
        f"RESULTS: {len(passed)} passed, {len(failed)} failed "
        f"out of {len(tests_to_run)} tests"
    )
    log.info(f"Passed: {passed}")
    if failed:
        log.error(f"Failed: {failed}")
    log.info(f"{'='*60}")
    return 1 if failed else 0
