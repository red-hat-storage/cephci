"""Agent communication, MGR failover, and network partition tests."""

import json
import time

from cephadm_agent.helpers import (
    AGENT_HEALTH_WARNING,
    DEFAULT_AGENT_DOWN_TIMEOUT,
    DEFAULT_AGENT_REFRESH,
    agent_service_name,
    get_agent_daemons,
    get_fsid,
    get_node_for_host,
    get_orch_ps,
    log,
    setup_run,
    shell,
    wait_for_agent_running,
    wait_for_health_warning,
)


def run_communication_test(ceph_cluster, installer):
    log.info("=== TEST: Communication - Block host → mgr ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0, "No agent daemons found"
    target = agents[0]
    hostname = target["hostname"]

    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None, f"Node {hostname} not found"

    fsid = get_fsid(installer)
    service_name = agent_service_name(fsid, hostname)

    mgr_ip_out, _ = shell(installer, "ceph mgr dump -f json")
    mgr_dump = json.loads(mgr_ip_out)
    active_addr = mgr_dump.get("active_addr", "")
    mgr_ip = active_addr.split(":")[0].strip("/").strip()

    log.info(f"Blocking traffic from {hostname} to mgr at {mgr_ip}")
    target_node.exec_command(
        sudo=True,
        cmd=f"iptables -A OUTPUT -d {mgr_ip} -p tcp --dport 7150:7300 -j DROP",
    )

    try:
        down_timeout = DEFAULT_AGENT_DOWN_TIMEOUT + 90
        assert wait_for_health_warning(
            installer, AGENT_HEALTH_WARNING, timeout=down_timeout, interval=15
        ), f"CEPHADM_AGENT_DOWN not raised within {down_timeout}s after blocking traffic"
        log.info("CEPHADM_AGENT_DOWN detected after blocking traffic")
    finally:
        log.info(f"Unblocking traffic from {hostname} to {mgr_ip}")
        target_node.exec_command(
            sudo=True,
            cmd=f"iptables -D OUTPUT -d {mgr_ip} -p tcp --dport 7150:7300 -j DROP",
        )

    log.info("Restarting agent to force reconnection after unblock")
    target_node.exec_command(sudo=True, cmd=f"systemctl restart {service_name}")

    assert wait_for_health_warning(
        installer, AGENT_HEALTH_WARNING, timeout=300, interval=15, expect_present=False
    ), "CEPHADM_AGENT_DOWN did not clear after unblocking traffic"
    log.info("PASS: Communication failure detected and recovered")


def run_mgr_failover_test(ceph_cluster, installer):
    log.info("=== TEST: Mgr failover - Agent reconnects ===")

    mgr_ps = get_orch_ps(installer, daemon_type="mgr")
    running_mgrs = [m for m in mgr_ps if m.get("status_desc") == "running"]
    if len(running_mgrs) < 2:
        log.info(
            f"Only {len(running_mgrs)} mgr(s) running, need at least 2 for failover test"
        )
        log.info("Deploying standby mgr via 'ceph orch apply mgr --placement=2'")
        shell(installer, "ceph orch apply mgr --placement=2")
        end = time.time() + 300
        while time.time() < end:
            mgr_ps = get_orch_ps(installer, daemon_type="mgr")
            running_mgrs = [m for m in mgr_ps if m.get("status_desc") == "running"]
            if len(running_mgrs) >= 2:
                break
            time.sleep(15)
        assert (
            len(running_mgrs) >= 2
        ), f"Could not get 2 mgrs running within timeout. Got {len(running_mgrs)}"
        log.info(f"Now have {len(running_mgrs)} running mgrs")

    mgr_out, _ = shell(installer, "ceph mgr dump -f json")
    mgr_dump = json.loads(mgr_out)
    active_mgr = mgr_dump.get("active_name", "")
    assert active_mgr, "Could not determine active mgr"
    log.info(f"Active mgr before failover: {active_mgr}")

    shell(installer, f"ceph mgr fail {active_mgr}")
    log.info(f"Triggered failover of mgr.{active_mgr}")

    end = time.time() + 60
    new_active = ""
    while time.time() < end:
        try:
            mgr_out, _ = shell(installer, "ceph mgr dump -f json")
            mgr_dump = json.loads(mgr_out)
            new_active = mgr_dump.get("active_name", "")
            if new_active and new_active != active_mgr:
                break
        except Exception:
            pass
        time.sleep(5)

    assert new_active, "No active mgr after failover"
    assert new_active != active_mgr, f"Active mgr did not change: still {active_mgr}"
    log.info(f"New active mgr: {new_active}")

    time.sleep(DEFAULT_AGENT_REFRESH * 3)

    agents = get_agent_daemons(installer)
    running_agents = [a for a in agents if a.get("status_desc") == "running"]
    assert len(running_agents) > 0, "No agents running after mgr failover"

    no_agent_down = wait_for_health_warning(
        installer, AGENT_HEALTH_WARNING, timeout=180, interval=15, expect_present=False
    )
    assert (
        no_agent_down
    ), "CEPHADM_AGENT_DOWN raised after mgr failover — agents failed to reconnect"
    log.info("PASS: Agents reconnected to new active mgr after failover")


def run_network_partition_test(ceph_cluster, installer):
    log.info("=== TEST: Network partition — REJECT vs DROP ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0
    hostname = agents[0]["hostname"]
    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None

    fsid = get_fsid(installer)
    agent_dir = f"/var/lib/ceph/{fsid}/agent.{hostname}"
    service_name = agent_service_name(fsid, hostname)

    config_out, _ = target_node.exec_command(
        sudo=True, cmd=f"cat {agent_dir}/agent.json"
    )
    config = json.loads(config_out)
    mgr_ip = config["target_ip"]
    mgr_port = config["target_port"]
    log.info(f"MGR endpoint: {mgr_ip}:{mgr_port}")

    # Test REJECT
    log.info("Applying iptables REJECT rule...")
    target_node.exec_command(
        sudo=True,
        cmd=f"iptables -A OUTPUT -d {mgr_ip} -p tcp --dport {mgr_port} -j REJECT",
    )
    time.sleep(DEFAULT_AGENT_REFRESH + 5)

    journal_out, _ = target_node.exec_command(
        sudo=True,
        cmd=f"journalctl -u {service_name} --since '30 seconds ago' --no-pager 2>/dev/null | "
        f"grep -iE 'refused|error|failed' | tail -3 || echo 'NO_ERRORS'",
        check_ec=False,
    )
    log.info(f"REJECT behavior: {journal_out.strip()[:200]}")
    assert "NO_ERRORS" not in journal_out, "Agent should detect REJECT errors"

    target_node.exec_command(
        sudo=True,
        cmd=f"iptables -D OUTPUT -d {mgr_ip} -p tcp --dport {mgr_port} -j REJECT",
    )
    time.sleep(DEFAULT_AGENT_REFRESH + 5)

    journal_recovery, _ = target_node.exec_command(
        sudo=True,
        cmd=f"journalctl -u {service_name} --since '25 seconds ago' --no-pager 2>/dev/null | "
        f"grep -i 'Successfully processed' | tail -1 || echo 'NO_RECOVERY'",
        check_ec=False,
    )
    assert (
        "NO_RECOVERY" not in journal_recovery
    ), "Agent should recover after REJECT rule is removed"
    log.info("Agent recovered from REJECT partition. Good.")

    # Test DROP
    log.info("Applying iptables DROP rule...")
    target_node.exec_command(
        sudo=True,
        cmd=f"iptables -A OUTPUT -d {mgr_ip} -p tcp --dport {mgr_port} -j DROP",
    )
    time.sleep(35)

    journal_drop, _ = target_node.exec_command(
        sudo=True,
        cmd=f"journalctl -u {service_name} --since '40 seconds ago' --no-pager 2>/dev/null | "
        f"grep -iE 'timed out|timeout|error' | tail -3 || echo 'NO_TIMEOUT'",
        check_ec=False,
    )
    log.info(f"DROP behavior: {journal_drop.strip()[:200]}")

    target_node.exec_command(
        sudo=True,
        cmd=f"iptables -D OUTPUT -d {mgr_ip} -p tcp --dport {mgr_port} -j DROP",
    )
    time.sleep(DEFAULT_AGENT_REFRESH + 10)

    assert wait_for_agent_running(
        installer, hostname, timeout=120
    ), "Agent did not recover after DROP rule removal"
    wait_for_health_warning(
        installer, AGENT_HEALTH_WARNING, timeout=180, expect_present=False
    )
    log.info("PASS: Network partition test completed")


def run_multi_agent_failure_test(ceph_cluster, installer):
    log.info("=== TEST: Multiple agent failures - cluster remains operational ===")

    agents = get_agent_daemons(installer)
    assert (
        len(agents) >= 2
    ), f"Need at least 2 agents for multi-failure test, got {len(agents)}"

    fsid = get_fsid(installer)
    targets = agents[:2]
    stopped_hosts = []

    log.info(f"Stopping agents on {len(targets)} hosts simultaneously")
    for agent in targets:
        hostname = agent["hostname"]
        target_node = get_node_for_host(ceph_cluster, hostname)
        if target_node is None:
            continue
        service_name = agent_service_name(fsid, hostname)
        target_node.exec_command(sudo=True, cmd=f"systemctl stop {service_name}")
        stopped_hosts.append((hostname, target_node, service_name))
        log.info(f"Stopped agent on {hostname}")

    assert len(stopped_hosts) >= 2, "Could not stop agents on enough hosts"
    time.sleep(10)

    log.info("Verifying cluster remains operational with agents down")
    health_out, _ = shell(installer, "ceph -s -f json")
    health_data = json.loads(health_out)
    cluster_status = health_data.get("health", {}).get("status", "UNKNOWN")
    log.info(f"Cluster health with agents stopped: {cluster_status}")
    assert cluster_status != "HEALTH_ERR", (
        "Cluster went to HEALTH_ERR with agents stopped — "
        "agent failure should not break the cluster"
    )

    remaining_agents = get_agent_daemons(installer)
    remaining_running = [
        a
        for a in remaining_agents
        if a.get("status_desc") == "running"
        and a["hostname"] not in [h for h, _, _ in stopped_hosts]
    ]
    log.info(f"Remaining running agents: {len(remaining_running)}")

    log.info("Verifying CEPHADM_AGENT_DOWN warning appears")
    warned = wait_for_health_warning(
        installer,
        AGENT_HEALTH_WARNING,
        timeout=DEFAULT_AGENT_DOWN_TIMEOUT + 60,
        interval=10,
        expect_present=True,
    )
    if warned:
        log.info("CEPHADM_AGENT_DOWN raised for multiple stopped agents")
    else:
        log.warning("CEPHADM_AGENT_DOWN not raised within expected time")

    log.info("Restarting all stopped agents")
    for hostname, target_node, service_name in stopped_hosts:
        target_node.exec_command(
            sudo=True, cmd=f"systemctl start {service_name}", check_ec=False
        )
        log.info(f"Started agent on {hostname}")

    log.info("Waiting for all agents to recover")
    for hostname, _, _ in stopped_hosts:
        recovered = wait_for_agent_running(installer, hostname, timeout=120)
        assert recovered, f"Agent on {hostname} did not recover after restart"
        log.info(f"Agent on {hostname} recovered")

    wait_for_health_warning(
        installer,
        AGENT_HEALTH_WARNING,
        timeout=180,
        expect_present=False,
    )
    log.info("PASS: Cluster remained operational during multiple agent failures")


TEST_REGISTRY = {
    "communication": run_communication_test,
    "mgr_failover": run_mgr_failover_test,
    "network_partition": run_network_partition_test,
    "multi_agent_failure": run_multi_agent_failure_test,
}


def run(ceph_cluster, **kw):
    """Run agent communication tests."""
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
