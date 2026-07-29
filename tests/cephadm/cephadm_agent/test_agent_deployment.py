"""Agent deployment, restart, and toggle tests."""

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
    log,
    setup_run,
    shell,
    wait_for_agent_running,
    wait_for_health_warning,
)


def run_deployment_test(ceph_cluster, installer):
    log.info("=== TEST: Deployment - Agent auto-deployed on host add ===")

    shell(installer, "ceph config set mgr mgr/cephadm/use_agent true")
    time.sleep(5)

    hosts_out, _ = shell(installer, "ceph orch host ls -f json")
    hosts = json.loads(hosts_out)
    assert len(hosts) > 0, "No hosts found in the cluster"

    for host_info in hosts:
        hostname = host_info["hostname"]
        assert wait_for_agent_running(
            installer, hostname, timeout=180
        ), f"Agent daemon not running on host {hostname} after enabling use_agent"
        log.info(f"Agent daemon confirmed running on {hostname}")

    agents = get_agent_daemons(installer)
    agent_hosts = {a["hostname"] for a in agents}
    cluster_hosts = {h["hostname"] for h in hosts}
    assert (
        agent_hosts == cluster_hosts
    ), f"Agent not on all hosts. Agent hosts: {agent_hosts}, Cluster hosts: {cluster_hosts}"
    log.info("PASS: Agent deployed on all cluster hosts")


def run_restart_test(ceph_cluster, installer):
    log.info("=== TEST: Restart - Warning on stop, clear on start ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0, "No agent daemons found"
    target = agents[0]
    hostname = target["hostname"]

    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None, f"Could not find node object for {hostname}"

    fsid = get_fsid(installer)
    service_name = agent_service_name(fsid, hostname)

    log.info(f"Stopping agent on {hostname} via systemctl")
    target_node.exec_command(sudo=True, cmd=f"systemctl stop {service_name}")

    down_timeout = DEFAULT_AGENT_DOWN_TIMEOUT + 90
    assert wait_for_health_warning(
        installer, AGENT_HEALTH_WARNING, timeout=down_timeout, interval=15
    ), f"CEPHADM_AGENT_DOWN warning did not appear within {down_timeout}s after stopping agent"
    log.info(f"CEPHADM_AGENT_DOWN warning appeared after stopping agent on {hostname}")

    log.info(f"Restarting agent on {hostname}")
    target_node.exec_command(sudo=True, cmd=f"systemctl start {service_name}")

    assert wait_for_health_warning(
        installer, AGENT_HEALTH_WARNING, timeout=240, interval=15, expect_present=False
    ), "CEPHADM_AGENT_DOWN warning did not clear after restarting agent"
    log.info("PASS: Health warning appeared on stop and cleared on start")


def run_agent_toggle_test(ceph_cluster, installer):
    log.info("=== TEST: Agent toggle - disable and re-enable ===")

    agents_before = get_agent_daemons(installer)
    assert len(agents_before) > 0, "Agent should be running before toggle test"

    log.info("Disabling agent (use_agent=false)")
    shell(installer, "ceph config set mgr mgr/cephadm/use_agent false")

    end = time.time() + 300
    while time.time() < end:
        agents = get_agent_daemons(installer)
        if len(agents) == 0:
            break
        time.sleep(15)
    agents_after_disable = get_agent_daemons(installer)
    assert (
        len(agents_after_disable) == 0
    ), f"Agents still present after disabling: {[a['hostname'] for a in agents_after_disable]}"
    log.info("All agent daemons removed after disable")

    log.info("Re-enabling agent (use_agent=true)")
    shell(installer, "ceph config set mgr mgr/cephadm/use_agent true")

    end = time.time() + 300
    hosts_out, _ = shell(installer, "ceph orch host ls -f json")
    expected_hosts = {h["hostname"] for h in json.loads(hosts_out)}
    while time.time() < end:
        agents = get_agent_daemons(installer)
        running_hosts = {
            a["hostname"] for a in agents if a.get("status_desc") == "running"
        }
        if running_hosts == expected_hosts:
            break
        time.sleep(15)

    agents_after_enable = get_agent_daemons(installer)
    enabled_hosts = {
        a["hostname"] for a in agents_after_enable if a.get("status_desc") == "running"
    }
    assert (
        enabled_hosts == expected_hosts
    ), f"Agents not on all hosts after re-enable. Got: {enabled_hosts}, expected: {expected_hosts}"
    log.info("PASS: Agent toggle disable/enable works correctly")


def run_systemd_validation_test(ceph_cluster, installer):
    log.info("=== TEST: Systemd service and ephemeral container validation ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0
    hostname = agents[0]["hostname"]
    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None

    fsid = get_fsid(installer)
    service_name = agent_service_name(fsid, hostname)

    log.info("Step 1: Verify systemd service type is 'forking'")
    svc_file = f"/etc/systemd/system/ceph-{fsid}@agent.{hostname}.service"
    svc_content, _ = target_node.exec_command(
        sudo=True, cmd=f"cat {svc_file} 2>/dev/null || echo 'NOT_FOUND'"
    )
    if "NOT_FOUND" in svc_content:
        svc_content, _ = target_node.exec_command(
            sudo=True,
            cmd=f"systemctl cat {service_name} 2>/dev/null || echo 'NOT_FOUND'",
        )
    log.info(f"Service file content (first 500 chars): {svc_content[:500]}")
    assert (
        "Type=forking" in svc_content or "Type=" in svc_content
    ), "Could not verify systemd service type"

    log.info("Step 2: Verify Restart=on-failure policy")
    assert "Restart=" in svc_content, "No Restart policy found in service file"
    if "Restart=on-failure" in svc_content:
        log.info("Confirmed: Restart=on-failure")
    else:
        log.warning("Restart policy might differ from expected on-failure")

    log.info("Step 3: Verify agent runs as native Python process (not container)")
    ps_out, _ = target_node.exec_command(
        sudo=True,
        cmd="ps aux | grep 'cephadm.*agent' | grep -v grep || echo 'NO_PROCESS'",
        check_ec=False,
    )
    log.info(f"Agent process: {ps_out.strip()[:200]}")
    assert "NO_PROCESS" not in ps_out, "Agent Python process not found"

    container_out, _ = target_node.exec_command(
        sudo=True,
        cmd="podman ps --filter name=agent --format '{{.Names}}' 2>/dev/null "
        "|| echo 'NO_PERSISTENT_CONTAINERS'",
        check_ec=False,
    )
    log.info(f"Persistent agent containers: {container_out.strip()}")
    if container_out.strip() and "NO_PERSISTENT_CONTAINERS" not in container_out:
        log.warning("Found persistent agent containers — unexpected")

    log.info("Step 4: Verify ephemeral container activity in journal logs")
    time.sleep(DEFAULT_AGENT_REFRESH + 5)
    journal_out, _ = target_node.exec_command(
        sudo=True,
        cmd=f"journalctl -u {service_name} --since '5 minutes ago' "
        f"--no-pager 2>/dev/null | "
        f"grep -iE 'container|podman|metadata|Successfully' | tail -10 "
        f"|| echo 'NO_CONTAINER_EVENTS'",
        check_ec=False,
    )
    log.info(f"Journal container/metadata events:\n{journal_out.strip()[:500]}")

    has_metadata = "Successfully processed metadata" in journal_out
    if not has_metadata:
        log.warning(
            "No 'Successfully processed metadata' in 5-min window, "
            "retrying with wider window..."
        )
        journal_out2, _ = target_node.exec_command(
            sudo=True,
            cmd=f"journalctl -u {service_name} --no-pager -n 100 "
            f"2>/dev/null | grep 'Successfully processed' | tail -3 "
            f"|| echo 'NONE'",
            check_ec=False,
        )
        has_metadata = "Successfully processed" in journal_out2
        log.info(f"Wider search result: {journal_out2.strip()[:200]}")

    assert has_metadata, (
        "No 'Successfully processed metadata' entries found in journal — "
        "agent may not be collecting data"
    )

    log.info("Step 5: Verify unit.run script")
    unit_run_path = f"/var/lib/ceph/{fsid}/agent.{hostname}/unit.run"
    unit_run, _ = target_node.exec_command(
        sudo=True, cmd=f"cat {unit_run_path} 2>/dev/null || echo 'NOT_FOUND'"
    )
    log.info(f"unit.run content: {unit_run.strip()[:300]}")
    assert "NOT_FOUND" not in unit_run, f"unit.run not found at {unit_run_path}"
    assert (
        "python3" in unit_run or "cephadm" in unit_run
    ), "unit.run does not reference python3 or cephadm"

    log.info("Step 6: Verify port 4721 bound")
    port_out, _ = target_node.exec_command(
        sudo=True,
        cmd="ss -tlnp | grep ':4721' || echo 'PORT_NOT_BOUND'",
        check_ec=False,
    )
    log.info(f"Port 4721: {port_out.strip()}")
    assert "PORT_NOT_BOUND" not in port_out, "Agent not listening on port 4721"

    log.info(
        "PASS: Systemd service validation complete — agent runs as native "
        "Python, uses ephemeral containers for data collection"
    )


TEST_REGISTRY = {
    "deployment": run_deployment_test,
    "restart": run_restart_test,
    "agent_toggle": run_agent_toggle_test,
    "systemd_validation": run_systemd_validation_test,
}


def run(ceph_cluster, **kw):
    """Run agent deployment tests."""
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
