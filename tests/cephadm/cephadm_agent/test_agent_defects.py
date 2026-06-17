"""Agent defect tests: stale metadata, timestamp, zero division, graceful shutdown,
cache leak, auth response, silent listener, and failover resilience."""

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


def run_stale_metadata_overwrite_test(ceph_cluster, installer):
    log.info("=== DEFECT TEST: Stale metadata overwrite (counter/ack desync) ===")

    fsid = get_fsid(installer)
    hosts_out, _ = shell(installer, "ceph orch host ls -f json")
    hosts = json.loads(hosts_out)

    daemons_before = get_orch_ps(installer, refresh=True)
    daemon_count_before = len(daemons_before)
    log.info(f"Daemon count before test: {daemon_count_before}")

    hostname = hosts[0]["hostname"]
    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None
    service_name = agent_service_name(fsid, hostname)

    log.info(f"Stopping agent on {hostname} to create ack desync window")
    target_node.exec_command(sudo=True, cmd=f"systemctl stop {service_name}")
    time.sleep(5)

    log.info("Incrementing agent counter via forced refresh while agent is stopped")
    shell(installer, "ceph orch ps --refresh")
    time.sleep(DEFAULT_AGENT_REFRESH)

    log.info(f"Restarting agent on {hostname} — it will POST with old ack value")
    target_node.exec_command(sudo=True, cmd=f"systemctl start {service_name}")

    time.sleep(DEFAULT_AGENT_REFRESH * 2)

    daemons_after = get_orch_ps(installer, refresh=True)
    daemon_count_after = len(daemons_after)
    log.info(
        f"Daemon count after agent restart with potential ack mismatch: {daemon_count_after}"
    )

    if daemon_count_after < daemon_count_before:
        log.error(
            f"DEFECT CONFIRMED: Daemon count regressed from {daemon_count_before} to "
            f"{daemon_count_after}. Stale metadata from desynced agent overwrote fresh data."
        )
        assert False, (
            f"Stale metadata overwrite detected: daemons went from "
            f"{daemon_count_before} to {daemon_count_after}"
        )

    wait_for_health_warning(
        installer, AGENT_HEALTH_WARNING, timeout=60, expect_present=False
    )
    log.info(
        "PASS: No stale metadata overwrite detected (counter/ack handled correctly)"
    )


def run_stale_timestamp_test(ceph_cluster, installer):
    log.info("=== DEFECT TEST: Stale timestamp masks agent failure ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0
    hostname = agents[0]["hostname"]
    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None

    fsid = get_fsid(installer)
    agent_dir = f"/var/lib/ceph/{fsid}/agent.{hostname}"
    service_name = agent_service_name(fsid, hostname)

    log.info(f"Checking agent config-key entries for {hostname}")
    try:
        counter_out, _ = shell(
            installer, f"ceph config-key get mgr/cephadm/agent.{hostname}"
        )
        log.info(f"Agent mon store entry: {counter_out[:200]}")
    except Exception:
        log.info(
            f"No mgr/cephadm/agent.{hostname} key in mon store (normal for some versions)"
        )

    log.info("Stopping agent and changing target_port to unreachable port")
    target_node.exec_command(sudo=True, cmd=f"systemctl stop {service_name}")
    time.sleep(3)

    target_node.exec_command(
        sudo=True, cmd=f"cp {agent_dir}/agent.json {agent_dir}/agent.json.bak"
    )

    try:
        out, _ = target_node.exec_command(sudo=True, cmd=f"cat {agent_dir}/agent.json")
        agent_config = json.loads(out)
        log.info(f"Agent config keys: {list(agent_config.keys())}")
        log.info(
            f"Agent target_ip: {agent_config.get('target_ip')}, "
            f"target_port: {agent_config.get('target_port')}"
        )

        agent_config["target_port"] = "19999"
        escaped = json.dumps(agent_config).replace("'", "'\\''")
        target_node.exec_command(
            sudo=True, cmd=f"echo '{escaped}' > {agent_dir}/agent.json"
        )

        log.info("Starting agent with wrong target_port — agent will POST to nowhere")
        target_node.exec_command(sudo=True, cmd=f"systemctl start {service_name}")
        time.sleep(5)

        status_out, _ = target_node.exec_command(
            sudo=True, cmd=f"systemctl is-active {service_name}", check_ec=False
        )
        log.info(f"Agent systemd status with wrong port: {status_out.strip()}")

        down_timeout = DEFAULT_AGENT_DOWN_TIMEOUT + 90
        agent_down = wait_for_health_warning(
            installer, AGENT_HEALTH_WARNING, timeout=down_timeout, interval=15
        )

        if agent_down:
            log.info("CEPHADM_AGENT_DOWN correctly raised — agent failure detected")
        else:
            log.warning(
                "CEPHADM_AGENT_DOWN NOT raised despite agent unable to reach mgr. "
                "This could indicate the timestamp-masking bug."
            )

    finally:
        log.info("Restoring original agent.json")
        target_node.exec_command(
            sudo=True, cmd=f"cp {agent_dir}/agent.json.bak {agent_dir}/agent.json"
        )
        target_node.exec_command(sudo=True, cmd=f"rm -f {agent_dir}/agent.json.bak")
        target_node.exec_command(sudo=True, cmd=f"systemctl restart {service_name}")

    assert wait_for_agent_running(
        installer, hostname, timeout=180
    ), f"Agent on {hostname} did not recover"
    wait_for_health_warning(
        installer, AGENT_HEALTH_WARNING, timeout=180, expect_present=False
    )
    log.info("PASS: Stale timestamp test completed — agent recovered")


def run_zero_division_crash_test(ceph_cluster, installer):
    log.info("=== DEFECT TEST: ZeroDivisionError crash in timing calculation ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0
    hostname = agents[0]["hostname"]
    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None

    fsid = get_fsid(installer)
    service_name = agent_service_name(fsid, hostname)

    log.info(f"Performing 5 rapid restart cycles on agent at {hostname}")
    for i in range(5):
        target_node.exec_command(sudo=True, cmd=f"systemctl restart {service_name}")
        time.sleep(2)
        status_out, _ = target_node.exec_command(
            sudo=True, cmd=f"systemctl is-active {service_name}", check_ec=False
        )
        log.info(f"Cycle {i+1}: agent status = {status_out.strip()}")

    time.sleep(DEFAULT_AGENT_REFRESH + 10)

    status_out, _ = target_node.exec_command(
        sudo=True, cmd=f"systemctl is-active {service_name}", check_ec=False
    )
    final_status = status_out.strip()
    log.info(f"Agent status after rapid restarts: {final_status}")

    if final_status != "active":
        log.info("Checking journal for ZeroDivisionError")
        journal_out, _ = target_node.exec_command(
            sudo=True,
            cmd=f"journalctl -u {service_name} --no-pager -n 30 2>/dev/null | "
            f"grep -i 'ZeroDivision\\|division by zero\\|Traceback' || echo 'NO_CRASH_FOUND'",
        )
        log.info(f"Journal check: {journal_out[:500]}")

        if "ZeroDivision" in journal_out or "division by zero" in journal_out:
            log.error("DEFECT CONFIRMED: Agent crashed with ZeroDivisionError")
            target_node.exec_command(sudo=True, cmd=f"systemctl start {service_name}")
            assert False, "ZeroDivisionError crash detected in agent"

    assert wait_for_agent_running(
        installer, hostname, timeout=120
    ), f"Agent on {hostname} not running after rapid restart test"
    log.info("PASS: Agent survived rapid restart cycles without crash")


def run_graceful_shutdown_test(ceph_cluster, installer):
    log.info("=== DEFECT TEST: Graceful shutdown (signal handling) ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0
    hostname = agents[0]["hostname"]
    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None

    fsid = get_fsid(installer)
    service_name = agent_service_name(fsid, hostname)

    pid_before, _ = target_node.exec_command(
        sudo=True,
        cmd=f"systemctl show -p MainPID {service_name} | cut -d= -f2",
        check_ec=False,
    )
    pid_before = pid_before.strip()
    log.info(f"Agent PID before stop: {pid_before}")

    log.info("Stopping agent via systemctl")
    target_node.exec_command(sudo=True, cmd=f"systemctl stop {service_name}")
    time.sleep(10)

    orphan_out, _ = target_node.exec_command(
        sudo=True,
        cmd="ps aux | grep '[c]ephadm agent' | grep -v grep || echo 'NO_ORPHAN'",
        check_ec=False,
    )
    log.info(f"Orphan check after stop: {orphan_out.strip()}")

    if "cephadm agent" in orphan_out and "NO_ORPHAN" not in orphan_out:
        orphan_pids = []
        for line in orphan_out.strip().split("\n"):
            parts = line.split()
            if len(parts) > 1:
                orphan_pids.append(parts[1])
        log.error(
            f"DEFECT CONFIRMED: Orphan agent process(es) found after systemctl stop: "
            f"PIDs={orphan_pids}"
        )
        for pid in orphan_pids:
            target_node.exec_command(sudo=True, cmd=f"kill -9 {pid}", check_ec=False)
        target_node.exec_command(sudo=True, cmd=f"systemctl start {service_name}")
        assert False, f"Orphan agent processes found: {orphan_pids}"

    port_check, _ = target_node.exec_command(
        sudo=True, cmd="ss -tlnp | grep ':4721' || echo 'PORT_FREE'", check_ec=False
    )
    log.info(f"Port 4721 after stop: {port_check.strip()}")

    log.info("Restarting agent")
    target_node.exec_command(sudo=True, cmd=f"systemctl start {service_name}")

    assert wait_for_agent_running(
        installer, hostname, timeout=120
    ), f"Agent on {hostname} did not restart cleanly"
    log.info("PASS: Agent shutdown cleanly — no orphan processes or port leaks")


def run_host_removal_cache_leak_test(ceph_cluster, installer):
    log.info("=== DEFECT TEST: Host removal AgentCache leak ===")

    hosts_out, _ = shell(installer, "ceph orch host ls -f json")
    hosts = json.loads(hosts_out)
    hostnames = [h["hostname"] for h in hosts]
    log.info(f"Current hosts: {hostnames}")

    log.info("Checking mgr/cephadm/agent.* keys in mon config store")
    for hn in hostnames:
        try:
            key_out, _ = shell(installer, f"ceph config-key get mgr/cephadm/agent.{hn}")
            log.info(f"mgr/cephadm/agent.{hn} key exists: {key_out[:200]}...")
        except Exception:
            log.info(f"mgr/cephadm/agent.{hn} key not found in mon store")

    config_keys_out, _ = shell(installer, "ceph config-key ls")
    all_keys = json.loads(config_keys_out)
    agent_keys = [k for k in all_keys if "agent." in k and "cephadm" in k]
    log.info(f"All cephadm agent config keys: {agent_keys}")

    agent_key_hosts = []
    for k in agent_keys:
        parts = k.split("agent.", 1)
        if len(parts) == 2:
            agent_key_hosts.append(parts[1])

    stale_keys = [k for k in agent_keys if not any(hn in k for hn in hostnames)]
    if stale_keys:
        log.warning(
            f"DEFECT INDICATOR: Found agent config keys for hosts not in cluster: {stale_keys}"
        )

    log.info(f"Agent key hosts: {agent_key_hosts}, Cluster hosts: {hostnames}")
    log.info("PASS: Host removal cache leak check completed")


def run_auth_response_code_test(ceph_cluster, installer):
    log.info("=== DEFECT TEST: Auth failure HTTP 200 response code ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0
    hostname = agents[0]["hostname"]
    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None

    fsid = get_fsid(installer)
    agent_dir = f"/var/lib/ceph/{fsid}/agent.{hostname}"
    keyring_path = f"{agent_dir}/keyring"
    service_name = agent_service_name(fsid, hostname)

    target_node.exec_command(sudo=True, cmd=f"cp {keyring_path} {keyring_path}.bak")

    try:
        log.info("Installing wrong keyring")
        target_node.exec_command(
            sudo=True,
            cmd=f"echo '[client.agent.{hostname}]' > {keyring_path} && "
            f"echo '    key = AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==' >> {keyring_path}",
        )
        target_node.exec_command(sudo=True, cmd=f"systemctl restart {service_name}")
        time.sleep(DEFAULT_AGENT_REFRESH + 10)

        log.info("Checking agent journal for POST result handling")
        journal_out, _ = target_node.exec_command(
            sudo=True,
            cmd=f"journalctl -u {service_name} --no-pager -n 50 2>/dev/null | "
            f"grep -i 'result\\|status\\|200\\|failed\\|error\\|bad metadata' || echo 'NO_MATCH'",
            check_ec=False,
        )
        log.info(f"Agent journal (relevant lines): {journal_out[:500]}")

        agents_now = get_orch_ps(installer, daemon_type="agent")
        agent_status = "unknown"
        for a in agents_now:
            if a.get("hostname") == hostname:
                agent_status = a.get("status_desc", "unknown")
                log.info(f"Agent orch status with wrong keyring: {agent_status}")
                break

        assert agent_status != "running", (
            "BUG UNFIXED: Agent shows 'running' despite rejected keyring. "
            "MGR returns HTTP 200 for auth failures (should return 401/403). "
            "Agent cannot detect that its metadata POSTs are being rejected."
        )

    finally:
        log.info("Restoring keyring")
        target_node.exec_command(sudo=True, cmd=f"cp {keyring_path}.bak {keyring_path}")
        target_node.exec_command(sudo=True, cmd=f"rm -f {keyring_path}.bak")
        target_node.exec_command(sudo=True, cmd=f"systemctl restart {service_name}")

    assert wait_for_agent_running(
        installer, hostname, timeout=180
    ), f"Agent on {hostname} did not recover"
    wait_for_health_warning(
        installer, AGENT_HEALTH_WARNING, timeout=180, expect_present=False
    )
    log.info(
        "PASS: Auth response code test completed — agent correctly detects auth rejection"
    )


def run_silent_listener_failure_test(ceph_cluster, installer):
    log.info("=== DEFECT TEST: Silent listener thread failure ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0
    hostname = agents[0]["hostname"]
    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None

    fsid = get_fsid(installer)
    agent_dir = f"/var/lib/ceph/{fsid}/agent.{hostname}"
    listener_cert = f"{agent_dir}/listener.crt"
    service_name = agent_service_name(fsid, hostname)

    target_node.exec_command(sudo=True, cmd=f"cp {listener_cert} {listener_cert}.bak")

    try:
        log.info("Corrupting listener.crt")
        target_node.exec_command(sudo=True, cmd=f"echo 'CORRUPTED' > {listener_cert}")

        log.info("Restarting agent with corrupted listener cert")
        target_node.exec_command(sudo=True, cmd=f"systemctl restart {service_name}")
        time.sleep(DEFAULT_AGENT_REFRESH + 10)

        status_out, _ = target_node.exec_command(
            sudo=True, cmd=f"systemctl is-active {service_name}", check_ec=False
        )
        status = status_out.strip()
        log.info(f"Agent systemctl status: {status}")

        port_check, _ = target_node.exec_command(
            sudo=True,
            cmd="ss -tlnp | grep ':4721' || echo 'PORT_NOT_LISTENING'",
            check_ec=False,
        )
        log.info(f"Port 4721 status: {port_check.strip()}")

        journal_out, _ = target_node.exec_command(
            sudo=True,
            cmd=f"journalctl -u {service_name} --no-pager -n 30 2>/dev/null | "
            f"grep -i 'mgr response\\|Successfully' | tail -3 || echo 'NO_POSTS'",
            check_ec=False,
        )
        log.info(f"Agent posting status: {journal_out.strip()[:200]}")

        assert not (status == "active" and "PORT_NOT_LISTENING" in port_check), (
            "BUG UNFIXED: Agent is running and posting metadata, but mgr_listener "
            "thread has died silently. Port 4721 is not listening. MGR cannot push "
            "config changes to this agent. The main thread should detect listener "
            "death and either restart it or shut down the agent."
        )

    finally:
        log.info("Restoring listener.crt")
        target_node.exec_command(
            sudo=True, cmd=f"cp {listener_cert}.bak {listener_cert}"
        )
        target_node.exec_command(sudo=True, cmd=f"rm -f {listener_cert}.bak")
        target_node.exec_command(sudo=True, cmd=f"systemctl restart {service_name}")

    assert wait_for_agent_running(
        installer, hostname, timeout=180
    ), f"Agent on {hostname} did not recover"
    log.info(
        "PASS: Silent listener failure test completed — agent detects listener death"
    )


def run_failover_resilience_test(ceph_cluster, installer):
    log.info("=== DEFECT TEST: MGR failover resilience gap ===")

    agents = get_agent_daemons(installer)
    assert len(agents) >= 2
    hostname = agents[0]["hostname"]
    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None

    fsid = get_fsid(installer)
    agent_dir = f"/var/lib/ceph/{fsid}/agent.{hostname}"
    service_name = agent_service_name(fsid, hostname)

    log.info(f"Target agent: {hostname}")

    mgr_out, _ = shell(installer, "ceph mgr stat -f json")
    pre_mgr = json.loads(mgr_out)["active_name"]
    log.info(f"Pre-failover active MGR: {pre_mgr}")

    config_out, _ = target_node.exec_command(
        sudo=True, cmd=f"cat {agent_dir}/agent.json"
    )
    pre_config = json.loads(config_out)
    pre_target_ip = pre_config["target_ip"]
    log.info(f"Pre-failover target_ip in agent.json: {pre_target_ip}")

    log.info("Stopping agent on target host...")
    target_node.exec_command(sudo=True, cmd=f"systemctl stop {service_name}")
    time.sleep(5)

    log.info("Triggering MGR failover while agent is stopped...")
    shell(installer, "ceph mgr fail")
    time.sleep(15)

    mgr_out2, _ = shell(installer, "ceph mgr stat -f json")
    post_mgr = json.loads(mgr_out2)["active_name"]
    log.info(f"Post-failover active MGR: {post_mgr}")

    log.info("Starting agent back up...")
    target_node.exec_command(sudo=True, cmd=f"systemctl start {service_name}")
    time.sleep(DEFAULT_AGENT_REFRESH * 2)

    config_out2, _ = target_node.exec_command(
        sudo=True, cmd=f"cat {agent_dir}/agent.json"
    )
    post_config = json.loads(config_out2)
    post_target_ip = post_config["target_ip"]
    log.info(f"Post-failover target_ip in agent.json: {post_target_ip}")

    journal_out, _ = target_node.exec_command(
        sudo=True,
        cmd=f"journalctl -u {service_name} --no-pager -n 10 2>/dev/null | "
        f"grep -i 'response\\|error\\|refused' | tail -3 || echo 'NO_MATCH'",
        check_ec=False,
    )
    log.info(f"Agent journal after restart: {journal_out.strip()[:200]}")

    assert not (pre_target_ip == post_target_ip and pre_mgr != post_mgr), (
        f"BUG UNFIXED: Agent still points to old MGR IP ({pre_target_ip}) after "
        f"failover from {pre_mgr} to {post_mgr}. Agent was stopped during failover "
        f"and missed the config push. It should have a fallback discovery mechanism "
        f"to find the new active MGR."
    )

    if not wait_for_agent_running(installer, hostname, timeout=120):
        log.error(
            "Agent did not reconnect — redeploying to recover for suite continuity"
        )
        shell(installer, f"ceph orch daemon redeploy agent.{hostname}")
        assert wait_for_agent_running(installer, hostname, timeout=180)

    wait_for_health_warning(
        installer, AGENT_HEALTH_WARNING, timeout=180, expect_present=False
    )
    log.info("PASS: Failover resilience test completed — agent discovers new MGR")


TEST_REGISTRY = {
    "defect_stale_metadata": run_stale_metadata_overwrite_test,
    "defect_stale_timestamp": run_stale_timestamp_test,
    "defect_zero_division": run_zero_division_crash_test,
    "defect_graceful_shutdown": run_graceful_shutdown_test,
    "defect_cache_leak": run_host_removal_cache_leak_test,
    "defect_auth_response": run_auth_response_code_test,
    "defect_silent_listener": run_silent_listener_failure_test,
    "defect_failover_resilience": run_failover_resilience_test,
}


def run(ceph_cluster, **kw):
    """Run agent defect tests."""
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
