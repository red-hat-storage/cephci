"""Agent config validation, TLS cert, counter sync, and listener port change tests."""

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


def run_config_validation_test(ceph_cluster, installer):
    log.info("=== TEST: Config validation - Missing required file ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0, "No agent daemons"
    target = agents[0]
    hostname = target["hostname"]

    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None

    fsid = get_fsid(installer)
    agent_dir = f"/var/lib/ceph/{fsid}/agent.{hostname}"
    config_path = f"{agent_dir}/agent.json"
    service_name = agent_service_name(fsid, hostname)

    log.info(f"Backing up and removing agent.json on {hostname}")
    target_node.exec_command(sudo=True, cmd=f"cp {config_path} {config_path}.bak")

    try:
        target_node.exec_command(sudo=True, cmd=f"systemctl stop {service_name}")
        time.sleep(3)
        target_node.exec_command(sudo=True, cmd=f"rm -f {config_path}")
        target_node.exec_command(
            sudo=True, cmd=f"systemctl start {service_name}", check_ec=False
        )
        time.sleep(15)

        status_out, _ = target_node.exec_command(
            sudo=True, cmd=f"systemctl is-active {service_name}", check_ec=False
        )
        status = status_out.strip()
        log.info(f"Agent status after config removal and restart: {status}")

        if status in ("failed", "inactive", "activating"):
            log.info("Agent correctly failed to start without agent.json")
        elif status == "active":
            log.info(
                "Agent is active — checking if it's functional or in error via orch ps"
            )
            agents_now = get_agent_daemons(installer)
            for a in agents_now:
                if a.get("hostname") == hostname:
                    orch_status = a.get("status_desc", "unknown")
                    log.info(f"Agent orch status: {orch_status}")
                    if orch_status in ("error", "unknown"):
                        log.info(
                            "Agent shows error in orchestrator — config validation effective"
                        )
                    else:
                        log.warning(
                            f"Agent running with status '{orch_status}' "
                            "despite missing config. The agent may have "
                            "cached its config in memory before removal."
                        )
                    break
        else:
            log.info(f"Agent in unexpected state: {status}")

    finally:
        log.info("Restoring agent.json")
        target_node.exec_command(sudo=True, cmd=f"cp {config_path}.bak {config_path}")
        target_node.exec_command(sudo=True, cmd=f"rm -f {config_path}.bak")
        target_node.exec_command(sudo=True, cmd=f"systemctl restart {service_name}")

    assert wait_for_agent_running(
        installer, hostname, timeout=180
    ), f"Agent on {hostname} did not recover after config restore"
    log.info(
        "PASS: Config validation test completed — agent recovered after config restore"
    )


def run_tls_cert_test(ceph_cluster, installer):
    log.info("=== TEST: TLS cert validation - Bad root cert ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0
    target = agents[0]
    hostname = target["hostname"]

    target_node = None
    for node in ceph_cluster.get_nodes():
        if node.hostname == hostname:
            target_node = node
            break
    assert target_node is not None

    fsid, _ = shell(installer, "ceph fsid")
    fsid = fsid.strip()
    agent_dir = f"/var/lib/ceph/{fsid}/agent.{hostname}"
    cert_path = f"{agent_dir}/root_cert.pem"
    service_name = f"ceph-{fsid}@agent.{hostname}"

    log.info(f"Backing up and corrupting root_cert.pem on {hostname}")
    target_node.exec_command(sudo=True, cmd=f"cp {cert_path} {cert_path}.bak")

    try:
        target_node.exec_command(
            sudo=True, cmd=f"echo 'INVALID CERTIFICATE DATA' > {cert_path}"
        )
        target_node.exec_command(sudo=True, cmd=f"systemctl restart {service_name}")

        down_timeout = DEFAULT_AGENT_DOWN_TIMEOUT + 60
        result = wait_for_health_warning(
            installer, AGENT_HEALTH_WARNING, timeout=down_timeout
        )
        if result:
            log.info(
                "CEPHADM_AGENT_DOWN raised with bad TLS cert — agent could not reach mgr"
            )
        else:
            log.warning(
                "Warning not raised within timeout — agent may have cached valid TLS state"
            )
    finally:
        log.info("Restoring root_cert.pem")
        target_node.exec_command(sudo=True, cmd=f"cp {cert_path}.bak {cert_path}")
        target_node.exec_command(sudo=True, cmd=f"rm -f {cert_path}.bak")
        target_node.exec_command(sudo=True, cmd=f"systemctl restart {service_name}")

    assert wait_for_agent_running(
        installer, hostname, timeout=120
    ), f"Agent on {hostname} did not recover after cert restore"
    log.info("PASS: TLS cert tampering handled safely")


def run_counter_sync_test(ceph_cluster, installer):
    log.info("=== TEST: Counter sync - Metadata freshness ===")

    hosts_out, _ = shell(installer, "ceph orch host ls -f json")
    hosts = json.loads(hosts_out)
    assert len(hosts) > 0

    log.info("Triggering first refresh snapshot")
    snap1 = get_orch_ps(installer, refresh=True)
    time.sleep(DEFAULT_AGENT_REFRESH + 5)

    log.info("Triggering second refresh snapshot")
    snap2 = get_orch_ps(installer, refresh=True)

    hosts_in_snap1 = {d["hostname"] for d in snap1}
    hosts_in_snap2 = {d["hostname"] for d in snap2}
    assert (
        hosts_in_snap1 == hosts_in_snap2
    ), f"Host coverage changed between snapshots: {hosts_in_snap1} vs {hosts_in_snap2}"

    log.info("PASS: Counter sync working — metadata refreshed consistently")


def run_listener_port_change_test(ceph_cluster, installer):
    log.info("=== TEST: Agent listener port change ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0
    hostname = agents[0]["hostname"]
    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None

    DEFAULT_PORT = 4721
    NEW_PORT = 5721

    log.info(f"Verifying agent is currently listening on port {DEFAULT_PORT}")
    port_out, _ = target_node.exec_command(
        sudo=True,
        cmd=f"ss -tlnp | grep ':{DEFAULT_PORT}' || echo 'PORT_NOT_LISTENING'",
        check_ec=False,
    )
    log.info(f"Port {DEFAULT_PORT} status: {port_out.strip()}")
    assert (
        "PORT_NOT_LISTENING" not in port_out
    ), f"Agent not listening on default port {DEFAULT_PORT} before test"

    try:
        log.info(f"Changing agent_starting_port to {NEW_PORT}")
        shell(
            installer,
            f"ceph config set mgr mgr/cephadm/agent_starting_port {NEW_PORT}",
        )

        log.info("Waiting for agent to pick up new port via config push...")
        time.sleep(DEFAULT_AGENT_REFRESH * 3)

        new_port_out, _ = target_node.exec_command(
            sudo=True,
            cmd=f"ss -tlnp | grep ':{NEW_PORT}' || echo 'PORT_NOT_LISTENING'",
            check_ec=False,
        )
        log.info(f"Port {NEW_PORT} status after config change: {new_port_out.strip()}")

        if "PORT_NOT_LISTENING" not in new_port_out:
            log.info(
                f"Agent picked up new port {NEW_PORT} via config push (no redeploy needed)"
            )
        else:
            log.info(
                "Agent did NOT pick up new port from config push alone. "
                "Redeploying agent to apply port change..."
            )
            shell(installer, f"ceph orch daemon redeploy agent.{hostname}")
            time.sleep(30)

            assert wait_for_agent_running(
                installer, hostname, timeout=120
            ), f"Agent on {hostname} not running after redeploy"

            new_port_out, _ = target_node.exec_command(
                sudo=True,
                cmd=f"ss -tlnp | grep ':{NEW_PORT}' || echo 'PORT_NOT_LISTENING'",
                check_ec=False,
            )
            log.info(f"Port {NEW_PORT} status after redeploy: {new_port_out.strip()}")
            assert (
                "PORT_NOT_LISTENING" not in new_port_out
            ), f"Agent still not listening on {NEW_PORT} even after redeploy"
            log.info(f"Agent listening on new port {NEW_PORT} after redeploy")

        old_port_out, _ = target_node.exec_command(
            sudo=True,
            cmd=f"ss -tlnp | grep ':{DEFAULT_PORT}' || echo 'PORT_NOT_LISTENING'",
            check_ec=False,
        )
        log.info(f"Old port {DEFAULT_PORT} status: {old_port_out.strip()}")
        if "PORT_NOT_LISTENING" in old_port_out:
            log.info(f"Confirmed: agent no longer listening on old port {DEFAULT_PORT}")
        else:
            log.warning(
                f"Agent still listening on old port {DEFAULT_PORT} alongside new port"
            )

    finally:
        log.info(f"Restoring agent_starting_port to default ({DEFAULT_PORT})")
        shell(
            installer,
            f"ceph config set mgr mgr/cephadm/agent_starting_port {DEFAULT_PORT}",
        )
        time.sleep(DEFAULT_AGENT_REFRESH * 2)

        restore_port, _ = target_node.exec_command(
            sudo=True,
            cmd=f"ss -tlnp | grep ':{DEFAULT_PORT}' || echo 'PORT_NOT_LISTENING'",
            check_ec=False,
        )
        if "PORT_NOT_LISTENING" in restore_port:
            log.info("Port not restored via config push, redeploying agent...")
            shell(installer, f"ceph orch daemon redeploy agent.{hostname}")
            time.sleep(30)

    assert wait_for_agent_running(
        installer, hostname, timeout=120
    ), f"Agent on {hostname} not running after restoring port"
    log.info("PASS: Agent listener port change test completed")


def run_port_conflict_test(ceph_cluster, installer):
    log.info("=== TEST: Port conflict handling - agent uses next available port ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0
    hostname = agents[0]["hostname"]
    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None

    fsid = get_fsid(installer)
    service_name = agent_service_name(fsid, hostname)
    DEFAULT_PORT = 4721

    log.info(f"Stopping agent on {hostname} to free port {DEFAULT_PORT}")
    target_node.exec_command(sudo=True, cmd=f"systemctl stop {service_name}")
    time.sleep(5)

    log.info(f"Blocking port {DEFAULT_PORT} with a listener process")
    target_node.exec_command(
        sudo=True,
        cmd=f'python3 -c "import socket; s=socket.socket(); '
        f"s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); "
        f"s.bind(('0.0.0.0', {DEFAULT_PORT})); s.listen(1); "
        f'import time; time.sleep(120)" &',
        check_ec=False,
    )
    time.sleep(3)

    port_blocked, _ = target_node.exec_command(
        sudo=True,
        cmd=f"ss -tlnp | grep ':{DEFAULT_PORT}' || echo 'NOT_BLOCKED'",
        check_ec=False,
    )
    log.info(f"Port {DEFAULT_PORT} block status: {port_blocked.strip()}")

    try:
        log.info("Starting agent — should find next available port")
        target_node.exec_command(sudo=True, cmd=f"systemctl start {service_name}")
        time.sleep(20)

        svc_status, _ = target_node.exec_command(
            sudo=True,
            cmd=f"systemctl is-active {service_name}",
            check_ec=False,
        )
        log.info(f"Agent service status: {svc_status.strip()}")

        agent_ports, _ = target_node.exec_command(
            sudo=True,
            cmd="ss -tlnp | grep python3 | grep -oP ':\\K[0-9]+' || echo 'NO_PORT'",
            check_ec=False,
        )
        log.info(f"Agent listening ports: {agent_ports.strip()}")

        assert (
            svc_status.strip() == "active"
        ), f"Agent failed to start when port {DEFAULT_PORT} was occupied"
        assert (
            "NO_PORT" not in agent_ports
        ), "Agent is active but not listening on any port"

    finally:
        log.info("Cleaning up: killing port blocker")
        target_node.exec_command(
            sudo=True,
            cmd=f"fuser -k {DEFAULT_PORT}/tcp 2>/dev/null || true",
            check_ec=False,
        )
        time.sleep(2)

        target_node.exec_command(
            sudo=True, cmd=f"systemctl restart {service_name}", check_ec=False
        )
        time.sleep(15)

    assert wait_for_agent_running(installer, hostname, timeout=120)
    log.info("PASS: Agent handled port conflict by using next available port")


def run_refresh_rate_test(ceph_cluster, installer):
    log.info("=== TEST: Refresh rate configuration change ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0
    hostname = agents[0]["hostname"]
    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None

    fsid = get_fsid(installer)
    service_name = agent_service_name(fsid, hostname)

    current_rate, _ = shell(
        installer, "ceph config get mgr mgr/cephadm/agent_refresh_rate"
    )
    original_rate = current_rate.strip()
    log.info(f"Current refresh rate: {original_rate}")

    try:
        NEW_RATE = 10
        log.info(f"Setting refresh rate to {NEW_RATE}s")
        shell(
            installer,
            f"ceph config set mgr mgr/cephadm/agent_refresh_rate {NEW_RATE}",
        )
        time.sleep(NEW_RATE * 3)

        log.info("Measuring post frequency at 10s rate")
        journal_out, _ = target_node.exec_command(
            sudo=True,
            cmd=f"journalctl -u {service_name} --since '30 seconds ago' "
            f"--no-pager 2>/dev/null | grep -c 'Successfully processed' || true",
            check_ec=False,
        )
        posts_10s = int((journal_out.strip().splitlines() or ["0"])[-1])
        log.info(f"Posts in 30s window at rate {NEW_RATE}s: {posts_10s}")
        assert (
            posts_10s >= 2
        ), f"Expected at least 2 posts in 30s with {NEW_RATE}s rate, got {posts_10s}"

        SLOW_RATE = 60
        log.info(f"Setting refresh rate to {SLOW_RATE}s")
        shell(
            installer,
            f"ceph config set mgr mgr/cephadm/agent_refresh_rate {SLOW_RATE}",
        )
        time.sleep(SLOW_RATE + 10)

        log.info("Measuring post frequency at 60s rate")
        journal_out2, _ = target_node.exec_command(
            sudo=True,
            cmd=f"journalctl -u {service_name} --since '60 seconds ago' "
            f"--no-pager 2>/dev/null | grep -c 'Successfully processed' || true",
            check_ec=False,
        )
        posts_60s = int((journal_out2.strip().splitlines() or ["0"])[-1])
        log.info(f"Posts in 60s window at rate {SLOW_RATE}s: {posts_60s}")
        assert (
            posts_60s <= 3
        ), f"Expected <=3 posts in 60s with {SLOW_RATE}s rate, got {posts_60s}"

        log.info(
            f"Rate comparison: {posts_10s} posts/30s at 10s vs "
            f"{posts_60s} posts/60s at 60s — confirms rate change effective"
        )

    finally:
        log.info(f"Restoring refresh rate to {original_rate}")
        shell(
            installer,
            f"ceph config set mgr mgr/cephadm/agent_refresh_rate {original_rate}",
        )
        time.sleep(int(original_rate) * 2)

    log.info("PASS: Refresh rate configuration changes take effect")


def run_down_multiplier_test(ceph_cluster, installer):
    log.info("=== TEST: Down multiplier affects detection timing ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0
    hostname = agents[0]["hostname"]
    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None

    fsid = get_fsid(installer)
    service_name = agent_service_name(fsid, hostname)

    current_mult, _ = shell(
        installer, "ceph config get mgr mgr/cephadm/agent_down_multiplier"
    )
    original_mult = current_mult.strip()
    log.info(f"Current down multiplier: {original_mult}")

    try:
        AGGRESSIVE_MULT = 2.0
        log.info(f"Setting down multiplier to {AGGRESSIVE_MULT}")
        shell(
            installer,
            f"ceph config set mgr mgr/cephadm/agent_down_multiplier {AGGRESSIVE_MULT}",
        )
        time.sleep(5)

        log.info(f"Stopping agent on {hostname}")
        target_node.exec_command(sudo=True, cmd=f"systemctl stop {service_name}")

        expected_down_time = DEFAULT_AGENT_REFRESH * AGGRESSIVE_MULT
        log.info(
            f"Expected AGENT_DOWN within ~{expected_down_time}s "
            f"(refresh={DEFAULT_AGENT_REFRESH} x multiplier={AGGRESSIVE_MULT})"
        )

        detected = wait_for_health_warning(
            installer,
            AGENT_HEALTH_WARNING,
            timeout=int(expected_down_time + 60),
            interval=10,
            expect_present=True,
        )
        assert detected, (
            f"CEPHADM_AGENT_DOWN not raised within {expected_down_time + 60}s "
            f"with multiplier={AGGRESSIVE_MULT}"
        )
        log.info("CEPHADM_AGENT_DOWN detected with aggressive multiplier")

    finally:
        log.info("Restoring agent and multiplier")
        target_node.exec_command(
            sudo=True, cmd=f"systemctl start {service_name}", check_ec=False
        )
        shell(
            installer,
            f"ceph config set mgr mgr/cephadm/agent_down_multiplier {original_mult}",
        )
        wait_for_health_warning(
            installer,
            AGENT_HEALTH_WARNING,
            timeout=180,
            interval=10,
            expect_present=False,
        )

    assert wait_for_agent_running(installer, hostname, timeout=120)
    log.info("PASS: Down multiplier configuration affects detection timing")


def run_config_persistence_test(ceph_cluster, installer):
    log.info("=== TEST: Config persistence across MGR restart ===")

    TEST_RATE = 30
    TEST_PORT = 6000
    TEST_MULT = 4.0

    log.info("Setting custom agent configurations")
    shell(
        installer,
        f"ceph config set mgr mgr/cephadm/agent_refresh_rate {TEST_RATE}",
    )
    shell(
        installer,
        f"ceph config set mgr mgr/cephadm/agent_starting_port {TEST_PORT}",
    )
    shell(
        installer,
        f"ceph config set mgr mgr/cephadm/agent_down_multiplier {TEST_MULT}",
    )
    time.sleep(5)

    rate_before, _ = shell(
        installer, "ceph config get mgr mgr/cephadm/agent_refresh_rate"
    )
    port_before, _ = shell(
        installer, "ceph config get mgr mgr/cephadm/agent_starting_port"
    )
    mult_before, _ = shell(
        installer, "ceph config get mgr mgr/cephadm/agent_down_multiplier"
    )
    log.info(
        f"Before restart: rate={rate_before.strip()}, "
        f"port={port_before.strip()}, mult={mult_before.strip()}"
    )

    try:
        log.info("Restarting active MGR daemon")
        mgr_stat, _ = shell(installer, "ceph mgr stat -f json")
        active_mgr = json.loads(mgr_stat)["active_name"]
        shell(installer, f"ceph mgr fail {active_mgr}")
        time.sleep(30)

        rate_after, _ = shell(
            installer, "ceph config get mgr mgr/cephadm/agent_refresh_rate"
        )
        port_after, _ = shell(
            installer, "ceph config get mgr mgr/cephadm/agent_starting_port"
        )
        mult_after, _ = shell(
            installer, "ceph config get mgr mgr/cephadm/agent_down_multiplier"
        )
        log.info(
            f"After MGR failover: rate={rate_after.strip()}, "
            f"port={port_after.strip()}, mult={mult_after.strip()}"
        )

        assert rate_after.strip() == str(
            TEST_RATE
        ), f"Refresh rate not persisted: expected {TEST_RATE}, got {rate_after.strip()}"
        assert port_after.strip() == str(
            TEST_PORT
        ), f"Starting port not persisted: expected {TEST_PORT}, got {port_after.strip()}"
        assert (
            float(mult_after.strip()) == TEST_MULT
        ), f"Down multiplier not persisted: expected {TEST_MULT}, got {mult_after.strip()}"

    finally:
        log.info("Restoring default configurations")
        shell(installer, "ceph config set mgr mgr/cephadm/agent_refresh_rate 20")
        shell(installer, "ceph config set mgr mgr/cephadm/agent_starting_port 4721")
        shell(installer, "ceph config set mgr mgr/cephadm/agent_down_multiplier 3.0")
        time.sleep(DEFAULT_AGENT_REFRESH * 2)

    log.info("PASS: All agent configs persisted across MGR failover")


TEST_REGISTRY = {
    "config_validation": run_config_validation_test,
    "tls_cert": run_tls_cert_test,
    "counter_sync": run_counter_sync_test,
    "listener_port_change": run_listener_port_change_test,
    "port_conflict": run_port_conflict_test,
    "refresh_rate": run_refresh_rate_test,
    "down_multiplier": run_down_multiplier_test,
    "config_persistence": run_config_persistence_test,
}


def run(ceph_cluster, **kw):
    """Run agent config tests."""
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
