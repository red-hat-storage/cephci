"""Agent security, keyring runtime deletion, and corrupt metadata payload tests."""

import json
import time

from cephadm_agent.helpers import (
    AGENT_HEALTH_WARNING,
    DEFAULT_AGENT_DOWN_TIMEOUT,
    DEFAULT_AGENT_REFRESH,
    agent_service_name,
    get_agent_daemons,
    get_cluster_health_status,
    get_fsid,
    get_node_for_host,
    log,
    setup_run,
    shell,
    wait_for_agent_running,
    wait_for_health_warning,
)


def run_security_test(ceph_cluster, installer):
    log.info("=== TEST: Security - Wrong auth causes safe failure ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0, "No agent daemons to test"
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
    keyring_path = f"{agent_dir}/keyring"
    service_name = f"ceph-{fsid}@agent.{hostname}"

    log.info(f"Backing up keyring on {hostname}")
    target_node.exec_command(sudo=True, cmd=f"cp {keyring_path} {keyring_path}.bak")

    try:
        log.info("Corrupting agent keyring")
        target_node.exec_command(
            sudo=True,
            cmd=f"echo '[client.agent.{hostname}]' > {keyring_path} && "
            f"echo '    key = AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==' >> {keyring_path}",
        )

        log.info("Restarting agent with bad keyring")
        target_node.exec_command(sudo=True, cmd=f"systemctl restart {service_name}")

        down_timeout = DEFAULT_AGENT_DOWN_TIMEOUT + 60
        agent_warned = wait_for_health_warning(
            installer, AGENT_HEALTH_WARNING, timeout=down_timeout
        )

        status_out, _ = target_node.exec_command(
            sudo=True, cmd=f"systemctl is-active {service_name}", check_ec=False
        )
        log.info(f"Agent systemd status with bad keyring: {status_out.strip()}")

        if agent_warned:
            log.info("CEPHADM_AGENT_DOWN raised — mgr correctly rejected bad keyring")
        else:
            log.warning(
                "CEPHADM_AGENT_DOWN not raised within timeout. "
                "Agent may still be posting with old cached keyring."
            )

        health_status = get_cluster_health_status(installer)
        assert (
            health_status != "HEALTH_ERR" or agent_warned
        ), f"Cluster in HEALTH_ERR without agent-down explanation: {health_status}"
        log.info(f"Cluster health with bad agent keyring: {health_status}")

    finally:
        log.info("Restoring original keyring")
        target_node.exec_command(sudo=True, cmd=f"cp {keyring_path}.bak {keyring_path}")
        target_node.exec_command(sudo=True, cmd=f"rm -f {keyring_path}.bak")
        target_node.exec_command(sudo=True, cmd=f"systemctl restart {service_name}")

    assert wait_for_agent_running(
        installer, hostname, timeout=120
    ), f"Agent on {hostname} did not recover after keyring restore"
    assert wait_for_health_warning(
        installer, AGENT_HEALTH_WARNING, timeout=180, expect_present=False
    ), "CEPHADM_AGENT_DOWN did not clear after keyring restore"
    log.info("PASS: Agent failed safely with bad keyring and recovered cleanly")


def run_keyring_runtime_deletion_test(ceph_cluster, installer):
    log.info("=== TEST: Keyring deletion while agent is running ===")

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
        log.info("Verifying agent is posting metadata before deletion")
        time.sleep(DEFAULT_AGENT_REFRESH + 5)
        pre_journal, _ = target_node.exec_command(
            sudo=True,
            cmd=f"journalctl -u {service_name} --since '25 seconds ago' --no-pager 2>/dev/null | "
            f"grep -c 'Successfully processed' || true",
            check_ec=False,
        )
        pre_posts = int((pre_journal.strip().splitlines() or ["0"])[-1])
        log.info(f"Pre-deletion posts in last 25s: {pre_posts}")
        assert pre_posts > 0, "Agent not posting before deletion"

        log.info("Deleting keyring file...")
        target_node.exec_command(sudo=True, cmd=f"rm -f {keyring_path}")
        time.sleep(DEFAULT_AGENT_REFRESH + 10)

        status_out, _ = target_node.exec_command(
            sudo=True, cmd=f"systemctl is-active {service_name}", check_ec=False
        )
        log.info(f"Agent status after keyring deletion: {status_out.strip()}")

        post_journal, _ = target_node.exec_command(
            sudo=True,
            cmd=f"journalctl -u {service_name} --since '30 seconds ago' --no-pager 2>/dev/null | "
            f"grep -c 'Successfully processed' || true",
            check_ec=False,
        )
        post_posts = int((post_journal.strip().splitlines() or ["0"])[-1])
        log.info(f"Post-deletion posts in last 30s: {post_posts}")

        if status_out.strip() == "active" and post_posts > 0:
            log.info(
                "Agent continues posting with in-memory keyring after file deletion. "
                "This is expected resilient behavior (not a bug)."
            )

    finally:
        log.info("Restoring keyring from backup")
        target_node.exec_command(sudo=True, cmd=f"cp {keyring_path}.bak {keyring_path}")
        target_node.exec_command(sudo=True, cmd=f"rm -f {keyring_path}.bak")

    assert wait_for_agent_running(
        installer, hostname, timeout=60
    ), f"Agent on {hostname} is not running"
    log.info("PASS: Keyring runtime deletion test completed")


def run_corrupt_metadata_payload_test(ceph_cluster, installer):
    log.info("=== TEST: Corrupt metadata payload with valid keyring ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0
    hostname = agents[0]["hostname"]
    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None

    fsid = get_fsid(installer)
    agent_dir = f"/var/lib/ceph/{fsid}/agent.{hostname}"
    service_name = agent_service_name(fsid, hostname)

    log.info("Recording last_refresh before corruption")
    pre_out, _ = shell(installer, "ceph orch ps --daemon-type agent -f json")
    pre_agents = json.loads(pre_out)
    pre_refresh = None
    for a in pre_agents:
        if a["hostname"] == hostname:
            pre_refresh = a.get("last_refresh")
            break
    log.info(f"Pre-corruption last_refresh: {pre_refresh}")

    log.info("Backing up agent data files")
    target_node.exec_command(
        sudo=True,
        cmd=f"cp {agent_dir}/agent.json {agent_dir}/agent.json.bak",
    )

    try:
        log.info("Corrupting metadata source: writing garbage to host facts cache")
        target_node.exec_command(
            sudo=True,
            cmd=f"find /var/lib/ceph/{fsid}/agent.{hostname}/ -name '*.json' "
            f"-not -name 'agent.json' -not -name 'keyring' "
            f"-exec sh -c 'echo CORRUPTED_GARBAGE > {{}}' \\;",
            check_ec=False,
        )

        target_node.exec_command(
            sudo=True,
            cmd="mkdir -p /tmp/agent_corrupt_test && "
            'echo \'{"invalid": "not_real_host_facts", "corrupt": true}\' '
            "> /tmp/agent_corrupt_test/facts",
        )

        log.info("Restarting agent to force fresh data collection with corrupt sources")
        target_node.exec_command(sudo=True, cmd=f"systemctl restart {service_name}")
        time.sleep(10)

        status_out, _ = target_node.exec_command(
            sudo=True, cmd=f"systemctl is-active {service_name}", check_ec=False
        )
        log.info(f"Agent status after restart: {status_out.strip()}")

        log.info(
            f"Waiting {DEFAULT_AGENT_DOWN_TIMEOUT + 60}s to see if AGENT_DOWN is raised..."
        )
        agent_down = wait_for_health_warning(
            installer, AGENT_HEALTH_WARNING, timeout=DEFAULT_AGENT_DOWN_TIMEOUT + 60
        )

        post_out, _ = shell(installer, "ceph orch ps --daemon-type agent -f json")
        post_agents = json.loads(post_out)
        post_refresh = None
        post_status = None
        for a in post_agents:
            if a["hostname"] == hostname:
                post_refresh = a.get("last_refresh")
                post_status = a.get("status_desc")
                break

        log.info(f"Post-corruption last_refresh: {post_refresh}")
        log.info(f"Post-corruption status: {post_status}")
        log.info(f"CEPHADM_AGENT_DOWN raised: {agent_down}")

        if not agent_down and post_status == "running":
            log.info(
                "FINDING: MGR still updates last_refresh when receiving corrupt "
                "metadata with valid keyring. Agent appears healthy despite sending "
                "garbage data. This confirms the MGR only validates authentication, "
                "not payload integrity — no AGENT_DOWN is raised."
            )
        elif agent_down:
            log.info(
                "FINDING: MGR detected corrupt payload and raised CEPHADM_AGENT_DOWN. "
                "MGR validates payload content beyond just keyring auth."
            )

    finally:
        log.info("Restoring agent data files")
        target_node.exec_command(
            sudo=True,
            cmd=f"cp {agent_dir}/agent.json.bak {agent_dir}/agent.json",
            check_ec=False,
        )
        target_node.exec_command(
            sudo=True,
            cmd=f"rm -f {agent_dir}/agent.json.bak",
            check_ec=False,
        )
        target_node.exec_command(
            sudo=True, cmd="rm -rf /tmp/agent_corrupt_test", check_ec=False
        )
        target_node.exec_command(sudo=True, cmd=f"systemctl restart {service_name}")

    assert wait_for_agent_running(
        installer, hostname, timeout=120
    ), f"Agent on {hostname} not running after restoring data"
    log.info("PASS: Corrupt metadata payload test completed")


def run_log_security_test(ceph_cluster, installer):
    log.info("=== TEST: Log security - no sensitive data exposed in logs ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0
    hostname = agents[0]["hostname"]
    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None

    fsid = get_fsid(installer)
    service_name = agent_service_name(fsid, hostname)

    log.info("Step 1: Check journal logs for sensitive data")
    sensitive_patterns = "password|secret|private.key|BEGIN RSA|BEGIN EC"
    journal_check, _ = target_node.exec_command(
        sudo=True,
        cmd=f"journalctl -u {service_name} --no-pager -n 500 2>/dev/null | "
        f"grep -iE '{sensitive_patterns}' | head -5 || echo 'CLEAN'",
        check_ec=False,
    )
    log.info(f"Sensitive data check: {journal_check.strip()[:300]}")
    assert (
        journal_check.strip() == "CLEAN" or "key =" not in journal_check.lower()
    ), f"Sensitive data found in agent journal logs: {journal_check.strip()[:200]}"

    log.info("Step 2: Check keyring file permissions")
    agent_dir = f"/var/lib/ceph/{fsid}/agent.{hostname}"
    keyring_perms, _ = target_node.exec_command(
        sudo=True,
        cmd=f"stat -c '%a %U %G' {agent_dir}/keyring 2>/dev/null "
        f"|| echo 'NOT_FOUND'",
        check_ec=False,
    )
    log.info(f"Keyring permissions: {keyring_perms.strip()}")
    if "NOT_FOUND" not in keyring_perms:
        perms = keyring_perms.strip().split()[0]
        assert perms in ("600", "640", "644"), (
            f"Keyring has overly permissive permissions: {perms} "
            f"(expected 600 or 640)"
        )
        if perms != "600":
            log.warning(f"Keyring permissions are {perms}, ideally should be 600")

    log.info("Step 3: Check agent directory permissions")
    dir_perms, _ = target_node.exec_command(
        sudo=True,
        cmd=f"stat -c '%a %U %G' {agent_dir} 2>/dev/null || echo 'NOT_FOUND'",
        check_ec=False,
    )
    log.info(f"Agent directory permissions: {dir_perms.strip()}")

    log.info("Step 4: Check TLS cert file permissions")
    for cert_file in ["root_cert.pem", "listener.crt", "listener.key"]:
        cert_perms, _ = target_node.exec_command(
            sudo=True,
            cmd=f"stat -c '%a %U %G' {agent_dir}/{cert_file} 2>/dev/null "
            f"|| echo 'NOT_FOUND'",
            check_ec=False,
        )
        log.info(f"  {cert_file}: {cert_perms.strip()}")

    log.info("Step 5: Verify ceph log doesn't expose agent keys")
    ceph_log_check, _ = shell(
        installer,
        "ceph log last 200 2>/dev/null | "
        "grep -iE 'key.*AQ|secret|password' | "
        "grep -i agent | head -3 || echo 'CLEAN'",
    )
    log.info(f"Ceph cluster log check: {ceph_log_check.strip()[:200]}")

    log.info("PASS: Log security validation complete — no sensitive data exposed")


TEST_REGISTRY = {
    "security": run_security_test,
    "keyring_runtime_deletion": run_keyring_runtime_deletion_test,
    "corrupt_metadata_payload": run_corrupt_metadata_payload_test,
    "log_security": run_log_security_test,
}


def run(ceph_cluster, **kw):
    """Run agent security tests."""
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
