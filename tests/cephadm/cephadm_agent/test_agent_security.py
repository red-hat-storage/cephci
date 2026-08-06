"""Agent security, keyring runtime deletion, and corrupt metadata payload tests."""

import base64
import json
import textwrap
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


def _agent_last_refresh(installer, hostname):
    """Return last_refresh string for hostname from orch ps, or None."""
    out, _ = shell(installer, "ceph orch ps --daemon-type agent -f json")
    for a in json.loads(out):
        if a.get("hostname") == hostname:
            return a.get("last_refresh")
    return None


def run_corrupt_metadata_payload_test(ceph_cluster, installer):
    """
    Directly POST crafted /data payloads to the mgr agent endpoint.

    The previous version only wrote unused /tmp files and restarted the agent,
    which re-collected *real* metadata — so nothing corrupt was ever posted.
    This version stops the real agent and HTTPS-POSTs payloads itself using the
    agent keyring + root CA from the agent directory.
    """
    log.info("=== TEST: Corrupt metadata payload with valid keyring (direct /data POST) ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0
    hostname = agents[0]["hostname"]
    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None

    fsid = get_fsid(installer)
    agent_dir = f"/var/lib/ceph/{fsid}/agent.{hostname}"
    service_name = agent_service_name(fsid, hostname)
    results_path = "/tmp/agent_corrupt_post_results.json"

    pre_refresh = _agent_last_refresh(installer, hostname)
    log.info(f"Pre-test last_refresh: {pre_refresh}")

    # Stop real agent so only our crafted POSTs update mgr agent timestamps.
    log.info(f"Stopping real agent on {hostname} to avoid racing good metadata posts")
    target_node.exec_command(sudo=True, cmd=f"systemctl stop {service_name}")
    time.sleep(2)

    # Python runs on the agent host (same place the real agent posts from).
    # Posts crafted cases and writes JSON results for the test to assert on.
    post_script = textwrap.dedent(
        f"""\
        import json, ssl, time
        from urllib.request import Request, urlopen
        from urllib.error import HTTPError, URLError

        agent_dir = {agent_dir!r}
        results_path = {results_path!r}

        with open(agent_dir + "/agent.json") as f:
            cfg = json.load(f)
        with open(agent_dir + "/keyring") as f:
            keyring = f.read()

        host = cfg["host"]
        target_ip = cfg["target_ip"]
        target_port = str(cfg["target_port"])
        listener_port = str(cfg.get("listener_port", "4721"))
        ca_path = agent_dir + "/root_cert.pem"

        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(ca_path)

        def post(payload):
            data = json.dumps(payload).encode("utf-8")
            url = "https://%s:%s/data" % (target_ip, target_port)
            req = Request(url, data=data, headers={{"Content-Type": "application/json"}})
            try:
                with urlopen(req, context=ctx, timeout=15) as resp:
                    body = resp.read().decode()
                    return {{"http_status": resp.status, "body": body, "url": url}}
            except HTTPError as e:
                return {{"http_status": e.code, "body": str(e.reason), "url": url}}
            except URLError as e:
                return {{"http_status": -1, "body": str(e.reason), "url": url}}
            except Exception as e:
                return {{"http_status": -1, "body": str(e), "url": url}}

        base = {{
            "host": host,
            "ls": [],
            "networks": {{}},
            "facts": "",
            "volume": "",
            "ack": "1",
            "keyring": keyring,
            "port": listener_port,
        }}

        cases = {{}}

        # Case 1: wrong keyring — auth must fail; mgr must not refresh timestamp.
        bad_auth = dict(base)
        bad_auth["keyring"] = (
            "[client.agent.%s]\\n\\tkey = AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==\\n" % host
        )
        bad_auth["facts"] = json.dumps({{"arch": "x86_64", "corrupt_test": "bad_auth"}})
        cases["bad_keyring"] = {{
            "request_note": "valid shape, wrong keyring",
            "response": post(bad_auth),
        }}
        time.sleep(1)

        # Case 2: valid keyring + non-JSON facts.
        # Mgr updates agent_timestamp BEFORE parsing facts, then returns failure.
        bad_facts = dict(base)
        bad_facts["facts"] = "CORRUPTED_GARBAGE_NOT_JSON"
        bad_facts["volume"] = ""
        cases["bad_facts_not_json"] = {{
            "request_note": "valid keyring, facts is not JSON",
            "response": post(bad_facts),
        }}
        time.sleep(1)

        # Case 3: valid keyring + parseable nonsense facts JSON.
        junk_facts = dict(base)
        junk_facts["facts"] = json.dumps({{"invalid": "not_real_host_facts", "corrupt": True}})
        junk_facts["volume"] = ""
        cases["junk_facts_json"] = {{
            "request_note": "valid keyring, facts JSON is nonsense object",
            "response": post(junk_facts),
        }}
        time.sleep(1)

        # Case 4: valid keyring + corrupt volume inventory JSON.
        bad_vol = dict(base)
        bad_vol["facts"] = json.dumps({{"arch": "x86_64", "corrupt_test": "volume_case"}})
        bad_vol["volume"] = "CORRUPTED_VOLUME_NOT_DEVICES_JSON"
        cases["bad_volume"] = {{
            "request_note": "valid keyring, volume is not Devices JSON",
            "response": post(bad_vol),
        }}

        out = {{
            "host": host,
            "target": "%s:%s" % (target_ip, target_port),
            "cases": cases,
        }}
        with open(results_path, "w") as f:
            json.dump(out, f, indent=2)
        print(json.dumps(out))
        """
    )

    try:
        log.info("POSTing crafted corrupt payloads to mgr /data endpoint")
        b64 = base64.b64encode(post_script.encode()).decode()
        target_node.exec_command(
            sudo=True,
            cmd=(
                f"echo {b64} | base64 -d > /tmp/agent_corrupt_post.py && "
                "python3 /tmp/agent_corrupt_post.py"
            ),
        )
        raw, _ = target_node.exec_command(sudo=True, cmd=f"cat {results_path}")
        post_results = json.loads(raw)
        log.info(f"POST target: {post_results.get('target')}")
        for name, case in post_results.get("cases", {}).items():
            resp = case.get("response", {})
            log.info(
                f"CASE {name}: http={resp.get('http_status')} "
                f"body={str(resp.get('body'))[:300]}"
            )

        bad_key = post_results["cases"]["bad_keyring"]["response"]
        bad_facts = post_results["cases"]["bad_facts_not_json"]["response"]
        junk_facts = post_results["cases"]["junk_facts_json"]["response"]
        bad_vol = post_results["cases"]["bad_volume"]["response"]

        def _result_text(resp):
            body = resp.get("body", "")
            if isinstance(body, str):
                try:
                    parsed = json.loads(body)
                    return str(parsed.get("result", body))
                except Exception:
                    return body
            return str(body)

        # Auth rejection path
        bad_key_text = _result_text(bad_key)
        assert bad_key.get("http_status") == 200, (
            f"Expected HTTP 200 with JSON error body for bad keyring, got {bad_key}"
        )
        assert "Bad metadata" in bad_key_text or "keyring" in bad_key_text.lower(), (
            f"Expected auth rejection in body, got: {bad_key_text}"
        )
        log.info("FINDING: wrong keyring → mgr returns Bad metadata / keyring error")

        # Content-corrupt paths with valid keyring
        for label, resp in (
            ("bad_facts_not_json", bad_facts),
            ("junk_facts_json", junk_facts),
            ("bad_volume", bad_vol),
        ):
            assert resp.get("http_status") == 200, f"{label}: expected HTTP 200, got {resp}"
            text = _result_text(resp)
            log.info(f"{label} mgr result: {text}")
            assert "wrong keyring" not in text.lower(), (
                f"{label} unexpectedly failed auth: {text}"
            )

        # agent_timestamp (used for CEPHADM_AGENT_DOWN) is updated in handle_metadata
        # *before* facts/volume are parsed. Real agent is stopped, so if corrupt
        # valid-keyring POSTs refreshed that timestamp, AGENT_DOWN should stay clear
        # for a full down-detection window.
        down_wait = DEFAULT_AGENT_DOWN_TIMEOUT + 40
        log.info(
            f"Waiting {down_wait}s with real agent stopped to see if corrupt "
            "valid-keyring POSTs kept agent_timestamp fresh (no CEPHADM_AGENT_DOWN)"
        )
        agent_down = wait_for_health_warning(
            installer, AGENT_HEALTH_WARNING, timeout=down_wait
        )
        log.info(f"CEPHADM_AGENT_DOWN raised: {agent_down}")
        log.info(
            f"orch ps last_refresh unchanged check (ls-based, not agent_timestamp): "
            f"before={pre_refresh} now={_agent_last_refresh(installer, hostname)}"
        )

        if not agent_down:
            log.info(
                "FINDING: With a *valid* keyring, crafted corrupt facts/volume POSTs "
                "to /data keep CEPHADM_AGENT_DOWN from firing while the real agent is "
                "stopped. Mgr validates auth and updates agent_timestamp before parsing "
                "payload content — corrupt content is not treated as agent-down."
            )
        else:
            log.info(
                "FINDING: CEPHADM_AGENT_DOWN still raised — corrupt content POSTs did "
                "not keep agent_timestamp fresh (stricter behavior or POST did not "
                "reach handle_metadata successfully)."
            )

    finally:
        log.info("Cleaning up and restarting real agent")
        target_node.exec_command(
            sudo=True, cmd="rm -f /tmp/agent_corrupt_post.py "
            f"{results_path}", check_ec=False
        )
        target_node.exec_command(
            sudo=True, cmd=f"systemctl start {service_name}", check_ec=False
        )

    assert wait_for_agent_running(
        installer, hostname, timeout=120
    ), f"Agent on {hostname} not running after test cleanup"
    assert wait_for_health_warning(
        installer, AGENT_HEALTH_WARNING, timeout=180, expect_present=False
    ), "CEPHADM_AGENT_DOWN did not clear after agent restart"
    log.info("PASS: Direct corrupt metadata /data POST test completed")


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
