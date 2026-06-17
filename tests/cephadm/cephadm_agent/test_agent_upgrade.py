"""Agent upgrade test."""

from cephadm_agent.helpers import (
    AGENT_HEALTH_WARNING,
    get_agent_daemons,
    log,
    setup_run,
    wait_for_health_warning,
)


def run_upgrade_test(ceph_cluster, installer):
    log.info("=== TEST: Upgrade - Agent health after upgrade ===")

    agents_before = get_agent_daemons(installer)
    hosts_before = {a["hostname"] for a in agents_before}
    log.info(f"Agent daemons before upgrade check on hosts: {hosts_before}")

    for agent in agents_before:
        hostname = agent["hostname"]
        status = agent.get("status_desc", "unknown")
        assert status == "running", (
            f"Agent on {hostname} not running (status={status}) "
            "— expected running state post-upgrade"
        )
        log.info(
            f"Agent on {hostname}: status={status}, "
            f"image={agent.get('container_image_name', 'N/A')}"
        )

    no_agent_down = wait_for_health_warning(
        installer, AGENT_HEALTH_WARNING, timeout=60, expect_present=False
    )
    assert (
        no_agent_down
    ), "CEPHADM_AGENT_DOWN warning present — agent may not have survived upgrade"
    log.info("PASS: All agents healthy post-upgrade")


TEST_REGISTRY = {
    "upgrade": run_upgrade_test,
}


def run(ceph_cluster, **kw):
    """Run agent upgrade tests."""
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
