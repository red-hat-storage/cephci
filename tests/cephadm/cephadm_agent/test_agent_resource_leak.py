"""Agent resource leak test (memory, FDs, sockets)."""

import time

from cephadm_agent.helpers import (
    agent_service_name,
    get_agent_daemons,
    get_fsid,
    get_node_for_host,
    log,
    setup_run,
)


def run_resource_leak_test(ceph_cluster, installer):
    log.info("=== TEST: Resource leak validation (memory/FD/sockets) ===")

    agents = get_agent_daemons(installer)
    assert len(agents) > 0
    hostname = agents[0]["hostname"]
    target_node = get_node_for_host(ceph_cluster, hostname)
    assert target_node is not None

    fsid = get_fsid(installer)
    service_name = agent_service_name(fsid, hostname)

    pid_out, _ = target_node.exec_command(
        sudo=True, cmd=f"systemctl show {service_name} --property=MainPID --value"
    )
    pid = pid_out.strip()
    assert pid and pid != "0", "Agent not running"
    log.info(f"Agent PID: {pid}")

    samples = []
    for i in range(7):
        rss_out, _ = target_node.exec_command(
            sudo=True,
            cmd=f"awk '/VmRSS/{{print $2}}' /proc/{pid}/status 2>/dev/null || echo 0",
        )
        fd_out, _ = target_node.exec_command(
            sudo=True, cmd=f"ls /proc/{pid}/fd 2>/dev/null | wc -l"
        )
        sock_out, _ = target_node.exec_command(
            sudo=True,
            cmd=f"ls -la /proc/{pid}/fd 2>/dev/null | grep -c socket || true",
        )
        samples.append(
            {
                "rss_kb": int((rss_out.strip().splitlines() or ["0"])[-1]),
                "fds": int((fd_out.strip().splitlines() or ["0"])[-1]),
                "sockets": int((sock_out.strip().splitlines() or ["0"])[-1]),
            }
        )
        log.info(
            f"  Sample {i+1}/7: RSS={samples[-1]['rss_kb']}kB "
            f"FDs={samples[-1]['fds']} sockets={samples[-1]['sockets']}"
        )
        if i < 6:
            time.sleep(30)

    rss_growth = samples[-1]["rss_kb"] - samples[0]["rss_kb"]
    fd_growth = samples[-1]["fds"] - samples[0]["fds"]
    sock_growth = samples[-1]["sockets"] - samples[0]["sockets"]

    log.info(
        f"Growth over 3min: RSS={rss_growth}kB FDs={fd_growth} sockets={sock_growth}"
    )

    assert (
        rss_growth < 5120
    ), f"Possible memory leak: RSS grew {rss_growth}kB in 3 minutes"
    assert fd_growth < 5, f"Possible FD leak: FDs grew by {fd_growth} in 3 minutes"
    assert (
        sock_growth < 3
    ), f"Possible socket leak: sockets grew by {sock_growth} in 3 minutes"

    log.info("PASS: No resource leaks detected")


TEST_REGISTRY = {
    "resource_leak": run_resource_leak_test,
}


def run(ceph_cluster, **kw):
    """Run agent resource leak tests."""
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
