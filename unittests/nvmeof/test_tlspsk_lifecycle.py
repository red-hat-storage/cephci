"""The TLS-PSK lifecycle deploys the gateway service once and always tears it down."""

import pytest

from tests.nvmeof.test_ceph_nvmeof_tlspsk_lifecycle import (
    LIFECYCLE_PHASES,
    execute_lifecycle,
)


def test_lifecycle_phase_order():
    assert LIFECYCLE_PHASES == (
        "connect",
        "rotate",
        "keep_connections",
        "alb",
        "dhchap",
        "masking",
        "restart",
        "ha",
        "listener_abort",
        "mode_switch",
    )


def test_lifecycle_deploys_once_and_tears_down_after_every_phase():
    calls = []

    def deploy():
        calls.append("deploy")

    def run_phase(phase):
        calls.append(phase)

    def restore_daemons():
        calls.append("restore")

    def teardown():
        calls.append("teardown")

    execute_lifecycle(
        LIFECYCLE_PHASES, deploy, run_phase, restore_daemons, teardown
    )
    assert calls == ["deploy", *LIFECYCLE_PHASES, "restore", "teardown"]


def test_failed_phase_skips_later_phases_and_still_tears_down():
    calls = []

    def deploy():
        calls.append("deploy")

    def run_phase(phase):
        calls.append(phase)
        if phase == "ha":
            raise RuntimeError("ha failed")

    def restore_daemons():
        calls.append("restore")

    def teardown():
        calls.append("teardown")

    with pytest.raises(RuntimeError, match="ha failed"):
        execute_lifecycle(
            LIFECYCLE_PHASES, deploy, run_phase, restore_daemons, teardown
        )
    assert calls == [
        "deploy",
        "connect",
        "rotate",
        "keep_connections",
        "alb",
        "dhchap",
        "masking",
        "restart",
        "ha",
        # listener_abort and mode_switch skipped after ha failure
        "restore",
        "teardown",
    ]


def test_teardown_runs_when_daemon_restore_fails():
    calls = []

    def restore_daemons():
        calls.append("restore")
        raise RuntimeError("daemon still stopped")

    def teardown():
        calls.append("teardown")

    with pytest.raises(RuntimeError, match="daemon still stopped"):
        execute_lifecycle(
            (),
            lambda: calls.append("deploy"),
            lambda phase: None,
            restore_daemons,
            teardown,
        )
    assert calls == ["deploy", "restore", "teardown"]
