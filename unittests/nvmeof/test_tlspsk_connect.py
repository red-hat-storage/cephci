"""Plain-connect checks must not treat a down listener as TLS enforcement."""

import pytest

from ceph.ceph import CommandFailed
from tests.nvmeof.workflows import tlspsk
from tests.nvmeof.workflows.tlspsk import (
    assert_plain_connect_refused,
    connect_error_is_stale_psk,
    connect_with_tls_until,
    host_psk_args,
    nvmeof_daemons_restarted,
    nvmeof_daemons_serving,
)


class _Node:
    def __init__(self, exc=None):
        self.exc = exc

    def exec_command(self, cmd, sudo=False):
        if self.exc:
            raise self.exc
        return "connected", ""


def test_connection_refused_is_listener_down():
    node = _Node(
        CommandFailed(
            "nvme connect returned could not add new controller: connection refused"
        )
    )
    with pytest.raises(AssertionError, match="listener is down"):
        assert_plain_connect_refused(node, "10.0.66.110", 4420, "nqn.example")


def test_nvme_rejection_counts_as_tls_enforced():
    node = _Node(
        CommandFailed(
            "nvme connect returned failed to write to nvme-fabrics device: "
            "Invalid argument"
        )
    )
    assert_plain_connect_refused(node, "10.0.66.110", 4420, "nqn.example")


def test_successful_plain_connect_means_tls_lost():
    with pytest.raises(AssertionError, match="TLS enforcement lost"):
        assert_plain_connect_refused(_Node(), "10.0.66.110", 4420, "nqn.example")


def test_daemons_serving_requires_running_and_not_user_stopped():
    stopped = [
        {"status_desc": "stopped", "user_stopped": True},
        {"status_desc": "running", "user_stopped": False},
    ]
    running = [
        {"status_desc": "running", "user_stopped": False},
        {"status_desc": "running", "user_stopped": False},
    ]
    assert nvmeof_daemons_serving(stopped) is False
    assert nvmeof_daemons_serving([]) is False
    assert nvmeof_daemons_serving(running) is True


def test_restart_waits_for_a_new_daemon_generation():
    before = [
        {
            "daemon_name": "nvmeof.node5",
            "status_desc": "running",
            "user_stopped": False,
            "started": "t0",
        }
    ]
    still_old = [
        {
            "daemon_name": "nvmeof.node5",
            "status_desc": "running",
            "user_stopped": False,
            "started": "t0",
        }
    ]
    restarted = [
        {
            "daemon_name": "nvmeof.node5",
            "status_desc": "running",
            "user_stopped": False,
            "started": "t1",
        }
    ]
    assert nvmeof_daemons_restarted(before, still_old) is False
    assert nvmeof_daemons_restarted(before, restarted) is True


def test_host_add_includes_dhchap_key_when_the_subsystem_has_one():
    args = host_psk_args("nqn.sub", "nqn.host", "NVMeTLSkey-1:01:abc:", "DHHC-1:00:def:")
    assert args["psk"] == "NVMeTLSkey-1:01:abc:"
    assert args["dhchap-key"] == "DHHC-1:00:def:"


def test_host_add_without_dhchap_omits_the_key():
    args = host_psk_args("nqn.sub", "nqn.host", "NVMeTLSkey-1:01:abc:")
    assert "dhchap-key" not in args


def test_fabrics_write_failure_is_a_stale_host_psk():
    exc = CommandFailed(
        "could not add new controller: failed to write to nvme-fabrics device"
    )
    assert connect_error_is_stale_psk(exc) is True


def test_listener_down_is_not_a_stale_host_psk():
    exc = CommandFailed("failed to connect to remote host: connection refused")
    assert connect_error_is_stale_psk(exc) is False


class _Scripted:
    def __init__(self, results):
        self.results = list(results)
        self.calls = 0

    def exec_command(self, cmd, sudo=False):
        self.calls += 1
        item = self.results.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def test_peer_connect_retries_until_the_new_psk_is_present(monkeypatch):
    monkeypatch.setattr(tlspsk.time, "sleep", lambda _seconds: None)
    node = _Scripted(
        [
            CommandFailed("failed to write to nvme-fabrics device"),
            ("connected", ""),
        ]
    )
    out, _err = connect_with_tls_until(
        node, "10.0.64.215", 4420, "nqn.example", timeout=45, interval=5
    )
    assert out == "connected"
    assert node.calls == 2


def test_listener_down_is_not_retried(monkeypatch):
    slept = []
    monkeypatch.setattr(tlspsk.time, "sleep", slept.append)
    node = _Scripted([CommandFailed("connection refused")])
    with pytest.raises(AssertionError):
        connect_with_tls_until(
            node, "10.0.64.215", 4420, "nqn.example", timeout=45, interval=5
        )
    assert node.calls == 1
    assert slept == []


def test_stale_psk_past_the_poll_window_fails(monkeypatch):
    clock = {"now": 0.0}

    def monotonic():
        return clock["now"]

    def sleep(seconds):
        clock["now"] += seconds

    monkeypatch.setattr(tlspsk.time, "monotonic", monotonic)
    monkeypatch.setattr(tlspsk.time, "sleep", sleep)
    node = _Scripted(
        [CommandFailed("failed to write to nvme-fabrics device") for _ in range(20)]
    )
    with pytest.raises(AssertionError, match="failed to write to nvme-fabrics"):
        connect_with_tls_until(
            node, "10.0.64.215", 4420, "nqn.example", timeout=45, interval=5
        )
    assert node.calls == 10
