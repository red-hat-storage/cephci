"""Retry NVMe-oF CLI calls while a gateway is restarting or redeploying."""

import pytest

from ceph.ceph import CommandFailed
from ceph.nvmeof.cli.v2.common import run_with_connect_retry


def test_retries_while_gateway_is_going_down():
    calls = {"n": 0}

    def cmd():
        calls["n"] += 1
        if calls["n"] < 3:
            raise CommandFailed(
                "Error EINVAL: Gateway client.nvmeof.nvmeof.group.node5.htcxhs "
                "is going down"
            )
        return "deleted"

    assert run_with_connect_retry(cmd, timeout=5, interval=0) == "deleted"
    assert calls["n"] == 3


def test_retries_connect_refused_to_all_addresses():
    calls = {"n": 0}

    def cmd():
        calls["n"] += 1
        if calls["n"] == 1:
            raise CommandFailed(
                "Error EINVAL: failed to connect to all addresses; "
                "last error: Connection refused"
            )
        return "ok"

    assert run_with_connect_retry(cmd, timeout=5, interval=0) == "ok"
    assert calls["n"] == 2


def test_unrelated_error_is_not_retried():
    calls = {"n": 0}

    def cmd():
        calls["n"] += 1
        raise CommandFailed("Error EINVAL: subsystem does not exist")

    with pytest.raises(CommandFailed, match="subsystem does not exist"):
        run_with_connect_retry(cmd, timeout=5, interval=0)
    assert calls["n"] == 1
