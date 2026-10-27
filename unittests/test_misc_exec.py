"""Exercise the exec workflow with CephNode's two command return contracts."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from ceph.ceph import CommandFailed
from tests.misc_env import exec as workflow


@pytest.mark.parametrize("result", [("output", ""), ("", "error output")])
def test_short_command_preserves_stdout_and_stderr(result):
    node = Mock()
    node.exec_command.return_value = result
    assert workflow.exec_command(node, cmd="test-command", check_ec=False) == result


def test_long_command_success_has_no_captured_output():
    node = Mock()
    node.exec_command.return_value = 0
    assert workflow.exec_command(
        node, cmd="dnf install -y cephadm", long_running=True, check_ec=True
    ) == ("", "")
    node.exec_command.assert_called_once_with(
        cmd="dnf install -y cephadm", long_running=True, check_ec=True
    )


@pytest.mark.parametrize("check_ec", [None, True])
def test_long_command_nonzero_status_cannot_pass_when_checked(check_ec):
    node = Mock()
    node.exec_command.return_value = 7
    kwargs = {"cmd": "failing-command", "long_running": True}
    if check_ec is not None:
        kwargs["check_ec"] = check_ec
    with pytest.raises(CommandFailed, match="failing-command.*status 7"):
        workflow.exec_command(node, **kwargs)


def test_long_command_explicitly_unchecked_failure_is_reported(monkeypatch):
    node = Mock()
    node.exec_command.return_value = 7
    log = Mock()
    monkeypatch.setattr(workflow, "LOG", log)
    assert workflow.exec_command(
        node, cmd="expected-failure", long_running=True, check_ec=False
    ) == ("", "")
    log.warning.assert_called_once()
    assert "status 7" in log.warning.call_args.args[0]
    assert "checking is disabled" in log.warning.call_args.args[0]


@pytest.mark.parametrize("long_running", [False, True])
def test_node_command_failures_propagate_unchanged(long_running):
    node = Mock()
    error = CommandFailed("node rejected command")
    node.exec_command.side_effect = error
    with pytest.raises(CommandFailed) as caught:
        workflow.exec_command(
            node, cmd="failing-command", long_running=long_running, check_ec=True
        )
    assert caught.value is error


@pytest.fixture
def run_environment():
    first, second = Mock(), Mock()
    cluster = Mock(name="ceph-cluster")
    cluster.name = "ceph"
    cluster.get_nodes.return_value = [first, second]
    config = {
        "role": "osd",
        "idx": 1,
        "sudo": True,
        "long_running": True,
        "check_status": True,
        "rhbuild": "9.2-rhel-9",
        "commands": [
            "dnf install -y --nogpgcheck cephadm",
            "test -x /usr/sbin/cephadm",
        ],
    }
    return SimpleNamespace(cluster=cluster, first=first, node=second, config=config)


@pytest.mark.parametrize("long_running", [False, True])
def test_run_completes_checked_commands_on_selected_node(run_environment, long_running):
    env = run_environment
    env.config["long_running"] = long_running
    env.node.exec_command.return_value = 0 if long_running else ("installed", "")
    assert workflow.run(env.cluster, config=env.config, ceph_cluster_dict={}) == 0
    assert env.node.exec_command.call_count == 2
    env.first.exec_command.assert_not_called()
    for call in env.node.exec_command.call_args_list:
        assert call.kwargs["check_ec"] is True
        assert call.kwargs["long_running"] is long_running


@pytest.mark.parametrize("long_running", [False, True])
def test_run_stops_after_checked_command_failure(run_environment, long_running):
    env = run_environment
    env.config["long_running"] = long_running
    env.node.exec_command.side_effect = CommandFailed("installation failed")
    assert workflow.run(env.cluster, config=env.config, ceph_cluster_dict={}) == 1
    assert env.node.exec_command.call_count == 1


def test_run_rejects_raw_nonzero_long_command_status(run_environment):
    env = run_environment
    env.node.exec_command.return_value = 3
    assert workflow.run(env.cluster, config=env.config, ceph_cluster_dict={}) == 1
    assert env.node.exec_command.call_count == 1
