"""Exercise explicit runtime bootstrap SSH paths without changing suite defaults."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from cli.exceptions import ConfigError
from tests.cephadm import test_cephadm_bootstrap as workflow


@pytest.fixture
def environment(monkeypatch):
    node = Mock()
    cluster = Mock()
    cluster.get_nodes.return_value = [node]
    config = {
        "bootstrap": {
            "mon-ip": "node1",
            "ssh-user": "root",
            "apply-spec": {
                "file_type": "yaml",
                "spec": [{"service_type": "host", "hostname": "node1"}],
            },
        },
        "rhbuild": "9.0-rhel-9",
        "build_type": "stable",
        "ibm_build": True,
        "base_url": "https://example.test/tools",
        "container_image": "example.test/ceph:stable",
    }
    setup = Mock()
    bootstrap = Mock()
    monkeypatch.setattr(workflow, "setup_installer_node", setup)
    monkeypatch.setattr(
        workflow, "get_tools_repo", Mock(return_value=config["base_url"])
    )
    monkeypatch.setattr(workflow, "bootstrap", bootstrap)
    monkeypatch.setattr(workflow, "set_container_image_config", Mock())
    return SimpleNamespace(
        cluster=cluster, node=node, config=config, setup=setup, bootstrap=bootstrap
    )


def test_runtime_ssh_paths_reach_bootstrap_without_mutating_original_config(
    environment, tmp_path
):
    env = environment
    original = deepcopy(env.config)
    private_path = str(tmp_path / "provided-key-file")
    public_path = str(tmp_path / "provided-key-file.pub")
    runtime = {
        "bootstrap-ssh-private-key": private_path,
        "bootstrap-ssh-public-key": public_path,
    }
    runtime_original = deepcopy(runtime)

    def bootstrap(**kwargs):
        assert kwargs["ssh-private-key"] == private_path
        assert kwargs["ssh-public-key"] == public_path
        # The real bootstrap resolves hosts and consumes nested file_type values.
        kwargs["apply-spec"].pop("file_type")
        kwargs["apply-spec"]["spec"][0]["hostname"] = "resolved-installer"

    env.bootstrap.side_effect = bootstrap

    assert (
        workflow.run(
            env.cluster, config=env.config, test_data={"custom_config_dict": runtime}
        )
        == 0
    )
    assert env.config == original
    assert runtime == runtime_original
    env.bootstrap.assert_called_once()


@pytest.mark.parametrize(
    "test_data",
    [
        {},
        {"custom_config_dict": {}},
        {"custom_config_dict": {"unrelated-option": "value"}},
    ],
)
def test_missing_runtime_overrides_preserve_normal_bootstrap_defaults(
    environment, test_data
):
    env = environment
    original = deepcopy(env.config)

    assert workflow.run(env.cluster, config=env.config, test_data=test_data) == 0

    flags = env.bootstrap.call_args.kwargs
    assert "ssh-private-key" not in flags
    assert "ssh-public-key" not in flags
    assert flags["mon-ip"] == "node1"
    assert flags["ssh-user"] == "root"
    assert env.config == original


def test_runtime_override_takes_precedence_over_existing_suite_key_path(
    environment, tmp_path
):
    env = environment
    suite_path = str(tmp_path / "suite-key-file")
    runtime_path = str(tmp_path / "runtime-key-file")
    env.config["bootstrap"]["ssh-private-key"] = suite_path

    assert (
        workflow.run(
            env.cluster,
            config=env.config,
            test_data={
                "custom_config_dict": {"bootstrap-ssh-private-key": runtime_path}
            },
        )
        == 0
    )

    assert env.bootstrap.call_args.kwargs["ssh-private-key"] == runtime_path
    assert env.config["bootstrap"]["ssh-private-key"] == suite_path


def test_existing_suite_flags_are_preserved_without_runtime_overrides(
    environment, tmp_path
):
    env = environment
    suite_path = str(tmp_path / "existing-key-file")
    env.config["bootstrap"]["ssh-public-key"] = suite_path

    assert workflow.run(env.cluster, config=env.config) == 0

    assert env.bootstrap.call_args.kwargs["ssh-public-key"] == suite_path


@pytest.mark.parametrize(
    "option", ["bootstrap-ssh-private-key", "bootstrap-ssh-public-key"]
)
@pytest.mark.parametrize("value", [None, "", " ", 123])
def test_invalid_explicit_runtime_paths_fail_before_host_setup(
    environment, option, value
):
    env = environment

    with pytest.raises(ConfigError, match=option):
        workflow.run(
            env.cluster,
            config=env.config,
            test_data={"custom_config_dict": {option: value}},
        )

    env.setup.assert_not_called()
    env.bootstrap.assert_not_called()
