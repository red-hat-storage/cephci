"""Unit tests for utility.cephadm_agent_config."""

from utility.cephadm_agent_config import (
    USE_AGENT_CONF_KEY,
    USE_AGENT_KEY,
    apply_use_agent_to_bootstrap_config,
    apply_use_agent_to_cli_bootstrap_config,
    resolve_custom_overrides,
    use_agent_requested,
)


def test_resolve_overrides_from_list_and_dict():
    assert resolve_custom_overrides(["use_agent=true", "ibm-build=true"]) == {
        "use_agent": "true",
        "ibm-build": "true",
    }
    assert resolve_custom_overrides({"use_agent": "true"})["use_agent"] == "true"
    assert resolve_custom_overrides(
        ["use_agent=false"], overrides={"use_agent": "true"}
    )["use_agent"] == "true"


def test_use_agent_requested():
    assert use_agent_requested(["use_agent=true"])
    assert use_agent_requested(["use_agent=1"])
    assert use_agent_requested(overrides={USE_AGENT_KEY: True})
    assert not use_agent_requested(["use_agent=false"])
    assert not use_agent_requested([])
    assert not use_agent_requested(None)


def test_apply_use_agent_cephadmin_noop_without_flag():
    args = {"mon-ip": "node1"}
    apply_use_agent_to_bootstrap_config(args, overrides={})
    assert "config" not in args


def test_apply_use_agent_cephadmin_merges_when_flag_set():
    args = {
        "mon-ip": "node1",
        "config": {"global": {"public_network": "10.0.0.0/24"}},
    }
    apply_use_agent_to_bootstrap_config(args, overrides={USE_AGENT_KEY: "true"})
    assert args["config"]["global"]["public_network"] == "10.0.0.0/24"
    assert args["config"]["mgr"][USE_AGENT_CONF_KEY] == "true"


def test_apply_use_agent_cephadmin_suite_mgr_wins():
    args = {
        "config": {"mgr": {USE_AGENT_CONF_KEY: "false"}},
    }
    apply_use_agent_to_bootstrap_config(args, overrides={USE_AGENT_KEY: "true"})
    # setdefault — suite/explicit value preserved
    assert args["config"]["mgr"][USE_AGENT_CONF_KEY] == "false"


def test_apply_use_agent_cli_creates_config():
    bootstrap = {"mon-ip": "node1"}
    apply_use_agent_to_cli_bootstrap_config(
        bootstrap, overrides={USE_AGENT_KEY: "true"}
    )
    assert bootstrap["config"]["file_type"] == "ini"
    assert USE_AGENT_CONF_KEY + "=true" in bootstrap["config"]["mgr"]


def test_apply_use_agent_cli_appends_to_existing_mgr_string():
    bootstrap = {
        "config": {
            "file_type": "ini",
            "mgr": "mgr/cephadm/container_image_grafana=x",
        }
    }
    apply_use_agent_to_cli_bootstrap_config(
        bootstrap, overrides={USE_AGENT_KEY: True}
    )
    mgr = bootstrap["config"]["mgr"]
    assert "mgr/cephadm/container_image_grafana=x" in mgr
    assert f"{USE_AGENT_CONF_KEY}=true" in mgr


def test_apply_use_agent_cli_idempotent():
    bootstrap = {
        "config": {
            "file_type": "ini",
            "mgr": f"{USE_AGENT_CONF_KEY}=true",
        }
    }
    apply_use_agent_to_cli_bootstrap_config(
        bootstrap, overrides={USE_AGENT_KEY: "true"}
    )
    assert bootstrap["config"]["mgr"].count("use_agent=") == 1
