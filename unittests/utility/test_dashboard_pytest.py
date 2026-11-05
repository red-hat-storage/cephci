"""Tests for the dashboard endpoint handoff and the optional pytest runner."""

import importlib.util
import json
import os
from pathlib import Path

import docopt
import pytest
import yaml

from utility.dashboard_pytest import (
    DashboardPytestError,
    ensure_dashboard_venv,
    run_dashboard_pytest,
    validate_dashboard_pytest_args,
)


def _export_module():
    path = Path("tests/misc_env/export_dashboard_endpoint.py")
    spec = importlib.util.spec_from_file_location("export_dashboard_endpoint", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_EXPORT = _export_module()
find_bootstrap_credentials = _EXPORT.find_bootstrap_credentials
host_for_client = _EXPORT.host_for_client
parse_bootstrap_banner = _EXPORT.parse_bootstrap_banner
parse_dashboard_url = _EXPORT.parse_dashboard_url
wait_until_usable = _EXPORT.wait_until_usable
write_endpoint = _EXPORT.write_endpoint

_BOOTSTRAP_LOG = """
other output
Ceph Dashboard is now available at:

    URL: https://node1.example.com:8443/
    User: admin
    Password: generated-secret
"""


def _run_py_usage():
    text = Path("run.py").read_text(encoding="utf-8")
    start = text.index('doc = """') + len('doc = """')
    end = text.index('"""', start)
    return text[start:end]


def test_docopt_dashboard_flags_are_optional():
    usage = _run_py_usage()
    args = docopt.docopt(
        usage,
        argv=[
            "--release",
            "8.1",
            "--platform",
            "rhel-9",
            "--suite",
            "s.yaml",
            "--global-conf",
            "g.yaml",
        ],
    )
    assert args["--dashboard-pytest"] is None
    assert args["--dashboard-marker"] is None


def test_docopt_accepts_dashboard_flags():
    usage = _run_py_usage()
    args = docopt.docopt(
        usage,
        argv=[
            "--release",
            "8.1",
            "--platform",
            "rhel-9",
            "--suite",
            "s.yaml",
            "--global-conf",
            "g.yaml",
            "--dashboard-pytest",
            "/tmp/dash",
            "--dashboard-marker",
            "p0 and readonly",
        ],
    )
    assert args["--dashboard-pytest"] == "/tmp/dash"
    assert args["--dashboard-marker"] == "p0 and readonly"


def test_docopt_cleanup_usage_unchanged():
    usage = _run_py_usage()
    args = docopt.docopt(
        usage, argv=["--cleanup", "instance", "--osp-cred", "cred.yaml"]
    )
    assert args["--cleanup"] == "instance"
    assert args["--dashboard-pytest"] is None


def test_validate_dashboard_flags_absent():
    assert validate_dashboard_pytest_args(None, None) is None
    assert validate_dashboard_pytest_args("", "  ") is None


def test_validate_dashboard_flags_require_each_other(tmp_path):
    with pytest.raises(DashboardPytestError, match="requires --dashboard-pytest"):
        validate_dashboard_pytest_args(None, "smoke")
    with pytest.raises(DashboardPytestError, match="requires --dashboard-marker"):
        validate_dashboard_pytest_args(str(tmp_path), None)


def test_validate_dashboard_flags_missing_checkout():
    with pytest.raises(DashboardPytestError, match="does not exist"):
        validate_dashboard_pytest_args("/no/such/dashboard/checkout", "smoke")


def test_validate_dashboard_flags_returns_absolute_path(tmp_path):
    found = validate_dashboard_pytest_args(str(tmp_path), "smoke")
    assert found == str(tmp_path)


def test_parse_dashboard_url():
    assert parse_dashboard_url("https://10.1.2.3:8443/") == ("10.1.2.3", 8443)
    assert parse_dashboard_url("https://mgr.example.com:8444/") == (
        "mgr.example.com",
        8444,
    )
    with pytest.raises(ValueError):
        parse_dashboard_url("https://mgr.example.com")


def test_parse_bootstrap_banner_reads_generated_password():
    parsed = parse_bootstrap_banner(_BOOTSTRAP_LOG)
    assert parsed["username"] == "admin"
    assert parsed["password"] == "generated-secret"
    assert parsed["url"].startswith("https://node1.example.com")


def test_find_bootstrap_credentials_ignores_subdirectory(tmp_path):
    (tmp_path / "startup.log").write_text("no banner\n", encoding="utf-8")
    nested = tmp_path / "dashboard"
    nested.mkdir()
    (nested / "session_1.log").write_text(_BOOTSTRAP_LOG, encoding="utf-8")
    assert find_bootstrap_credentials(str(tmp_path)) is None
    (tmp_path / "Test_cluster_deployment_using_cephadm_0.log").write_text(
        _BOOTSTRAP_LOG, encoding="utf-8"
    )
    assert find_bootstrap_credentials(str(tmp_path))["password"] == "generated-secret"


def test_host_for_client_rewrites_hostname_to_ip():
    class Node:
        hostname = "node1.example.com"
        shortname = "node1"
        ip_address = "10.1.2.3"

    assert host_for_client("node1.example.com", [Node()]) == "10.1.2.3"
    assert host_for_client("10.9.9.9", [Node()]) == "10.9.9.9"


def test_wait_until_usable_accepts_warn_and_rejects_persistent_err():
    calls = {"n": 0}

    def exec_cmd(_command):
        calls["n"] += 1
        status = "HEALTH_ERR" if calls["n"] == 1 else "HEALTH_WARN"
        return json.dumps({"status": status})

    clock = {"now": 0}

    def now():
        return clock["now"]

    def sleep(_interval):
        clock["now"] += 15

    assert (
        wait_until_usable(exec_cmd, "sudo cephadm shell --", 60, sleep=sleep, now=now)
        == "HEALTH_WARN"
    )

    def always_err(_command):
        return json.dumps({"status": "HEALTH_ERR"})

    clock["now"] = 0
    with pytest.raises(RuntimeError, match="HEALTH_ERR"):
        wait_until_usable(always_err, "sudo cephadm shell --", 10, sleep=sleep, now=now)


def test_write_endpoint_is_restricted(tmp_path):
    path = write_endpoint(
        str(tmp_path),
        {"host": "10.0.0.1", "port": 8443, "username": "admin", "password": "secret"},
    )
    assert json.loads(Path(path).read_text(encoding="utf-8"))["password"] == "secret"
    assert oct(os.stat(path).st_mode & 0o777) == oct(0o600)
    assert os.path.basename(path) == "dashboard_endpoint.json"


def test_deploy_suite_pins_dashboard_login_and_exports_endpoint():
    root = Path("suites")
    suites = sorted(root.glob("*/dashboard/deploy.yaml"))
    assert [path.relative_to(root).parts[0] for path in suites] == [
        "reef",
        "squid",
        "tentacle",
        "umbrella",
    ]
    texts = [path.read_text(encoding="utf-8") for path in suites]
    assert len(set(texts)) == 1
    loaded = yaml.safe_load(texts[0])
    names = [item["test"]["name"] for item in loaded["tests"]]
    assert names[-1] == "Export dashboard endpoint"
    bootstrap = loaded["tests"][1]["test"]["config"]["args"]
    assert bootstrap["initial-dashboard-user"] == "admin"
    assert bootstrap["initial-dashboard-password"] == "admin@123"
    assert bootstrap["dashboard-password-noupdate"] is True
    export = loaded["tests"][-1]["test"]
    assert export["module"] == "export_dashboard_endpoint.py"
    assert export["config"]["password"] == bootstrap["initial-dashboard-password"]


def test_ensure_dashboard_venv_skips_install_when_present(tmp_path):
    venv_python = tmp_path / ".venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True)
    venv_python.write_text("", encoding="utf-8")
    calls = []

    def run(*_args, **_kwargs):
        calls.append(1)

    assert ensure_dashboard_venv(str(tmp_path), run=run) == str(venv_python)
    assert calls == []


def test_run_dashboard_pytest_writes_under_dashboard_dir(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    log_dir = tmp_path / "cephci-logs"
    log_dir.mkdir()
    (log_dir / "dashboard_endpoint.json").write_text(
        json.dumps(
            {
                "host": "10.2.3.4",
                "port": 8443,
                "username": "admin",
                "password": "admin@123",
            }
        ),
        encoding="utf-8",
    )
    seen = {}

    class Proc:
        stdout = iter(["collected 1 item\n"])

        @staticmethod
        def wait():
            return 0

    def popen(cmd, cwd, env, stdout, stderr, text):
        seen["cmd"] = cmd
        seen["cwd"] = cwd
        seen["env"] = env
        return Proc()

    rc = run_dashboard_pytest(
        str(repo),
        "smoke",
        str(log_dir),
        popen=popen,
        venv_python="/usr/bin/python3",
    )
    assert rc == 0
    assert seen["cwd"] == str(repo)
    assert seen["env"]["DASHBOARD_HOST"] == "10.2.3.4"
    assert seen["env"]["ADMIN_PASSWORD"] == "admin@123"
    assert seen["cmd"][4] == "smoke"
    assert seen["cmd"][5].startswith("--junitxml=")
    assert "/dashboard/junit.xml" in seen["cmd"][5].replace("\\", "/")
    assert "/dashboard/session_" in seen["cmd"][6].replace("\\", "/")
    assert (log_dir / "dashboard" / "pytest.out").read_text(encoding="utf-8") == (
        "collected 1 item\n"
    )
    assert not list(log_dir.glob("session_*.log"))


def test_run_falls_back_to_bootstrap_log(tmp_path):
    (tmp_path / "Test_cluster_deployment_using_cephadm_0.log").write_text(
        _BOOTSTRAP_LOG, encoding="utf-8"
    )

    class Node:
        hostname = "node1.example.com"
        shortname = "node1"
        ip_address = "10.8.8.8"

        @staticmethod
        def exec_command(cmd):
            raise RuntimeError("mgr services unavailable: %s" % cmd)

    class Cluster:
        @staticmethod
        def get_nodes(role=None):
            return [Node()]

    rc = _EXPORT.run(
        Cluster(),
        {"health-wait-seconds": 0},
        run_config={"log_dir": str(tmp_path)},
    )
    assert rc == 0
    endpoint = json.loads(
        (tmp_path / "dashboard_endpoint.json").read_text(encoding="utf-8")
    )
    assert endpoint["host"] == "10.8.8.8"
    assert endpoint["port"] == 8443
    assert endpoint["username"] == "admin"
    assert endpoint["password"] == "generated-secret"
    assert not (tmp_path / "dashboard").exists()


def test_run_dashboard_pytest_skips_when_endpoint_missing(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    log_dir = tmp_path / "logs"
    log_dir.mkdir()

    def popen(*_args, **_kwargs):
        raise AssertionError("pytest must not start without an endpoint file")

    assert (
        run_dashboard_pytest(
            str(repo), "smoke", str(log_dir), popen=popen, venv_python="python"
        )
        == 1
    )
    assert not (log_dir / "dashboard").exists()
