"""Run ceph_dashboard_api_automation after a successful cephci deploy.

Invoked only when run.py is given --dashboard-pytest. The dashboard repo is a
separate checkout. This module does not import it.
"""

import os
import subprocess
import sys
from datetime import datetime

from utility.log import Log

LOG = Log(__name__)


class DashboardPytestError(Exception):
    """The dashboard pytest flags are inconsistent or the checkout is missing."""


def validate_dashboard_pytest_args(pytest_path, marker):
    """Check the optional dashboard flags before any VM is created.

    Both absent is the existing cephci path and returns None.
    """
    pytest_path = (pytest_path or "").strip()
    marker = (marker or "").strip()
    if marker and not pytest_path:
        raise DashboardPytestError("--dashboard-marker requires --dashboard-pytest")
    if pytest_path and not marker:
        raise DashboardPytestError("--dashboard-pytest requires --dashboard-marker")
    if not pytest_path:
        return None
    absolute = os.path.abspath(pytest_path)
    if not os.path.isdir(absolute):
        raise DashboardPytestError(
            "dashboard pytest path does not exist: %s" % absolute
        )
    return absolute


def ensure_dashboard_venv(repo, python=None, run=None):
    """Return the dashboard venv python, creating the venv when it is missing."""
    python = python or sys.executable
    run = run or subprocess.run
    venv_python = os.path.join(repo, ".venv", "bin", "python")
    if os.path.isfile(venv_python):
        return venv_python
    LOG.info("Creating dashboard virtualenv at %s", os.path.join(repo, ".venv"))
    run([python, "-m", "venv", os.path.join(repo, ".venv")], check=True)
    run(
        [venv_python, "-m", "pip", "install", "-r", "requirements.txt"],
        cwd=repo,
        check=True,
    )
    return venv_python


def _stream_process(cmd, cwd, env, console_path, popen):
    with open(console_path, "w", encoding="utf-8") as console:
        proc = popen(
            cmd,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        for line in proc.stdout:
            console.write(line)
            console.flush()
            sys.stdout.write(line)
            sys.stdout.flush()
        return proc.wait()


def run_dashboard_pytest(repo, marker, log_dir, popen=None, venv_python=None):
    """Run pytest in the dashboard repo using dashboard_endpoint.json.

    Logs and junit land under <log_dir>/dashboard/. Returns the pytest exit code.
    Returns 1 when the endpoint file is missing. Does not log the password.
    """
    import json

    endpoint_path = os.path.join(log_dir, "dashboard_endpoint.json")
    if not os.path.isfile(endpoint_path):
        LOG.error(
            "dashboard_endpoint.json is missing at %s; not starting pytest",
            endpoint_path,
        )
        return 1

    with open(endpoint_path, encoding="utf-8") as handle:
        endpoint = json.load(handle)

    repo = os.path.abspath(repo)
    python = venv_python or ensure_dashboard_venv(repo)
    dash_dir = os.path.join(log_dir, "dashboard")
    os.makedirs(dash_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(dash_dir, "session_%s.log" % stamp)
    junit = os.path.join(dash_dir, "junit.xml")
    console_path = os.path.join(dash_dir, "pytest.out")
    cmd = [
        python,
        "-m",
        "pytest",
        "-m",
        marker,
        "--junitxml=%s" % junit,
        "--log-file=%s" % log_file,
    ]
    env = os.environ.copy()
    env["DASHBOARD_HOST"] = str(endpoint["host"])
    env["DASHBOARD_PORT"] = str(endpoint["port"])
    env["ADMIN_USER"] = str(endpoint["username"])
    env["ADMIN_PASSWORD"] = str(endpoint["password"])
    env["VERIFY_TLS"] = "false"

    LOG.info("Dashboard pytest marker: %s", marker)
    LOG.info("Dashboard pytest log: %s", log_file)
    LOG.info("Dashboard pytest junit: %s", junit)
    return _stream_process(cmd, repo, env, console_path, popen or subprocess.Popen)
