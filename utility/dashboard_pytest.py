"""Run ceph_dashboard_api_automation after a successful cephci deploy.

Invoked only when run.py is given --dashboard-repo. The dashboard repo is a
separate checkout. This module does not import it.
"""

import os
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

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
        raise DashboardPytestError("--dashboard-marker requires --dashboard-repo")
    if pytest_path and not marker:
        raise DashboardPytestError("--dashboard-repo requires --dashboard-marker")
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
    rc = _stream_process(cmd, repo, env, console_path, popen or subprocess.Popen)
    try:
        publish_dashboard_report(log_dir)
    except Exception as exc:
        LOG.error("Could not publish the dashboard HTML report: %s", exc)
    return rc


def publish_dashboard_report(log_dir):
    """Publish the pytest HTML report where the suite directory page can link to it.

    ``index.html`` in the suite directory is written before pytest and is what
    the log server shows for that directory. Copy the session report to
    ``dashboard/index.html`` and add a relative link on the suite page.
    """
    dash_dir = os.path.join(log_dir, "dashboard")
    report = _newest_session_html(dash_dir)
    if not report:
        return
    index_path = os.path.join(dash_dir, "index.html")
    if os.path.abspath(report) != os.path.abspath(index_path):
        shutil.copyfile(report, index_path)
    suite_index = os.path.join(log_dir, "index.html")
    if not os.path.isfile(suite_index):
        return
    _link_dashboard_report(
        suite_index, _junit_counts(os.path.join(dash_dir, "junit.xml"))
    )


def _newest_session_html(dash_dir):
    """Return the latest ``session_*.html`` in the dashboard log directory."""
    if not os.path.isdir(dash_dir):
        return None
    matches = [
        os.path.join(dash_dir, name)
        for name in os.listdir(dash_dir)
        if name.startswith("session_") and name.endswith(".html")
    ]
    if not matches:
        return None
    return max(matches, key=os.path.getmtime)


def _junit_counts(path):
    """Return ``(tests, failed, skipped, seconds)`` from a pytest junit file."""
    if not os.path.isfile(path):
        return None
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError:
        return None
    suites = list(root) if root.tag == "testsuites" else [root]
    if not suites:
        return None
    tests = failures = errors = skipped = 0
    seconds = 0.0
    for suite in suites:
        tests += int(suite.attrib.get("tests", 0) or 0)
        failures += int(suite.attrib.get("failures", 0) or 0)
        errors += int(suite.attrib.get("errors", 0) or 0)
        skipped += int(suite.attrib.get("skipped", 0) or 0)
        seconds += float(suite.attrib.get("time", 0) or 0)
    return tests, failures + errors, skipped, seconds


def _result_summary(counts):
    """Comment text for the dashboard row, such as ``11 passed, 2 failed``."""
    if counts is None:
        return ""
    tests, failed, skipped, _seconds = counts
    passed = max(tests - failed - skipped, 0)
    parts = ["%d passed" % passed]
    if failed:
        parts.append("%d failed" % failed)
    if skipped:
        parts.append("%d skipped" % skipped)
    return ", ".join(parts)


def _dashboard_result_row(counts):
    """One Test Results row linking to ``dashboard/index.html``."""
    href = "./dashboard/index.html"
    failed = counts[1] if counts else 0
    if failed:
        status = "Failed"
        color = "red"
    else:
        status = "Pass"
        color = "green"
    duration = ""
    if counts and counts[3]:
        duration = str(timedelta(seconds=counts[3]))
    return (
        "<tr>"
        '<td><a href="{href}">Dashboard API results</a></td>'
        "<td>Pytest results for the dashboard API.</td>"
        "<td>{duration}</td>"
        '<td><a style="color: {color};" href="{href}">{status}</a></td>'
        "<td>{comments}</td>"
        "</tr>"
    ).format(
        href=href,
        duration=duration,
        color=color,
        status=status,
        comments=_result_summary(counts),
    )


def _insert_result_row(html, row):
    """Place the row after Export dashboard endpoint, still inside its table."""
    marker = "Export dashboard endpoint"
    start = html.find(marker)
    if start != -1:
        end = html.find("</tr>", start)
        if end != -1:
            end += len("</tr>")
            return html[:end] + "\n" + row + html[end:]
    if "</body>" in html:
        return html.replace("</body>", row + "\n</body>", 1)
    return html + "\n" + row + "\n"


def _link_dashboard_report(suite_index, counts):
    """Insert a Test Results row that links to the dashboard report."""
    with open(suite_index, encoding="utf-8") as handle:
        html = handle.read()
    if "./dashboard/index.html" in html:
        return
    html = _insert_result_row(html, _dashboard_result_row(counts))
    with open(suite_index, "w", encoding="utf-8") as handle:
        handle.write(html)
