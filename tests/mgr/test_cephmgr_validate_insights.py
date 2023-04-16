import json
import tempfile

from cli.cephadm.cephadm import CephAdm
from utility.log import Log

log = Log(__name__)

AUDIT_LOG_PATH = "/var/log/ceph/cephadm.log"


class CephInsightOpsFailure(Exception):
    pass


def _ceph_run_insight(node, step_to_validate):
    """
    Method verifies whether the insights command executes without any errors,
    verify whether the report is generated and
    whether the generated report contains all the sections required
    Args:
        node (str): Node object where the command has to be executed
        step_to_validate (str): The validation step to be performed

    Returns: Returns exception is the checks failed.

    """
    # Check 1: Run ceph insight cluster and verify the command succeeded
    out = CephAdm(node).ceph.insights()
    if out:
        CephInsightOpsFailure("Failed to get ceph insights")

    # Check 2: Check whether the report generation was successful
    if step_to_validate in [
        "verify-report",
        "verify-report-sections",
        "verify-prune-health",
    ]:
        insight_data = json.loads(out)
        if not insight_data:
            CephInsightOpsFailure("Ceph insights command failed to return json output")

    # Check 3: Check whether the report has all the required sections
    if step_to_validate in ["verify-report-sections", "verify-prune-health"]:
        expected_sections = [
            "health",
            "crashes",
            "version",
            "config",
            "osd_dump",
            "manager_map",
            "mon_map",
            "service_map",
            "fs_map",
            "osd_tree",
            "crush_map",
        ]
        for section in expected_sections:
            if not insight_data.get(section):
                log.error("The given section is not present in :", insight_data)
                raise CephInsightOpsFailure(
                    f"Insights data doesn't have {section} section"
                )
    if step_to_validate == "verify-prune-health":
        # Verify whether the prune-health command deletes the report generated
        prune_timeout = 0  # Delete all logs
        out = CephAdm(node).ceph.insights(prune=True, hours=prune_timeout)
        if out:
            CephInsightOpsFailure("Failed to prune ceph insights report")

        # Generate report and validate it's not same as earlier
        out = CephAdm(node).ceph.insights()
        if out:
            CephInsightOpsFailure("Failed to get ceph insights report after prune")
        new_insights_data = json.loads(out)
        if new_insights_data == insight_data:
            CephInsightOpsFailure("The insights report has not been cleared")

    if step_to_validate == "verify-log":
        # Verify the ceph.audit.log has health history recorded and is not consuming more space
        # and Excessive logging is not found
        temp_file = tempfile.NamedTemporaryFile(suffix=".log")

        node.download_file(src=AUDIT_LOG_PATH, dst=temp_file.name, sudo=True)
        with open(temp_file.name, "r") as f:
            logs = f.read()

        if "health" not in logs:
            raise CephInsightOpsFailure("No ceph health history updated in audit log")
        unexpected_entries = [
            "Received MSG_CHANNEL_OPEN_CONFIRMATION",
            "Initial send window",
            "Sent MSG_IGNORE",
            "Sent MSG_CHANNEL_CLOSE",
            "Received channel close",
            "Received MSG_CHANNEL_REQUEST",
            "Received MSG_CHANNEL_EOF",
        ]
        for item in unexpected_entries:
            if item in logs:
                raise CephInsightOpsFailure("Unexpected data dump found in audit log")


def run(ceph_cluster, **kw):
    """Verify Ceph Mgr Insights functionality
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    node = ceph_cluster.get_nodes(role="mgr")[0]
    step_to_validate = kw.get("config").get("operation")

    if step_to_validate in ["enable", "disable"]:
        if CephAdm(node).ceph.mgr.module(action=step_to_validate, module="insights"):
            CephInsightOpsFailure(f"Failed to {step_to_validate} ceph insights")

    else:
        _ceph_run_insight(node, step_to_validate)

    return 0
