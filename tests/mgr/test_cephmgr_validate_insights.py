import json

from cli.cephadm.cephadm import CephAdm
from utility.log import Log

log = Log(__name__)


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
    if step_to_validate in ["verify-report", "verify-report-sections"]:
        insight_data = json.loads(out)
        if not insight_data:
            CephInsightOpsFailure("Ceph insights command failed to return json output")

    # Check 3: Check whether the report has all the required sections
    if step_to_validate == "verify-report-sections":
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
