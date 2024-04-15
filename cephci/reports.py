import datetime
import os
import sys
import xml.etree.ElementTree as ET

from docopt import docopt
from utils.configs import get_configs, get_reports
from utils.utility import set_logging_env

from cli.exceptions import NotSupportedError
from utility.log import Log

LOG = Log(__name__)

DEFAULT_PROJECT = "CEPH"
DEFAULT_QUERY = "NOT HAS_VALUE:resolution AND type:testcase AND NOT status:inactive AND caseautomation.KEY:automated"
TEST_FILTER = "staticQueryResult"


doc = """
Utility to report result to service

    Usage:
        cephci/reports.py --service <SERVICE>
            [--testrun-template <STR>]
            [--project <STR>]
            [--testrun-id <STR>]
            [--testrun-title <STR>]
            [--testplan-id <STR>]
            [--xunit <PATH>...]
            [--query <STR>]
            [--config <YAML>]
            [--log-level <LOG>]
            [--log-dir <PATH>]

        cephci/reports.py --help

    Options:
        -h --help                       Help
        -s --service <SERVICE>          Service [polarion|report-portal|email]
        -t --testrun-template <STR>     Test run template
        --testrun-id <STR>              Test run ID
        --testrun-title <STR>           Test run title
        --testplan-id <STR>             Test plan
        --xunit <PATH>                  XML result files
        --project <STR>                 Project name
        --query <STR>                   Query to create test plan
        --config <YAML>                 Config file with service details
        --log-level <LOG>               Log level for log utility Default: DEBUG
        --log-dir <PATH>                Log directory for logs
"""


def _set_polarion_env(config, project):
    """Set polarion environment"""
    LOG.info(f"Setting polarion environment for project '{project}'")

    os.environ["POLARION_URL"] = config.get("url")
    os.environ["POLARION_REPO"] = config.get("svn_repo")
    os.environ["POLARION_USERNAME"] = config.get("user")
    os.environ["POLARION_TOKEN"] = config.get("token")
    os.environ["POLARION_PROJECT"] = (
        project if project else config.get("default_project")
    )
    os.environ["POLARION_CERT_PATH"] = config.get("cert_path")


def create_testrun_template(project, id, query):
    """Create Test Run Template"""
    from pylero.test_run import TestRun

    LOG.info(f"Checking for testrun template '{id}'")
    try:
        # Create testrun template
        TestRun.create_template(
            project_id=project,
            template_id=id,
            select_test_cases_by=TEST_FILTER,
            query=query,
        )
        LOG.info(f"Testrun template '{id}' created successfully")
    except Exception as e:
        if "Most probably the TestRun with the same ID already exists." in str(e):
            LOG.info(f"Testrun template '{id}' already present")
            return

        msg = f"Falied to create testrun template {id} with error :\n{str(e)}"
        LOG.error(msg)
        raise Exception(msg)


def create_testrun(
    project, id, title=None, template=None, status="inprogress", testplan_id=None
):
    """Creating Test Run"""
    from pylero.test_run import TestRun

    LOG.info(f"Checking for testrun '{id}'")
    try:
        # Create testruns
        tr = TestRun.create(
            project_id=project, test_run_id=id, template=template, title=title
        )

        # Set testrun status
        tr.status = status

        # Set testplan
        if testplan_id:
            tr.plannedin = testplan_id

        # Update testruns
        tr.update()

        LOG.info(f"Testrun '{id}' created successfully")
    except Exception as e:
        if "Most probably the TestRun with the same ID already exists." in str(e):
            LOG.info(f"Testrun '{id}' already present")
            return

        msg = f"Failed to create testrun {id} with error :\n{str(e)}"
        LOG.error(msg)
        raise Exception(msg)


def _update_testrun(TestRun, testcase_id, result, comment, user, executed, duration):
    """Update testrun results"""
    LOG.info(f"Updating polarion with testcase '{testcase_id}'")
    try:
        TestRun.update_test_record_by_fields(
            test_case_id=testcase_id,
            test_result=result,
            test_comment=comment,
            executed_by=user,
            executed=executed,
            duration=duration,
        )
        LOG.info(f"Updated testcase '{testcase_id}' in polarion")
    except Exception as e:
        LOG.error(f"Failed to update testcase '{testcase_id}' with error: \n{str(e)}")


def update_test_results(testrun_id, xunit, user):
    """Update testrun with xunit results"""
    from pylero.test_run import TestRun

    TestRun = TestRun(test_run_id=testrun_id, project_id=project)

    LOG.info(f"Updating testruns '{testrun_id}' with results '{xunit}'")
    root = ET.parse(xunit).getroot()
    for tc in root.findall("testsuite/testcase"):
        result, executed = "passed", datetime.datetime.now()
        duration, comment = tc.attrib.get("time"), tc.attrib.get("name")
        for properties in tc:
            # Check for failed message
            if properties.tag == "failure":
                result = "failed"

            # Update polarion with testcase id
            for property in properties:
                testcase_id = property.attrib.get("value")
                _update_testrun(
                    TestRun, testcase_id, result, comment, user, executed, duration
                )


if __name__ == "__main__":
    # Set user parameters
    args = docopt(doc)

    # Get user parameters
    service = args.get("--service")
    if service.lower() not in ["polarion"]:
        raise NotSupportedError(f"Service '{service}' not supported")

    project = args.get("--project")
    testrun_template = args.get("--testrun-template")
    testrun_id = args.get("--testrun-id")
    testrun_title = args.get("--testrun-title")
    testplan_id = args.get("--testplan-id")
    query = args.get("--query")
    xunit_results = args.get("--xunit")
    config = args.get("--config")
    log_level = args.get("--log-level")
    log_dir = args.get("--log-dir")

    # Set log level
    LOG = set_logging_env(level=log_level, path=log_dir)

    # Set default project
    if not project:
        project = DEFAULT_PROJECT
    LOG.info(f"Using project '{project}' for service '{service}'")

    # Read configuration for reporting service
    get_configs(config)

    # Set reporting environment
    config = get_reports(service)
    if service.lower() == "polarion":
        _set_polarion_env(config, project)

    # Create testrun template
    create_testrun_template(
        project, testrun_template, query if query else DEFAULT_QUERY
    )

    # Check for testrun id
    if not testrun_id:
        sys.exit(0)

    # Create testrun
    create_testrun(
        project, testrun_id, testrun_title, testrun_template, testplan_id=testplan_id
    )

    # Update results with testrun
    for xunit in xunit_results:
        if not os.path.exists(xunit):
            LOG.error(
                f"xunit results {xunit} not present. Skipping polarion update ..."
            )
            continue

        update_test_results(testrun_id, xunit, config.get("user"))
