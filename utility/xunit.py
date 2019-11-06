"""Create xUnit result files."""

import logging

from junitparser import Failure, JUnitXml, TestCase, TestSuite

log = logging.getLogger(__name__)


def create_xunit_results(suite_name, test_cases, run_dir):
    """
    Create an xUnit result file for the test suite's executed test cases.

    Args:
        suite_name: the test suite name
        test_cases: the test cases objects
        run_dir: the run directory to store xUnit result files

    Returns: None
    """
    xml_file = f"{run_dir}/{suite_name}.xml"

    log.info(f"Creating xUnit result file for test suite: {suite_name}")

    suite = TestSuite(suite_name)

    for tc in test_cases:
        case = TestCase(tc['name'])
        if tc['status'] != "Pass":
            case.result = Failure("test failed")
        suite.add_testcase(case)

    suite.update_statistics()

    xml = JUnitXml()
    xml.add_testsuite(suite)
    xml.write(xml_file, pretty=True)

    log.info(f"xUnit result file created: {xml_file}")
