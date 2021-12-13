"""Create xUnit result files."""

import logging
from datetime import timedelta

from junitparser import Failure, JUnitXml, TestCase, TestSuite

log = logging.getLogger(__name__)


def create_xunit_results(suite_name, test_cases, run_dir, test_run_metadata):
    """
    Create an xUnit result file for the test suite's executed test cases.

    Args:
        suite_name: the test suite name
        test_cases: the test cases objects
        run_dir: the run directory to store xUnit result files
        test_run_metadata: test run meta information in dict

    Returns: None
    """
    _file = suite_name.split("/")[-1].strip(".yaml")
    xml_file = f"{run_dir}/xunit.xml"

    log.info(f"Creating xUnit result file for test suite: {_file}")

    suite = TestSuite(_file)
    for k, v in test_run_metadata.items():
        suite.add_property(k, v if v else "--NA--")

    for tc in test_cases:
        case = TestCase(tc["name"])
        elapsed = tc.get("duration")
        if isinstance(elapsed, timedelta):
            case.time = elapsed.total_seconds()
        else:
            case.time = 0.0

        if tc["status"] != "Pass":
            case.result = Failure("test failed")
        suite.add_testcase(case)

    suite.update_statistics()

    xml = JUnitXml()
    xml.add_testsuite(suite)
    xml.write(xml_file, pretty=True)

    log.info(f"xUnit result file created: {xml_file}")
