"""Create xUnit result files."""

from datetime import timedelta

from junitparser import (
    Failure,
    JUnitXml,
    Properties,
    Property,
    Skipped,
    TestCase,
    TestSuite,
)

from utility.log import Log

log = Log(__name__)


def generate_test_case(
    name, duration, status, err_type=None, err_msg=None, err_text=None, polarion_id=None
):
    """Create test case object.

    Args:
        name: test case name
        duration: test run duration
        status: test status
        polarion_id: polarion Id (default: None)

    Returns:
        test_case: junit parser test case object
    """
    test_case = TestCase(name)

    if isinstance(duration, timedelta):
        test_case.time = duration.total_seconds()
    else:
        test_case.time = 0.0

    if status == "Pass":
        pass  # Test passed, no need to add Failure or Skipped
    elif status == "Skipped":
        test_case.result = [Skipped()]
    else:
        _result = Failure(err_msg, err_type)
        _result.text = err_text
        test_case.result = [_result]

    if polarion_id:
        props = Properties()
        props.append(Property(name="polarion-testcase-id", value=polarion_id))
        test_case.append(props)

    return test_case


def create_xunit_results(suite_name, test_cases, test_run_metadata):
    """Create an xUnit result file for the test suite's executed test cases.

    Args:
        suite_name: the test suite name
        test_cases: the test cases objects
        test_run_metadata: test run meta information in dict

    Returns: None
    """
    _file = suite_name.split("/")[-1].split(".")[0]
    run_dir = test_run_metadata["log-dir"]
    run_id = test_run_metadata["run-id"]
    xml_file = f"{run_dir}/xunit.xml"
    ceph_version = test_run_metadata["ceph-version"]
    ansible_version = test_run_metadata["ceph-ansible-version"]
    distribution = test_run_metadata["distro"]
    build = test_run_metadata["build"]
    test_run_id = f"RHCS-{build}-{_file}-{run_id}".replace(".", "-")
    test_group_id = (
        f"ceph-build: {ceph_version} "
        f"ansible-build: {ansible_version} OS distro: {distribution}"
    )
    log.info(f"Creating xUnit {_file} for test run-id {test_run_id}")

    suite = TestSuite(_file)
    for k, v in test_run_metadata.items():
        suite.add_property(k, f" {v}" if v else " --NA--")

    for tc in test_cases:
        test_name = tc["name"]
        pol_ids = tc.get("polarion-id")
        test_status = tc["status"]
        elapsed_time = tc.get("duration")
        err_type = tc.get("err_type")
        err_msg = tc.get("err_msg")
        err_text = tc.get("err_text")

        if pol_ids:
            _ids = pol_ids.split(",")
            for _id in _ids:
                suite.add_testcase(
                    generate_test_case(
                        test_name,
                        elapsed_time,
                        test_status,
                        err_type=err_type,
                        err_msg=err_msg,
                        err_text=err_text,
                        polarion_id=_id,
                    )
                )
        else:
            suite.add_testcase(
                generate_test_case(
                    test_name,
                    elapsed_time,
                    test_status,
                    err_type=err_type,
                    err_msg=err_msg,
                    err_text=err_text,
                )
            )

    suite.update_statistics()

    xml = JUnitXml()
    props = Properties()
    props.append(Property(name="polarion-project-id", value="CEPH"))
    props.append(Property(name="polarion-testrun-id", value=test_run_id))
    props.append(Property(name="polarion-group-id", value=test_group_id))
    xml.append(props)
    xml.add_testsuite(suite)
    xml.write(xml_file, pretty=True)

    log.info(f"xUnit result file created: {xml_file}")
