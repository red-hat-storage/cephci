"""
This Utility will push the results to Report Portal.
It accepts config file location and payload_directory
In config file:
    We will have the details of report portal and launch details
In Payload_directory:
    We will have the zipped logs of the suites and error files if there is an error while executing the test case

Payload Directory Structure
payload
  |- results
      - test1.xml
      - test2.xml
  |- attachments
      - test1
         - test1.zip
         - failed_test.err

config.json:
{

    "reportportal": {
        "api_token": "8ca12f8b-8a3c-4d22-9f04-1e81692b7230",
        "auto_dashboard": false,
        "host_url": "https://reportportal-rhcephqe.apps.ocp-c1.prod.psi.redhat.com/",
        "merge_launches": true,
        "project": "cephci",
        "property_filter": [".*"],
        "simple_xml": false,
        "launch": {
            "name": "RHCEPH-4.3 - tier-0-amk",
            "description": "Test executed on Thu Mar 17 06:16:24 UTC 2022\n",
            "attributes": {
                "ceph_version": "14.2.22-86",
                "rhcs": "4.3",
                "tier": "tier-0"
            }
        }
    }
}

We are going to parse the xunit files and get the details of the results and upload them.
if there are any attachments present for the xunit file in the attachments folder with xunit folder name
those files will get attached to the suite and if there are any .err files we are going to attach them to the respective
test cases based on the name of the test case.
If the .zip file not present in the attachments. for those we are just updating the results and skipping the attachments
"""
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import xmltodict
from docopt import docopt
from rp_utils.preproc import PreProcClient
from rp_utils.reportportalV1 import Launch, ReportPortalV1, RpLog
from rp_utils.xunit_xml import TestCase, TestSuite, XunitXML

log = logging.getLogger(__name__)
doc = """
Standard script to push all the logs from Xunit files to Report Portal

    Usage:
        rp_client.py --config_file <str> --payload_dir <str>
        rp_client.py (-h | --help)

    Options:
        -h --help          Shows the command usage
        -c --config_file <str>         Config file with report portal details.
        -d --payload_dir <str>   Paylod directory where we have results and attachments
"""


def upload_logs(args):
    """
    Uploads the logs to Reportportal launch
    """
    preproc = PreProcClient((args))
    rportal = ReportPortalV1(preproc.configs.rp_config)
    results_file_dir = os.path.join(preproc.configs.payload_dir, "results")
    payload_subdir = os.listdir(preproc.configs.payload_dir)
    log.info(f"The payload dir file list is: {payload_subdir}")
    if preproc.config_file_path:
        log.info(f"Config file path is {preproc.config_file_path}")
        filename = os.path.basename(preproc.config_file_path)
        log.info(f"Remove config file path {filename} from {payload_subdir}")
        payload_subdir.remove(filename)
    result_file_list = XunitXML.get_file_list(results_file_dir)
    if not isinstance(result_file_list, list):
        log.info("The value of result_file_list is %s" % result_file_list)
        log.info(
            "ERROR: The payload directory path %s does not exist" % results_file_dir
        )
        return 1
    return_obj = {}
    with ThreadPoolExecutor() as executor:
        log.info("Processing xunit files concurrently")
        log.info(dir(rportal.service))
        launch = Launch(rportal)
        log.info(dir(preproc))
        launch.start()
        log.info(result_file_list)
        list(
            executor.map(partial(process_xml_file, preproc, rportal), result_file_list)
        )
        launch.finish()
    return_obj["launches"] = rportal.launches.list
    log.info("RETURN OBJECT: %s", return_obj)
    return return_obj


def process_xml_file(preproc, rportal, fqpath):
    with open(fqpath) as xmlfd:
        log.info("Processing fqpath %s", fqpath)
        filename = os.path.basename(fqpath)
        filename_base, _ = os.path.splitext(filename)
        log.info("%s %s", filename, filename_base)

        log.info("Parsing XML...")
        xml_data = xmltodict.parse(xmlfd.read())
        xunit_xml = XunitXML(
            rportal, name=filename_base, configs=preproc._configs, xml_data=xml_data
        )
        process(xunit_xml, rportal)


def process(xmlObj, rportal):
    """Process xUnit XML data"""
    # override env var with config provided vars
    rp_host_url = os.environ.get("RP_HOST_URL", None)
    log.info("rp_host_url: %s", rp_host_url)

    # check for multiple testsuites in xUnit
    if xmlObj.xml_data.get("testsuites"):
        # get test suites list
        testsuites_object = xmlObj.xml_data.get("testsuites")
        testsuites = (
            testsuites_object.get("testsuite")
            if isinstance(testsuites_object.get("testsuite"), list)
            else [testsuites_object.get("testsuite")]
        )
    else:
        testsuites = [xmlObj.xml_data.get("testsuite")]
    rplog = RpLog(rportal)
    # create testsuite(s)
    log.info("Processing %s testsuite(s)", len(testsuites))
    for testsuite in testsuites:
        testcases = testsuite.get("testcase")
        if not testcases:
            log.info(
                "Found empty test suite Name: %s. Skipping this test suite"
                % testsuite.get("@name")
            )
            continue
        elif not isinstance(testcases, list):
            testcases = [testcases]

        tsuite = TestSuite(xmlObj.rportal, xmlObj.name, testsuite)
        tsuite.start()

        # create all testcases
        log.info("Starting testcases")
        for testcase in testcases:
            process_testcase(xmlObj, testcase, tsuite)
        log.info("\nFinished testcases")
        fqpath = os.path.join(xmlObj._configs.payload_dir, "attachments")
        if os.path.exists(f"{fqpath}/{tsuite.xml_name}/{tsuite.xml_name}.zip"):
            rplog.add_attachment(
                tsuite.item_id, f"{fqpath}/{tsuite.xml_name}/{tsuite.xml_name}.zip"
            )
        tsuite.finish()


def process_testcase(xunit_xml, testcase, tsuite):
    # Skip testcases which has empty name
    if testcase.get("@name") == "" or not testcase.get("@name"):
        log.info("Skipping testcase because name is empty: %s", testcase)
        return
    tcase = TestCase(
        xunit_xml.rportal,
        xunit_xml.name,
        testcase,
        configs=xunit_xml._configs,
        parent_id=tsuite.item_id,
    )
    tcase.start()
    fqpath = os.path.join(tcase._configs.payload_dir, "attachments")
    log.info(f"{fqpath}/{tcase.tc_name}.err")
    if tcase.status == "FAILED" and os.path.exists(
        f"{fqpath}/{tsuite.xml_name}/{tcase.tc_name.replace(' ', '_')}_0.err"
    ):
        with open(
            f"{fqpath}/{tsuite.xml_name}/{tcase.tc_name.replace(' ', '_')}_0.err", "r"
        ) as file:
            error = file.readline()
            while error:
                tcase.rplog.add_message(
                    message=error, level="ERROR", test_item_id=tcase.test_item_id
                )
                error = file.readline()
    tcase.finish()


if __name__ == "__main__":
    args = docopt(doc)
    log.info(args)
    arguments = {
        "config_file": args.get("--config_file"),
        "payload_dir": args.get("--payload_dir"),
    }
    upload_logs(arguments)
