import logging
import os
import re
import time
from functools import partial, reduce

from utility.rp_utils.reportportalV1 import RpLog

log = logging.getLogger(__name__)


class XunitXML:
    """Class for processing the xUnit XML file for ReportPortal"""

    def __init__(self, rportal, name=None, configs=None, xml_data=None):
        self.rportal = rportal
        self.name = name
        self._configs = configs
        self.xml_data = xml_data

    @staticmethod
    def get_file_list(results_dir):
        """Get a list of XML results files from a directory"""
        # get list of xml result files
        result_file_list = []
        if os.path.exists(results_dir):
            for root, _, files in os.walk(results_dir):
                log.debug("root dir : %s" % root)
                for thefile in files:
                    fqpath = os.path.join(root, thefile)
                    if os.path.splitext(fqpath)[1] == ".xml":
                        result_file_list.append(fqpath)
                        log.debug("fqpath %s", fqpath)

            return result_file_list
        log.debug("payload directory path %s does not exist" % results_dir)
        return False


class TestSuite:
    """Class to handle TestSuite xUnit specific items"""

    def __init__(self, rportal, xml_name, testsuite):
        self.service = rportal.service
        self.xml_name = xml_name
        self.testsuite = testsuite
        self.item_id = None
        self.name = testsuite.get("@name", testsuite.get("@id", "NULL"))
        self.item_type = "SUITE"
        self.num_failures = int(self.testsuite.get("@failures", "0"))
        self.num_errors = int(self.testsuite.get("@errors", "0"))
        if self.num_failures > 0 or self.num_errors > 0:
            self.status = "FAILED"
        else:
            self.status = "PASSED"
        self.ts_properties = None

        if testsuite.get("properties"):
            properties = testsuite.get("properties").get("property")
            if not isinstance(properties, list):
                properties = [properties]
            self.ts_properties = map_attributes(
                properties,
                partial(filter_property, rportal.config.get("property_filter", list())),
            )
            # rp client cannot consume empty dict
            if not self.ts_properties:
                self.ts_properties = None

    def start(self):
        """Start a testsuite section in ReportPortal"""
        log.debug("Starting testsuite %s", self.name)
        # adding ts_properties as attributes(seen as tags on RP)
        self.item_id = self.service.start_test_item(
            name=self.name,
            start_time=str(int(time.time() * 1000)),
            item_type=self.item_type,
            attributes=self.ts_properties,
        )

    def finish(self):
        """Finish a testsuite section in ReportPortal"""
        self.service.finish_test_item(
            self.item_id, end_time=str(int(time.time() * 1000)), status=self.status
        )
        log.debug("Finished testsuite %s", self.name)


class TestCase:
    """Class to handle xUnit TestCase conversion to ReportPortal API calls"""

    def __init__(self, rportal, xml_name, testcase, configs=None, parent_id=None):
        self.service = rportal.service
        self.xml_name = xml_name
        self.testcase = testcase
        self._configs = configs
        self.parent_id = parent_id
        self.tc_classname = testcase.get("@classname", "")
        self.tc_name = testcase.get("@name", testcase.get("@id", None))
        self.tc_time = testcase.get("@time")
        self.description = "{} time: {}".format(self.tc_name, self.tc_time)
        self.status = "PASSED"
        self.issue = None
        self.rplog = RpLog(rportal)
        self.test_item_id = None
        self.tc_properties = dict()

        if testcase.get("properties"):
            properties = testcase.get("properties").get("property")
            if not isinstance(properties, list):
                properties = [properties]
            self.tc_properties = map_attributes(
                properties,
                partial(filter_property, rportal.config.get("property_filter", list())),
            )

    def start(self):
        """Start a testcase in ReportPortal"""

        # getting test case properties if any to be attached as tags for the test cases
        attributes = list()
        if self.tc_properties:
            for key, val in self.tc_properties.items():
                # creating the property in format key:val to be used as tag
                attributes.append(key + ":" + val)

        self.test_item_id = self.service.start_test_item(
            name=self.tc_name[:255],
            description=self.description,
            attributes=attributes,
            start_time=str(int(time.time() * 1000)),
            item_type="STEP",
            parent_item_id=self.parent_id,
        )
        # FIXME: start_time line length

        # Add system_out log
        if self.testcase.get("system-out"):
            # TODO: resolve get each call vs above for efficiency in python

            sys_out = self.testcase.get("system-out")
            if not isinstance(sys_out, list):
                sys_out = [sys_out]
            msg_txt = ""
            for msg in sys_out:
                msg_txt = msg_txt + msg

            itempath = os.path.join(
                self._configs.payload_dir, "attachments", self.test_item_id
            )
            message = self.rplog.truncate_message(
                msg_txt, self.tc_name, self.test_item_id, itempath, "system-out"
            )

            # write the message as a log entry
            self.rplog.add_message(
                message=message, level="INFO", test_item_id=self.test_item_id
            )

        # Add system_err log
        if self.testcase.get("system-err"):
            # TODO: resolve get each call vs above for efficiency in python

            sys_err = self.testcase.get("system-err")
            if not isinstance(sys_err, list):
                sys_out = [sys_err]
            msg_txt = ""
            for msg in sys_err:
                msg_txt = msg_txt + msg

            itempath = os.path.join(
                self._configs.payload_dir, "attachments", self.test_item_id
            )
            message = self.rplog.truncate_message(
                msg_txt, self.tc_name, self.test_item_id, itempath, "system-err"
            )

            # write the message as a log entry
            self.rplog.add_message(
                message=message, level="INFO", test_item_id=self.test_item_id
            )

        # Indicate type of test case (skipped, failures, passed)
        if self.testcase.get("skipped"):
            self.issue = {"issue_type": "NOT_ISSUE"}
            self.status = "SKIPPED"
            skipped_case = self.testcase.get("skipped")
            msg = (
                skipped_case.get("@message", "#text")
                if isinstance(skipped_case, dict)
                else skipped_case
            )
            self.rplog.add_message(
                message=msg, level="DEBUG", test_item_id=self.test_item_id
            )
        elif self.testcase.get("failure") or self.testcase.get("error"):
            self.status = "FAILED"
            failures = self.testcase.get("failure", self.testcase.get("error"))
            failures_txt = ""
            if not isinstance(failures, list):
                failures = [failures]
            for failure in failures:
                msg = failure.get("@message") if isinstance(failure, dict) else failure
                failures_txt += "Message: {msg}\n".format(msg=msg)
                failtype = (
                    failure.get("@type") if isinstance(failure, dict) else failure
                )
                failures_txt += "Type: {failtype}\n".format(failtype=failtype)
                txt = failure.get("#text") if isinstance(failure, dict) else failure
                failures_txt += "\nText:\n{txt}\n".format(txt=txt)

            self.rplog.add_message(
                message=failures_txt, level="ERROR", test_item_id=self.test_item_id
            )

            # handle attachments
            tc_attach_dir = "{}.{}".format(self.tc_classname, self.tc_name)
            fqpath = os.path.join(self._configs.payload_dir, "attachments")
            attached_files = self.rplog.add_attachments(
                fqpath, self.xml_name, tc_attach_dir, self.test_item_id
            )
            log.debug(
                "Finished attaching files for failed test cases %s", attached_files
            )

        else:
            self.status = "PASSED"

    def finish(self):
        """Finish a testcase in ReportPortal"""
        self.service.finish_test_item(
            self.test_item_id,
            end_time=str(int(time.time() * 1000)),
            status=self.status,
            issue=self.issue,
        )


def valid_attribute(prop):
    """Check if property can be used as report portal attribute"""
    return bool(
        prop.get("@value").strip() and 0 < len(prop.get("@name").strip()) <= 128
    )


def map_attributes(properties, filter_function):
    """Map properties to attributes"""
    if not isinstance(properties, list):
        properties = list(properties)

    return dict(
        map(
            lambda x: (x.get("@name"), x.get("@value")),
            filter(filter_function, properties),
        )
    )


def filter_property(filters, prop):
    """Test if testsuite property is allowed"""
    name = prop.get("@name")

    allowed = reduce(
        lambda x, y: x or y, map(lambda x: bool(re.match(x, name)), filters), False
    )

    return allowed and valid_attribute(prop)
