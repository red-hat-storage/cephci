import json
import os
import sys

import yaml
from docopt import docopt
from jinja2 import Template

doc = """
This script updates the confluence page titled "RHCS Test Execution Summary"
with the results of execution given as input.

Usage:
get_email_body.py --template <template_name> --testResults <test_results> --testArtifacts <test_artifacts>
get_email_body.py --template <template> --testResultsFile <file> --testArtifacts <test_artifacts> --runType <run_type>

get_email_body.py (-h | --help)

Options:
    -h --help          Shows the command usage
    -c --template template_name     The name of the template to be used to render email
    --testResults test_results          The test results required to generate email body
    --testResultsFile test_results_file          The test results file required to generate email body
    --testArtifacts test_artifacts The test artifacts to be present in email body
    --runType run_type  Execution type (Sanity_Run, etc.,)
"""


def get_html_rows(spec):
    """
    Formats the given email body, by sorting it in the incremental order of tiers and stages
    Returns the html string for the formatted email body for all the rows
    Args:
        spec: the dictionary of test results

    Returns:
        html string for the formatted email body for all the rows
    """
    html_body = '<table style="border: 2px solid black;"><tr><th>Tier Level</th><th>Results of Execution</th></tr>'
    spec_keys = sorted(spec.keys())
    total_tcs = 0
    pass_tcs = 0
    fail_tcs = 0
    skip_tcs = 0

    for tier in spec_keys:
        html_body += f"<tr><td>{tier}</td><td><table>"
        html_body += (
            "<tr><th>Stage Level</th><th>Build URL</th><th>Report Portal URL</th>"
        )
        html_body += "<th>Test Suite</th><th>Result</th><th>Total Tests</th><th>Tests Passed</th>"
        html_body += "<th>Tests Failed</th><th>Tests Skipped</th></tr>"
        stage_keys = sorted(spec[tier].keys())
        for stage in stage_keys:
            if not spec[tier][stage]["test_results"]:
                continue
            html_body += f"<tr><td>{ stage }</td>"
            build_url = spec[tier][stage]["build_url"]
            build_url_text = [s for s in build_url.split("/") if s.isdigit()][-1]
            html_body += f"<td><a href={ build_url }>{ build_url_text }</a></td>"
            report_portal_url = spec[tier][stage]["report_portal"]
            report_portal_text = report_portal_url.split("/")[-1]
            html_body += (
                f"<td><a href={ report_portal_url }>{ report_portal_text }</a></td>"
            )
            suite_table = '<td><table class="artifacts">'
            result_table = '<td><table class="artifacts">'
            total_table = '<td><table class="artifacts">'
            pass_table = '<td><table class="artifacts">'
            fail_table = '<td><table class="artifacts">'
            skip_table = '<td><table class="artifacts">'
            for results in spec[tier][stage]["test_results"]:
                suite_table += f"<tr><td>{results['name']}</td></tr>"
                bg_color = "F1948A" if results["status"] == "FAILED" else "82E0AA"
                result_table += f"<tr><td bgcolor={bg_color}>"
                result_table += (
                    f"<a href={results['url']}>{results['status']}</a></td></tr>"
                )
                total_tcs += int(results.get("total", "0"))
                pass_tcs += int(results.get("passed", "0"))
                fail_tcs += int(results.get("failed", "0"))
                skip_tcs += int(results.get("skipped", "0"))
                total_table += f"<tr><td>{results.get('total', 0)}</td></tr>"
                pass_table += f"<tr><td>{results.get('passed', 0)}</td></tr>"
                fail_table += f"<tr><td>{results.get('failed', 0)}</td></tr>"
                skip_table += f"<tr><td>{results.get('skipped', 0)}</td></tr>"

            suite_table += "</table></td>"
            result_table += "</table></td>"
            total_table += "</table></td>"
            pass_table += "</table></td>"
            fail_table += "</table></td>"
            skip_table += "</table></td>"
            html_body += (
                suite_table
                + result_table
                + total_table
                + pass_table
                + fail_table
                + skip_table
            )
            html_body += "</tr>"
        html_body += "</table></td></tr>"
    html_body += "</table>"

    html_obj = {
        "html_body": html_body,
        "total_tcs": total_tcs,
        "pass_tcs": pass_tcs,
        "fail_tcs": fail_tcs,
        "skip_tcs": skip_tcs,
    }
    return html_obj


def read_test_results(filePath, runType):
    """
    Reads the consolidated results from the given path and
    returns the result for specified run type
    Args:
        filePath: "/ceph/cephci-jenkins/results/<cephVersion>.yaml"
        runType: "Sanity_Run:

    Returns:
        test results for the given parameters
    """
    with open(filePath, "r", encoding="utf-8") as f:
        content = f.read()
        content = yaml.safe_load(content)
        return content.get(runType)

    return None


class GenerateEmailBody:
    """
    Generates html for an email body with the specified template
    """

    def __init__(self):
        self.template_path = os.getcwd() + "/jinja_templates/"

    def _get_template(self, template_name):
        """
        Return Jinja template with the given template_name

        Args:
            template_name (Str): name of the jinja template

        Returns:
            template
        """
        path = self.template_path + f"{template_name}"
        with open(path) as fd:
            template = fd.read()
        return Template(template)

    def get_consolidate_email(self, **kwargs):
        """
        This method creates consolidated email html body based on the template specified
        Args:
            **kwargs:
                The test results and test artifacts for generating email body
        Returns:
            The html containing the email body
        """
        template_name = kwargs.get("template")
        myTemplate = self._get_template(template_name)
        test_results = kwargs.get("testResults")
        artifactDetails = kwargs.get("testArtifacts")
        return myTemplate.render(
            test_results=test_results, test_artifacts=artifactDetails
        )

    def get_stage_email(self, **kwargs):
        """
        This method creates stage level email html body based on the template specified
        Args:
            **kwargs:
                The test results and test artifacts for generating email body
        Returns:
            The html containing the email body
        """
        template_name = kwargs.get("template")
        myTemplate = self._get_template(template_name)
        test_results = kwargs.get("testResults")
        artifactDetails = kwargs.get("testArtifacts")
        return myTemplate.render(
            test_results=test_results, test_artifacts=artifactDetails
        )


if __name__ == "__main__":
    cli_args = docopt(doc)
    template = cli_args.get("--template")
    if cli_args.get("--testResults"):
        testResults = json.loads(cli_args.get("--testResults"))
    if cli_args.get("--testArtifacts"):
        testArtifacts = json.loads(cli_args.get("--testArtifacts"))
    try:
        cls = GenerateEmailBody()

        if template == "stage_email.jinja":
            html = cls.get_stage_email(
                template=template, testResults=testResults, testArtifacts=testArtifacts
            )
        else:
            if cli_args.get("--testResultsFile"):
                testResultsFile = cli_args.get("--testResultsFile")
                run_type = cli_args.get("--runType")
                content = read_test_results(testResultsFile, run_type)
                testResults = get_html_rows(content)
                html = cls.get_consolidate_email(
                    template=template,
                    testResults=testResults,
                    testArtifacts=testArtifacts,
                )
            else:
                raise "Test Results File Path not specified"
        html = html.replace("\n", "").replace("\\", "")
    except Exception as e:
        raise e
    sys.stdout.write(yaml.dump({"email_body": html}))
