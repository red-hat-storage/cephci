import logging
import os
from copy import deepcopy
from subprocess import call
from tempfile import NamedTemporaryFile

from jinja2 import Environment, FileSystemLoader

from utility.utils import get_cephci_config

log = logging.getLogger(__name__)


def post_to_polarion(tc):
    """
    Function to post test results polarion
    It returns nothing and is essentially like noop
    in case of no polarion details found in test object

    Args:
       tc: test case object with details

    Returns:
      None
    """
    current_dir = os.getcwd()
    polarion_cred = get_cephci_config()["polarion"]

    if tc["polarion-id"] is not None:
        # add polarion attributes
        ids = tc["polarion-id"].split(",")
        tc["space"] = "Smoke Suite"
        tc["test_run_id"] = (
            tc["ceph-version"]
            + "_"
            + tc["suite-name"]
            + "_"
            + tc["distro"]
            + "_Automated_Smoke_Runs"
        )
        tc["test_run_id"] = tc["test_run_id"].replace(".", "_")
        log.info("Updating test run: %s " % tc["test_run_id"])
        tc["ceph-build"] = "_".join(
            [
                _f
                for _f in [
                    tc["ceph-version"],
                    tc["ceph-ansible-version"],
                    tc["compose-id"],
                ]
                if _f
            ]
        )
        if tc.get("docker-containers-list"):
            tc["docker-container"] = "\ncontainer: {container}".format(
                container=",".join(list(set(tc.get("docker-containers-list"))))
            )
        tc["test_case_title"] = tc["desc"]
        if tc["desc"] is None:
            log.info("cannot update polarion with no description")
            return
        if tc["status"] == "Pass":
            tc["result"] = ""
        else:
            tc["result"] = '<failure message="test failed" type="failure"/>'
        current_dir += "/templates/"
        j2_env = Environment(loader=FileSystemLoader(current_dir), trim_blocks=True)
        for id in ids:
            tc["polarion-id"] = id
            f = NamedTemporaryFile(delete=False)
            test_results = j2_env.get_template("importer-template.xml").render(tc=tc)
            log.info("updating results for %s " % id)
            f.write(test_results.encode())
            f.close()
            url = polarion_cred.get("url")
            user = polarion_cred.get("username")
            pwd = polarion_cred.get("password")
            call(
                [
                    "curl",
                    "-k",
                    "-u",
                    "{user}:{pwd}".format(user=user, pwd=pwd),
                    "-X",
                    "POST",
                    "-F",
                    "file=@{name}".format(name=f.name),
                    url,
                ]
            )
            os.unlink(f.name)


def post_to_polarion_v2(test_run):
    """Function to post test results to polarion.

    It returns nothing and is essentially like noop
    in case of no polarion details found in test object

    Args:
       test_run: test case object with details

    """
    test_run["suite-name"] = test_run["suite-name"].split("/")[-1].split(".")[0]
    test_run["test_run_id"] = "_".join(
        [
            test_run["ceph-version"],
            test_run["suite-name"],
            test_run["distro"],
            "Automated_Smoke_Runs",
        ]
    )
    test_run["test_run_id"] = test_run["test_run_id"].replace(".", "_")

    test_run["ceph-build"] = "_".join(
        [
            test_run["ceph-version"],
            test_run["ceph-ansible-version"],
            test_run["compose-id"],
        ]
    )

    test_run["docker-container"] = test_run["test_cases"][0]["docker-containers-list"]

    test_cases = []
    test_run["tests"] = 0
    test_run["failed"] = 0
    for test_case in test_run["test_cases"]:
        ids = test_case.get("polarion-id").split(",")

        # In-case of zero polarion test case IDs
        if not ids:
            continue

        test_cases_ = []
        for id_ in ids:
            tc = deepcopy(test_case)
            test_run["tests"] += 1
            tc["polarion-id"] = id_
            if test_case["status"] == "Pass":
                tc["result"] = ""
            else:
                tc["result"] = '<failure message="{log_link}" type="failure"/>'.format(
                    log_link=tc["log-link"]
                )
                test_run["failed"] += 1
            test_cases_.append(tc)
        test_cases.extend(test_cases_)

    # If no test cases found
    if not test_cases:
        log.warning("No Polarion test cases found")
        return

    test_run["test_cases"] = test_cases

    current_dir = os.getcwd() + "/templates/"
    j2_env = Environment(loader=FileSystemLoader(current_dir), trim_blocks=True)
    f = NamedTemporaryFile(delete=False)
    test_results = j2_env.get_template("polarion-test-run-template.xml").render(
        test_run=test_run
    )

    print(test_results)

    log.info(f"updating results for {test_run['suite-name']}")
    f.write(test_results.encode())
    f.close()
    polarion_cred = get_cephci_config()["polarion"]
    url = polarion_cred.get("url")
    user = polarion_cred.get("username")
    pwd = polarion_cred.get("password")
    call(
        [
            "curl",
            "-k",
            "-u",
            "{user}:{pwd}".format(user=user, pwd=pwd),
            "-X",
            "POST",
            "-F",
            "file=@{name}".format(name=f.name),
            url,
        ]
    )
    os.unlink(f.name)
