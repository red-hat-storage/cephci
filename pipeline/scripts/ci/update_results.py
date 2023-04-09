import json
import os
import sys

import yaml
from docopt import docopt

doc = """
    This script updates the /ceph/cephci-jenkins/results folder with the given test results"

    Usage:
        update_results.py --cephVersion <cephVersion> --testResults <results>
        update_results.py --cephVersion <cephVersion> --rpLink <link>
        update_results.py (-h | --help)

    Options:
        -h --help          Shows the command usage
        -c --cephVersion cephVersion        The ceph version for which the results have to be updated
        --rpLink attributes                 rplink attributes
        --testResults testResults           The test results to be updated in json format.
                                                Ex: '{"Sanity_Run": {"tier-0": {"stage-1": {"build_url": "",
                                                                                        "report_portal": "",
                                                                                        "test_suite_name": {
                                                                                            "result": "PASS",
                                                                                            "logdir": ""}}}}}'
"""


def update_nested_dict(current, updated):
    """
    Updates the nested dictionary current with the nested dictionary updated.
    If key exists both in current and updated, the value from updated is written to current
    If key doesn't exist in current, then a new key value pair is created
    Args:
        current: The nested dictionary to be updated
        updated: The nested dictionary with values to be updated to current
    """
    for key in updated:
        if isinstance(updated[key], str) or isinstance(updated[key], list):
            current[key] = updated[key]
        elif key in current:
            update_nested_dict(current[key], updated[key])
        else:
            current.update({key: updated[key]})


def update_results(ceph_version, test_results):
    """
    Updates the given test results to the corresponding ceph_version file
    If the file doesn't exist, it creates one and adds results,
    If the file exists then it appends the results at the right place
    Args:
        ceph_version: the ceph version for which test results need to be updated
        test_results: the results to be updated
    """
    test_results_json = json.loads(test_results)
    file_path = f"/ceph/cephci-jenkins/results/{ceph_version}.yaml"

    # implement lock file functionality to avoid overwrites when file is already being written by another build
    if not os.path.exists(file_path):
        with open(file_path, "w", encoding="utf-8") as f:
            yaml.dump(test_results_json, f)
    else:
        with open(file_path, "r", encoding="utf-8") as f:
            current_content = f.read()
            print("current_content")
            print(current_content)
            current_content = yaml.safe_load(current_content)
        with open(file_path, "w", encoding="utf-8") as f:
            if not current_content:
                current_content = test_results_json
            else:
                update_nested_dict(current_content, test_results_json)
            yaml.dump(current_content, f)


def update_rp_link(ceph_version, build_type, rp_link):
    """
    Update the results file with report portal link
    Args:
        ceph_version: the ceph version for which test results need to be updated
        build_type: name of the pipeline
        rp_link: report portal url
    """
    file_path = f"/ceph/cephci-jenkins/results/{ceph_version}.yaml"
    with open(file_path, "r") as f:
        current_content = f.read()
    print("current_content")
    print(current_content)
    content = yaml.safe_load(current_content)
    for key in content:
        if key == build_type:
            content[key]["rp_link"] = rp_link
            print(content[key])
    print(f"content after update --- {content}")
    with open(file_path, "w", encoding="utf-8") as wf:
        yaml.dump(content, wf)


if __name__ == "__main__":
    cli_args = docopt(doc)
    cephVersion = cli_args.get("--cephVersion")
    testResults = cli_args.get("--testResults")
    rpLink = cli_args.get("--rpLink")
    if rpLink:
        attributes = json.loads(rpLink)
        if attributes:
            update_rp_link(cephVersion, attributes["build_type"], attributes["rp_link"])
            sys.exit(0)
    update_results(cephVersion, testResults)
