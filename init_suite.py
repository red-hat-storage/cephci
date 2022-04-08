"""Retrieve and process CephCI suites."""
import os
from copy import deepcopy
from glob import glob
from typing import List

import yaml

from utility.log import Log

log = Log(__name__)


def merge_dicts(dict1, dict2):
    """
    Returns dict1 by recursively merging dict2 into dict1

    Args:
        dict1 (dict):     The dictionary corressponding to test in <test_suite>.
        dict2 (dict):     The dictionary corressponding to test in <overrides>.

    Returns:
        Dict -> dictionary after merging overrides dict into test_suite dict
    """
    if isinstance(dict1, list) and isinstance(dict2, list):
        dict1.extend(dict2)
        return dict1
    if not isinstance(dict1, dict) or not isinstance(dict2, dict):
        return dict2
    for k in dict2:
        if k in dict1:
            dict1[k] = merge_dicts(dict1[k], dict2[k])
        else:
            dict1[k] = dict2[k]
    return dict1


def read_yaml(file_name):
    """read the given yaml

    Args:
        file_name: yaml file to read

    Returns:
        dict: data from yaml
    """
    file_path = os.path.abspath(file_name)
    with open(file_path) as fp:
        data = yaml.safe_load(fp)

    return data


def process_override(dir_name: str) -> List:
    """
    Returns a readable dictionary based on the files found in dir_name.

    The directory must contain two files if you want to override.
        - <test_suite>.yaml
        - overrides.yaml

    overrides.yaml could have
        tests:
          - test:
              index : <index of the test we want to update>
                # index starts from 1 refering to the first test in test suite file.
                # It is optional. default is 1

              <key>: <override_value>
                # give override_value as null if you want to ignore the existing key in test
          - test:
              <key>: <override_value>

        clusters:
          - <cluster_name>
          - <cluster_name>:
              <config>:
                <key>: <override_value>

    Args:
        dir_name (str):     The directory to be processed.

    Returns:
        Dict -> Test case after processing the override section.
    """
    # At this point, the entry/item is a directory. Inside this folder, we
    # support only three files
    override_data = dict()
    test_data = dict()

    log.debug(f"Processing overrides of {dir_name}")

    files = glob(os.path.join(dir_name, "*"))
    for file in files:
        if file.endswith("overrides.yaml"):
            override_data = read_yaml(file)
            continue

        test_data = read_yaml(file)

    if not override_data:
        return test_data["tests"]

    for test in override_data.get("tests", list()):
        index = test["test"].pop("index", 1) - 1
        merge_dicts(test_data["tests"][index]["test"], test["test"])

    if not override_data.get("clusters"):
        return test_data["tests"]

    clusters = override_data["clusters"]
    tests = test_data.get("tests")
    for test in tests:
        config = test["test"].pop("config", {})
        for cluster in clusters:
            if isinstance(cluster, str):
                cluster_name = cluster
            else:
                cluster_name = [key for key in cluster.keys()][0]
                override_config = cluster[cluster_name].get("config", {})
                merge_dicts(config, override_config)

            if "clusters" not in test["test"].keys():
                test["test"]["clusters"] = dict()

            test["test"]["clusters"][cluster_name] = dict(
                {"config": deepcopy(config) if config else None}
            )

    return test_data["tests"]


class Directory:
    """process given suite directory and return fragments in the suite directory"""

    def __init__(self, test_suite):
        self.suite_dir = test_suite

    @property
    def fragments(self):
        """fragments of the suite directory

        Returns:
            list: sorted fragments in the suite directory
        """
        log.info("Retrieving the fragments from the top level.")

        path_ = os.path.join(self.suite_dir, "*")
        log.debug(f"got path: {path_}")

        files = glob(path_)
        log.debug(f"Found the following: {files}")

        sorted_ = sorted(files)
        log.debug(f"sorted fragments: {sorted_}")

        return sorted_


class Suite:
    """process suite data"""

    def __init__(self, test_suites):
        self._test_suites = test_suites
        self.supported_patterns = (".yaml", ".yml")

    @property
    def suites(self):
        return self.__collate()

    def __collate(self):
        """
        Collate the data from the given test suites into a dictionary.

        A supported files contents are appended to the list of test information being
        gathered. The recommended file extension of is yaml though we do support yml.

        There is support for overrides with the help of python dict merge. Hence, there
        would be cases that we don't support. At the time of implementation, we agreed
        not to complicate it by performing deep merge of dictionaries.

        How to override
            Create a directory with link to the test suite (and or) overrides.yaml
            (and or) clusters.yaml

        - Direct
            # cat suite.yaml
            tests:
              - test:
                abort-on-fail: true
                desc: Install software pre-requisites
                module: install_prereq.py
                name: setup pre-requisites

            # cat overrides.yaml
            tests:
              - test:
                abort-on-fail: false

            # Final output should be
            # cat suite.yaml
            tests:
              - test:
                abort-on-fail: false
                desc: Install software pre-requisites
                module: install_prereq.py
                name: setup pre-requisites

        - Adding new keys

            tests:
              - test:
                abort-on-fail: true
                desc: Install software pre-requisites
                module: install_prereq.py
                name: setup pre-requisites

            # cat overrides.yaml
            tests:
              - test:
                config:
                  is_production: true

            # final output should be
            # cat suite.yaml
            tests:
              - test:
                abort-on-fail: true
                config:
                  is_production: true
                desc: Install software pre-requisites
                module: install_prereq.py
                name: setup pre-requisites

        Adding cluster overrides

            # cat suite.yaml
            tests:
               - test:
                  name: Buckets and Objects test
                  desc: test_Mbuckets_with_Nobjects_aws4 on secondary
                  polarion-id: CEPH-9637
                  module: sanity_rgw_multisite.py
                  config:
                    script-name: test_Mbuckets_with_Nobjects.py
                    config-file-name: test_Mbuckets_with_Nobjects_aws4.yaml
                    timeout: 300

            # cat overrides.yaml
            clusters:
              - site1

            # final output should be
              - test:
                  name: Buckets and Objects test
                  desc: test_Mbuckets_with_Nobjects_aws4 on secondary
                  polarion-id: CEPH-9637
                  module: sanity_rgw_multisite.py
                  clusters:
                    site1:
                      config:
                        script-name: test_Mbuckets_with_Nobjects.py
                        config-file-name: test_Mbuckets_with_Nobjects_aws4.yaml
                        timeout: 300

        Returns:
            dict:
                suite = {
                    "tests": <list of tests from the yamls>,
                    "nan": <list of unsupported and not found file(s) or directory(s)>
                }
        """
        suites = dict({"tests": list(), "nan": list()})

        for suite in self._test_suites:
            if os.path.isfile(suite) and suite.endswith(self.supported_patterns):
                tests = read_yaml(suite)
                suites["tests"].extend(tests.get("tests"))
                continue

            if not os.path.isdir(suite):
                log.debug(f"Not a supported file: {suite}")
                suites["nan"].append(suite)
                continue

            suites["tests"].extend(process_override(suite))

        return suites


def load_suites(test_suites):
    """high level wrapper to process list of test_suites

    Args:
        test_suites [list]: test_suites list may contain elements of
                            a. list of directory(s) AKA suites
                            b. list of suite file(yaml)

    Returns:
        dict: suite of tests and nan
    """

    test_suite_catalogue = []

    log.info(f"List of test suites provided: \n{test_suites}")

    for test_suite in test_suites:
        if os.path.isdir(test_suite):
            log.info("Provided suite is a directory")
            fragments = Directory(test_suite).fragments
            log.debug(f"got fragments: \n{fragments}")
            test_suite_catalogue.extend(fragments)
        else:
            log.info("Provided suite is a file")
            test_suite_catalogue.append(test_suite)

    return Suite(test_suite_catalogue).suites
