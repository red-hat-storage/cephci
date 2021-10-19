import logging
import os
from glob import glob

import yaml

log = logging.getLogger(__name__)


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
        log.info("getting the fragments")

        path_ = os.path.join(self.suite_dir, "*")
        log.debug(f"got path: {path_}")

        files = glob(path_)
        log.debug(f"for file in path: {files}")

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
        """collate the data from yaml and unify the tests in a dict

            suite = {
                "tests": <list of tests from the yamls>,
                "nan": <list of unsupported and not found file(s) or directory(s)>
            }

        Returns:
            dict: suite
        """
        found, nan = self.__lookup()
        suite = {"tests": list(), "nan": nan}
        for f in found:
            data = read_yaml(f)
            suite["tests"].extend(data.get("tests"))
        return suite

    def __lookup(self):
        """look up the files from test_suites list

            found:  list of only supported file formats (.yaml, .yml)
                    filter only existing files
            nan:    unsupported and file not found lists

        Returns:
            tuple: found, nan
        """
        found = list(
            filter(
                lambda f: f.lower().endswith(self.supported_patterns)
                and os.path.isfile(f)
                and os.path.exists(f),
                self._test_suites,
            )
        )

        nan = list(set(self._test_suites) - set(found))

        return found, nan


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

    log.info(f"got suites: \n{test_suites}")
    log.info("starting to process suite based on type - directory or file")

    for test_suite in test_suites:
        if os.path.isdir(test_suite):
            log.info("suite is directory")
            fragments = Directory(test_suite).fragments
            log.debug(f"got fragments: \n{fragments}")
            test_suite_catalogue.extend(fragments)
        else:
            log.info("suite is file")
            test_suite_catalogue.append(test_suite)

    return Suite(test_suite_catalogue).suites
