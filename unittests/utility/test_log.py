# -*- code: utf-8 -*-
"""Unit testing modules for log filters.

This test module uses pytest to unit test the sensitive data log filter
"""

import os
from copy import deepcopy

import pytest

from utility.log import Log

str_data = "This has password something."
str_data_no_passwd = "This test has no sensitive data."
dict_data = {
    "name": "cephuser",
    "password": "Should be masked",
    "nodes": {"access-key": "1sd02dalg0", "user": "Nested dict having sensitive data."},
    "list": ["passwd x", "you need look at first item", True, False, 10],
    "tuple": (1, 0, 0, "Has token secret"),
    "age": 10,
}
list_dict_data = [
    {
        "test": {
            "name": "Test M buckets with N objects",
            "desc": "Test to create 'M' no of buckets and 'N' no of objects",
            "polarion-id": "CEPH-9789",
            "module": "sanity_rgw.py",
            "config": {
                "script-name": "test_Mbuckets_with_Nobjects.py",
                "config-file-name": "test_Mbuckets_with_Nobjects.yaml",
                "timeout": 300,
            },
        }
    },
    {
        "test": {
            "name": "cephfs-basics",
            "desc": "CephFS basic operations",
            "polarion-id": "CEPH-11293,CEPH-11296,CEPH-11297,CEPH-11295",
            "module": "cephfs_basic_tests.py",
        }
    },
]


@pytest.fixture
def logger():
    """Returns the logger object."""
    log = Log()
    log.configure_logger("unit-testing-log-filter", "/tmp", True)
    yield log
    log.close_and_remove_filehandlers()
    os.remove("/tmp/unit-testing-log-filter.log")
    os.remove("/tmp/unit-testing-log-filter.err")


def read_log():
    """Read the logfile contents."""
    with open("/tmp/unit-testing-log-filter.log", "r+") as fh:
        log_data = fh.read(-1)

    return log_data


def test_log_filter_sensitive_data(logger):
    _test_data = deepcopy(str_data)
    expected_string = "This has password <masked>"

    logger.info(_test_data)
    log_contents = read_log()

    assert str_data == _test_data
    assert expected_string in log_contents


def test_log_filter_non_sensitive_data(logger):
    _test_data = deepcopy(str_data_no_passwd)

    logger.info(_test_data)
    log_contents = read_log()

    assert _test_data == str_data_no_passwd
    assert str_data_no_passwd in log_contents


def test_log_filter_dict(logger):
    _test_data = deepcopy(dict_data)

    logger.info(_test_data)
    log_contents = read_log()

    # check log contents
    assert "'password': '<masked>'" in log_contents
    assert "'access-key': '<masked>'" in log_contents
    assert "'passwd <masked>'" in log_contents
    assert "'Has token <masked>'" in log_contents

    # Ensure no variable modification
    assert _test_data["password"] == dict_data["password"]
    assert _test_data["nodes"]["access-key"] == dict_data["nodes"]["access-key"]
    assert _test_data["list"][0] == dict_data["list"][0]
    assert _test_data["tuple"][3] == dict_data["tuple"][3]


def test_log_list_of_dictionaries(logger):
    """Ensuring we are logging correctly."""
    _test_data = deepcopy(list_dict_data)

    logger.info(_test_data)
    log_contents = read_log()

    assert list_dict_data[0]["test"]["name"] in log_contents
    assert list_dict_data[1]["test"]["module"] in log_contents
    assert "masked" not in log_contents
    assert None not in _test_data
