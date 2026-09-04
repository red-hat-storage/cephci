import importlib.util
import os
import sys

import pytest

MODULE_PATH = os.path.join(os.path.dirname(__file__), "getTestsForPipeline.py")
spec = importlib.util.spec_from_file_location("getTestsForPipeline", MODULE_PATH)
get_tests = importlib.util.module_from_spec(spec)
sys.modules["getTestsForPipeline"] = get_tests
spec.loader.exec_module(get_tests)


def test_enable_fips_mode_metadata_maps_to_custom_config():
    suite_args = {
        "suite": "suites/squid/nvmeof/tier-2_nvmeof_e2e_fips.yaml",
        "global-conf": "conf/squid/nvmeof/ceph_nvmeof_sanity.yaml",
        "platform": "rhel-9",
        "rhbuild": "8.1",
        "enable-fips-mode": True,
    }
    cli = get_tests.append_run_py_args(".venv/bin/python run.py", suite_args)
    assert "--custom-config enable-fips-mode=true" in cli
    assert "--enable-fips-mode" not in cli
    assert "enable-fips-mode" not in suite_args


def test_custom_config_list_is_preserved():
    suite_args = {
        "suite": "suites/example.yaml",
        "custom-config": ["ibm-build=True", "enable-fips-mode=true"],
    }
    cli = get_tests.append_run_py_args("run.py", suite_args)
    assert "--custom-config ibm-build=True" in cli
    assert "--custom-config enable-fips-mode=true" in cli


def test_cloud_type_is_not_passed_to_run_py():
    suite_args = {"suite": "suites/example.yaml", "cloud_type": "ibmc"}
    cli = get_tests.append_run_py_args("run.py", suite_args)
    assert "--cloud_type" not in cli
