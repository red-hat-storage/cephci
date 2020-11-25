import os
import pytest

from utility.utils import custom_ceph_config

suite_config = {'global': {'osd_pool_default_pg_num': 64,
                           'osd_default_pool_size': 2,
                           'osd_pool_default_pgp_num': 64,
                           'mon_max_pg_per_osd': 1024,
                           'osd_objectstore': 'bluestore'}}
cli_config = ['osd_pool_default_pg_num=128',
              'osd_default_pool_size=2',
              'osd_pool_default_pgp_num=128',
              'mon_max_pg_per_osd=1024']


@pytest.fixture
def config_file(fixtures_dir):
    return os.path.join(fixtures_dir, 'custom_ceph_config.yaml')


def test_custom_ceph_config_no_values():
    expected = {}
    result = custom_ceph_config(None, None, None)
    assert result == expected


def test_custom_ceph_config_suite_only():
    result = custom_ceph_config(suite_config, None, None)
    assert result == suite_config


def test_custom_ceph_config_cli_only():
    expected = {'global': {'osd_pool_default_pg_num': '128',
                           'osd_default_pool_size': '2',
                           'osd_pool_default_pgp_num': '128',
                           'mon_max_pg_per_osd': '1024'}}
    result = custom_ceph_config(None, cli_config, None)
    assert result == expected


def test_custom_ceph_config_file_only(config_file):
    expected = {'global': {'osd_pool_default_pg_num': 64,
                           'osd_default_pool_size': 2,
                           'osd_pool_default_pgp_num': 64,
                           'mon_max_pg_per_osd': 2048,
                           'osd_journal_size': 10000},
                'mon': {'mon_osd_full_ratio': .80,
                        'mon_osd_nearfull_ratio': .70}}
    result = custom_ceph_config(None, None, config_file)
    assert result == expected


def test_custom_ceph_config_suite_and_cli():
    expected = {'global': {'osd_pool_default_pg_num': '128',
                           'osd_default_pool_size': '2',
                           'osd_pool_default_pgp_num': '128',
                           'mon_max_pg_per_osd': '1024',
                           'osd_objectstore': 'bluestore'}}
    result = custom_ceph_config(suite_config, cli_config, None)
    assert result == expected


def test_custom_ceph_config_suite_and_file(config_file):
    expected = {'global': {'osd_pool_default_pg_num': 64,
                           'osd_default_pool_size': 2,
                           'osd_pool_default_pgp_num': 64,
                           'mon_max_pg_per_osd': 2048,
                           'osd_objectstore': 'bluestore',
                           'osd_journal_size': 10000},
                'mon': {'mon_osd_full_ratio': .80,
                        'mon_osd_nearfull_ratio': .70}}

    result = custom_ceph_config(suite_config, None, config_file)
    assert result == expected


def test_custom_ceph_config_cli_and_file(config_file):
    expected = {'global': {'osd_pool_default_pg_num': '128',
                           'osd_default_pool_size': '2',
                           'osd_pool_default_pgp_num': '128',
                           'mon_max_pg_per_osd': '1024',
                           'osd_journal_size': 10000},
                'mon': {'mon_osd_full_ratio': .80,
                        'mon_osd_nearfull_ratio': .70}}
    result = custom_ceph_config(None, cli_config, config_file)
    assert result == expected


def test_custom_ceph_config_all(config_file):
    expected = {'global': {'osd_pool_default_pg_num': '128',
                           'osd_default_pool_size': '2',
                           'osd_pool_default_pgp_num': '128',
                           'mon_max_pg_per_osd': '1024',
                           'osd_objectstore': 'bluestore',
                           'osd_journal_size': 10000},
                'mon': {'mon_osd_full_ratio': .80,
                        'mon_osd_nearfull_ratio': .70}}
    result = custom_ceph_config(suite_config, cli_config, config_file)
    assert result == expected
