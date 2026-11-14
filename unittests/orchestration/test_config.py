"""Characterization of the command line contract.

The invocations exercised here are the shapes the Jenkins libraries under ``pipeline``
build. They are parsed by docopt exactly as a real run would, so a change that breaks a
pipeline breaks a test.
"""

import mock
import pytest
from docopt import docopt

from cephci.orchestration.config import (
    CephCIArgumentError,
    RunConfig,
    parse_custom_config,
)
from cephci.orchestration.doc import doc

# A tier execution as built by pipeline/vars/v3.groovy.
TIER_EXECUTION = [
    "--log-level",
    "DEBUG",
    "--osp-cred",
    "/home/jenkins/osp-cred-ci-2.yaml",
    "--instances-name",
    "ci-abc12",
    "--rhbuild",
    "8.1",
    "--platform",
    "rhel-9",
    "--build",
    "tier-0",
    "--cluster-conf",
    "conf/squid/cephadm/sanity.yaml",
    "--suite",
    "suites/squid/cephadm/tier0-cephadm.yaml",
    "--skip-sos-report",
]

# A compose verification run, which overrides every build artifact.
CVP_EXECUTION = [
    "--rhbuild",
    "8.1",
    "--platform",
    "rhel-9",
    "--global-conf",
    "conf/squid/cephadm/sanity.yaml",
    "--suite",
    "suites/squid/cephadm/tier0-cephadm.yaml",
    "--inventory",
    "conf/inventory/rhel-9-latest.yaml",
    "--osp-cred",
    "osp/osp-cred-ci-2.yaml",
    "--rhs-ceph-repo",
    "http://example.com/compose",
    "--docker-registry",
    "registry.example.com",
    "--docker-image",
    "rhceph/rhceph-8-rhel9",
    "--docker-tag",
    "latest",
    "--insecure-registry",
]


def parse(argv):
    """Parse an argument vector the way the entry point does."""
    return RunConfig.from_args(docopt(doc, argv=argv))


def test_tier_execution_is_understood():
    config = parse(TIER_EXECUTION)

    assert config.rhbuild == "8.1"
    assert config.release == "8.1"
    assert config.product == "redhat"
    assert config.platform == "rhel-9"
    assert config.build == "tier-0"
    assert config.cluster_conf_file == "conf/squid/cephadm/sanity.yaml"
    assert config.suites == ["suites/squid/cephadm/tier0-cephadm.yaml"]
    assert config.instances_name == "ci-abc12"
    assert config.skip_sos_report is True
    assert config.log_level == "DEBUG"
    assert config.cloud_type == "openstack"
    assert config.is_cleanup_only is False


def test_build_overrides_are_carried():
    config = parse(CVP_EXECUTION)

    assert config.rhs_ceph_repo == "http://example.com/compose"
    assert config.docker_registry == "registry.example.com"
    assert config.docker_image == "rhceph/rhceph-8-rhel9"
    assert config.docker_tag == "latest"
    assert config.insecure_registry is True


def test_cleanup_invocation():
    config = parse(["--cleanup", "ci-abc12", "--osp-cred", "osp/cred.yaml"])

    assert config.is_cleanup_only is True
    assert config.cleanup_name == "ci-abc12"


def test_release_takes_precedence_over_rhbuild():
    config = parse(
        [
            "--release",
            "9.0",
            "--platform",
            "rhel-10",
            "--global-conf",
            "c.yaml",
            "--suite",
            "s.yaml",
            "--product",
            "ibm",
        ]
    )

    assert config.release == "9.0"
    assert config.rhbuild == "9.0"
    assert config.product == "ibm"


def test_upstream_build_selects_the_community_product():
    config = parse(
        [
            "--rhbuild",
            "8.1",
            "--platform",
            "rhel-9",
            "--global-conf",
            "c.yaml",
            "--suite",
            "s.yaml",
            "--upstream-build",
            "squid",
        ]
    )

    assert config.product == "community"
    assert config.release == "squid"
    # The deprecated option does not change what the reports call the build.
    assert config.rhbuild == "8.1"


def test_ibm_build_custom_config_selects_the_ibm_product():
    config = parse(
        [
            "--rhbuild",
            "8.1",
            "--platform",
            "rhel-9",
            "--global-conf",
            "c.yaml",
            "--suite",
            "s.yaml",
            "--custom-config",
            "ibm-build=true",
        ]
    )

    assert config.product == "ibm"
    assert config.ibm_build is True


def test_multiple_suites_are_kept_in_order():
    config = parse(
        [
            "--rhbuild",
            "8.1",
            "--platform",
            "rhel-9",
            "--global-conf",
            "c.yaml",
            "--suite",
            "one.yaml",
            "--suite",
            "two.yaml",
        ]
    )

    assert config.suites == ["one.yaml", "two.yaml"]


def test_cluster_conf_wins_over_global_conf():
    args = docopt(
        doc,
        argv=[
            "--rhbuild",
            "8.1",
            "--platform",
            "rhel-9",
            "--cluster-conf",
            "cluster.yaml",
            "--suite",
            "s.yaml",
        ],
    )
    args["--global-conf"] = "global.yaml"

    assert RunConfig.from_args(args).cluster_conf_file == "cluster.yaml"


def test_instance_name_dots_become_dashes():
    config = parse(
        [
            "--rhbuild",
            "8.1",
            "--platform",
            "rhel-9",
            "--global-conf",
            "c.yaml",
            "--suite",
            "s.yaml",
            "--instances-name",
            "ci.abc.12",
        ]
    )

    assert config.instances_name == "ci-abc-12"


def test_skip_tc_accepts_a_single_value_and_a_list():
    base = [
        "--rhbuild",
        "8.1",
        "--platform",
        "rhel-9",
        "--global-conf",
        "c.yaml",
        "--suite",
        "s.yaml",
    ]

    assert parse(base + ["--skip-tc", "test_a"]).skip_tc_list == ["test_a"]
    assert parse(base + ["--skip-tc", "test_a,test_b"]).skip_tc_list == [
        "test_a",
        "test_b",
    ]


@pytest.mark.parametrize(
    "custom_config, expected",
    [
        ([], {}),
        (["a=1", "b=2"], {"a": "1", "b": "2"}),
        # A value holding an equals sign is truncated, as it always has been.
        (["url=http://host/a=b"], {"url": "http://host/a"}),
    ],
)
def test_custom_config_parsing(custom_config, expected):
    assert parse_custom_config(custom_config) == expected


def test_collection_flags_come_from_custom_config():
    config = parse(
        [
            "--rhbuild",
            "8.1",
            "--platform",
            "rhel-9",
            "--global-conf",
            "c.yaml",
            "--suite",
            "s.yaml",
            "--custom-config",
            "collect-coredump=true",
            "--custom-config",
            "collect-ceph-logs=true",
            "--custom-config",
            "crimson=true",
        ]
    )

    assert config.collect_coredump is True
    assert config.collect_ceph_logs is True
    assert config.crimson is True


def test_onecloud_falls_back_to_the_default_credentials():
    argv = [
        "--rhbuild",
        "8.1",
        "--platform",
        "rhel-9",
        "--global-conf",
        "c.yaml",
        "--suite",
        "s.yaml",
        "--cloud",
        "onecloud",
    ]

    with mock.patch("cephci.orchestration.config.os.path.exists", return_value=True):
        config = parse(argv)

    assert config.osp_cred_file.endswith("osp-cred-ci-2.yaml")


class TestValidation:
    """The invocations that cannot describe a run."""

    @staticmethod
    def execution_args(**overrides):
        argv = [
            "--rhbuild",
            "8.1",
            "--platform",
            "rhel-9",
            "--global-conf",
            "c.yaml",
            "--suite",
            "s.yaml",
        ]
        config = parse(argv)

        return config, overrides

    def test_cleanup_needs_credentials(self):
        config = parse(["--cleanup", "ci-abc", "--osp-cred", "cred.yaml"])

        with pytest.raises(CephCIArgumentError, match="cloud credentials"):
            config.validate_cleanup({})

        config.validate_cleanup({"globals": {}})

    def test_layout_is_required(self):
        config = parse(["--cleanup", "ci-abc", "--osp-cred", "cred.yaml"])

        with pytest.raises(CephCIArgumentError, match="cluster layout"):
            config.validate_execution({"globals": {}})

    def test_credentials_are_required_for_a_cloud(self):
        config = parse(
            [
                "--rhbuild",
                "8.1",
                "--platform",
                "rhel-9",
                "--global-conf",
                "c.yaml",
                "--suite",
                "s.yaml",
                "--cloud",
                "openstack",
            ]
        )

        with pytest.raises(CephCIArgumentError, match="Require cloud credentials"):
            config.validate_execution({})

    def test_inventory_is_required_for_a_cloud(self):
        config = parse(
            [
                "--rhbuild",
                "8.1",
                "--platform",
                "rhel-9",
                "--global-conf",
                "c.yaml",
                "--suite",
                "s.yaml",
                "--cloud",
                "openstack",
                "--osp-cred",
                "cred.yaml",
            ]
        )

        with pytest.raises(CephCIArgumentError, match="system configuration"):
            config.validate_execution({"globals": {}})

    def test_baremetal_needs_neither_credentials_nor_inventory(self):
        config = parse(
            [
                "--rhbuild",
                "8.1",
                "--platform",
                "rhel-9",
                "--cluster-conf",
                "c.yaml",
                "--suite",
                "s.yaml",
                "--cloud",
                "baremetal",
            ]
        )

        config.validate_execution({})

    def test_reuse_skips_the_environment_checks(self):
        config = parse(
            [
                "--rhbuild",
                "8.1",
                "--platform",
                "rhel-9",
                "--global-conf",
                "c.yaml",
                "--suite",
                "s.yaml",
                "--cloud",
                "openstack",
                "--reuse",
                "rerun/ci-abc",
            ]
        )

        config.validate_execution({})

    def test_ibm_and_upstream_builds_are_incompatible(self):
        config = parse(
            [
                "--rhbuild",
                "8.1",
                "--platform",
                "rhel-9",
                "--cluster-conf",
                "c.yaml",
                "--suite",
                "s.yaml",
                "--cloud",
                "baremetal",
                "--upstream-build",
                "squid",
                "--custom-config",
                "ibm-build=true",
            ]
        )

        with pytest.raises(CephCIArgumentError, match="not compatible"):
            config.validate_execution({})


def test_deprecated_options_are_reported():
    argv = [
        "--rhbuild",
        "8.1",
        "--platform",
        "rhel-9",
        "--global-conf",
        "c.yaml",
        "--suite",
        "s.yaml",
        "--upstream-build",
        "squid",
    ]
    args = docopt(doc, argv=argv)
    config = RunConfig.from_args(args)

    with mock.patch("cephci.orchestration.config.log") as logger:
        config.warn_deprecations(args)

    warnings = " ".join(call.args[0] for call in logger.warning.call_args_list)
    assert "--rhbuild is deprecated" in warnings
    assert "--upstream-build is deprecated" in warnings


def test_modern_options_are_not_reported_as_deprecated():
    argv = [
        "--release",
        "9.0",
        "--product",
        "ibm",
        "--platform",
        "rhel-10",
        "--global-conf",
        "c.yaml",
        "--suite",
        "s.yaml",
    ]
    args = docopt(doc, argv=argv)
    config = RunConfig.from_args(args)

    with mock.patch("cephci.orchestration.config.log") as logger:
        config.warn_deprecations(args)

    logger.warning.assert_not_called()


def test_custom_config_values_are_truthy_strings():
    """Any value enables the flag, including the string false."""
    config = parse(
        [
            "--rhbuild",
            "8.1",
            "--platform",
            "rhel-9",
            "--global-conf",
            "c.yaml",
            "--suite",
            "s.yaml",
            "--custom-config",
            "collect-coredump=false",
        ]
    )

    assert config.collect_coredump is True
