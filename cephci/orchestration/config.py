"""Translation of the command line into a validated settings object.

``RunConfig`` is the single place where the docopt dictionary is read. Everything
downstream consumes the dataclass, which keeps the option names in one module and makes
the orchestration testable without a command line.
"""

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from utility.log import Log

log = Log(__name__)

# Clouds that cannot be provisioned without credentials.
CREDENTIAL_REQUIRED_CLOUDS = ["openstack", "ibmc", "aws", "ocpvirt"]

# Clouds that cannot be provisioned without an inventory.
INVENTORY_REQUIRED_CLOUDS = ["openstack", "ibmc", "aws", "onecloud", "ocpvirt"]

# Looked up when OneCloud is selected without an explicit --osp-cred.
ONECLOUD_DEFAULT_CRED = ""


class CephCIArgumentError(Exception):
    """Raised when the provided arguments cannot describe a test run."""


def parse_custom_config(custom_config: Optional[List[str]]) -> Dict[str, str]:
    """Convert the repeatable --custom-config option into a dictionary.

    Args:
        custom_config (list): Items of the form ``<key>=<value>``.

    Returns:
        dict: The key/value pairs. Values containing an equals sign are truncated at
            the second field, which is long standing behaviour of this option.
    """
    if not custom_config:
        return {}

    return {item.split("=")[0]: item.split("=")[1] for item in custom_config}


@dataclass(frozen=True)
class RunConfig:
    """Everything the orchestration needs from the command line.

    The attribute names follow the option they are derived from. Values are not
    interpreted here beyond the derivations that the command line itself implies, such
    as ``--release`` taking precedence over the deprecated ``--rhbuild``.
    """

    # Test definition
    suites: List[str]
    cluster_conf_file: Optional[str]
    inventory_file: Optional[str]
    osp_cred_file: Optional[str]

    # Build under test
    product: str
    release: str
    platform: str
    rhbuild: str
    build: Optional[str] = None
    upstream_build: Optional[str] = None
    crimson: bool = False
    ibm_build: bool = False

    # Build artifact overrides
    rhs_ceph_repo: Optional[str] = None
    docker_registry: Optional[str] = None
    docker_image: Optional[str] = None
    docker_tag: Optional[str] = None
    insecure_registry: bool = False
    add_repo: Optional[str] = None
    hotfix_repo: Optional[str] = None
    kernel_repo: Optional[str] = None

    # Environment
    cloud_type: str = "openstack"
    instances_name: Optional[str] = None
    osp_image: Optional[str] = None
    reuse: Optional[str] = None
    store: bool = False
    cleanup_name: Optional[str] = None

    # Execution behaviour
    skip_setup: bool = False
    skip_subscription: bool = False
    skip_enabling_rhel_rpms: bool = False
    skip_version_compare: bool = False
    skip_sos_report: bool = False
    skip_tc_list: List[str] = field(default_factory=list)
    enable_eus: bool = False
    filestore: bool = False
    ec_pool_vals: Optional[str] = None
    enable_perf_mon: bool = False

    # Logging and reporting
    log_level: Optional[str] = None
    log_directory: Optional[str] = None
    disable_console_log: bool = False
    post_results: bool = False
    xunit_results: bool = False

    # Custom configuration
    custom_config: List[str] = field(default_factory=list)
    custom_config_file: Optional[str] = None
    custom_config_dict: Dict[str, str] = field(default_factory=dict)
    collect_coredump: bool = False
    collect_ceph_logs: bool = False

    @classmethod
    def from_args(cls, args: Dict[str, Any]) -> "RunConfig":
        """Build an instance from the dictionary returned by docopt.

        Args:
            args (dict): Parsed command line options.

        Returns:
            RunConfig: The derived settings.
        """
        rhbuild = args.get("--release") or args.get("--rhbuild")
        custom_config = args.get("--custom-config") or []
        custom_config_dict = parse_custom_config(custom_config)

        cloud_type = args.get("--cloud")
        osp_cred_file = args.get("--osp-cred")
        if cloud_type == "onecloud" and osp_cred_file is None:
            default_cred = os.path.expanduser(ONECLOUD_DEFAULT_CRED)
            if os.path.exists(default_cred):
                osp_cred_file = default_cred

        product = args.get("--product") or "redhat"
        release = args.get("--release") or rhbuild

        upstream_build = args.get("--upstream-build")
        if upstream_build:
            product = "community"
            release = upstream_build

        ibm_build = False
        if "ibm-build" in custom_config_dict:
            ibm_build = bool(custom_config_dict["ibm-build"])
            if ibm_build:
                product = "ibm"

        instances_name = args.get("--instances-name")
        if instances_name:
            instances_name = instances_name.replace(".", "-")

        skip_tc = args.get("--skip-tc")
        skip_tc_list = skip_tc.split(",") if skip_tc else []

        cluster_conf_file = args.get("--cluster-conf") or args.get("--global-conf")

        return cls(
            suites=args.get("--suite") or [],
            cluster_conf_file=cluster_conf_file,
            inventory_file=args.get("--inventory"),
            osp_cred_file=osp_cred_file,
            product=product,
            release=release,
            platform=args.get("--platform"),
            rhbuild=rhbuild,
            build=args.get("--build"),
            upstream_build=upstream_build,
            crimson=bool(custom_config_dict.get("crimson", False)),
            ibm_build=ibm_build,
            rhs_ceph_repo=args.get("--rhs-ceph-repo"),
            docker_registry=args.get("--docker-registry"),
            docker_image=args.get("--docker-image"),
            docker_tag=args.get("--docker-tag"),
            insecure_registry=bool(args.get("--insecure-registry")),
            add_repo=args.get("--add-repo"),
            hotfix_repo=args.get("--hotfix-repo"),
            kernel_repo=args.get("--kernel-repo"),
            cloud_type=cloud_type,
            instances_name=instances_name,
            osp_image=args.get("--osp-image"),
            reuse=args.get("--reuse"),
            store=bool(args.get("--store")),
            cleanup_name=args.get("--cleanup"),
            skip_setup=bool(args.get("--skip-cluster")),
            skip_subscription=bool(args.get("--skip-subscription")),
            skip_enabling_rhel_rpms=bool(args.get("--skip-enabling-rhel-rpms")),
            skip_version_compare=bool(args.get("--skip-version-compare")),
            skip_sos_report=bool(args.get("--skip-sos-report")),
            skip_tc_list=skip_tc_list,
            enable_eus=bool(args.get("--enable-eus")),
            filestore=bool(args.get("--filestore")),
            ec_pool_vals=args.get("--use-ec-pool"),
            enable_perf_mon=bool(args.get("--monitor-performance")),
            log_level=args.get("--log-level"),
            log_directory=args.get("--log-dir"),
            disable_console_log=bool(args.get("--disable-console-log")),
            post_results=bool(args.get("--post-results")),
            xunit_results=bool(args.get("--xunit-results")),
            custom_config=custom_config,
            custom_config_file=args.get("--custom-config-file"),
            custom_config_dict=custom_config_dict,
            collect_coredump=bool(custom_config_dict.get("collect-coredump", False)),
            collect_ceph_logs=bool(custom_config_dict.get("collect-ceph-logs", False)),
        )

    @property
    def is_cleanup_only(self) -> bool:
        """Whether the invocation only removes a previously created environment."""
        return bool(self.cleanup_name)

    def validate_cleanup(self, osp_cred: Dict) -> None:
        """Ensure a cleanup only invocation can reach the cloud.

        Args:
            osp_cred (dict): Contents of the credentials file.

        Raises:
            CephCIArgumentError: When credentials are unavailable.
        """
        if not osp_cred:
            raise CephCIArgumentError("Need cloud credentials to perform cleanup.")

    def validate_execution(self, osp_cred: Dict) -> None:
        """Ensure the arguments describe an environment that can run tests.

        Args:
            osp_cred (dict): Contents of the credentials file.

        Raises:
            CephCIArgumentError: When the environment cannot be determined.
        """
        if self.cluster_conf_file is None and not self.reuse:
            raise CephCIArgumentError(
                "Unable to gather information about cluster layout."
            )

        if (
            self.osp_cred_file is None
            and not self.reuse
            and self.cloud_type in CREDENTIAL_REQUIRED_CLOUDS
        ):
            raise CephCIArgumentError("Require cloud credentials to create cluster.")

        if not self.reuse and self.cloud_type == "onecloud" and not osp_cred:
            raise CephCIArgumentError(
                "OneCloud requires credentials. Use --osp-cred <file> or place "
                "globals.onecloud-credentials in ~/osp-cred-ci-2.yaml."
            )

        if (
            self.inventory_file is None
            and not self.reuse
            and self.cloud_type in INVENTORY_REQUIRED_CLOUDS
        ):
            raise CephCIArgumentError(
                "Require system configuration information to provision."
            )

        if self.ibm_build and self.upstream_build:
            raise CephCIArgumentError(
                "The following are not compatible --upstream-build and"
                " --custom-config ibm-build=True. Please rectify."
            )

    def warn_deprecations(self, args: Dict[str, Any]) -> None:
        """Log the deprecated spellings that were used on the command line.

        Args:
            args (dict): Parsed command line options.

        Returns:
            None
        """
        if args.get("--rhbuild") and not args.get("--release"):
            log.warning(
                "--rhbuild is deprecated, use --release to state the Ceph version."
            )

        if args.get("--upstream-build"):
            log.warning(
                "--upstream-build is deprecated, use --product community along with"
                " --release <upstream name>."
            )

        if self.ibm_build:
            log.warning(
                "--custom-config ibm-build=true is deprecated, use --product ibm."
            )
