"""Execution of a single test of a suite.

The executor owns everything that happens between picking a test out of the suite and
knowing its return code: assembling the configuration handed to the test module,
running it against each cluster it asks for, and gathering the error details a failed
test leaves behind.
"""

import datetime
import os
import traceback
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from cephci.cluster_info import collect_recipe
from cephci.orchestration.build import BuildArtifacts
from cephci.orchestration.config import RunConfig
from cephci.orchestration.results import store_cluster_state
from cli.performance.memory_and_cpu_utils import (
    start_logging_processes,
    stop_logging_process,
    upload_mem_and_cpu_logger_script,
)
from utility.log import Log

log = Log(__name__)

# Set by the test module when it wants a value to reach the next test.
KERNEL_REPO_ENV = "KERNEL-REPO-URL"


def build_test_config(
    test_config: Dict,
    config: RunConfig,
    artifacts: BuildArtifacts,
    osp_cred: Dict,
    skip_version_compare: bool,
) -> Tuple[Dict, bool]:
    """Merge the run settings into the configuration of a single test.

    The dictionary from the suite is updated in place, which is what the test modules
    expect.

    Args:
        test_config (dict): The ``config`` block of the test.
        config (RunConfig): The run settings.
        artifacts (BuildArtifacts): The build under test.
        osp_cred (dict): Credentials for the cloud.
        skip_version_compare (bool): The value in effect for this run so far.

    Returns:
        tuple: The enriched configuration, and the value of skip_version_compare to
            use from here on. A test that sets the key changes it for the tests that
            follow it, which is long standing behaviour.
    """
    if not test_config.get("base_url"):
        test_config["base_url"] = artifacts.base_url

    test_config["rhbuild"] = f"{config.rhbuild}-{config.platform}"
    test_config["cloud-type"] = config.cloud_type

    if config.skip_setup is True:
        test_config["skip_setup"] = True

    if config.skip_subscription is True:
        test_config["skip_subscription"] = True

    if test_config.get("skip_version_compare"):
        skip_version_compare = test_config.get("skip_version_compare")

    if config.add_repo and config.add_repo.startswith("http"):
        test_config["add-repo"] = config.add_repo

    test_config["build_type"] = artifacts.build
    test_config["upstream_build"] = config.upstream_build
    test_config["enable_eus"] = config.enable_eus
    test_config["skip_enabling_rhel_rpms"] = config.skip_enabling_rhel_rpms
    test_config["docker-insecure-registry"] = config.insecure_registry
    test_config["skip_version_compare"] = skip_version_compare
    test_config["container_image"] = artifacts.container_image

    if config.custom_config_dict:
        test_config["ibm_build"] = config.ibm_build

        if "enable-fips-mode" in config.custom_config_dict:
            test_config["enable_fips_mode"] = config.custom_config_dict[
                "enable-fips-mode"
            ]

        if "enable-firewall" in config.custom_config_dict:
            test_config["enable_firewall"] = config.custom_config_dict[
                "enable-firewall"
            ]

        if "podman-auth-file" in config.custom_config_dict:
            test_config["podman_auth_file"] = config.custom_config_dict[
                "podman-auth-file"
            ]

    test_config["ceph_docker_registry"] = artifacts.docker_registry
    test_config["ceph_docker_image"] = artifacts.docker_image
    test_config["ceph_docker_image_tag"] = artifacts.docker_tag

    # New manifest details
    test_config["manifest"] = artifacts.manifest
    test_config["product"] = artifacts.manifest.product
    test_config["platform"] = artifacts.manifest.platform
    test_config["release"] = artifacts.manifest.release

    if config.filestore:
        test_config["filestore"] = config.filestore

    if config.ec_pool_vals:
        test_config["ec-pool-k-m"] = config.ec_pool_vals

    if config.hotfix_repo and config.hotfix_repo.startswith("http"):
        test_config["hotfix_repo"] = config.hotfix_repo

    if config.kernel_repo is not None:
        test_config["kernel-repo"] = config.kernel_repo

    if osp_cred:
        test_config["osp_cred"] = osp_cred

    # if Kernel Repo is defined in ENV then set the value in config
    if os.environ.get(KERNEL_REPO_ENV) is not None:
        test_config["kernel-repo"] = os.environ.get(KERNEL_REPO_ENV)

    return test_config, skip_version_compare


def collect_module_errors(test_mod) -> Optional[Log]:
    """Return the logger a test module writes its errors to.

    Test modules keep a module level :class:`utility.log.Log` instance, and the errors
    reported through it are the only machine readable detail of a failure.

    Args:
        test_mod (ModuleType): The module under execution.

    Returns:
        Log: The logger of the module, or None when it does not define one.
    """
    for attribute in vars(test_mod).values():
        if type(attribute) is Log:
            return attribute

    return None


@dataclass
class ExecutionOutcome:
    """Result of running one test of the suite."""

    rc: int = 0
    parallel_results: List[Dict] = field(default_factory=list)
    clusters_used: List[str] = field(default_factory=list)


class TestExecutor:
    """Runs the tests of a suite against the clusters of the environment."""

    # Keeps pytest from collecting the class as a suite of its own.
    __test__ = False

    def __init__(
        self,
        config: RunConfig,
        artifacts: BuildArtifacts,
        clusters: Dict,
        clients: List,
        osp_cred: Dict,
        test_data: Dict,
        run_config: Dict,
        download_path: str,
        cluster_state_file: Optional[str] = None,
    ) -> None:
        """Initialize the executor.

        Args:
            config (RunConfig): The run settings.
            artifacts (BuildArtifacts): The build under test.
            clusters (dict): Cluster name to :class:`ceph.ceph.Ceph`.
            clients (list): Windows clients of the environment.
            osp_cred (dict): Credentials for the cloud.
            test_data (dict): State shared between the tests of a run.
            run_config (dict): Logging details of the current test.
            download_path (str): Where collected artifacts are stored.
            cluster_state_file (str): Where the environment is pickled after each
                test, when --store was requested.
        """
        self.config = config
        self.artifacts = artifacts
        self.clusters = clusters
        self.clients = clients
        self.osp_cred = osp_cred
        self.test_data = test_data
        self.run_config = run_config
        self.download_path = download_path
        self.cluster_state_file = cluster_state_file

        self.skip_version_compare = config.skip_version_compare
        self.enable_perf_mon = config.enable_perf_mon
        self.rhcs_version = artifacts.manifest.release

    def execute(
        self, test: Dict, tc: Dict, test_mod, module_name: str
    ) -> ExecutionOutcome:
        """Run a test against every cluster it applies to.

        Args:
            test (dict): The ``test`` block of the suite entry.
            tc (dict): The result record of this test, updated in place with the
                failure details when there are any.
            test_mod (ModuleType): The imported test module.
            module_name (str): The module name, used to honour --skip-tc.

        Returns:
            ExecutionOutcome: The return code and any results of parallel tests.
        """
        outcome = ExecutionOutcome()
        skipped = module_name in self.config.skip_tc_list and not test.get(
            "do-not-skip-tc", False
        )

        for cluster_name in test.get("clusters", self.clusters):
            outcome.clusters_used.append(cluster_name)
            cluster = self.clusters[cluster_name]

            self._start_monitoring_prereq(cluster)

            if test.get("clusters"):
                test_config = test.get("clusters").get(cluster_name).get("config", {})
            else:
                test_config = test.get("config", {})

            parallel = test.get("parallel", [])
            test_config, self.skip_version_compare = build_test_config(
                test_config,
                self.config,
                self.artifacts,
                self.osp_cred,
                self.skip_version_compare,
            )

            logging_process, tracker = None, None
            if self.enable_perf_mon:
                logging_process, tracker = start_logging_processes(
                    cluster, self.run_config["test_name"]
                )

            try:
                # FixMe: I don't think we use `build` as part of test data
                # configuration. This needs to investigated and fixed.
                if "build" in test_config.keys():
                    self.rhcs_version = test_config["build"]

                # Initialize the cluster with the expected rhcs_version
                cluster.rhcs_version = self.rhcs_version

                if skipped:
                    outcome.rc = -1
                elif parallel:
                    parallel_tcs, outcome.rc = test_mod.run(
                        ceph_cluster=cluster,
                        ceph_nodes=cluster,
                        config=test_config,
                        parallel=parallel,
                        test_data=self.test_data,
                        ceph_cluster_dict=self.clusters,
                        clients=self.clients,
                        run_config=self.run_config,
                        tc=tc,
                    )
                    outcome.parallel_results.extend(_as_durations(parallel_tcs))
                else:
                    outcome.rc = test_mod.run(
                        ceph_cluster=cluster,
                        ceph_nodes=cluster,
                        config=test_config,
                        parallel=parallel,
                        test_data=self.test_data,
                        ceph_cluster_dict=self.clusters,
                        clients=self.clients,
                        run_config=self.run_config,
                    )
            except BaseException as be:  # noqa
                # Log exception to stdout
                log.exception(be)

                # Set failure details
                tc["err_type"] = "exception"
                tc["err_msg"] = str(be)
                tc["err_text"] = traceback.format_exc()

                # Set return code to 1
                outcome.rc = 1
            finally:
                self._stop_monitoring(cluster, logging_process, tracker)
                collect_recipe(cluster)

                if self.cluster_state_file:
                    store_cluster_state(self.clusters, self.cluster_state_file)

                # Artifacts from test appended to comments
                if test_config.get("artifacts"):
                    tc["comments"] += f"\n{test_config['artifacts']}"

            module_log = collect_module_errors(test_mod)
            if outcome.rc != 0:
                if module_log and not (tc.get("err_type") == "exception"):
                    tc["err_type"] = "error"
                    tc["err_msg"] = "\n".join(map(str, module_log._log_errors))

                break

        self._reset_module_errors(test_mod)

        return outcome

    def _start_monitoring_prereq(self, cluster) -> None:
        """Upload the performance monitoring scripts when monitoring is requested."""
        if not self.enable_perf_mon:
            return

        if not upload_mem_and_cpu_logger_script(cluster):
            log.error(
                "Failed to upload Memory and CPU monitoring scripts to nodes. "
                "The tests will proceed without monitoring"
            )
            self.enable_perf_mon = False

    def _stop_monitoring(self, cluster, logging_process, tracker) -> None:
        """Stop the performance monitoring started for a test."""
        if not self.enable_perf_mon or logging_process is None:
            return

        stop_logging_process(cluster, logging_process, self.download_path, tracker)

    @staticmethod
    def _reset_module_errors(test_mod) -> None:
        """Drop the errors a test module reported, before the next test runs."""
        module_log = collect_module_errors(test_mod)
        if module_log:
            module_log._log_errors = []


def _as_durations(parallel_tcs: List[Dict]) -> List[Dict]:
    """Convert the duration of parallel results into timedelta objects.

    Args:
        parallel_tcs (list): Result records of the tests run in parallel.

    Returns:
        list: The same records, with ``duration`` usable by the xunit writer.
    """
    for ptc in parallel_tcs:
        value = ptc.get("duration", "00:00:00")
        if isinstance(value, str):
            hours, minutes, seconds = value.split(":")
            ptc["duration"] = datetime.timedelta(
                hours=int(hours), minutes=int(minutes), seconds=float(seconds)
            )

    return parallel_tcs
