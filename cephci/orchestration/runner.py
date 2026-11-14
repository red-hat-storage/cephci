"""Orchestration of a CephCI execution.

The run is a sequence of phases: understand the arguments, resolve the build, create or
reuse an environment, execute the suite against it, then report and collect whatever
the failures left behind.
"""

import datetime
import pickle
import traceback
from copy import deepcopy
from typing import Dict, List, Optional, Tuple

import init_suite
from cephci.orchestration import build
from cephci.orchestration.artifacts import ArtifactCollector
from cephci.orchestration.config import RunConfig
from cephci.orchestration.console import print_results
from cephci.orchestration.context import RunContext
from cephci.orchestration.execution import TestExecutor
from cephci.orchestration.loader import TestModuleLoader
from cephci.orchestration.provisioning import cleanup_environment, create_nodes
from cephci.orchestration.results import (
    RunResults,
    elapsed_since,
    status_of,
    store_cluster_state,
)
from cephci.providers import get_provider
from cephci.utils.utility import load_file
from utility.log import Log
from utility.polarion import post_to_polarion
from utility.utils import create_unique_test_name

log = Log(__name__)


def run(args: Dict) -> int:
    """Execute the test suites described by the given arguments.

    Args:
        args (dict): Parsed command line options.

    Returns:
        int: 0 when every test passed, 1 otherwise.
    """
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    config = RunConfig.from_args(args)
    osp_cred = load_file(config.osp_cred_file) if config.osp_cred_file else dict()

    context = RunContext.create(config)
    config.warn_deprecations(args)

    if config.is_cleanup_only:
        return _cleanup_only(config, osp_cred)

    config.validate_execution(osp_cred)

    artifacts = build.resolve(config)
    log.info(f"The CLI for the current run :\n{context.cli_arguments}\n")
    log.info(f"RPM Compose source - {artifacts.base_url}")
    log.info(f"Red Hat Ceph Image used - {artifacts.container_image}")
    log.info("Testing Ceph Version: %s" % (artifacts.ceph_version))

    cluster_conf = load_file(config.cluster_conf_file)
    suite = init_suite.load_suites(config.suites)
    log.debug(f"Found the following valid test suites: {suite['tests']}")
    if suite["nan"] and not suite["tests"]:
        raise Exception("Please provide valid test suite name")

    inventory = _load_inventory(config)
    results = RunResults(
        context, config, artifacts, _collect_distro(cluster_conf, inventory)
    )

    provider = get_provider(config.cloud_type)
    clusters, clients = _environment(
        config, context, results, provider, cluster_conf, inventory, osp_cred, suite
    )
    if clusters is None:
        return 1

    cluster_state_file = None
    if config.store:
        cluster_state_file = context.cluster_state_file(config.instances_name)
        store_cluster_state(clusters, cluster_state_file)

    loader = TestModuleLoader()
    loader.install()

    test_data = {
        "custom-config": config.custom_config,
        "custom-config-file": config.custom_config_file,
        "custom_config_dict": deepcopy(config.custom_config_dict),
    }
    run_config = {"log_dir": context.run_dir, "run_id": context.run_id}
    executor = TestExecutor(
        config=config,
        artifacts=artifacts,
        clusters=clusters,
        clients=clients,
        osp_cred=osp_cred,
        test_data=test_data,
        run_config=run_config,
        download_path=config.log_directory or context.run_dir,
        cluster_state_file=cluster_state_file,
    )

    jenkins_rc = _execute_suite(
        suite.get("tests"),
        config=config,
        context=context,
        results=results,
        executor=executor,
        loader=loader,
        run_config=run_config,
        provider=provider,
        osp_cred=osp_cred,
        cluster_conf=cluster_conf,
        inventory=inventory,
    )

    _report(context, config, results)
    _collect_artifacts(config, context, executor.clusters, jenkins_rc)

    return jenkins_rc


def _cleanup_only(config: RunConfig, osp_cred: Dict) -> int:
    """Remove an environment and return without executing any test.

    Args:
        config (RunConfig): The run settings.
        osp_cred (dict): Credentials for the cloud.

    Returns:
        int: Always 0.
    """
    config.validate_cleanup(osp_cred)

    provider = get_provider(config.cloud_type)
    if provider is None:
        log.warning("Unknown cloud type.")
        return 0

    provider.cleanup(osp_cred, config.cleanup_name, custom_config=config.custom_config)

    return 0


def _environment(
    config: RunConfig,
    context: RunContext,
    results: RunResults,
    provider,
    cluster_conf: Dict,
    inventory: Optional[Dict],
    osp_cred: Dict,
    suite: Dict,
) -> Tuple[Optional[Dict], List]:
    """Create or restore the environment the suite runs against.

    Args:
        config (RunConfig): The run settings.
        context (RunContext): Identity and timing of the run.
        results (RunResults): Collection the failure is reported through.
        provider (CloudProvider): The infrastructure to provision on.
        cluster_conf (dict): Cluster layout.
        inventory (dict): Test environment.
        osp_cred (dict): Credentials for the cloud.
        suite (dict): The loaded test suites.

    Returns:
        tuple: Mapping of cluster name to :class:`ceph.ceph.Ceph` and the Windows
            clients, or ``(None, [])`` when provisioning failed. The failure has been
            reported by e-mail in that case.
    """
    if config.reuse:
        with open(config.reuse, "rb") as stored_nodes:
            clusters = pickle.load(stored_nodes)

        for cluster in clusters.values():
            for node in cluster:
                node.reconnect()

        return clusters, []

    try:
        if provider is None:
            log.error(f"Unknown cloud type: {config.cloud_type}")
            raise AssertionError("Unsupported test environment.")

        return create_nodes(
            cluster_conf,
            inventory,
            osp_cred,
            context.run_id,
            provider,
            config.cloud_type,
            config.instances_name,
            enable_eus=config.enable_eus,
            custom_config=config.custom_config,
            platform=config.platform,
        )
    except Exception as err:
        log.error(err)
        results.email(
            {
                "status": "Fail",
                "trace": (traceback.format_exc(limit=2)).split("\n"),
            },
            records=results.record_all(suite.get("tests")),
            with_clusters=False,
        )

        return None, []


def _execute_suite(
    tests: List[Dict],
    config: RunConfig,
    context: RunContext,
    results: RunResults,
    executor: TestExecutor,
    loader: TestModuleLoader,
    run_config: Dict,
    provider,
    osp_cred: Dict,
    cluster_conf: Dict,
    inventory: Optional[Dict],
) -> int:
    """Run every test of the suite and record what happened.

    Args:
        tests (list): The suite entries.
        config (RunConfig): The run settings.
        context (RunContext): Identity and timing of the run.
        results (RunResults): Collection of the result records.
        executor (TestExecutor): Runs a single test.
        loader (TestModuleLoader): Imports the module of a test.
        run_config (Dict): Logging details, updated for every test.
        provider (CloudProvider): The infrastructure in use.
        cluster_conf (dict): Cluster layout, for tests recreating the environment.
        inventory (dict): Test environment, for tests recreating the environment.
        osp_cred (dict): Credentials for the cloud.

    Returns:
        int: 0 when every test passed, 1 otherwise.
    """
    jenkins_rc = 0
    test_names: List[str] = []

    for entry in tests:
        test = entry.get("test")
        tc = results.record(test)

        unique_test_name = create_unique_test_name(tc["name"], test_names)
        test_names.append(unique_test_name)

        tc["log-link"] = log.configure_logger(
            unique_test_name, context.run_dir, config.disable_console_log
        )
        run_config.update({"test_name": unique_test_name, "log_link": tc["log-link"]})

        test_file = tc["file"]
        module_name = loader.module_name(test_file)
        test_mod = loader.resolve(test_file)

        print("\nRunning test: {test_name}".format(test_name=tc["name"]))
        if tc.get("log-link"):
            print("Test logfile location: {log_url}".format(log_url=tc["log-link"]))

        log.info(f"Running test {test_file}")
        start = datetime.datetime.now()

        outcome = executor.execute(test, tc, test_mod, module_name)

        results.note_clusters(outcome.clusters_used)
        results.extend(outcome.parallel_results)

        # Calculate test execution time
        tc["duration"] = elapsed_since(start)
        tc["status"] = status_of(outcome.rc)

        msg = "Test {} {}".format(
            test_mod,
            {"Pass": "passed", "Skipped": "Skipped"}.get(tc["status"], "failed"),
        )
        log.info(msg)
        print(msg)

        if tc["status"] == "Failed":
            jenkins_rc = 1

        if config.post_results:
            post_to_polarion(tc=tc)

        if tc["status"] == "Failed" and test.get("abort-on-fail", False):
            log.info("Aborting on test failure")
            results.add(tc)
            break

        if test.get("destroy-cluster") is True:
            # User provided infrastructure is never torn down.
            if provider and provider.cleanup_before_create:
                cleanup_environment(
                    provider, osp_cred, config.instances_name, config.custom_config
                )

        if test.get("recreate-cluster") is True:
            executor.clusters, executor.clients = create_nodes(
                cluster_conf,
                inventory,
                osp_cred,
                context.run_id,
                provider,
                config.cloud_type,
                config.instances_name,
                enable_eus=config.enable_eus,
                platform=config.platform,
            )

        results.add(tc)

    return jenkins_rc


def _report(context: RunContext, config: RunConfig, results: RunResults) -> None:
    """Publish the outcome of the run.

    Args:
        context (RunContext): Identity and timing of the run.
        config (RunConfig): The run settings.
        results (RunResults): The collected result records.

    Returns:
        None
    """
    log.info("\nAll test logs located here: {base}".format(base=context.url_base))
    log.close_and_remove_filehandlers()

    results.xunit()

    print("\nAll test logs located here: {base}".format(base=context.url_base))
    print_results(results.records)

    results.write_summary()
    results.email({"status": "Pass"})


def _collect_artifacts(
    config: RunConfig, context: RunContext, clusters: Dict, jenkins_rc: int
) -> None:
    """Gather the diagnostics requested for this run.

    Args:
        config (RunConfig): The run settings.
        context (RunContext): Identity and timing of the run.
        clusters (dict): Cluster name to :class:`ceph.ceph.Ceph`.
        jenkins_rc (int): Outcome of the run.

    Returns:
        None
    """
    collector = ArtifactCollector(clusters, context.run_dir, context.url_base)

    if jenkins_rc or config.collect_coredump:
        collector.coredumps()

    if jenkins_rc and not config.skip_sos_report:
        collector.sosreports()

    if jenkins_rc or config.collect_ceph_logs:
        collector.ceph_logs()


def _load_inventory(config: RunConfig) -> Optional[Dict]:
    """Read the inventory, applying the image requested on the command line.

    Args:
        config (RunConfig): The run settings.

    Returns:
        dict: The inventory, or None when the run does not use one.
    """
    if not config.inventory_file:
        return None

    inventory = load_file(config.inventory_file)
    if config.osp_image and inventory.get("instance", {}).get("create"):
        inventory.get("instance").get("create").update({"image-name": config.osp_image})

    return inventory


def _collect_distro(cluster_conf: Dict, inventory: Optional[Dict]) -> str:
    """Determine the operating system images the environment is built from.

    Args:
        cluster_conf (dict): Cluster layout.
        inventory (dict): Test environment.

    Returns:
        str: The images in use, as a comma separated list.
    """
    distro = []
    if inventory:
        distro.extend(_image_names(inventory))

    for cluster in cluster_conf.get("globals"):
        cluster_inventory = cluster.get("ceph-cluster").get("inventory")
        if cluster_inventory:
            distro.extend(_image_names(load_file(cluster_inventory)))

    return ", ".join(list(set(distro)))


def _image_names(inventory: Dict) -> List[str]:
    """Return the image an inventory creates instances from.

    Args:
        inventory (dict): Test environment.

    Returns:
        list: The image name, when the inventory states one.
    """
    create = inventory.get("instance", {}).get("create", {})
    image_name = create.get("image-name") or create.get("image_id")

    if not create or image_name is None:
        return []

    return [str(image_name).replace(".iso", "")]
