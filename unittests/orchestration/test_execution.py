"""Assembly of the configuration handed to a test module."""

import mock
import pytest

from cephci.orchestration.build import BuildArtifacts
from cephci.orchestration.config import RunConfig
from cephci.orchestration.execution import build_test_config, collect_module_errors
from utility.log import Log


@pytest.fixture
def artifacts():
    manifest = mock.MagicMock()
    manifest.product = "ibm"
    manifest.platform = "rhel-9"
    manifest.release = "8.1"

    return BuildArtifacts(
        manifest=manifest,
        build="nightly",
        base_url="http://example.com/compose",
        docker_registry="registry.example.com",
        docker_image="rhceph/rhceph-8-rhel9",
        docker_tag="latest",
        ceph_version="19.2.0-1",
    )


@pytest.fixture
def config():
    return RunConfig(
        suites=["suite.yaml"],
        cluster_conf_file="conf.yaml",
        inventory_file=None,
        osp_cred_file=None,
        product="ibm",
        release="8.1",
        platform="rhel-9",
        rhbuild="8.1",
        build="nightly",
        cloud_type="openstack",
    )


def test_run_details_reach_the_test(config, artifacts):
    result, _ = build_test_config({}, config, artifacts, {}, False)

    assert result["base_url"] == "http://example.com/compose"
    assert result["rhbuild"] == "8.1-rhel-9"
    assert result["cloud-type"] == "openstack"
    assert result["build_type"] == "nightly"
    assert result["container_image"] == (
        "registry.example.com/rhceph/rhceph-8-rhel9:latest"
    )
    assert result["ceph_docker_registry"] == "registry.example.com"
    assert result["ceph_docker_image"] == "rhceph/rhceph-8-rhel9"
    assert result["ceph_docker_image_tag"] == "latest"
    assert result["product"] == "ibm"
    assert result["platform"] == "rhel-9"
    assert result["release"] == "8.1"
    assert result["manifest"] is artifacts.manifest


def test_suite_base_url_is_not_overwritten(config, artifacts):
    result, _ = build_test_config(
        {"base_url": "http://suite/compose"}, config, artifacts, {}, False
    )

    assert result["base_url"] == "http://suite/compose"


def test_skip_flags_are_only_set_when_requested(config, artifacts):
    result, _ = build_test_config({}, config, artifacts, {}, False)
    assert "skip_setup" not in result
    assert "skip_subscription" not in result

    skipping = RunConfig(**{**config.__dict__, "skip_setup": True})
    result, _ = build_test_config({}, skipping, artifacts, {}, False)
    assert result["skip_setup"] is True


def test_a_test_can_turn_off_version_comparison_for_the_rest_of_the_run(
    config, artifacts
):
    first, skip_version_compare = build_test_config(
        {"skip_version_compare": True}, config, artifacts, {}, False
    )
    assert first["skip_version_compare"] is True
    assert skip_version_compare is True

    # The next test inherits the value even though it does not ask for it.
    second, _ = build_test_config({}, config, artifacts, {}, skip_version_compare)
    assert second["skip_version_compare"] is True


def test_repositories_are_only_accepted_as_urls(config, artifacts):
    with_repos = RunConfig(
        **{
            **config.__dict__,
            "add_repo": "http://example.com/repo",
            "hotfix_repo": "not-a-url",
        }
    )

    result, _ = build_test_config({}, with_repos, artifacts, {}, False)

    assert result["add-repo"] == "http://example.com/repo"
    assert "hotfix_repo" not in result


def test_kernel_repo_comes_from_the_environment_when_set(config, artifacts):
    with_repo = RunConfig(**{**config.__dict__, "kernel_repo": "http://kernel/repo"})

    result, _ = build_test_config({}, with_repo, artifacts, {}, False)
    assert result["kernel-repo"] == "http://kernel/repo"

    with mock.patch.dict("os.environ", {"KERNEL-REPO-URL": "http://env/repo"}):
        result, _ = build_test_config({}, with_repo, artifacts, {}, False)

    assert result["kernel-repo"] == "http://env/repo"


def test_custom_configuration_is_forwarded(config, artifacts):
    custom = RunConfig(
        **{
            **config.__dict__,
            "ibm_build": True,
            "custom_config_dict": {
                "enable-fips-mode": "true",
                "enable-firewall": "true",
                "podman-auth-file": "/root/auth.json",
            },
        }
    )

    result, _ = build_test_config({}, custom, artifacts, {}, False)

    assert result["ibm_build"] is True
    assert result["enable_fips_mode"] == "true"
    assert result["enable_firewall"] == "true"
    assert result["podman_auth_file"] == "/root/auth.json"


def test_credentials_are_shared_with_the_test(config, artifacts):
    credentials = {"globals": {"openstack-credentials": {}}}

    result, _ = build_test_config({}, config, artifacts, credentials, False)

    assert result["osp_cred"] is credentials


def test_the_logger_of_a_module_is_found():
    module = mock.MagicMock()
    logger = Log("test")
    module_attributes = {"log": logger, "other": "value"}

    with mock.patch(
        "cephci.orchestration.execution.vars", return_value=module_attributes
    ):
        assert collect_module_errors(module) is logger


def test_a_module_without_a_logger_reports_nothing():
    with mock.patch("cephci.orchestration.execution.vars", return_value={"a": 1}):
        assert collect_module_errors(mock.MagicMock()) is None


class FakeModule:
    """A test module that records how it was called."""

    def __init__(self, results=None):
        self.results = list(results or [0])
        self.calls = []

    def run(self, **kwargs):
        self.calls.append(kwargs)

        return self.results.pop(0) if self.results else 0


def make_executor(config, artifacts, clusters=None, **overrides):
    """Build an executor with the collaborators stubbed out."""
    from cephci.orchestration.execution import TestExecutor

    arguments = {
        "config": config,
        "artifacts": artifacts,
        "clusters": clusters or {"ceph": mock.MagicMock()},
        "clients": [],
        "osp_cred": {},
        "test_data": {},
        "run_config": {"test_name": "test_bootstrap"},
        "download_path": "/tmp",
        "cluster_state_file": None,
    }
    arguments.update(overrides)

    return TestExecutor(**arguments)


def record():
    return {"comments": "", "name": "Bootstrap"}


class TestExecution:
    """Running a single test of a suite."""

    @pytest.fixture(autouse=True)
    def _no_recipe(self):
        with mock.patch("cephci.orchestration.execution.collect_recipe") as recipe:
            self.recipe = recipe
            yield

    def test_a_test_runs_against_the_only_cluster(self, config, artifacts):
        module = FakeModule()
        executor = make_executor(config, artifacts)

        outcome = executor.execute({"config": {}}, record(), module, "test_bootstrap")

        assert outcome.rc == 0
        assert outcome.clusters_used == ["ceph"]
        assert len(module.calls) == 1
        assert module.calls[0]["config"]["rhbuild"] == "8.1-rhel-9"

    def test_a_test_runs_against_every_cluster_it_names(self, config, artifacts):
        module = FakeModule([0, 0])
        clusters = {"site1": mock.MagicMock(), "site2": mock.MagicMock()}
        executor = make_executor(config, artifacts, clusters=clusters)
        test = {
            "clusters": {
                "site1": {"config": {"role": "primary"}},
                "site2": {"config": {"role": "secondary"}},
            }
        }

        outcome = executor.execute(test, record(), module, "test_multisite")

        assert outcome.clusters_used == ["site1", "site2"]
        assert [call["config"]["role"] for call in module.calls] == [
            "primary",
            "secondary",
        ]

    def test_a_failure_skips_the_remaining_clusters(self, config, artifacts):
        module = FakeModule([1, 0])
        clusters = {"site1": mock.MagicMock(), "site2": mock.MagicMock()}
        executor = make_executor(config, artifacts, clusters=clusters)
        test = {"clusters": {"site1": {"config": {}}, "site2": {"config": {}}}}

        outcome = executor.execute(test, record(), module, "test_multisite")

        assert outcome.rc == 1
        assert len(module.calls) == 1

    def test_an_exception_is_recorded_against_the_test(self, config, artifacts):
        module = mock.MagicMock()
        module.run.side_effect = RuntimeError("cluster unreachable")
        tc = record()

        outcome = make_executor(config, artifacts).execute(
            {"config": {}}, tc, module, "test_bootstrap"
        )

        assert outcome.rc == 1
        assert tc["err_type"] == "exception"
        assert tc["err_msg"] == "cluster unreachable"
        assert "RuntimeError" in tc["err_text"]

    def test_skipped_tests_are_not_run(self, config, artifacts):
        module = FakeModule()
        skipping = RunConfig(**{**config.__dict__, "skip_tc_list": ["test_bootstrap"]})

        outcome = make_executor(skipping, artifacts).execute(
            {"config": {}}, record(), module, "test_bootstrap"
        )

        assert outcome.rc == -1
        assert module.calls == []

    def test_a_test_can_refuse_to_be_skipped(self, config, artifacts):
        module = FakeModule()
        skipping = RunConfig(**{**config.__dict__, "skip_tc_list": ["test_bootstrap"]})

        outcome = make_executor(skipping, artifacts).execute(
            {"config": {}, "do-not-skip-tc": True}, record(), module, "test_bootstrap"
        )

        assert outcome.rc == 0
        assert len(module.calls) == 1

    def test_parallel_tests_report_their_own_results(self, config, artifacts):
        module = mock.MagicMock()
        module.run.return_value = ([{"name": "child", "duration": "00:01:30"}], 0)
        test = {"config": {}, "parallel": [{"test": {"name": "child"}}]}

        outcome = make_executor(config, artifacts).execute(
            test, record(), module, "test_parallel"
        )

        assert outcome.rc == 0
        assert outcome.parallel_results[0]["duration"].total_seconds() == 90

    def test_artifacts_of_a_test_reach_its_comments(self, config, artifacts):
        module = FakeModule()
        tc = record()

        make_executor(config, artifacts).execute(
            {"config": {"artifacts": "http://logs/report.html"}},
            tc,
            module,
            "test_bootstrap",
        )

        assert "http://logs/report.html" in tc["comments"]

    def test_the_environment_is_stored_after_every_test(self, config, artifacts):
        module = FakeModule()
        executor = make_executor(config, artifacts, cluster_state_file="rerun/ci-abc")

        with mock.patch("cephci.orchestration.execution.store_cluster_state") as store:
            executor.execute({"config": {}}, record(), module, "test_bootstrap")

        store.assert_called_once_with(executor.clusters, "rerun/ci-abc")

    def test_the_version_of_the_cluster_follows_the_test(self, config, artifacts):
        module = FakeModule()
        cluster = mock.MagicMock()
        executor = make_executor(config, artifacts, clusters={"ceph": cluster})

        executor.execute({"config": {}}, record(), module, "test_bootstrap")
        assert cluster.rhcs_version == "8.1"

        executor.execute({"config": {"build": "9.0"}}, record(), module, "test_upgrade")
        assert cluster.rhcs_version == "9.0"

    def test_the_recipe_of_the_cluster_is_collected(self, config, artifacts):
        make_executor(config, artifacts).execute(
            {"config": {}}, record(), FakeModule(), "test_bootstrap"
        )

        self.recipe.assert_called_once()
