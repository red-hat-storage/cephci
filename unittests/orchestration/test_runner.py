"""Phase orchestration of a run."""

import mock
import pytest

from cephci.orchestration import runner
from cephci.orchestration.build import BuildArtifacts
from cephci.orchestration.config import RunConfig
from cephci.orchestration.context import RunContext
from cephci.orchestration.execution import ExecutionOutcome
from cephci.orchestration.results import RunResults


def make_config(**overrides):
    defaults = {
        "suites": ["suite.yaml"],
        "cluster_conf_file": "conf.yaml",
        "inventory_file": None,
        "osp_cred_file": None,
        "product": "ibm",
        "release": "8.1",
        "platform": "rhel-9",
        "rhbuild": "8.1",
        "instances_name": "ci-abc",
    }
    defaults.update(overrides)

    return RunConfig(**defaults)


@pytest.fixture
def harness(tmp_path):
    """A run with its execution and reporting stubbed out."""
    config = make_config()
    context = RunContext("abc123", str(tmp_path))
    artifacts = BuildArtifacts(
        manifest=mock.MagicMock(),
        build="nightly",
        base_url="http://example.com",
        docker_registry="registry",
        docker_image="image",
        docker_tag="tag",
        ceph_version="19.2.0",
    )
    results = RunResults(context, config, artifacts, "RHEL-9.4")

    executor = mock.MagicMock()
    executor.execute.return_value = ExecutionOutcome(rc=0, clusters_used=["ceph"])

    loader = mock.MagicMock()
    loader.module_name.side_effect = lambda name: name.replace(".py", "")

    return {
        "config": config,
        "context": context,
        "results": results,
        "executor": executor,
        "loader": loader,
    }


def suite(*tests):
    return [{"test": test} for test in tests]


TEST_ENTRY = {
    "name": "Bootstrap",
    "desc": "Bootstrap the cluster",
    "module": "test_bootstrap.py",
}


def execute(harness, tests, **overrides):
    """Run the suite loop with the collaborators of the harness."""
    arguments = {
        "config": harness["config"],
        "context": harness["context"],
        "results": harness["results"],
        "executor": harness["executor"],
        "loader": harness["loader"],
        "run_config": {},
        "provider": mock.MagicMock(),
        "osp_cred": {},
        "cluster_conf": {"globals": []},
        "inventory": None,
    }
    arguments.update(overrides)

    with mock.patch.object(runner, "post_to_polarion") as polarion:
        rc = runner._execute_suite(tests, **arguments)

    return rc, polarion


def test_a_passing_suite_returns_success(harness):
    rc, _ = execute(harness, suite(TEST_ENTRY, TEST_ENTRY))

    records = harness["results"].records
    assert rc == 0
    assert [record["status"] for record in records] == ["Pass", "Pass"]
    assert harness["results"].clusters_used == ["ceph"]


def test_a_failure_fails_the_run_without_stopping_it(harness):
    harness["executor"].execute.side_effect = [
        ExecutionOutcome(rc=1),
        ExecutionOutcome(rc=0),
    ]

    rc, _ = execute(harness, suite(TEST_ENTRY, TEST_ENTRY))

    assert rc == 1
    assert [record["status"] for record in harness["results"].records] == [
        "Failed",
        "Pass",
    ]


def test_abort_on_fail_stops_the_suite(harness):
    harness["executor"].execute.side_effect = [
        ExecutionOutcome(rc=1),
        ExecutionOutcome(rc=0),
    ]
    aborting = dict(TEST_ENTRY, **{"abort-on-fail": True})

    rc, _ = execute(harness, suite(aborting, TEST_ENTRY))

    assert rc == 1
    assert len(harness["results"].records) == 1


def test_a_skipped_test_does_not_fail_the_run(harness):
    harness["executor"].execute.return_value = ExecutionOutcome(rc=-1)

    rc, _ = execute(harness, suite(TEST_ENTRY))

    assert rc == 0
    assert harness["results"].records[0]["status"] == "Skipped"


def test_results_are_posted_when_requested(harness):
    _, polarion = execute(harness, suite(TEST_ENTRY))
    polarion.assert_not_called()

    harness["config"] = make_config(post_results=True)
    _, polarion = execute(harness, suite(TEST_ENTRY))
    polarion.assert_called_once()


def test_parallel_results_are_reported_before_their_parent(harness):
    harness["executor"].execute.return_value = ExecutionOutcome(
        rc=0, parallel_results=[{"name": "child", "status": "Pass"}]
    )

    execute(harness, suite(TEST_ENTRY))

    assert [record["name"] for record in harness["results"].records] == [
        "child",
        "Bootstrap",
    ]


def test_a_suite_can_ask_for_the_environment_to_be_removed(harness):
    provider = mock.MagicMock()
    provider.cleanup_before_create = True
    destroying = dict(TEST_ENTRY, **{"destroy-cluster": True})

    with mock.patch.object(runner, "cleanup_environment") as cleanup:
        execute(harness, suite(destroying), provider=provider)

    cleanup.assert_called_once_with(provider, {}, "ci-abc", [])


def test_user_provided_infrastructure_is_not_removed_by_a_suite(harness):
    provider = mock.MagicMock()
    provider.cleanup_before_create = False
    destroying = dict(TEST_ENTRY, **{"destroy-cluster": True})

    with mock.patch.object(runner, "cleanup_environment") as cleanup:
        execute(harness, suite(destroying), provider=provider)

    cleanup.assert_not_called()


def test_a_suite_can_ask_for_a_new_environment(harness):
    recreating = dict(TEST_ENTRY, **{"recreate-cluster": True})

    with mock.patch.object(
        runner, "create_nodes", return_value=({"ceph": "cluster"}, [])
    ) as create:
        execute(harness, suite(recreating))

    create.assert_called_once()
    assert harness["executor"].clusters == {"ceph": "cluster"}
    # Unlike the initial creation, the custom configuration is not forwarded.
    assert "custom_config" not in create.call_args.kwargs


def test_test_names_are_made_unique(harness):
    execute(harness, suite(TEST_ENTRY, TEST_ENTRY))

    names = [call.args[0] for call in harness["loader"].resolve.call_args_list]
    assert names == ["test_bootstrap.py", "test_bootstrap.py"]
    assert harness["context"].run_id == "abc123"


class TestInventory:
    """Reading the inventory and the images it installs."""

    def test_no_inventory_is_allowed(self):
        assert runner._load_inventory(make_config()) is None

    def test_the_requested_image_replaces_the_one_of_the_inventory(self):
        inventory = {"instance": {"create": {"image-name": "RHEL-9.4"}}}
        config = make_config(inventory_file="inv.yaml", osp_image="RHEL-10.0")

        with mock.patch.object(runner, "load_file", return_value=inventory):
            resolved = runner._load_inventory(config)

        assert resolved["instance"]["create"]["image-name"] == "RHEL-10.0"

    def test_images_of_every_inventory_are_collected(self):
        cluster_conf = {
            "globals": [
                {"ceph-cluster": {"name": "ceph", "inventory": "other.yaml"}},
                {"ceph-cluster": {"name": "ceph-sec"}},
            ]
        }
        inventory = {"instance": {"create": {"image-name": "RHEL-9.4"}}}
        other = {"instance": {"create": {"image_id": "RHEL-10.0.iso"}}}

        with mock.patch.object(runner, "load_file", return_value=other):
            distro = runner._collect_distro(cluster_conf, inventory)

        assert sorted(distro.split(", ")) == ["RHEL-10.0", "RHEL-9.4"]

    def test_an_inventory_without_an_image_contributes_nothing(self):
        assert runner._collect_distro({"globals": []}, {"instance": {}}) == ""


class TestEndToEnd:
    """The phases of a run, wired together as the entry point wires them."""

    SUITE = {
        "tests": [
            {
                "test": {
                    "name": "Bootstrap",
                    "desc": "Bootstrap the cluster",
                    "module": "test_bootstrap.py",
                    "config": {"command": "bootstrap"},
                }
            },
            {
                "test": {
                    "name": "Add host",
                    "desc": "Add a host to the cluster",
                    "module": "test_host.py",
                }
            },
        ],
        "nan": [],
    }

    @staticmethod
    def argv(log_dir):
        return [
            "--release",
            "8.1",
            "--product",
            "ibm",
            "--platform",
            "rhel-9",
            "--cloud",
            "baremetal",
            "--cluster-conf",
            "conf/squid/cephadm/sanity.yaml",
            "--suite",
            "suites/squid/cephadm/tier0-cephadm.yaml",
            "--log-dir",
            str(log_dir),
        ]

    @staticmethod
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

    def run(self, tmp_path, test_module, suite=None):
        """Drive runner.run() with the infrastructure and reporting stubbed out."""
        from docopt import docopt

        from cephci.orchestration.doc import doc

        args = docopt(doc, argv=self.argv(tmp_path))
        loader = mock.MagicMock()
        loader.module_name.side_effect = lambda name: name.replace(".py", "")
        loader.resolve.return_value = test_module

        with mock.patch.object(
            runner, "load_file", return_value={"globals": []}
        ), mock.patch.object(
            runner.init_suite, "load_suites", return_value=suite or self.SUITE
        ), mock.patch.object(
            runner.build, "resolve", return_value=self.artifacts()
        ), mock.patch.object(
            runner, "create_nodes", return_value=({"ceph": mock.MagicMock()}, [])
        ), mock.patch.object(
            runner, "TestModuleLoader", return_value=loader
        ), mock.patch.object(
            runner, "ArtifactCollector"
        ) as collector, mock.patch(
            "cephci.orchestration.execution.collect_recipe"
        ), mock.patch(
            "cephci.orchestration.results.email_results"
        ) as email, mock.patch(
            "cephci.orchestration.results.create_xunit_results"
        ):
            rc = runner.run(args)

        self.collector = collector.return_value

        return rc, email

    def test_a_passing_run_reports_success(self, tmp_path):
        module = mock.MagicMock()
        module.run.return_value = 0

        rc, email = self.run(tmp_path, module)

        assert rc == 0
        assert module.run.call_count == 2

        records = email.call_args.kwargs["test_result"]["result"]
        assert [record["status"] for record in records] == ["Pass", "Pass"]
        assert records[0]["name"] == "Bootstrap"

    def test_the_run_configuration_reaches_the_test(self, tmp_path):
        module = mock.MagicMock()
        module.run.return_value = 0

        self.run(tmp_path, module)

        config = module.run.call_args.kwargs["config"]
        assert config["rhbuild"] == "8.1-rhel-9"
        assert config["cloud-type"] == "baremetal"
        assert config["base_url"] == "http://example.com/compose"
        assert config["container_image"] == (
            "registry.example.com/rhceph/rhceph-8-rhel9:latest"
        )

    def test_a_failing_test_fails_the_run(self, tmp_path):
        module = mock.MagicMock()
        module.run.side_effect = [1, 0]

        rc, email = self.run(tmp_path, module)

        assert rc == 1
        records = email.call_args.kwargs["test_result"]["result"]
        assert [record["status"] for record in records] == ["Failed", "Pass"]

    def test_an_exception_is_reported_as_a_failure(self, tmp_path):
        module = mock.MagicMock()
        module.run.side_effect = RuntimeError("the cluster is unreachable")

        rc, email = self.run(tmp_path, module)

        assert rc == 1
        record = email.call_args.kwargs["test_result"]["result"][0]
        assert record["status"] == "Failed"
        assert record["err_type"] == "exception"
        assert "unreachable" in record["err_msg"]

    def test_a_run_that_cannot_provision_is_reported(self, tmp_path):
        from docopt import docopt

        from cephci.orchestration.doc import doc

        args = docopt(doc, argv=self.argv(tmp_path))

        with mock.patch.object(
            runner, "load_file", return_value={"globals": []}
        ), mock.patch.object(
            runner.init_suite, "load_suites", return_value=self.SUITE
        ), mock.patch.object(
            runner.build, "resolve", return_value=self.artifacts()
        ), mock.patch.object(
            runner, "create_nodes", side_effect=RuntimeError("no nodes available")
        ), mock.patch(
            "cephci.orchestration.results.email_results"
        ) as email:
            rc = runner.run(args)

        assert rc == 1
        payload = email.call_args.kwargs["test_result"]
        assert payload["info"]["status"] == "Fail"
        assert "cluster_info" not in payload
        assert [record["status"] for record in payload["result"]] == [
            "Not Executed",
            "Not Executed",
        ]

    def test_an_unknown_cloud_is_reported_as_a_failure(self, tmp_path):
        from docopt import docopt

        from cephci.orchestration.doc import doc

        argv = self.argv(tmp_path)
        argv[argv.index("baremetal")] = "gcp"
        args = docopt(doc, argv=argv)

        with mock.patch.object(
            runner, "load_file", return_value={"globals": []}
        ), mock.patch.object(
            runner.init_suite, "load_suites", return_value=self.SUITE
        ), mock.patch.object(
            runner.build, "resolve", return_value=self.artifacts()
        ), mock.patch(
            "cephci.orchestration.results.email_results"
        ):
            assert runner.run(args) == 1

    def test_the_summary_file_is_written(self, tmp_path):
        import glob
        import json

        module = mock.MagicMock()
        module.run.return_value = 0

        self.run(tmp_path, module)

        summaries = glob.glob(str(tmp_path / "**/run_summary.json"), recursive=True)
        assert summaries
        with open(summaries[0]) as handle:
            assert json.load(handle) == {}

    def test_diagnostics_are_only_gathered_after_a_failure(self, tmp_path):
        passing = mock.MagicMock()
        passing.run.return_value = 0
        self.run(tmp_path, passing)

        self.collector.coredumps.assert_not_called()
        self.collector.sosreports.assert_not_called()
        self.collector.ceph_logs.assert_not_called()

        failing = mock.MagicMock()
        failing.run.return_value = 1
        self.run(tmp_path, failing)

        self.collector.coredumps.assert_called_once()
        self.collector.sosreports.assert_called_once()
        self.collector.ceph_logs.assert_called_once()

    def test_the_email_always_claims_the_run_itself_succeeded(self, tmp_path):
        """The verdict comes from the records, not from this field."""
        module = mock.MagicMock()
        module.run.return_value = 1

        rc, email = self.run(tmp_path, module)

        assert rc == 1
        assert email.call_args.kwargs["test_result"]["info"] == {"status": "Pass"}
