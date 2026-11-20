"""Result records of a run and the payloads built from them."""

import datetime
import json
import os
import pickle

import mock
import pytest

from cephci.orchestration.build import BuildArtifacts
from cephci.orchestration.config import RunConfig
from cephci.orchestration.context import RunContext
from cephci.orchestration.results import (
    RunResults,
    status_of,
    store_cluster_state,
)


@pytest.fixture
def results(tmp_path):
    manifest = mock.MagicMock()
    manifest.product = "ibm"
    manifest.platform = "rhel-9"
    manifest.release = "8.1"

    artifacts = BuildArtifacts(
        manifest=manifest,
        build="nightly",
        base_url="http://example.com/compose",
        docker_registry="registry.example.com",
        docker_image="rhceph/rhceph-8-rhel9",
        docker_tag="latest",
        ceph_version="19.2.0-1",
    )
    config = RunConfig(
        suites=["suites/squid/cephadm/tier0-cephadm.yaml"],
        cluster_conf_file="conf/squid/cephadm/sanity.yaml",
        inventory_file=None,
        osp_cred_file=None,
        product="ibm",
        release="8.1",
        platform="rhel-9",
        rhbuild="8.1",
        instances_name="ci-abc12",
        cloud_type="openstack",
    )
    context = RunContext("abc123", str(tmp_path))

    return RunResults(context, config, artifacts, "RHEL-9.4")


TEST_ENTRY = {
    "name": "Bootstrap cluster",
    "desc": "Bootstrap with the default options",
    "module": "test_bootstrap.py",
    "polarion-id": "CEPH-83573719",
}


@pytest.mark.parametrize(
    "rc, expected", [(0, "Pass"), (-1, "Skipped"), (1, "Failed"), (2, "Failed")]
)
def test_status_of(rc, expected):
    assert status_of(rc) == expected


def test_record_carries_the_run_details(results):
    record = results.record(TEST_ENTRY)

    assert record["name"] == "Bootstrap cluster"
    assert record["file"] == "test_bootstrap.py"
    assert record["polarion-id"] == "CEPH-83573719"
    assert record["polarion-id-link"].endswith("CEPH-83573719")
    assert record["status"] == "Not Executed"
    assert record["duration"] == "0s"
    assert record["comments"] == ""
    assert record["rhbuild"] == "8.1"
    assert record["ceph-version"] == "19.2.0-1"
    assert record["platform"] == "rhel-9"
    assert record["product"] == "ibm"
    assert record["release"] == "8.1"
    assert record["distro"] == "RHEL-9.4"
    assert record["instance-name"] == "ci-abc12"
    assert record["conf-file"] == "conf/squid/cephadm/sanity.yaml"
    assert record["suite-name"] == "suites/squid/cephadm/tier0-cephadm.yaml"


def test_records_of_a_run_that_never_started(results):
    records = results.record_all([{"test": TEST_ENTRY}, {"test": TEST_ENTRY}])

    assert len(records) == 2
    assert all(record["status"] == "Not Executed" for record in records)


def test_clusters_are_recorded_once(results):
    results.note_clusters(["ceph", "ceph-sec"])
    results.note_clusters(["ceph"])

    assert results.clusters_used == ["ceph", "ceph-sec"]


def test_metadata_describes_the_run(results):
    metadata = results.metadata()

    assert metadata["build"] == "8.1"
    assert metadata["ceph-version"] == "19.2.0-1"
    assert metadata["base_url"] == "http://example.com/compose"
    assert metadata["container-registry"] == "registry.example.com"
    assert metadata["container-image"] == "rhceph/rhceph-8-rhel9"
    assert metadata["container-tag"] == "latest"
    assert metadata["polarion-project-id"] == "CEPH"
    assert metadata["run-id"] == "abc123"


def test_email_reports_the_collected_records(results):
    results.add(results.record(TEST_ENTRY))
    results.note_clusters(["ceph"])

    with mock.patch("cephci.orchestration.results.email_results") as email:
        results.email({"status": "Pass"})

    payload = email.call_args.kwargs["test_result"]
    assert payload["run_id"] == "abc123"
    assert payload["cluster_info"] == ["ceph"]
    assert len(payload["result"]) == 1
    assert set(payload) >= {
        "result",
        "run_id",
        "trigger_user",
        "run_directory",
        "total_time",
        "info",
        "send_to_cephci",
        "prefix",
    }


def test_a_run_that_failed_to_provision_reports_no_cluster(results):
    with mock.patch("cephci.orchestration.results.email_results") as email:
        results.email(
            {"status": "Fail", "trace": []},
            records=results.record_all([{"test": TEST_ENTRY}]),
            with_clusters=False,
        )

    payload = email.call_args.kwargs["test_result"]
    assert "cluster_info" not in payload
    assert len(payload["result"]) == 1


def test_xunit_is_only_written_when_requested(results):
    with mock.patch("cephci.orchestration.results.create_xunit_results") as xunit:
        results.xunit()
    xunit.assert_not_called()

    results.config = RunConfig(**{**results.config.__dict__, "xunit_results": True})
    with mock.patch("cephci.orchestration.results.create_xunit_results") as xunit:
        results.xunit()
    xunit.assert_called_once()


def test_summary_file_is_written(results):
    results.write_summary()

    summary = os.path.join(results.context.run_dir, "run_summary.json")
    with open(summary) as handle:
        assert json.load(handle) == {}


def test_cluster_state_round_trips(tmp_path):
    state_file = str(tmp_path / "ci-abc-123")
    store_cluster_state({"ceph": ["node1", "node2"]}, state_file)

    with open(state_file, "rb") as handle:
        assert pickle.load(handle) == {"ceph": ["node1", "node2"]}


def test_elapsed_is_reported_for_the_report(results):
    results.context.start_time = datetime.datetime.now() - datetime.timedelta(
        minutes=3, seconds=4
    )

    assert results.context.elapsed()["total"] == "3 mins, 4 secs"


def test_the_version_name_has_no_source(results):
    """The key is part of the payload but nothing has populated it for a long time."""
    assert results.record(TEST_ENTRY)["ceph-version-name"] is None
