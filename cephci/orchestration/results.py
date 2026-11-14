"""Result records of a run and the payloads built from them.

Every test contributes one record. The same records feed the console summary, the
e-mail, the xunit file and Polarion, so they are created in one place with the run
level details already filled in.
"""

import datetime
import json
import os
import pickle
from typing import Dict, List, Optional

from cephci.orchestration.build import BuildArtifacts
from cephci.orchestration.config import RunConfig
from cephci.orchestration.context import RunContext
from utility.log import Log
from utility.utils import email_results
from utility.xunit import create_xunit_results

log = Log(__name__)

POLARION_WORKITEM_URL = (
    "https://polarion.engineering.redhat.com/polarion/#/project/CEPH/workitem?id="
)


def store_cluster_state(
    ceph_cluster_object: Dict, ceph_clusters_file_name: str
) -> None:
    """Serialise the environment so that a later run can reuse it.

    Args:
        ceph_cluster_object (dict): Cluster name to :class:`ceph.ceph.Ceph`.
        ceph_clusters_file_name (str): Destination of the pickle.

    Returns:
        None
    """
    with open(ceph_clusters_file_name, "w+b") as cluster_state:
        pickle.dump(ceph_cluster_object, cluster_state)

    log.info("ceph_clusters_file %s", ceph_clusters_file_name)


class RunResults:
    """Collects the outcome of a run and reports it."""

    def __init__(
        self,
        context: RunContext,
        config: RunConfig,
        artifacts: BuildArtifacts,
        distro: str,
    ) -> None:
        """Initialize the collection.

        Args:
            context (RunContext): Identity and timing of the run.
            config (RunConfig): The run settings.
            artifacts (BuildArtifacts): The build under test.
            distro (str): The operating system images in use.
        """
        self.context = context
        self.config = config
        self.artifacts = artifacts
        self.distro = distro
        self.records: List[Dict] = []
        self.clusters_used: List[str] = []

    @property
    def suite_name(self) -> str:
        """The suites of this run as a single identifier."""
        return "::".join(self.config.suites)

    def record(self, test: Dict) -> Dict:
        """Create the result record of a test.

        Args:
            test (dict): The ``test`` block of a suite entry.

        Returns:
            dict: The record, with the run level details filled in.
        """
        polarion_id = test.get("polarion-id")

        return {
            "docker-containers-list": [],
            "name": test.get("name"),
            "desc": test.get("desc"),
            "file": test.get("module"),
            "cli_arguments": self.context.cli_arguments,
            "polarion-id": polarion_id,
            "polarion-id-link": f"{POLARION_WORKITEM_URL}{polarion_id}",
            "testrail-id": test.get("testrail-id"),
            "rhbuild": self.config.rhbuild,
            "cloud-type": self.config.cloud_type,
            "ceph-version": self.artifacts.ceph_version,
            "platform": self.config.platform,
            "product": self.config.product,
            "release": self.config.release,
            "distro": self.distro,
            "suite-name": self.suite_name,
            "suite-file": self.config.suites,
            "conf-file": self.config.cluster_conf_file,
            # Retained for the consumers of the payload, it has no source today.
            "ceph-version-name": None,
            "duration": "0s",
            "status": "Not Executed",
            "comments": test.get("comments", str()),
            "jenkin-url": self.context.jenkins_url,
            "instance-name": self.config.instances_name,
            "invoked-by": self.context.trigger_user,
        }

    def record_all(self, tests: List[Dict]) -> List[Dict]:
        """Create the records of every test of a suite that did not run.

        Args:
            tests (list): The suite entries.

        Returns:
            list: One record per test.
        """
        return [self.record(test.get("test")) for test in tests]

    def add(self, record: Dict) -> None:
        """Append a completed record.

        Args:
            record (dict): The record of a test.

        Returns:
            None
        """
        self.records.append(record)

    def extend(self, records: List[Dict]) -> None:
        """Append the records of tests that ran in parallel.

        Args:
            records (list): The records to add.

        Returns:
            None
        """
        self.records.extend(records)

    def note_clusters(self, cluster_names: List[str]) -> None:
        """Remember which clusters took part in the run.

        Args:
            cluster_names (list): Names of the clusters used by a test.

        Returns:
            None
        """
        for name in cluster_names:
            if name not in self.clusters_used:
                self.clusters_used.append(name)

    def metadata(self) -> Dict:
        """Describe the run for the xunit writer.

        Returns:
            dict: The run level details.
        """
        return {
            "jenkin-url": self.context.jenkins_url,
            "build": self.config.rhbuild,
            "ceph-version": self.artifacts.ceph_version,
            "base_url": self.artifacts.base_url,
            "suite-name": self.suite_name,
            "conf-file": self.config.cluster_conf_file,
            "polarion-project-id": "CEPH",
            "distro": self.distro,
            "container-registry": self.artifacts.docker_registry,
            "container-image": self.artifacts.docker_image,
            "container-tag": self.artifacts.docker_tag,
            "log-dir": self.context.run_dir,
            "run-id": self.context.run_id,
            "cloud-type": self.config.cloud_type,
            "instance-name": self.config.instances_name,
            "invoked-by": self.context.trigger_user,
        }

    def email(
        self,
        info: Dict,
        records: Optional[List[Dict]] = None,
        with_clusters: bool = True,
    ) -> None:
        """Send the result e-mail.

        Args:
            info (dict): Overall status of the run, and a trace when it failed
                before the tests could run.
            records (list): Records to report, defaulting to the collected ones.
            with_clusters (bool): Whether to report the clusters that took part. A
                run that failed to provision has none.

        Returns:
            None
        """
        payload = {
            "result": self.records if records is None else records,
            "run_id": self.context.run_id,
            "trigger_user": self.context.trigger_user,
            "run_directory": self.context.run_dir,
            "total_time": self.context.elapsed(),
            "info": info,
            "send_to_cephci": self.config.post_results,
            "prefix": self.config.instances_name,
        }

        if with_clusters:
            payload["cluster_info"] = self.clusters_used

        email_results(test_result=payload)

    def xunit(self) -> None:
        """Write the xunit result file when it was requested.

        Returns:
            None
        """
        if not self.config.xunit_results:
            return

        create_xunit_results(self.suite_name, self.records, self.metadata())

    def write_summary(self) -> None:
        """Write the run summary file collected by the pipelines.

        Returns:
            None
        """
        summary_file = os.path.join(self.context.run_dir, "run_summary.json")
        with open(summary_file, "w", encoding="utf-8") as summary:
            json.dump(self.context.summary, summary, ensure_ascii=False, indent=4)


def status_of(rc: int) -> str:
    """Translate the return code of a test module into a status.

    Args:
        rc (int): The value returned by the test module.

    Returns:
        str: One of Pass, Skipped or Failed.
    """
    if rc == 0:
        return "Pass"

    return "Skipped" if rc == -1 else "Failed"


def elapsed_since(start: datetime.datetime) -> datetime.timedelta:
    """Return the time passed since the given moment.

    Args:
        start (datetime): When the measurement started.

    Returns:
        timedelta: The duration.
    """
    return datetime.datetime.now() - start
