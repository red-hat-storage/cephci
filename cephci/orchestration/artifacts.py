"""Collection of the diagnostics a failed run leaves on the cluster nodes."""

from typing import Dict

from cephci.cluster_info import collect_ceph_coredumps, get_ceph_var_logs
from utility import sosreport
from utility.log import Log
from utility.utils import setup_cluster_access

log = Log(__name__)


class ArtifactCollector:
    """Gathers coredumps, sosreports and Ceph logs from the environment."""

    def __init__(self, clusters: Dict, run_dir: str, url_base: str) -> None:
        """Initialize the collector.

        Args:
            clusters (dict): Cluster name to :class:`ceph.ceph.Ceph`.
            run_dir (str): Directory the artifacts are written to.
            url_base (str): Location the artifacts can be read from.
        """
        self.clusters = clusters
        self.run_dir = run_dir
        self.url_base = url_base

    def coredumps(self) -> None:
        """Preserve the coredumps of every cluster.

        Returns:
            None
        """
        log.info(
            "\n\nPreserving core-dump directory due to failures in testcase or user instructed"
        )
        for cluster in self.clusters.values():
            collect_ceph_coredumps(cluster, self.run_dir)

        log.info(f"Generated coredump location : {self.url_base}/ceph_coredumps\n")

    def sosreports(self) -> None:
        """Generate a sosreport on the installer of every cluster.

        Returns:
            None
        """
        log.info(
            "\n\nGenerating sosreports for all the nodes due to failures in testcase"
        )
        for name, cluster in self.clusters.items():
            log.info(f"Installing Ceph-common on {name} nodes to gather Sos report")

            for node in cluster.get_nodes():
                setup_cluster_access(cluster, node)

            installer = cluster.get_nodes(role="installer")[0]
            sosreport.run(
                installer.ip_address,
                installer.username,
                installer.password or "cephuser",
                self.run_dir,
            )

        log.info(f"Generated sosreports location : {self.url_base}/sosreports\n")

    def ceph_logs(self) -> None:
        """Copy the Ceph logs of every cluster.

        Returns:
            None
        """
        log.info(
            "\n\nCopying Ceph cluster logs due to failures in testcase or user instructed"
        )
        for cluster in self.clusters.values():
            get_ceph_var_logs(cluster, self.run_dir)

        log.info(f"Generated cluster log location : {self.url_base}/ceph_logs\n")
