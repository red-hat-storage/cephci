"""Remove NFS Multi-Active clusters."""

from ceph.waiter import WaitUntil
from tests.nfs.lib.multi_active.constants import log_section
from utility.log import Log

log = Log(__name__)


class NfsMultiActiveCleanup:
    """Remove the NFS cluster."""

    def __init__(self, ceph_cluster):
        self.ceph_cluster = ceph_cluster

    @staticmethod
    def _wait_cluster_teardown(client, nfs_name, timeout=300):
        """Wait until nfs cluster ls, orch ps, and orch ls are clear for ``nfs_name``."""
        services = (f"nfs.{nfs_name}", f"ingress.nfs.{nfs_name}")
        for _ in WaitUntil(timeout=timeout, interval=5):
            ls, _ = client.exec_command(
                sudo=True, cmd="ceph nfs cluster ls", check_ec=False
            )
            if nfs_name in (ls or ""):
                continue
            drained = True
            for svc in services:
                out, _ = client.exec_command(
                    sudo=True,
                    cmd=f"ceph orch ps --service_name {svc}",
                    check_ec=False,
                )
                if "No daemons reported" not in (out or ""):
                    drained = False
                    break
                out, _ = client.exec_command(
                    sudo=True,
                    cmd=f"ceph orch ls --service_name {svc}",
                    check_ec=False,
                )
                if "No services reported" not in (out or ""):
                    drained = False
                    break
            if drained:
                log.info("NFS cluster %r teardown complete", nfs_name)
                return
        log.warning(
            "Timed out after %ss waiting for NFS cluster %r to drain", timeout, nfs_name
        )

    def remove_cluster(self, nfs_name):
        clients = self.ceph_cluster.get_nodes("client")
        client = clients[0] if clients else self.ceph_cluster.get_nodes("installer")[0]

        log_section(log, "NFS MULTI-ACTIVE CLUSTER CLEANUP")
        log.info("Removing NFS cluster %s", nfs_name)
        try:
            client.exec_command(
                sudo=True,
                cmd=f"ceph nfs cluster rm {nfs_name}",
                check_ec=False,
            )
        except Exception as ex:
            log.warning("Failed to remove NFS cluster %s: %s", nfs_name, ex)
        else:
            self._wait_cluster_teardown(client, nfs_name)
