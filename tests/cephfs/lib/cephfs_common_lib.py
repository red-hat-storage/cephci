"""
This is cephfs utilsV1 extension to include further common reusable methods for FS regression testing

"""

import datetime
import time

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


class CephFSCommonUtils(FsUtils):
    def __init__(self, ceph_cluster):
        """
        FS Utility V2 object
        Args:
            ceph_cluster (ceph.ceph.Ceph): ceph cluster
        """
        self.ceph_cluster = ceph_cluster
        super().__init__(ceph_cluster)

    def wait_for_healthy_ceph(self, client, wait_time):
        """
        This method will run ceph status and if its not HEALTH_OK, will wait for wait_time for it to be healthy
        Args:
        Required:
        Client : Client object to run command
        wait_time : Time to wait for HEALTH_OK, in seconds

        Returns 0 if healthy, 1 if unhealthy even after wait_time
        """
        ceph_healthy = 0
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=wait_time)
        while ceph_healthy == 0 and (datetime.datetime.now() < end_time):
            try:
                self.get_ceph_health_status(client)
                ceph_healthy = 1
            except Exception as ex:
                log.info(ex)
                out, rc = client.exec_command(sudo=True, cmd="ceph health detail")
                if "experiencing slow operations in BlueStore" in str(out):
                    log.info("Ignoring the known warning for Bluestore Slow ops")
                    ceph_healthy = 1
                else:
                    log.info(
                        "Wait for sometime to check if Cluster health can be OK, current state : %s",
                        ex,
                    )
                    time.sleep(5)

        if ceph_healthy == 0:
            client.exec_command(
                sudo=True,
                cmd="ceph fs status;ceph -s;ceph health detail",
            )
            return 1
        return 0
