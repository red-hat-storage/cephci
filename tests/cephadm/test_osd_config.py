import os
import re

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import CephadmOpsExecutionError, MonDaemonError
from utility.log import Log

log = Log(__name__)

CEPH_LIB_DIR = "/var/lib/ceph"


def run(ceph_cluster, **kw):
    """
    Check the Number of Mon nodes in OSD config file
    """
    installer = ceph_cluster.get_ceph_object("installer")
    osd_nodes = ceph_cluster.get_nodes(role="osd")

    # Get cluster FSID
    fsid = CephAdm(installer).ceph.fsid()
    if not fsid:
        raise CephadmOpsExecutionError("Failed to get cluster FSID")

    # Check the mons in each OSD node
    for onode in osd_nodes:
        # Get the osd directory path
        dir_path = os.path.join(CEPH_LIB_DIR, fsid)
        dir_out = onode.get_dir_list(sudo=True, dir_path=dir_path)

        # Get the list of all the osd directories
        osd_dirs = [f for f in dir_out if f.startswith("osd")]

        # Read config file from each osd directory
        for osd_dir in osd_dirs:
            config_file = os.path.join(dir_path, osd_dir, "config")
            output = onode.remote_file(sudo=True, file_name=config_file, file_mode="r")
            # Get the number of mons in each config file
            for line in output.readlines():
                if line.strip().startswith("mon_host"):
                    mon_hosts = re.findall(r"mon_host\s*=\s*(.*)", line)
                    if not mon_hosts:
                        raise MonDaemonError("No mon_host found in the config file")

                    mon_hosts = mon_hosts[0].strip("[]").split()
                    log.info(f"Number of mons in the config file: {mon_hosts}")
                    if len(mon_hosts) != 3:
                        raise MonDaemonError(
                            "Unexpected number of mon hosts in config file"
                        )
    return 0
