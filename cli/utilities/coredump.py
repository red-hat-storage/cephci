import os

from cli import Cli
from cli.utilities.utils import (
    get_running_containers,
    podman_copy_to_localhost,
    podman_exec,
)
from utility.log import Log

log = Log(__name__)


CORE_PATTERN_PATH = "/proc/sys/kernel/core_pattern"
SYSTEMD_COREDUMP_PATTERN = "%P %u %g %s %t %c %h %e"
COREDUMP_LOGS_PATH = "/var/lib/systemd/coredump"

CEPH_CTR_NAME_EXPR = "name={}*"
CEPH_DAEMON_PROC_ID_CMD = "ps --no-headers -o pid -C ceph-{}"
INSTALL_COREDUMP_PKGS_CMD = "dnf install -y procps-ng gdb"
COREDUMP_CMD = "gcore {}"
COREDUMP_FILE_NAME = "core.{}"
TEST_COREDUMP_FILE_CMD = "test -f {}"


class Coredump(Cli):
    def __init__(self, nodes):
        super(Coredump, self).__init__(nodes)

    def set_systemd(self):
        """Set the core pattern to the systemd-coredump service."""

        cmd = f"echo '| /usr/lib/systemd/systemd-coredump {SYSTEMD_COREDUMP_PATTERN}' > {CORE_PATTERN_PATH}"
        self.execute(sudo=True, cmd=cmd)

    def get(self, logs_path, timeout=300, interval=10):
        """Get coredumps generated

        Args:
            logs_path (str): Log location to download coredumps
            timeout (int): timeout in sec
            interval (int): time interval to wait for each iteration
        """
        for node in self.ctx:
            download_path = os.path.join(logs_path, node.shortname)
            if not os.path.exists(download_path):
                os.mkdir(download_path)
                log.info(
                    f"Created local directory '{download_path}' to download coredumps"
                )

            coredumps = node.get_dir_list(COREDUMP_LOGS_PATH)
            log.info(f"Coredump files presents remotely -\n{coredumps}")
            for c in coredumps:
                node.download_file(
                    os.path.join(COREDUMP_LOGS_PATH, c), os.path.join(download_path, c)
                )
                log.info("Downloaded coredump file '{c}' at '{download_path}'")

    def capture(self, logs_path, daemon):
        """Generate coredumps manually and download

        Args:
            logs_path (str): Log location to download coredumps
            daemon (str): Ceph Monitor or OSD daemon
        """
        for node in self.ctx:
            download_path = os.path.join(logs_path, node.shortname)
            if not os.path.exists(download_path):
                os.mkdir(download_path)
                log.info(
                    f"Created local directory '{download_path}' to download coredumps"
                )

            ctr_id, _ = get_running_containers(
                node,
                expr=CEPH_CTR_NAME_EXPR.format(daemon),
                template="{{.ID}}",
                sudo=True,
            )
            if not ctr_id:
                log.error(
                    f"'{daemon}' daemon not present on node, continuing for next node"
                )
                continue
            log.info(f"Getting coredumps for container id '{ctr_id}'")

            podman_exec(node, INSTALL_COREDUMP_PKGS_CMD, ctr_id, sudo=True)

            proc_id = podman_exec(
                node, CEPH_DAEMON_PROC_ID_CMD.format(daemon), ctr_id, sudo=True
            )
            log.info(f"Getting coredumps for process id '{proc_id}'")

            podman_exec(node, COREDUMP_CMD.format(proc_id), ctr_id, sudo=True)
            coredump_file = COREDUMP_FILE_NAME.format(proc_id)
            status = podman_exec(
                node,
                TEST_COREDUMP_FILE_CMD.format(coredump_file),
                ctr_id,
                sudo=True,
                long_running=True,
            )
            if status:
                log.error(
                    f"Failed to generate coredumps for proc_id '{proc_id}' on container '{ctr_id}'"
                )

            coredump_localhost_path = os.path.join("/tmp", coredump_file)
            if podman_copy_to_localhost(
                node,
                ctr_id,
                coredump_file,
                coredump_localhost_path,
                sudo=True,
                long_running=True,
            ):
                log.error(f"Failed to copy '{coredump_file}' from container '{ctr_id}'")
            log.info(
                f"Generate coredumps for container '{ctr_id}' at '{coredump_localhost_path}'"
            )
            node.download_file(
                coredump_localhost_path, os.path.join(download_path, coredump_file)
            )
