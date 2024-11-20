import copy
from logging import getLogger

import yaml

from cli import Cli
from utility.utils import get_ceph_version_from_cluster

log = getLogger(__name__)


class CbtError(Exception):
    pass


class CBT(Cli):
    """
    CBT class inherits from Cli and initializes connection parameters for a specific server and table.

    Attributes:
        primary_client (object): The primary client instance.
        archive_path (str): path for storing the results.
                            Please provide the path where user has permissions to create directories
    """

    def __init__(self, primary_client, archive_path="~/cephCI/cbt/"):
        """
        Initialize the CBT class.

        """
        super(CBT, self).__init__(primary_client)
        self.primary_client = primary_client
        self.archive_path = archive_path
        self.setup_CBT()

    def setup_CBT(self):
        """
        Set up the CBT environment on the primary client.

        This includes enabling EPEL repository, uploading setup script,
        executing the setup script, and retrieving the Ceph version.
        """
        self.enable_epel()
        self.primary_client.upload_file(
            src="cli/io/cbt/setup_cbt.sh", dst="setup_cbt.sh"
        )
        self.execute(cmd="bash setup_cbt.sh")
        self.ceph_version = get_ceph_version_from_cluster(self.primary_client)

    def collect_cluster_conf(self, mon_node, mon_ip, osd_nodes):
        """
        Collect and store the cluster configuration details.

        Args:
            mon_node (str): The monitor node hostname.
            mon_ip (str): The monitor node IP address.
            osd_nodes (list): List of OSD nodes.
        """
        self.placeholders = {
            "head_placeholder": f"{self.primary_client.node.hostname}",
            "client_placeholder": f"{self.primary_client.node.hostname}",
            "osd_nodes": f"{osd_nodes}",
            "mon_placeholder": f"{mon_node}",
            "ip_placeholder": f"{mon_ip}",
            "port_placeholder": "6789",
        }
        log.info(self.placeholders)
        # Temporary assignment of mons
        self.placeholders["mon_list"] = "{mon_list}"

    def prepare_cbt_conf(self):
        """
        Prepare the CBT configuration file.

        Reads the template configuration file, formats it with placeholders,
        and updates it with the monitor node list.
        """
        with open("cli/io/cbt/conf.yaml", "r") as cbt_conf:
            self.cbt_conf = cbt_conf.read()
        cbt_conf_content = self.cbt_conf.format(**self.placeholders)
        self.conf_yaml = yaml.safe_load(cbt_conf_content)
        mon_list = {
            f"{self.placeholders['mon_placeholder']}": {
                "a": f"{self.placeholders['ip_placeholder']}:{self.placeholders['port_placeholder']}"
            }
        }
        self.conf_yaml["cluster"]["mons"] = mon_list

    def execute_cbt(self, benchmarks, benchmark_name):
        """
        Execute the CBT benchmark.

        Args:
            benchmarks (dict): Benchmark configurations.
            benchmark_name (str): Name of the benchmark to execute.
        """
        conf_yaml_copy = copy.deepcopy(self.conf_yaml)
        conf_yaml_copy.update(benchmarks)
        self.benchmark_name = benchmark_name
        log.info(f"{conf_yaml_copy}")
        with open(f"{benchmark_name}.yaml", "w") as yaml_file:
            yaml.dump(conf_yaml_copy, yaml_file, default_flow_style=False)
        self.primary_client.upload_file(
            src=f"{benchmark_name}.yaml", dst=f"{benchmark_name}.yaml"
        )
        self.execute(
            cmd=f".venv/bin/python cbt/cbt.py --archive={self.archive_path}{self.benchmark_name} "
            f"--conf=/etc/ceph/ceph.conf {benchmark_name}.yaml",
            long_running=True,
        )

    def enable_epel(self):
        """
        Enable the Extra Packages for Enterprise Linux (EPEL) repository.

        Determines the appropriate EPEL repository URL based on the primary client's OS version
        and installs the EPEL repository.
        """
        EPEL_REPOS = {
            "8": "https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm",
            "9": "https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm",
        }

        # Epel Repo
        self.execute(
            sudo=True,
            cmd=f"dnf install {EPEL_REPOS[self.primary_client.distro_info['VERSION_ID'][0]]} -y",
            check_ec=False,
        )
