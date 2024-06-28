import copy
import json
import re
from pathlib import Path

import requests
import yaml

from cli import Cli
from utility.log import Log

required_packages = [
    "blktrace",
    "seekwatcher",
    "perf",
    "valgrind",
    "fio",
    "cosbench",
    "pdsh",
]
log = Log(__name__)


class CbtError(Exception):
    pass


class CBT(Cli):
    def __init__(self, primary_client):
        super(CBT, self).__init__(primary_client)
        self.primary_client = primary_client

        self.required_packages = [
            "blktrace",
            "seekwatcher",
            "perf",
            "valgrind",
            "fio",
            "cosbench",
            "pdsh",
            "pdsh-rcmd-ssh",
        ]

    def setup_CBT(self):
        self.execute(cmd="git clone https://github.com/AmarnatReddy/cbt.git")
        self.execute(sudo=True, cmd="yum install ceph-common -y")
        self.enable_epel()
        for package in self.required_packages:
            self.execute(sudo=True, cmd=f"yum install {package} -y")
        self.primary_client.upload_file(
            src="cli/io/cbt/setup_venv.sh", dst="setup_venv.sh"
        )
        out, rc = self.execute(cmd="bash setup_venv.sh")
        log.info(out)

    def collect_cluster_conf(self, mon_node, mon_ip, osd_nodes):
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
        with open("cli/io/cbt/cbt_conf.yaml", "r") as cbt_conf:
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
        conf_yaml_copy = copy.deepcopy(self.conf_yaml)
        benchmarks_dict = yaml.safe_load(benchmarks)
        conf_yaml_copy.update(benchmarks_dict)
        self.benchmark_name = benchmark_name
        log.info(f"{conf_yaml_copy}")
        with open(f"{benchmark_name}.yaml", "w") as yaml_file:
            yaml.dump(conf_yaml_copy, yaml_file, default_flow_style=False)
        self.primary_client.upload_file(
            src=f"{benchmark_name}.yaml", dst=f"{benchmark_name}.yaml"
        )
        self.execute(
            cmd=f".venv/bin/python cbt/cbt.py --archive=/tmp/{self.benchmark_name} "
            f"--conf=/etc/ceph/ceph.conf {benchmark_name}.yaml",
        )

    def collect_results(self, ctx, config):
        log.info("Collecting CBT performance data config")

        tasks = ctx.get("config", {}).get("tasks", None)
        for task in tasks:
            if "cbt" in task:
                benchmark = task["cbt"]
                break

        cbt_results_arry = self.read_results()
        for cbt_results in cbt_results_arry:
            cbt_results = json.loads(json.dumps(cbt_results))
            if cbt_results:
                data = {
                    "job_id": ctx.get("config", {}).get("job_id", None),
                    "started_at": ctx.get("config", {}).get("timestamp", None),
                    "benchmark_mode": cbt_results.get("Benchmark_mode", None),
                    "seq": cbt_results.get("seq", None),
                    "total_cpu_cycles": cbt_results.get("total_cpu_cycles", None),
                    "branch": ctx.get("config", {}).get("branch", None),
                    "sha1": ctx.get("config", {}).get("sha1", None),
                    "os_type": ctx.get("config", {}).get("os_type", None),
                    "os_version": ctx.get("config", {}).get("os_version", None),
                    "machine_type": ctx.get("config", {}).get("machine_type", None),
                    "benchmark": benchmark["benchmarks"],
                    "results": cbt_results.get("results", None),
                }
                response = requests.post(
                    self.endpoint_url, json=data, headers=self.headers, auth=self.auth
                )
                if response.status_code == 201:
                    log.info("Data inserted successfully.")
                else:
                    log.info(f"Error inserting data: {response}")

    def read_results(self, json_output_paths):
        """
        json_output_paths = "/tmp/test_cbt_3/results/00000000/id-01984f6e/"
                            "json_output.0.ceph-amk-cbt-c9dbme-node1-installer"
        """
        results = []
        for json_output_path in json_output_paths:
            if json_output_path:
                path_full = Path(json_output_path)
                match = re.search(r"/json_output\.(?P<json>\d+)", json_output_path)
                if match:
                    Benchmark_mode = (
                        path_full.parent.name
                        if path_full.parent.name in ["rand", "write", "seq"]
                        else "fio"
                    )
                    seq = match.group("json")

                    results.append(
                        {
                            "results": self.read_json_file(json_output_path),
                            "Benchmark_mode": Benchmark_mode,
                            "seq": seq,
                        }
                    )
        return results

    def enable_epel(self):
        EPEL_REPOS = {
            "7": "https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm",
            "8": "https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm",
            "9": "https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm",
        }

        # Epel Repo
        self.execute(
            sudo=True,
            cmd=f"dnf install {EPEL_REPOS[self.primary_client.distro_info['VERSION_ID'][0]]} -y",
            check_ec=False,
        )
