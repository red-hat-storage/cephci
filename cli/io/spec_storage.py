import csv
import os
import xml.etree.ElementTree as ET

from cli import Cli
from cli.utilities.packages import Package


class SpecStorageError(Exception):
    pass


class SpecStorage(Cli):
    def __init__(self, primary_client):
        super(SpecStorage, self).__init__(primary_client)
        self.primary_client = primary_client
        self.packages = ["matplotlib", "PyYAML", "pylibyaml"]
        self.config = "sfs_rc"
        self.install_dest = "/root/specStorage/"
        self.base_cmd = f"python3 {self.install_dest}SPECstorage2020/SM2020"
        self.outputlog = "result"
        self.benchmark_file = "storage2020.yml"
        self.install_loc = "http://magna002.ceph.redhat.com/spec_storage/"

    def install_spec_storage(self):
        """
        Install SPECstorage
        """
        try:
            # Install prerequiste packages
            # Package(self.primary_client).install("sshpass", nogpgcheck=True)
            # for package in self.packages:
            #     Package(self.primary_client).pip_install(package)
            cmd = "dnf install -y sshpass"
            self.execute(sudo=True, cmd=cmd)

            # Add SpecStorage tool
            self.execute(sudo=True, cmd=f"mkdir -p {self.install_dest}")
            cmd = (
                f"wget {self.install_loc}SPECstorage2020-2529.tgz "
                f"-O {self.install_dest}/SPECstorage2020-2529.tgz"
            )
            self.execute(sudo=True, cmd=cmd)
            cmd = (
                f"wget {self.install_loc}SPECstorage_clients.sh "
                f"-O {self.install_dest}/SPECstorage_clients.sh"
            )
            self.execute(sudo=True, cmd=cmd)
            cmd = f"tar zxvf {self.install_dest}/SPECstorage2020-2529.tgz -C {self.install_dest}"
            self.execute(sudo=True, cmd=cmd)

            # Run SPECstorage clients script
            cmd = f". {self.install_dest}/SPECstorage_clients.sh"
            self.execute(sudo=True, cmd=cmd)
        except Exception:
            raise SpecStorageError("SPECstorage installation failed")

    def update_config(
        self,
        benchmark,
        load,
        incr_load,
        num_runs,
        clients,
        nfs_mount,
        benchmark_defination,
    ):
        """
        Update SPECstorage configuration file
        Args:
            benchmark (str): Benchmark example: SWBUILD, VDA, EDA, AI_IMAGE, GENOMICS
            load (str): Starting load value
            incr_load (str): Incremental increase value in load for successive data points in a run
            num_runs (str): The number of load points to run
            clients (str): All Clients
            nfs_mount (str): Clients mount points
            benchmark_defination (dir) : benchmark defination parameters with values
        """
        try:
            # Add config file "sfc_rc"
            cmd = f"wget  {self.install_loc}/{self.config} -O {self.install_dest}/{self.config}"
            self.execute(sudo=True, cmd=cmd)
            # Update installer Location
            cmd = (
                f"sed -i '/EXEC_PATH=/d' {self.install_dest}/{self.config} && "
                f"echo EXEC_PATH={self.install_dest}/SPECstorage2020/binaries/linux/x86_64/netmist >> "
                f"{self.install_dest}/{self.config}"
            )
            self.execute(sudo=True, cmd=cmd)

            # Update clients with mount point
            client_mountpoints = "CLIENT_MOUNTPOINTS="
            for client in clients:
                client_mountpoints += f"{client.ip_address}:{nfs_mount} "
            cmd = f"echo {client_mountpoints.rstrip()} >> {self.install_dest}/{self.config}"
            self.execute(sudo=True, cmd=cmd)

            # Update benchmark
            cmd = f"echo BENCHMARK={benchmark} >> {self.install_dest}/{self.config}"
            self.execute(sudo=True, cmd=cmd)

            # Update load
            cmd = f"echo LOAD={load} >> {self.install_dest}/{self.config}"
            self.execute(sudo=True, cmd=cmd)

            # Update incr_load
            cmd = f"echo INCR_LOAD={incr_load} >> {self.install_dest}/{self.config}"
            self.execute(sudo=True, cmd=cmd)

            # Update num_runs
            cmd = f"echo NUM_RUNS={num_runs} >> {self.install_dest}/{self.config}"
            self.execute(sudo=True, cmd=cmd)

            # Update benchmark defination
            for parameter, value in benchmark_defination.items():
                cmd = (
                    f"sed -i '/Benchmark_name:/,/{parameter}:/ s/{parameter}:.*/{parameter}: {value}/'"
                    f" {self.install_dest}/{self.benchmark_file}"
                )
                self.execute(sudo=True, cmd=cmd)
        except Exception:
            raise SpecStorageError("SPECstorage Configuration failed")

    def run_spec_storage(
        self,
        benchmark,
        load,
        incr_load,
        num_runs,
        clients,
        nfs_mount,
        benchmark_defination,
    ):
        """
        Run SPECstorage
        Args:
            benchmark (str): Benchmark example: SWBUILD, VDA, EDA, AI_IMAGE, GENOMICS
            load (str): Starting load value
            incr_load (str): Incremental increase value in load for successive data points in a run
            num_runs (str): The number of load points to run
            clients (str): All Clients
            nfs_mount (str): Clients mount points
        """
        # Install SPECstorage
        self.install_spec_storage()

        # Update SPECstorage configuration
        self.update_config(
            benchmark,
            load,
            incr_load,
            num_runs,
            clients,
            nfs_mount,
            benchmark_defination,
        )
        nfs_mount = nfs_mount.rstrip("/")
        last_path_component = os.path.basename(nfs_mount)
        # Run SPECstorage
        cmd = (
            f"{self.base_cmd} -b {self.install_dest}/SPECstorage2020/{self.benchmark_file}"
            f" -r {self.install_dest}/{self.config}"
            f" -s {benchmark}-{self.outputlog}-{last_path_component}"
        )
        print(cmd)
        return self.execute(sudo=True, long_running=True, cmd=cmd, timeout=5400)

    def append_to_csv(self, output_file, metrics):
        fieldnames = list(metrics.keys())
        file_exists = os.path.isfile(output_file)

        with open(output_file, "a", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(metrics)

    def extract_metrics(self, remote_file, **kwargs):
        # Create an ElementTree object from the remote file
        tree = ET.parse(remote_file)
        root = tree.getroot()

        metrics = kwargs
        business_metric = root.find(".//business_metric").text
        metrics["business_metric"] = business_metric
        benchmark = root.find(".//benchmark").attrib["name"]
        metrics["benchmark"] = benchmark
        for metric in root.findall(".//metric"):
            name = metric.attrib["name"]
            value = metric.text
            metrics[name] = value
        return metrics

    def parse_spectorage_results(self, results_dir, output_file, **kwargs):
        dir_items = self.primary_client.get_dir_list(results_dir, sudo=True)
        for item in dir_items:
            if item.endswith(".xml") and "_parsed" not in item:
                remote_file_xml = self.primary_client.remote_file(
                    file_name=f"{results_dir}/{item}", file_mode="r", sudo=True
                )
                metrics = self.extract_metrics(remote_file_xml, **kwargs)
                self.append_to_csv(output_file, metrics)
                # Rename the processed XML file
                new_name = item.replace(".xml", "_parsed.xml")
                self.primary_client.exec_command(
                    cmd=f"mv {results_dir}/{item} {results_dir}/{new_name}", sudo=True
                )
