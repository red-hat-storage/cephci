from cli import Cli
from cli.utilities.packages import Package


class SpecStorageError(Exception):
    pass


class SpecStorage(Cli):
    def __init__(self, primary_client):
        super(SpecStorage, self).__init__(primary_client)
        self.base_cmd = "python3 /root/SPECstorage2020/SM2020"
        self.packages = ["matplotlib", "PyYAML", "pylibyaml"]
        self.config = "sfs_rc"
        self.install_dest = "/root/SPECstorage2020"
        self.outputlog = "result"
        self.benchmark_file = "storage2020.yml"

    def install_spec_storage(self):
        """
        Install SPECstorage
        """
        try:
            # Install prerequiste packages
            Package(self.primary_client).install("sshpass", nogpgcheck=True)
            for package in self.packages:
                Package(self.primary_client).pip_install(package)

            # Add SpecStorage tool
            cmd = "mount -t nfs -o sec=sys,nfsvers=4.1 reesi004.ceph.redhat.com:/ /tmp"
            self.execute(sudo=True, cmd=cmd)
            cmd = "tar zxvf /tmp/spec_storage/SPECstorage2020-2529.tgz -C /root"
            self.execute(sudo=True, cmd=cmd)

            # Run SPECstorage clients script
            cmd = ". /tmp/spec_storage/SPECstorage_clients.sh"
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
            cmd = f"cp /tmp/spec_storage/{self.config} {self.install_dest}"
            self.execute(sudo=True, cmd=cmd)

            # Update clients with mount point
            client_mountpoints = "CLIENT_MOUNTPOINTS="
            for client in clients:
                client_mountpoints += f"{client.hostname}:{nfs_mount} "
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

        # Run SPECstorage
        cmd = (
            f"{self.base_cmd} -b {self.install_dest}/{self.benchmark_file}"
            f" -r {self.install_dest}/{self.config}"
            f" -s {benchmark}-{self.outputlog}"
        )
        return self.execute(sudo=True, long_running=True, cmd=cmd)
