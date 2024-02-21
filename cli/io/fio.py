import itertools

from cli import Cli


class Fio(Cli):
    def __init__(self, client):
        super(Fio, self).__init__(client)
        self.client = client
        self.ioengine = None
        self.mount_dir = None
        self.workloads = None
        self.sizes = None
        self.iodepth_values = None
        self.numjobs = None

    def create_config(self):
        """
        Generates all the combimations of workloads sizes, iodepth as per the fio_config
        """

        configs = []
        filenames = []
        for combo in itertools.product(self.workloads, self.sizes, self.iodepth_values):
            workload, size, iodepth = combo
            filename = f"file_{workload}_{size}_{iodepth}.fio"
            configs.append(
                (
                    filename,
                    self._generate_fio_config(workload, size, iodepth),
                )
            )
        for filename, config in configs:
            if self.ctx.role == "windows_client":
                remote_file = self.ctx.remote_file(file_name=filename, file_mode="w")
            else:
                remote_file = self.ctx.remote_file(
                    sudo=True, file_name=filename, file_mode="w"
                )
            remote_file.write(config)
            remote_file.flush()
            filenames.append(filename)
        return filenames

    def _generate_fio_config(self, workload, size, iodepth):
        """
        Generates the config required for FIO tool
        Args:
            workload (str): Workload name
            size (str): size of workload
            iodepth (str): IO depth
        """
        config = f"""
[global]
ioengine={self.ioengine}
direct=1
time_based
runtime=60
size={size}

[{workload}]
rw={workload}
directory={self.mount_dir}
bs= 4k
numjobs={int(self.numjobs)}
iodepth= {iodepth}
"""
        return config

    def run(self, mount_dir, ioengine, workloads, sizes, iodepth_values, numjobs):
        """
        Run Fio
        Args:
            ioengine (str): Io engine name
            mount_dir (str): Mount directory
            workloads (str): work load
            sizes (str): size
            iodepth_values (str): Depth of IO
            numjobs (str): Number of jobs
        """
        self.ioengine = ioengine
        self.mount_dir = mount_dir
        self.workloads = workloads
        self.sizes = sizes
        self.iodepth_values = iodepth_values
        self.numjobs = numjobs
        # Update Fio configuration
        filenames = self.create_config()

        # Run Fio
        for file in filenames:
            if self.ctx.role == "windows_client":
                cmd = (
                    f"fio {file} --output-format=json "
                    f"--output={file}_windows_{mount_dir.replace('/', '')}.json"
                )
                self.execute(sudo=True, long_running=True, cmd=cmd)
            else:
                cmd = (
                    f"fio {file} --output-format=json "
                    f"--output={file}_{self.ctx.hostname}_{mount_dir.replace('/', '')}.json"
                )
                self.execute(sudo=True, long_running=True, cmd=cmd)

        return True
