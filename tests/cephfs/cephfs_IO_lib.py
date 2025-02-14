"""
This is cephfs IO utility module
It contains all the re-useable functions related to cephfs IO

"""

import datetime
import random
import string
import time

from ceph.parallel import parallel
from utility.log import Log

log = Log(__name__)


class FSIO(object):
    def __init__(self, ceph_cluster):
        """
        FS Utility object
        Args:
            ceph_cluster (ceph.ceph.Ceph): ceph cluster
        """
        self.result_vals = {}
        self.return_counts = {}
        self.ceph_cluster = ceph_cluster
        self.mons = ceph_cluster.get_ceph_objects("mon")
        self.mgrs = ceph_cluster.get_ceph_objects("mgr")
        self.osds = ceph_cluster.get_ceph_objects("osd")
        self.mdss = ceph_cluster.get_ceph_objects("mds")
        self.clients = ceph_cluster.get_ceph_objects("client")
        self.installer = ceph_cluster.get_nodes(role="installer")[0]

    def run_ios_V1(self, client, mounting_dir, io_tools=["dd", "smallfile"], **kwargs):
        """
        Run the IO module, one of IO modules dd,small_file,wget and file_extract or all for given duration
        Args:
            client: client node , type - client object
            mounting_dir: mount path in client to run IO, type - str
            io_tools : List of IO tools to run, default : dd,smallfile, type - list
            run_time : run duration in minutes till when the IO would run, default - 1min
            **kwargs:
                run_time : duration in mins for test execution time, default - 1, type - int
                smallfile_params : A dict type io_params data to define small file io_tool params as below,
                    smallfile_params = {
                    "testdir_prefix": "smallfile_io_dir",
                    "threads": 8,
                    "file-size": 10240,
                    "files": 10,
                    }
                dd_params : A dict type io_params data to define dd io_tool params as below,
                dd_params = {
                    "file_name": "dd_test_file",
                    "input_type": "random",
                    "bs": "10M",
                    "count": 10,
                    }
                NOTE: If default prams are being over-ridden,
                    smallfile params : Both file-size and files params SHOULD to be passed
                    dd_params : Both bs and count SHOULD be passed
                for a logically correct combination of params in IO.

                example on method usage: fs_util.run_ios_V1(test_params["client"],io_path,io_tools=["dd"],
                run_time=duration_min,dd_params=test_io_params)
                    test_io_params = {
                        "file_name": "test_file",
                        "input_type": "zero",
                        "bs": "1M",
                        "count": 100,
                    }
        Return: None

        """

        def smallfile():
            log.info("IO tool scheduled : SMALLFILE")
            io_params = {
                "testdir_prefix": "smallfile_io_dir",
                "threads": 8,
                "file-size": 10240,
                "files": 10,
            }
            if kwargs.get("smallfile_params"):
                smallfile_params = kwargs.get("smallfile_params")
                for io_param in io_params:
                    if smallfile_params.get(io_param):
                        io_params[io_param] = smallfile_params[io_param]
            dir_suffix = "".join(
                [
                    random.choice(string.ascii_lowercase + string.digits)
                    for _ in range(4)
                ]
            )
            io_path = f"{mounting_dir}/{io_params['testdir_prefix']}_{dir_suffix}"
            client.exec_command(sudo=True, cmd=f"mkdir {io_path}")
            client.exec_command(
                sudo=True,
                cmd=f"for i in create read append read delete create overwrite rename delete-renamed mkdir rmdir "
                f"create symlink stat chmod ls-l delete cleanup  ; "
                f"do python3 /home/cephuser/smallfile/smallfile_cli.py "
                f"--operation $i --threads {io_params['threads']} --file-size {io_params['file-size']} "
                f"--files {io_params['files']} --top {io_path} ; done",
                long_running=True,
            )

        def file_extract():
            log.info("IO tool scheduled : FILE_EXTRACT")
            io_params = {"testdir_prefix": "file_extract_dir", "compile_test": 0}
            if kwargs.get("file_extract_params"):
                file_extract_params = kwargs.get("file_extract_params")
                for io_param in io_params:
                    if file_extract_params.get(io_param):
                        io_params[io_param] = file_extract_params[io_param]

            dir_suffix = "".join(
                [random.choice(string.ascii_letters) for _ in range(2)]
            )
            io_path = f"{mounting_dir}/{io_params['testdir_prefix']}_{dir_suffix}"
            client.exec_command(sudo=True, cmd=f"mkdir {io_path}")
            client.exec_command(
                sudo=True,
                cmd=f"cd {io_path};wget -O linux.tar.xz http://download.ceph.com/qa/linux-6.5.11.tar.xz",
            )
            client.exec_command(
                sudo=True,
                cmd=f"cd {io_path};tar -xJf linux.tar.xz",
                long_running=True,
                timeout=3600,
            )
            log.info(f"untar suceeded on {mounting_dir}")
            if io_params["compile_test"] == 1:
                log.info("Install dependent packages")
                cmd = "yum install -y --nogpgcheck flex bison bc elfutils-libelf-devel openssl-devel"
                client.exec_command(
                    sudo=True,
                    cmd=cmd,
                )
                out, rc = client.exec_command(
                    sudo=True,
                    cmd="grep -c processor /proc/cpuinfo",
                )
                cpu_cnt = out.strip()
                log.info(f"cpu_cnt:{cpu_cnt},out:{out}")
                cmd = f"cd {io_path}/; cd linux-6.5.11;make defconfig;make -j {cpu_cnt}"
                client.exec_command(sudo=True, cmd=cmd, timeout=3600)
                log.info(f"Compile test passed on {mounting_dir}")

            # cleanup
            client.exec_command(
                sudo=True,
                cmd=f"rm -rf  {io_path}",
            )

        def wget():
            log.info("IO tool scheduled : WGET")
            io_params = {"testdir_prefix": "wget_dir"}
            if kwargs.get("wget_params"):
                wget_params = kwargs.get("wget_params")
                for io_param in io_params:
                    if wget_params.get(io_param):
                        io_params[io_param] = wget_params[io_param]
            dir_suffix = "".join(
                [random.choice(string.ascii_letters) for _ in range(2)]
            )
            io_path = f"{mounting_dir}/{io_params['testdir_prefix']}_{dir_suffix}"
            client.exec_command(sudo=True, cmd=f"mkdir {io_path}")
            client.exec_command(
                sudo=True,
                cmd=f"cd {io_path};wget -O linux.tar.gz http://download.ceph.com/qa/linux-5.4.tar.gz",
            )

        def dd():
            log.info("IO tool scheduled : DD")
            io_params = {
                "file_name": "dd_test_file",
                "input_type": "random",
                "bs": "10M",
                "count": 10,
            }
            if kwargs.get("dd_params"):
                dd_params = kwargs.get("dd_params")
                for io_param in io_params:
                    if dd_params.get(io_param):
                        io_params[io_param] = dd_params[io_param]
            suffix = "".join([random.choice(string.ascii_letters) for _ in range(2)])
            file_path = f"{mounting_dir}/{io_params['file_name']}_{suffix}"
            client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/{io_params['input_type']} of={file_path} bs={io_params['bs']} "
                f"count={io_params['count']}",
                long_running=True,
            )

        def iozone():
            """
            iozone description: A filesystem performance benchmarking tool that runs database type ops to evaluate.
            io modes: 13((0=write/rewrite, 1=read/re-read, 2=random-read/write
                 3=Read-backwards, 4=Re-write-record, 5=stride-read, 6=fwrite/re-fwrite
                 7=fread/Re-fread, 8=random_mix, 9=pwrite/Re-pwrite, 10=pread/Re-pread
                 11=pwritev/Re-pwritev, 12=preadv/Re-preadv))
            """
            log.info("IO tool scheduled : IOZONE")
            io_params = {
                "file_name": None,  # default None, else list of filenames required to test ['file1','file2']
                "io_type": ["all"],
                "file_size": "auto",  # mention alternate size in as 64k/64m/64g for KB/MB/GB
                "max_file_size": "100m",
                "reclen": "auto",  # mention alternate size as 64k/64m/64g for KB/MB/GB
                "cpu_use_report": True,  # to report cpu use by each test
                "spreadsheet": True,  # to copy stats to spreadsheet
                "throughput_test": False,
                "threads": 2,  # to be used when throughput_test is True
                "ext_opts": None,  # other options as "-C -e -K",default is 'None'
            }

            if kwargs.get("iozone_params"):
                iozone_params = kwargs.get("iozone_params")
                for io_param in io_params:
                    if iozone_params.get(io_param):
                        io_params[io_param] = iozone_params[io_param]

            iozone_path = "iozone"
            iozone_cmd = f"{iozone_path} -a"
            if io_params["throughput_test"]:
                iozone_cmd = f"{iozone_path} -t {io_params['threads']}"
            if io_params["cpu_use_report"]:
                iozone_cmd += " -+u"
            if io_params["spreadsheet"]:
                iozone_cmd += f" -b {mounting_dir}/iozone_report.xls"
            if "all" not in io_params["io_type"]:
                for io_type in io_params["io_type"]:
                    iozone_cmd += f" -i {io_type}"
            if "auto" not in io_params["file_size"]:
                iozone_cmd += f" -s {io_params['file_size']}"
            elif io_params["file_name"] is None:
                iozone_cmd += f" -g {io_params['max_file_size']}"
            if "auto" not in io_params["reclen"]:
                iozone_cmd += f" -r {io_params['reclen']}"
            if io_params.get("ext_opts"):
                iozone_cmd += f" {io_params['ext_opts']}"
            if io_params.get("file_names"):
                thread_len = len(io_params["file_names"])
                iozone_cmd += f" -t {thread_len} -F "
                for file_name in io_params["file_names"]:
                    io_path = f"{mounting_dir}/{file_name}"
                    iozone_cmd += f" {io_path}"

            cmd = f"cd {mounting_dir};{iozone_cmd}"
            client.exec_command(
                sudo=True,
                cmd=cmd,
                long_running=True,
            )

        def dbench():
            log.info("IO tool scheduled : dbench")
            io_params = {
                "clients": random.choice(
                    range(8, 33, 8)  # Randomly selects 8, 16, 24, or 32 for clients
                ),
                "duration": random.choice(
                    range(60, 601, 60)
                ),  # Randomly selects 60, 120, ..., 600 seconds
                "testdir_prefix": "dbench_io_dir",
            }
            if kwargs.get("dbench_params"):
                dbench_params = kwargs.get("dbench_params")
                for io_param in io_params:
                    if dbench_params.get(io_param):
                        io_params[io_param] = dbench_params[io_param]

            dir_suffix = "".join(
                [
                    random.choice(string.ascii_lowercase + string.digits)
                    for _ in range(4)
                ]
            )
            io_path = f"{mounting_dir}/{io_params['testdir_prefix']}_{dir_suffix}"
            client.exec_command(sudo=True, cmd=f"mkdir {io_path}")

            client.exec_command(
                sudo=True,
                cmd=f"dbench {io_params['clients']} -t {io_params['duration']} -D {io_path}",
                long_running=True,
            )

        def postgresIO():
            log.info("IO tool scheduled : PostgresIO")

            io_params = {
                "scale": random.choice(
                    range(
                        40, 101, 10
                    )  # The size of the database(test data) increases linearly with the scale factor
                ),
                "workers": random.choice(range(4, 17, 4)),
                "clients": random.choice(
                    range(8, 56, 8)  # Randomly selects 8, 16, 24, 32,.. for clients
                ),
                "duration": random.choice(
                    range(60, 601, 60)
                ),  # Randomly selects 60, 120, ..., 600 seconds
                "testdir_prefix": "postgres_io_dir",
                "db_name": "",
                "container_name": "postgres-container",
            }

            if kwargs.get("postgresIO_params"):
                postgresIO_params = kwargs.get("postgresIO_params")
                for io_param in io_params:
                    if postgresIO_params.get(io_param):
                        io_params[io_param] = postgresIO_params[io_param]

            dir_suffix = "".join(
                [
                    random.choice(string.ascii_lowercase + string.digits)
                    for _ in range(4)
                ]
            )
            io_path = f"{mounting_dir}/{io_params['testdir_prefix']}_{dir_suffix}"
            client.exec_command(sudo=True, cmd=f"mkdir {io_path}")

            # Initialize mode
            client.exec_command(
                sudo=True,
                cmd=(
                    f"podman exec -it {io_params['container_name']} bash -c "
                    f"'pgbench -i --scale={io_params['scale']} -U pguser -d {io_params['db_name']}'"
                ),
                long_running=True,
            )

            # Creating tables and populating data based on the scale factor
            client.exec_command(
                sudo=True,
                cmd=(
                    f"podman exec -it {io_params['container_name']} bash -c "
                    f"'pgbench -c {io_params['clients']} "
                    f"-j {io_params['workers']} "
                    f"-T {io_params['duration']} "
                    f"-U postgres -d {io_params['db_name']}'"
                ),
                long_running=True,
            )

        io_tool_map = {
            "dd": dd,
            "smallfile": smallfile,
            "wget": wget,
            "file_extract": file_extract,
            "iozone": iozone,
            "dbench": dbench,
            "postgresIO": postgresIO,
        }

        log.info(f"IO tools planned to run : {io_tools}")

        io_tools = [io_tool_map.get(i) for i in io_tools if io_tool_map.get(i)]

        run_time = kwargs.get("run_time", 1)
        end_time = datetime.datetime.now() + datetime.timedelta(minutes=run_time)
        i = 0
        while datetime.datetime.now() < end_time:
            log.info(f"Iteration : {i}")
            with parallel() as p:
                for io_tool in io_tools:
                    p.spawn(io_tool)
            i += 1
            time.sleep(30)

    def setup_postgresql_IO(self, client, mount_dir, container_name, db_name):
        """
        Setup Steps:
        1. Create the PostgreSQL data directory as Mount dir
        2. Set permissions and owners for Mount dir
        3. Initialise the Postgres Service
        4. Create DB
        """

        # Remove if postgres container already running
        client.exec_command(sudo=True, cmd=f"podman rm -f {container_name}")

        log.info("Setting up PostgresSQL")
        client.exec_command(sudo=True, cmd=f"mkdir -p {mount_dir}")

        log.debug(f"Setting up postgres persmission and user for the dir {mount_dir}")
        client.exec_command(sudo=True, cmd=f"chown -R postgres:postgres {mount_dir}")
        client.exec_command(sudo=True, cmd=f"chmod 700 {mount_dir}")

        rhel_version, _ = client.node.exec_command(
            sudo=True,
            cmd=("rpm -E %rhel"),
        )

        if int(rhel_version.strip()) >= 8:
            rh_registry_postgresql = (
                f"registry.redhat.io/rhel{rhel_version.strip()}/postgresql-16"
            )
            log.debug(f"RH Postgresql Registry: {rh_registry_postgresql}")
        else:
            log.error(
                "Postgresql support is not available for RHEL version lesser than 8"
            )

        client.exec_command(
            sudo=True,
            cmd=(
                f"podman run -d --name {container_name} "
                f"-e POSTGRESQL_USER=pguser "
                f"-e POSTGRESQL_PASSWORD=pgpassword "
                f"-e POSTGRESQL_DATABASE={db_name} "
                f"-v {mount_dir}:/var/lib/pgsql/data:Z "
                f"-p 5432:5432 "
                f"{rh_registry_postgresql}"
            ),
        )

        if not self.wait_until_container_running(client, container_name):
            log.error("PostgreSQL failed to start")

    def cleanup_container(self, client, container_name):
        """
        Used to stop and remove any running containers
        Currently used to clean up PostgreSQL containers
        Args:
            container_name: Container name to be deleted
        """

        client.exec_command(sudo=True, cmd=f"podman stop {container_name}")
        client.exec_command(sudo=True, cmd=f"podman rm {container_name}")
        log.info(f"Cleanup of container {container_name} is successful")

    def wait_until_container_running(
        self, client, container_name, timeout=180, interval=5
    ):
        """
        Checks if the specified Podman container is running within a given timeout.

        :param client: Remote client object
        :param container_name: Name of the container
        :param timeout: Maximum time to wait in seconds (default: 180)
        :param interval: Interval between retries in seconds (default: 5)
        :return: boolean (True if container is running, False if timeout reached)
        """
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
        log.info(f"Waiting for container '{container_name}' to be in running state")

        while datetime.datetime.now() < end_time:
            out, _ = client.exec_command(
                sudo=True,
                cmd=f"podman ps --filter name={container_name} --filter status=running --format '{{{{.Names}}}}'",
            )
            log.info(f"Podman output: {out.strip()}")

            if container_name in out.strip():
                log.info("PostgreSQL is running")
                return True

            time.sleep(interval)

        log.error(
            f"Container '{container_name}' did not reach running state within {timeout} seconds"
        )
        return False
