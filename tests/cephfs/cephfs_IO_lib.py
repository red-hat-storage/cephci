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

            iozone_path = "/home/cephuser/iozone3_506/src/current/iozone"
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

        io_tool_map = {
            "dd": dd,
            "smallfile": smallfile,
            "wget": wget,
            "file_extract": file_extract,
            "iozone": iozone,
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
