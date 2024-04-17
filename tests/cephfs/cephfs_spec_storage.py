import os
import random
import string
import traceback
import xml.etree.ElementTree as ET

from ceph.ceph import CommandFailed
from cli.exceptions import OperationFailedError
from cli.io.spec_storage import SpecStorage
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log
from utility.utils import get_ceph_version_from_cluster

log = Log(__name__)


def run_spec_storage_io(
    primary_client,
    benchmark,
    load,
    incr_load,
    num_runs,
    clients,
    nfs_mount,
    benchmark_defination,
    parse_results,
    log_dir,
    **kwargs,
):
    if SpecStorage(primary_client).run_spec_storage(
        benchmark, load, incr_load, num_runs, clients, nfs_mount, benchmark_defination
    ):
        raise OperationFailedError("SPECstorage run failed")
    log.info("SPECstorage run completed")
    if parse_results:
        ceph_version = get_ceph_version_from_cluster(primary_client)
        SpecStorage(primary_client).parse_spectorage_results(
            results_dir="/root/results",
            output_file=f"{log_dir}/output.csv",
            ceph_version=ceph_version,
            fg_name="cephfs",
            fs_type=kwargs.get("fs_type"),
            max_mds=kwargs.get("max_mds"),
            mount_type="fuse" if "fuse" in nfs_mount else "kernel",
        )


def run(ceph_cluster, **kw):
    try:
        log.info(f"MetaData Information {log.metadata} in {__name__}")
        fs_util = FsUtils(ceph_cluster)
        log_dir = os.path.dirname(log.logger.handlers[0].baseFilename)
        config = kw.get("config")
        build = config.get("build", config.get("rhbuild"))
        clients = ceph_cluster.get_ceph_objects("client")
        spec_clients = ceph_cluster.get_nodes("client")
        fs_name = config.get("fs_name", "cephfs")
        fs_util.prepare_clients(clients, build)
        fs_util.auth_list(clients)
        fs_details = FsUtils.get_fs_info(clients[0], fs_name)
        mdss = ceph_cluster.get_ceph_objects("mds")
        if len(mdss) < 3:
            log.error("This test requires atleast 3 mds nodes with role in config file")
            return 1
        mds1 = mdss[0].node.hostname
        mds2 = mdss[1].node.hostname
        mds3 = mdss[2].node.hostname
        if not fs_details:
            if not config.get("erasure"):
                fs_util.create_fs(
                    client=clients[0],
                    vol_name=fs_name,
                    placement=f"3 {mds1} {mds2} {mds3}",
                )
            else:
                clients[0].exec_command(
                    sudo=True, cmd=f"ceph osd pool create {fs_name}-data-ec 64 erasure"
                )
                clients[0].exec_command(
                    sudo=True, cmd=f"ceph osd pool create {fs_name}-metadata-ec 64"
                )
                clients[0].exec_command(
                    sudo=True,
                    cmd=f"ceph osd pool set {fs_name}-data-ec allow_ec_overwrites true",
                )
                clients[0].exec_command(
                    sudo=True,
                    cmd=f"ceph fs new {fs_name} {fs_name}-metadata-ec {fs_name}-data-ec --force",
                )
                clients[0].exec_command(
                    sudo=True,
                    cmd=f"ceph orch apply mds {fs_name} --placement='3 {mds1} {mds2} {mds3}'",
                )
        if config.get("max_mds"):
            clients[0].exec_command(
                sudo=True,
                cmd=f"ceph orch apply mds {fs_name} --placement='3 {mds1} {mds2} {mds3}'",
            )
            clients[0].exec_command(
                sudo=True, cmd=f"ceph fs set {fs_name} max_mds {config.get('max_mds')}"
            )
        mounting_dir = "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in list(range(10))
        )
        fuse_mounting_dir = f"/mnt/cephfs_fuse{mounting_dir}/"
        fs_util.fuse_mount(
            clients, fuse_mounting_dir, extra_params=f" --client_fs {fs_name}"
        )

        kernel_mounting_dir = f"/mnt/cephfs_kernel{mounting_dir}/"
        mon_node_ips = fs_util.get_mon_node_ips()
        fs_util.kernel_mount(
            clients,
            kernel_mounting_dir,
            ",".join(mon_node_ips),
            extra_params=f",fs={fs_name}",
        )
        benchmark = config.get("benchmark", "SWBUILD")

        run_spec_storage(
            config, spec_clients, [kernel_mounting_dir, fuse_mounting_dir], log_dir
        )

        log.info("Cleaning up!-----")
        rc = fs_util.client_clean_up(
            [],
            clients,
            kernel_mounting_dir,
            "umount",
        )
        if rc != 0:
            raise CommandFailed("fuse clients cleanup failed")
        log.info("Fuse clients cleaned up successfully")

        rc = fs_util.client_clean_up(
            clients,
            [],
            fuse_mounting_dir,
            "umount",
        )
        if rc != 0:
            raise CommandFailed("kernel clients cleanup failed")
        log.info("kernel clients cleaned up successfully")
        return 0

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
    finally:
        log_dir = os.path.dirname(log.logger.handlers[0].baseFilename)
        os.makedirs(f"{log_dir}/spec_storage_{benchmark}_results/", exist_ok=True)
        download_dir(
            spec_clients[0],
            remote_dir="/root/results",
            local_dir=f"{log_dir}/spec_storage_{benchmark}_results/",
        )
        spec_clients[0].exec_command(
            sudo=True,
            cmd="mkdir -p /root/tmp_results;mv /root/results/* /root/tmp_results/",
        )


def download_dir(client, remote_dir, local_dir):
    os.makedirs(local_dir, exist_ok=True)
    dir_items = client.get_dir_list(remote_dir, sudo=True)
    for item in dir_items:
        remote_path = remote_dir + "/" + item
        local_path = os.path.join(local_dir, item)
        out, rc = client.exec_command(
            sudo=True, cmd=f"test -d {remote_path}", check_ec=False
        )
        log.info(f"Directory results {out}, {rc}")
        if client.exit_status:
            client.download_file(remote_path, local_path, sudo=True)
        else:
            download_dir(client, remote_path, local_path)


def parse_xml(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()

    data = {}
    for metric in root.findall(".//metric"):
        name = metric.get("name")
        value = metric.text
        unit = metric.get("units")
        data[name] = f"{value} {unit}"

    return data


def print_table(data_list):
    max_name_length = max(
        max(len(name) for data in data_list for name in data.keys()), default=0
    )
    max_value_length = max(
        max(len(value) for data in data_list for value in data.values()), default=0
    )
    table_width = (max_name_length + max_value_length + 10) * len(data_list) + 4

    print("-" * table_width)
    print("| Metric" + " " * (max_name_length - 6), end="")
    for i in range(len(data_list)):
        print(f"| Value (File {i+1})" + " " * (max_value_length - 10), end="")
    print("|")
    print("-" * table_width)
    metrics = set().union(*[data.keys() for data in data_list])
    for metric in metrics:
        print(f"| {metric:>{max_name_length}} ", end="")
        for data in data_list:
            value = data.get(metric, "")
            print(f"| {value:>{max_value_length}} ", end="")
        print("|")
    print("-" * table_width)


def print_results(log_dir, benchmark, output_xml):
    xml_files = [f"{log_dir}/spec_storage_{benchmark}_results/{i}" for i in output_xml]
    data_list = [parse_xml(xml_file) for xml_file in xml_files]
    print_table(data_list)


def run_spec_storage(config, spec_clients, mount_points, log_dir):
    benchmark = config.get("benchmark", "SWBUILD")
    benchmark_defination = config.get("benchmark_defination")
    load = config.get("load", "1")
    incr_load = config.get("incr_load", "1")
    num_runs = config.get("num_runs", "1")
    output_xml = []
    for mount in mount_points:
        run_spec_storage_io(
            spec_clients[0],
            benchmark,
            load,
            incr_load,
            num_runs,
            spec_clients,
            mount,
            benchmark_defination,
            parse_results=config.get("parse_result", False),
            log_dir=log_dir,
            fs_type="erasure" if config.get("erasure") else "replicated",
            max_mds=config.get("max_mds"),
        )
        nfs_mount = mount.rstrip("/")
        last_path_component = os.path.basename(nfs_mount)
        output_xml.append(f"sfssum_{benchmark}-result-{last_path_component}.xml")
