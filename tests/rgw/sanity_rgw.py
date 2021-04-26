import logging

from utility.utils import setup_cluster_access

log = logging.getLogger(__name__)

DIR = {
    "v1": {
        "script": "/ceph-qe-scripts/rgw/v1/tests/s3/",
        "lib": "/ceph-qe-scripts/rgw/v1/lib/",
        "config": "/ceph-qe-scripts/rgw/v1/tests/s3/yamls/",
    },
    "v2": {
        "script": "/ceph-qe-scripts/rgw/v2/tests/s3_swift/",
        "lib": "/ceph-qe-scripts/rgw/v2/lib/",
        "config": "/ceph-qe-scripts/rgw/v2/tests/s3_swift/configs/",
    },
}


def run(ceph_cluster, **kw):
    """

    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    """
    config = kw.get("config")
    log.info("Running RGW test version: %s", config.get("test-version", "v2"))

    rgw_ceph_object = ceph_cluster.get_ceph_object("rgw")
    run_io_verify = config.get("run_io_verify", False)
    rgw_node = rgw_ceph_object.node

    log.info("Flushing iptables")
    rgw_node.exec_command(cmd="sudo iptables -F", check_ec=False)

    test_folder = "rgw-tests"
    test_folder_path = f"~/{test_folder}"
    git_url = "https://github.com/red-hat-storage/ceph-qe-scripts.git"
    git_clone = f"git clone {git_url} -b master"
    rgw_node.exec_command(
        cmd=f"sudo rm -rf {test_folder}"
        + f" && mkdir {test_folder}"
        + f" && cd {test_folder}"
        + f" && {git_clone}"
    )

    # Clone the repository once for the entire test suite
    pip_cmd = "venv/bin/pip"
    python_cmd = "venv/bin/python"
    out, err = rgw_node.exec_command(cmd="ls -l venv", check_ec=False)

    if not out.read().decode():
        rgw_node.exec_command(
            cmd="yum install python3 -y --nogpgcheck", check_ec=False, sudo=True
        )
        rgw_node.exec_command(cmd="python3 -m venv venv")
        rgw_node.exec_command(cmd=f"{pip_cmd} install --upgrade pip")

        rgw_node.exec_command(
            cmd=f"{pip_cmd} install "
            + f"-r {test_folder}/ceph-qe-scripts/rgw/requirements.txt"
        )

        # Ceph object's containerized attribute is not initialized and bound to err
        # as a workaround for 5.x, checking if the environment is 5.0
        if ceph_cluster.rhcs_version == "5.0" or ceph_cluster.containerized:
            if ceph_cluster.rhcs_version == "5.0":
                setup_cluster_access(ceph_cluster, rgw_node)

            rgw_node.exec_command(
                sudo=True, cmd="dnf install -y ceph-radosgw --nogpgcheck"
            )

    script_name = config.get("script-name")
    config_file_name = config.get("config-file-name")
    test_version = config.get("test-version", "v2")
    script_dir = DIR[test_version]["script"]
    config_dir = DIR[test_version]["config"]
    lib_dir = DIR[test_version]["lib"]
    timeout = config.get("timeout", 300)

    out, err = rgw_node.exec_command(
        cmd=f"sudo {python_cmd} "
        + test_folder_path
        + script_dir
        + script_name
        + " -c "
        + test_folder
        + config_dir
        + config_file_name,
        timeout=timeout,
    )
    log.info(out.read().decode())
    log.error(err.read().decode())

    if run_io_verify:
        log.info("running io verify script")
        verify_out, err = rgw_node.exec_command(
            cmd=f"sudo {python_cmd} " + test_folder_path + lib_dir + "read_io_info.py",
            timeout=timeout,
        )
        log.info(verify_out.read().decode())
        log.error(verify_out.read().decode())

    return 0
