"""
This module will allow us to run tests from ceph-qe-scritps repo.
Repo: https://github.com/red-hat-storage/ceph-qe-scripts
Folder: rgw

Below configs are needed in order to run the tests

    config:
        script-name:
                    The script to run
        config-file-name:
                    Config file for the above script,
                    use this file if custom test-config is not given
        test-config (optional):
                    Custom test config supported for the above script,
                    refer structure in ceph-qe-scripts/rgw
                    example:
                        test-config:
                            user_count: 1
                            bucket_count: 10
                            objects_count: 5
                            objects_size_range:
                            min: 5M
                            max: 15M
        run_io_verify (optional):
                    true or false
        env-vars (optional):
                    - cleanup=False
                    - objects_count: 500
        extra-pkgs (optional):
                    Packages to install
                    example:
                        a. distro specific packages
                            extra-pkgs:
                                7:
                                    - pkg1
                                    - pkg2
                                8:
                                    - pkg1
                                    - pkg2
                        b. list of packages which are not distro version specific
                            extra-pkgs:
                                - pkg1
                                - pkg2

"""


import yaml

from utility import utils
from utility.log import Log
from utility.utils import install_start_kafka, setup_cluster_access

log = Log(__name__)

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
    extra_pkgs = config.get("extra-pkgs")
    install_start_kafka_broker = config.get("install_start_kafka")
    cloud_type = config.get("cloud-type")
    test_config = {"config": config.get("test-config", {})}
    rgw_node = rgw_ceph_object.node
    distro_version_id = rgw_node.distro_info["VERSION_ID"]

    # install extra package which are test specific
    if extra_pkgs:
        log.info(f"got extra pkgs: {extra_pkgs}")
        if isinstance(extra_pkgs, dict):
            _pkgs = extra_pkgs.get(int(distro_version_id[0]))
            pkgs = " ".join(_pkgs)
        else:
            pkgs = " ".join(extra_pkgs)

        rgw_node.exec_command(
            sudo=True, cmd=f"yum install -y {pkgs}", long_running=True
        )

    if install_start_kafka_broker:
        install_start_kafka(rgw_node, cloud_type)

    log.info("Flushing iptables")
    rgw_node.exec_command(cmd="sudo iptables -F", check_ec=False)
    config["git-url"] = config.get(
        "git-url", "https://github.com/red-hat-storage/ceph-qe-scripts.git"
    )

    test_folder = "rgw-tests"
    test_folder_path = f"~/{test_folder}"
    # Clone the repository once for the entire test suite
    pip_cmd = "venv/bin/pip"
    python_cmd = "venv/bin/python"
    out, err = rgw_node.exec_command(cmd=f"ls -l {test_folder}", check_ec=False)
    if not out:
        rgw_node.exec_command(cmd=f"sudo mkdir {test_folder}")
        utils.clone_the_repo(config, rgw_node, test_folder_path)

        if ceph_cluster.rhcs_version.version[0] > 4:
            setup_cluster_access(ceph_cluster, rgw_node)
            rgw_node.exec_command(
                sudo=True, cmd="yum install -y ceph-common --nogpgcheck", check_ec=False
            )

        if ceph_cluster.rhcs_version.version[0] in [3, 4]:
            if ceph_cluster.containerized:
                # install ceph-common on the host hosting the container
                rgw_node.exec_command(
                    sudo=True,
                    cmd="yum install -y ceph-common --nogpgcheck",
                    check_ec=False,
                )
    out, err = rgw_node.exec_command(cmd="ls -l venv", check_ec=False)

    if not out:
        rgw_node.exec_command(
            cmd="yum install python3 -y --nogpgcheck", check_ec=False, sudo=True
        )
        rgw_node.exec_command(cmd="python3 -m venv venv")
        rgw_node.exec_command(cmd=f"{pip_cmd} install --upgrade pip")

        rgw_node.exec_command(
            cmd=f"{pip_cmd} install "
            + f"-r {test_folder}/ceph-qe-scripts/rgw/requirements.txt"
        )

    script_name = config.get("script-name")
    config_file_name = config.get("config-file-name")
    test_version = config.get("test-version", "v2")
    script_dir = DIR[test_version]["script"]
    config_dir = DIR[test_version]["config"]
    lib_dir = DIR[test_version]["lib"]
    timeout = config.get("timeout", 300)

    if test_config["config"]:
        log.info("creating custom config")
        f_name = test_folder + config_dir + config_file_name
        remote_fp = rgw_node.remote_file(file_name=f_name, file_mode="w")
        remote_fp.write(yaml.dump(test_config, default_flow_style=False))

    cmd_env = " ".join(config.get("env-vars", []))
    test_status = rgw_node.exec_command(
        cmd=cmd_env
        + f"sudo {python_cmd} "
        + test_folder_path
        + script_dir
        + script_name
        + " -c "
        + test_folder
        + config_dir
        + config_file_name,
        long_running=True,
    )

    if run_io_verify:
        log.info("running io verify script")
        verify_out, err = rgw_node.exec_command(
            cmd=f"sudo {python_cmd} " + test_folder_path + lib_dir + "read_io_info.py",
            timeout=timeout,
        )
        log.info(verify_out)
        log.error(err)

    return test_status
