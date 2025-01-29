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
from utility.utils import (
    configure_kafka_security,
    install_start_kafka,
    setup_cluster_access,
)

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
    client_ceph_object = ceph_cluster.get_ceph_object("client")
    run_io_verify = config.get("run_io_verify", False)
    extra_pkgs = config.get("extra-pkgs")
    install_start_kafka_broker = config.get("install_start_kafka")
    configure_kafka_broker_security = config.get("configure_kafka_security")
    cloud_type = config.get("cloud-type")
    test_config = {"config": config.get("test-config", {})}
    rgw_node = rgw_ceph_object.node
    client_node = client_ceph_object.node
    run_on_rgw = (
        True
        if ceph_cluster.rhcs_version.version[0] == 4
        else config.get("run-on-rgw", False)
    )
    run_on_haproxy = (
        True
        if ceph_cluster.rhcs_version.version[0] == 4
        else config.get("run-on-haproxy", False)
    )

    if run_on_rgw:
        exec_from = rgw_node
        append_param = ""
    elif run_on_haproxy:
        exec_from = client_node
        append_param = ""
    else:
        exec_from = client_node
        append_param = " --rgw-node " + str(rgw_node.ip_address)

    # install extra package which are test specific
    if extra_pkgs:
        log.info(f"got extra pkgs: {extra_pkgs}")
        package_path = "/root/rgw_rpms"
        pkgs_str = ""
        for pkg in extra_pkgs:
            pkgs_str += f"{package_path}/{pkg}.rpm "

        exec_from.exec_command(
            sudo=True, cmd=f"yum install -y {pkgs_str}", long_running=True
        )

    log.info("Flushing iptables")
    exec_from.exec_command(cmd="sudo iptables -F", check_ec=False)
    config["git-url"] = config.get(
        "git-url", "https://github.com/red-hat-storage/ceph-qe-scripts.git"
    )

    test_folder = "rgw-tests"
    test_folder_path = f"~/{test_folder}"
    # Clone the repository once for the entire test suite
    pip_cmd = "venv/bin/pip"
    python_cmd = "venv/bin/python"
    out, err = exec_from.exec_command(cmd=f"ls -l {test_folder}", check_ec=False)
    if not out:
        exec_from.exec_command(cmd=f"sudo mkdir {test_folder}")
        utils.clone_the_repo(config, exec_from, test_folder_path)

    install_common = config.get("install_common", True)
    if install_common:
        if ceph_cluster.rhcs_version.version[0] > 4:
            setup_cluster_access(ceph_cluster, rgw_node)
            setup_cluster_access(ceph_cluster, client_node)

        if ceph_cluster.rhcs_version.version[0] in [3, 4]:
            if ceph_cluster.containerized:
                # install ceph-common on the host hosting the container
                exec_from.exec_command(
                    sudo=True,
                    cmd="yum install -y ceph-common --nogpgcheck",
                    check_ec=False,
                )

    if install_start_kafka_broker:
        install_start_kafka(rgw_node, cloud_type)
    if configure_kafka_broker_security:
        configure_kafka_security(rgw_node, cloud_type)

    out, err = exec_from.exec_command(cmd="ls -l venv", check_ec=False)
    if not out:
        exec_from.exec_command(
            cmd="yum install python3 -y --nogpgcheck", check_ec=False, sudo=True
        )
        exec_from.exec_command(cmd="python3 -m venv venv")
        exec_from.exec_command(cmd=f"{pip_cmd} install --upgrade pip")

        exec_from.exec_command(
            cmd=f"{pip_cmd} install "
            + f"-r {test_folder}/ceph-qe-scripts/rgw/requirements.txt"
        )

    script_name = config.get("script-name")
    config_file_name = config.get("config-file-name")
    test_version = config.get("test-version", "v2")
    script_dir = DIR[test_version]["script"]
    config_dir = DIR[test_version]["config"]
    lib_dir = DIR[test_version]["lib"]
    timeout = config.get("timeout", 3600)

    if test_config["config"]:
        log.info("creating custom config")
        f_name = test_folder + config_dir + config_file_name
        remote_fp = exec_from.remote_file(file_name=f_name, file_mode="w")
        remote_fp.write(yaml.dump(test_config, default_flow_style=False))

    cmd_env = " ".join(config.get("env-vars", []))
    test_status = exec_from.exec_command(
        cmd=cmd_env
        + f"sudo {python_cmd} "
        + test_folder_path
        + script_dir
        + script_name
        + " -c "
        + test_folder
        + config_dir
        + config_file_name
        + append_param,
        long_running=True,
        timeout=timeout,
    )

    if run_io_verify:
        log.info("running io verify script")
        verify_status = exec_from.exec_command(
            cmd=f"sudo {python_cmd} "
            + test_folder_path
            + lib_dir
            + f"read_io_info.py -c {config_file_name}",
            long_running=True,
        )
        log.info(f"verify io status code is : {verify_status}")
        if verify_status != 0:
            log.error(verify_status)
            return verify_status

    return test_status
