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
        ibm_cloud_api_key_required (optional):
                    If true, IBM_CLOUD_API_KEY must be set (env or
                    ibm_cloud.api-key in ~/.cephci.yaml). Checked before
                    test execution; key is never logged (masked). The key
                    is written to the pre-defined path CEPHCI_SECRETS_DIR/
                    IBM_CLOUD_API_KEY_FILENAME; the test script must read
                    from that location (no key is passed via the command).
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

import os

import yaml

from utility import utils
from utility.log import Log
from utility.utils import (
    config_keystone_ldap,
    configure_kafka_security,
    get_cephci_config,
    install_start_kafka,
    setup_cluster_access,
)

log = Log(__name__)

CEPHCI_SECRETS_DIR = "/home/cephuser/.cephci/secrets"
IBM_CLOUD_API_KEY_FILENAME = "ibm_cloud_api_key"

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
    install_keystone_ldap = config.get("install_keystone_ldap")
    cloud_type = config.get("cloud-type")
    log.info(f"Cloud Type is {cloud_type}")
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
    # Clone the repository once for the entire test suite (clone done as root; execution later by other user)
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

    if install_keystone_ldap:
        config_keystone_ldap(rgw_node, cloud_type)

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
        f_name = f"/home/cephuser/{test_folder}" + config_dir + config_file_name
        remote_fp = exec_from.remote_file(file_name=f_name, file_mode="w", sudo=True)
        remote_fp.write(yaml.dump(test_config, default_flow_style=False))

    # Build env vars for test execution; inject IBM_CLOUD_API_KEY from env or cephci.yaml
    env_vars = list(config.get("env-vars", []))
    ibm_cloud_api_key_required = config.get("ibm_cloud_api_key_required", False)
    ibm_cloud_api_key = os.environ.get("IBM_CLOUD_API_KEY")
    if not ibm_cloud_api_key:
        try:
            cephci_config = get_cephci_config()
            ibm_cloud_config = cephci_config.get("ibm_cloud", {})
            ibm_cloud_api_key = ibm_cloud_config.get("api-key")
        except (IOError, KeyError, AttributeError) as e:
            log.debug("Could not read IBM_CLOUD_API_KEY from cephci.yaml: %s", e)
    if ibm_cloud_api_key_required and not ibm_cloud_api_key:
        raise ValueError(
            "Set IBM_CLOUD_API_KEY in the environment or ibm_cloud.api-key in cephci.yaml"
        )
    ibm_key_file = None
    if ibm_cloud_api_key:
        log.info("IBM_CLOUD_API_KEY configured (masked), adding to test execution")
        ibm_key_file = f"{CEPHCI_SECRETS_DIR}/{IBM_CLOUD_API_KEY_FILENAME}"
        try:
            exec_from.exec_command(
                cmd=f"mkdir -p {CEPHCI_SECRETS_DIR} && chmod 700 {CEPHCI_SECRETS_DIR}"
            )
            remote_fp = exec_from.remote_file(
                file_name=ibm_key_file, file_mode="w", sudo=False
            )
            remote_fp.write(ibm_cloud_api_key)
            remote_fp.flush()
            exec_from.exec_command(cmd=f"sudo chmod 600 {ibm_key_file}")
        except Exception:
            log.error(
                "Failed to write IBM_CLOUD_API_KEY file on remote (key not logged)"
            )
            raise
    # IBM key is written to CEPHCI_SECRETS_DIR; test script reads from that path when needed.
    # Ensure rgw-tests is owned by current user so test script can create dirs (e.g. test_data)
    exec_from.exec_command(
        cmd="sudo chown -R $(whoami):$(id -gn) ~/rgw-tests",
        check_ec=False,
    )
    cmd_env = " ".join(env_vars)
    test_script_args = (
        test_folder_path
        + script_dir
        + script_name
        + " -c "
        + test_folder
        + config_dir
        + config_file_name
        + append_param
    )
    # Run test script with sudo (same as before; scripts may need /etc/ceph write, etc.)
    if cmd_env:
        run_cmd = f"sudo env {cmd_env} {python_cmd} " + test_script_args
    else:
        run_cmd = f"sudo {python_cmd} " + test_script_args
    try:
        test_status = exec_from.exec_command(
            cmd=run_cmd,
            long_running=True,
            timeout=timeout,
        )
    finally:
        if ibm_key_file:
            exec_from.exec_command(
                cmd=f"sudo rm -f {ibm_key_file}",
                check_ec=False,
            )

    if run_io_verify:
        log.info("running io verify script")
        verify_io_info_configs = config.get(
            "verify-io-info-configs", [config_file_name]
        )
        for io_config in verify_io_info_configs:
            verify_status = exec_from.exec_command(
                cmd=f"sudo {python_cmd} "
                + test_folder_path
                + lib_dir
                + f"read_io_info.py -c {io_config}",
                long_running=True,
            )
            log.info(f"verify io status code is : {verify_status}")
            if verify_status != 0:
                raise Exception(f"verify io failed for {io_config}")

    return test_status
