"""
Entry module for executing RBD test scripts from ceph-qe-scripts.

This acts as a wrapper around the automation scripts in ceph-qe-scripts for cephci. The
following things are done

- Call the appropriate test script
- Return the status code of the script.
"""
from utility.log import Log

log = Log(__name__)


def run(**kw):
    """
    Execute the test script.

    Args:
        kw: Supports the below keys
            ceph_cluster:   Ceph object
            config:         User configuration provided in the test suite.
    """
    log.info("Running rbd tests")

    ceph_cluster = kw["ceph_cluster"]
    client_nodes = ceph_cluster.get_nodes(role="client")
    client_node = client_nodes[0]

    if not client_node:
        log.error("Require a client node to execute the tests.")
        return 1

    # function constants
    test_folder = "rbd-tests"
    script_folder = "ceph-qe-scripts/rbd/system"
    venv_folder = "venv"
    python_cmd = "sudo venv/bin/python"

    git_url = "https://github.com/red-hat-storage/ceph-qe-scripts.git"
    git_clone = f"git clone {git_url}"

    # Cleaning up the cloned repo to avoid test residues
    client_node.exec_command(
        cmd=f"sudo rm -rf {test_folder}"
        + f" ; mkdir {test_folder}"
        + f" ; cd {test_folder}"
        + f" ; {git_clone}"
    )

    # Optimizing the installation of prerequisites so that they are executed once
    check_venv, err = client_node.exec_command(cmd="ls -l venv", check_ec=False)
    if not check_venv:
        commands = ["sudo yum install -y python3", f"python3 -m venv {venv_folder}"]

        for command in commands:
            client_node.exec_command(cmd=command)

    config = kw["config"]
    script_name = config["test_name"]
    timeout = config.get("timeout", 1800)

    command = f"{python_cmd} {test_folder}/{script_folder}/{script_name}"

    if config.get("ec-pool-k-m", None):
        ec_pool_arg = " --ec-pool-k-m " + config.get("ec-pool-k-m")
        command = command + f" {ec_pool_arg}"

    if config.get("test_case_name", None):
        test_case_name = "--test-case " + config.get("test_case_name")
        command = command + f" {test_case_name}"

    out, err = client_node.exec_command(cmd=command, check_ec=False, timeout=timeout)

    if out:
        log.info(out)

    if err:
        log.error(err)

    rc = client_node.exit_status
    if rc == 0:
        log.info("%s completed successfully", command)
    else:
        log.error("%s has failed", command)

    return rc
