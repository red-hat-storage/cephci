"""
Module to verify configuration values are accessible via cephadm-ansible module.
This is achieved by setting configuration value using `ceph config set` and
Configuration value retrieved by `ceph config get` is checked in output of
cephadm-ansible playbook.
"""

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.mgr_workflows import MgrWorkflows
from cli.utilities.configure import SSH_ID_RSA_PUB, SSH_KEYGEN, SSHPASS
from cli.utilities.packages import Package
from cli.utilities.utils import get_ip_from_node
from tests.rados.monitor_configurations import MonConfigMethods
from utility.log import Log

log = Log(__name__)
SSH_COPYID = "ssh-copy-id -f -i {} -o StrictHostKeyChecking=no {}@{}"


def run(ceph_cluster, **kw):
    """
    BZ https://bugzilla.redhat.com/show_bug.cgi?id=2213766#c37 :

    1. Set mgr/dashboard/node_name/server_addr configuration
       of mon daemon to IP address using `ceph config set` command.
    2. Retrieve configuration value using `ceph config get` command.
    3. Execute `cephadm-ansible` playbook to retrieve configuration
       value.
    4. Validate `cephadm-ansible` playbook output contains configuration value
       from step #2.

    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster

    """
    log.info("Running test for bz_2213766")
    config = kw.get("config")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mgr_obj = MgrWorkflows(node=cephadm)
    installer_node = ceph_cluster.get_ceph_object("installer")
    ansible_dir = "/usr/share/cephadm-ansible"
    config_obj = MonConfigMethods(rados_obj=rados_obj)
    playbook_name = "test_bz_2213766.yaml"
    playbook_absolute_path = f"{ansible_dir}/{playbook_name}"
    ansible_package_name = "cephadm-ansible"

    try:

        log.info(f"Installing {ansible_package_name} package for test")

        Package(installer_node).install(ansible_package_name, nogpgcheck=False)

        log.info(
            f"Successfully installed {ansible_package_name} package\n"
            "Retrieving active manager hostname, host object, ip ddress"
        )

        active_mgr = mgr_obj.get_active_mgr()
        hostname = active_mgr.split(".")[0]
        host_obj = rados_obj.get_host_object(hostname=hostname)
        host_ip_address = get_ip_from_node(host_obj)[1]

        log.info(
            f"Retrieved active manager hostname, host object and ip address\n"
            f"Hostname {hostname}\n"
            f"Host object {host_obj}\n"
            f"IP address {host_ip_address}\n"
            f"Proceeding to generate ssh keys for {installer_node.node.ceph_nodename}"
        )

        _, err, rc, _ = installer_node.exec_command(
            sudo=True, cmd=SSH_KEYGEN, verbose=True
        )
        if rc != 0:
            log.debug(f"{SSH_KEYGEN} execution failed with error : {err}")
            raise Exception(
                f"Error generating ssh-keys for {installer_node.node.ceph_nodename}"
            )

        log.info(
            f"Successfully generated ssh keys for {installer_node.node.ceph_nodename}"
            f"Proceeding with copying public ssh key of {installer_node.node.ceph_nodename} to {host_obj.hostname}"
        )

        _, err, rc, _ = installer_node.exec_command(
            cmd="{} {}".format(
                SSHPASS.format(host_obj.root_passwd),
                SSH_COPYID.format(SSH_ID_RSA_PUB, "root", host_obj.hostname),
            ),
            sudo=True,
            verbose=True,
        )

        if rc != 0:
            log.debug(
                f"ssh-copy-id execution failed with error : {err}\nexit code: {rc}"
            )
            raise Exception(
                f"Error copying public ssh-keys of {installer_node.node.ceph_nodename} to {host_obj.hostname}"
            )

        log.info(
            f"Successfully copied public ssh key of {installer_node.node.ceph_nodename} to {host_obj.hostname}"
        )

        config_section = "mgr"
        config_name = f"mgr/dashboard/{hostname}/server_addr"
        config_value = host_ip_address

        log.info(f"Proceeding to set configuration {config_name} for {config_section}")
        if (
            config_obj.set_config(
                section=config_section,
                name=config_name,
                value=config_value,
            )
            is False
        ):
            raise AssertionError(
                f"Unable to set configuration for {config_section} parameter:{config_name} value:{config_value}"
            )

        log.info(
            f"Successfully set configuration {config_name} for {config_section}\n"
            f"Retrieving parameter {config_value} using `ceph config get`"
        )

        ceph_config_get_value = config_obj.get_config(
            section=config_section,
            param=config_name,
        )

        log.info(
            "Successfully retrieved configuration parametereter value using `ceh config get`\n"
            f"parametereter: {config_name}\n"
            f"Daemon: {config_section}\n"
            f"Value: {ceph_config_get_value}\n"
            f"Proceeding to create Ansible playbook at location {playbook_absolute_path}"
        )

        cmd = f"""cat <<EOF > {playbook_absolute_path}
---
- name: Get config parameterters
  hosts: {host_ip_address}
  tasks:
    - name: get the dashboard server address
      ceph_config:
         action: get
         who: {config_section}
         option: {config_name}
EOF"""
        _, err = installer_node.exec_command(cmd=cmd, sudo=True)
        if err:
            raise AssertionError(
                f"Failed to create ansible playbook at location {playbook_absolute_path}\n"
                f"Error: {err}"
            )

        log.info(
            f"Successfully created Ansible playbook at location: {playbook_absolute_path}"
        )
        log.info(f"Ansible playbook contents: {cmd}")
        log.info("Proceeding to execute Ansible playbook")

        cmd = f"cd {ansible_dir} ; ansible-playbook -v -i {host_ip_address}, {playbook_absolute_path}"
        out, err, rc, _ = installer_node.exec_command(cmd=cmd, sudo=True, verbose=True)
        if rc != 0:
            raise AssertionError(
                f"Exit code: {rc}\n `{cmd}` execution failed with error : \n{err}"
            )

        log.info("Ansible playbook execution successful")
        log.info(f"Ansible playbook execution output \n {out}")

        search_text = f"stdout: {ceph_config_get_value}"
        if search_text not in out:
            raise AssertionError(
                f'Search text "{search_text}" does not exist in `cephadm-ansible` playbook output'
                f"`ceph config get` parametereter value : {ceph_config_get_value}\n"
                f"`cephadm-ansible` output : {out} \n"
            )

        log.info(
            f'Search text "{search_text}" exists in `cephadm-ansible` playbook output'
        )

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1

    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )

        if (config_name in locals() and config_name in globals()) and (
            config_section in locals() and config_section in globals()
        ):
            log.info(
                f"Removing configuration value for {config_name} of {config_section}"
            )
            config_obj.remove_config(
                section=config_section,
                name=config_name,
            )
            log.info(f"Successfully removed configuration value for {config_name}")

        # if ansible_package_name in Package(installer_node).list(ansible_package_name):
        #     log.info(f"Removing Ansible package {ansible_package_name}")
        #     Package(installer_node).remove(ansible_package_name)
        #     log.info(f"Successfully removed Ansible package {ansible_package_name}")

        # log cluster health
        rados_obj.log_cluster_health()

        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info(
        "Successfully verified configuration values are accessible through cephadm-ansible module"
    )
    return 0
