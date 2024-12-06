from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.mgr_workflows import MgrWorkflows
from cli.utilities.utils import get_ip_from_node
from utility.log import Log

log = Log(__name__)


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
    log.info("Running test for bz-1829646")
    config = kw.get("config")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mgr_obj = MgrWorkflows(node=cephadm)
    installer_node = ceph_cluster.get_ceph_object("installer")
    ansible_dir = "/usr/share/cephadm-ansible"
    inventory = "hosts"

    try:

        log.info("Retrieving active manager hostname, host object, ip ddress")

        active_mgr = mgr_obj.get_active_mgr()
        hostname = active_mgr.split(".")[0]
        host_obj = rados_obj.get_host_object(hostname=hostname)
        value = get_ip_from_node(host_obj)[1]

        log.info(
            f"Retrieved active manager hostname, host object and ip address\n"
            f"Hostname {hostname}\n"
            f"Host object {host_obj}\n"
            f"IP address {value}\n"
        )

        who = "mgr"
        param = f"mgr/dashboard/{hostname}/server_addr"

        log.info(f"Proceeding to set Parameter {param} for {who} on host {hostname}")

        if not rados_obj.set_configuration(who="mgr", param=param, value=value):
            raise AssertionError(
                f"Unable to set configuration for {who} param:{param} value:{value}"
            )

        log.info(
            f"Successfully set configuration {param} for {who}\n"
            f"Retrieving param {param} using `ceph config get`"
        )

        ceph_config_get_value = rados_obj.get_configuration(who=who, param=param)

        log.info(
            "Successfully retrieved configuration parameter value using `ceh config get`\n"
            f"Parameter: {param}\n"
            f"Daemon: {who}\n"
            f"Value: {ceph_config_get_value}\n"
            f"Retrieving configuration parameter {param} using `cephadm-ansible` playbook"
        )

        cmd = f"""cat <<EOF > {ansible_dir}/test_bz_2213766.yaml
---
- name: Get config paramters
  hosts:  {value}
  tasks:
    - name: get the dashboard server address
      ceph_config:
         action: get
         who: {who}
         option: {param}
EOF"""
        rc = installer_node.exec_command(cmd=cmd, long_running=True, sudo=True)

        if rc != 0:
            raise AssertionError("ansible-playbook failed to execute")

        cmd = f"cd {ansible_dir} ; ansible-playbook -v -i {inventory} {ansible_dir}/test_bz_2213766.yaml"
        out, err = installer_node.exec_command(cmd=cmd, sudo=True)
        if not err:
            raise AssertionError("ansible-playbook failed to execute")

        log.info("ansible-playbook execution completed")
        search_text = f"stdout: {ceph_config_get_value}"
        if search_text not in out:
            raise AssertionError(
                f'Search text "{search_text}" does not exist in `cephadm-ansible` playbook output'
                f"`ceph config get` parameter value : {ceph_config_get_value}\n"
                f"`cephadm-ansible` output : {out} \n"
            )

        log.info(
            f'Search text "{search_text}" exists in `cephadm-ansible` playbook output'
            f"`ceph config get` parameter value : {ceph_config_get_value}\n"
            f"`cephadm-ansible` output : {out} \n"
        )

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1

    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )

        if (param in locals() and param in globals()) and (
            who in locals() and who in globals()
        ):
            rados_obj.remove_configuration(who=who, param=param)

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
