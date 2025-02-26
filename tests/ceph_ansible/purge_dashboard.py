"""Module to purge ceph dashboard."""

import json

from utility.log import Log

log = Log(__name__)


class DashboardValidationFailure(Exception):
    pass


def run(ceph_cluster, **kw):
    """Module to purge ceph dashboard.

    Args:
        kw: Arbitrary keyword arguments.
        ceph_cluster (ceph.ceph.Ceph): ceph cluster.

    Returns:
        0 on success and non-zero for failures.
    """
    log.info("Running purge-dashboard test")
    installer_node = ceph_cluster.get_ceph_object("installer")
    ansible_dir = "/usr/share/ceph-ansible"
    inventory = "hosts"
    playbook_command = (
        "infrastructure-playbooks/purge-dashboard.yml --extra-vars 'ireallymeanit=yes'"
    )

    # command to be formed ansible-playbook -vvvv -i hosts infrastructure-playbooks/purge-dashboard.yml

    cmd = f"cd {ansible_dir} ; ansible-playbook -vvvv -i {inventory} {playbook_command}"

    # executing the command to purge dashboard
    rc = installer_node.exec_command(cmd=cmd, long_running=True)

    if rc != 0:
        log.info("ansible-playbook failed to purge ceph dashboard")
        return 1

    log.info("ansible-playbook purge dashboard is successfull")
    mon_node = ceph_cluster.get_ceph_object("mon")
    validate_purge_dashboard(mon_node)
    return 0


def validate_purge_dashboard(node):
    """Method to validate purge dashboard playbook.

    After purging the dashboard module
    validating by no more dashboard service enabled.

    Args:
        node (Ceph object): Mon node to execute ceph commands.
    """
    log.info("Validating purge dashboard")
    out, rc = node.exec_command(sudo=True, cmd="ceph mgr module ls")
    output = json.loads(out.rstrip())
    if "dashboard" in output["enabled_modules"]:
        raise DashboardValidationFailure(
            "Dashboard module is still enabled after purge-dashboard"
        )
    log.info("Dashboard module not enabled as expected")
