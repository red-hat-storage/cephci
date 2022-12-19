import os

from cli.cephadm.ansible import Ansible
from cli.cephadm.cephadm import CephAdm
from cli.cephadm.exceptions import CephadmOpsExecutionError, ConfigNotFoundError
from cli.utilities.packages import Package, Rpm, RpmError
from cli.utilities.utils import get_node_ip, put_cephadm_ansible_playbook
from utility.install_prereq import (
    ConfigureCephadmAnsibleInventory,
    EnableToolsRepositories,
)
from utility.log import Log

log = Log(__name__)

CEPHADM_PREFLIGHT_PLAYBOOK = "cephadm-preflight.yml"
CEPHADM_PREFLIGHT_VARS = {"ceph_origin": "rhcs"}


def run(ceph_cluster, **kwargs):
    """Module to execute cephadm-ansible wrapper playbooks

    Example:
        - test:
            name: Bootstrap cluster using cephadm-ansible playbook
            desc: Execute playbooks/bootstrap-cluster.yaml
            config:
                args:
                    custom_repo: "cdn"
                    registry-url: registry.redhat.io
                    mon_ip: node1
                ansible_wrapper:
                    module: cephadm_bootstrap
                    playbook: bootstrap-cluster.yaml
                    module_args:
                        mon_node: node1
                    extra_vars:
                        ceph_origin: rhcs
                    extra_args:
                        limit: osds
    """
    nodes = ceph_cluster.get_nodes()
    installer = ceph_cluster.get_ceph_object("installer")

    config = kwargs.get("config")
    aw = config.get("ansible_wrapper")
    if not aw:
        raise ConfigNotFoundError("Mandatory parameter 'ansible_wrapper' not found")

    module = aw.get("module")
    playbook = aw.get("playbook")
    if not playbook:
        raise ConfigNotFoundError("Mandatory resource 'playbook' not found")
    mon_node = aw.get("module_args", {}).get("mon_node")
    if module == "cephadm_bootstrap" and not mon_node:
        raise ConfigNotFoundError(
            "'cephadm_bootstrap' module requires 'mon_node' parameter"
        )

    try:
        Rpm(installer).query("cephadm-ansible")
    except RpmError:
        ceph_cluster.setup_ssh_keys()
        EnableToolsRepositories().run(installer)

        Package(installer).install("cephadm-ansible")

        ConfigureCephadmAnsibleInventory().run(nodes)
        Ansible(installer).run_playbook(
            playbook=CEPHADM_PREFLIGHT_PLAYBOOK,
            extra_vars=CEPHADM_PREFLIGHT_VARS,
        )

    extra_vars = aw.get("extra_vars")
    extra_args = aw.get("extra_args")
    if module == "cephadm_bootstrap" and mon_node:
        if not extra_vars:
            extra_vars = {}
        extra_vars["mon_ip"] = get_node_ip(nodes, mon_node)
    put_cephadm_ansible_playbook(installer, playbook)
    Ansible(installer).run_playbook(
        playbook=os.path.basename(playbook),
        extra_vars=extra_vars,
        extra_args=extra_args,
    )

    if not CephAdm(installer).ceph.status():
        raise CephadmOpsExecutionError("Failed to bootstrap cluster")

    return 0
