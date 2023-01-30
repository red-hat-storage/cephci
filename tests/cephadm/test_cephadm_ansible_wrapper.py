import os

from cli.cephadm.ansible import Ansible
from cli.cephadm.cephadm import CephAdm
from cli.cephadm.exceptions import CephadmOpsExecutionError, ConfigNotFoundError
from cli.utilities.packages import Rpm, RpmError
from cli.utilities.utils import get_node_ip, put_cephadm_ansible_playbook
from utility.install_prereq import ConfigureCephadmAnsibleNode, ExecutePreflightPlaybook


def validate_configs(config):
    aw = config.get("ansible_wrapper")
    if not aw:
        raise ConfigNotFoundError("Mandatory parameter 'ansible_wrapper' not found")

    playbook = aw.get("playbook")
    if not playbook:
        raise ConfigNotFoundError("Mandatory resource 'playbook' not found")

    mon_node = aw.get("module_args", {}).get("mon_node")
    module = aw.get("module")
    if module == "cephadm_bootstrap" and not mon_node:
        raise ConfigNotFoundError(
            "'cephadm_bootstrap' module requires 'mon_node' parameter"
        )


def setup_cluster(ceph_cluster, config):
    installer = ceph_cluster.get_ceph_object("installer")
    nodes = ceph_cluster.get_nodes()

    base_url = config.get("base_url")
    rhbuild = config.get("rhbuild")
    cloud_type = config.get("cloud-type")
    build_type = config.get("build_type")

    try:
        Rpm(installer).query("cephadm-ansible")
    except RpmError:
        ceph_cluster.setup_ssh_keys()
        ConfigureCephadmAnsibleNode.run(
            installer, nodes, build_type, base_url, rhbuild, cloud_type
        )
        ExecutePreflightPlaybook.run(installer, base_url, cloud_type, build_type)


def validate_cephadm_ansible_module(installer, playbook, extra_vars, extra_args):
    put_cephadm_ansible_playbook(installer, playbook)
    Ansible(installer).run_playbook(
        playbook=os.path.basename(playbook),
        extra_vars=extra_vars,
        extra_args=extra_args,
    )

    if not CephAdm(installer).ceph.status():
        raise CephadmOpsExecutionError("Failed to bootstrap cluster")


def run(ceph_cluster, **kwargs):
    """Module to execute cephadm-ansible wrapper playbooks

    Example:
        - test:
            name: Bootstrap cluster using cephadm-ansible playbook
            desc: Execute playbooks/bootstrap-cluster.yaml
            config:
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
    config = kwargs.get("config")

    validate_configs(config)
    setup_cluster(ceph_cluster, config)

    aw = config.get("ansible_wrapper")

    extra_vars, extra_args = aw.get("extra_vars", {}), aw.get("extra_args")
    module = aw.get("module")
    mon_node = aw.get("module_args", {}).get("mon_node")

    if module == "cephadm_bootstrap" and mon_node:
        nodes = ceph_cluster.get_nodes()
        extra_vars["mon_ip"] = get_node_ip(nodes, mon_node)
        if config.get("build_type") not in ["ga", "ga-async"]:
            extra_vars["image"] = config.get("container_image")

    installer = ceph_cluster.get_ceph_object("installer")
    playbook = aw.get("playbook")
    validate_cephadm_ansible_module(installer, playbook, extra_vars, extra_args)

    return 0
