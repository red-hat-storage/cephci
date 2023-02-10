import os

from cli.cephadm.ansible import Ansible
from cli.cephadm.cephadm import CephAdm
from cli.cephadm.exceptions import CephadmOpsExecutionError, ConfigNotFoundError
from cli.utilities.packages import Rpm, RpmError
from cli.utilities.utils import (
    get_node_by_id,
    get_node_ip,
    put_cephadm_ansible_playbook,
)
from utility.install_prereq import (
    ConfigureCephadmAnsibleNode,
    CopyCephSshKeyToHost,
    ExecutePreflightPlaybook,
    SetUpSSHKeys,
)


def validate_configs(config):
    aw = config.get("ansible_wrapper")
    if not aw:
        raise ConfigNotFoundError("Mandatory parameter 'ansible_wrapper' not found")

    playbook = aw.get("playbook")
    if not playbook:
        raise ConfigNotFoundError("Mandatory resource 'playbook' not found")

    module = aw.get("module")
    module_args = aw.get("module_args", {})

    mon_node = module_args.get("mon_node")
    osd_node = module_args.get("osd_node")
    label = module_args.get("label")
    if module == "cephadm_bootstrap" and not mon_node:
        raise ConfigNotFoundError(
            "'cephadm_bootstrap' module requires 'mon_node' parameter"
        )

    elif module in ["ceph_orch_host", "ceph_orch_apply"] and not osd_node and not label:
        raise ConfigNotFoundError(
            f"'{module}' module requires 'osd_node' and 'label' parameter"
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
        SetUpSSHKeys.run(installer, nodes)
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
    """
    config = kwargs.get("config")

    validate_configs(config)
    setup_cluster(ceph_cluster, config)

    nodes = ceph_cluster.get_nodes()
    installer = ceph_cluster.get_ceph_object("installer")
    aw = config.get("ansible_wrapper")

    extra_vars, extra_args = aw.get("extra_vars", {}), aw.get("extra_args")
    module = aw.get("module")
    module_args = aw.get("module_args", {})

    if module == "cephadm_bootstrap":
        extra_vars["mon_ip"] = get_node_ip(nodes, module_args.get("mon_node"))
        if config.get("build_type") not in ["ga", "ga-async"]:
            extra_vars["image"] = config.get("container_image")

    elif module in ["ceph_orch_host", "ceph_orch_apply"]:
        osd_node = module_args.get("osd_node")
        osd_node = get_node_by_id(nodes, osd_node)
        extra_vars["label"] = module_args.get("label")

        if module == "ceph_orch_host":
            extra_vars["osd_node"] = osd_node.hostname
            extra_vars["ip_address"] = get_node_ip(nodes, osd_node.hostname)
            CopyCephSshKeyToHost.run(installer, osd_node)

    playbook = aw.get("playbook")
    validate_cephadm_ansible_module(installer, playbook, extra_vars, extra_args)

    return 0
