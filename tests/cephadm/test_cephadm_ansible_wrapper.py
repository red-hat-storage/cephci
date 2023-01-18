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

    module_args = aw.get("module_args", {})
    module = aw.get("module")

    mon_node = module_args.get("mon_node")
    daemon_id = module_args.get("daemon_id")
    daemon_type = module_args.get("daemon_type")
    node = module_args.get("host")
    label = module_args.get("label")

    if module == "cephadm_bootstrap" and not mon_node:
        raise ConfigNotFoundError(
            "'cephadm_bootstrap' module requires 'mon_node' parameter"
        )

    elif module == "ceph_orch_apply" and not node and not label:
        raise ConfigNotFoundError(
            f"'{module}' module requires 'host' and 'label' parameter"
        )

    elif module == "ceph_orch_daemon" and (not daemon_id or not daemon_type):
        raise ConfigNotFoundError(
            "'ceph_orch_daemon' module requires 'daemon_id' and 'daemon_type' parameter"
        )

    elif module == "ceph_orch_host" and not node:
        raise ConfigNotFoundError("'ceph_orch_host' module requires 'host' parameter")


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

    elif module == "ceph_orch_apply":
        extra_vars["label"] = module_args.get("label")

    elif module == "ceph_orch_host":
        node = module_args.get("host")
        state = module_args.get("state")
        label = module_args.get("label")
        extra_vars["ip_address"] = get_node_ip(nodes, node)
        node = get_node_by_id(nodes, node)
        extra_vars["node"] = node.hostname
        if state:
            extra_vars["state"] = state
        if label:
            extra_vars["label"] = label

        # NOTE: This logic has to be revisited to work when state is present
        # and also when the state is not passed
        CopyCephSshKeyToHost.run(installer, node)

    elif module == "ceph_orch_daemon":
        extra_vars["daemon_id"] = module_args.get("daemon_id")
        extra_vars["daemon_type"] = module_args.get("daemon_type")

    playbook = aw.get("playbook")
    validate_cephadm_ansible_module(installer, playbook, extra_vars, extra_args)

    return 0
