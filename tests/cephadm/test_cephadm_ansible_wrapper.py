import os

from cli.cephadm.ansible import Ansible
from cli.cephadm.exceptions import ConfigNotFoundError
from cli.utilities.packages import Package, Rpm
from cli.utilities.utils import (
    get_node_by_id,
    get_node_ip,
    put_cephadm_ansible_playbook,
    set_selinux_mode,
)
from utility.install_prereq import (
    ConfigureCephadmAnsibleNode,
    CopyCephSshKeyToHost,
    ExecutePreflightPlaybook,
    SetUpSSHKeys,
)
from utility.utils import get_cephci_config


class ClusterConfigurationFailure(Exception):
    pass


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
    daemon_state = module_args.get("state")
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

    elif module == "ceph_orch_daemon" and (
        not daemon_id or not daemon_type or not daemon_state
    ):
        raise ConfigNotFoundError(
            "'ceph_orch_daemon' module requires 'daemon_id' and 'daemon_type' and 'daemon_state' parameter"
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
    ibm_build = config.get("ibm_build")

    if not Rpm(installer).query("cephadm-ansible"):
        SetUpSSHKeys.run(installer, nodes)
        ConfigureCephadmAnsibleNode.run(
            installer, nodes, build_type, base_url, rhbuild, cloud_type, ibm_build
        )
        ExecutePreflightPlaybook.run(
            installer, base_url, cloud_type, build_type, ibm_build
        )


def validate_cephadm_ansible_module(installer, playbook, extra_vars, extra_args):
    put_cephadm_ansible_playbook(installer, playbook)
    Ansible(installer).run_playbook(
        playbook=os.path.basename(playbook),
        extra_vars=extra_vars,
        extra_args=extra_args,
    )


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

    # Check if selinux mode has to be changed
    selinux_mode = module_args.get("selinux")
    if selinux_mode:
        if not set_selinux_mode(nodes, selinux_mode):
            raise ClusterConfigurationFailure("Failed to set Selinux to specified mode")

    # Check if the scenario includes docker
    if module_args.get("docker"):
        extra_vars["docker"] = "true"
        if not Rpm(installer).query("docker"):
            Package(nodes).install("docker", nogpgcheck=True)

    if module == "cephadm_bootstrap":
        extra_vars["mon_ip"] = get_node_ip(nodes, module_args.get("mon_node"))
        if config.get("build_type") not in ["ga", "ga-async"]:
            extra_vars["image"] = config.get("container_image")

        # Checking if the bootstrap has to be done using given registry details
        if module_args.get("registry-url"):
            extra_vars["registry_url"] = module_args.get("registry-url")

            # Fetch creds from the cephci config
            _config = get_cephci_config()["cdn_credentials"]
            extra_vars["registry_username"] = _config["username"]
            extra_vars["registry_password"] = _config["password"]

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
        extra_vars["daemon_state"] = module_args.get("state")

    elif module == "cephadm_registry_login":
        extra_vars["registry_url"] = module_args.get("registry-url")

        # Fetch registry details from cephci config
        _config = get_cephci_config()["cdn_credentials"]
        extra_vars["registry_username"] = _config["username"]
        extra_vars["registry_password"] = _config["password"]

    playbook = aw.get("playbook")
    validate_cephadm_ansible_module(installer, playbook, extra_vars, extra_args)

    return 0
