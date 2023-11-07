from cli.exceptions import ConfigError, OperationFailedError, ResourceNotFoundError
from cli.ops.cephadm import set_container_image_config
from cli.ops.cephadm_ansible import autoload_registry_details, exec_cephadm_bootstrap
from cli.utilities.configure import (
    get_tools_repo,
    set_selinux_mode,
    setup_client_node,
    setup_installer_node,
)


def run(ceph_cluster, **kwargs):
    """Verify cephadm bootstrap"""
    # Get config specs
    config, custom_config = (
        kwargs.get("config"),
        kwargs.get("test_data", {}).get("custom-config", {}),
    )

    # Check mandatory parameter bootstrap
    if not config.get("bootstrap"):
        raise ConfigError("Mandatory config 'bootstrap' not provided")

    # Get cluster config
    installer, nodes = (
        ceph_cluster.get_nodes(role="installer")[0],
        ceph_cluster.get_nodes(),
    )

    # Get build details
    rhbuild, build_type, ibm_build, tools_repo, image = (
        config.get("rhbuild"),
        config.get("build_type"),
        config.get("ibm_build", False),
        config.get("base_url"),
        config.get("container_image"),
    )

    # Get tools repo url
    tools_repo = get_tools_repo(tools_repo, ibm_build)

    # Set ansible preflight tag
    ansible_preflight = True

    # Configure installer node
    setup_installer_node(
        installer, nodes, rhbuild, tools_repo, build_type, ibm_build, ansible_preflight
    )

    # Get bootstrap config
    bootstrap_config = config.get("bootstrap")

    # Get bootstrap module configs
    playbook = bootstrap_config.get("playbook")
    module_args = bootstrap_config.get("module_args")
    module_args.update({"image": image})

    # Set selinux mode
    if bootstrap_config.get("set_selinux"):
        if not set_selinux_mode(nodes, bootstrap_config.get("set_selinux")):
            raise OperationFailedError("Failed to set selinux mode")

    # Get registry details
    if bootstrap_config.get("autoload_registry_details") or ibm_build:
        module_args.update(autoload_registry_details(ibm_build))

    # Execute cephadm bootstrap playbook
    exec_cephadm_bootstrap(installer, nodes, playbook, **module_args)

    # Set container configs
    set_container_image_config(installer, custom_config)

    # Get client configuration
    client_config = config.get("client")
    if not client_config:
        return 0

    # Get client node
    client = ceph_cluster.get_nodes("client")
    if not client:
        raise ResourceNotFoundError("No client node available")

    # Get ansible clients tag
    ansible_clients = client_config.get("ansible_clients")
    if ansible_clients and not ansible_preflight:
        raise ConfigError("Execute ansible preflight to use clients playbook")

    # Setup client node
    setup_client_node(installer, ansible_clients)

    return 0
