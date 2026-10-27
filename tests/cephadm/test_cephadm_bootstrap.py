from cli.exceptions import ConfigError, ResourceNotFoundError
from cli.ops.cephadm import bootstrap, set_container_image_config
from cli.utilities.configure import (
    get_tools_repo,
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

    # Get ansible preflight tag
    ansible_preflight = config.get("ansible_preflight")

    # Configure installer node
    setup_installer_node(
        installer, nodes, rhbuild, tools_repo, build_type, ibm_build, ansible_preflight
    )

    # Get bootstrap config
    bootstrap_config = config.get("bootstrap")

    # Bootstrap cluster
    bootstrap(
        installer=installer,
        nodes=nodes,
        ibm_build=ibm_build,
        image=image,
        **bootstrap_config
    )

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
