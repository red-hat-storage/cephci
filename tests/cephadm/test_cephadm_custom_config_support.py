from cli.cephadm.cephadm import CephAdm
from cli.exceptions import CephadmOpsExecutionError
from cli.ops.cephadm import generate_bootstrap_config
from cli.utilities.containers import Container


def check_custom_file(node, config):
    """Check custom file updated to conatiner"""
    for spec in config.get("spec"):
        service = spec.get("service_name")
        container = Container(node).ps(
            filter={"name": service}, format='"{{.ID}} {{.IMAGE}}"'
        )[0]
        mount_path = spec.get("custom_configs")[1].get("mount_path")
        result = Container(node).exec(
            container=container, interactive=True, tty=True, cmds=f"cat {mount_path}"
        )[0]
        if result != spec.get("custom_configs")[1].get("content"):
            raise CephadmOpsExecutionError(
                "{service} container file content not updated"
            )


def run(ceph_cluster, **kwargs):
    """Verify cephadm custom config file support"""
    # Get config specs
    config = kwargs.get("config")

    # Get cluster installer node
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Generate custom config file
    config_file = generate_bootstrap_config(installer, config)

    # Mount custom config file
    conf = {"pos_args": []}
    out = CephAdm(installer, config_file).ceph.orch.apply(input=config_file, **conf)
    if "Scheduled" not in out:
        raise CephadmOpsExecutionError("Fail to mount custom config file")

    # Redeploy services
    for spec in config.get("spec"):
        service = spec.get("service_name")
        out = CephAdm(installer).ceph.orch.redeploy(service)
        if "Scheduled" not in out:
            raise CephadmOpsExecutionError("Fail to redeploy {service}")

    # Validate custom file mounted in containers
    check_custom_file(installer, config)

    return 0
