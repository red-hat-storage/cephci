from cephci.prereq import setup_private_container_registry
from cephci.utils.configure import exec_cephadm_preflight, setup_ssh_keys
from cli.exceptions import NotSupportedError
from cli.ops.cephadm import bootstrap
from cli.utilities.configs import get_registry_details
from cli.utilities.packages import Package, SubscriptionManager

PRIVATE_REG_USER = "myregistryusername"
PRIVATE_REG_PASS = "myregistrypassword"
PORT = "5000"


def run(ceph_cluster, **kw):
    """Verify re-deploying of monitoring stack with custom images
    Args:
        ceph_cluster(ceph.ceph.Ceph): CephNode or list of CephNode object
    """

    # Get config file and node
    config = kw.get("config")
    installer = ceph_cluster.get_nodes(role="installer")[0]
    mon_node = ceph_cluster.get_nodes(role="mon")[0]
    bootstrap_node = config.get("bootstrap_node")
    nodes = ceph_cluster.get_nodes()

    rhbuild = config.get("rhbuild")
    bootstrap_image = config.get("bootstrap_image")
    rhceph_repo = config.get("rhceph_repo")

    # Get the rhbuild
    if rhbuild.startswith("8"):
        rhbuild = "squid"
    elif rhbuild.startswith("7"):
        rhbuild = "reef"
    elif rhbuild.startswith("6"):
        rhbuild = "quincy"
    elif rhbuild.startswith("5"):
        rhbuild = "pacific"
    else:
        raise NotSupportedError(f"Unsupported version: {rhbuild}")

    # Install cephadm and cephadm-ansible
    SubscriptionManager(installer).repos.enable(rhceph_repo)

    Package(installer).install("cephadm-ansible", nogpgcheck=False)
    Package(installer).install("cephadm", nogpgcheck=False)

    # Execute cephadm preflight
    exec_cephadm_preflight(installer=installer, build_type="rh")

    # Get registry credentials
    reg_details = get_registry_details()
    registry = reg_details["registry-url"]
    reg_username = reg_details["registry-username"]
    reg_password = reg_details["registry-password"]

    # Get installer node
    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Get cluster nodes
    ceph_nodes = kw.get("ceph_nodes")

    # Get registry docker image
    docker_reg_image = config.get("registry_image").get("docker_registry_image")

    # Setup ssh key to all nodes
    setup_ssh_keys(installer, ceph_nodes)

    # Set up a private registry on installer node
    setup_private_container_registry(
        installer=installer,
        nodes=ceph_nodes,
        registry=registry,
        reg_username=reg_username,
        reg_password=reg_password,
        build_type=rhbuild,
        private_reg_username=PRIVATE_REG_USER,
        private_reg_password=PRIVATE_REG_PASS,
        docker_reg_image=docker_reg_image,
        images=[bootstrap_image],
    )

    # Perform bootstrap using the private registry created
    registry_url = f"{mon_node.hostname}:{PORT}"
    image = f"{installer.hostname}:{PORT}/{bootstrap_image}"
    kw = {
        "mon-ip": bootstrap_node,
        "registry-url": registry_url,
        "registry-username": PRIVATE_REG_USER,
        "registry-password": PRIVATE_REG_PASS,
    }
    bootstrap(installer=installer, nodes=nodes, image=image, **kw)
    return 0
