import re
import tempfile

from ceph.ceph_admin import CephAdmin
from ceph.waiter import WaitUntil
from cephci.utils.configure import add_images_to_private_registry
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import NotSupportedError, OperationFailedError
from cli.utilities.configs import get_registry_details
from cli.utilities.configure import setup_ssh_keys
from cli.utilities.containers import Registry
from cli.utilities.packages import Package, SubscriptionManager
from utility.log import Log

log = Log(__name__)
PRIVATE_REG_USER = "myregistryusername"
PRIVATE_REG_PASS = "myregistrypassword"
PORT = "5000"


def create_cephadm_pub_key_file(cls):
    """
    Creates the cephadm pub key file for the cluster
    Args:
        cls: cephadm instance object

    Returns:
        str: filename of the key file generated
    """
    ceph_pub_key, _ = cls.shell(args=["ceph", "cephadm", "get-pub-key"])
    node = cls.installer
    temp_file = tempfile.NamedTemporaryFile(suffix=".pub")
    pub_key_file = node.node.remote_file(
        sudo=True, file_name=temp_file.name, file_mode="w"
    )
    pub_key_file.write(ceph_pub_key)
    pub_key_file.flush()

    return temp_file.name


def run(ceph_cluster, **kw):
    """Verify re-deploying of monitoring stack with custom images
    Args:
        ceph_cluster(ceph.ceph.Ceph): CephNode or list of CephNode object
    """

    # Get config file and node
    config = kw.get("config")
    nodes = ceph_cluster.get_nodes()
    installer = ceph_cluster.get_nodes(role="installer")[0]
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

    # Upgrade cephadm and cephadm-ansible
    SubscriptionManager(installer).repos.enable(rhceph_repo)

    Package(installer).upgrade("cephadm-ansible")
    Package(installer).upgrade("cephadm")

    # Get registry credentials
    reg_details = get_registry_details()
    registry = reg_details["registry-url"]
    reg_username = reg_details["registry-username"]
    reg_password = reg_details["registry-password"]

    # Add upgrade image to private registry
    if not add_images_to_private_registry(
        node=installer,
        src_username=reg_username,
        src_password=reg_password,
        username=PRIVATE_REG_USER,
        password=PRIVATE_REG_PASS,
        registry=registry,
        build_type=rhbuild,
        images=[bootstrap_image],
    ):
        return False

    # Create a node list excluding installer and client node
    _nodes = [
        node
        for node in ceph_cluster.get_nodes()
        if node.role not in ["installer", "client"]
    ]

    # Copy cluster’s public SSH keys to the root user’s authorized_keys file on the new hosts
    instance = CephAdmin(cluster=ceph_cluster, **config)
    log.info("Fetching pub key file")
    pub_key_file = create_cephadm_pub_key_file(instance)
    log.info("Distributing ceph pub key")
    instance.distribute_cephadm_gen_pub_key(
        ssh_key_path=pub_key_file,
        nodes=_nodes,
    )

    # Add hosts to cluster
    for _node in _nodes:
        out = CephAdm(installer).ceph.orch.host.add(
            hostname=_node.hostname, ip_address=_node.ip_address, label="mgr mon"
        )

    # Setup ssh key to all new nodes
    for _node in _nodes:
        setup_ssh_keys(_node, nodes)

    # Login to the registry using podman command on all the new nodes
    private_registry_url = f"{installer.hostname}:5000"
    for _node in _nodes:
        Registry(_node).login(private_registry_url, PRIVATE_REG_USER, PRIVATE_REG_PASS)

    # Redeploy the MGR service
    conf = {"pos_args": [], "placement": "2"}
    out = CephAdm(installer).ceph.orch.apply(service_name="mgr", **conf)
    if "Scheduled mgr update" not in out:
        raise OperationFailedError("Apply mgr placement failed")

    # Redeploy the Mon service
    conf = {"pos_args": [], "placement": "3"}
    out = CephAdm(installer).ceph.orch.apply(service_name="mon", **conf)
    if "Scheduled mon update" not in out:
        raise OperationFailedError("Fail to redeploy Mon daemon")

    # Wait for active and standby mgr to be up
    timeout, interval = 60, 5
    for w in WaitUntil(timeout=timeout, interval=interval):
        out = CephAdm(installer).ceph.status()
        match = re.search(r"mgr: (.+?)\((.+?)\), standbys: (.+?)$", out, re.MULTILINE)
        if match:
            break
    if w.expired:
        raise OperationFailedError("Not enough mgr avaiable in cluster")

    # Check image for upgrade
    image = f"{installer.hostname}:{PORT}/{bootstrap_image}"
    kw = {"image": f"{image}"}
    out = CephAdm(installer).ceph.orch.upgrade.check(**kw)

    # Perform Upgrade
    CephAdm(installer).ceph.orch.upgrade.start(**kw)

    return 0
