import tempfile

from ceph.ceph_admin import CephAdmin
from utility.log import Log

log = Log(__name__)


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
    """
    Add node to the cluster

    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data

    - Perform all pre requisites necessary to add a node with role pool to the cluster

        Example::
            config:
                command: add_node
                base_cmd_args:
                    verbose: true
                args:
                    role: <roles to be considered>
                    ssh-user: <user>
                    ssh-public-key: <full path to the ssh public key file>
                    ssh-private-key: <full path to the ssh private key file>
    """
    config = kw.get("config")

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")
    log.info("Executing %s" % command)

    instance = CephAdmin(cluster=ceph_cluster, **config)
    log.info("Fetching pub key file")
    pub_key_file = create_cephadm_pub_key_file(instance)
    config["args"]["ssh-public-key"] = pub_key_file
    log.info("Fetching nodes")
    args = config["args"]
    nodes = []
    if args.get("role"):
        nodes = instance.cluster.get_nodes(role=args["role"])

    log.info("Distributing ceph pub key")

    instance.distribute_cephadm_gen_pub_key(
        ssh_key_path=args.get("output-pub-ssh-key") or args.get("ssh-public-key"),
        nodes=nodes,
    )
    log.info("pub key distribution success")
    return 0
