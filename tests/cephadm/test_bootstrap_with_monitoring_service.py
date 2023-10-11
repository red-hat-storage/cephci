import tempfile

import yaml

from utility.log import Log

log = Log(__name__)


def generate_monitoring_spec(node, specs):
    """
    Generate monitoring stack specs
    Args:
        node: node
        specs: monitoring stack specs
    """

    log.info(f"Spec conf file content:\n{specs}")

    temp_file = tempfile.NamedTemporaryFile(suffix=".conf")
    spec_file = node.remote_file(sudo=True, file_name=temp_file.name, file_mode="wb")
    spec = yaml.dump(specs, sort_keys=False, indent=2).encode("utf-8")
    spec_file.write(spec)
    spec_file.flush()
    return temp_file.name


def run(ceph_cluster, **kwargs):
    """Bootstrap the cluster with monitoring stack images
    Args:
        ceph_cluster(ceph.ceph.Ceph): CephNode or list of CephNode object
    """

    # Get installer node
    install_node = ceph_cluster.get_nodes(role="installer")[0]
    image = kwargs.get("image")

    # Get config specs
    config = kwargs.get("config")
    monitoring_config = config.get("spec")

    # Convert specs to file
    file = generate_monitoring_spec(install_node, monitoring_config)

    # Use file to boostrap the cluster
    cmd = f"cephadm --image bootstrap {image} --mon-ip 10.0.211.239 --registry-url registry.redhat.io --registry-username qa@redhat.com --registry-password MTQj5t3n5K86p3gH --config {file}"
    install_node.exec_command(sudo=True, cmd=cmd)
