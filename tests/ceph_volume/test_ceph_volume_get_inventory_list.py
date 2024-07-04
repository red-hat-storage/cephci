from cli.exceptions import OperationFailedError
from cli.utilities.containers import Container


def get_inventory_list(node, output_format):
    # Note: This cmd uses rhceph-5-rhel8:latest, the requirement is just to ensure whether
    # the ceph inventory is able to list the details and any container image is sufficient
    cmd = f"--cluster ceph inventory --format={output_format}"
    volumes = [
        "/run/lock/lvm:/run/lock/lvm:z",
        "/var/run/udev/:/var/run/udev/:z",
        "/dev:/dev",
        "/etc/ceph:/etc/ceph:z",
        "/run/lvm/:/run/lvm/",
        "/var/lib/ceph/:/var/lib/ceph/:z",
        "/var/log/ceph/:/var/log/ceph/:z",
    ]
    out = Container(node).run(
        rm=True,
        privileged=True,
        user="root",
        cmds=cmd,
        entry_point="ceph-volume",
        volume=volumes,
        image="registry.redhat.io/rhceph/rhceph-5-rhel8:latest",
        long_running=False,
    )
    return out


def run(ceph_cluster, **kw):
    """
    Verify ceph-ansible fails to get an inventory
    list with KeyError: 'ceph.cluster_name' in json format
    but not in plain format
    """

    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Non bootstrap osd Node
    non_bs_node = list(set(ceph_cluster.get_nodes(role="osd")) - {installer})[0]

    # Verify whether format as plain is returning an error
    for node in (installer, non_bs_node):
        # Get the version using podman command
        out = get_inventory_list(node, output_format="plain")
        if not out:
            raise OperationFailedError(
                "Not able to list the ceph inventory even without json format"
            )

    # Try to execute the command in both the nodes and make sure no error is seen BZ: #1977888
    for node in (installer, non_bs_node):
        out = get_inventory_list(node, output_format="json")
        if "KeyError: 'ceph.cluster_name'" in out:
            raise OperationFailedError(
                "Not able to list the ceph inventory with json format"
            )
    return 0
