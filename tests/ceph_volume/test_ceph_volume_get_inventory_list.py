from cli.exceptions import OperationFailedError


def run(ceph_cluster, **kw):
    """
    Verify ceph-ansible fails to get an inventory
    list with KeyError: 'ceph.cluster_name' in json format
    but not in plain format
    """

    installer = ceph_cluster.get_nodes(role="installer")[0]

    # Non bootstrap osd Node
    non_bs_node = list(set(ceph_cluster.get_nodes(role="osd")) - {installer})[0]

    # Note: This cmd uses rhceph-5-rhel8:latest, the requirement is just to ensure whether
    # the ceph inventory is able to list the details and any container image is sufficient
    cmd = (
        "podman run --rm --privileged --net=host --ipc=host --ulimit nofile=1024:4096 -v "
        "/run/lock/lvm:/run/lock/lvm:z -v /var/run/udev/:/var/run/udev/:z -v /dev:/dev -v /etc/ceph:/etc/ceph:z -v "
        "/run/lvm/:/run/lvm/ -v /var/lib/ceph/:/var/lib/ceph/:z -v /var/log/ceph/:/var/log/ceph/:z "
        "--entrypoint=ceph-volume registry.redhat.io/rhceph/rhceph-5-rhel8:latest --cluster ceph inventory "
        "--format={output_format}"
    )

    # Verify whether format as plain is returning an error
    for node in (installer, non_bs_node):
        out, _ = node.exec_command(cmd=cmd.format(output_format="plain"), sudo=True)
        if not out:
            raise OperationFailedError(
                "Not able to list the ceph inventory even without json format"
            )

    # Try to execute the command in both the nodes and make sure no error is seen BZ: #1977888
    for node in (installer, non_bs_node):
        out, err = node.exec_command(cmd=cmd.format(output_format="json"), sudo=True)
        if err or "KeyError: 'ceph.cluster_name'" in out:
            raise OperationFailedError(
                "Not able to list the ceph inventory with json format"
            )
    return 0
