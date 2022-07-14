"""Module that executes bootstraping the storage cluster"""
from ceph.ceph import ResourceNotFoundError

from install_network_node import create_images_dict


def run(ceph_cluster, **kw):
    # Starting cluster deployment
    config = kw.get("config")
    args = config.get("args")
    ceph_nodes = kw.get("ceph_nodes")
    network_node = ceph_nodes.get_ceph_object("network-node").node
    ceph_installer = ceph_cluster.get_ceph_object("installer")
    mon_node = args.pop("mon-ip", ceph_installer.node.shortname)
    if mon_node:
        for node in ceph_nodes:
            # making sure conditions works in all the scenario
            if (
                node.shortname == mon_node
                or node.shortname.endswith(mon_node)
                or f"{mon_node}-" in node.shortname
            ):
                ceph_installer.exec_command(
                    sudo=True,
                    cmd=f"cephadm --image {network_node.hostname}:5000/testm/testm1\
                                            bootstrap --mon-ip {node.ip_address} \
                                            --registry-url {network_node.hostname}:5000\
                                            --registry-username myregistryusername \
                                            --registry-password myregistrypassword1",
                )
                break
        else:
            raise ResourceNotFoundError(f"Unknown {mon_node} node name.")

    # Changing configurations of custom container images
    image_dict, image = create_images_dict(config)
    for key, value in image_dict.items():
        cmd = "cephadm shell --"
        cmd += f" ceph config set mgr mgr/cephadm/container_image_{key}"
        cmd += f" {network_node.hostname}:5000/{value}"
        ceph_installer.exec_command(sudo=True, cmd=cmd)
    ceph_installer.exec_command(sudo=True, cmd="ceph orch redeploy node-exporter")
    return 0
