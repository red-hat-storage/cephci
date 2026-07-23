"""
Deploy HAProxy ingress in SSL passthrough mode with an OpenStack floating IP as VIP.

Allocates a floating IP from OpenStack so the VIP is routable from all cluster
nodes (OpenStack port-security blocks keepalived VIPs that aren't registered).
Writes the VIP to /tmp/ingress_vip on the installer node for downstream tests.

Suite config example::

    - test:
        module: test_ssl_ingress_deploy.py
        config:
          backend_service: rgw.rgw.ssl
          frontend_port: 443
          monitor_port: 1967
          use_tcp_mode_over_rgw: true
"""

import json
from time import sleep

import yaml

from compute.openstack import get_openstack_driver
from utility.log import Log

log = Log(__name__)


def _get_openstack_params(config):
    """Extract OpenStack driver params from the osp_cred injected by run.py."""
    osp_cred = config.get("osp_cred")
    if not osp_cred:
        raise ValueError("osp_cred not found in config; pass --osp-cred to run.py")
    os_cred = osp_cred.get("globals", {}).get("openstack-credentials", {})
    return {
        "username": os_cred["username"],
        "password": os_cred["password"],
        "auth_url": os_cred["auth-url"],
        "auth_version": os_cred["auth-version"],
        "tenant_name": os_cred["tenant-name"],
        "service_region": os_cred["service-region"],
        "domain_name": os_cred["domain"],
        "tenant_domain_id": os_cred["tenant-domain-id"],
    }


def _allocate_floating_ip(driver, ingress_node):
    """Allocate an OpenStack floating IP on the ingress node's network."""
    for node_obj in driver.list_nodes():
        if ingress_node.private_ip in node_obj.private_ips:
            network_pool = list(node_obj.extra.get("addresses").keys())
            floating_ip = driver.ex_create_floating_ip(network_pool[0])
            log.info(f"Allocated floating IP: {floating_ip.ip_address}")
            return floating_ip
    raise RuntimeError(
        f"Could not find OpenStack node for ingress IP {ingress_node.private_ip}"
    )


def run(ceph_cluster, **kw):
    config = kw.get("config", {})
    cloud_type = config.get("cloud-type", "openstack")

    if cloud_type != "openstack":
        log.error(f"Floating IP allocation not supported for cloud type: {cloud_type}")
        return 1

    installer = ceph_cluster.get_ceph_object("installer")
    ingress_obj = ceph_cluster.get_ceph_object("ingress")
    if not ingress_obj:
        log.error("No node with 'ingress' role found in cluster config")
        return 1
    ingress_node = ingress_obj.node

    os_params = _get_openstack_params(config)
    driver = get_openstack_driver(**os_params)
    floating_ip = None

    try:
        floating_ip = _allocate_floating_ip(driver, ingress_node)
        vip = floating_ip.ip_address

        backend_service = config.get("backend_service", "rgw.rgw.ssl")
        frontend_port = config.get("frontend_port", 443)
        monitor_port = config.get("monitor_port", 1967)
        use_tcp = config.get("use_tcp_mode_over_rgw", True)

        spec = {
            "service_type": "ingress",
            "service_id": backend_service,
            "placement": {"hosts": [ingress_node.shortname]},
            "spec": {
                "backend_service": backend_service,
                "virtual_ip": f"{vip}/32",
                "frontend_port": frontend_port,
                "monitor_port": monitor_port,
                "use_tcp_mode_over_rgw": use_tcp,
            },
        }

        spec_yaml = yaml.dump(spec, default_flow_style=False)
        log.info(f"Ingress spec:\n{spec_yaml}")

        spec_file = "/tmp/ingress_spec.yaml"
        remote_fp = installer.remote_file(file_name=spec_file, file_mode="w", sudo=True)
        remote_fp.write(spec_yaml)
        remote_fp.flush()

        installer.exec_command(
            sudo=True,
            cmd=f"cephadm shell --mount /tmp:/tmp -- ceph orch apply -i {spec_file}",
        )
        log.info("Ingress spec applied, waiting 60s for daemons to start")
        sleep(60)

        out, _ = installer.exec_command(
            sudo=True,
            cmd="cephadm shell -- ceph orch ls ingress --format json",
        )
        svc_list = json.loads(out)
        if not svc_list:
            log.error("No ingress service found after apply")
            return 1

        svc = svc_list[0]
        running = svc.get("status", {}).get("running", 0)
        size = svc.get("status", {}).get("size", 0)
        log.info(f"Ingress service: {running}/{size} running, VIP={vip}")

        if running < size:
            log.warning(f"Ingress not fully up: {running}/{size}")

        ingress_ip = str(ingress_node.ip_address)
        ingress_node.exec_command(
            sudo=True,
            cmd=(
                f"nft add table ip nat;"
                f" nft add chain ip nat prerouting"
                f" '{{ type nat hook prerouting priority -100; policy accept; }}';"
                f" nft add rule ip nat prerouting"
                f" ip daddr {ingress_ip} tcp dport {frontend_port}"
                f" dnat to {vip}:{frontend_port}"
            ),
        )
        log.info(
            f"DNAT rule added (nftables): {ingress_ip}:{frontend_port} -> {vip}:{frontend_port}"
        )

        installer.exec_command(cmd=f"echo '{ingress_ip}' > /tmp/ingress_vip")
        log.info(f"Ingress endpoint saved to /tmp/ingress_vip: {ingress_ip}")

        return 0

    except Exception:
        log.exception("Failed to deploy ingress with floating IP")
        if floating_ip:
            try:
                driver.ex_delete_floating_ip(floating_ip)
                log.info("Cleaned up floating IP after failure")
            except Exception:
                log.warning("Failed to clean up floating IP")
        return 1
