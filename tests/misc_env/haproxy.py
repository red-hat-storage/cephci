"""
This module contains the workflows for installing and configuring HAProxy

Sample test script

    - test:
        abort-on-fail: true
        config:
          haproxy_client:
            - node6
          rgw_endpoints:
              - node6:<port>
              - node7:<port>
              - node7:<port2>
        desc: Configure HAproxy
        module: haproxy.py
        name: Configure HAproxy
"""
from typing import List

from jinja2 import Template

from ceph.ceph import Ceph, CephNode, CommandFailed
from ceph.utils import get_nodes_by_ids
from utility.log import Log

LOG = Log(__name__)

HAPRX_CONF = """
global
    log         127.0.0.1 local2
    chroot      /var/lib/haproxy
    pidfile     /var/run/haproxy.pid
    maxconn     4000
    user        haproxy
    group       haproxy
    daemon
    stats socket /var/lib/haproxy/stats
    ssl-default-bind-ciphers PROFILE=SYSTEM
    ssl-default-server-ciphers PROFILE=SYSTEM
defaults
    mode                    http
    log                     global
    option                  httplog
    option                  dontlognull
    option http-server-close
    option forwardfor       except 127.0.0.0/8
    option                  redispatch
    retries                 3
    timeout http-request    10s
    timeout queue           1m
    timeout connect         10s
    timeout client          1m
    timeout server          1m
    timeout http-keep-alive 10s
    timeout check           10s
    maxconn                 3000
frontend main
    bind *:5000
    acl url_static       path_beg       -i /static /images /javascript /stylesheets
    acl url_static       path_end       -i .jpg .gif .png .css .js
    use_backend static          if url_static
    default_backend             app
backend static
    balance     roundrobin
{% for item in data %}
    server      static {{item}} check
{%- endfor %}
backend app
    balance     roundrobin
{% for item in data %}
    server      app {{item}} check
{%- endfor %}
"""


def install(node: CephNode) -> None:
    """
    Installs  HAproxy.

    Args:
        nodes (list):   The list of nodes on which the packages are installed.

    Returns:
        None

    Raises:
        CommandFailed
    """
    node.exec_command(sudo=True, cmd="yum install -y haproxy")
    node.exec_command(sudo=True, cmd="chmod 666 /etc/haproxy/haproxy.cfg")
    LOG.info("Successfully installed HAproxy!!!")


def restart_service(node: CephNode) -> None:
    """
    restart  HAproxy service.

    Args:
        node : The node on which service need to restart.

    Returns:
        None

    Raises:
        CommandFailed
    """
    node.exec_command(sudo=True, cmd="systemctl restart haproxy")
    LOG.info("HAproxy Service restarted!!!")


def config(node: CephNode, data: List) -> None:
    """
    Writes the haproxy.cfg configuration file.

    Args:
        node:   Haproxy client node
        data:   A list having rgw endpoints(ip_address & port)
    Returns:
        None
    Raises:
        CommandFailed
    """
    LOG.info("Generating the HAproxy.cfg file.")
    templ = Template(HAPRX_CONF)
    conf = templ.render(data=data)

    conf_file = node.remote_file(file_name="/etc/haproxy/haproxy.cfg", file_mode="w")
    conf_file.write(conf)
    conf_file.flush()


def enable_ports(node: CephNode, port: int = 5000) -> None:
    """
    Opens the required firewall ports.

    Args:
        node (CephNode):    The list of nodes for which the port has to be opened.
        port (int):         The network port that needs to be opened

    Returns:
        None

    Raises:
        CommandFailed
    """
    LOG.debug("Opening the required network ports if firewall is configured.")

    try:
        out, err = node.exec_command(sudo=True, cmd="firewall-cmd --state")
        if out.lower() != "running":
            return
    except CommandFailed:
        LOG.debug(f"{node.shortname} has no firewall configuration.")
        return

    node.exec_command(
        sudo=True, cmd=f"firewall-cmd --zone public --permanent --port {port}/tcp"
    )


def generate_endpoint_list(ceph_cluster: Ceph, endpoints):
    """Generates the list of endpoints.

    Returns:
         list holding the ip address and port
    """
    LOG.debug("Get endpoints")
    rgw_endpoints = []
    for endpt in endpoints:
        host = []
        if ":" in endpt:
            host.append(endpt.split(":")[0])
            hostname = get_nodes_by_ids(ceph_cluster, host)
            rgw_node_ip = hostname[0].ip_address
            port = endpt.split(":")[1]
        else:
            host.append(endpt)
            hostname = get_nodes_by_ids(ceph_cluster, host)
            rgw_node_ip = hostname[0].ip_address
            port = 80
        endpoint = str(rgw_node_ip) + ":" + str(port)
        rgw_endpoints.append(endpoint)
    return rgw_endpoints


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """
    Entry point to this module that configures haproxy.
    """
    LOG.info("Configuring HAProxy")
    haproxy_clients = get_nodes_by_ids(
        ceph_cluster, kwargs["config"]["haproxy_clients"]
    )

    try:
        rgw_endpoints = generate_endpoint_list(
            ceph_cluster, kwargs["config"]["rgw_endpoints"]
        )
        for hprx in haproxy_clients:
            install(hprx)
            enable_ports(hprx, port=5000)
            config(hprx, rgw_endpoints)
            restart_service(hprx)

    except BaseException as be:  # noqa
        LOG.error(be)
        return 1

    LOG.info("Successfully Configured HAProxy!!!")
    return 0
