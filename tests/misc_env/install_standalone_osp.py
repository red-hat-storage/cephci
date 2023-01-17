import ipaddress
import json
import time

from utility.log import Log
from utility.osp_utils import (
    change_osp_project_password,
    configure_cpp,
    configure_standalone_parameters,
    create_os_auth_sh,
    deploy_standalone_osp,
    enable_repos,
    get_clock_server,
    get_dns,
    get_gateway,
    get_ip_and_netmask,
    set_hostname_with_domain,
    set_password_authentication,
    set_permit_root_login,
    setup_subscription_manager,
)
from utility.utils import get_cephci_config

log = Log(__name__)


def update_certs(osp_node):
    log.info("Waiting for cloud config to complete on " + osp_node.hostname)
    osp_node.exec_command(
        sudo=True,
        cmd="while [ ! -f /ceph-qa-ready ]; do sleep 15; done",
        long_running=True,
    )
    osp_node.exec_command(
        sudo=True,
        cmd="curl -m 120 --fail https://password.corp.redhat.com/RH-IT-Root-CA.crt"
        + " -o /etc/pki/ca-trust/source/anchors/RH-IT-Root-CA.crt",
        long_running=True,
    )
    osp_node.exec_command(sudo=True, cmd="update-ca-trust extract", long_running=True)
    osp_node.exec_command(
        sudo=True,
        cmd="curl -m 120 --fail http://magna002.ceph.redhat.com/cephci-jenkins/.cephqe-ca.pem"
        + " -o /etc/pki/ca-trust/source/anchors/cephqe-ca.pem",
        long_running=True,
    )
    osp_node.exec_command(sudo=True, cmd="update-ca-trust extract", long_running=True)
    osp_node.exec_command(sudo=True, cmd="cat /etc/os-release", long_running=True)
    osp_node.exec_command(
        sudo=True,
        cmd="sudo rm -f /etc/yum.repos.d/apache-arrow.repo",
        long_running=True,
    )


def run(**kw):
    log.info("Running test")
    config = kw.get("config")
    osp_version = config.get("osp_version")
    osp_cluster = kw.get("ceph_cluster")
    osp_node = osp_cluster.get_ceph_object("osp").node
    cephci_config = get_cephci_config()
    username = cephci_config["cdn_credentials"]["username"]
    password = cephci_config["cdn_credentials"]["password"]

    # Update certs and Remove apache-arrow.repo for baremetal
    update_certs(osp_node)

    # registering to subscription-manager
    setup_subscription_manager(osp_node, username, password, config)

    # enabling necessary repositories
    enable_repos(osp_node, config)

    # rebooting the node
    osp_node.exec_command(sudo=True, cmd="reboot", check_ec=False)
    log.info("sleeping for 60 seconds to reboot")
    time.sleep(60)
    osp_node.reconnect()
    time.sleep(10)

    # installing python3-tripleoclient
    osp_node.exec_command(
        cmd="sudo dnf install -y python3-tripleoclient", long_running=True
    )

    # configuring containers-prepare-parameters.yaml file
    configure_cpp(osp_node, username, password)

    # prepare environment variables
    dns = get_dns(osp_node).split("\n")
    dns1, dns2 = dns[0], dns[1]
    ip, netmask = get_ip_and_netmask(osp_node).split("\n")[0].split("/")
    vip = str(ipaddress.ip_address(ip) + 1)
    gateway = get_gateway(osp_node).split("\n")[0]
    clock = get_clock_server(osp_node).split("\n")[0]
    clock = "clock.corp.redhat.com"
    env_vars = {
        "dns1": dns1,
        "dns2": dns2,
        "ip": ip,
        "netmask": netmask,
        "vip": vip,
        "gateway": gateway,
        "clock": clock,
    }
    set_env_vars = [
        f"export IP={ip}",
        f"export NETMASK={netmask}",
        "export INTERFACE=eth0",
        f"export DNS1={dns1}",
        f"export DNS2={dns2}",
    ]
    if osp_version.startswith("17"):
        set_env_vars.insert(1, f"export VIP={vip}")

    # configuring standalone_parameters.yaml file
    configure_standalone_parameters(osp_node, env_vars, set_env_vars, config)

    # setting hostname with domain
    set_hostname_with_domain(osp_node, "novalocal")

    # Deploying the all-in-one Red Hat OpenStack Platform environment
    status = deploy_standalone_osp(
        osp_node, osp_version, username, password, set_env_vars
    )

    # permit root login which might have been disabled after openstack installation
    set_permit_root_login(osp_node, "yes")

    # enable password authentication which might have been disabled after openstack installation
    set_password_authentication(osp_node, "yes")

    # restart sshd service to apply the above changes
    osp_node.exec_command(cmd="sudo systemctl restart sshd.service", long_running=True)

    if status == 0:
        log.info(
            f"standalone openstack deployment successful. you can access the dashboard at http://{ip}/dashboard."
        )

        # displaying the endpoints
        out, err = osp_node.exec_command(
            cmd="export OS_CLOUD=standalone; openstack endpoint list"
        )
        log.info(f"listing the endpoints:\n{out}")

        # removing existing swift endpoints
        log.info("removing existing swift endpoints")
        out, _ = osp_node.exec_command(
            cmd="export OS_CLOUD=standalone; openstack endpoint list --format json"
        )
        endpoints = json.loads(out)
        for endpoint in endpoints:
            if endpoint["Service Name"] == "swift":
                osp_node.exec_command(
                    cmd=f"export OS_CLOUD=standalone; openstack endpoint delete {endpoint['ID']}",
                    long_running=True,
                )

        # changing admin project password
        admin_password = config.get("admin_password")
        change_osp_project_password(osp_node, admin_password, "standalone", ip)

        # creating a shell script to set environment variables
        create_os_auth_sh(osp_node, "standalone", "/home/cephuser/os_auth.sh")

    else:
        log.error("standalone openstack deployment failed")
    return status
