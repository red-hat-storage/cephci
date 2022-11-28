import ipaddress
import json
import time

import yaml

from utility.log import Log
from utility.utils import get_cephci_config

log = Log(__name__)


repos = [
    "rhel-9-for-x86_64-baseos-eus-rpms",
    "rhel-9-for-x86_64-appstream-eus-rpms",
    "rhel-9-for-x86_64-highavailability-eus-rpms",
    "openstack-17-for-rhel-9-x86_64-rpms",
    "fast-datapath-for-rhel-9-x86_64-rpms",
]

standalone_parameters = """
parameter_defaults:
  CloudName: $IP
  CloudDomain: localdomain
  ControlPlaneStaticRoutes:
    - ip_netmask: 0.0.0.0/0
      next_hop: $GATEWAY
      default: true
  Debug: true
  DeploymentUser: $USER
  KernelIpNonLocalBind: 1
  DockerInsecureRegistryAddress:
    - $IP:8787
  NeutronPublicInterface: $INTERFACE
  NeutronDnsDomain: localdomain
  NeutronBridgeMappings: datacentre:br-ctlplane
  NeutronPhysicalBridge: br-ctlplane
  StandaloneEnableRoutedNetworks: false
  StandaloneHomeDir: $HOME
  StandaloneLocalMtu: 1500
  NtpServer:
    - clock.corp.redhat.com iburst
"""

container_prepare_parameters = """
  ContainerImageRegistryLogin: true
  ContainerImageRegistryCredentials:
    registry.redhat.io:
      """


def run(**kw):
    log.info("Running test")
    config = kw.get("config")
    osp_cluster = kw.get("ceph_cluster")
    osp_node = osp_cluster.get_ceph_object("osp").node
    cephci_config = get_cephci_config()
    username = cephci_config["cdn_credentials"]["username"]
    password = cephci_config["cdn_credentials"]["password"]

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

    # registering to subscription-manager
    osp_node.exec_command(
        sudo=True,
        cmd=f"sudo subscription-manager register --username={username} --password={password}",
        long_running=True,
    )
    osp_node.exec_command(
        cmd="sudo subscription-manager attach --auto", long_running=True
    )
    osp_node.exec_command(
        cmd="sudo subscription-manager release --set=9.0", long_running=True
    )
    osp_node.exec_command(cmd="sudo dnf install -y dnf-utils", long_running=True)

    # enabling necessary repositories
    osp_node.exec_command(
        cmd="sudo subscription-manager repos --disable=*", long_running=True
    )
    for repo in repos:
        osp_node.exec_command(
            cmd=f"sudo subscription-manager repos --enable={repo}", long_running=True
        )
    osp_node.exec_command(cmd="sudo dnf update -y", long_running=True)

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
    osp_node.exec_command(
        cmd="openstack tripleo container image prepare default"
        + " --output-env-file $HOME/containers-prepare-parameters.yaml",
        long_running=True,
    )
    cpp = container_prepare_parameters + f"{username}: '{password}'"
    osp_node.exec_command(cmd=f"echo '{cpp}' >> containers-prepare-parameters.yaml")

    # configuring standalone_parameters.yaml file
    osp_node.exec_command(
        cmd="touch $HOME/standalone_parameters.yaml", long_running=True
    )
    out, err = osp_node.exec_command(
        cmd="cat /etc/resolv.conf | grep -i  '^nameserver'|head -n2|cut -d ' ' -f2"
    )
    dns = out.split("\n")
    dns1, dns2 = dns[0], dns[1]
    out, err = osp_node.exec_command(
        cmd="ip -o -f inet addr show | awk '/scope global/ {print $4}'"
    )
    ip, netmask = out.split("\n")[0].split("/")
    vip = str(ipaddress.ip_address(ip) + 1)
    out, _ = osp_node.exec_command(cmd="ip route show 0.0.0.0/0 | cut -d ' ' -f3")
    gateway = out.split("\n")[0]
    log.info(f"Gateway is: {gateway}")
    sp_string = str(standalone_parameters)
    sp_string = sp_string.replace("$GATEWAY", gateway)
    osp_node.exec_command(cmd="timedatectl", long_running=True)
    out, _ = osp_node.exec_command(
        cmd="cat /etc/chrony.conf |grep -i  '^server'|head -n1|cut -d ' ' -f2"
    )
    clock = out.split("\n")[0]
    if clock != "":
        log.info(f"setting clock for NtpServer as: {clock}")
        sp_string = sp_string.replace("clock.corp.redhat.com", clock)
    commands = [
        f"export IP={ip}",
        f"export VIP={vip}",
        f"export NETMASK={netmask}",
        "export INTERFACE=eth0",
        f"export DNS1={dns1}",
        f"export DNS2={dns2}",
        f"cat <<EOF > $HOME/standalone_parameters.yaml\n{sp_string}EOF",
    ]
    cmd = " ; ".join(commands)
    osp_node.exec_command(cmd=cmd, long_running=True)

    # setting hostname with domain
    hostname, err = osp_node.exec_command(cmd="hostname -s")
    hostname = hostname.strip()
    osp_node.exec_command(
        cmd=f"sudo hostnamectl set-hostname {hostname}.novalocal", long_running=True
    )
    osp_node.exec_command(
        cmd=f"sudo hostnamectl set-hostname {hostname}.novalocal --transient",
        long_running=True,
    )

    # Deploying the all-in-one Red Hat OpenStack Platform environment
    commands = [
        f"sudo podman login registry.redhat.io --username={username} --password={password}",
        f"export IP={ip}",
        f"export VIP={vip}",
        f"export NETMASK={netmask}",
        "export INTERFACE=eth0",
        f"export DNS1={dns1}",
        f"export DNS2={dns2}",
        "sudo openstack tripleo deploy --templates --local-ip=$IP/$NETMASK --control-virtual-ip=$VIP"
        + " -e /usr/share/openstack-tripleo-heat-templates/environments/standalone/standalone-tripleo.yaml"
        + " -r /usr/share/openstack-tripleo-heat-templates/roles/Standalone.yaml"
        + " -e $HOME/containers-prepare-parameters.yaml"
        + " -e $HOME/standalone_parameters.yaml"
        + " --output-dir $HOME --standalone",
    ]
    cmd = " ; ".join(commands)
    status = osp_node.exec_command(cmd=cmd, long_running=True)

    # enable password authentication to root user through ssh, which is disabled by openstack installation
    osp_node.exec_command(
        cmd="echo 'PermitRootLogin yes' | sudo tee -a /etc/ssh/sshd_config",
        long_running=True,
    )
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
        log.info(f"changing the admin project password to '{admin_password}'")
        osp_node.exec_command(
            cmd=f"export OS_CLOUD=standalone; openstack user set --password {admin_password} admin",
            long_running=True,
        )
        out, err = osp_node.exec_command(cmd="cat ~/.config/openstack/clouds.yaml")
        log.info(f"openstack project details present in clouds.yaml:\n{out}")
        cloud_yaml = yaml.full_load(out)
        cloud_yaml["clouds"]["standalone"]["auth"]["password"] = admin_password
        cloud_yaml["clouds"]["standalone"]["auth"]["auth_url"] = f"http://{ip}:5000"
        osp_node.exec_command(
            cmd=f"echo '{yaml.dump(cloud_yaml, default_flow_style=False)}' > ~/.config/openstack/clouds.yaml"
        )
        log.info(
            "admin project password changed successfully. "
            + f"admin project details present under ~/.config/openstack/clouds.yaml :\n{cloud_yaml}"
        )

        # creating a shell script to set environment variables
        auth = cloud_yaml["clouds"]["standalone"]["auth"]
        commands = [
            f"OS_USERNAME={auth['username']}",
            f"OS_PASSWORD={auth['password']}",
            f"OS_PROJECT_NAME={auth['project_name']}",
            f"OS_PROJECT_DOMAIN_NAME={auth['project_domain_name']}",
            f"OS_USER_DOMAIN_NAME={auth['user_domain_name']}",
            f"OS_AUTH_URL={auth['auth_url']}",
            f"OS_IDENTITY_API_VERSION={cloud_yaml['clouds']['standalone']['identity_api_version']}",
            "export OS_USERNAME OS_PASSWORD OS_PROJECT_NAME OS_USER_DOMAIN_NAME OS_PROJECT_DOMAIN_NAME"
            + " OS_AUTH_URL OS_IDENTITY_API_VERSION",
        ]
        cmd = "\n".join(commands)
        osp_node.exec_command(cmd="touch ~/os_auth.sh")
        osp_node.exec_command(cmd=f"echo '{cmd}' > ~/os_auth.sh")
        log.info(
            "created shell script os_auth.sh that can be used to set environment variables"
            + " for executing osp commands and swift commands"
        )

    else:
        log.error("standalone openstack deployment failed")
    return status
