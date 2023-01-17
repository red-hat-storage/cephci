import yaml

from utility.log import Log

log = Log(__name__)

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
  NeutronPublicInterface: $INTERFACE
  NeutronDnsDomain: localdomain
  NeutronBridgeMappings: datacentre:br-ctlplane
  NeutronPhysicalBridge: br-ctlplane
  StandaloneEnableRoutedNetworks: false
  StandaloneHomeDir: $HOME
  StandaloneLocalMtu: 1500
  NtpServer:
    - clock.corp.redhat.com
"""


container_prepare_parameters = """
  ContainerImageRegistryLogin: true
  ContainerImageRegistryCredentials:
    registry.redhat.io:
      """


class MyDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super(MyDumper, self).increase_indent(flow, False)


def setup_subscription_manager(osp_node, username, password, config):
    releases = {"17.0": "9.0", "16.2": "8.4", "16.1": "8.4"}
    osp_version = config.get("osp_version")
    release = releases[osp_version]
    osp_node.exec_command(
        sudo=True,
        cmd=f"sudo subscription-manager register --username={username} --password={password}",
        long_running=True,
    )
    osp_node.exec_command(
        cmd="sudo subscription-manager attach --auto", long_running=True
    )
    osp_node.exec_command(
        cmd=f"sudo subscription-manager release --set={release}", long_running=True
    )


def enable_repos(osp_node, config):
    openstack_rpms = {"17.0": "17", "16.2": "16.2", "16.1": "16.1"}
    rhel_rpms = {"17.0": "9", "16.2": "8", "16.1": "8"}
    osp_version = config.get("osp_version")
    osp_rpm = openstack_rpms[osp_version]
    rhel_version = rhel_rpms[osp_version]
    repos = [
        f"rhel-{rhel_version}-for-x86_64-baseos-eus-rpms",
        f"rhel-{rhel_version}-for-x86_64-appstream-eus-rpms",
        f"rhel-{rhel_version}-for-x86_64-highavailability-eus-rpms",
    ]

    if osp_version.startswith("16"):
        repos += ["ansible-2.9-for-rhel-8-x86_64-rpms"]

    repos += [
        f"openstack-{osp_rpm}-for-rhel-{rhel_version}-x86_64-rpms",
        f"fast-datapath-for-rhel-{rhel_version}-x86_64-rpms",
    ]

    osp_node.exec_command(cmd="sudo dnf install -y dnf-utils", long_running=True)
    osp_node.exec_command(
        cmd="sudo subscription-manager repos --disable=*", long_running=True
    )
    for repo in repos:
        osp_node.exec_command(
            cmd=f"sudo subscription-manager repos --enable={repo}", long_running=True
        )

    # set the container tools module version for osp 16
    if osp_version.startswith("16"):
        osp_node.exec_command(
            cmd="sudo dnf module disable -y container-tools:rhel8", long_running=True
        )
        osp_node.exec_command(
            cmd="sudo dnf module enable -y container-tools:3.0", long_running=True
        )

    status = osp_node.exec_command(cmd="sudo dnf update -y", long_running=True)
    if status != 0:
        raise Exception("yum update unsuccessful")


def configure_cpp(osp_node, username, password):
    osp_node.exec_command(
        cmd="openstack tripleo container image prepare default"
        + " --output-env-file $HOME/containers-prepare-parameters.yaml",
        long_running=True,
    )
    cpp = container_prepare_parameters + f"{username}: '{password}'"
    osp_node.exec_command(
        cmd=f"echo '{cpp}' >> $HOME/containers-prepare-parameters.yaml"
    )


def get_dns(osp_node):
    out, err = osp_node.exec_command(
        cmd="cat /etc/resolv.conf | grep -i  '^nameserver'|head -n2|cut -d ' ' -f2"
    )
    log.info(out)
    return out


def get_ip_and_netmask(osp_node):
    out, err = osp_node.exec_command(
        cmd="ip -o -f inet addr show | awk '/scope global/ {print $4}'"
    )
    log.info(out)
    return out


def get_gateway(osp_node):
    out, err = osp_node.exec_command(cmd="ip route show 0.0.0.0/0 | cut -d ' ' -f3")
    log.info(out)
    return out


def get_clock_server(osp_node):
    out, _ = osp_node.exec_command(
        cmd="cat /etc/chrony.conf |grep -i  '^server'|head -n1|cut -d ' ' -f2"
    )
    log.info(out)
    return out


def configure_standalone_parameters(osp_node, env_vars, set_env_vars, config):
    osp_version = config.get("osp_version")
    osp_node.exec_command(
        cmd="touch $HOME/standalone_parameters.yaml", long_running=True
    )

    sp_string = str(standalone_parameters)
    log.info(f"Gateway is: {env_vars['gateway']}")
    sp = yaml.full_load(sp_string)
    parameter_defaults = sp["parameter_defaults"]
    parameter_defaults["ControlPlaneStaticRoutes"][0]["next_hop"] = env_vars["gateway"]
    osp_node.exec_command(cmd="timedatectl", long_running=True)
    clock = env_vars["clock"]
    if osp_version != "16.1":
        clock = [clock]
    parameter_defaults["NtpServer"] = clock
    if osp_version.startswith("17"):
        parameter_defaults["KernelIpNonLocalBind"] = 1
        parameter_defaults["DockerInsecureRegistryAddress"] = ["$IP:8787"]
    if osp_version.startswith("16"):
        parameter_defaults["DnsServers"] = ["$DNS1", "$DNS2"]
        parameter_defaults["CloudDomain"] = "novalocal"
    sp_string = yaml.dump(sp, Dumper=MyDumper, default_flow_style=False)
    commands = set_env_vars + [
        f"cat <<EOF > $HOME/standalone_parameters.yaml\n{sp_string}EOF",
    ]
    cmd = " ; ".join(commands)
    osp_node.exec_command(cmd=cmd, long_running=True)


def set_hostname_with_domain(osp_node, domain):
    hostname, err = osp_node.exec_command(cmd="hostname -s")
    hostname = hostname.strip()
    osp_node.exec_command(
        cmd=f"sudo hostnamectl set-hostname {hostname}.{domain}", long_running=True
    )
    osp_node.exec_command(
        cmd=f"sudo hostnamectl set-hostname {hostname}.{domain} --transient",
        long_running=True,
    )


def deploy_standalone_osp(osp_node, osp_version, username, password, set_env_vars):
    commands = set_env_vars + [
        f"sudo podman login registry.redhat.io --username={username} --password={password}",
        "sudo openstack tripleo deploy --templates --local-ip=$IP/$NETMASK"
        + f"{' --control-virtual-ip=$VIP' if osp_version.startswith('17') else ''}"
        + " -e /usr/share/openstack-tripleo-heat-templates/environments/standalone/standalone-tripleo.yaml"
        + " -r /usr/share/openstack-tripleo-heat-templates/roles/Standalone.yaml"
        + " -e $HOME/containers-prepare-parameters.yaml"
        + " -e $HOME/standalone_parameters.yaml"
        + " --output-dir $HOME --standalone",
    ]
    cmd = " ; ".join(commands)
    status = osp_node.exec_command(cmd=cmd, long_running=True)
    return status


def set_permit_root_login(osp_node, option):
    out = osp_node.exec_command(
        sudo=True,
        cmd="grep -q '^PermitRootLogin.*' /etc/ssh/sshd_config"
        + f"&& sed -i 's/^PermitRootLogin.*/PermitRootLogin {option}/' /etc/ssh/sshd_config"
        + f"|| echo 'PermitRootLogin {option}' >> /etc/ssh/sshd_config",
        long_running=True,
    )
    return out


def set_password_authentication(osp_node, option):
    out = osp_node.exec_command(
        sudo=True,
        cmd="grep -q '^PasswordAuthentication.*' /etc/ssh/sshd_config"
        + f" && sed -i 's/^PasswordAuthentication.*/PasswordAuthentication {option}/' /etc/ssh/sshd_config"
        + f" || echo 'PasswordAuthentication {option}' >> /etc/ssh/sshd_config",
        long_running=True,
    )
    return out


def change_osp_project_password(osp_node, admin_password, cloud_name, ip):
    log.info(f"changing the admin project password to '{admin_password}'")
    osp_node.exec_command(
        cmd=f"export OS_CLOUD=standalone; openstack user set --password {admin_password} admin",
        long_running=True,
    )
    out, err = osp_node.exec_command(cmd="cat ~/.config/openstack/clouds.yaml")
    log.info(f"openstack project details present in clouds.yaml:\n{out}")
    cloud_yaml = yaml.full_load(out)
    cloud_yaml["clouds"][cloud_name]["auth"]["password"] = admin_password
    cloud_yaml["clouds"][cloud_name]["auth"]["auth_url"] = f"http://{ip}:5000"
    osp_node.exec_command(
        cmd=f"echo '{yaml.dump(cloud_yaml, default_flow_style=False)}' > ~/.config/openstack/clouds.yaml"
    )
    log.info(
        "admin project password changed successfully. "
        + f"admin project details present under ~/.config/openstack/clouds.yaml :\n{cloud_yaml}"
    )


def create_os_auth_sh(osp_node, cloud_name, auth_file):
    out, err = osp_node.exec_command(cmd="cat ~/.config/openstack/clouds.yaml")
    log.info(f"openstack project details present in clouds.yaml:\n{out}")
    cloud_yaml = yaml.full_load(out)
    auth = cloud_yaml["clouds"][cloud_name]["auth"]
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
    osp_node.exec_command(cmd=f"touch {auth_file}")
    osp_node.exec_command(cmd=f"echo '{cmd}' > {auth_file}")
    log.info(
        "created shell script os_auth.sh that can be used to set environment variables"
        + " for executing osp commands and swift commands"
    )
