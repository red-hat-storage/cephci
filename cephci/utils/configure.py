# -*- code: utf-8 -*-

from json import loads
from logging import getLogger

from cli.cephadm.ansible import Ansible
from cli.exceptions import AnsiblePlaybookExecutionError, OperationFailedError
from cli.utilities.containers import Container
from cli.utilities.packages import Package

log = getLogger(__name__)

CEPHADM_PREFLIGHT_PLAYBOOK = "cephadm-preflight.yml"
ETC_HOSTS = "/etc/hosts"

SSH = "~/.ssh"
SSH_CONFIG = f"{SSH}/config"
SSH_ID_RSA_PUB = f"{SSH}/id_rsa.pub"
SSH_KNOWN_HOSTS = f"{SSH}/known_hosts"

SSH_KEYGEN = f"ssh-keygen -b 2048 -f {SSH}/id_rsa -t rsa -q -N ''"
SSH_COPYID = "ssh-copy-id -f -i {} {}@{}"
SSH_KEYSCAN = "ssh-keyscan {}"

CHMOD_CONFIG = f"chmod 600 {SSH_CONFIG}"
SSHPASS = "sshpass -p {}"
REGISTRY_PATH = "/opt/registry"
REGISTRY_AUTH_PATH = f"{REGISTRY_PATH}/auth/htpasswd"
DOMAIN_KEY_PATH = f"{REGISTRY_PATH}/certs/domain.key"
DOMAIN_CERT_PATH = f"{REGISTRY_PATH}/certs/domain.crt"
REGISTRY_CERT_PATH = f"{REGISTRY_PATH}/certs/registry.crt"
CA_TRUST_ANCHORS_PATH = "/etc/pki/ca-trust/source/anchors/"
SKOPEO_IMAGE = "registry.redhat.io/rhel8/skopeo:8.5-8"
REGISTRY_DATA = f"{REGISTRY_PATH}/data"
REGISTRY_AUTH = f"{REGISTRY_PATH}/auth"
REGISTRY_CERTS = f"{REGISTRY_PATH}/certs"
DOMAIN_CERT_PATH_CERT = f"{REGISTRY_PATH}/certs/domain.cert"
PRIVATE_REGISTRY_NAME = "myprivateregistry"
PRIVATE_REGISTRY_PORT = "5000:5000"


def setup_ssh_keys(installer, nodes):
    """Set up SSH keys

    Args:
        installer (Node): Cluster installer node object
        nodes (List): List of Node objects
    """
    hosts, config = (
        "",
        "Host *\n\tStrictHostKeyChecking no\n\tServerAliveInterval 2400\n",
    )
    for node in nodes:
        config += f"\nHost {node.ip_address}"
        config += f"\n\tHostname {node.hostname}"
        config += "\n\tUser root"

        hosts += f"\n{node.ip_address}"
        hosts += f"\t{node.hostname}"
        hosts += f"\t{node.shortname}"

        node.exec_command(cmd=f"rm -rf {SSH}", check_ec=False)
        node.exec_command(cmd=SSH_KEYGEN)

        node.exec_command(sudo=True, cmd=f"rm -rf {SSH}", check_ec=False)
        node.exec_command(sudo=True, cmd=SSH_KEYGEN)

    installer.exec_command(sudo=True, cmd=f"echo -e '{hosts}' >> {ETC_HOSTS}")
    installer.exec_command(
        cmd=f"touch {SSH_CONFIG} && echo -e '{config}' > {SSH_CONFIG}"
    )
    installer.exec_command(cmd=CHMOD_CONFIG)
    Package(installer).install("sshpass")

    for node in nodes:
        installer.exec_command(
            cmd="{} >> {}".format(SSH_KEYSCAN.format(node.hostname), SSH_KNOWN_HOSTS),
        )
        installer.exec_command(
            sudo=True,
            cmd="{} >> {}".format(SSH_KEYSCAN.format(node.hostname), SSH_KNOWN_HOSTS),
        )

        installer.exec_command(
            cmd="{} {}".format(
                SSHPASS.format(node.password),
                SSH_COPYID.format(SSH_ID_RSA_PUB, node.username, node.hostname),
            ),
        )
        installer.exec_command(
            cmd="{} {}".format(
                SSHPASS.format(node.root_passwd),
                SSH_COPYID.format(SSH_ID_RSA_PUB, "root", node.hostname),
            ),
        )
        installer.exec_command(
            sudo=True,
            cmd="{} {}".format(
                SSHPASS.format(node.root_passwd),
                SSH_COPYID.format(SSH_ID_RSA_PUB, "root", node.hostname),
            ),
        )


def accept_eula(node):
    """
    Accepts the eula for IBM Storage Ceph License
    Args:
        node(Node): Node object
    """
    cmd = "cat" if accept_eula else "touch"
    cmd += " /usr/share/ibm-storage-ceph-license/accept"

    Package(node).install("ibm-storage-ceph-license", env_vars=accept_eula)
    return node.exec_command(sudo=True, cmd=cmd, check_ec=True)


def exec_cephadm_preflight(installer, build_type, ceph_repo=None):
    """Executes cephadm preflight playbook"""
    try:
        if build_type == "ibm":
            accept_eula(installer)

        extra_vars = {"ceph_origin": build_type}
        if ceph_repo:
            extra_vars = {
                "ceph_origin": "custom",
                "gpgcheck": "no",
                "custom_repo_url": ceph_repo,
            }
        Ansible(installer).run_playbook(
            playbook=CEPHADM_PREFLIGHT_PLAYBOOK,
            extra_vars=extra_vars,
        )
    except Exception as e:
        raise AnsiblePlaybookExecutionError(
            f"Failed to execute cephadm preflight playbook:\n {e}"
        )


def create_registry_directories(node):
    """
    Create private registry directories for disconnected install
    """
    path = "/opt/registry/{auth,certs,data}"
    cmd = "mkdir -p {}".format(path)
    out, _ = node.exec_command(cmd=cmd, sudo=True)
    if out:
        log.error("Failed to create directory for private registry")
        return False
    return True


def set_registry_credentials(node, username, password):
    """
    Create credentials for accessing the private registry
    Args:
        node (ceph): Node to execute cmd
        username (str): Username for the private registry
        password (str): Password for the private registry
    """
    Package(node).install("httpd-tools")
    cmd = f"htpasswd -bBc {REGISTRY_AUTH_PATH} {username} {password}"
    out, _ = node.exec_command(cmd=cmd, sudo=True)
    if out:
        log.error("Failed to create credentials for accessing the private registry")
        return False
    return True


def create_self_signed_certificate(node):
    """Creates self-signed certificate for private registry"""
    Package(node).install("openssl")
    cmd = (
        f"openssl req -newkey rsa:4096 -nodes -sha256 -keyout {DOMAIN_KEY_PATH} -x509 -days 365 -out "
        f'{DOMAIN_CERT_PATH} -addext "subjectAltName = DNS:{node.hostname}" '
        f'-subj "/C=IN/ST=ST/L=XYZ/O=ORG/CN={node.hostname}"'
    )
    out, _ = node.exec_command(cmd=cmd, sudo=True)
    if out:
        log.error("Failed to create a self-signed certificate")
        return False
    return True


def create_link_to_domain_cert(node):
    """Creates a link to the domain certificate"""
    # Create symlink
    cmd = f"ln -s {DOMAIN_CERT_PATH} {DOMAIN_CERT_PATH_CERT}"
    out, _ = node.exec_command(cmd=cmd, sudo=True)
    if out and "File exists" not in out:
        log.error("Failed to create a link to the domain certificate")
        return False
    return True


def add_cert_to_trusted_list(node):
    """Add the certificate to the trusted list on the private registry node"""
    cmd = f"cp {DOMAIN_CERT_PATH} {CA_TRUST_ANCHORS_PATH}"
    out, _ = node.exec_command(cmd=cmd, sudo=True)
    if out:
        log.error("Failed to update-ca-trust")
        return False

    # Update the ca-trust
    if not update_ca_trust(node):
        return False
    return True


def update_ca_trust(node):
    """Update the ca trust"""
    cmd = "update-ca-trust"
    out, _ = node.exec_command(cmd=cmd, sudo=True)
    if out:
        log.error("Failed to update-ca-trust")
        return False
    return True


def validate_trusted_list(node, secondary_node):
    """Verify trust list is updated with hostname"""
    cmd = f"trust list | grep -i {node.hostname}"
    out, _ = secondary_node.exec_command(cmd=cmd, sudo=True)
    if node.hostname not in out:
        log.error("Trust list not updated")
        return False
    return True


def copy_cert_to_secondary_node(node, secondary_nodes):
    """Copy the certificate to another nodes in the cluster"""
    for secondary_node in secondary_nodes:
        cmd = f"scp {DOMAIN_CERT_PATH} root@{secondary_node.hostname}:{CA_TRUST_ANCHORS_PATH}"
        out, _ = node.exec_command(cmd=cmd, sudo=True, timeout=800)
        if out:
            log.error(
                f"Failed to copy the certificate to {secondary_node.hostname} node"
            )
            return False

        # Update the ca-trust
        if not update_ca_trust(secondary_node):
            log.error(f"Failed to update ca trust on {secondary_node.hostname} node")
            return False

        # Validate the trusted list
        if not validate_trusted_list(node, secondary_node):
            log.error(
                f"Failed to validate trusted list on {secondary_node.hostname} node"
            )
            return False
    return True


def get_private_registry_image(node, username, password):
    """Using the curl command, verify the images reside in the local registry
    Args:
        node (ceph.ceph.Ceph): CephNode or list of CephNode object
    """
    cmd = f"curl -u {username}:{password} https://{node.hostname}:5000/v2/_catalog"
    out = loads(node.exec_command(sudo=True, cmd=cmd)[0])
    return out


def start_local_private_registry(node, image):
    """Start local private registry"""

    volumes = [
        f"{REGISTRY_DATA}:/var/lib/registry:z",
        f"{REGISTRY_AUTH}:/auth:z",
        f"{REGISTRY_CERTS}:/certs:z",
    ]
    env = [
        '"REGISTRY_AUTH=htpasswd"',
        '"REGISTRY_AUTH_HTPASSWD_REALM=Registry Realm"',
        "REGISTRY_AUTH_HTPASSWD_PATH=/auth/htpasswd",
        '"REGISTRY_HTTP_TLS_CERTIFICATE=/certs/domain.crt"',
        '"REGISTRY_HTTP_TLS_KEY=/certs/domain.key"',
        "REGISTRY_COMPATIBILITY_SCHEMA1_ENABLED=true",
    ]

    Container(node).run(
        name=PRIVATE_REGISTRY_NAME,
        image=image,
        volume=volumes,
        env=env,
        ports=PRIVATE_REGISTRY_PORT,
        restart="always",
        detach=True,
        long_running=False,
    )
    return True


def add_images_to_private_registry(
    node,
    src_username,
    src_password,
    username,
    password,
    registry,
    build_type,
    images=None,
):
    """Red Hat Ceph Storage 5 image, Prometheus images, and Dashboard image from the Red Hat Customer Portal
    to the private registry:"""

    volumes = [
        f"{REGISTRY_CERTS}:/certs:Z",
        f"{DOMAIN_CERT_PATH_CERT}:/certs/domain.cert:Z",
    ]

    src_creds = f"{src_username}:{src_password}"
    dest_creds = f"{username}:{password}"
    dst_creds_dir = "./certs/"

    for image in images:
        img_reg = image.replace(f"{registry}/", "")
        if registry not in img_reg:
            src_image = f"{registry}/{img_reg}"
        else:
            src_image = img_reg
        dst_image = f"{node.hostname}:5000/{img_reg}"
        skopeo_cmd = generate_skopeo_copy_cmd(
            src_image,
            dst_image,
            src_creds,
            dst_creds_dir,
            dest_creds,
            remove_signature=True,
        )
        Container(node).run(
            long_running=False,
            volume=volumes,
            rm=True,
            image=SKOPEO_IMAGE,
            cmds=skopeo_cmd,
        )
    return True


def generate_skopeo_copy_cmd(
    src_image,
    dst_image,
    src_creds=None,
    dst_cert_dir=None,
    dest_creds=None,
    remove_signature=False,
):
    """Copy an image from one registry to another using skopeo"""
    cmd = "skopeo copy"
    if remove_signature:
        cmd += " --remove-signatures"
    if src_creds:
        cmd += f" --src-creds {src_creds}"
    if dst_cert_dir:
        cmd += f" --dest-cert-dir={dst_cert_dir}"
    if dest_creds:
        cmd += f" --dest-creds {dest_creds}"
    cmd += f" docker://{src_image} docker://{dst_image}"
    return cmd


def setup_fio(client):
    """
    Installs fio IO tool on the given client
    Args:
        client (ceph): Client node
    """
    try:
        # Install Fio packages
        Package(client).install("fio", nogpgcheck=True)
    except Exception:
        raise OperationFailedError(f"Fio installation failed on {client.hostname}")


def setup_smallfile_io(client):
    """
    Git pulls Smallfile repo
    Args:
        client (ceph): Client nod
    """
    try:
        # Install Fio packages
        cmd = "git clone https://github.com/distributed-system-analysis/smallfile.git"
        client.exec_command(sudo=True, cmd=cmd)
    except Exception:
        raise OperationFailedError("Failed to pull SmallFile")
