"""Module that allows QE to interface with cephadm bootstrap CLI."""
import json
import tempfile
from typing import Dict

from ceph.ceph import ResourceNotFoundError
from ceph.ceph_admin.cephadm_ansible import CephadmAnsible
from utility.log import Log
from utility.utils import get_cephci_config

from .common import config_dict_to_string
from .helper import GenerateServiceSpec, create_ceph_config_file
from .typing_ import CephAdmProtocol

logger = Log(__name__)

__DEFAULT_CEPH_DIR = "/etc/ceph"
__DEFAULT_CONF_PATH = "/etc/ceph/ceph.conf"
__DEFAULT_KEYRING_PATH = "/etc/ceph/ceph.client.admin.keyring"
__DEFAULT_SSH_PATH = "/etc/ceph/ceph.pub"


def construct_registry(cls, registry: str, json_file: bool = False):
    """
    Construct registry credentials for bootstrapping cluster

    Args:
        cls (CephAdmin): class object
        registry (Str): registry name
        json_file (Bool): registry credentials in JSON file (default:False)

    Example::

        json_file:
            - False : Constructs registry credentials for bootstrap
            - True  : Creates file with registry name attached with it,
                      and saved as /tmp/<registry>.json file.

    Returns:
        constructed string of registry credentials ( Str )
    """
    # Todo: Retrieve credentials based on registry name
    _config = get_cephci_config()
    cdn_cred = _config.get("registry_credentials", _config["cdn_credentials"])
    reg_args = {
        "registry-url": cdn_cred.get("registry", registry),
        "registry-username": cdn_cred.get("username"),
        "registry-password": cdn_cred.get("password"),
    }
    if json_file:
        reg = dict((k.lstrip("registry-"), v) for k, v in reg_args.items())

        # Create file and return file_path
        temp_file = tempfile.NamedTemporaryFile(suffix=".json")
        reg_args = {"registry-json": temp_file.name}
        reg_file = cls.installer.node.remote_file(
            sudo=True, file_name=temp_file.name, file_mode="w"
        )
        reg_file.write(json.dumps(reg, indent=4))
        reg_file.flush()

    return config_dict_to_string(reg_args)


def copy_ceph_configuration_files(cls, ceph_conf_args):
    """
    Copy ceph configuration files to ceph default "/etc/ceph" path.

    Args:
        cls (CephAdmin): cephadm instance
        ceph_conf_args (Dict): bootstrap arguments

    Example::

        ceph_conf_args:
            output-dir: "/root/ceph"
            output-keyring : "/root/ceph/ceph.client.admin.keyring"
            output-config : "/root/ceph/ceph.conf"
            output-pub-ssh-key : "/root/ceph/ceph.pub"
            ssh-public-key : "/root/ceph/ceph.pub"

    :Note: we can eliminate this definition when we have support to access
            ceph cli via custom ceph config files.
    """
    ceph_dir = ceph_conf_args.get("output-dir")
    if ceph_dir:
        cls.installer.exec_command(cmd=f"mkdir -p {__DEFAULT_CEPH_DIR}", sudo=True)

    def copy_file(node, src, destination):
        node.exec_command(cmd=f"cp {src} {destination}", sudo=True)

    ceph_files = {
        "output-keyring": __DEFAULT_KEYRING_PATH,
        "output-config": __DEFAULT_CONF_PATH,
        "output-pub-ssh-key": __DEFAULT_SSH_PATH,
        "ssh-public-key": __DEFAULT_SSH_PATH,
    }

    for arg, default_path in ceph_files.items():
        if ceph_conf_args.get(arg):
            copy_file(cls.installer, ceph_conf_args.get(arg), default_path)


def generate_ssl_certificate(cls, dashboard_key, dashboard_crt):
    """
    Construct dashboard key and certificate files for bootstrapping cluster
    with dashboard custom key and certificate files for ssl

    Args:
        cls (CephAdmin): class object
        dashboard_key (Str): path to generate ssl key
        dashboard_crt (Str): path to generate ssl certificate

    Returns:
         constructed string of SSL CLI option (Str)
    """

    # Installing openssl package needed for ssl
    cls.installer.exec_command(
        sudo=True,
        cmd="yum install -y openssl",
    )

    # Generating key and cert using openssl in /home/cephuser
    cls.installer.exec_command(
        sudo=True,
        cmd=f'openssl req -new -nodes -x509 \
            -subj "/O=IT/CN=ceph-mgr-dashboard" -days 3650 \
            -keyout {dashboard_key} \
            -out {dashboard_crt} -extensions v3_ca',
    )
    cert_args = {"dashboard-key": dashboard_key, "dashboard-crt": dashboard_crt}
    return config_dict_to_string(cert_args)


class BootstrapMixin:
    """Add bootstrap support to the child class."""

    def bootstrap(self: CephAdmProtocol, config: Dict):
        """
        Execute cephadm bootstrap with the passed kwargs on the installer node.::

            Bootstrap involves,
              - Creates /etc/ceph directory with permissions
              - CLI creation with bootstrap options with custom/default image
              - Execution of bootstrap command

        Args:
            config (Dict): Key/value pairs passed from the test case.

        Example::

            config:
                command: bootstrap
                base_cmd_args:
                    verbose: true
                args:
                    custom_repo: custom repository path
                    custom_image: <image path> or <boolean>
                    mon-ip: <node_name>
                    mgr-id: <mgr_id>
                    fsid: <id>
                    registry-url: <registry.url.name>
                    registry-json: <registry.url.name>
                    initial-dashboard-user: <admin123>
                    initial-dashboard-password: <admin123>

        custom_image::

            image path: compose path for example alpha build,
                ftp://partners.redhat.com/d960e6f2052ade028fa16dfc24a827f5/rhel-8/Tools/x86_64/os/

            boolean:
                True: use latest image from test config
                False: do not use latest image from test config,
                        and also indicates usage of default image from cephadm source-code.

        """
        self.cluster.setup_ssh_keys()
        args = config.get("args")
        custom_repo = args.pop("custom_repo", "")
        custom_image = args.pop("custom_image", True)
        build_type = self.config.get("build_type")
        rhbuild = self.config.get("rhbuild")

        if build_type == "upstream":
            self.setup_upstream_repository()
            # work-around to enable ceph x86_64 RPM pkgs.
            # which is currently unavailable in upstream builds.
            self.set_cdn_tool_repo()
        elif build_type == "released" or custom_repo.lower() == "cdn":
            custom_image = False
            self.set_cdn_tool_repo()
        elif custom_repo:
            self.set_tool_repo(repo=custom_repo)
        else:
            self.set_tool_repo()

        ansible_run = config.get("cephadm-ansible", None)
        if ansible_run:
            cephadm_ansible = CephadmAnsible(cluster=self.cluster)
            cephadm_ansible.execute_playbook(
                playbook=ansible_run["playbook"],
                extra_vars=ansible_run.get("extra-vars"),
                extra_args=ansible_run.get("extra-args"),
            )
        else:
            self.install()

        cmd = "cephadm"
        if config.get("base_cmd_args"):
            cmd += config_dict_to_string(config["base_cmd_args"])

        if custom_image:
            if isinstance(custom_image, str):
                cmd += f" --image {custom_image}"
            else:
                cmd += f" --image {self.config['container_image']}"

        cmd += " bootstrap"

        # Construct registry credentials as string or json.
        registry_url = args.pop("registry-url", None)
        if registry_url:
            cmd += construct_registry(self, registry_url)

        registry_json = args.pop("registry-json", None)
        if registry_json:
            cmd += construct_registry(self, registry_json, json_file=True)

        """ Generate dashboard certificate and key if bootstrap cli
            have this options as dashboard-key and dashboard-crt """
        dashboard_key_path = args.pop("dashboard-key", False)
        dashboard_cert_path = args.pop("dashboard-crt", False)

        if dashboard_cert_path and dashboard_key_path:
            cmd += generate_ssl_certificate(
                self, dashboard_key_path, dashboard_cert_path
            )

        # To be generic, the mon-ip contains the global node name. Here, we replace the
        # name with the IP address. The replacement allows us to be inline with the
        # CLI option.

        # Todo: need to switch installer node on any other node name provided
        #       other than installer node
        mon_node = args.pop("mon-ip", self.installer.node.shortname)
        if mon_node:
            for node in self.cluster.get_nodes():
                # making sure conditions works in all the scenario
                if (
                    node.shortname == mon_node
                    or node.shortname.endswith(mon_node)
                    or f"{mon_node}-" in node.shortname
                ):
                    cmd += f" --mon-ip {node.ip_address}"
                    break
            else:
                raise ResourceNotFoundError(f"Unknown {mon_node} node name.")

        # apply-spec
        specs = args.get("apply-spec")
        if specs:
            spec_cls = GenerateServiceSpec(
                node=self.installer, cluster=self.cluster, specs=specs
            )
            args["apply-spec"] = spec_cls.create_spec_file()

        # config
        conf = args.get("config")
        if conf:
            args["config"] = create_ceph_config_file(node=self.installer, config=conf)

        cmd += config_dict_to_string(args)

        # Todo: This patch is specific to 5.1 release,
        #   should be removed for next 5.x development builds or release.
        if rhbuild.split("-")[0] in ["5.1", "5.2"]:
            cmd += " --yes-i-know"

        out, err = self.installer.exec_command(
            sudo=True,
            cmd=cmd,
            timeout=1800,
            check_ec=True,
        )

        logger.info("Bootstrap output : %s", out)
        logger.error("Bootstrap error: %s", err)

        # The path to ssh public key mentioned in either output-pub-ssh-key or
        # ssh-public-key options will be considered for distributing the ssh public key,
        # if these are not specified, then the default ssh key path /etc/ceph/ceph.pub
        # will be considered.
        self.distribute_cephadm_gen_pub_key(
            args.get("output-pub-ssh-key") or args.get("ssh-public-key")
        )

        # Copy all the ceph configuration files to default path /etc/ceph
        # if they are already not present in the default path
        copy_ceph_configuration_files(self, args)

        # Check for image overrides
        if self.config.get("overrides"):
            override_dict = dict(item.split("=") for item in self.config["overrides"])
            supported_overrides = [
                "grafana",
                "keepalived",
                "haproxy",
                "prometheus",
                "node_exporter",
                "alertmanager",
            ]

            for image in supported_overrides:
                image_key = f"{image}_image"
                if override_dict.get(image_key):
                    cmd = "cephadm shell --"
                    cmd += f" ceph config set mgr mgr/cephadm/container_image_{image}"
                    cmd += f" {override_dict[image_key]}"
                    self.installer.exec_command(sudo=True, cmd=cmd)

        # Set public and cluster networks if provided.
        # https://docs.ceph.com/en/latest/rados/configuration/network-config-ref/
        public_nws = self.cluster.get_public_networks()
        cluster_nws = self.cluster.get_cluster_networks()
        if public_nws:
            self.shell(
                args=["ceph", "config", "set", "global public_network", public_nws]
            )
        if cluster_nws:
            self.shell(
                args=["ceph", "config", "set", "global cluster_network", cluster_nws]
            )

        return out, err
