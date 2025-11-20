"""Module that allows QE to interface with cephadm bootstrap CLI."""

import json
import tempfile
from copy import deepcopy
from typing import Dict

from looseversion import LooseVersion

from ceph.ceph_admin.cephadm_ansible import CephadmAnsible
from ceph.utils import get_node_by_id, get_public_network, setup_repos
from cephci.utils.build_info import CephTestManifest
from utility.log import Log
from utility.utils import get_cephci_config

from ..ceph import ResourceNotFoundError
from .common import config_dict_to_string
from .helper import GenerateServiceSpec, create_ceph_config_file, validate_spec_services
from .typing_ import CephAdmProtocol

logger = Log(__name__)

__DEFAULT_CEPH_DIR = "/etc/ceph"
__DEFAULT_CONF_PATH = "/etc/ceph/ceph.conf"
__DEFAULT_KEYRING_PATH = "/etc/ceph/ceph.client.admin.keyring"
__DEFAULT_SSH_PATH = "/etc/ceph/ceph.pub"


def construct_registry(
    cls, registry: str, json_file: bool = False, ibm_build: bool = False
):
    """
    Construct registry credentials for bootstrapping cluster

    Args:
        cls (CephAdmin): class object
        registry (Str): registry name
        json_file (Bool): registry credentials in JSON file (default:False)
        ibm_build: flag to fetch IBM registry creds

    Example::

        json_file:
            - False : Constructs registry credentials for bootstrap
            - True  : Creates file with registry name attached with it,
                      and saved as /tmp/<registry>.json file.

    Returns:
        constructed string of registry credentials ( Str )
    """
    # Todo: Retrieve credentials based on registry name
    build_type = "ibm" if ibm_build else "rh"

    _config = get_cephci_config()
    cdn_cred = _config.get(
        f"{build_type}_registry_credentials", _config["cdn_credentials"]
    )
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
                True:   use the latest image from test config
                False:  do not use the latest image from test config,
                        and also indicates usage of default image from cephadm
                        source-code.

            # Install a released version unavailable in CDN
                config:
                  command: bootstrap
                  args:
                    rhcs-version: 5.0
                    release: <ga | z1 | z1-async1>
                    mon-ip: <node-name>
        """
        self.cluster.setup_ssh_keys()

        # Initialize the build manifest object
        manifest_obj = self.config["manifest"]

        # Process the test input
        args = config.get("args")
        custom_repo = args.pop("custom_repo", "")
        custom_image = args.pop("custom_image", True)

        build_type = self.config.get("build_type")
        rhbuild = self.config.get("rhbuild")
        base_url = self.config.get("base_url")
        cloud_type = self.config.get("cloud-type", "openstack")
        ibm_build = self.config.get("ibm_build", False)

        # Support installation of the baseline cluster whose version is not available in
        # CDN. This is primarily used for an upgrade scenario. This support is currently
        # available only for RH network.
        _ceph_version = args.pop("rhcs-version", None)
        _manifest_section = args.pop("release", None)
        _target_platform = args.pop("platform", self.config["platform"])
        if _ceph_version and _manifest_section:
            manifest_obj = CephTestManifest(
                product=self.config["product"],
                release=_ceph_version,
                build_type=_manifest_section,
                platform=_target_platform,
            )
            self.config["base_url"] = manifest_obj.repository
            self.config["container_image"] = manifest_obj.ceph_image
            rhbuild = f"{manifest_obj.ceph_version}-{manifest_obj.platform}"
            base_url = manifest_obj.repository

        self.cluster.rhcs_version = manifest_obj.release

        if build_type == "upstream" or manifest_obj.product == "community":
            self.setup_upstream_repository()
        elif build_type == "cdn" or custom_repo.lower() == "cdn":
            custom_image = False
            self.cluster.use_cdn = True
            self.set_cdn_tool_repo(_ceph_version, manifest_obj)
        elif build_type == "released" and base_url == manifest_obj.repository:
            custom_image = False
            self.cluster.use_cdn = True
            self.set_cdn_tool_repo()
        elif custom_repo:
            self.set_tool_repo(repo=custom_repo)
        else:
            for node in self.cluster.get_nodes():
                setup_repos(
                    ceph=node,
                    base_url=base_url,
                    platform=manifest_obj.platform,
                    repos=["Tools"],
                    cloud_type=cloud_type,
                    ibm_build=ibm_build,
                )

        ansible_run = config.get("cephadm-ansible", None)
        if ansible_run:
            cephadm_ansible = CephadmAnsible(cluster=self.cluster)
            cephadm_ansible.execute_playbook(
                playbook=ansible_run["playbook"],
                extra_vars=ansible_run.get("extra-vars"),
                extra_args=ansible_run.get("extra-args"),
            )
        elif base_url != manifest_obj.repository:
            self.install()
        else:
            _os_major = manifest_obj.platform.split("-")[-1]
            _ceph_version = manifest_obj.ceph_version
            # * is to enable any test hotfix provided
            _rpm_version = f"2:{_ceph_version}*.el{_os_major}cp"
            self.install(**{"rpm_version": _rpm_version})

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
        if registry_url or manifest_obj.product == "ibm":
            cmd += construct_registry(
                self,
                registry_url,
                ibm_build=True if manifest_obj.product == "ibm" else False,
            )

        registry_json = args.pop("registry-json", None)
        if registry_json:
            cmd += construct_registry(
                self,
                registry_json,
                json_file=True,
                ibm_build=True if manifest_obj.product == "ibm" else False,
            )

        # Generate dashboard certificate and key if bootstrap cli
        # have this options as dashboard-key and dashboard-crt
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
        mon_node = get_node_by_id(
            self.cluster, args.pop("mon-ip", self.installer.node.shortname)
        )
        if not mon_node:
            raise ResourceNotFoundError(f"Unknown {mon_node} node name.")
        cmd += f" --mon-ip {mon_node.ip_address}"

        # Bootstrap with Ceph service specification
        specs = args.get("apply-spec")
        if specs:
            args["apply-spec"] = GenerateServiceSpec(
                node=self.installer, cluster=self.cluster, specs=specs
            ).create_spec_file()

        # Bootstrap with ceph config options like ceph.conf file
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
            timeout=600,
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

        # We are forcing custom images by default
        images_dict: dict[str, str] = deepcopy(manifest_obj.custom_images)

        # Ignore if CDN is provided
        if build_type == "released" or custom_repo.lower() == "cdn":
            images_dict = {}

        # Honor CLI passed arguments as it has the highest precendence
        if not _ceph_version and self.config["overrides"]:
            images_dict = dict()
            for key, value in self.config["overrides"].items():
                if not key.endswith("_image"):
                    continue

                images_dict[key] = value

        ignore_images = ["cephcsi", "nvmeof_cli", "crimson"]
        check_ignored_images = lambda image: image in ignore_images
        for image, value in images_dict.items():
            _image = image.removesuffix("_image")
            if check_ignored_images(_image):
                continue
            cmd = "cephadm shell -- ceph config set mgr"
            cmd += f" mgr/cephadm/container_image_{_image} {value}"
            self.installer.exec_command(sudo=True, cmd=cmd)

        # Set public and cluster networks if provided.
        # https://docs.ceph.com/en/latest/rados/configuration/network-config-ref/
        public_nws = self.cluster.get_public_networks()
        cluster_nws = self.cluster.get_cluster_networks()

        # Todo: Temporary fix issue for RHCEPHQE-6072
        # Todo: get network address(es) from node rather than config.
        # Todo: remove this code commit once we have network config from node_obj.
        if config.get("update_public_nw", True):
            public_nws = ",".join(
                [public_nws, get_public_network(self.cluster.get_nodes())]
            )
            public_nws = ",".join(filter(lambda x: x, list(set(public_nws.split(",")))))

        if public_nws:
            # public network config level has been changed to 'global' instead of 'mon'
            # Refer below bug trackers for respective releases and more context:
            # - 6.1z1: https://bugzilla.redhat.com/show_bug.cgi?id=2156919
            # - 6.1z9: https://bugzilla.redhat.com/show_bug.cgi?id=2314604
            # - 7.1z3: https://bugzilla.redhat.com/show_bug.cgi?id=2314606
            # - 8.0: https://bugzilla.redhat.com/show_bug.cgi?id=2314438
            # - https://access.redhat.com/solutions/7088483
            config_level = "global" if rhbuild.split(".")[0] >= "6" else "mon"
            self.shell(
                args=[
                    "ceph",
                    "config",
                    "set",
                    f"{config_level} public_network",
                    public_nws,
                ]
            )
        if cluster_nws:
            self.shell(
                args=["ceph", "config", "set", "global cluster_network", cluster_nws]
            )
        if self.cluster.rhcs_version >= LooseVersion("8.0"):
            wa_txt = """
            Disabling the balancer module as a WA for bug : https://bugzilla.redhat.com/show_bug.cgi?id=2314146
            Issue : If any mgr module based operation is performed right after mgr failover, The command execution fails
            as the module isn't loaded by mgr daemon. Issue was identified to be with Balancer module.
            Disabling automatic balancing on the cluster as a WA until we get the fix for the same.
            Disabling balancer should unblock Upgrade tests.
            Error snippet :
    Error ENOTSUP: Warning: due to ceph-mgr restart, some PG states may not be up to date
    Module 'crash' is not enabled/loaded (required by command 'crash ls'): use `ceph mgr module enable crash` to enable
            """
            logger.info(wa_txt)
            self.shell(args=["ceph balancer off"])

        # validate spec file
        if specs:
            validate_spec_services(
                self.installer, specs=specs, rhcs_version=self.cluster.rhcs_version
            )

        return out, err
