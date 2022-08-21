"""
Contains helper functions that can used across the module.
"""
import json
import os
import tempfile
from datetime import datetime, timedelta
from distutils.version import LooseVersion
from os.path import dirname
from time import sleep
from typing import Optional

from jinja2 import Template

from ceph.ceph import CommandFailed
from ceph.utils import get_node_by_id, get_nodes_by_ids
from utility.log import Log
from utility.utils import generate_self_signed_certificate

LOG = Log(__name__)


class UnknownSpecFound(Exception):
    pass


class GenerateServiceSpec:
    """Creates the spec yaml file for deploying services and daemons using cephadm."""

    COMMON_SERVICES = [
        "mon",
        "mgr",
        "alertmanager",
        "crash",
        "grafana",
        "node-exporter",
        "prometheus",
    ]

    def __init__(self, node, cluster, specs):
        """
        Initialize the GenerateServiceSpec

        Args:
            node (CephNode): ceph node where spec file to be created
            cluster (Ceph.Ceph): ceph cluster (ceph-nodes)
            specs (Dict): service specifications

        Example::

            specs:
              - service_type: host
                address: true
                labels: apply-all-labels
                nodes:
                    - node2
                    - node3
              - service_type: mon
                placement:
                  nodes:
                    - node2
                    - node3
              - service_type: mgr
                placement:
                    count: 2
              - service_type: alertmanager
                placement:
                    count: 1
              - service_type: crash
                placement:
                    host_pattern: '*'
              - service_type: grafana
                placement:
                    count: 1
              - service_type: node-exporter
                placement:
                    host_pattern: '*'
              - service_type: prometheus
                placement:
                    count: 1
        """
        self.cluster = cluster
        self.node = node
        self.specs = specs
        self.template_path = dirname(__file__) + "/jinja_templates/"

    @staticmethod
    def get_hostname(node):
        """
        Returns Host Name of node

        Args:
            node (CephNode): node object

        Returns:
            hostname (Str)
        """
        return node.hostname

    @staticmethod
    def get_addr(node):
        """
        Returns IP Address of node

        Args:
            node (CephNode): node object

        Returns:
            IP Address (Str)
        """
        return node.ip_address

    @staticmethod
    def get_labels(node):
        """
        Returns role list of node

        Args:
            node (CephNode): node object

        Returns:
            node role list (List)
        """
        label_set = set(node.role.role_list)
        return list(label_set)

    def get_hostnames(self, node_names):
        """
        Return list of hostnames

        Args:
            node_names (List): node names

        Returns:
            list of hostanmes (List)
        """
        nodes = get_nodes_by_ids(self.cluster, node_names)
        return [node.hostname for node in nodes]

    def _get_template(self, service_type):
        """
        Return Jinja template based on the service_type

        Args:
            service_type (Str): service name (ex., "host")

        Returns:
            template
        """
        path = self.template_path + f"{service_type}.jinja"
        with open(path) as fd:
            template = fd.read()
        return Template(template)

    def generate_host_spec(self, spec):
        """
        Return hosts spec content based on host config

        Args:
            spec (Dict): hosts specification

        Returns:
            hosts_spec (Str)

        Example::

            spec:
              - service_type: host
                address: true
                labels: apply-all-labels
                nodes:
                    - node2
                    - node3
        """
        template = self._get_template("host")
        hosts = []
        address = spec.get("address")
        for node_name in spec["nodes"]:
            labels = spec.get("labels")
            host = dict()
            node = get_node_by_id(self.cluster, node_name)
            host["hostname"] = self.get_hostname(node)
            if address:
                host["address"] = self.get_addr(node)

            if labels:
                # Skipping client node, if only client label is attached
                if len(node.role.role_list) == 1 and ["client"] == node.role.role_list:
                    continue

                if isinstance(labels, str) and labels == "apply-all-labels":
                    labels = self.get_labels(node)
                host["labels"] = labels

            if spec.get("location"):
                host["location"] = spec["location"]

            hosts.append(host)

        return template.render(hosts=hosts)

    def generate_generic_spec(self, spec):
        """
        Return spec content for common services
        which is mentioned in COMMON_SERVICES::

             - mon
             - mgr
             - alertmanager
             - crash
             - grafana
             - node-exporter
             - prometheus

        Args:
            spec (Dict): common service spec config

        Returns:
            service_spec

        Example::

            spec:
              - service_type: mon
                unmanaged: boolean    # true or false
                placement:
                  count: 2
                  label: "mon"
                  host_pattern: "*"   # either hosts or host_pattern
                  nodes:
                    - node2
                    - node3
        """
        template = self._get_template("common_svc_template")
        node_names = spec["placement"].pop("nodes", None)
        if node_names:
            spec["placement"]["hosts"] = self.get_hostnames(node_names)

        return template.render(spec=spec)

    def generate_osd_spec(self, spec):
        """
        Return spec content for osd service

        Args:
            spec (Dict): osd service spec config

        Returns:
            service_spec (Str)

        Example::

            spec:
              - service_type: osd
                unmanaged: boolean    # true or false

                placement:
                  host_pattern: "*"   # either hosts or host_pattern
                  nodes:
                    - node2
                    - node3
                data_devices:
                    all: boolean      # true or false
                encrypted: boolean    # true or false

            spec:
              - service_type: osd
                unmanaged: boolean    # true or false
                placement:
                  host_pattern: "*"   # either hosts or host_pattern
                  nodes:
                    - node2
                    - node3
                spec:
                    data_devices:         # all - consider all device/LVs from node
                        paths: all        # else whatever values provided from config.
                    encrypted: boolean    # true or false
                extra_container_args:
                    - "--cpus=2"
        """
        template = self._get_template("osd")
        node_names = spec["placement"].pop("nodes", None)

        if node_names:
            spec["placement"]["hosts"] = self.get_hostnames(node_names)
        for item, values in spec["spec"].items():
            if item in ["data_devices", "db_devices", "wal_devices"]:
                if values.get("paths"):
                    if values["paths"] == "all":
                        devs = []
                        for host in spec["placement"]["hosts"]:
                            node = get_node_by_id(self.cluster, host)
                            devs.extend(
                                [
                                    i if isinstance(i, str) else i.path
                                    for i in node.volume_list
                                ]
                            )
                        spec["spec"][item]["paths"] = [i for i in set(devs)]

        return template.render(spec=spec)

    def generate_mds_spec(self, spec):
        """
        Return spec content for mds service

        Args:
            spec (Dict): mds service spec config

        Returns:
            service_spec (Str)

        Example::

            spec:
              - service_type: mds
                service_id: cephfs
                unmanaged: boolean    # true or false
                placement:
                  host_pattern: "*"   # either hosts or host_pattern
                  nodes:
                    - node2
                    - node3
                  label: mds

        :Note: make sure volume is already created.

        """
        template = self._get_template("mds")
        node_names = spec["placement"].pop("nodes", None)
        if node_names:
            spec["placement"]["hosts"] = self.get_hostnames(node_names)

        return template.render(spec=spec)

    def generate_nfs_spec(self, spec):
        """
        Return spec content for nfs service

        Args:
            spec (Dict): mds service spec config

        Returns:
            service_spec (Str)

        Example::

            spec:
              - service_type: nfs
                service_id: nfs-name
                unmanaged: boolean    # true or false
                placement:
                  host_pattern: "*"   # either hosts or host_pattern
                  nodes:
                    - node2
                    - node3
                  label: nfs
                spec:
                  pool: pool-name
                  namespace: namespace-name

        :Note: make sure pool is already created.
        """
        template = self._get_template("nfs")
        node_names = spec["placement"].pop("nodes", None)
        if node_names:
            spec["placement"]["hosts"] = self.get_hostnames(node_names)

        return template.render(spec=spec)

    def generate_rgw_spec(self, spec):
        """
        Return spec content for rgw service

        Args:
            spec (Dict): rgw service spec config

        Returns:
            service_spec (Str)

        Example::

            spec:
              - service_type: rgw
                service_id: my-rgw
                unmanaged: boolean    # true or false
                placement:
                  host_pattern: "*"   # either hosts or host_pattern
                  nodes:
                    - node2
                    - node3
                  label: rgw
                spec:
                  rgw_frontend_port: 8080
                  rgw_realm: east
                  rgw_zone: india
                  rgw_frontend_ssl_certificate: create-cert | <contents of crt>

            contents of rgw_spec.yaml file

                service_type: rgw
                service_id: rgw.india
                placement:
                  hosts:
                    - node5
                spec:
                  ssl: true
                  rgw_frontend_ssl_certificate: |
                    -----BEGIN PRIVATE KEY------
                    ...

        :Note: make sure realm, zone group and zone is already created.

        """
        template = self._get_template("rgw")
        node_names = spec["placement"].pop("nodes", None)
        if node_names:
            spec["placement"]["hosts"] = self.get_hostnames(node_names)

        if spec["spec"].get("rgw_frontend_ssl_certificate") == "create-cert":
            subject = {
                "common_name": spec["placement"]["hosts"][0],
                "ip_address": self.cluster.get_node_by_hostname(
                    spec["placement"]["hosts"][0]
                ).ip_address,
            }
            key, cert, ca = generate_self_signed_certificate(subject=subject)
            pem = key + cert + ca
            cert_value = "|\n" + pem
            spec["spec"]["rgw_frontend_ssl_certificate"] = "\n    ".join(
                cert_value.split("\n")
            )
            LOG.debug(pem)

        return template.render(spec=spec)

    def generate_snmp_spec(self, spec):
        """
        Return spec content for snmp-destination

        Args:
            spec (Dict): snmp-destination service spec config

        Returns:
            service_spec (Str)

        Example::

        specs:
              - service_type: snmp-destination
                spec:
                  credentials:
                    snmp_v3_auth_username: myadmin
                    snmp_v3_auth_password: mypassword
        """
        template = self._get_template("snmp")
        destination_node = spec["spec"].pop("snmp_destination", None)
        node = get_node_by_id(self.cluster, destination_node)
        if destination_node:
            spec["spec"]["snmp_destination"] = self.get_addr(node) + ":162"
        node_installer = get_node_by_id(self.cluster, "node1")
        cmd = "cephadm shell ceph fsid"
        out, err = node_installer.exec_command(sudo=True, cmd=cmd)
        LOG.info(f"fsid: {out}")
        engine_id = out.replace("-", "")
        if engine_id:
            spec["spec"]["engine_id"] = engine_id
        return template.render(spec=spec)

    def generate_snmp_dest_conf(self, spec):
        """
        Return conf content for snmp-gateway service

        Args:
            spec (Dict): snmp-gateway service config

        Returns:
            destination_conf (Str)

        Example::

            spec:
                - service_type: snmp-gateway
                  service_name: snmp-gateway
                  placement:
                    count: 1
                  spec:
                    credentials:
                      snmp_v3_auth_username: <user_name>
                      snmp_v3_auth_password: <password>
                    port: 9464
                    snmp_destination: node
                    snmp_version: V3

        """
        template = self._get_template("snmp_destination")
        node = get_node_by_id(self.cluster, "node1")
        cmd = "cephadm shell ceph fsid"
        out, err = node.exec_command(sudo=True, cmd=cmd)
        LOG.info(f"fsid: {out}")
        fsid = out.replace("-", "")
        engine_id = fsid[0:32]
        if engine_id:
            spec["spec"]["engine_id"] = engine_id
        LOG.info(f"fsid:{engine_id}")

        return template.render(spec=spec)

    def generate_ingress_spec(self, spec):
        """
        Return spec content for ha proxy ingress service

        Args:
            spec (Dict): ha proxy ingress service spec config

        Returns:
            service_spec (Str)

        Example::

            spec:
              - service_type: ingress
                service_id: rgw.my-rgw
                unmanaged: boolean    # true or false
                placement:
                  host_pattern: "*"   # either hosts or host_pattern
                  nodes:
                    - node2
                    - node3
                  label: rgw
                spec:
                  backend_service: rgw.ceph-scale-test-y7nmci-node2
                  virtual_ip: 10.0.208.0/22
                  frontend_port: 8000
                  monitor_port: 1967

        :Note: make sure rgw service is already created.

        """
        template = self._get_template("ingress")
        node_names = spec["placement"].pop("nodes", None)
        if node_names:
            spec["placement"]["hosts"] = self.get_hostnames(node_names)

        return template.render(spec=spec)

    def _get_render_method(self, service_type):
        """
        Return render definition based on service_type

        Args:
            service_type (Str): service name

        Returns:
            method (Func)
        """
        render_definitions = {
            "host": self.generate_host_spec,
            "osd": self.generate_osd_spec,
            "mds": self.generate_mds_spec,
            "nfs": self.generate_nfs_spec,
            "rgw": self.generate_rgw_spec,
            "snmp-gateway": self.generate_snmp_spec,
            "snmp-destination": self.generate_snmp_dest_conf,
            "ingress": self.generate_ingress_spec,
        }

        try:
            if service_type in self.COMMON_SERVICES:
                return self.generate_generic_spec
            return render_definitions[service_type]
        except KeyError:
            raise NotImplementedError

    def create_spec_file(self):
        """
        Create spec file based on spec config and return file name

        Returns:
            temp_filename (Str)

        """
        spec_content = ""
        for spec in self.specs:
            method = self._get_render_method(spec["service_type"])
            if not method:
                raise UnknownSpecFound(f"unknown spec found - {spec}")
            spec_content += method(spec=spec)

        LOG.info(f"Spec yaml file content:\n{spec_content}")
        # Create spec yaml file
        temp_file = tempfile.NamedTemporaryFile(suffix=".yaml")
        spec_file = self.node.node.remote_file(
            sudo=True, file_name=temp_file.name, file_mode="w"
        )
        spec_file.write(spec_content)
        spec_file.flush()

        return temp_file.name

    def create_snmp_conf_file(self):
        """
        Create snmp conf file based on spec config and return file name

        Returns:
            temp_filename (Str)

        """
        spec_content = ""
        for spec in self.specs:
            method = self._get_render_method(spec["service_type"])
            if not method:
                raise UnknownSpecFound(f"unknown spec found - {spec}")
            spec_content += method(spec=spec)
        LOG.info(f"SNMP Conf file content:\n{spec_content}")
        temp_file = tempfile.NamedTemporaryFile(suffix=".yaml")
        conf_file = self.node.remote_file(
            sudo=True, file_name=temp_file.name, file_mode="w"
        )
        conf_file.write(spec_content)
        conf_file.flush()
        return temp_file.name


def create_ceph_config_file(node, config):
    """
    Create config file based on config options and return file name

    Returns:
        temp_filename (Str)

    """
    path = dirname(__file__) + "/jinja_templates/config.jinja"
    with open(path) as fd:
        template = fd.read()

    conf_content = Template(template).render(config=config)

    LOG.info(f"Conf yaml file content:\n{conf_content}")
    # Create conf yaml file
    temp_file = tempfile.NamedTemporaryFile(suffix=".yaml")
    conf_file = node.node.remote_file(
        sudo=True, file_name=temp_file.name, file_mode="w"
    )
    conf_file.write(conf_content)
    conf_file.flush()

    return temp_file.name


def get_cluster_state(cls, commands=None):
    """
    fetch cluster state using commands provided along
    with the default set of commands::

        - ceph status
        - ceph orch ls -f json-pretty
        - ceph orch ps -f json-pretty
        - ceph health detail -f yaml

    Args:
        cls (CephAdmin): ceph.ceph_admin instance with shell access
        commands (List): list of commands

    """
    __CLUSTER_STATE_COMMANDS = [
        "ceph status",
        "ceph orch host ls",
        "ceph orch ls -f json-pretty",  # https://bugzilla.redhat.com/show_bug.cgi?id=2068366
        "ceph orch ps -f json-pretty",
        "ceph health detail -f yaml",
        "ceph mgr dump",  # https://bugzilla.redhat.com/show_bug.cgi?id=2033165#c2
        "ceph mon stat",
    ]

    commands = commands if commands else list()

    __CLUSTER_STATE_COMMANDS.extend(commands)

    for cmd in __CLUSTER_STATE_COMMANDS:
        out, err = cls.shell(args=[cmd])
        LOG.info("STDOUT:\n %s" % out)
        LOG.error("STDERR:\n %s" % err)


def get_host_osd_map(cls):
    """
    Method to get the OSDs deployed in each of the hosts
    Args:
        cls: cephadm instance object

    Returns:
        Dictionary with host names as keys and osds deployed as value list
    """
    out, _ = cls.shell(args=["ceph", "osd", "tree", "-f", "json"])
    osd_obj = json.loads(out)
    osd_dict = {}
    for obj in osd_obj["nodes"]:
        if obj["type"] == "host":
            osd_dict[obj["name"]] = obj["children"]

    return osd_dict


def get_host_daemon_map(cls):
    """
    Method to get the daemons deployed in each of the hosts
    Args:
        cls: cephadm instance object

    Returns:
        Dictionary with host names as keys and names of the daemons deployed as value
        list
    """
    out, _ = cls.shell(args=["ceph", "orch", "ps", "-f", "json"])
    daemon_obj = json.loads(out)
    daemon_dict = dict()
    for daemon in daemon_obj:
        daemon_name = daemon["daemon_type"] + "." + daemon["daemon_id"]
        if daemon["hostname"] in daemon_dict.keys():
            daemon_dict[daemon["hostname"]].append(daemon_name)
        else:
            daemon_dict[daemon["hostname"]] = [daemon_name]

    return daemon_dict


def get_hosts_deployed(cls):
    """
    Method to get all the hosts deployed in the cluster
    Args:
        cls: cephadm instance object

    Returns:
        List of the names of hosts deployed in the cluster
    """
    out, _ = cls.shell(args=["ceph", "orch", "host", "ls", "-f", "json"])
    hosts = list()
    host_obj = json.loads(out)
    for host in host_obj:
        hosts.append(host["hostname"])

    return hosts


def file_or_path_exists(node, file_or_path):
    """Method to check abs path exists.

    Args:
        node: node object where file should be exists
        file_or_path: ceph file or directory path

    Returns:
        boolean
    """
    try:
        out, _ = node.exec_command(cmd=f"ls -l {file_or_path}", sudo=True)
        LOG.info(f"Output : {out}")
        return True
    except CommandFailed as err:
        LOG.error(f"Error: {err}")

    return False


def monitoring_file_existence(node, file_or_path, file_exist=True, timeout=180):
    """Method to monitor a file existence.

    Args:
        node: node object where file should be exists
        file_or_path: ceph file or directory path
        file_exist: checks file existence (default =True)
        timeout (Int):  In seconds, the maximum allowed time (default=180)

    Returns:
        boolean
    """
    end_time = datetime.now() + timedelta(seconds=timeout)
    while end_time > datetime.now():
        if file_exist == file_or_path_exists(node, file_or_path):
            return True
        sleep(3)
    return False


def validate_log_file_after_enable(cls):
    """
    Verify generation of log files in default log directory when logging not enabled.

    Args:
        cls: cephadm instance object

    Returns:
        boolean
    """
    out, _ = cls.shell(args=["ceph", "config", "set", "global", "log_to_file", "true"])
    out, _ = cls.shell(args=["ceph", "fsid"])
    fsid = out.strip()
    log_file_path = os.path.join("/var/log/ceph", fsid)
    daemon_dict = get_host_daemon_map(cls)
    roles_to_validate = ["mon", "mgr", "osd", "rgw", "mds"]

    daemon_valid = {
        k: [val for val in v if val.split(".")[0] in roles_to_validate]
        for (k, v) in daemon_dict.items()
    }

    for node in cls.cluster.get_nodes():
        try:
            if node.hostname not in daemon_valid.keys():
                continue
            daemons = daemon_valid[node.hostname]
            for daemon in daemons:
                file = os.path.join(log_file_path, f"ceph-{daemon}.log")
                if "rgw" in daemon:
                    file = os.path.join(log_file_path, f"ceph-client.{daemon}.log")
                LOG.info(
                    f"Verifying existence of log file {file} in host {node.hostname}"
                )
                file_exists = file_or_path_exists(node, file)
                if not file_exists:
                    LOG.error(
                        f"Log for {daemon} is not present in the node {node.ip_address}"
                    )
                    return False
            LOG.info(f"Log verification on node {node.ip_address} successful")
        except CommandFailed as err:
            LOG.error("Error: %s" % err)
            return False

    return True


def check_service_exists(
    installer,
    service_name: Optional[str] = None,
    service_type: Optional[str] = None,
    timeout: int = 1800,
    interval: int = 20,
    rhcs_version: Optional[LooseVersion] = None,
) -> bool:
    """
    Verify the provided service is running for the given list of ids.

    Args:
        installer (CephNode): ceph installer node
        service_name (str): The name of the service to be checked.
        service_type (str): The type of the service to be checked.
        timeout (int):  In seconds, the maximum allowed time (default=1800)
        interval (int): In seconds, the polling interval time (default=20)
        rhcs_version (LooseVersion):  RHCS version

    Returns:
        Boolean: True if the service and the list of daemons are running else False.

    """
    end_time = datetime.now() + timedelta(seconds=timeout)
    cmd_args = ["cephadm", "shell", "--", "ceph", "orch", "ls"]
    if service_name:
        cmd_args += ["--service_name", service_name]

    # Due to a BZ, the running field is always 0 for RGW daemon. This has been fixed in
    # the later release.
    if rhcs_version and rhcs_version == "5.0" and service_type == "rgw":
        cmd_args += ["--format", "json", "--refresh"]
    else:
        cmd_args += ["--service_type", service_type, "--format", "json", "--refresh"]

    _retries = 3  # cross-verification retries
    _count = 0
    while end_time > datetime.now():
        sleep(interval)
        out, err = installer.exec_command(
            sudo=True, cmd=" ".join(cmd_args), check_ec=True
        )
        out = json.loads(out)[0]
        running = out["status"]["running"]
        count = out["status"]["size"]

        LOG.info(f"{running}/{count} {service_name or service_type} up... retrying")

        if count + running < 1:
            continue

        if count == running and _count == count:
            if _retries < 1:
                return True
            _retries -= 1

        if _count != count:
            _count = count
            _retries = 3

    # Identify the failure
    out, err = installer.exec_command(sudo=True, cmd=" ".join(cmd_args))
    out = json.loads(out)
    LOG.error(f"{service_name or service_type} failed with \n{out[0].get('events')}")
    return False


def validate_spec_services(installer, specs, rhcs_version) -> None:
    """
    This method ensures the services provided in the spec file are in running state.

    Args:
        installer       The node having cephadm package
        specs           A list of CephAdm spec file
        rhcs_version    The version of Ceph
    """
    LOG.info("Validating spec services")
    for spec in specs:
        svc_type = spec["service_type"]
        svc_id = spec.get("service_id")
        svc_name = None

        # continue if it is host
        if "host" == svc_type:
            continue

        if svc_id:
            svc_name = f"{svc_type}.{spec['service_id']}"

        if not check_service_exists(
            installer=installer,
            service_name=svc_name,
            service_type=svc_type,
            rhcs_version=rhcs_version,
        ):
            raise Exception(f"{svc_name or svc_type} service deployment failed!!!")
