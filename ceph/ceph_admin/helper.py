"""
Contains helper functions that can used across the module.
"""
import logging
import tempfile
from os.path import dirname

from jinja2 import Template

from ceph.utils import get_node_by_id, get_nodes_by_ids

LOG = logging.getLogger(__name__)


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
            node: ceph node where spec file to be created
            cluster: ceph cluster (ceph-nodes)
            specs: service specifications

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
        return node.shortname

    @staticmethod
    def get_addr(node):
        return node.ip_address

    @staticmethod
    def get_labels(node):
        return node.role.role_list

    def _get_template(self, service_type):
        """
        Return Jinja template based on the service_type
        Args:
            service_type: service name (ex., "host")
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
        args:
            spec: hosts specification

        spec:
          - service_type: host
            address: true
            labels: apply-all-labels
            nodes:
                - node2
                - node3
        Returns:
            hosts_spec
        """
        template = self._get_template("host")
        hosts = []
        address = spec.get("address")
        labels = spec.get("labels")
        for node_name in spec["nodes"]:
            host = dict()
            node = get_node_by_id(self.cluster, node_name)
            host["hostname"] = self.get_hostname(node)
            if address:
                host["address"] = self.get_addr(node)
            if labels:
                host["labels"] = self.get_labels(node)
            hosts.append(host)

        return template.render(hosts=hosts)

    def generate_generic_spec(self, spec):
        """
        Return spec content for common services
        which is mentioned in COMMON_SERVICES
         - mon
         - mgr
         - alertmanager
         - crash
         - grafana
         - node-exporter
         - prometheus

        Args:
            spec: common service spec config

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
        Returns:
            service_spec
        """
        template = self._get_template("common_svc_template")
        node_names = spec["placement"].pop("nodes", None)
        if node_names:
            spec["placement"]["hosts"] = []
            nodes = get_nodes_by_ids(self.cluster, node_names)
            for node in nodes:
                spec["placement"]["hosts"].append(node.shortname)

        return template.render(spec=spec)

    def _get_render_method(self, service_type):
        """
        Return render definition based on service_type
        Args:
            service_type: service name
        Returns:
            method
        """
        render_definitions = {
            "host": self.generate_host_spec,
        }

        try:
            if service_type in self.COMMON_SERVICES:
                return self.generate_generic_spec
            return render_definitions[service_type]
        except KeyError:
            raise NotImplementedError

    def create_spec_file(self):
        """
        start to generate spec file based on spec config

        Returns:
            temp_filename
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


def get_cluster_state(cls, commands=[]):
    """
    fetch cluster state using commands provided along
    with the default set of commands

    - ceph status
    - ceph orch ls -f json-pretty
    - ceph orch ps -f json-pretty
    - ceph health detail -f yaml

    Args:
        cls: ceph.ceph_admin instance with shell access
        commands: list of commands

    """
    __CLUSTER_STATE_COMMANDS = [
        "ceph status",
        "ceph orch host ls",
        "ceph orch ls -f yaml",
        "ceph orch ps -f json-pretty",
        "ceph health detail -f yaml",
    ]

    __CLUSTER_STATE_COMMANDS.extend(commands)

    for cmd in __CLUSTER_STATE_COMMANDS:
        out, err = cls.shell(args=[cmd])
        LOG.info("STDOUT:\n %s" % out)
        LOG.error("STDERR:\n %s" % err)
