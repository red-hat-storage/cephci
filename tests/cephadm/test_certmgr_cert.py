from json import loads

import yaml

from ceph.utils import get_node_by_id
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError, UnexpectedStateError
from utility.log import Log

log = Log(__name__)


def _generate_ssl_cert(node, key_file_name, cert_file_name, days, hostname):
    """
    Generate SSL cert-key pair
    """
    cmd = f"""
            openssl req -newkey rsa:4096 -nodes -sha256 \
                -keyout {key_file_name} \
                -x509 -days {days} \
                -out {cert_file_name} \
                -addext "subjectAltName = DNS:{hostname}" \
                -subj "/C=IN/ST=ST/L=XYZ/O=ORG/CN={hostname}"
                """
    out, _ = node.exec_command(sudo=True, cmd=cmd)
    if out:
        log.error("Failed to create a self-signed certificate")
        return False
    return True


def run(ceph_cluster, **kw):
    """
    Validate certmgr commands for certificates stored in the cluster
    """
    config = kw["config"]
    installer = ceph_cluster.get_nodes(role="installer")[0]
    action = config.get("action")
    if action == "ls":
        # get the certificate name
        _cert_name = config.get("cert")
        # get the flags if any
        include_cephadm_signed = config.get("include_cephadm_signed", False)
        show_details = config.get("show_details", False)
        filter_by = bool(config.get("expression"))
        expression = config.get("expression")
        out = CephAdm(installer).ceph.orch.certmgr.ls(
            "cert", show_details, include_cephadm_signed, filter_by, expression
        )
        if _cert_name not in out:
            raise OperationFailedError(f"{_cert_name} is not set on the cluster")
        log.info(f"{_cert_name} is successfully set on the cluster")
    if action == "set":
        # get the entity name
        _entity = config.get("entity")
        # get the scope of the certificate
        _scope = config.get("scope")
        # get the service for which cert needs to be set
        _service_name = loads(
            CephAdm(installer).ceph.orch.ls(service_type=_entity, format="json")
        )[0]["service_name"]
        # get the node where the cert needs to be set
        _node = config.get("node")
        hostname = get_node_by_id(ceph_cluster, _node).hostname
        # get the cert name from entity list
        out = yaml.safe_load(CephAdm(installer).ceph.orch.certmgr.ls("entity"))
        _cert_name = out[_scope][_entity]["certs"][0]
        # check if user has provided value for cert/key or needs to generate ssl certificate
        generate_ssl_cert = config.get("generate_ssl_cert")
        cert_value = config.get("cert_value")
        if not generate_ssl_cert and not cert_value:
            raise ConfigError(
                "Mandatory config missing: please provide if want to generate ssl cert or provide a cert to be set"
            )
        # check if cert needs to be generated
        if generate_ssl_cert:
            if not config.get("cert_file_name"):
                raise ConfigError("Mandatory config 'cert_file_name' not provided")
            if not config.get("key_file_name"):
                raise ConfigError("Mandatory config 'key_file_name' not provided")
            # get the file name for storing cert
            _cert_file = config.get("cert_file_name")
            # get the file name for storing key
            _key_file = config.get("key_file_name")
            # get the days for the certificate validity
            _days = config.get("days")
            # generate certificate
            _generate_ssl_cert(
                node=installer,
                key_file_name=_key_file,
                cert_file_name=_cert_file,
                days=_days,
                hostname=hostname,
            )
            # set the certificate for the entity
            out = CephAdm(installer).ceph.orch.certmgr.set_cert(
                certificate_name=_cert_name,
                scope=_scope,
                hostname=hostname,
                service_value=_service_name,
                cert_file_name=_cert_file,
            )
            if "set correctly" not in out:
                raise OperationFailedError(f"Failed to set certificate for {_entity}")
            log.info(
                f"Successfully set the certificate for {_entity} using CLI command"
            )
        if cert_value:
            out = CephAdm(installer).ceph.orch.certmgr.set_cert(
                certificate_name=_cert_name, cert_value=cert_value, hostname=hostname
            )
    if action == "get":
        # get the entity name
        _entity = config.get("entity")
        # get the scope of the certificate
        _scope = config.get("scope")
        # get the service for which cert needs to be checked
        _service_name = None
        if _scope == "service":
            _service_name = loads(
                CephAdm(installer).ceph.orch.ls(service_type=_entity, format="json")
            )[0]["service_name"]
        # get the node where the cert needs to be checked
        hostname = None
        if _scope == "host":
            _node = config.get("node")
            hostname = get_node_by_id(ceph_cluster, _node).hostname
        # get the cert name from entity list
        out = yaml.safe_load(CephAdm(installer).ceph.orch.certmgr.ls("entity"))
        _cert_name = out[_scope][_entity]["certs"][0]
        # get the flags if any
        no_exception_when_missing = config.get("no_exception_when_missing", False)
        # retrieve the certificate
        out = CephAdm(installer).ceph.orch.certmgr.get_cert(
            certificate_name=_cert_name,
            scope=_scope,
            hostname=hostname,
            service_value=_service_name,
            no_exception_when_missing=no_exception_when_missing,
        )
        if "BEGIN CERTIFICATE" not in out:
            raise OperationFailedError(f"Failed to get certificate for {_entity}")
        log.info(
            f"Successfully retrieved the certificate for {_entity} using CLI command"
        )
    if action == "check":
        out = CephAdm(installer).ceph.orch.certmgr.check()
        if "No issues detected" not in out:
            raise UnexpectedStateError("Cluster has expired/invalid certificated")
    if action == "reload":
        out = CephAdm(installer).ceph.orch.certmgr.reload()
        if "OK" not in out:
            raise OperationFailedError(
                "Failed to reload reload the certificate manager "
                "configuration from the Monitors"
            )
    if action == "remove":
        # get the entity name
        _entity = config.get("entity")
        # get the scope of the certificate
        _scope = config.get("scope")
        # get the service for which cert needs to be removed
        _service_name = None
        if _scope == "service":
            _service_name = loads(
                CephAdm(installer).ceph.orch.ls(service_type=_entity, format="json")
            )[0]["service_name"]
        # get the node from where the cert needs to be removed
        hostname = None
        if _scope == "host":
            _node = config.get("node")
            hostname = get_node_by_id(ceph_cluster, _node).hostname
        # get the cert name from entity list
        out = yaml.safe_load(CephAdm(installer).ceph.orch.certmgr.ls("entity"))
        _cert_name = out[_scope][_entity]["certs"][0]
        # remove the certificate
        out = CephAdm(installer).ceph.orch.certmgr.rm_cert(
            certificate_name=_cert_name,
            scope=_scope,
            hostname=hostname,
            service_value=_service_name,
        )
        if "removed correctly" not in out:
            raise OperationFailedError(
                f"Failed to remove user certificate for {_entity}"
            )
    return 0
