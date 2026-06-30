from json import loads

import yaml

from ceph.utils import get_node_by_id
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Validate certmgr commands for keys stored in the cluster
    """
    config = kw["config"]
    installer = ceph_cluster.get_nodes(role="installer")[0]
    action = config.get("action")
    if action == "ls":
        # get the key name
        _key_name = config.get("key")
        # get the flags if any
        include_cephadm_signed = config.get("include_cephadm_signed", False)
        filter_by = bool(config.get("expression"))
        expression = config.get("expression")
        out = CephAdm(installer).ceph.orch.certmgr.ls(
            "key", include_cephadm_signed, filter_by, expression
        )
        if _key_name not in out:
            raise OperationFailedError(f"{_key_name} is not set on the cluster")
        log.info(f"{_key_name} is successfully set on the cluster")
    if action == "set":
        # get the entity name
        _entity = config.get("entity")
        # get the scope of the certificate
        _scope = config.get("scope")
        # get the service for which key needs to be set
        _service_name = loads(
            CephAdm(installer).ceph.orch.ls(service_type=_entity, format="json")
        )[0]["service_name"]
        # get the host name from the entity list
        _node = config.get("node")
        hostname = get_node_by_id(ceph_cluster, _node).hostname
        # get the key name from entity list
        out = yaml.safe_load(CephAdm(installer).ceph.orch.certmgr.ls("entity"))
        _key_name = out[_scope][_entity]["keys"][0]
        # check if user has provided value for cert/key or needs to generate ssl certificate key
        generate_ssl_key = config.get("generate_ssl_cert")
        key_value = config.get("key_value")
        if not generate_ssl_key and not key_value:
            raise ConfigError(
                "Mandatory config missing: please provide if want to generate ssl key or provide a key to be set"
            )
        # check if key needs to be generated
        if generate_ssl_key:
            if not config.get("key_file_name"):
                raise ConfigError("Mandatory config 'key_file_name' not provided")
            # get the file name for storing key
            _key_file = config.get("key_file_name")
            # set the ssl key
            out = CephAdm(installer).ceph.orch.certmgr.set_key(
                key_name=_key_name,
                scope=_scope,
                hostname=hostname,
                service_value=_service_name,
                key_file_name=_key_file,
            )
            if "set correctly" not in out:
                raise OperationFailedError(f"Failed to set key for {_entity}")
            log.info(f"Successfully set the key for {_entity} using CLI command")
        if key_value:
            out = CephAdm(installer).ceph.orch.certmgr.set_key(
                key_name=_key_name, key_value=key_value, hostname=hostname
            )
    if action == "get":
        # get the entity name
        _entity = config.get("entity")
        # get the scope of the certificate
        _scope = config.get("scope")
        # get the service for which key needs to be checked
        _service_name = None
        if _scope == "service":
            _service_name = loads(
                CephAdm(installer).ceph.orch.ls(service_type=_entity, format="json")
            )[0]["service_name"]
        # get the node where the key needs to be checked
        hostname = None
        if _scope == "host":
            _node = config.get("node")
            hostname = get_node_by_id(ceph_cluster, _node).hostname
        # get the key name from entity list
        out = yaml.safe_load(CephAdm(installer).ceph.orch.certmgr.ls("entity"))
        _key_name = out[_scope][_entity]["keys"][0]
        # get the flags if any
        no_exception_when_missing = config.get("no_exception_when_missing", False)
        # retrieve the certificate
        out = CephAdm(installer).ceph.orch.certmgr.get_key(
            key_name=_key_name,
            scope=_scope,
            hostname=hostname,
            service_value=_service_name,
            no_exception_when_missing=no_exception_when_missing,
        )
        if "BEGIN PRIVATE KEY" not in out:
            raise OperationFailedError(f"Failed to get key for {_entity}")
        log.info(f"Successfully retrieved the key for {_entity} using CLI command")
    if action == "remove":
        # get the entity name
        _entity = config.get("entity")
        # get the scope of the certificate
        _scope = config.get("scope")
        # get the service for which key needs to be removed
        _service_name = None
        if _scope == "service":
            _service_name = loads(
                CephAdm(installer).ceph.orch.ls(service_type=_entity, format="json")
            )[0]["service_name"]
        # get the node from where the key needs to be removed
        hostname = None
        if _scope == "host":
            _node = config.get("node")
            hostname = get_node_by_id(ceph_cluster, _node).hostname
        # get the key name from entity list
        out = yaml.safe_load(CephAdm(installer).ceph.orch.certmgr.ls("entity"))
        _key_name = out[_scope][_entity]["keys"][0]
        # remove the certificate
        out = CephAdm(installer).ceph.orch.certmgr.rm_key(
            key_name=_key_name,
            scope=_scope,
            hostname=hostname,
            service_value=_service_name,
        )
        if "removed correctly" not in out:
            raise OperationFailedError(f"Failed to remove private key for {_entity}")
    return 0
