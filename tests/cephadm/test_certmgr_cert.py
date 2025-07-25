import yaml

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from ceph.utils import get_node_by_id
from utility.log import Log

log = Log(__name__)

# def generate_ssl_certificate(node, key_file_name, cert_file_name, days):
#     cmd = (
#         f"cephadm shell -- openssl req -newkey rsa:4096 -nodes -sha256 -keyout /var/lib/ceph/{key_file_name} -x509 -days {days} -out "
#         f'/var/lib/ceph/{cert_file_name} -addext "subjectAltName = DNS:{node.hostname}" '
#         f'-subj "/C=IN/ST=ST/L=XYZ/O=ORG/CN={node.hostname}"'
#     )
#     out, _ = node.exec_command(cmd=cmd, sudo=True)
#     if out:
#         log.error("Failed to create a self-signed certificate")
#         return False
#     return True


def run(ceph_cluster, **kw):
    """
    Validate cert is present through certmgr ls command options
    """
    config = kw["config"]
    installer = ceph_cluster.get_nodes(role="installer")[0]
    action = config.get("action")
    if action == "check":
        # get the certificate name
        _cert_name = config.get("cert")
        # get the flags if any
        include_cephadm_signed = config.get("include_cephadm_signed", False)
        show_details = config.get("show_details", False)
        filter_by = bool(config.get("expression"))
        expression = config.get("expression")
        out = CephAdm(installer).ceph.orch.certmgr.ls("cert", show_details, include_cephadm_signed, filter_by, expression)
        if _cert_name not in out:
            raise OperationFailedError(f'{_cert_name} is not set on the cluster')
        log.info(f'{_cert_name} is successfully set on the cluster')
        print(type(out))
        print(out)
    if action == "set":
        # get the entity name
        _entity = config.get("entity")
        # get the node where the cert needs to be set
        _node = config.get("node")
        node = get_node_by_id(ceph_cluster, _node)
        hostname= node.hostname
        # get the cert name from entity list
        out = yaml.safe_load(CephAdm(installer).ceph.orch.certmgr.ls("entity"))
        _cert_name = out['host'][_entity]['certs'][0]
        print(_cert_name)
        # get the file name for storing cert
        _cert_file = config.get("cert_file_name")
        # get the file name for storing key
        _key_file = config.get("key_file_name")
        # get the days for the certificate validity
        _days = config.get("days")
        # generate ssl certificate
        #generate_ssl_certificate(node, _key_file, _cert_file, _days)
        # set the certificate for the entity
        out = CephAdm(installer).ceph.orch.certmgr.set_cert(node=node, key_file_name=_key_file, cert_file_name=_cert_file, days=_days, certificate_name=_cert_name, hostname=hostname, cert_path=_cert_file)
        print("######################")
        print(type(out))
        print(out)
    return 0