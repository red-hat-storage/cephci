from cli import Cli
from utility.log import Log

log = Log(__name__)


class CertMgr(Cli):
    def __init__(self, nodes, base_cmd):
        super(CertMgr, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} certmgr"

    def ls(self, value, show_details=False, include_cephadm_signed=False, filter_by=False, expression=None):
        """
        List hosts
        Args:
            value (str): item to be listed
                Supported values:
                    cert (str): list all available certificates managed by the orchestrator
                    key (str): list all private keys associated with managed certificates
                    entity (str): list all entities associated with certificates
            show-details (bool): whether to show detailed information
            include-cephadm-signed (bool): whether to inlcude certificates signed by cephadm
            filter-by (bool): this argument provides advanced certificates filtering.
            expression (str): expression to use for --filter-by flag
                e.g.
                ceph orch certmgr cert ls --filter-by "scope=service,status=expiring"
                ceph orch certmgr cert ls --include-cephadm-signed --filter-by "name=rgw*,status=valid"
        """
        cmd = f"{self.base_cmd} {value} ls"
        if show_details:
            cmd += " --show-details"
        if include_cephadm_signed:
            cmd += " --include-cephadm-signed"
        if filter_by and expression:
            cmd += f' --filter-by {expression}'
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def set_cert(self, node, key_file_name, cert_file_name, days, certificate_name, cert_value=None, service_value=None, hostname=None, cert_path=None):
        """
        List hosts
        Args:
            certificate_name (str): certificate to be set/updated
            cert_value (str): value to be set as cert
            service_value (str): service name if its a service level certificate
            hostname (str): hostname if its a host level certificate

            filter-by (bool): this argument provides advanced certificates filtering.
            expression (str): expression to use for --filter-by flag
                e.g.
                ceph orch certmgr cert ls --filter-by "scope=service,status=expiring"
                ceph orch certmgr cert ls --include-cephadm-signed --filter-by "name=rgw*,status=valid"
        """
        cmd = f"""cephadm shell -- bash -c '
        openssl req -newkey rsa:4096 -nodes -sha256 \
            -keyout {key_file_name} \
            -x509 -days {days} \
            -out {cert_file_name} \
            -addext "subjectAltName = DNS:{node.hostname}" \
            -subj "/C=IN/ST=ST/L=XYZ/O=ORG/CN={node.hostname}" && \
        ceph orch certmgr cert set {certificate_name}"""
        if cert_value:
            cmd += f' --cert {cert_value}'
        if service_value:
            cmd += f' --service_name {service_value}'
        if hostname:
            cmd += f' --hostname {hostname}'
        if cert_path:
            cmd += f' -i {cert_path}'
        cmd += "'"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out