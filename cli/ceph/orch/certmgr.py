from cli import Cli
from utility.log import Log

log = Log(__name__)


class CertMgr(Cli):
    def __init__(self, nodes, base_cmd):
        super(CertMgr, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} certmgr"

    def ls(
        self,
        value,
        show_details=False,
        include_cephadm_signed=False,
        filter_by=False,
        expression=None,
    ):
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
            cmd += f" --filter-by {expression}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def set_cert(
        self,
        certificate_name,
        scope,
        cert_file_name=None,
        cert_value=None,
        service_value=None,
        hostname=None,
    ):
        """
        Add/replace an existing certificate
        Args:
            key_file_name (str): pem file name to store the generated key
            cert_file_name (str): cert file name to store the generated certificate
            days (str): days for which the certificate is valid
            certificate_name (str): name of certificate to be set/updated
            cert_value (str): value to be set as cert
            service_value (str): service name if its a service level certificate
            hostname (str): hostname if its a host level certificate
        """
        if cert_value:
            cmd = f"{self.base_cmd} key set {certificate_name} -- {cert_value}"
            if scope == "service":
                cmd += f" --service_name {service_value}"
            if scope == "host":
                cmd += f" --hostname {hostname}"
            out = self.execute(sudo=True, cmd=cmd)
        else:
            cmd = f"""cephadm shell -v /root/{cert_file_name}:/etc/ceph/{cert_file_name} \
            -- bash -c 'ceph orch certmgr cert set {certificate_name} -i /etc/ceph/{cert_file_name}"""
            if scope == "service":
                cmd += f" --service_name {service_value}"
            if scope == "host":
                cmd += f" --hostname {hostname}"
            cmd += "'"
            out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def get_cert(
        self,
        certificate_name,
        scope,
        service_value=None,
        hostname=None,
        no_exception_when_missing=False,
    ):
        """
        Retrieve the content of a specific certificate
        Args:
            certificate_name (str): name of certificate to be retrived
            service_value (str): service name if it is a service level certificate
            hostname (str): hostname if it is a host level certificate
        """
        cmd = f"{self.base_cmd} cert get {certificate_name}"
        if scope == "service":
            cmd += f" --service_name {service_value}"
        if scope == "host":
            cmd += f" --hostname {hostname}"
        if no_exception_when_missing:
            cmd += " --no-exception-when-missing"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def rm_cert(self, certificate_name, scope, service_value=None, hostname=None):
        """
        Remove an existing certificate
        Args:
            certificate_name (str): name of certificate to be removed
            service_value (str): service name if it is a service level certificate
            hostname (str): hostname if it is a host level certificate
        """
        cmd = f"{self.base_cmd} cert rm {certificate_name}"
        if scope == "service":
            cmd += f" --service_name {service_value}"
        if scope == "host":
            cmd += f" --hostname {hostname}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def reload(self):
        """
        Reload the certificate manager configuration from the Monitors

        Ensures that any changes made to certificate configurations are
        applied immediately without requiring a service restart
        """
        cmd = f"{self.base_cmd} reload"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def check(self):
        """
        Check the status and validity of certificates
        """
        cmd = f"{self.base_cmd} cert check"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def set_key(
        self,
        key_file_name,
        key_name,
        scope,
        key_value=None,
        service_value=None,
        hostname=None,
    ):
        """
        Add/replace an existing certificate
        Args:
            key_file_name (str): pem file name to store the generated key
            cert_file_name (str): cert file name to store the generated certificate
            days (str): days for which the key is valid
            key_name (str): name of key to be set/updated
            key_value (str): value to be set as key
            service_value (str): service name if its a service level key
            hostname (str): hostname if its a host level key
        """
        if key_value:
            cmd = f"{self.base_cmd} key set {key_name} -- {key_value}"
            if scope == "service":
                cmd += f" --service_name {service_value}"
            if scope == "host":
                cmd += f" --hostname {hostname}"
            out = self.execute(sudo=True, cmd=cmd)
        else:
            cmd = f"""cephadm shell -v /root/{key_file_name}:/etc/ceph/{key_file_name} \
            -- bash -c 'ceph orch certmgr key set {key_name} -i /etc/ceph/{key_file_name}"""
            if scope == "service":
                cmd += f" --service_name {service_value}"
            if scope == "host":
                cmd += f" --hostname {hostname}"
            cmd += "'"
            out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def get_key(
        self,
        key_name,
        scope,
        service_value=None,
        hostname=None,
        no_exception_when_missing=False,
    ):
        """
        Retrieve the content of a specific certificate
        Args:
            certificate_name (str): name of key to be retrived
            service_value (str): service name if it is a service level certificate
            hostname (str): hostname if it is a host level certificate
        """
        cmd = f"{self.base_cmd} key get {key_name}"
        if scope == "service":
            cmd += f" --service_name {service_value}"
        if scope == "host":
            cmd += f" --hostname {hostname}"
        if no_exception_when_missing:
            cmd += " --no-exception-when-missing"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def rm_key(self, key_name, scope, service_value=None, hostname=None):
        """
        Remove an existing private key
        Args:
            key_name (str): name of key to be removed
            service_value (str): service name if it is a service level certificate
            hostname (str): hostname if it is a host level certificate
        """
        cmd = f"{self.base_cmd} key rm {key_name}"
        if scope == "service":
            cmd += f" --service_name {service_value}"
        if scope == "host":
            cmd += f" --hostname {hostname}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
