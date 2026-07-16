"""MIT Kerberos KDC setup for NFS-Ganesha tests (host KDC on clients[1])."""

import time

from cli.utilities.packages import Package
from utility.log import Log

log = Log(__name__)

DEFAULT_REALM = "CEPH.TEST"
DEFAULT_DOMAIN = "ceph.test"
DEFAULT_KDC_HOSTNAME = "kdc.ceph.test"
DEFAULT_MASTER_PASSWORD = "cephci-kdc-master"
DEFAULT_TEST_USER = "nfsuser"
DEFAULT_TEST_PASSWORD = "password123"


def _remote_test(node, path, flag="f"):
    """Return True if ``test -<flag> path`` succeeds on ``node``."""
    out, _ = node.exec_command(
        sudo=True,
        cmd="test -{} {} && echo yes".format(flag, path),
        check_ec=False,
    )
    return "yes" in (out or "")


class MITKDCSetup:
    """Bootstrap a host MIT Kerberos realm for cephci NFS tests."""

    def __init__(
        self,
        node,
        realm=DEFAULT_REALM,
        domain=DEFAULT_DOMAIN,
        kdc_hostname=DEFAULT_KDC_HOSTNAME,
        master_password=DEFAULT_MASTER_PASSWORD,
        test_user=DEFAULT_TEST_USER,
        test_password=DEFAULT_TEST_PASSWORD,
    ):
        self.node = node
        self.realm = realm.upper()
        self.domain = domain.lower()
        self.kdc_hostname = kdc_hostname
        self.master_password = master_password
        self.test_user = test_user
        self.test_password = test_password
        self._bootstrapped = False

    @property
    def test_principal(self):
        return "{}@{}".format(self.test_user, self.realm)

    def setup_kdc(self):
        """Install and start MIT KDC on the host."""
        self._setup_kdc_host()
        self._open_kdc_firewall()
        self._log_kdc_listeners()
        self._bootstrapped = True
        log.info(
            "MIT KDC ready on %s (%s), realm %s",
            self.node.hostname,
            self.node.ip_address,
            self.realm,
        )

    def add_user_principal(self, username=None, password=None):
        user = username or self.test_user
        pw = password or self.test_password
        principal = "{}@{}".format(user, self.realm)
        self._kadmin_local(
            'addprinc -pw "{}" {}'.format(pw, principal),
            check_ec=False,
        )
        log.info("Ensured user principal %s", principal)
        return principal

    def add_nfs_service_principal(self, nfs_fqdn):
        spn = "nfs/{}@{}".format(nfs_fqdn, self.realm)
        self._kadmin_local("addprinc -randkey {}".format(spn), check_ec=False)
        log.info("Ensured NFS service principal %s", spn)
        return spn

    def add_host_service_principal(self, host_fqdn):
        """Host principal so RHEL starts ``rpc-gssd`` (requires ``/etc/krb5.keytab``)."""
        spn = "host/{}@{}".format(host_fqdn, self.realm)
        self._kadmin_local("addprinc -randkey {}".format(spn), check_ec=False)
        log.info("Ensured host principal %s", spn)
        return spn

    def export_keytab_for_principal(
        self, principal, remote_path="/tmp/cephci-nfs.keytab"
    ):
        self._kadmin_local(
            "ktadd -k {} {}".format(remote_path, principal),
            check_ec=False,
        )
        if not _remote_test(self.node, remote_path, flag="s"):
            raise RuntimeError(
                "Keytab {} was not created for principal {}".format(
                    remote_path, principal
                )
            )
        return remote_path

    def fetch_keytab_bytes(self, remote_path="/tmp/cephci-nfs.keytab"):
        """Read a binary keytab from the KDC host via SFTP (not shell cat)."""
        handle = self.node.remote_file(sudo=True, file_name=remote_path, file_mode="rb")
        try:
            data = handle.read()
        finally:
            handle.close()
        if not data:
            raise RuntimeError("Failed to read keytab from {}".format(remote_path))
        return data

    def cleanup_kdc(self):
        log.info("Cleaning up MIT KDC on %s", self.node.hostname)
        self.node.exec_command(
            sudo=True, cmd="systemctl stop kadmin krb5kdc", check_ec=False
        )
        self._close_kdc_firewall()

    def _remove_stale_podman_kdc(self):
        """Remove leftover podman KDC containers from prior test runs."""
        self.node.exec_command(
            sudo=True,
            cmd="podman rm -f mit-kdc 2>/dev/null || true",
            check_ec=False,
        )
        time.sleep(2)

    def _host_kdc_active(self):
        kdc, _ = self.node.exec_command(
            sudo=True,
            cmd="systemctl is-active krb5kdc",
            check_ec=False,
        )
        return "active" in (kdc or "")

    def _start_host_kdc_services(self):
        """Enable and start host KDC, clearing stale container/port conflicts."""
        self._remove_stale_podman_kdc()
        self.node.exec_command(
            sudo=True,
            cmd="systemctl stop kadmin krb5kdc 2>/dev/null || true",
            check_ec=False,
        )
        self.node.exec_command(
            sudo=True,
            cmd="systemctl reset-failed krb5kdc kadmin 2>/dev/null || true",
            check_ec=False,
        )
        self.node.exec_command(
            sudo=True,
            cmd="systemctl enable krb5kdc kadmin",
            check_ec=False,
        )
        self.node.exec_command(
            sudo=True,
            cmd="systemctl start krb5kdc kadmin",
            check_ec=False,
        )
        for attempt in range(30):
            if self._host_kdc_active():
                log.info(
                    "Host krb5kdc active on %s after %s s",
                    self.node.hostname,
                    attempt + 1,
                )
                return
            time.sleep(1)
        diag, _ = self.node.exec_command(
            sudo=True,
            cmd=(
                "systemctl status krb5kdc --no-pager -l 2>&1 | head -30; "
                "echo '---'; journalctl -xeu krb5kdc.service --no-pager -n 15 2>&1; "
                "echo '---'; ss -lnptu '( sport = :88 or sport = :749 )' 2>&1 || "
                "ss -lnptu | grep -E ':88 |:749 ' || true"
            ),
            check_ec=False,
        )
        raise RuntimeError(
            "krb5kdc did not become active on {}:\n{}".format(self.node.hostname, diag)
        )

    def _setup_kdc_host(self):
        log.info("Setting up host MIT KDC on %s", self.node.hostname)
        Package(self.node).install("krb5-server krb5-workstation")

        krb5_conf = _krb5_conf_content(
            self.realm,
            self.domain,
            self.kdc_hostname,
            kdc_ip=self.node.ip_address,
        )
        kdc_conf = _kdc_conf_content(self.realm)
        kadm5_acl = "*/admin@{}	*".format(self.realm)

        self._write_remote_file("/etc/krb5.conf", krb5_conf)
        self._write_remote_file("/var/kerberos/krb5kdc/kdc.conf", kdc_conf)
        self._write_remote_file("/var/kerberos/krb5kdc/kadm5.acl", kadm5_acl)

        marker = "/var/kerberos/krb5kdc/.cephci_bootstrapped"
        if not _remote_test(self.node, marker):
            self.node.exec_command(
                sudo=True,
                cmd=("bash -c 'echo -e \"{pw}\\n{pw}\" | kdb5_util create -s'").format(
                    pw=self.master_password
                ),
            )
            self.node.exec_command(sudo=True, cmd="touch {}".format(marker))

        self._remove_stale_podman_kdc()
        self._start_host_kdc_services()

    def _kadmin_local(self, subcmd, check_ec=True):
        cmd = 'kadmin.local -q "{}"'.format(subcmd.replace('"', '\\"'))
        return self.node.exec_command(sudo=True, cmd=cmd, check_ec=check_ec)

    def _write_remote_file(self, path, content, mode=None):
        handle = self.node.remote_file(sudo=True, file_name=path, file_mode="w")
        handle.write(content)
        handle.flush()
        if mode:
            self.node.exec_command(sudo=True, cmd="chmod {} {}".format(mode, path))

    def _open_kdc_firewall(self):
        """Open Kerberos ports on the KDC host (runtime + permanent)."""
        fw_state, _ = self.node.exec_command(
            sudo=True,
            cmd="systemctl is-active firewalld",
            check_ec=False,
        )
        if "active" not in (fw_state or ""):
            log.info(
                "firewalld inactive on %s; ensuring iptables accepts Kerberos",
                self.node.hostname,
            )
            for proto in ("tcp", "udp"):
                self.node.exec_command(
                    sudo=True,
                    cmd=(
                        "iptables -C INPUT -p {p} --dport 88 -j ACCEPT "
                        "2>/dev/null || iptables -I INPUT -p {p} --dport 88 "
                        "-j ACCEPT"
                    ).format(p=proto),
                    check_ec=False,
                )
            return
        for port in ("88/tcp", "88/udp", "749/tcp"):
            self.node.exec_command(
                sudo=True,
                cmd="firewall-cmd --add-port={} --permanent".format(port),
                check_ec=False,
            )
            self.node.exec_command(
                sudo=True,
                cmd="firewall-cmd --add-port={}".format(port),
                check_ec=False,
            )
        self.node.exec_command(
            sudo=True,
            cmd="firewall-cmd --add-service=kerberos --permanent",
            check_ec=False,
        )
        self.node.exec_command(
            sudo=True,
            cmd="firewall-cmd --add-service=kerberos",
            check_ec=False,
        )
        self.node.exec_command(sudo=True, cmd="firewall-cmd --reload", check_ec=False)

    def _log_kdc_listeners(self):
        out, _ = self.node.exec_command(
            sudo=True,
            cmd=(
                "ss -H -lnptu | grep -E ':88 |:749 ' || "
                "ss -lnptu | grep -E ':88 |:749 ' || true"
            ),
            check_ec=False,
        )
        log.info(
            "KDC listeners on %s: %s",
            self.node.hostname,
            (out or "none").strip().replace("\n", "; "),
        )

    def _close_kdc_firewall(self):
        for port in ("88/tcp", "88/udp", "749/tcp"):
            self.node.exec_command(
                sudo=True,
                cmd="firewall-cmd --remove-port={} --permanent".format(port),
                check_ec=False,
            )
        self.node.exec_command(sudo=True, cmd="firewall-cmd --reload", check_ec=False)


def _krb5_conf_content(realm, domain, kdc_hostname, kdc_ip=None):
    """Build krb5.conf; prefer ``kdc_ip`` so clients do not rely on stale /etc/hosts."""
    kdc_target = kdc_ip if kdc_ip else kdc_hostname
    return """[libdefaults]
    default_realm = {realm}
    dns_lookup_kdc = false
    dns_lookup_realm = false
    rdns = false
    ticket_lifetime = 24h
    forwardable = true

[realms]
    {realm} = {{
        kdc = {kdc_target}:88
        admin_server = {kdc_target}:749
        default_domain = {domain}
    }}

[domain_realm]
    .{domain} = {realm}
    {domain} = {realm}
""".format(
        realm=realm,
        domain=domain,
        kdc_target=kdc_target,
    )


def _kdc_conf_content(realm):
    return """[kdcdefaults]
    kdc_ports = 88
    kdc_tcp_ports = 88

[realms]
    {realm} = {{
        acl_file = /var/kerberos/krb5kdc/kadm5.acl
        dict_file = /usr/share/dict/words
        admin_keytab = /var/kerberos/krb5kdc/kadm5.keytab
        max_life = 24h 0m 0s
        max_renewable_life = 7d 0h 0m 0s
        master_key_type = aes256-cts-hmac-sha1-96
        supported_enctypes = aes256-cts-hmac-sha1-96:normal aes128-cts-hmac-sha1-96:normal
        default_principal_flags = +preauth
    }}

[domain_realm]
    .ceph.test = {realm}
    ceph.test = {realm}
""".format(
        realm=realm
    )
