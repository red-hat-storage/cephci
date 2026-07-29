"""MIT Kerberos KDC setup for NFS-Ganesha tests (host KDC on clients[1])."""

import os
import re
import secrets
import shlex
import time

from cli.utilities.packages import Package
from utility.log import Log

log = Log(__name__)

DEFAULT_REALM = "CEPH.TEST"
DEFAULT_DOMAIN = "ceph.test"
DEFAULT_KDC_HOSTNAME = "kdc.ceph.test"
DEFAULT_TEST_USER = "nfsuser"

# User/client auth uses keytabs (like SSH keys), not passwords.
# KDC database master key still needs a secret at first create only.


def get_default_master_password():
    return os.environ.get("CEPHCI_KDC_MASTER_PASSWORD") or secrets.token_urlsafe(32)


def validate_kerberos_realm(realm):
    """Validate Kerberos realm format (uppercase alphanumeric / dots / dashes)."""
    if not realm or not isinstance(realm, str):
        raise ValueError("Realm must be a non-empty string")
    realm = realm.strip().upper()
    if not re.match(r"^[A-Z][A-Z0-9.-]*$", realm):
        raise ValueError("Invalid realm format: {!r}".format(realm))
    return realm


def validate_kerberos_domain(domain):
    """Validate DNS-style Kerberos domain (lowercase)."""
    if not domain or not isinstance(domain, str):
        raise ValueError("Domain must be a non-empty string")
    domain = domain.strip().lower()
    if not re.match(r"^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$", domain):
        raise ValueError("Invalid domain format: {!r}".format(domain))
    return domain


def validate_kerberos_hostname(hostname):
    """Validate a hostname / FQDN used for SPNs and krb5.conf."""
    if not hostname or not isinstance(hostname, str):
        raise ValueError("Hostname must be a non-empty string")
    hostname = hostname.strip().lower()
    if not re.match(r"^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$", hostname):
        raise ValueError("Invalid hostname format: {!r}".format(hostname))
    return hostname


def _kadmin_safe_token(value, name="value"):
    """Restrict tokens embedded in ``kadmin.local -q`` queries."""
    if not value or not isinstance(value, str):
        raise ValueError("{} must be a non-empty string".format(name))
    if re.search(r"[\'\"\\$;`|&<>()\n\r\t ]", value):
        raise ValueError("Invalid characters in {}: {!r}".format(name, value))
    return value


def _remote_test(node, path, flag="f"):
    """Return True if ``test -<flag> path`` succeeds on ``node``."""
    out, _ = node.exec_command(
        sudo=True,
        cmd="test -{} {} && echo yes".format(flag, shlex.quote(path)),
        check_ec=False,
    )
    return "yes" in (out or "")


def _enforce_keytab_permissions(node, keytab_path):
    """Enforce keytab mode 0600 and root ownership."""
    path = shlex.quote(keytab_path)
    node.exec_command(
        sudo=True,
        cmd="chmod 600 {} && chown root:root {}".format(path, path),
    )


class MITKDCSetup:
    """Bootstrap a host MIT Kerberos realm for cephci NFS tests."""

    def __init__(
        self,
        node,
        realm=DEFAULT_REALM,
        domain=DEFAULT_DOMAIN,
        kdc_hostname=DEFAULT_KDC_HOSTNAME,
        master_password=None,
        test_user=DEFAULT_TEST_USER,
    ):
        self.node = node
        self.realm = validate_kerberos_realm(realm)
        self.domain = validate_kerberos_domain(domain)
        self.kdc_hostname = validate_kerberos_hostname(kdc_hostname)
        self.master_password = _kadmin_safe_token(
            master_password or get_default_master_password(), "master_password"
        )
        self.test_user = _kadmin_safe_token(test_user, "test_user")
        self._bootstrapped = False

    @property
    def test_principal(self):
        return "{}@{}".format(self.test_user, self.realm)

    def setup_kdc(self):
        """Install and start MIT KDC on the host."""
        if self._bootstrapped:
            log.info("KDC already bootstrapped on %s", self.node.hostname)
            return
        try:
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
        except Exception as exc:
            log.error("KDC setup failed, attempting cleanup: %s", exc)
            try:
                self.cleanup_kdc()
            except Exception as cleanup_exc:
                log.warning("Cleanup also failed: %s", cleanup_exc)
            raise

    def add_user_principal(self, username=None):
        """
        Ensure a user principal with a random key (keytab auth, no password).

        Analogous to SSH key auth: clients authenticate with a keytab, not a
        password typed into ``kinit``.
        """
        user = _kadmin_safe_token(username or self.test_user, "username")
        principal = _kadmin_safe_token("{}@{}".format(user, self.realm), "principal")
        self._kadmin_local(
            "addprinc -randkey {}".format(principal),
            check_ec=False,
        )
        log.info("Ensured user principal %s (keytab auth)", principal)
        return principal

    def add_nfs_service_principal(self, nfs_fqdn):
        fqdn = validate_kerberos_hostname(nfs_fqdn)
        spn = _kadmin_safe_token("nfs/{}@{}".format(fqdn, self.realm), "spn")
        self._kadmin_local("addprinc -randkey {}".format(spn), check_ec=False)
        log.info("Ensured NFS service principal %s", spn)
        return spn

    def add_host_service_principal(self, host_fqdn):
        """Host principal so RHEL starts ``rpc-gssd`` (requires ``/etc/krb5.keytab``)."""
        fqdn = validate_kerberos_hostname(host_fqdn)
        spn = _kadmin_safe_token("host/{}@{}".format(fqdn, self.realm), "spn")
        self._kadmin_local("addprinc -randkey {}".format(spn), check_ec=False)
        log.info("Ensured host principal %s", spn)
        return spn

    def export_keytab_for_principal(
        self, principal, remote_path="/tmp/cephci-nfs.keytab"
    ):
        principal = _kadmin_safe_token(principal, "principal")
        if not (
            remote_path.startswith("/tmp/cephci-")
            and remote_path.endswith(".keytab")
            and ".." not in remote_path
        ):
            raise ValueError(
                "Refusing keytab path outside /tmp/cephci-*.keytab: {!r}".format(
                    remote_path
                )
            )
        self.node.exec_command(
            sudo=True, cmd="rm -f {}".format(shlex.quote(remote_path)), check_ec=False
        )
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
        _enforce_keytab_permissions(self.node, remote_path)
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
        self._bootstrapped = False

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
            # Avoid embedding the master password in a shell string.
            pw_file = "/tmp/cephci-kdc-master.pw"
            self._write_remote_file(
                pw_file,
                "{}\n{}\n".format(self.master_password, self.master_password),
                mode="600",
            )
            try:
                self.node.exec_command(
                    sudo=True,
                    cmd="kdb5_util create -s < {}".format(shlex.quote(pw_file)),
                )
                self.node.exec_command(sudo=True, cmd="touch {}".format(marker))
            finally:
                self.node.exec_command(
                    sudo=True,
                    cmd="rm -f {}".format(shlex.quote(pw_file)),
                    check_ec=False,
                )

        self._remove_stale_podman_kdc()
        self._start_host_kdc_services()

    def _kadmin_local(self, subcmd, check_ec=True):
        cmd = "kadmin.local -q {}".format(shlex.quote(subcmd))
        return self.node.exec_command(sudo=True, cmd=cmd, check_ec=check_ec)

    def _write_remote_file(self, path, content, mode=None):
        handle = self.node.remote_file(sudo=True, file_name=path, file_mode="w")
        handle.write(content)
        handle.flush()
        if mode:
            self.node.exec_command(
                sudo=True,
                cmd="chmod {} {}".format(mode, shlex.quote(path)),
            )

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
