import os
import secrets
import time

from utility.log import Log

log = Log(__name__)

DEFAULT_LDAP_CONTAINER = "ldap-server"
DEFAULT_LDAP_PORT = 389
DEFAULT_LDAP_ORG = "Ceph"
DEFAULT_LDAP_DOMAIN = "ceph.com"
DEFAULT_LDAP_BASE_DN = "dc=ceph,dc=com"
DEFAULT_LDAP_TEST_USER = "tester"
DEFAULT_LDAP_TEST_USER_2 = "tester2"
DEFAULT_NFS_LDAP_CLUSTER = "cephfs-nfs-ldap"
DEFAULT_LDAP_MOUNT = "/mnt/nfs_ldap"
LDAP_ADMIN_PASS_STASH = "/tmp/cephci-ldap-admin.pass"


def get_default_ldap_admin_password():
    return os.environ.get("CEPHCI_LDAP_ADMIN_PASSWORD") or secrets.token_urlsafe(16)


def stash_admin_password(node, password):
    node.remote_file(sudo=True, file_name=LDAP_ADMIN_PASS_STASH, file_mode="w").write(
        password
    )
    node.exec_command(
        sudo=True, cmd="chmod 600 {}".format(LDAP_ADMIN_PASS_STASH), check_ec=False
    )


def load_stashed_admin_password(node):
    out, _ = node.exec_command(
        sudo=True,
        cmd="test -s {} && cat {}".format(LDAP_ADMIN_PASS_STASH, LDAP_ADMIN_PASS_STASH),
        check_ec=False,
    )
    if out and str(out).strip():
        return str(out).strip()
    return None


class LDAPSetup:
    def __init__(
        self,
        node,
        ldap_container_name=DEFAULT_LDAP_CONTAINER,
        ldap_port=DEFAULT_LDAP_PORT,
        ldap_admin_pass=None,
        ldap_org=DEFAULT_LDAP_ORG,
        ldap_domain=DEFAULT_LDAP_DOMAIN,
        ldap_base_dn=DEFAULT_LDAP_BASE_DN,
        test_user=DEFAULT_LDAP_TEST_USER,
        test_uid=10005,
        test_gid=10005,
        test_user_2=DEFAULT_LDAP_TEST_USER_2,
        test_uid_2=10006,
        test_gid_2=10006,
        ldap_image="docker.io/osixia/openldap:latest",
    ):
        self.node = node
        self.ldap_container_name = ldap_container_name
        self.ldap_port = ldap_port
        self.ldap_admin_pass = ldap_admin_pass or get_default_ldap_admin_password()
        self.ldap_org = ldap_org
        self.ldap_domain = ldap_domain
        self.ldap_base_dn = ldap_base_dn
        self.test_user = test_user
        self.test_uid = test_uid
        self.test_gid = test_gid
        self.test_user_2 = test_user_2
        self.test_uid_2 = test_uid_2
        self.test_gid_2 = test_gid_2
        self.ldap_image = ldap_image

    def is_container_running(self):
        out, _ = self.node.exec_command(
            sudo=True,
            cmd="podman ps --filter name=^{} --format '{{{{.Names}}}}'".format(
                self.ldap_container_name
            ),
            check_ec=False,
        )
        return self.ldap_container_name in (out or "")

    def setup_ldap_container(self):
        """Deploy OpenLDAP container and populate test users."""
        log.info("Setting up LDAP container on {}".format(self.node.hostname))

        if self.is_container_running():
            log.info(
                "LDAP container %s already running on %s; refreshing users",
                self.ldap_container_name,
                self.node.hostname,
            )
            self.restore_test_users()
            return

        self.node.exec_command(
            sudo=True,
            cmd="podman rm -f {}".format(self.ldap_container_name),
            check_ec=False,
        )

        cmd = (
            "podman run -d -p {port}:389 --name {name} "
            "--env LDAP_ORGANISATION='{org}' "
            "--env LDAP_DOMAIN='{domain}' "
            "--env LDAP_ADMIN_PASSWORD='{password}' "
            "{image}"
        ).format(
            port=self.ldap_port,
            name=self.ldap_container_name,
            org=self.ldap_org,
            domain=self.ldap_domain,
            password=self.ldap_admin_pass,
            image=self.ldap_image,
        )
        self.node.exec_command(sudo=True, cmd=cmd)

        log.info("Opening port {} on {}".format(self.ldap_port, self.node.hostname))
        self.node.exec_command(
            sudo=True,
            cmd="firewall-cmd --add-port={}/tcp --permanent".format(self.ldap_port),
            check_ec=False,
        )
        self.node.exec_command(sudo=True, cmd="firewall-cmd --reload", check_ec=False)

        log.info("Waiting for LDAP server to initialize...")
        time.sleep(15)
        self.restore_test_users()

    def _users_ldif_content(self):
        return """
dn: cn=ceph-users,{base_dn}
objectClass: posixGroup
cn: ceph-users
gidNumber: {gid}

dn: uid={user},{base_dn}
objectClass: inetOrgPerson
objectClass: posixAccount
objectClass: top
cn: {user}
sn: user
uid: {user}
uidNumber: {uid}
gidNumber: {gid}
homeDirectory: /home/{user}
loginShell: /bin/bash
userPassword: {user_pw}

dn: uid={user2},{base_dn}
objectClass: inetOrgPerson
objectClass: posixAccount
objectClass: top
cn: {user2}
sn: user2
uid: {user2}
uidNumber: {uid2}
gidNumber: {gid2}
homeDirectory: /home/{user2}
loginShell: /bin/bash
userPassword: {user2_pw}
""".format(
            base_dn=self.ldap_base_dn,
            gid=self.test_gid,
            user=self.test_user,
            uid=self.test_uid,
            user2=self.test_user_2,
            uid2=self.test_uid_2,
            gid2=self.test_gid_2,
            user_pw=secrets.token_urlsafe(12),
            user2_pw=secrets.token_urlsafe(12),
        )

    def restore_test_users(self):
        """Create or refresh LDAP test users and group (idempotent)."""
        ldif_path = "/tmp/cephci-users.ldif"
        self.node.remote_file(sudo=True, file_name=ldif_path, file_mode="w").write(
            self._users_ldif_content()
        )
        log.info("Adding LDAP test users on %s", self.node.hostname)
        cmd = (
            "cat {path} | podman exec -i {container} ldapadd -x "
            "-D 'cn=admin,{base_dn}' -w {password} -f /dev/stdin"
        ).format(
            path=ldif_path,
            container=self.ldap_container_name,
            base_dn=self.ldap_base_dn,
            password=self.ldap_admin_pass,
        )
        self.node.exec_command(sudo=True, cmd=cmd, check_ec=False)
        log.info("LDAP test users ensured on %s", self.node.hostname)

    def delete_test_user(self, username=None):
        user = username or self.test_user
        cmd = (
            "podman exec {container} ldapdelete -x "
            "-D 'cn=admin,{base_dn}' -w {password} 'uid={user},{base_dn}'"
        ).format(
            container=self.ldap_container_name,
            base_dn=self.ldap_base_dn,
            password=self.ldap_admin_pass,
            user=user,
        )
        self.node.exec_command(sudo=True, cmd=cmd, check_ec=False)

    def cleanup_ldap(self):
        """Remove LDAP container."""
        log.info("Cleaning up LDAP container on %s...", self.node.hostname)
        self.node.exec_command(
            sudo=True,
            cmd="podman rm -f {}".format(self.ldap_container_name),
            check_ec=False,
        )
