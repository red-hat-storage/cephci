import time

from utility.log import Log

log = Log(__name__)


class LDAPSetup:
    def __init__(
        self,
        node,
        ldap_container_name="ldap-server",
        ldap_port=389,
        ldap_admin_pass="password",
        ldap_org="Ceph",
        ldap_domain="ceph.com",
        ldap_base_dn="dc=ceph,dc=com",
        test_user="tester",
        test_uid=10005,
        test_gid=10005,
        test_user_2="tester2",
        test_uid_2=10006,
        test_gid_2=10006,
        ldap_image="docker.io/osixia/openldap:latest",
    ):
        self.node = node
        self.ldap_container_name = ldap_container_name
        self.ldap_port = ldap_port
        self.ldap_admin_pass = ldap_admin_pass
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

    def setup_ldap_container(self):
        """Deploy OpenLDAP container and populate it."""
        log.info("Setting up LDAP container on {}".format(self.node.hostname))

        # Clean up any existing container
        self.node.exec_command(
            sudo=True,
            cmd="podman rm -f {}".format(self.ldap_container_name),
            check_ec=False,
        )

        # Run LDAP container
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

        # Open firewall port for LDAP
        log.info("Opening port {} on {}".format(self.ldap_port, self.node.hostname))
        self.node.exec_command(
            sudo=True,
            cmd="firewall-cmd --add-port={}/tcp --permanent".format(self.ldap_port),
        )
        self.node.exec_command(sudo=True, cmd="firewall-cmd --reload")

        # Wait for LDAP to be ready
        log.info("Waiting for LDAP server to initialize...")
        time.sleep(15)

        # Create LDIF content
        ldif_content = """
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
userPassword: password123

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
userPassword: password123
""".format(
            base_dn=self.ldap_base_dn,
            gid=self.test_gid,
            user=self.test_user,
            uid=self.test_uid,
            user2=self.test_user_2,
            uid2=self.test_uid_2,
            gid2=self.test_gid_2,
        )
        ldif_path = "/tmp/users.ldif"
        self.node.remote_file(sudo=True, file_name=ldif_path, file_mode="w").write(
            ldif_content
        )

        # Add users to LDAP
        log.info("Adding users to LDAP...")
        cmd = (
            "cat {path} | podman exec -i {container} ldapadd -x "
            "-D 'cn=admin,{base_dn}' -w {password} -f /dev/stdin"
        ).format(
            path=ldif_path,
            container=self.ldap_container_name,
            base_dn=self.ldap_base_dn,
            password=self.ldap_admin_pass,
        )
        self.node.exec_command(sudo=True, cmd=cmd)
        log.info("LDAP user and group created.")

    def cleanup_ldap(self):
        """Cleanup LDAP container."""
        log.info("Cleaning up LDAP container...")
        self.node.exec_command(
            sudo=True,
            cmd="podman rm -f {}".format(self.ldap_container_name),
            check_ec=False,
        )
