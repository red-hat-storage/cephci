"""Manage the client via cephadm CLI."""
import time
from typing import Dict

from ceph.ceph_admin.orch import Orch
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from utility import utils
from utility.log import Log

log = Log(__name__)


def add(cls, config: Dict) -> None:
    """configure client using the provided configuration.

    Args:
        cls: cephadm object
        config: Key/value pairs provided by the test case to create the client.

    Example::

        config:
            command: add
            id: client.1                    # client Id
            node: "node8"                   # client node
            copy_ceph_conf: true|false      # copy ceph conf to provided node
            store-keyring: true             # store keyrin locally under /etc/ceph
            install_packages:
              - ceph_common                 # install ceph common packages
            copy_admin_keyring: true|false  # copy admin keyring
            caps:                           # authorize client capabilities
              - "mon 'allow r'"
              - "osd 'allow rw pool=liverpool'"
    """
    id_ = config["id"]
    client_file = f"/etc/ceph/ceph.{id_}.keyring"

    # Create client
    cmd = ["ceph", "auth", "get-or-create", f"{id_}"]
    [cmd.append(f"{k} '{v}'") for k, v in config.get("caps", {}).items()]
    cnt_key, err = cls.shell(args=cmd)

    def put_file(client, file_name, content, file_mode, sudo=True):
        file_ = client.remote_file(sudo=sudo, file_name=file_name, file_mode=file_mode)
        file_.write(content)
        file_.flush()

    nodes_ = config.get("nodes", config.get("node"))
    default_version = cls.cluster.rhcs_version.version[0]
    use_cdn = cls.cluster.use_cdn
    if nodes_:
        if not isinstance(nodes_, list):
            nodes_ = [{nodes_: {}}]

        def setup(host):
            rhcs_version = str(
                [
                    host[entry].get(
                        "release", default_version if not use_cdn else "default"
                    )
                    for entry in host
                ][0]
            )
            name = [entry for entry in host][0]
            node = get_node_by_id(cls.cluster, name)

            rhel_version = node.distro_info["VERSION_ID"][0]
            log.debug(
                f"RHCS version is : {rhcs_version} for host : {node.hostname}\n"
                f"with RHEL major version as : {rhel_version}"
            )
            enable_cmd = "subscription-manager repos --enable="
            disable_all = [
                r"subscription-manager repos --disable=*",
                r"yum-config-manager --disable \*",
            ]
            cmd = 'subscription-manager repos --list-enabled | grep -i "Repo ID"'
            cdn_ceph_repo = {
                "7": {"4": ["rhel-7-server-rhceph-4-tools-rpms"]},
                "8": {
                    "4": ["rhceph-4-tools-for-rhel-8-x86_64-rpms"],
                    "5": ["rhceph-5-tools-for-rhel-8-x86_64-rpms"],
                },
                "9": {"5": ["rhceph-5-tools-for-rhel-9-x86_64-rpms"]},
            }

            rhel_repos = {
                "7": ["rhel-7-server-rpms", "rhel-7-server-extras-rpms"],
                "8": [
                    "rhel-8-for-x86_64-baseos-rpms",
                    "rhel-8-for-x86_64-appstream-rpms",
                ],
                "9": [
                    "rhel-9-for-x86_64-appstream-rpms",
                    "rhel-9-for-x86_64-baseos-rpms",
                ],
            }

            # Collecting already enabled repos
            out, _ = node.exec_command(sudo=True, cmd=cmd, check_ec=False)
            enabled_repos = list()
            if out:
                out = out.strip().split("\n")
                for entry in out:
                    repo = entry.split(":")[-1].strip()
                    enabled_repos.append(repo)
            log.debug(f"Enabled repos on the system are : {enabled_repos}")

            if rhcs_version != "default":
                # Disabling all the repos and enabling the ones we need to install the ceph client
                for cmd in disable_all:
                    node.exec_command(sudo=True, cmd=cmd, timeout=1200)

                # Enabling the required CDN repos
                for repos in rhel_repos[rhel_version]:
                    node.exec_command(sudo=True, cmd=f"{enable_cmd}{repos}")

                # Todo: Sticking to RHCS 5 CDN repo rpms, Since 6x not yet GAed
                #       Revert this commit/code changes once 6x is GAed.
                if rhcs_version > "5":
                    rhcs_version = "5"
                for repos in cdn_ceph_repo[rhel_version][rhcs_version]:
                    node.exec_command(sudo=True, cmd=f"{enable_cmd}{repos}")

            # Copy the keyring to client
            node.exec_command(sudo=True, cmd="mkdir -p /etc/ceph")
            put_file(node, client_file, cnt_key, "w")

            if config.get("copy_ceph_conf", True):
                # Get minimal ceph.conf
                ceph_conf, err = cls.shell(
                    args=["ceph", "config", "generate-minimal-conf"]
                )
                # Copy the ceph.conf to client
                put_file(node, "/etc/ceph/ceph.conf", ceph_conf, "w")

            # Copy admin keyring to client node
            if config.get("copy_admin_keyring"):
                admin_keyring, _ = cls.shell(
                    args=["ceph", "auth", "get", "client.admin"]
                )
                put_file(
                    node, "/etc/ceph/ceph.client.admin.keyring", admin_keyring, "w"
                )

            # Install ceph-common
            if config.get("install_packages"):
                for pkg in config.get("install_packages"):
                    node.exec_command(
                        cmd=f"yum install -y --nogpgcheck {pkg}", sudo=True
                    )
            if config.get("git_clone", False):
                log.info("perform cloning operation")
                role = config.get("git_node_role", "client")
                ceph_object = cls.cluster.get_ceph_object(role)
                node_value = ceph_object.node
                utils.perform_env_setup(config, node_value, cls.cluster)

            out, _ = node.exec_command(cmd="ls -ltrh /etc/ceph/", sudo=True)
            log.info(out)

            # Hold local copy of the client key-ring in the installer node
            if config.get("store-keyring"):
                put_file(cls.installer, client_file, cnt_key, "w")

        with parallel() as p:
            for node in nodes_:
                if not isinstance(node, dict):
                    node = {node: {}}
                p.spawn(
                    setup,
                    node,
                )
                time.sleep(20)


def remove(cls, config: Dict) -> None:
    """
    configure client using the provided configuration.

    Args:
        cls: cephadm object
        config: Key/value pairs provided by the test case to create the client.

    Example::

        config:
            command: remove
            id: client.0                # client Id
            node: "node8"               # client node
            remove_packages:
                - ceph-common           # Remove ceph common packages
            remove_admin_keyring: true  # Copy admin keyring to node
    """
    node = get_node_by_id(cls.cluster, config["node"])
    id_ = config["id"]

    cls.shell(
        args=["ceph", "auth", "del", id_],
    )

    if config.get("remove_admin_keyring"):
        node.exec_command(
            cmd="rm -rf /etc/ceph/ceph.client.admin.keyring",
            sudo=True,
        )

    node.exec_command(
        sudo=True, cmd=f"rm -rf /etc/ceph/ceph.{id_}.keyring", check_ec=False
    )

    out, _ = node.exec_command(cmd="ls -ltrh /etc/ceph/", sudo=True)
    log.info(out)

    # Remove packages like ceph-common
    # Be-careful it may remove entire /etc/ceph directory
    if config.get("remove_packages"):
        for pkg in config.get("remove_packages"):
            node.exec_command(
                cmd=f"yum remove -y {pkg}",
                sudo=True,
            )


MAP_ = {"add": add, "remove": remove}


def run(ceph_cluster, **kw):
    """
    test module to manage client operations
    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
        kw: test data

    kw:
      config:
        command: add
        id: client.1                      # client Id (<type>.<Id>)
        node: client1                     # client node
        install_packages:
          - ceph_common                   # install ceph common packages
        copy_admin_keyring: true          # Copy admin keyring to node
        caps:                             # authorize client capabilities
          mon: "allow *"
          osd: "allow *"
          mds: "allow *"
          mgr: "allow *"
    """
    config = kw["config"]

    build = config.get("build", config.get("rhbuild"))
    ceph_cluster.rhcs_version = build

    # Manage Ceph using ceph-admin orchestration
    command = config.pop("command")
    log.info("Executing client %s" % command)
    orch = Orch(cluster=ceph_cluster, **config)
    method = MAP_[command]
    method(orch, config)
    return 0
