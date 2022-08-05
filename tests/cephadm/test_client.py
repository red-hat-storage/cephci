"""Manage the client via cephadm CLI."""
import time
from typing import Dict

from ceph.ceph_admin.orch import Orch
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
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

    if config.get("nodes"):
        nodes = config["nodes"]
        if not isinstance(nodes, list):
            nodes = [nodes]

        def setup(host):
            node = get_node_by_id(cls.cluster, host)

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

            out, _ = node.exec_command(cmd="ls -ltrh /etc/ceph/", sudo=True)
            log.info(out)

            # Hold local copy of the client key-ring in the installer node
            if config.get("store-keyring"):
                put_file(cls.installer, client_file, cnt_key, "w")

        with parallel() as p:
            for node in nodes:
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
