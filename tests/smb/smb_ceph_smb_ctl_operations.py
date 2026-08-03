import json
import shlex

from smb_operations import (
    check_smb_cluster,
    get_smb_shares,
    smb_cifs_mount,
    smb_cleanup,
    verify_smb_service,
)

from ceph.waiter import WaitUntil
from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


COMMANDS = (
    "info",
    "status",
    "close-share",
    "kill-client-connection",
    "config-dump",
    "get-debug-level",
    "set-debug-level",
)


def _ceph_smb_ctl(node, args, cluster_id=None):
    """Run ceph-smb-ctl against a local Unix socket.

    Args:
        node: Node on which cephadm shell is invoked.
        args: ceph-smb-ctl command and arguments.
        cluster_id: SMB cluster ID used to select a local Unix socket.
    """
    ctl_args = []
    if cluster_id:
        ctl_args.extend(["--cluster", cluster_id])

    quoted_args = " ".join(shlex.quote(str(arg)) for arg in [*ctl_args, *args])
    cmd = f"cephadm shell -- ceph-smb-ctl {quoted_args}"
    log.info("Running ceph-smb-ctl on %s: %s", node.hostname, cmd)
    return node.exec_command(sudo=True, cmd=cmd)[0].strip()


def _validate_json(output, command):
    """Return parsed ceph-smb-ctl JSON output."""
    try:
        return json.loads(output)
    except (TypeError, json.JSONDecodeError) as error:
        raise OperationFailedError(
            f"ceph-smb-ctl {command} did not return valid JSON: {output}"
        ) from error


def _verify_configuration(installer, cluster_id):
    """Verify locally_enabled in the applied SMB cluster resource."""
    cmd = f"cephadm shell -- ceph smb show ceph.smb.cluster.{shlex.quote(cluster_id)}"
    output = installer.exec_command(sudo=True, cmd=cmd)[0].strip()
    resource = _validate_json(output, "ceph smb show")
    if isinstance(resource, list):
        resources = resource
        resource = next(
            (
                item
                for item in resources
                if item.get("resource_type") == "ceph.smb.cluster"
                and item.get("cluster_id") == cluster_id
            ),
            {},
        )
    elif "resources" in resource:
        resources = resource["resources"]
        resource = next(
            (
                item
                for item in resources
                if item.get("resource_type") == "ceph.smb.cluster"
                and item.get("cluster_id") == cluster_id
            ),
            {},
        )
    if resource.get("remote_control", {}).get("locally_enabled") is not True:
        raise OperationFailedError(
            f"remote_control.locally_enabled is not true for {cluster_id}: {output}"
        )


def _verify_sidecar(smb_node):
    """Verify the remotectl sidecar using podman."""
    output, _ = smb_node.exec_command(sudo=True, cmd="podman ps | grep remotectl")
    output = output.strip()
    if "remotectl" not in output:
        raise OperationFailedError(
            f"Remote-control sidecar is not running on {smb_node.hostname}"
        )


def _mount_client(smb_node, client, share, config):
    smb_cifs_mount(
        smb_node,
        client,
        share,
        config.get("smb_user_name", "user1"),
        config.get("smb_user_password", "passwd"),
        config.get("auth_mode", "user"),
        config.get("domain_realm"),
        config.get("cifs_mount_point", "/mnt/smb"),
    )


def _unmount_client(client, mount_point):
    client.exec_command(sudo=True, cmd=f"umount {shlex.quote(mount_point)}")


def _test_help(command_node, cluster_id):
    output = _ceph_smb_ctl(command_node, ["--help"], cluster_id)
    missing = [command for command in COMMANDS if command not in output]
    if missing:
        raise OperationFailedError(
            f"ceph-smb-ctl --help is missing commands: {', '.join(missing)}"
        )


def _test_debug_level(command_node, cluster_id, config):
    subsystem = config.get("subsystem", "smbd")
    requested_level = str(config.get("debug_level", 5))
    original = _ceph_smb_ctl(command_node, ["get-debug-level", subsystem], cluster_id)
    try:
        _ceph_smb_ctl(
            command_node,
            ["set-debug-level", subsystem, requested_level],
            cluster_id,
        )
        current = _ceph_smb_ctl(
            command_node, ["get-debug-level", subsystem], cluster_id
        )
        if requested_level not in current:
            raise OperationFailedError(
                f"Debug level for {subsystem} was not set to {requested_level}: "
                f"{current}"
            )
    finally:
        # The get command can return either a bare level or a JSON response.
        try:
            original_level = str(json.loads(original).get("level"))
        except (AttributeError, json.JSONDecodeError):
            original_level = original.strip()
        if original_level and original_level != "None":
            _ceph_smb_ctl(
                command_node,
                ["set-debug-level", subsystem, original_level],
                cluster_id,
            )


def _test_kill_client(command_node, smb_node, client, share, cluster_id, config):
    mount_point = config.get("cifs_mount_point", "/mnt/smb")
    _mount_client(smb_node, client, share, config)
    try:
        status = _ceph_smb_ctl(command_node, ["status"], cluster_id)
        if client.ip_address not in status:
            raise OperationFailedError(
                f"Client {client.ip_address} is not present in SMB status"
            )
        _ceph_smb_ctl(
            command_node,
            ["kill-client-connection", client.ip_address],
            cluster_id,
        )
        for attempt in WaitUntil(timeout=30, interval=5):
            status = _ceph_smb_ctl(command_node, ["status"], cluster_id)
            if client.ip_address not in status:
                break
        if attempt.expired:
            raise OperationFailedError(
                f"Client {client.ip_address} remains connected after kill request"
            )
    finally:
        _unmount_client(client, mount_point)


def _test_close_share(command_node, smb_node, client, share, cluster_id, config):
    """Invoke close-share while the selected share has an active client."""
    mount_point = config.get("cifs_mount_point", "/mnt/smb")
    _mount_client(smb_node, client, share, config)
    try:
        status = _ceph_smb_ctl(command_node, ["status"], cluster_id)
        if share not in status:
            raise OperationFailedError(
                f"Share {share} is not present in SMB status before close-share"
            )
        _ceph_smb_ctl(command_node, ["close-share", share], cluster_id)
    finally:
        _unmount_client(client, mount_point)


def run(ceph_cluster, **kw):
    """Validate ceph-smb-ctl over a local Unix socket."""
    config = kw.get("config") or {}
    operation = config.get("operation")
    cluster_id = config.get("smb_cluster_id")
    if not operation:
        raise ConfigError("Mandatory config 'operation' not provided")
    if not cluster_id:
        raise ConfigError("Mandatory config 'smb_cluster_id' not provided")

    installer = ceph_cluster.get_nodes(role="installer")[0]
    smb_nodes = ceph_cluster.get_nodes("smb")
    clients = ceph_cluster.get_nodes(role="client")
    if not smb_nodes:
        raise ConfigError("No node with the 'smb' role is available")

    smb_node = smb_nodes[0]
    command_node = smb_node
    command_cluster_id = cluster_id if config.get("select_cluster") else None

    check_smb_cluster(installer, cluster_id)
    verify_smb_service(installer, service_name="smb")
    shares = get_smb_shares(installer, cluster_id)

    try:
        if operation == "verify_configuration":
            _verify_configuration(installer, cluster_id)
        elif operation == "verify_sidecar":
            _verify_sidecar(smb_node)
        elif operation == "help":
            _test_help(command_node, command_cluster_id)
        elif operation in ("info", "status"):
            output = _ceph_smb_ctl(command_node, [operation], command_cluster_id)
            _validate_json(output, operation)
        elif operation == "config_dump":
            output = _ceph_smb_ctl(
                command_node,
                ["config-dump", config.get("config_format", "samba")],
                command_cluster_id,
            )
            if not output:
                raise OperationFailedError("ceph-smb-ctl config-dump returned no data")
        elif operation == "debug_level":
            _test_debug_level(command_node, command_cluster_id, config)
        elif operation == "kill_client_connection":
            if not clients:
                raise ConfigError("No node with the 'client' role is available")
            if not shares:
                raise ConfigError(f"SMB cluster {cluster_id} has no shares")
            _test_kill_client(
                command_node,
                smb_node,
                clients[0],
                shares[0],
                command_cluster_id,
                config,
            )
        elif operation == "close_share":
            if not clients:
                raise ConfigError("No node with the 'client' role is available")
            if not shares:
                raise ConfigError(f"SMB cluster {cluster_id} has no shares")
            _test_close_share(
                command_node,
                smb_node,
                clients[0],
                shares[0],
                command_cluster_id,
                config,
            )
        else:
            raise ConfigError(f"Unsupported ceph-smb-ctl operation: {operation}")
    except Exception as error:
        log.error("ceph-smb-ctl operation %s failed: %s", operation, error)
        return 1
    finally:
        if config.get("smb_cluster_cleanup"):
            smb_cleanup(installer, shares, cluster_id)
    return 0
