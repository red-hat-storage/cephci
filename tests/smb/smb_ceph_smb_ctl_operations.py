import json
import re
import shlex
import uuid
from functools import partial

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
    "close-share",
    "config-dump",
    "config-shares-list",
    "config-summary",
    "ctdb-move-ip",
    "ctdb-status",
    "get-debug-level",
    "info",
    "kill-client-connection",
    "set-debug-level",
    "status",
)

GLOBAL_OPTIONS = (
    "--address",
    "--cluster",
    "--header",
    "--no-tls",
    "--quiet",
    "--tls-ca-cert",
    "--tls-cert",
    "--tls-key",
    "--verbose",
    "-c",
    "-q",
    "-v",
)

SUBCOMMAND_OPTIONS = {
    "close-share": ("share_name", "--denied-users-only", "-d"),
    "config-dump": ("source", "--sha256", "ctdb", "samba", "sambacc"),
    "config-shares-list": ("source", "ctdb", "samba", "sambacc"),
    "config-summary": ("source", "--sha256", "ctdb", "samba", "sambacc"),
    "ctdb-move-ip": ("ip_address", "node"),
    "ctdb-status": (),
    "get-debug-level": ("process", "ctdb", "smb", "winbind"),
    "info": (),
    "kill-client-connection": ("ip_address",),
    "set-debug-level": ("process", "debug_level", "ctdb", "smb", "winbind"),
    "status": (),
}


def _ceph_smb_ctl(node, args, cluster_id=None, global_args=None, auth=None):
    """Run ceph-smb-ctl against a local Unix socket.

    Args:
        node: Node on which cephadm shell is invoked.
        args: ceph-smb-ctl command and arguments.
        cluster_id: SMB cluster ID used to select a local Unix socket.
        global_args: Additional options placed before the subcommand.
        auth: Ceph identity and temporary key-file information.
    """
    ctl_args = []
    if cluster_id:
        ctl_args.extend(["--cluster", cluster_id])

    quoted_args = " ".join(
        shlex.quote(str(arg)) for arg in [*(global_args or []), *ctl_args, *args]
    )
    auth_args = ""
    if auth:
        user_header = shlex.quote(f"ceph-auth-user={auth['user']}")
        key_file = shlex.quote(auth["key_file"])
        auth_args = (
            f'--header {user_header} --header "ceph-auth-key=$(cat {key_file})" '
        )
    cmd = f"cephadm shell -- ceph-smb-ctl {auth_args}{quoted_args}"
    display_cmd = re.sub(
        r"ceph-auth-key=\$\(cat [^)]+\)",
        "ceph-auth-key=<key-file>",
        cmd,
    )
    log.info("Running ceph-smb-ctl on %s: %s", node.hostname, display_cmd)
    return node.exec_command(sudo=True, cmd=cmd)[0].strip()


def _prepare_ceph_auth(installer, command_node, config):
    """Create local cephx metadata without exposing the key in command logs."""
    ceph_user = config.get("ceph_user", "client.admin")
    cmd = f"cephadm shell -- ceph auth get-key {shlex.quote(ceph_user)}"
    ceph_key = installer.exec_command(sudo=True, cmd=cmd)[0].strip()
    if not ceph_key:
        raise OperationFailedError(f"No cephx key returned for {ceph_user}")

    key_file = f"/tmp/ceph-smb-ctl-{uuid.uuid4().hex}.key"
    with command_node.remote_file(
        sudo=True, file_name=key_file, file_mode="w"
    ) as remote_key:
        remote_key.write(ceph_key)
        remote_key.flush()
    command_node.exec_command(sudo=True, cmd=f"chmod 600 {shlex.quote(key_file)}")
    return {"user": ceph_user, "key_file": key_file}


def _remove_ceph_auth(command_node, auth):
    """Remove temporary cephx material from the SMB node."""
    if auth:
        command_node.exec_command(
            sudo=True,
            cmd=f"rm -f {shlex.quote(auth['key_file'])}",
            check_ec=False,
        )


def _validate_json(output, command):
    """Return parsed ceph-smb-ctl JSON output."""
    try:
        return json.loads(output)
    except (TypeError, json.JSONDecodeError) as error:
        raise OperationFailedError(
            f"ceph-smb-ctl {command} did not return valid JSON: {output}"
        ) from error


def _validate_nonempty_json(output, command):
    """Return parsed JSON and reject empty successful responses."""
    value = _validate_json(output, command)
    if value in (None, {}, []):
        raise OperationFailedError(
            f"ceph-smb-ctl {command} returned an empty JSON response: {output}"
        )
    return value


def _debug_level(output):
    """Normalize bare and JSON debug-level responses for exact comparison."""
    try:
        value = json.loads(output)
    except json.JSONDecodeError:
        return output.strip()
    if isinstance(value, dict):
        for key in ("level", "debug_level"):
            if key in value:
                return str(value[key])
    if isinstance(value, (str, int, float)):
        return str(value)
    raise OperationFailedError(f"Unexpected debug-level response: {output}")


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


def _test_help(ctl, command_node, cluster_id):
    """Verify top-level and subcommand CLI discovery."""
    output = ctl(command_node, ["--help"], cluster_id)
    missing = [command for command in COMMANDS if command not in output]
    missing.extend(option for option in GLOBAL_OPTIONS if option not in output)
    if missing:
        raise OperationFailedError(
            f"ceph-smb-ctl --help is missing commands/options: {', '.join(missing)}"
        )

    for command, expected in SUBCOMMAND_OPTIONS.items():
        output = ctl(command_node, [command, "--help"], cluster_id)
        missing = [item for item in expected if item not in output]
        if missing:
            raise OperationFailedError(
                f"ceph-smb-ctl {command} --help is missing arguments/options: "
                f"{', '.join(missing)}"
            )


def _json_tokens(value):
    """Return all string values and mapping keys in a JSON-compatible value."""
    if isinstance(value, dict):
        tokens = set(value)
        for item in value.values():
            tokens.update(_json_tokens(item))
        return tokens
    if isinstance(value, list):
        tokens = set()
        for item in value:
            tokens.update(_json_tokens(item))
        return tokens
    return {value} if isinstance(value, str) else set()


def _status_has_client(status, ip_address):
    """Return whether a parsed status response refers to a client IP."""
    return any(ip_address in token for token in _json_tokens(status))


def _test_config_shares_list(
    ctl, command_node, cluster_id, expected_shares, source="samba"
):
    """Verify the server reports every configured share."""
    output = ctl(command_node, ["config-shares-list", source], cluster_id)
    configured = _json_tokens(_validate_json(output, "config-shares-list"))
    missing = [share for share in expected_shares if share not in configured]
    if missing:
        raise OperationFailedError(
            "ceph-smb-ctl config-shares-list is missing configured shares "
            f"{', '.join(missing)}: {output}"
        )


def _test_config_summary(ctl, command_node, cluster_id, source="samba", sha256=False):
    """Verify the configuration digest is non-empty and stable."""
    args = ["config-summary"]
    if sha256:
        args.append("--sha256")
    args.append(source)
    first = ctl(command_node, args, cluster_id)
    second = ctl(command_node, args, cluster_id)
    if not first:
        raise OperationFailedError("ceph-smb-ctl config-summary returned no data")
    if first != second:
        raise OperationFailedError(
            "ceph-smb-ctl config-summary changed without a configuration update: "
            f"first={first}, second={second}"
        )
    if sha256 and not re.search(r"(?i)(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])", first):
        raise OperationFailedError(
            f"ceph-smb-ctl config-summary did not return a SHA256 digest: {first}"
        )


def _test_global_options(ctl, command_node, cluster_id):
    """Exercise non-transport global flags against the local socket."""
    cases = (
        (["--quiet"], "quiet"),
        (["--verbose"], "verbose"),
        (["--header", "x-cephci-test=remote-control"], "header"),
    )
    for global_args, name in cases:
        output = ctl(
            command_node,
            ["info"],
            cluster_id,
            global_args=global_args,
        )
        _validate_json(output, f"info with {name}")


def _test_ctdb_status(ctl, command_node, cluster_id):
    """Verify CTDB status is available for the clustered SMB deployment."""
    output = ctl(command_node, ["ctdb-status"], cluster_id)
    status = _validate_json(output, "ctdb-status")
    if not status:
        raise OperationFailedError("ceph-smb-ctl ctdb-status returned no data")


def _test_debug_level(ctl, command_node, cluster_id, config):
    subsystem = config.get("subsystem", "smb")
    requested_level = str(config.get("debug_level", 5))
    original = ctl(command_node, ["get-debug-level", subsystem], cluster_id)
    original_level = _debug_level(original)
    log.info(
        "Original debug level for %s is %s; requested level is %s",
        subsystem,
        original_level,
        requested_level,
    )
    try:
        ctl(
            command_node,
            ["set-debug-level", subsystem, requested_level],
            cluster_id,
        )
        current = ctl(command_node, ["get-debug-level", subsystem], cluster_id)
        current_level = _debug_level(current)
        log.info("Current debug level for %s is %s", subsystem, current_level)
        if current_level != requested_level:
            raise OperationFailedError(
                f"Debug level for {subsystem} was not set to {requested_level}: "
                f"{current}"
            )
    finally:
        if original_level:
            ctl(
                command_node,
                ["set-debug-level", subsystem, original_level],
                cluster_id,
            )
            restored = ctl(command_node, ["get-debug-level", subsystem], cluster_id)
            restored_level = _debug_level(restored)
            log.info("Restored debug level for %s is %s", subsystem, restored_level)
            if restored_level != original_level:
                raise OperationFailedError(
                    f"Debug level for {subsystem} was not restored to "
                    f"{original_level}: {restored}"
                )


def _test_kill_client(ctl, command_node, smb_node, client, share, cluster_id, config):
    mount_point = config.get("cifs_mount_point", "/mnt/smb")
    _mount_client(smb_node, client, share, config)
    mounted = True
    try:
        status_output = ctl(command_node, ["status"], cluster_id)
        status = _validate_json(status_output, "status")
        if not _status_has_client(status, client.ip_address):
            raise OperationFailedError(
                f"Client {client.ip_address} is not present in SMB status"
            )
        output = ctl(
            command_node,
            ["kill-client-connection", client.ip_address],
            cluster_id,
        )
        _validate_json(output, "kill-client-connection")

        # An active Linux CIFS mount automatically reconnects after its SMB
        # connection is terminated. Unmount before polling so a replacement
        # connection from the same client IP does not cause a false failure.
        _unmount_client(client, mount_point)
        mounted = False

        disconnected = False
        for _ in WaitUntil(timeout=30, interval=5):
            status_output = ctl(command_node, ["status"], cluster_id)
            status = _validate_json(status_output, "status")
            if not _status_has_client(status, client.ip_address):
                disconnected = True
                break
        if not disconnected:
            raise OperationFailedError(
                f"Client {client.ip_address} remains connected after kill request"
            )
    finally:
        if mounted:
            _unmount_client(client, mount_point)


def _test_close_share(ctl, command_node, smb_node, client, share, cluster_id, config):
    """Invoke close-share while the selected share has an active client."""
    mount_point = config.get("cifs_mount_point", "/mnt/smb")
    _mount_client(smb_node, client, share, config)
    try:
        status = ctl(command_node, ["status"], cluster_id)
        if share not in status:
            raise OperationFailedError(
                f"Share {share} is not present in SMB status before close-share"
            )
        ctl(command_node, ["close-share", share], cluster_id)
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
    command_cluster_id = None
    if config.get("select_cluster"):
        selector_style = config.get("cluster_selector", "cluster_id")
        if selector_style == "cluster_id":
            command_cluster_id = cluster_id
        elif selector_style == "service_name":
            command_cluster_id = f"smb.{cluster_id}"
        elif selector_style == "fsid_service_name":
            fsid = installer.exec_command(sudo=True, cmd="cephadm shell -- ceph fsid")[
                0
            ].strip()
            command_cluster_id = f"{fsid}/smb.{cluster_id}"
        else:
            raise ConfigError(f"Unsupported cluster selector style: {selector_style}")

    check_smb_cluster(installer, cluster_id)
    verify_smb_service(installer, service_name="smb")
    shares = get_smb_shares(installer, cluster_id)

    auth = None
    try:
        if operation not in ("verify_configuration", "verify_sidecar"):
            auth = _prepare_ceph_auth(installer, command_node, config)
        ctl = partial(_ceph_smb_ctl, auth=auth)

        if operation == "verify_configuration":
            _verify_configuration(installer, cluster_id)
        elif operation == "verify_sidecar":
            _verify_sidecar(smb_node)
        elif operation == "help":
            _test_help(ctl, command_node, command_cluster_id)
        elif operation == "global_options":
            _test_global_options(ctl, command_node, command_cluster_id)
        elif operation == "ctdb_status":
            _test_ctdb_status(ctl, command_node, command_cluster_id)
        elif operation in ("info", "status"):
            output = ctl(command_node, [operation], command_cluster_id)
            _validate_nonempty_json(output, operation)
        elif operation == "config_dump":
            args = ["config-dump"]
            if config.get("sha256"):
                args.append("--sha256")
            args.append(config.get("config_source", "samba"))
            output = ctl(
                command_node,
                args,
                command_cluster_id,
            )
            if not output:
                raise OperationFailedError("ceph-smb-ctl config-dump returned no data")
            if config.get("sha256") and not re.search(
                r"(?i)(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])", output
            ):
                raise OperationFailedError(
                    f"ceph-smb-ctl config-dump did not return a SHA256 digest: {output}"
                )
        elif operation == "config_shares_list":
            if not shares:
                raise ConfigError(f"SMB cluster {cluster_id} has no shares")
            _test_config_shares_list(
                ctl,
                command_node,
                command_cluster_id,
                shares,
                config.get("config_source", "samba"),
            )
        elif operation == "config_summary":
            _test_config_summary(
                ctl,
                command_node,
                command_cluster_id,
                config.get("config_source", "samba"),
                config.get("sha256", False),
            )
        elif operation == "debug_level":
            _test_debug_level(ctl, command_node, command_cluster_id, config)
        elif operation == "kill_client_connection":
            if not clients:
                raise ConfigError("No node with the 'client' role is available")
            if not shares:
                raise ConfigError(f"SMB cluster {cluster_id} has no shares")
            _test_kill_client(
                ctl,
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
                ctl,
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
        _remove_ceph_auth(command_node, auth)
        if config.get("smb_cluster_cleanup"):
            smb_cleanup(installer, shares, cluster_id)
    return 0
