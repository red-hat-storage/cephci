"""
Shared helpers for NFSv4 delegation tests (cluster template, exports, Ganesha log capture).
"""

import json
import re
import time

from nfs_operations import get_ganesha_info_from_container, verify_nfs_ganesha_service

from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Unmount
from utility.log import Log

log = Log(__name__)

CONF_KEY = "mgr/cephadm/services/nfs/ganesha.conf"
DEFAULT_TEMPLATE_PATH = (
    "/usr/share/ceph/mgr/cephadm/templates/services/nfs/ganesha.conf.j2"
)
WORK_TEMPLATE_PATH = "/tmp/ganesha_delegation.conf.j2"
BACKUP_TEMPLATE_PATH = "/tmp/ganesha_delegation_backup.conf.j2"
MOUNTED_TEMPLATE_PATH = "/var/lib/ceph/ganesha.conf"
LOG_TEMPLATE_WORK_PATH = "/tmp/ganesha_logging_work.conf.j2"
LOG_TEMPLATE_BACKUP_PATH = "/tmp/ganesha_logging_backup.conf.j2"
GANESHA_FULL_DEBUG_LOG_BLOCK = """LOG {
    Default_Log_Level = FULL_DEBUG;
    Facility {
        name = FILE;
        destination = "/var/log/ganesha.log";
        enable = active;
    }
    Components {
        CONFIG = FULL_DEBUG;
        FSAL = FULL_DEBUG;
        NFS4 = FULL_DEBUG;
        STATE = FULL_DEBUG;
    }
}"""

DELEGATIONS_PATTERN = re.compile(r"Delegations\s*=\s*(true|false)\s*;", re.I)
SUPPORTED_DELEGATIONS = frozenset({"rw", "ro", "none"})

GANESHA_DELEG_TAILF_GREP_E = (
    "OPEN handler|END OF|CLOSE handler|successfully recalled|Successful exit|"
    "Share Access|DELEGRETURN|Recalling|Revoking|NFS4ERR_RECLAIM_BAD|do_delegation"
)
GANESHA_DELEG_TAILF_CAPTURE = "/tmp/ganesha_deleg_tailf.log"
GANESHA_DELEG_TAILF_PID = "/tmp/ganesha_deleg_tailf.pid"


def _upsert_ganesha_log_block(ganesha_template, log_block):
    """Insert or replace top-level LOG block in a ganesha template."""
    content = ganesha_template or ""
    marker = "LOG"
    start = content.find(marker)
    while start != -1:
        left_ok = start == 0 or not (
            content[start - 1].isalnum() or content[start - 1] == "_"
        )
        if left_ok:
            idx = start + len(marker)
            while idx < len(content) and content[idx].isspace():
                idx += 1
            if idx < len(content) and content[idx] == "{":
                brace_start = idx
                depth = 0
                end = brace_start
                while end < len(content):
                    if content[end] == "{":
                        depth += 1
                    elif content[end] == "}":
                        depth -= 1
                        if depth == 0:
                            end += 1
                            break
                    end += 1
                if depth != 0:
                    raise OperationFailedError(
                        "Unbalanced LOG block braces in template"
                    )
                return f"{content[:start].rstrip()}\n\n{log_block}\n\n{content[end:].lstrip()}"
        start = content.find(marker, start + len(marker))
    return f"{content.rstrip()}\n\n{log_block}\n"


def enable_ganesha_debug_logging(
    cmd_host,
    log_block=GANESHA_FULL_DEBUG_LOG_BLOCK,
    config_key=CONF_KEY,
):
    """
    Enable Ganesha debug logging by upserting LOG block into cephadm NFS template.

    Returns:
        bool: True if a prior custom template backup exists, False otherwise.
    """
    cmd_host.exec_command(
        sudo=True,
        cmd=(
            f"cephadm shell -- ceph config-key get {config_key} "
            f"> {LOG_TEMPLATE_BACKUP_PATH}"
        ),
        check_ec=False,
    )
    backup_out, _ = cmd_host.exec_command(
        sudo=True,
        cmd=(f"test -s {LOG_TEMPLATE_BACKUP_PATH} && echo present || echo absent"),
        check_ec=False,
    )
    backup_exists = "present" in (backup_out or "")

    current_template, _ = cmd_host.exec_command(
        sudo=True,
        cmd=f"cephadm shell -- ceph config-key get {config_key}",
        check_ec=False,
    )
    if not current_template or not str(current_template).strip():
        current_template, _ = cmd_host.exec_command(
            sudo=True,
            cmd=f"cephadm shell -- cat {DEFAULT_TEMPLATE_PATH}",
        )
    if not current_template or not str(current_template).strip():
        raise OperationFailedError("Unable to read Ganesha template for logging update")

    updated_template = _upsert_ganesha_log_block(current_template, log_block)
    fp = cmd_host.remote_file(
        sudo=True,
        file_name=LOG_TEMPLATE_WORK_PATH,
        file_mode="w",
    )
    fp.write(updated_template)
    fp.flush()
    fp.close()

    cmd_host.exec_command(
        sudo=True,
        cmd=(
            f"cephadm shell --mount {LOG_TEMPLATE_WORK_PATH}:{MOUNTED_TEMPLATE_PATH} -- "
            f"ceph config-key set {config_key} -i {MOUNTED_TEMPLATE_PATH}"
        ),
    )
    verify_out, _ = cmd_host.exec_command(
        sudo=True,
        cmd=f"cephadm shell -- ceph config-key get {config_key}",
    )
    if "Default_Log_Level = FULL_DEBUG;" not in (verify_out or ""):
        raise OperationFailedError(
            "Failed to enable Ganesha debug logging in ceph config-key template"
        )
    return backup_exists


def restore_ganesha_template_after_logging_update(
    cmd_host,
    backup_exists,
    config_key=CONF_KEY,
):
    """Restore prior cephadm NFS template state after logging update."""
    if backup_exists:
        cmd_host.exec_command(
            sudo=True,
            cmd=(
                f"cephadm shell --mount {LOG_TEMPLATE_BACKUP_PATH}:{MOUNTED_TEMPLATE_PATH} -- "
                f"ceph config-key set {config_key} -i {MOUNTED_TEMPLATE_PATH}"
            ),
        )
        return
    cmd_host.exec_command(
        sudo=True,
        cmd=f"cephadm shell -- ceph config-key rm {config_key}",
        check_ec=False,
    )


def ensure_ceph_conf_and_admin_keyring_on_host(source_node, target_node):
    """
    Place minimal ceph.conf and client.admin keyring on target_node.

    Mirrors test_client behavior so ``cephadm shell`` and ``ceph`` work on NFS hosts.
    """
    target_node.exec_command(sudo=True, cmd="mkdir -p /etc/ceph")

    ceph_conf, _ = source_node.exec_command(
        sudo=True,
        cmd="cephadm shell -- ceph config generate-minimal-conf",
        timeout=300,
    )
    if not ceph_conf or not str(ceph_conf).strip():
        raise OperationFailedError(
            "ceph config generate-minimal-conf returned empty output on %s"
            % getattr(source_node, "hostname", source_node)
        )
    conf_fp = target_node.remote_file(
        sudo=True, file_name="/etc/ceph/ceph.conf", file_mode="w"
    )
    conf_fp.write(ceph_conf)
    conf_fp.flush()
    conf_fp.close()

    admin_keyring, _ = source_node.exec_command(
        sudo=True,
        cmd="cephadm shell -- ceph auth get client.admin",
        timeout=300,
    )
    if not admin_keyring or not str(admin_keyring).strip():
        raise OperationFailedError(
            "ceph auth get client.admin returned empty output on %s"
            % getattr(source_node, "hostname", source_node)
        )
    key_fp = target_node.remote_file(
        sudo=True,
        file_name="/etc/ceph/ceph.client.admin.keyring",
        file_mode="w",
    )
    key_fp.write(admin_keyring)
    key_fp.flush()
    key_fp.close()

    log.info(
        "Installed /etc/ceph/ceph.conf and ceph.client.admin.keyring on %s",
        target_node.hostname,
    )


def ensure_ceph_conf_and_admin_keyring_on_hosts(source_node, target_nodes):
    """Push ceph.conf + admin keyring to all target nodes."""
    nodes = target_nodes if isinstance(target_nodes, list) else [target_nodes]
    for target in nodes:
        ensure_ceph_conf_and_admin_keyring_on_host(source_node, target)


def find_nfs_node_by_hostname(nfs_nodes, hostname):
    short_name = hostname.split(".")[0]
    for node in nfs_nodes:
        if node.hostname == hostname or node.hostname.split(".")[0] == short_name:
            return node
    raise OperationFailedError(f"Unable to map daemon host {hostname} to nfs node")


def run_cephadm_shell(node, command, check_ec=True):
    return node.exec_command(
        sudo=True,
        cmd=f"cephadm shell -- {command}",
        check_ec=check_ec,
    )


def resolve_nfs_cmd_host(nfs_nodes, config):
    if not nfs_nodes:
        raise ConfigError("No NFS nodes available for cephadm shell commands")
    want = config.get("nfs_cmd_hostname")
    if want:
        for n in nfs_nodes:
            if n.hostname == want or n.hostname.split(".")[0] == want.split(".")[0]:
                return n
        raise ConfigError(f"nfs_cmd_hostname={want!r} not found among nfs nodes")
    idx = int(config.get("nfs_cmd_host_index", 0))
    if idx < 0 or idx >= len(nfs_nodes):
        raise ConfigError(f"nfs_cmd_host_index={idx} out of range")
    return nfs_nodes[idx]


def backup_current_template(cmd_host):
    run_cephadm_shell(
        cmd_host,
        f"ceph config-key get {CONF_KEY} > {BACKUP_TEMPLATE_PATH}",
        check_ec=False,
    )
    out, _ = cmd_host.exec_command(
        sudo=True,
        cmd=f"test -s {BACKUP_TEMPLATE_PATH} && echo present || echo absent",
        check_ec=False,
    )
    return "present" in out


def restore_previous_template(cmd_host, backup_exists):
    if backup_exists:
        cmd_host.exec_command(
            sudo=True,
            cmd=(
                f"cephadm shell --mount {BACKUP_TEMPLATE_PATH}:{MOUNTED_TEMPLATE_PATH} -- "
                f"ceph config-key set {CONF_KEY} -i {MOUNTED_TEMPLATE_PATH}"
            ),
        )
        return
    run_cephadm_shell(cmd_host, f"ceph config-key rm {CONF_KEY}", check_ec=False)


def set_cluster_delegation(cmd_host, delegation):
    current_template, _ = run_cephadm_shell(
        cmd_host, f"ceph config-key get {CONF_KEY}", check_ec=False
    )
    if not current_template or not str(current_template).strip():
        current_template, _ = run_cephadm_shell(
            cmd_host, f"cat {DEFAULT_TEMPLATE_PATH}"
        )
    if not current_template or not str(current_template).strip():
        raise OperationFailedError("Unable to read NFS ganesha template")

    fp = cmd_host.remote_file(
        sudo=True,
        file_name=WORK_TEMPLATE_PATH,
        file_mode="w",
    )
    fp.write(current_template)
    fp.flush()
    fp.close()

    cmd_host.exec_command(
        sudo=True,
        cmd=(
            "sed -i -E "
            f"'s/(Delegations[[:space:]]*=[[:space:]]*)(true|false)([[:space:]]*;)/"
            f"\\1{delegation}\\3/' {WORK_TEMPLATE_PATH}"
        ),
    )
    out, _ = cmd_host.exec_command(
        sudo=True,
        cmd=f"grep -F 'Delegations = {delegation};' {WORK_TEMPLATE_PATH}",
    )
    if not out or f"Delegations = {delegation};" not in out:
        raise OperationFailedError(
            "sed did not produce Delegations = %s; in %s (check template path/content)"
            % (delegation, WORK_TEMPLATE_PATH)
        )
    cmd_host.exec_command(
        sudo=True,
        cmd=(
            f"cephadm shell --mount {WORK_TEMPLATE_PATH}:{MOUNTED_TEMPLATE_PATH} -- "
            f"ceph config-key set {CONF_KEY} -i {MOUNTED_TEMPLATE_PATH}"
        ),
    )
    current, _ = run_cephadm_shell(cmd_host, f"ceph config-key get {CONF_KEY}")
    expected = f"Delegations = {delegation};"
    if expected not in current:
        raise OperationFailedError(
            f"Config-key update failed, expected '{expected}' in template output"
        )


def redeploy_nfs_clusters(
    cephadm, nfs_clusters, installer, redeploy_wait, service_wait_timeout
):
    for cluster_name in nfs_clusters:
        cephadm.orch.redeploy(f"nfs.{cluster_name}")
    time.sleep(redeploy_wait)
    verify_nfs_ganesha_service(node=installer, timeout=service_wait_timeout)


def verify_container_delegation(cmd_host, nfs_nodes, daemons, nfs_name, delegation):
    if not any(
        get_ganesha_info_from_container(cmd_host, nfs_name, nfs_node)[1]
        for nfs_node in nfs_nodes
    ):
        raise OperationFailedError(
            f"Unable to retrieve ganesha container details for nfs service {nfs_name}"
        )

    for daemon in daemons:
        hostname = daemon.get("hostname")
        container_id = daemon.get("container_id")
        if not hostname or not container_id:
            raise OperationFailedError(f"Incomplete daemon metadata: {daemon}")
        nfs_node = find_nfs_node_by_hostname(nfs_nodes, hostname)
        conf, _ = nfs_node.exec_command(
            sudo=True, cmd=f"podman exec {container_id} cat /etc/ganesha/ganesha.conf"
        )
        match = DELEGATIONS_PATTERN.search(conf or "")
        if not match:
            raise OperationFailedError(
                f"Delegations entry not found in container {container_id} on {hostname}"
            )
        observed = match.group(1).lower()
        if observed != delegation:
            raise OperationFailedError(
                f"Delegations mismatch on {hostname}/{container_id}: "
                f"expected {delegation}, got {observed}"
            )
        log.info(
            "Verified Delegations = %s on %s (container %s)",
            delegation,
            hostname,
            container_id,
        )


def read_ganesha_conf_on_host(nfs_node, nfs_name):
    fsids = nfs_node.get_dir_list("/var/lib/ceph", sudo=True)
    if not fsids:
        raise OperationFailedError(
            f"Unable to locate /var/lib/ceph fsid path on {nfs_node.hostname}"
        )
    base_path = f"/var/lib/ceph/{fsids[0]}"
    nfs_full_names = [
        item for item in nfs_node.get_dir_list(base_path, sudo=True) if nfs_name in item
    ]
    if not nfs_full_names:
        raise OperationFailedError(
            f"No nfs instance directory found for {nfs_name} on {nfs_node.hostname}"
        )
    for instance_name in nfs_full_names:
        conf_path = f"{base_path}/{instance_name}/etc/ganesha/ganesha.conf"
        out, _ = nfs_node.exec_command(
            sudo=True, cmd=f"cat {conf_path}", check_ec=False
        )
        if out:
            return out
    raise OperationFailedError(
        f"Unable to read ganesha.conf for {nfs_name} on {nfs_node.hostname}"
    )


def verify_host_conf_delegation(nfs_nodes, nfs_name, delegation):
    for nfs_node in nfs_nodes:
        conf = read_ganesha_conf_on_host(nfs_node, nfs_name)
        match = DELEGATIONS_PATTERN.search(conf or "")
        if not match:
            raise OperationFailedError(
                f"Delegations entry not found in host ganesha.conf on {nfs_node.hostname}"
            )
        observed = match.group(1).lower()
        if observed != delegation:
            raise OperationFailedError(
                f"Delegations mismatch in host ganesha.conf on {nfs_node.hostname}: "
                f"expected {delegation}, got {observed}"
            )


def create_or_replace_export_with_delegation(
    cmd_host, fs_name, nfs_name, export_name, export_path, delegation
):
    run_cephadm_shell(
        cmd_host,
        f"ceph nfs export delete {nfs_name} {export_name}",
        check_ec=False,
    )
    run_cephadm_shell(
        cmd_host,
        f"ceph nfs export create {fs_name} {nfs_name} {export_name} "
        f"{fs_name} {export_path} --delegations {delegation}",
    )


def validate_export_delegation_value(cmd_host, nfs_name, export_name, expected):
    out, _ = run_cephadm_shell(
        cmd_host,
        f"ceph nfs export get {nfs_name} {export_name} --format json",
    )
    content = (out or "").strip()
    if not content:
        raise OperationFailedError("Empty output from `ceph nfs export get`")
    try:
        data = json.loads(content)
    except json.JSONDecodeError as err:
        raise OperationFailedError(
            f"Unable to parse export get output as json: {err}; output={content!r}"
        )
    observed = str(data.get("delegations", "")).strip().lower()
    if observed != expected:
        raise OperationFailedError(
            f"Delegations mismatch for {export_name}: expected {expected!r}, got {observed!r}"
        )


def update_export_delegation(cmd_host, nfs_name, export_name, delegation):
    run_cephadm_shell(
        cmd_host,
        f"ceph nfs export update {nfs_name} {export_name} '{delegation}' --format json",
    )


def assert_export_cmd_fails(cmd_host, command, action):
    out, err = run_cephadm_shell(cmd_host, command, check_ec=False)
    combined = f"{out or ''}\n{err or ''}".lower()
    if any(token in combined for token in ("error", "invalid", "einval", "usage")):
        log.info("Negative test passed for %s: command failed as expected", action)
        return
    raise OperationFailedError(
        f"Negative test failed for {action}: command unexpectedly succeeded"
    )


def normalize_delegation_list(values, config_key):
    raw = values if values is not None else []
    normalized = [str(value).strip().lower() for value in raw]
    if any(value not in SUPPORTED_DELEGATIONS for value in normalized):
        raise ConfigError(f"{config_key} accepts only rw, ro, none")
    return normalized


def run_negative_export_delegation_checks(
    cmd_host, fs_name, nfs_name, export_name, export_path, config
):
    invalid_values = config.get("negative_delegation_values", ["invalid", "bad"])
    invalid_values = [str(v).strip() for v in invalid_values if str(v).strip()]
    if not invalid_values:
        raise ConfigError("negative_delegation_values must contain at least one value")

    seed = str(config.get("export_update_seed_delegation", "rw")).strip().lower()
    if seed not in SUPPORTED_DELEGATIONS:
        seed = "rw"

    create_or_replace_export_with_delegation(
        cmd_host, fs_name, nfs_name, export_name, export_path, seed
    )
    validate_export_delegation_value(cmd_host, nfs_name, export_name, seed)

    for invalid in invalid_values:
        assert_export_cmd_fails(
            cmd_host,
            (
                f"ceph nfs export create {fs_name} {nfs_name} {export_name} "
                f"{fs_name} {export_path} --delegations {invalid}"
            ),
            f"export create with invalid delegations={invalid!r}",
        )
        assert_export_cmd_fails(
            cmd_host,
            f"ceph nfs export update {nfs_name} {export_name} '{invalid}' --format json",
            f"export update with invalid delegations={invalid!r}",
        )


def run_export_level_delegation_checks(cmd_host, fs_name, nfs_name, config):
    group_name = config.get("subvolume_group", "ganeshagroup")
    subvolume_name = config.get(
        "subvolume_name", f"subvol_delegation_{int(time.time())}"
    )
    export_name = config.get("export_name", "/export1")
    delegation_values = normalize_delegation_list(
        config.get("delegation_values", ["rw", "ro", "none"]),
        "delegation_values",
    )
    export_update_values = normalize_delegation_list(
        config.get("export_update_values", ["ro", "rw", "none"]),
        "export_update_values",
    )
    run_export_create_checks = bool(config.get("run_export_create_checks", True))
    run_export_update_checks = bool(config.get("run_export_update_checks", True))
    run_negative_export_checks = bool(config.get("run_negative_export_checks", True))
    if not run_export_create_checks and not run_export_update_checks:
        raise ConfigError(
            "At least one of run_export_create_checks/run_export_update_checks must be true"
        )

    run_cephadm_shell(
        cmd_host,
        f"ceph fs subvolumegroup create {fs_name} {group_name}",
        check_ec=False,
    )
    run_cephadm_shell(
        cmd_host,
        f"ceph fs subvolume create {fs_name} {subvolume_name} {group_name}",
    )
    out, _ = run_cephadm_shell(
        cmd_host,
        f"ceph fs subvolume getpath {fs_name} {subvolume_name} --group_name {group_name}",
    )
    export_path = (out or "").strip()
    if not export_path:
        raise OperationFailedError(
            f"Unable to fetch subvolume path for {subvolume_name}"
        )

    try:
        if run_export_create_checks:
            for delegation in delegation_values:
                log.info("Applying export-level delegations via create=%s", delegation)
                create_or_replace_export_with_delegation(
                    cmd_host, fs_name, nfs_name, export_name, export_path, delegation
                )
                validate_export_delegation_value(
                    cmd_host, nfs_name, export_name, delegation
                )

        if run_export_update_checks:
            seed = (
                str(config.get("export_update_seed_delegation", "rw")).strip().lower()
            )
            if seed not in SUPPORTED_DELEGATIONS:
                raise ConfigError(
                    "export_update_seed_delegation accepts only rw, ro, none"
                )
            log.info(
                "Seeding export before runtime update checks with delegations=%s", seed
            )
            create_or_replace_export_with_delegation(
                cmd_host, fs_name, nfs_name, export_name, export_path, seed
            )
            validate_export_delegation_value(cmd_host, nfs_name, export_name, seed)

            for delegation in export_update_values:
                log.info("Applying export-level delegations via update=%s", delegation)
                update_export_delegation(cmd_host, nfs_name, export_name, delegation)
                validate_export_delegation_value(
                    cmd_host, nfs_name, export_name, delegation
                )

        if run_negative_export_checks:
            run_negative_export_delegation_checks(
                cmd_host, fs_name, nfs_name, export_name, export_path, config
            )
    finally:
        run_cephadm_shell(
            cmd_host,
            f"ceph nfs export delete {nfs_name} {export_name}",
            check_ec=False,
        )
        run_cephadm_shell(
            cmd_host,
            f"ceph fs subvolume rm {fs_name} {subvolume_name} --group_name {group_name}",
            check_ec=False,
        )


def cleanup_created_nfs_cluster(clients, nfs_cmd_host, nfs_mount, nfs_name, nfs_export):
    try:
        Unmount(clients[0]).unmount(nfs_mount)
    except Exception:
        pass
    clients[0].exec_command(sudo=True, cmd=f"rm -rf {nfs_mount}", check_ec=False)
    run_cephadm_shell(
        nfs_cmd_host,
        f"ceph nfs export delete {nfs_name} {nfs_export}_0",
        check_ec=False,
    )
    run_cephadm_shell(
        nfs_cmd_host,
        f"ceph nfs cluster delete {nfs_name}",
        check_ec=False,
    )
    time.sleep(30)


def ensure_cephfs_volume_exists(client, volume_name):
    """Ensure CephFS volume exists; create if missing (same pattern as other NFS tests)."""
    out, _ = client.exec_command(
        sudo=True,
        cmd="ceph fs volume ls --format json",
        check_ec=False,
    )
    raw = (out or "").strip()
    try:
        volumes = json.loads(raw)
        if isinstance(volumes, list):
            names = {
                str(v.get("name", "")).strip()
                for v in volumes
                if isinstance(v, dict) and v.get("name")
            }
            if volume_name in names:
                log.info("CephFS volume %s already present", volume_name)
                return
    except (json.JSONDecodeError, TypeError, ValueError):
        pass

    out_text, _ = client.exec_command(sudo=True, cmd="ceph fs volume ls")
    listing = (out_text or "").strip()
    if listing == "[]" or not listing:
        log.info("No CephFS volumes listed; creating volume %s", volume_name)
        client.exec_command(sudo=True, cmd=f"ceph fs volume create {volume_name}")
        return
    if volume_name in listing:
        log.info("CephFS volume %s already present (text volume ls)", volume_name)
        return

    log.info("CephFS volume %s not found; creating volume", volume_name)
    client.exec_command(sudo=True, cmd=f"ceph fs volume create {volume_name}")


def first_nfs_container_id(installer, nfs_nodes, nfs_name):
    """Resolve one running Ganesha container id for this NFS service."""
    for nfs_node in nfs_nodes:
        _, info = get_ganesha_info_from_container(installer, nfs_name, nfs_node)
        if info and info.get("container_id"):
            return nfs_node, info["container_id"]
    raise OperationFailedError(f"No Ganesha container found for nfs service {nfs_name}")


def truncate_ganesha_container_log(nfs_node, container_id):
    nfs_node.exec_command(
        sudo=True,
        cmd=f"podman exec {container_id} sh -c ':> /var/log/ganesha.log'",
        check_ec=False,
    )


def ganesha_log_line_count(nfs_node, container_id):
    raw, _ = nfs_node.exec_command(
        sudo=True,
        cmd=f"podman exec {container_id} wc -l /var/log/ganesha.log",
        check_ec=False,
    )
    first = (raw or "").strip().split()
    if not first:
        return 0
    try:
        return max(0, int(first[0]))
    except ValueError:
        return 0


def ganesha_log_lines_since(nfs_node, container_id, start_line):
    next_line = max(1, int(start_line) + 1)
    raw, _ = nfs_node.exec_command(
        sudo=True,
        cmd=f"podman exec {container_id} tail -n +{next_line} /var/log/ganesha.log",
        check_ec=False,
    )
    if not raw:
        return []
    return raw.splitlines()


def start_ganesha_deleg_tailf_follow(
    nfs_node, container_id, capture_path=None, grep_e=None
):
    capture = capture_path or GANESHA_DELEG_TAILF_CAPTURE
    pattern = grep_e or GANESHA_DELEG_TAILF_GREP_E
    pid_file = GANESHA_DELEG_TAILF_PID
    nfs_node.exec_command(
        sudo=True,
        cmd=(
            f"podman exec {container_id} bash -c "
            f"'rm -f {capture} {pid_file}; "
            f'nohup bash -c "tail -f /var/log/ganesha.log 2>/dev/null | '
            f'grep --line-buffered -E \\"{pattern}\\" >> {capture}" '
            f">/dev/null 2>&1 & echo \\$! > {pid_file}'"
        ),
        check_ec=False,
    )
    time.sleep(2)
    log.info(
        "Started tail -f | grep --line-buffered -E on container %s -> %s",
        container_id,
        capture,
    )


def stop_ganesha_deleg_tailf_follow(nfs_node, container_id):
    pid_file = GANESHA_DELEG_TAILF_PID
    nfs_node.exec_command(
        sudo=True,
        cmd=(
            f"podman exec {container_id} bash -c "
            f"'if [ -f {pid_file} ]; then "
            f"kill $(cat {pid_file}) 2>/dev/null; "
            f"pkill -P $(cat {pid_file}) 2>/dev/null; fi; "
            f'pkill -f "grep --line-buffered -E" 2>/dev/null; '
            f'pkill -f "tail -f /var/log/ganesha.log" 2>/dev/null; true\''
        ),
        check_ec=False,
    )
    time.sleep(3)


def read_ganesha_deleg_tailf_capture(nfs_node, container_id, capture_path=None):
    capture = capture_path or GANESHA_DELEG_TAILF_CAPTURE
    raw, _ = nfs_node.exec_command(
        sudo=True,
        cmd=f"podman exec {container_id} cat {capture}",
        check_ec=False,
    )
    if not raw:
        return []
    return [ln for ln in raw.splitlines() if ln.strip()]


GANESHA_DELEG_LOG_PATTERN = re.compile(
    r"OPEN handler|END OF nfs4_op_open|CLOSE handler|successfully recalled|Successful exit|"
    r"Share Access|DELEGRETURN|OP_DELEGRETURN|Recalling|Revoking|NFS4ERR_RECLAIM_BAD|"
    r"do_delegation|delegation_type|Attempting to grant delegation|Delegation type not supported",
    re.I,
)
_RE_ATTEMPT_GRANT = re.compile(r"Attempting to grant delegation", re.I)
_RE_DELEG_NOT_SUPPORTED = re.compile(r"Delegation type not supported", re.I)


def clamp_delegation_log_settle_seconds(config, default=82, lo=75, hi=90):
    settle = int(config.get("delegation_log_settle_seconds", default))
    return max(lo, min(hi, settle))


def nfs4_open_delegation_types(window_text):
    """Parse ``delegation_type N`` from each ``END OF nfs4_op_open`` line (Ganesha NFSv4)."""
    types = []
    for ln in (window_text or "").splitlines():
        if "END OF nfs4_op_open" not in ln:
            continue
        m = re.search(r"delegation_type\s+(\d+)", ln, re.I)
        if m:
            types.append(int(m.group(1)))
    return types


def delegation_log_hits_from_lines(lines):
    return [ln for ln in lines if GANESHA_DELEG_LOG_PATTERN.search(ln)]


def validate_delegation_ganesha_window(deleg, window_text):
    """
    Validate Ganesha log window against export ``delegations=`` mode.

    ``delegation_type`` on ``END OF nfs4_op_open``: 0 = none, 4 = read, 5 = write.
    Returns (ok, failure_reason).
    """
    text = window_text or ""
    types = nfs4_open_delegation_types(text)

    if deleg == "rw":
        if not _RE_ATTEMPT_GRANT.search(text):
            return (
                False,
                "missing 'Attempting to grant delegation' (write delegation path)",
            )
        if 5 not in types:
            return (
                False,
                f"expected delegation_type 5 (write deleg) on END OF nfs4_op_open, parsed types={types!r}",
            )
        return True, ""

    if deleg == "ro":
        if not _RE_DELEG_NOT_SUPPORTED.search(text):
            return (
                False,
                "missing 'Delegation type not supported' (expected on write OPEN for ro export)",
            )
        if not _RE_ATTEMPT_GRANT.search(text):
            return (
                False,
                "missing 'Attempting to grant delegation' (expected on read OPEN for ro export)",
            )
        if 4 not in types:
            return (
                False,
                f"expected delegation_type 4 (read deleg) on END OF nfs4_op_open, parsed types={types!r}",
            )
        if 5 in types:
            return (
                False,
                "unexpected delegation_type 5 (write deleg) on read-only export",
            )
        return True, ""

    if deleg == "none":
        if not _RE_DELEG_NOT_SUPPORTED.search(text):
            return (
                False,
                "missing 'Delegation type not supported' for delegations=none export",
            )
        if any(t in (4, 5) for t in types):
            return (
                False,
                f"delegations=none but saw read/write delegation types in nfs4_op_open: {types!r}",
            )
        if not types:
            if re.search(r"delegation_type\s+0\b", text) and not re.search(
                r"delegation_type\s+[45]\b", text
            ):
                return True, ""
            return (
                False,
                "could not parse delegation_type from END OF nfs4_op_open lines",
            )
        if not all(t == 0 for t in types):
            return (
                False,
                f"expected only delegation_type 0 on delegations=none export, got {types!r}",
            )
        return True, ""

    return False, f"unknown delegations mode {deleg!r}"


def create_delegation_exports(cmd_host, fs_name, nfs_name, subvolume_group, exports):
    """
    Create subvolumes and NFS exports for delegation scenario tests.

    ``exports``: iterable of ``(pseudo_path, delegations)`` e.g. ``("/deleg_rw", "rw")``.
    Returns list of subvolume names created.
    """
    run_cephadm_shell(
        cmd_host,
        f"ceph fs subvolumegroup create {fs_name} {subvolume_group}",
        check_ec=False,
    )
    subvol_names = []
    for pseudo, deleg in exports:
        sv = f"sv_{pseudo.strip('/').replace('-', '_')}_{int(time.time())}"
        subvol_names.append(sv)
        run_cephadm_shell(
            cmd_host,
            f"ceph fs subvolume create {fs_name} {sv} {subvolume_group}",
        )
        out, _ = run_cephadm_shell(
            cmd_host,
            f"ceph fs subvolume getpath {fs_name} {sv} --group_name {subvolume_group}",
        )
        path = (out or "").strip()
        if not path:
            raise OperationFailedError(f"Empty subvolume path for {sv}")
        run_cephadm_shell(
            cmd_host,
            f"ceph nfs export delete {nfs_name} {pseudo}",
            check_ec=False,
        )
        run_cephadm_shell(
            cmd_host,
            f"ceph nfs export create {fs_name} {nfs_name} {pseudo} "
            f"{fs_name} {path} --delegations {deleg}",
        )
        validate_export_delegation_value(cmd_host, nfs_name, pseudo, deleg)
    return subvol_names


def run_delegation_export_client_io(client, testfile, deleg):
    """Run client IO appropriate for ro/rw/none export delegation mode."""
    if deleg == "none":
        client.exec_command(
            sudo=True,
            cmd=f"bash -c 'echo none-deleg > {testfile} && sync'",
        )
        client.exec_command(sudo=True, cmd=f"cat {testfile}")
    elif deleg == "ro":
        client.exec_command(
            sudo=True,
            cmd=f"bash -c 'echo ro-deleg > {testfile} && sync'",
        )
        client.exec_command(sudo=True, cmd=f"cat {testfile}")
        client.exec_command(sudo=True, cmd=f"stat {testfile}")
    elif deleg == "rw":
        client.exec_command(
            sudo=True,
            cmd=f"bash -c 'echo rw-deleg > {testfile} && sync'",
        )
        client.exec_command(
            sudo=True,
            cmd=f"bash -c 'echo append >> {testfile} && sync'",
        )
        client.exec_command(sudo=True, cmd=f"cat {testfile}")
    else:
        raise ConfigError(f"Unsupported delegations mode for client IO: {deleg!r}")


def teardown_delegation_exports(
    cmd_host, fs_name, nfs_name, subvolume_group, export_pseudos, subvol_names
):
    for pseudo in export_pseudos:
        run_cephadm_shell(
            cmd_host,
            f"ceph nfs export delete {nfs_name} {pseudo}",
            check_ec=False,
        )
    for sv in reversed(subvol_names):
        run_cephadm_shell(
            cmd_host,
            f"ceph fs subvolume rm {fs_name} {sv} --group_name {subvolume_group}",
            check_ec=False,
        )
    run_cephadm_shell(
        cmd_host,
        f"ceph fs subvolumegroup rm {fs_name} {subvolume_group} --force",
        check_ec=False,
    )


def enable_cluster_delegation_and_debug_logging(
    nfs_cmd_host, cephadm, nfs_clusters, installer, redeploy_wait, service_wait_timeout
):
    """Enable cluster Delegations=true and Ganesha FULL_DEBUG logging; returns template backups."""
    deleg_backup = backup_current_template(nfs_cmd_host)
    log.info("Enabling cluster-level Delegations=true")
    set_cluster_delegation(nfs_cmd_host, "true")
    redeploy_nfs_clusters(
        cephadm, nfs_clusters, installer, redeploy_wait, service_wait_timeout
    )
    log.info("Enabling Ganesha FULL_DEBUG file logging to /var/log/ganesha.log")
    log_backup = enable_ganesha_debug_logging(nfs_cmd_host)
    redeploy_nfs_clusters(
        cephadm, nfs_clusters, installer, redeploy_wait, service_wait_timeout
    )
    return deleg_backup, log_backup
