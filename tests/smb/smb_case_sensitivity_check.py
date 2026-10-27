import json
from copy import deepcopy
from shlex import quote
from time import sleep
from uuid import uuid4

from smb_operations import (
    apply_smb_spec,
    check_ctdb_health,
    check_rados_clustermeta,
    check_smb_cluster,
    create_smb_cluster,
    create_smb_share,
    enable_smb_module,
    generate_smb_spec,
    smb_cifs_mount,
    smb_cleanup,
    smbclient_check_shares,
    verify_smb_service,
)

from ceph.ceph_admin import CephAdmin
from cli.exceptions import ConfigError
from cli.utilities.utils import create_files, remove_files
from utility.log import Log

log = Log(__name__)


WARNING = "CephFS subvolume is case sensitive; case insensitive mode is recommended"
REQUIRED = "CephFS subvolume is case sensitive; case insensitive mode is required"


def _command_result(installer, command):
    """Keep exit status and response data, including expected CLI failures."""
    out, err, status, _ = installer.exec_command(
        sudo=True, cmd=command, check_ec=False, verbose=True
    )
    return out, err, status


def _verify_share_result(result, success, warning):
    if result.get("success") is not success:
        raise AssertionError(f"Unexpected share result: {result}")
    warnings = result.get("warnings", [])
    if warnings != ([WARNING] if warning else []):
        raise AssertionError(f"Unexpected share warnings: {warnings}")
    if not success and result.get("msg") != REQUIRED:
        raise AssertionError(f"Unexpected rejection message: {result}")


def _run_share_scenario(ceph_cluster, config):
    """Exercise report SMB scenarios, including expected apply/CLI rejections."""
    installer = ceph_cluster.get_nodes(role="installer")[0]
    client = ceph_cluster.get_nodes(role="client")[0]
    smb_nodes = ceph_cluster.get_nodes("smb")
    spec = deepcopy(config["spec"])
    cluster = next(r for r in spec if r["resource_type"] == "ceph.smb.cluster")
    shares = [r for r in spec if r["resource_type"] == "ceph.smb.share"]
    users = next(r for r in spec if r["resource_type"] == "ceph.smb.usersgroups")
    user = users["values"]["users"][0]
    cluster_id = cluster["cluster_id"]
    volume = shares[0]["cephfs"]["volume"]
    group = shares[0]["cephfs"]["subvolumegroup"]
    prefix = "cephadm shell -- ceph"
    sensitive = config.get("protocols", "default") == "default"
    declarative = config.get("deployment_method", "imperative") == "declarative"
    invalid = config.get("invalid_value", False)
    failed = sensitive and any(
        r["cephfs"].get("case_insensitive") == "require" for r in shares
    )
    created_subvols = []
    spec_file = None
    result_code = 0
    try:
        installer.exec_command(
            sudo=True, cmd=f"{prefix} fs subvolumegroup create {volume} {group}"
        )
        for share in shares:
            subvol = share["cephfs"]["subvolume"]
            if subvol in created_subvols:
                continue
            installer.exec_command(
                sudo=True,
                cmd=(
                    f"{prefix} fs subvolume create {volume} {subvol} "
                    f"--group-name {group} --mode 0777 "
                    + ("--protocols smb" if not sensitive else "")
                ),
            )
            created_subvols.append(subvol)
            out, _ = installer.exec_command(
                sudo=True,
                cmd=f"{prefix} fs subvolume info {volume} {subvol} --group-name {group}",
            )
            info = json.loads(out)
            log.info(f"Subvolume info for {volume}/{group}/{subvol}: {info}")
            if info.get("casesensitive") is not sensitive:
                raise AssertionError(f"Unexpected subvolume case sensitivity: {info}")
        enable_smb_module(installer, cluster_id)
        sleep(30)
        if declarative:
            spec_file = generate_smb_spec(installer, "yaml", spec)
            out, err, status = _command_result(
                installer,
                f"cephadm shell --mount /tmp:/tmp -- ceph smb apply -i {quote(spec_file)}",
            )
            response = json.loads(out)
            if response.get("success") is not (not failed) or (status != 0) != failed:
                raise AssertionError(f"Unexpected apply outcome: {out} {err}")
            results = response["results"]
            expected_ids = {r["share_id"] for r in shares}
            actual_ids = {
                r["resource"]["share_id"]
                for r in results
                if r["resource"]["resource_type"] == "ceph.smb.share"
            }
            if actual_ids != expected_ids or len(results) != len(spec):
                raise AssertionError("Apply did not return every resource exactly once")
            warning_count = 0
            for resource in spec:
                matches = [
                    r
                    for r in results
                    if all(
                        r["resource"].get(k) == v
                        for k, v in resource.items()
                        if k
                        in (
                            "resource_type",
                            "share_id",
                            "cluster_id",
                            "users_groups_id",
                        )
                    )
                ]
                if len(matches) != 1:
                    raise AssertionError(
                        f"Missing or duplicate resource result: {resource}"
                    )
                item = matches[0]
                if resource["resource_type"] != "ceph.smb.share":
                    if item.get("success") is not True:
                        raise AssertionError(f"Unexpected resource failure: {item}")
                    continue
                policy = resource["cephfs"].get("case_insensitive", "warn")
                warning = sensitive and policy == "warn"
                _verify_share_result(
                    item, not (sensitive and policy == "require"), warning
                )
                warning_count += int(warning)
            summary = response.get("warnings_summary", {})
            if summary.get("count", 0) != warning_count:
                raise AssertionError(f"Unexpected warnings summary: {summary}")
            if warning_count and summary.get("recap") != [WARNING]:
                raise AssertionError(f"Unexpected warning recap: {summary}")
        else:
            create_smb_cluster(
                installer,
                cluster_id,
                cluster["auth_mode"],
                None,
                user["name"],
                user["password"],
                config.get("custom_dns"),
                cluster.get("clustering", "default"),
                config.get("client_compat"),
            )
            share = shares[0]
            policy = share["cephfs"].get("case_insensitive")
            option = f" --case_insensitive {quote(policy)}" if policy else ""
            out, err, status = _command_result(
                installer,
                f"{prefix} smb share create {cluster_id} {share['share_id']} "
                f"{volume} / --subvolume {group}/{share['cephfs']['subvolume']}"
                + option,
            )
            if invalid:
                if status == 0 or "not in warn|ignore|require" not in out + err:
                    raise AssertionError(
                        f"Invalid policy was not rejected: {out} {err}"
                    )
            else:
                if (status != 0) != failed:
                    raise AssertionError(
                        f"Unexpected CLI status: {status}: {out} {err}"
                    )
                _verify_share_result(
                    json.loads(out), not failed, sensitive and policy in (None, "warn")
                )
        out, _ = (
            installer.exec_command(sudo=True, cmd=f"{prefix} smb share ls {cluster_id}")
            if not (declarative and failed)
            else ("[]", "")
        )
        if declarative and failed:
            # Apply validation is atomic: checked resources are not persisted.
            out, _ = installer.exec_command(sudo=True, cmd=f"{prefix} smb cluster ls")
            if cluster_id in json.loads(out):
                raise AssertionError(
                    "Rejected apply unexpectedly persisted its cluster"
                )
        else:
            expected = [] if failed or invalid else [r["share_id"] for r in shares]
            if sorted(json.loads(out)) != sorted(expected):
                raise AssertionError(f"Unexpected persisted shares: {out}")
            if expected:
                # A rejected share does not require an SMB service deployment.
                verify_smb_service(installer, service_name="smb")
                smbclient_check_shares(
                    smb_nodes,
                    client,
                    expected,
                    user["name"],
                    user["password"],
                    cluster["auth_mode"],
                    None,
                    "445",
                    None,
                )
    except Exception as exc:
        log.error(f"SMB case sensitivity scenario failed: {exc}")
        result_code = 1
    finally:
        # Rejected resources may not exist; remove only resources owned by this test.
        for share in shares:
            _command_result(
                installer, f"{prefix} smb share rm {cluster_id} {share['share_id']}"
            )
        _command_result(installer, f"{prefix} smb cluster rm {cluster_id}")
        if declarative:
            cleanup_file = generate_smb_spec(
                installer,
                "yaml",
                [
                    {
                        "resource_type": "ceph.smb.usersgroups",
                        "users_groups_id": users["users_groups_id"],
                        "intent": "removed",
                    }
                ],
            )
            _, err, status = _command_result(
                installer,
                "cephadm shell --mount /tmp:/tmp -- ceph smb apply -i "
                + quote(cleanup_file),
            )
            if status:
                log.error(f"Users/groups cleanup failed: {err}")
                result_code = 1
            installer.exec_command(sudo=True, cmd=f"rm -f {quote(cleanup_file)}")
        for subvol in created_subvols:
            _, err, status = _command_result(
                installer,
                f"{prefix} fs subvolume rm {volume} {subvol} --group-name {group}",
            )
            if status:
                log.error(f"Subvolume cleanup failed: {err}")
                result_code = 1
        _, err, status = _command_result(
            installer, f"{prefix} fs subvolumegroup rm {volume} {group} --force"
        )
        if status:
            log.error(f"Subvolume group cleanup failed: {err}")
            result_code = 1
        if spec_file:
            installer.exec_command(sudo=True, cmd=f"rm -f {quote(spec_file)}")
    return result_code


def run(ceph_cluster, **kw):
    """Verify SMB protocol subvolume defaults and exercise a declarative share."""
    config = kw.get("config") or {}
    if not config.get("spec"):
        raise ConfigError("Mandatory config 'spec' not provided")

    # Protocol defaults must be tested on new subvolumes, never an existing
    # group/subvolume left by another test. Keep cleanup scoped to this run.
    config = deepcopy(config)
    group_suffix = uuid4().hex[:8]
    for resource in config["spec"]:
        if resource["resource_type"] == "ceph.smb.share":
            cephfs = resource["cephfs"]
            cephfs["subvolumegroup"] = f"{cephfs['subvolumegroup']}-{group_suffix}"

    if config.get("scenario"):
        return _run_share_scenario(ceph_cluster, config)

    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    installer = ceph_cluster.get_nodes(role="installer")[0]
    smb_nodes = ceph_cluster.get_nodes("smb")
    client = ceph_cluster.get_nodes(role="client")[0]
    smb_spec = config["spec"]
    file_mount = config.get("file_mount", "/tmp")
    cifs_mount_point = config.get("cifs_mount_point", "/mnt/smb")
    smb_subvolume_mode = config.get("smb_subvolume_mode", "0777")
    protocols = config.get("protocols", "smb")
    if protocols not in ("smb", "default"):
        raise ConfigError("'protocols' must be 'smb' or 'default'")
    expected_casesensitive = protocols == "default"
    deployment_method = config.get("deployment_method", "declarative").lower()
    if deployment_method not in ("declarative", "imperative"):
        raise ConfigError("'deployment_method' must be 'declarative' or 'imperative'")
    if deployment_method == "declarative" and not config.get("file_type"):
        raise ConfigError(
            "Mandatory config 'file_type' required for declarative deployment"
        )

    smb_shares, smb_subvols = [], []
    smb_cluster_id = None
    for spec in smb_spec:
        resource_type = spec["resource_type"]
        if resource_type == "ceph.smb.cluster":
            smb_cluster_id = spec["cluster_id"]
            auth_mode = spec["auth_mode"]
            clustering = spec.get("clustering", "default")
            domain_realm = spec.get("domain_settings", {}).get("realm")
            public_addrs = [
                addr["address"].split("/")[0] for addr in spec.get("public_addrs", [])
            ] or None
            smb_port = spec.get("custom_ports", {}).get("smb", "445")
        elif resource_type == "ceph.smb.usersgroups":
            user = spec["values"]["users"][0]
            smb_user_name, smb_user_password = user["name"], user["password"]
        elif resource_type == "ceph.smb.join.auth":
            smb_user_name = spec["auth"]["username"]
            smb_user_password = spec["auth"]["password"]
        elif resource_type == "ceph.smb.share":
            cephfs_vol = spec["cephfs"]["volume"]
            smb_subvol_group = spec["cephfs"]["subvolumegroup"]
            smb_subvols.append(spec["cephfs"]["subvolume"])
            smb_shares.append(spec["share_id"])

    if not smb_cluster_id or not smb_shares or not smb_subvols:
        raise ConfigError("SMB cluster and share resources are required in spec")

    try:
        # The volume is provisioned by the suite. Create the SMB group and
        # subvolume using the protocol option under test.
        installer.exec_command(
            sudo=True,
            cmd=f"cephadm shell -- ceph fs subvolumegroup create {cephfs_vol} {smb_subvol_group}",
        )
        for subvol in smb_subvols:
            installer.exec_command(
                sudo=True,
                cmd=(
                    f"cephadm shell -- ceph fs subvolume create {cephfs_vol} {subvol} "
                    f"--group-name={smb_subvol_group} --mode={smb_subvolume_mode} "
                    f"--protocols {protocols}"
                ),
            )

            out, _ = installer.exec_command(
                sudo=True,
                cmd=(
                    f"cephadm shell -- ceph fs subvolume info {cephfs_vol} {subvol} "
                    f"--group-name={smb_subvol_group} --format=json"
                ),
            )
            info = json.loads(out)
            log.info(
                f"Subvolume info for {cephfs_vol}/{smb_subvol_group}/{subvol} "
                f"with protocols={protocols}: {info}"
            )
            if info.get("casesensitive") is not expected_casesensitive:
                raise AssertionError(
                    f"Expected {subvol} casesensitive={expected_casesensitive}, "
                    f"got {info.get('casesensitive')} on a fresh subvolume "
                    f"in group {smb_subvol_group}. Full subvolume info: {info}"
                )
            earmark = info.get("earmark")
            if (protocols == "smb" and earmark != "smb") or (
                protocols == "default" and earmark == "smb"
            ):
                raise AssertionError(
                    f"Unexpected {subvol} earmark for protocols={protocols}: {earmark}"
                )

        # Deploy the SMB cluster and share using the selected interface.
        if deployment_method == "declarative":
            enable_smb_module(installer, smb_cluster_id)
            sleep(30)
            spec_file = generate_smb_spec(installer, config["file_type"], smb_spec)
            apply_smb_spec(installer, spec_file, file_mount)
            verify_smb_service(installer, service_name="smb")
        else:
            enable_smb_module(installer, smb_cluster_id)
            sleep(30)
            create_smb_cluster(
                installer,
                smb_cluster_id,
                auth_mode,
                domain_realm,
                smb_user_name,
                smb_user_password,
                config.get("custom_dns"),
                clustering,
                config.get("client_compat"),
            )
            check_smb_cluster(installer, smb_cluster_id)
            create_smb_share(
                installer,
                smb_shares,
                smb_cluster_id,
                cephfs_vol,
                config.get("path", "/"),
                smb_subvol_group,
                smb_subvols,
            )
            verify_smb_service(installer, service_name="smb")

        if clustering != "never":
            if not check_rados_clustermeta(cephadm, smb_cluster_id, smb_nodes):
                raise AssertionError("RADOS clustermeta for SMB cluster was not found")
            if not check_ctdb_health(smb_nodes, smb_cluster_id):
                raise AssertionError("CTDB health check failed")

        smbclient_check_shares(
            smb_nodes,
            client,
            smb_shares,
            smb_user_name,
            smb_user_password,
            auth_mode,
            domain_realm,
            smb_port,
            public_addrs,
        )
        smb_cifs_mount(
            smb_nodes[0],
            client,
            smb_shares[0],
            smb_user_name,
            smb_user_password,
            auth_mode,
            domain_realm,
            cifs_mount_point,
        )
        create_files(client, cifs_mount_point, 1)
        remove_files(client, cifs_mount_point, 1)
    except Exception as e:
        log.error(f"SMB protocols subvolume verification failed: {e}")
        return 1
    finally:
        client.exec_command(sudo=True, cmd=f"rm -rf {cifs_mount_point}/*")
        client.exec_command(sudo=True, cmd=f"umount {cifs_mount_point} || true")
        client.exec_command(sudo=True, cmd=f"rm -rf {cifs_mount_point}")
        if smb_cluster_id:
            smb_cleanup(
                installer, smb_shares, smb_cluster_id, cephfs_vol, smb_subvol_group
            )
    return 0
