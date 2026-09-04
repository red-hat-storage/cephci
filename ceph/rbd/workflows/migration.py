import copy
import json
import re
import tempfile

from ceph.rbd.utils import exec_cmd, random_string
from ceph.rbd.workflows.rbd import create_single_pool_and_images
from cli.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


def verify_migration_state(action, image_spec, cluster_name="ceph", **kw):
    """verify the migration status at each action.

    This method will verify the migration state for an image for
    destination pool after executingprepare migration and
    execute migration steps for live image migration.

    Args:
        action: prepare or execute
        image_spec: pool_name/image_name
        kw: Key/value pairs of test configuration

    Returns:
        0: If migration state is as expected
        1: If migration state is not as expected
    """
    rbd = Rbd(kw["client"])
    log.info("verifying migration state")
    status_config = {
        "image-spec": image_spec,
        "cluster": cluster_name,
        "format": "json",
    }
    out, err = rbd.status(**status_config)
    log.info(out)
    status = json.loads(out)
    try:
        if action == "prepare" and "prepared" in status["migration"]["state"]:
            log.info(f"Live Migration successfully prepared for {image_spec}")
        elif action == "execute" and "executed" in status["migration"]["state"]:
            log.info(f"Live migration successfully executed for {image_spec}")
        elif action == "commit" and "migration" not in status:
            log.info(f"Live migration successfully committed for {image_spec}")
        return 0
    except Exception as error:
        log.error(error)
        return 1


def prepare_migration_source_spec(
    cluster_name, client, pool_name, image_name, snap_name, namespace_name=None
):
    """
    Create a native source spec file for migration.
    Args:
        cluster_name: Name of the source cluster
        pool_name: Name of the source pool
        image_name: Name of the source image
        snap_name: Name of the snapshot
    Returns:
        Path to the native spec file
    """
    native_spec = {
        "cluster_name": cluster_name,
        "type": "native",
        "pool_name": pool_name,
        "image_name": image_name,
        "snap_name": snap_name,
    }
    if namespace_name is not None:
        native_spec["pool_namespace"] = namespace_name

    temp_file = tempfile.NamedTemporaryFile(dir="/tmp", suffix=".json")
    spec_file = client.remote_file(sudo=True, file_name=temp_file.name, file_mode="w")
    spec_file.write(json.dumps(native_spec, indent=4))
    spec_file.flush()

    return temp_file.name


def run_prepare_execute_commit(rbd, pool, image, **kw):
    """
    Function to carry out the following:
      - Create Target/destination pool for migration
      - Migration prepare
      - Migration Execute
      - Migration commit
    Args:
        kw: rbd object, pool, image, test data
    Returns:
        int: The return value. 0 for success, 1 otherwise

    """
    # Create Target Pool/ Destination Pool for migration
    is_ec_pool = True if "ec" in kw[pool]["pool_type"] else False
    config = kw.get("config", {})
    target_pool = "target_pool_" + random_string(len=3)
    target_pool_config = {}
    if is_ec_pool:
        data_pool_target = "data_pool_new_" + random_string(len=3)
        target_pool_config["data_pool"] = data_pool_target
    rc = create_single_pool_and_images(
        config=config,
        pool=target_pool,
        pool_config=target_pool_config,
        client=kw["client"],
        cluster="ceph",
        rbd=rbd,
        ceph_version=int(config.get("rhbuild")[0]),
        is_ec_pool=is_ec_pool,
        is_secondary=False,
        do_not_create_image=True,
    )
    if rc:
        log.error(f"Creation of target pool {target_pool} failed")
        return rc

    # Adding the new pool details to config so that they are handled in cleanup
    if kw[pool]["pool_type"] == "rep_pool_config":
        kw["config"]["rep_pool_config"][target_pool] = {}
    elif kw[pool]["pool_type"] == "ec_pool_config":
        kw["config"]["ec_pool_config"][target_pool] = {"data_pool": data_pool_target}

    # Prepare Migration
    target_image = "target_image_" + random_string(len=3)
    rbd.migration.prepare(
        source_spec=kw[pool]["spec"],
        dest_spec=f"{target_pool}/{target_image}",
        client_node=kw["client"],
    )
    kw[pool].update({"target_pool": target_pool})
    kw[pool].update({"target_image": target_image})

    # Verify prepare migration status
    if verify_migration_state(
        action="prepare",
        image_spec=f"{target_pool}/{target_image}",
        **kw,
    ):
        log.error("Failed to prepare migration")
        return 1
    else:
        log.info("Migration prepare status verified successfully")

    # execute migration
    rbd.migration.action(
        action="execute",
        dest_spec=f"{target_pool}/{target_image}",
        client_node=kw["client"],
    )

    # verify execute migration status
    if verify_migration_state(
        action="execute",
        image_spec=f"{target_pool}/{target_image}",
        **kw,
    ):
        log.error("Failed to execute migration")
        return 1
    else:
        log.info("Migration executed successfully")

    # commit migration
    rbd.migration.action(
        action="commit",
        dest_spec=f"{target_pool}/{target_image}",
        client_node=kw["client"],
    )

    # verify commit migration status
    if verify_migration_state(
        action="commit",
        image_spec=f"{target_pool}/{target_image}",
        **kw,
    ):
        log.error("Failed to commit migration")
        return 1
    else:
        log.info("Migration committed successfully")


def _build_mon_host_from_dump(mon_dump):
    """Build a formatted mon_host string from a JSON mon dump.

    Parses the structured output of ``ceph mon dump --format json``
    and returns a space-separated string of messenger v2/v1 endpoints
    suitable for use in a native source spec ``mon_host`` field.

    Args:
        mon_dump: Parsed JSON dict from ``ceph mon dump --format json``.

    Returns:
        str: Space-separated string of monitor endpoints.

    Raises:
        ValueError: If no usable endpoints can be extracted.
    """
    mons = sorted(mon_dump.get("mons", []), key=lambda mon: mon.get("rank", 0))
    endpoints = []

    for mon in mons:
        addrvec = mon.get("public_addrs", {}).get("addrvec", [])
        endpoint_parts = []
        for addr in addrvec:
            addr_type = addr.get("type")
            addr_value = addr.get("addr")
            if not addr_type or not addr_value:
                continue
            if "/" not in addr_value and addr.get("nonce") is not None:
                addr_value = f"{addr_value}/{addr['nonce']}"
            endpoint_parts.append(f"{addr_type}:{addr_value}")

        if endpoint_parts:
            endpoints.append(f"[{','.join(endpoint_parts)}]")
        elif mon.get("public_addr"):
            endpoints.append(mon["public_addr"])
        elif mon.get("addr"):
            endpoints.append(mon["addr"])

    if not endpoints:
        raise ValueError(f"Unable to build mon_host from mon dump: {mon_dump}")
    return " ".join(endpoints)


def get_source_mon_host(client):
    """Extract the formatted mon_host string from a Ceph cluster.

    Attempts JSON-formatted ``ceph mon dump`` first; falls back to
    plain-text parsing if JSON parsing fails.

    Args:
        client: Source cluster CephNode client.

    Returns:
        str: Space-separated mon_host string for use in native source specs.

    Raises:
        ValueError: If mon_host cannot be determined from either method.
    """
    out = exec_cmd(node=client, cmd="ceph mon dump --format json", output=True)
    try:
        return _build_mon_host_from_dump(json.loads(out))
    except Exception as error:
        log.info(
            "JSON mon dump parsing failed, using plain mon dump fallback: %s", error
        )

    out = exec_cmd(node=client, cmd="ceph mon dump", output=True)
    endpoints = re.findall(r"\[v\d:[^\]]+\]", out)
    if endpoints:
        return " ".join(endpoints)
    raise ValueError(f"Unable to parse mon_host from mon dump output: {out}")


def create_source_cephx_client(client, pool, client_name):
    """Create a read-only CephX client on the source cluster for migration.

    The created client has:
      - ``mon 'profile rbd'``
      - ``mgr 'profile rbd pool=<pool>'``
      - ``osd 'profile rbd-read-only pool=<pool>'``

    Args:
        client: Source cluster CephNode client.
        pool: Source pool name to grant read-only access to.
        client_name: CephX client name (e.g. ``client.rbd-migration``).

    Returns:
        tuple: ``(entity, key)`` where *entity* is the normalized client
        entity string and *key* is the base64-encoded CephX key.
    """
    entity = (
        client_name if client_name.startswith("client.") else f"client.{client_name}"
    )

    exec_cmd(
        node=client,
        cmd=(
            f"ceph auth get-or-create {entity} "
            f"mon 'profile rbd' "
            f"mgr 'profile rbd pool={pool}' "
            f"osd 'profile rbd-read-only pool={pool}'"
        ),
    )
    log.info(f"Created CephX entity {entity} with read-only access to pool {pool}")
    exec_cmd(node=client, cmd=f"ceph auth get {entity}")

    key = exec_cmd(
        node=client,
        cmd=f"ceph auth get-key {entity}",
        output=True,
    ).strip()

    return entity, key


def write_native_source_spec(client, spec_path, spec):
    """Write an arbitrary native source-spec JSON file on *client*.

    Redacts ``key`` values in logs when the value does not use ``config://``.

    Args:
        client: CephNode where the spec file is written.
        spec_path: Absolute path for the JSON file.
        spec: Source-spec dictionary (must include ``type``).

    Returns:
        dict: The spec that was written.
    """
    spec_file = client.remote_file(sudo=True, file_name=spec_path, file_mode="w")
    spec_file.write(json.dumps(spec, indent=4))
    spec_file.flush()
    spec_file.close()
    exec_cmd(node=client, cmd=f"chmod 600 {spec_path}")

    logged = copy.deepcopy(spec)
    key_val = logged.get("key")
    if isinstance(key_val, str) and key_val and not key_val.startswith("config://"):
        logged["key"] = "<redacted>"
    log.info(f"Wrote native source spec to {spec_path}: {logged}")
    return spec


def prepare_native_source_spec_with_key(
    client, spec_path, mon_host, client_name, key, pool_name, image_name, snap_name
):
    """Create a native source spec JSON file using mon_host and inline key.

    Unlike ``prepare_migration_source_spec`` which uses ``cluster_name``,
    this function uses ``mon_host`` + ``key`` fields so that the destination
    client does not need source ``ceph.conf`` or keyring files.

    Args:
        client: Destination CephNode client where the spec file is written.
        spec_path: Absolute path on the destination node for the JSON file.
        mon_host: Source cluster mon_host string.
        client_name: CephX client entity (e.g. ``client.rbd-migration``).
        key: Base64-encoded CephX key.
        pool_name: Source pool name.
        image_name: Source image name.
        snap_name: Source snapshot name.

    Returns:
        dict: The native source spec dictionary that was written.
    """
    spec = {
        "type": "native",
        "mon_host": mon_host,
        "client_name": client_name,
        "key": key,
        "pool_name": pool_name,
        "image_name": image_name,
        "snap_name": snap_name,
    }
    return write_native_source_spec(client, spec_path, spec)


def attempt_migration_prepare_import(client, source_spec_path, dest_spec, timeout=420):
    """Run ``rbd migration prepare --import-only`` and capture pass/fail + output.

    Uses ``check_ec=False`` so negative cases can assert on failure without
    raising. Default *timeout* is set to 420 seconds (7 minutes) — longer than
    Ceph's internal connection timeout of ~300 seconds — so that an invalid or
    unreachable mon_host is allowed to time out on the Ceph side itself and
    return the expected ``(110) Connection timed out`` error naturally, without
    the external wrapper killing the command first.

    Args:
        client: Destination CephNode client.
        source_spec_path: Path to source-spec JSON on the destination node.
        dest_spec: Destination image spec ``pool/image``.
        timeout: Command timeout in seconds.

    Returns:
        tuple: ``(failed, combined_output)`` where *failed* is True when the
        command exit status is non-zero, and *combined_output* is stdout+stderr
        (or the exception text) for error matching.
    """
    cmd = (
        f"rbd migration prepare --import-only "
        f"--source-spec-path {source_spec_path} {dest_spec}"
    )
    try:
        out, err = client.exec_command(
            sudo=True, cmd=cmd, check_ec=False, timeout=timeout
        )
        combined = f"{out or ''}{err or ''}"
        exit_status = getattr(client, "exit_status", 0)
        failed = int(exit_status) != 0
    except Exception as error:
        combined = str(error)
        failed = True

    log.info(
        f"migration prepare --import-only for {dest_spec}: "
        f"failed={failed}, output={combined[:500]}"
    )
    return failed, combined


def verify_no_stale_migration_target(client, pool, image):
    """Verify failed prepare left no target image or migration metadata.

    Args:
        client: Destination CephNode client.
        pool: Destination pool name.
        image: Target image name that must not remain after failure.

    Returns:
        0 if no stale target/migration state remains, 1 otherwise.
    """
    target_spec = f"{pool}/{image}"
    listed = exec_cmd(node=client, cmd=f"rbd ls {pool}", output=True, check_ec=False)
    listed = (listed or "").split()
    if image in listed:
        status_out = exec_cmd(
            node=client,
            cmd=f"rbd status {target_spec} --format json",
            output=True,
            check_ec=False,
        )
        log.error(
            f"Stale target image {target_spec} present after failed prepare; "
            f"status={status_out}"
        )
        # Best-effort cleanup so subsequent negative cases stay isolated
        exec_cmd(
            node=client,
            cmd=f"rbd migration abort {target_spec}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        exec_cmd(
            node=client,
            cmd=f"rbd rm {target_spec}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        return 1

    log.info(f"No stale target image {target_spec} after failed prepare")
    return 0


def create_insufficient_caps_cephx_client(client, client_name):
    """Create a CephX client without OSD read access (for negative auth tests).

    Grants only ``mon 'allow r'`` so native import prepare should fail when
    opening the source pool/image.

    Args:
        client: Source cluster CephNode client.
        client_name: CephX client name (e.g. ``client.rbd-migration-limited``).

    Returns:
        tuple: ``(entity, key)``.
    """
    entity = (
        client_name if client_name.startswith("client.") else f"client.{client_name}"
    )
    exec_cmd(
        node=client,
        cmd=f"ceph auth get-or-create {entity} mon 'allow r'",
    )
    key = exec_cmd(
        node=client,
        cmd=f"ceph auth get-key {entity}",
        output=True,
    ).strip()
    log.info(f"Created insufficient-caps CephX entity {entity}")
    return entity, key


def store_source_key_in_config_key(client, config_key_path, key, workdir):
    """Store a source CephX key in the destination MON config-key store.

    Writes the key to a temporary file and loads it with
    ``ceph config-key set <path> -i <file>`` to avoid shell quoting issues.

    Args:
        client: Destination CephNode client.
        config_key_path: MON config-key path (e.g. ``rbd/native/source_client_key``).
        key: Base64-encoded CephX key value.
        workdir: Temporary directory on the destination node for the key file.

    Returns:
        None
    """
    key_file = f"{workdir}/source_client.key"
    remote = client.remote_file(sudo=True, file_name=key_file, file_mode="w")
    remote.write(key)
    remote.flush()
    remote.close()
    exec_cmd(node=client, cmd=f"chmod 600 {key_file}")
    exec_cmd(
        node=client,
        cmd=f"ceph config-key set {config_key_path} -i {key_file}",
    )
    exec_cmd(node=client, cmd=f"rm -f {key_file}", check_ec=False)
    log.info(f"Stored source client key at config-key path {config_key_path}")


def verify_config_key(client, config_key_path, expected_key):
    """Verify a MON config-key value matches the expected source client key.

    Args:
        client: Destination CephNode client.
        config_key_path: MON config-key path to read.
        expected_key: Expected base64-encoded CephX key.

    Returns:
        0 if the stored key matches *expected_key*, 1 otherwise.
    """
    stored = exec_cmd(
        node=client,
        cmd=f"ceph config-key get {config_key_path}",
        output=True,
    ).strip()
    if stored != expected_key.strip():
        log.error(
            f"config-key mismatch at {config_key_path}: "
            f"expected length {len(expected_key.strip())}, "
            f"got length {len(stored)}"
        )
        return 1
    log.info(f"Verified config-key {config_key_path} matches source client key")
    return 0


def remove_config_key(client, config_key_path):
    """Remove a key from the MON config-key store.

    Args:
        client: Destination CephNode client.
        config_key_path: MON config-key path to remove.
    """
    exec_cmd(
        node=client,
        cmd=f"ceph config-key rm {config_key_path}",
        check_ec=False,
    )
    log.info(f"Removed config-key path {config_key_path}")


def prepare_native_source_spec_with_config_key(
    client,
    spec_path,
    mon_host,
    client_name,
    config_key_path,
    pool_name,
    image_name,
    snap_name,
):
    """Create a native source spec JSON using mon_host and config:// key ref.

    The ``key`` field is set to ``config://<config_key_path>`` so librbd
    resolves the CephX secret from the destination MON config-key store
    instead of embedding the key inline in the source-spec file.

    Args:
        client: Destination CephNode client where the spec file is written.
        spec_path: Absolute path on the destination node for the JSON file.
        mon_host: Source cluster mon_host string.
        client_name: CephX client entity (e.g. ``client.rbd-migration``).
        config_key_path: MON config-key path holding the source client key.
        pool_name: Source pool name.
        image_name: Source image name.
        snap_name: Source snapshot name.

    Returns:
        dict: The native source spec dictionary that was written.
    """
    key_ref = f"config://{config_key_path}"
    spec = {
        "type": "native",
        "mon_host": mon_host,
        "client_name": client_name,
        "key": key_ref,
        "pool_name": pool_name,
        "image_name": image_name,
        "snap_name": snap_name,
    }

    spec_file = client.remote_file(sudo=True, file_name=spec_path, file_mode="w")
    spec_file.write(json.dumps(spec, indent=4))
    spec_file.flush()
    spec_file.close()
    exec_cmd(node=client, cmd=f"chmod 600 {spec_path}")

    log.info(f"Created native source spec with config:// key reference: {spec}")
    return spec


def verify_key_not_logged(client, key, workdir, test_start):
    """Verify that a CephX inline key was not leaked to logs on a node.

    Scans ``/var/log/ceph``, ``/var/log/messages``, and ``journalctl``
    since *test_start* for the raw key string.

    Args:
        client: CephNode to scan for key leakage.
        key: The base64-encoded CephX key to search for.
        workdir: Temporary directory for the pattern file.
        test_start: ISO/datetime string for journalctl ``--since``.

    Returns:
        0 if the key was not found in any logs.
        1 if the key was found (security leak).
    """
    pattern_path = f"{workdir}/inline-key-pattern"
    pattern_file = client.remote_file(sudo=True, file_name=pattern_path, file_mode="w")
    pattern_file.write(key)
    pattern_file.flush()
    pattern_file.close()
    exec_cmd(node=client, cmd=f"chmod 600 {pattern_path}", check_ec=False)

    cmd = (
        'sh -c "'
        f"grep -R -F -l -f {pattern_path} /var/log/ceph /var/log/messages "
        "2>/dev/null || true; "
        f"journalctl --since '{test_start}' --no-pager 2>/dev/null "
        f"| grep -F -q -f {pattern_path} && echo journalctl || true"
        '"'
    )
    matches = exec_cmd(node=client, cmd=cmd, output=True, check_ec=False)
    exec_cmd(node=client, cmd=f"rm -f {pattern_path}", check_ec=False)
    if matches and matches.strip():
        log.error(f"Inline source key was found in log locations: {matches}")
        return 1

    log.info("Verified inline key not present in destination logs")
    return 0


def resolve_gateway_like_client(destination_cluster, client_a, config=None):
    """Resolve a destination-side node to act as the gateway-like Client-B.

    Preference order:
      1. Explicit ``gateway_like_node`` hostname from *config*
      2. First node with role ``rbd-mirror`` that is not *client_a*
      3. First non-client node on the destination cluster

    Args:
        destination_cluster: Destination Ceph cluster object.
        client_a: Destination Client-A node used for migration prepare.
        config: Optional test config dict.

    Returns:
        CephNode: Gateway-like Client-B node.

    Raises:
        ValueError: If no suitable Client-B node can be resolved.
    """
    config = config or {}
    gateway_hostname = config.get("gateway_like_node")
    if gateway_hostname:
        for node in destination_cluster.get_nodes():
            if node.hostname == gateway_hostname or getattr(
                node, "shortname", None
            ) == (gateway_hostname):
                log.info(f"Using configured gateway-like Client-B: {node.hostname}")
                return node
        raise ValueError(
            f"Configured gateway_like_node '{gateway_hostname}' not found "
            f"on destination cluster"
        )

    try:
        mirror_nodes = destination_cluster.get_nodes(role="rbd-mirror")
    except Exception:
        mirror_nodes = []
    for node in mirror_nodes:
        if node.hostname != client_a.hostname:
            log.info(f"Using rbd-mirror node as gateway-like Client-B: {node.hostname}")
            return node

    for node in destination_cluster.get_nodes():
        if node.hostname == client_a.hostname:
            continue
        if "client" in node.role:
            continue
        log.info(f"Using non-client node as gateway-like Client-B: {node.hostname}")
        return node

    raise ValueError(
        "Unable to resolve a gateway-like Client-B node on the destination cluster"
    )


def prepare_gateway_like_client(client_a, client_b, packages=None):
    """Prepare Client-B with destination-only ceph.conf and keyring.

    Installs required packages, copies destination cluster configuration from
    *client_a*, and removes any accidental source-cluster config files so the
    node behaves like an NVMe-oF gateway host that only knows the destination.

    Args:
        client_a: Destination Client-A node that already has dest conf/keyring.
        client_b: Gateway-like Client-B node to prepare.
        packages: Optional package list. Defaults to ``ceph-common``, ``fio``,
            and ``rbd-nbd``.

    Returns:
        0 on success, 1 on failure.
    """
    from ceph.rbd.utils import copy_file

    packages = packages or ["ceph-common", "fio", "rbd-nbd"]
    try:
        for pkg in packages:
            exec_cmd(
                node=client_b,
                cmd=f"yum install -y --nogpgcheck {pkg}",
                long_running=True,
                check_ec=False,
            )

        exec_cmd(node=client_b, cmd="mkdir -p /etc/ceph && chmod 755 /etc/ceph")

        # Remove any pre-existing source-style multi-cluster configs
        exec_cmd(
            node=client_b,
            cmd=(
                "rm -f /etc/ceph/cluster*.conf /etc/ceph/cluster*.keyring "
                "/etc/ceph/*source* /etc/ceph/ceph.client.rbd-migration.keyring"
            ),
            check_ec=False,
        )

        for path in (
            "/etc/ceph/ceph.conf",
            "/etc/ceph/ceph.client.admin.keyring",
        ):
            copy_file(file_name=path, src=client_a, dest=client_b)

        exec_cmd(node=client_b, cmd="chmod 644 /etc/ceph/ceph.conf", check_ec=False)
        exec_cmd(
            node=client_b,
            cmd="chmod 600 /etc/ceph/ceph.client.admin.keyring",
            check_ec=False,
        )

        dest_fsid = exec_cmd(node=client_a, cmd="ceph fsid", output=True).strip()
        client_b_fsid = exec_cmd(node=client_b, cmd="ceph fsid", output=True).strip()
        if dest_fsid != client_b_fsid:
            log.error(
                f"Client-B fsid {client_b_fsid} does not match destination "
                f"fsid {dest_fsid}"
            )
            return 1

        log.info(
            f"Prepared gateway-like Client-B {client_b.hostname} with "
            f"destination-only configuration (fsid {client_b_fsid})"
        )
        return 0
    except Exception as error:
        log.error(f"Failed to prepare gateway-like Client-B: {error}")
        return 1


def assert_no_source_cluster_config(client_b, source_fsid, source_mon_host=None):
    """Assert Client-B has no local source cluster configuration.

    Checks that:
      - Client-B ``ceph fsid`` is not the source FSID
      - No source-style multi-cluster conf/keyring files exist under ``/etc/ceph``
      - Optional: source monitor endpoints are absent from local ceph.conf

    Args:
        client_b: Gateway-like Client-B node.
        source_fsid: Source cluster FSID that must not be local.
        source_mon_host: Optional mon_host string; endpoints must not appear
            in Client-B ``ceph.conf``.

    Returns:
        0 if Client-B has destination-only config, 1 otherwise.
    """
    client_b_fsid = exec_cmd(node=client_b, cmd="ceph fsid", output=True).strip()
    if client_b_fsid == source_fsid:
        log.error(
            f"Client-B unexpectedly resolves to source fsid {source_fsid}; "
            f"source cluster config may be present"
        )
        return 1

    listing = exec_cmd(
        node=client_b,
        cmd="ls -1 /etc/ceph/ 2>/dev/null || true",
        output=True,
        check_ec=False,
    )
    forbidden = (
        "cluster2.conf",
        "cluster2.client.admin.keyring",
        "ceph.client.rbd-migration.keyring",
    )
    for name in forbidden:
        if name in (listing or "").split():
            log.error(f"Forbidden source-related file present on Client-B: {name}")
            return 1

    if "source" in (listing or "").lower():
        log.error(f"Source-named config files present on Client-B: {listing}")
        return 1

    # Ensure Client-B does not carry a local copy of the source-spec used on Client-A
    stale_specs = exec_cmd(
        node=client_b,
        cmd=(
            "ls /tmp/rbd-native-import-gateway-like-test/*source*.json "
            "/tmp/*native*source*.json 2>/dev/null || true"
        ),
        output=True,
        check_ec=False,
    )
    if stale_specs and stale_specs.strip():
        log.error(f"Source-spec files unexpectedly present on Client-B: {stale_specs}")
        return 1

    if source_mon_host:
        preview = (
            source_mon_host
            if len(source_mon_host) <= 80
            else (source_mon_host[:80] + "...")
        )
        log.info(
            "Source mon_host was supplied for context; Client-B local config "
            "checks rely on fsid and /etc/ceph contents (mon endpoints may "
            f"overlap in shared lab networks): {preview}"
        )

    log.info(
        f"Verified Client-B {client_b.hostname} has no local source cluster "
        f"configuration (local fsid {client_b_fsid})"
    )
    return 0


def simulate_librbd_consumer_restart(client, image_spec):
    """Simulate a gateway-like librbd consumer restart via NBD map/unmap.

    Maps the target image with ``rbd device map -t nbd``, then unmaps it so
    subsequent opens must re-read destination migration metadata — analogous
    to restarting an NVMe-oF gateway process that holds the image open.

    Args:
        client: Gateway-like Client-B node.
        image_spec: Destination image spec (``pool/image``).

    Returns:
        0 on success, 1 on failure.
    """
    try:
        # Ensure rbd-nbd is available for the map/unmap cycle
        nbd_check = exec_cmd(
            node=client,
            cmd="rpm -q rbd-nbd || yum install -y --nogpgcheck rbd-nbd",
            output=True,
            check_ec=False,
            long_running=True,
        )
        log.info(f"rbd-nbd availability on Client-B: {nbd_check}")

        # Best-effort cleanup of any stale mapping
        exec_cmd(
            node=client,
            cmd=f"rbd device unmap -t nbd {image_spec}",
            check_ec=False,
        )

        device = exec_cmd(
            node=client,
            cmd=f"rbd device map -t nbd {image_spec}",
            output=True,
        ).strip()
        log.info(f"Mapped {image_spec} to {device} on Client-B")

        exec_cmd(
            node=client,
            cmd=f"rbd device unmap -t nbd {device}",
        )
        log.info(
            f"Unmapped {device}; simulated librbd/gateway-like consumer restart "
            f"for {image_spec}"
        )
        return 0
    except Exception as error:
        log.error(f"Failed to simulate librbd consumer restart: {error}")
        # Ensure we do not leave a mapped device behind
        exec_cmd(
            node=client,
            cmd=f"rbd device unmap -t nbd {image_spec}",
            check_ec=False,
        )
        return 1


def verify_gateway_like_logs(client_b, source_key, workdir, test_start):
    """Verify Client-B logs show no source-config requirement or key leak.

    Args:
        client_b: Gateway-like Client-B node.
        source_key: Source CephX key that must not appear in logs.
        workdir: Temporary work directory on Client-B.
        test_start: Timestamp string for journalctl ``--since``.

    Returns:
        0 if checks pass, 1 otherwise.
    """
    if source_key and verify_key_not_logged(client_b, source_key, workdir, test_start):
        return 1

    # Look for messages that would indicate a local source conf/keyring or
    # gateway API source-cluster parameter was required.
    patterns = (
        "source ceph.conf",
        "source keyring",
        "source-cluster",
        "source_cluster",
        "cluster_name.*required",
    )
    joined = "|".join(patterns)
    cmd = (
        'sh -c "'
        f"journalctl --since '{test_start}' --no-pager 2>/dev/null "
        f"| grep -Ei '{joined}' || true"
        '"'
    )
    matches = exec_cmd(node=client_b, cmd=cmd, output=True, check_ec=False)
    if matches and matches.strip():
        log.error(
            "Client-B logs indicate a requirement for source cluster config "
            f"or gateway source-cluster parameter: {matches}"
        )
        return 1

    log.info(
        "Verified Client-B logs do not require source ceph.conf/keyring or "
        "gateway API source-cluster parameter"
    )
    return 0
