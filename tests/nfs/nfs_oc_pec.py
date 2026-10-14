"""NFS Ganesha Object Cache (OC), Per-Export Client (PEC), and pool helpers.

OC is an NFS orch spec (enable_client_object_cache / size / dirty) that
renders a CEPH {} block in ganesha.conf. PEC is --cmount_path on export
create (N CephFS clients / asoks / user_ids). Pool of libcephfs clients is
``--clients_per_pool K`` at cluster create (K asoks, cmount_path=/, CEPH_USERS).

OC/PEC require live cluster Ceph >= 20.2.2. Pool soft-skips when create -h
lacks the flag. Do not use --rhbuild.
"""

import json
import re
from contextlib import contextmanager
from time import sleep

from looseversion import LooseVersion

from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from tests.nfs.nfs_operations import (
    _nfs_daemon_not_ready,
    create_nfs_via_file_and_verify,
    get_ceph_version,
    wait_for_nfs_ganesha_daemons,
)
from utility.log import Log

log = Log(__name__)

OC_PEC_MIN_CEPH = "20.2.2"
_ASOK_PATH = "/run/ceph/$name.$pid.asok"
_SIZE_RE = re.compile(r"^(\d+)\s*(GiB|MiB|KiB|GB|MB|KB|B)?$", re.I)
_CLIENT_OC_RE = re.compile(r"client_oc\s*=\s*(true|false)", re.I)
_CLIENT_OC_SIZE_RE = re.compile(r"client_oc_size\s*=\s*(\d+)", re.I)
_CLIENT_OC_DIRTY_RE = re.compile(r"client_oc_max_dirty\s*=\s*(\d+)", re.I)
_CLIENTS_PER_POOL_CONF_RE = re.compile(r"clients_per_pool\s*=\s*(\d+)", re.I)
_USERS_BLOCK_RE = re.compile(r"\bUSERS\s*\{", re.I)
_USERIDS_RE = re.compile(r"Userids\s*=\s*([^;]+);", re.I | re.DOTALL)
_KEYS_RE = re.compile(r"Keys\s*=\s*([^;]+);", re.I | re.DOTALL)
_FILESYSTEM_RE = re.compile(r'filesystem\s*=\s*"?([^";\s]+)"?\s*;', re.I)


def oc_pec_supported(node, prefix_cephadm=False):
    """True when live Ceph on *node* is >= 20.2.2."""
    try:
        installed = get_ceph_version(node, prefix_cephadm=prefix_cephadm)
    except Exception as err:
        log.warning("Could not read Ceph version: %s", err)
        return False
    if not installed:
        log.warning("Unparseable Ceph version %r; OC/PEC not enabled", installed)
        return False
    supported = LooseVersion(installed) >= LooseVersion(OC_PEC_MIN_CEPH)
    log.info(
        "OC/PEC version gate: installed=%s min=%s supported=%s",
        installed,
        OC_PEC_MIN_CEPH,
        supported,
    )
    return supported


def skip_oc_pec_unless_supported(ceph_cluster):
    """Return True when standalone OC/PEC tests should skip (caller return 0)."""
    nodes = []
    installers = ceph_cluster.get_nodes(role="installer") or []
    clients = ceph_cluster.get_nodes("client") or []
    if installers:
        nodes.append((installers[0], True))
    if clients:
        nodes.append((clients[0], False))
    for node, prefix in nodes:
        if oc_pec_supported(node, prefix_cephadm=prefix):
            return False
    log.info(
        "Skipping OC/PEC test: live Ceph < %s (or version unknown)",
        OC_PEC_MIN_CEPH,
    )
    return True


@contextmanager
def ensure_nfs_admin_socket(node):
    """Point client.nfs admin sockets at /run/ceph for PEC asok checks; restore on exit.

    Without this, CephFS client asoks are not under /run/ceph inside the Ganesha
    container. Restores the prior value (or removes the key) so later NFS tests
    on the same cluster do not inherit the side effect.
    """
    prev = None
    try:
        out, _ = node.exec_command(
            sudo=True,
            cmd="ceph config get client.nfs admin_socket",
            check_ec=False,
        )
        prev = (out or "").strip() or None
        # Failed get often echoes an error string, not a path.
        if prev and ("Error" in prev or "ENOENT" in prev or "\n" in prev):
            prev = None
    except Exception:
        prev = None

    # Single-quote value so remote shell does not expand $name/$pid.
    Ceph(node).config.set(
        daemon="client.nfs",
        key="admin_socket",
        value=f"'{_ASOK_PATH}'",
    )
    log.info("Set client.nfs admin_socket=%s", _ASOK_PATH)
    try:
        yield
    finally:
        try:
            if prev is not None:
                restored = prev.strip("'")
                Ceph(node).config.set(
                    daemon="client.nfs",
                    key="admin_socket",
                    value=f"'{restored}'",
                )
                log.info("Restored client.nfs admin_socket=%s", restored)
            else:
                Ceph(node).config.rm("client.nfs", "admin_socket")
                log.info("Removed client.nfs admin_socket (was unset)")
        except Exception as err:
            log.warning("Failed to restore client.nfs admin_socket: %s", err)


def size_to_bytes(value):
    """Convert spec sizes like 200MiB / 2GiB to bytes."""
    match = _SIZE_RE.match(str(value).strip())
    if not match:
        raise OperationFailedError(f"Cannot parse size {value!r}")
    amount = int(match.group(1))
    unit = (match.group(2) or "B").lower()
    multipliers = {
        "b": 1,
        "kb": 1000,
        "kib": 1024,
        "mb": 1000 * 1000,
        "mib": 1024 * 1024,
        "gb": 1000 * 1000 * 1000,
        "gib": 1024 * 1024 * 1024,
    }
    return amount * multipliers[unit]


def _node_by_hostname(ceph_cluster, hostname):
    """Resolve orch hostname to a CephNode (FQDN or short name)."""
    node = ceph_cluster.get_node_by_hostname(hostname)
    if node:
        return node
    short = hostname.split(".")[0]
    for candidate in ceph_cluster.get_nodes():
        if candidate.hostname.split(".")[0] == short:
            return candidate
    raise OperationFailedError(f"No cluster node matching NFS host {hostname}")


def _nfs_daemon(node, cluster_id, timeout=180, interval=5):
    """Return (container_id, hostname) for a running nfs.<cluster_id> daemon.

    Polls until the daemon is running with a container_id. Uses
    Ceph().orch.ps (host ceph CLI) because verify_* runs on the client;
    CephAdm shell is installer-only.
    """
    last = []
    for _ in WaitUntil(timeout=timeout, interval=interval):
        raw = Ceph(node).orch.ps(service_name=f"nfs.{cluster_id}", format="json")
        last = json.loads(raw) if raw else []
        running = [d for d in last if not _nfs_daemon_not_ready(d)]
        if running:
            daemon = running[0]
            return daemon["container_id"], daemon["hostname"]
        log.info(
            "Waiting for nfs.%s daemon to be running (ps status=%s)",
            cluster_id,
            [d.get("status_desc") for d in last],
        )
    raise OperationFailedError(
        f"No running nfs.{cluster_id} daemon after {timeout}s (ps={last!r})"
    )


def read_ganesha_conf(ceph_cluster, node, cluster_id):
    """Return ganesha.conf from the NFS container."""
    container_id, hostname = _nfs_daemon(node, cluster_id)
    host = _node_by_hostname(ceph_cluster, hostname)
    out, _ = host.exec_command(
        sudo=True,
        cmd=f"podman exec {container_id} cat /etc/ganesha/ganesha.conf",
        timeout=60,
    )
    return str(out)


def list_cephfs_asoks(ceph_cluster, node, cluster_id):
    """Return CephFS asok filenames inside the NFS container."""
    container_id, hostname = _nfs_daemon(node, cluster_id)
    host = _node_by_hostname(ceph_cluster, hostname)
    out, _ = host.exec_command(
        sudo=True,
        cmd=(
            f"podman exec {container_id} bash -c "
            "'ls /run/ceph/ 2>/dev/null | grep cephfs | grep asok || true'"
        ),
        timeout=60,
    )
    return [line.strip() for line in str(out).splitlines() if line.strip()]


def deploy_nfs_with_object_cache(
    installer,
    cluster_id,
    hosts,
    port,
    size="200MiB",
    max_dirty="100MiB",
    timeout=300,
    nfs_nodes=None,
):
    """Create nfs.<cluster_id> from an orch spec that already has OC on.

    ``ceph nfs cluster create`` has no OC flags yet
    (https://ibm-ceph.atlassian.net/browse/IBMCEPH-18328). Until that
    lands, first-boot OC is orch apply with enable_client_object_cache.
    """
    host_list = hosts if isinstance(hosts, list) else [hosts]
    spec = {
        "service_type": "nfs",
        "service_id": cluster_id,
        "placement": {"count": 1, "hosts": host_list},
        "spec": {
            "port": int(port),
            "enable_client_object_cache": True,
            "client_object_cache_size": size,
            "client_object_cache_max_dirty": max_dirty,
        },
    }
    log.info(
        "Deploying nfs.%s with OC on port=%s hosts=%s size=%s max_dirty=%s",
        cluster_id,
        port,
        host_list,
        size,
        max_dirty,
    )
    if not create_nfs_via_file_and_verify(
        installer,
        [spec],
        timeout=timeout,
        nfs_nodes=nfs_nodes,
        nfs_name=cluster_id,
    ):
        raise OperationFailedError(
            f"orch apply failed deploying nfs.{cluster_id} with OC"
        )


def apply_nfs_object_cache(
    installer,
    cluster_id,
    enable=True,
    size="200MiB",
    max_dirty="100MiB",
    timeout=300,
):
    """Export current NFS orch spec, merge OC fields, and apply."""
    raw = CephAdm(installer).ceph.orch.ls(
        service_name=f"nfs.{cluster_id}", export=True, format="json"
    )
    specs = json.loads(raw) if raw else []
    if not specs:
        raise OperationFailedError(f"No orch spec for nfs.{cluster_id}")
    spec = specs[0]
    spec.setdefault("spec", {})
    spec["spec"]["enable_client_object_cache"] = bool(enable)
    if enable:
        spec["spec"]["client_object_cache_size"] = size
        spec["spec"]["client_object_cache_max_dirty"] = max_dirty
    spec.pop("status", None)
    spec.pop("events", None)
    log.info(
        "Applying OC spec nfs.%s enable=%s size=%s max_dirty=%s",
        cluster_id,
        enable,
        size,
        max_dirty,
    )
    if not create_nfs_via_file_and_verify(
        installer, [spec], timeout=timeout, nfs_name=cluster_id
    ):
        raise OperationFailedError(f"orch apply failed for nfs.{cluster_id} OC spec")
    # orch apply can report the old container still "running", then redeploy.
    # Settle briefly and wait again so verify_* does not race "starting".
    sleep(15)
    wait_for_nfs_ganesha_daemons(installer, timeout=timeout, nfs_name=cluster_id)
    return True


def verify_nfs_object_cache(
    ceph_cluster,
    node,
    cluster_id,
    expect_on,
    size=None,
    max_dirty=None,
):
    """Assert ganesha.conf CEPH {} matches OC on/off. Product assert is conf."""
    conf = read_ganesha_conf(ceph_cluster, node, cluster_id)
    log.info("ganesha.conf for nfs.%s:\n%s", cluster_id, conf)
    has_ceph_block = bool(re.search(r"\bCEPH\s*\{", conf, re.I))
    oc_match = _CLIENT_OC_RE.search(conf)
    oc_true = oc_match.group(1).lower() == "true" if oc_match else False

    if expect_on:
        if not has_ceph_block or not oc_true:
            raise OperationFailedError(
                f"nfs.{cluster_id} expected CEPH {{ client_oc=true }} in ganesha.conf"
            )
        if size is not None:
            size_match = _CLIENT_OC_SIZE_RE.search(conf)
            expected = size_to_bytes(size)
            if not size_match or int(size_match.group(1)) != expected:
                raise OperationFailedError(
                    f"nfs.{cluster_id} client_oc_size expected {expected} "
                    f"(from {size}), conf={size_match.group(0) if size_match else None}"
                )
        if max_dirty is not None:
            dirty_match = _CLIENT_OC_DIRTY_RE.search(conf)
            expected = size_to_bytes(max_dirty)
            if not dirty_match or int(dirty_match.group(1)) != expected:
                raise OperationFailedError(
                    f"nfs.{cluster_id} client_oc_max_dirty expected {expected} "
                    f"(from {max_dirty}), "
                    f"conf={dirty_match.group(0) if dirty_match else None}"
                )
    else:
        if has_ceph_block and oc_true:
            raise OperationFailedError(
                f"nfs.{cluster_id} expected OC off in ganesha.conf (no CEPH "
                f"block or client_oc=false)"
            )
    log.info("OC ganesha.conf verify nfs.%s expect_on=%s OK", cluster_id, expect_on)


def _export_details(node, cluster_id):
    out = Ceph(node).nfs.export.ls(cluster_id, detailed=True, format="json")
    data = json.loads(out) if out and str(out).strip() else []
    if isinstance(data, dict):
        data = [data]
    return data


def verify_pec(ceph_cluster, node, cluster_id, expect_clients, asok_timeout=120):
    """Assert N distinct CephFS clients (user_id + cmount_path + asoks).

    Asoks appear after Ganesha opens CephFS clients (typically after NFS mount).
    Callers should mount exports before this check; asok listing is retried.
    """
    exports = _export_details(node, cluster_id)
    user_ids = []
    for item in exports:
        fsal = item.get("fsal") or {}
        user_id = fsal.get("user_id")
        cmount = fsal.get("cmount_path") or item.get("cmount_path")
        if not user_id:
            raise OperationFailedError(
                f"nfs.{cluster_id} export {item.get('pseudo')} missing fsal.user_id"
            )
        if not cmount or cmount == "/":
            raise OperationFailedError(
                f"nfs.{cluster_id} export {item.get('pseudo')} cmount_path={cmount!r} "
                "(PEC requires subvol path, not /)"
            )
        user_ids.append(user_id)
    distinct = set(user_ids)
    if len(distinct) != expect_clients:
        raise OperationFailedError(
            f"nfs.{cluster_id} expected {expect_clients} distinct user_id, "
            f"got {len(distinct)}: {sorted(distinct)}"
        )
    asoks = []
    for _ in WaitUntil(timeout=asok_timeout, interval=5):
        asoks = list_cephfs_asoks(ceph_cluster, node, cluster_id)
        if len(asoks) >= expect_clients:
            break
        log.info(
            "Waiting for nfs.%s CephFS asoks: have %s need %s (%s)",
            cluster_id,
            len(asoks),
            expect_clients,
            asoks,
        )
    else:
        raise OperationFailedError(
            f"nfs.{cluster_id} expected at least {expect_clients} CephFS asoks, "
            f"got {len(asoks)}: {asoks}"
        )
    log.info(
        "PEC verify nfs.%s clients=%s user_ids=%s asoks=%s OK",
        cluster_id,
        expect_clients,
        sorted(distinct),
        asoks,
    )


def pool_clients_supported(node):
    """True when ``ceph nfs cluster create -h`` lists --clients_per_pool.

    Help text may wrap (``--clients_\\n per_pool``); compare whitespace-stripped.
    """
    out, err = node.exec_command(
        sudo=True,
        cmd="ceph nfs cluster create -h",
        check_ec=False,
    )
    help_text = f"{out or ''}{err or ''}"
    compact = re.sub(r"\s+", "", help_text)
    supported = "--clients_per_pool" in compact or "--clients-per-pool" in compact
    log.info("Pool clients_per_pool flag present=%s", supported)
    return supported


def skip_pool_unless_supported(ceph_cluster):
    """Return True when pool tests should soft-skip (caller return 0)."""
    clients = ceph_cluster.get_nodes("client") or []
    installers = ceph_cluster.get_nodes(role="installer") or []
    for node in clients[:1] + installers[:1]:
        try:
            if pool_clients_supported(node):
                return False
        except Exception as err:
            log.warning("pool flag probe failed on %s: %s", node.hostname, err)
    log.info("Skipping pool test: --clients_per_pool not in create -h")
    return True


def create_nfs_with_clients_per_pool(
    client, nfs_name, nfs_server, port, clients_per_pool
):
    """Create NFS cluster with --clients_per_pool via CLI kwargs passthrough."""
    kwargs = {"clients_per_pool": int(clients_per_pool)}
    if port and str(port) != "2049":
        kwargs["port"] = port
    log.info(
        "Creating nfs.%s clients_per_pool=%s port=%s host=%s",
        nfs_name,
        clients_per_pool,
        port,
        nfs_server,
    )
    return Ceph(client).nfs.cluster.create(
        name=nfs_name, nfs_server=nfs_server, **kwargs
    )


def ceph_users_object_name(cluster_id):
    return f"ceph-users-nfs.{cluster_id}"


def read_ceph_users(node, cluster_id, check_ec=False):
    """Return RADOS CEPH_USERS object text (may be empty)."""
    obj = ceph_users_object_name(cluster_id)
    cmd = f"rados -N {cluster_id} -p .nfs get {obj} -"
    out, err = node.exec_command(sudo=True, cmd=cmd, check_ec=check_ec)
    text = str(out or "")
    err_s = str(err or "")
    if "No such file" in text or "does not exist" in err_s or "No such file" in err_s:
        return ""
    return text


def _split_csv_ids(blob):
    parts = [
        p.strip().strip('"').strip("'") for p in blob.replace("\n", " ").split(",")
    ]
    return [p for p in parts if p]


def redact_ceph_users(text):
    """Redact Keys= values for safe logging (SEC-02)."""
    return _KEYS_RE.sub("Keys = <redacted>;", text or "")


def verify_ceph_users(node, cluster_id, expect_k, fs_name="cephfs", must_exist=True):
    """Assert CEPH_USERS uses native USERS{Userids,Keys,filesystem} with length K."""
    raw = read_ceph_users(node, cluster_id, check_ec=False)
    if not raw.strip():
        if must_exist:
            raise OperationFailedError(
                f"nfs.{cluster_id} missing CEPH_USERS object "
                f"{ceph_users_object_name(cluster_id)}"
            )
        log.info("nfs.%s CEPH_USERS absent/empty (expected)", cluster_id)
        return ""
    log.info(
        "nfs.%s CEPH_USERS (redacted):\n%s",
        cluster_id,
        redact_ceph_users(raw),
    )
    if not _USERS_BLOCK_RE.search(raw):
        raise OperationFailedError(
            f"nfs.{cluster_id} CEPH_USERS missing USERS {{ }} block "
            "(IBMCEPH-19374 USER{{}} form?)"
        )
    userids_m = _USERIDS_RE.search(raw)
    keys_m = _KEYS_RE.search(raw)
    if not userids_m or not keys_m:
        raise OperationFailedError(
            f"nfs.{cluster_id} CEPH_USERS missing Userids/Keys lists"
        )
    userids = _split_csv_ids(userids_m.group(1))
    keys = _split_csv_ids(keys_m.group(1))
    # ponytail: count tokens only; never log key material
    if len(userids) != int(expect_k):
        raise OperationFailedError(
            f"nfs.{cluster_id} Userids length {len(userids)} != K={expect_k}: "
            f"{userids}"
        )
    if len(keys) != int(expect_k):
        raise OperationFailedError(
            f"nfs.{cluster_id} Keys length {len(keys)} != K={expect_k}"
        )
    fs_match = _FILESYSTEM_RE.search(raw)
    if not fs_match:
        raise OperationFailedError(
            f"nfs.{cluster_id} CEPH_USERS missing filesystem= field"
        )
    if fs_name and fs_match.group(1) != fs_name:
        raise OperationFailedError(
            f"nfs.{cluster_id} CEPH_USERS filesystem={fs_match.group(1)!r} "
            f"(expected {fs_name!r})"
        )
    return raw


def _nfs_auth_ls(node, cluster_id):
    """Return ceph auth ls lines for client.nfs.<cluster_id>.* (keys redacted)."""
    out, _ = node.exec_command(
        sudo=True,
        cmd=(
            f"ceph auth ls 2>/dev/null | "
            f"grep -E 'client\\.nfs\\.{cluster_id}\\.' || true"
        ),
        check_ec=False,
    )
    text = str(out or "")
    safe = "\n".join(
        ln
        for ln in text.splitlines()
        if "key:" not in ln.lower() and not ln.strip().startswith("AQ")
    )
    return text, safe


def verify_pool_auth(node, cluster_id, fs_name="cephfs"):
    """Assert CephX pool.* entities exist for the NFS cluster FS."""
    text, safe = _nfs_auth_ls(node, cluster_id)
    log.info("nfs.%s auth entities:\n%s", cluster_id, safe)
    if f".{fs_name}.pool." not in text and ".pool." not in text:
        raise OperationFailedError(
            f"nfs.{cluster_id} missing CephX pool.* users for {fs_name}"
        )
    return text


def assert_pool_users_absent(node, cluster_id):
    """CFG-07: before first / export, no CEPH_USERS content and no pool.* auth."""
    raw = read_ceph_users(node, cluster_id)
    if raw.strip():
        raise OperationFailedError(
            f"nfs.{cluster_id} CEPH_USERS present before first / export "
            f"(redacted):\n{redact_ceph_users(raw)}"
        )
    text, safe = _nfs_auth_ls(node, cluster_id)
    log.info("nfs.%s auth before first export:\n%s", cluster_id, safe)
    if ".pool." in text:
        raise OperationFailedError(
            f"nfs.{cluster_id} CephX pool.* users exist before first / export:\n{safe}"
        )


def verify_pool(
    ceph_cluster,
    node,
    cluster_id,
    expect_clients,
    asok_timeout=120,
    check_conf=True,
    check_ceph_users=True,
    fs_name="cephfs",
    min_asoks=None,
):
    """Assert pool-of-clients: shared cmount_path=/, asoks, optional conf/USERS.

    Pure pool (no PEC exports): require exactly ``expect_clients`` asoks.
    Mixed pool+PEC: pass ``min_asoks`` (typically K + pec_count); total asok
    count alone must not be used as a pool-health signal.
    """
    exports = _export_details(node, cluster_id)
    shared = []
    pec_count = 0
    for item in exports:
        fsal = item.get("fsal") or {}
        user_id = fsal.get("user_id")
        cmount = fsal.get("cmount_path") or item.get("cmount_path") or "/"
        if cmount != "/":
            pec_count += 1
            continue
        if not user_id:
            raise OperationFailedError(
                f"nfs.{cluster_id} / export {item.get('pseudo')} missing user_id"
            )
        shared.append(user_id)
    if int(expect_clients) > 1 and not shared:
        raise OperationFailedError(
            f"nfs.{cluster_id} expected / exports for pool verify, found none"
        )
    if shared and len(set(shared)) != 1:
        raise OperationFailedError(
            f"nfs.{cluster_id} / exports must share one user_id, got {set(shared)}"
        )
    if check_conf and expect_clients > 1:
        conf = read_ganesha_conf(ceph_cluster, node, cluster_id)
        match = _CLIENTS_PER_POOL_CONF_RE.search(conf)
        if not match or int(match.group(1)) != int(expect_clients):
            raise OperationFailedError(
                f"nfs.{cluster_id} ganesha.conf clients_per_pool expected "
                f"{expect_clients}, got {match.group(0) if match else None}"
            )
    if check_ceph_users and expect_clients > 1:
        verify_ceph_users(node, cluster_id, expect_k=expect_clients, fs_name=fs_name)
        verify_pool_auth(node, cluster_id, fs_name=fs_name)

    if min_asoks is None:
        # Pure pool: exact K. Mixed without explicit min: at least K + PEC.
        need = int(expect_clients) + pec_count if pec_count else int(expect_clients)
        exact = pec_count == 0
    else:
        need = int(min_asoks)
        exact = False

    asoks = []
    for _ in WaitUntil(timeout=asok_timeout, interval=5):
        asoks = list_cephfs_asoks(ceph_cluster, node, cluster_id)
        if exact and len(asoks) == need:
            break
        if not exact and len(asoks) >= need:
            break
        log.info(
            "Waiting for nfs.%s pool asoks: have %s need %s exact=%s (%s)",
            cluster_id,
            len(asoks),
            need,
            exact,
            asoks,
        )
    else:
        raise OperationFailedError(
            f"nfs.{cluster_id} expected {'exactly' if exact else '>='} {need} "
            f"CephFS asoks, got {len(asoks)}: {asoks}"
        )
    log.info(
        "Pool verify nfs.%s expect=%s need_asoks=%s asoks=%s pec=%s shared_user=%s OK",
        cluster_id,
        expect_clients,
        need,
        asoks,
        pec_count,
        shared[0] if shared else None,
    )
    return asoks


def verify_pec_exports(node, cluster_id, expect_clients):
    """Assert PEC exports only (cmount_path != /) have distinct user_ids.

    Use on mixed pool+PEC clusters; do not call verify_pec (rejects /).
    """
    exports = _export_details(node, cluster_id)
    user_ids = []
    for item in exports:
        fsal = item.get("fsal") or {}
        cmount = fsal.get("cmount_path") or item.get("cmount_path")
        if not cmount or cmount == "/":
            continue
        user_id = fsal.get("user_id")
        if not user_id:
            raise OperationFailedError(
                f"nfs.{cluster_id} PEC export {item.get('pseudo')} missing user_id"
            )
        user_ids.append(user_id)
    distinct = set(user_ids)
    if len(distinct) != int(expect_clients):
        raise OperationFailedError(
            f"nfs.{cluster_id} expected {expect_clients} PEC user_ids, "
            f"got {len(distinct)}: {sorted(distinct)}"
        )
    log.info(
        "PEC-exports verify nfs.%s clients=%s user_ids=%s OK",
        cluster_id,
        expect_clients,
        sorted(distinct),
    )
    return sorted(distinct)
