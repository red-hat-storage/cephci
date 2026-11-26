"""NFS Ganesha Object Cache (OC), Per-Export Client (PEC), and pool helpers.

OC is an NFS orch spec (enable_client_object_cache / size / dirty) that
renders a CEPH {} block in ganesha.conf. PEC is --cmount_path on export
create (N CephFS clients / asoks / user_ids). Pool of libcephfs clients is
``--clients_per_pool K`` at cluster create (K asoks, cmount_path=/, CEPH_USERS).

OC/PEC require live cluster Ceph >= 20.2.2. Pool soft-skips when create -h
lacks the flag. Pool/NFS enhancement scenarios (raise K, brownfield, K
ceiling, client_pool info, pool rotate-key, NFS spec log_to_file) require
Ceph >= 21.0.0 (IBM Storage Ceph 10.0 / Umbrella). Do not use --rhbuild.
"""

import json
import re
import shlex
from contextlib import contextmanager
from time import sleep

from looseversion import LooseVersion

from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.filesys import Unmount
from tests.nfs.nfs_operations import (
    GANESHA_SUBVOL_GROUP,
    _nfs_daemon_not_ready,
    create_nfs_via_file_and_verify,
    get_ceph_version,
    mount_nfs_export_with_wait,
    verify_nfs_ganesha_service,
    wait_for_nfs_ganesha_daemons,
)
from utility.log import Log

log = Log(__name__)

OC_PEC_MIN_CEPH = "20.2.2"
# IBM Storage Ceph 10.0 / Umbrella (upstream 21.x)
POOL_ENHANCEMENT_MIN_CEPH = "21.0.0"


class SoftSkip(Exception):
    """Scenario soft-skip; run() exits 0 unless crash check fails."""


def _probe_ceph_version(ceph_cluster):
    """Return installed Ceph version string, or empty if unreadable."""
    installers = ceph_cluster.get_nodes(role="installer") or []
    clients = ceph_cluster.get_nodes("client") or []
    probes = []
    if installers:
        probes.append((installers[0], True))
    if clients:
        probes.append((clients[0], False))
    for node, prefix in probes:
        try:
            ver = get_ceph_version(node, prefix_cephadm=prefix)
            if ver:
                return ver
        except Exception as err:
            log.warning("Ceph version probe failed on %s: %s", node.hostname, err)
    return ""


def pool_enhancement_supported(ceph_cluster):
    """True when live Ceph >= 21.0.0 (IBM Ceph 10.0 enhancement scenarios)."""
    installed = _probe_ceph_version(ceph_cluster)
    if not installed:
        log.warning("Unparseable Ceph version; 10.0 enhancement scenarios disabled")
        return False
    supported = LooseVersion(installed) >= LooseVersion(POOL_ENHANCEMENT_MIN_CEPH)
    log.info(
        "Pool enhancement version gate: installed=%s min=%s supported=%s",
        installed,
        POOL_ENHANCEMENT_MIN_CEPH,
        supported,
    )
    return supported


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
    installed = _probe_ceph_version(ceph_cluster)
    if installed and LooseVersion(installed) >= LooseVersion(OC_PEC_MIN_CEPH):
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


def read_ganesha_conf(ceph_cluster, node, cluster_id, daemon_timeout=180):
    """Return ganesha.conf from the NFS container."""
    container_id, hostname = _nfs_daemon(node, cluster_id, timeout=daemon_timeout)
    host = _node_by_hostname(ceph_cluster, hostname)
    out, _ = host.exec_command(
        sudo=True,
        cmd=f"podman exec {container_id} cat /etc/ganesha/ganesha.conf",
        timeout=60,
    )
    return str(out)


def list_cephfs_asoks(ceph_cluster, node, cluster_id):
    """Return CephFS client asok filenames inside the NFS container.

    Match ``client.nfs.<cluster_id>.cephfs.<…>`` only. A bare ``grep cephfs``
    false-positives when the NFS cluster *name* contains ``cephfs``
    (e.g. ``cephfs-nfs-cephx-pool`` → ``client.nfs.…pool.2.asok``).
    """
    container_id, hostname = _nfs_daemon(node, cluster_id)
    host = _node_by_hostname(ceph_cluster, hostname)
    out, _ = host.exec_command(
        sudo=True,
        cmd=(
            f"podman exec {container_id} bash -c "
            "'ls /run/ceph/ 2>/dev/null | grep asok || true'"
        ),
        timeout=60,
    )
    needle = f"client.nfs.{cluster_id}.cephfs."
    return [line.strip() for line in str(out).splitlines() if needle in line.strip()]


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
    # Daemon may still be the pre-redeploy container; conf is polled in verify_*.
    wait_for_nfs_ganesha_daemons(installer, timeout=timeout, nfs_name=cluster_id)
    return True


def verify_nfs_object_cache(
    ceph_cluster,
    node,
    cluster_id,
    expect_on,
    size=None,
    max_dirty=None,
    timeout=180,
    interval=5,
):
    """Assert ganesha.conf CEPH {} matches OC on/off. Product assert is conf.

    Retries while cephadm redeploy lags: orch can leave the old container
    ``running`` without ``client_oc`` until the new one starts.
    """
    last_err = None
    conf = ""
    for _ in WaitUntil(timeout=timeout, interval=interval):
        try:
            # Short daemon wait: avoid nesting another full timeout per tick.
            conf = read_ganesha_conf(
                ceph_cluster, node, cluster_id, daemon_timeout=interval
            )
            has_ceph_block = bool(re.search(r"\bCEPH\s*\{", conf, re.I))
            oc_match = _CLIENT_OC_RE.search(conf)
            oc_true = oc_match.group(1).lower() == "true" if oc_match else False
            if expect_on:
                if not has_ceph_block or not oc_true:
                    raise OperationFailedError(
                        f"nfs.{cluster_id} expected CEPH {{ client_oc=true }} "
                        "in ganesha.conf"
                    )
                if size is not None:
                    size_match = _CLIENT_OC_SIZE_RE.search(conf)
                    expected = size_to_bytes(size)
                    if not size_match or int(size_match.group(1)) != expected:
                        raise OperationFailedError(
                            f"nfs.{cluster_id} client_oc_size expected {expected} "
                            f"(from {size}), "
                            f"conf={size_match.group(0) if size_match else None}"
                        )
                if max_dirty is not None:
                    dirty_match = _CLIENT_OC_DIRTY_RE.search(conf)
                    expected = size_to_bytes(max_dirty)
                    if not dirty_match or int(dirty_match.group(1)) != expected:
                        raise OperationFailedError(
                            f"nfs.{cluster_id} client_oc_max_dirty expected "
                            f"{expected} (from {max_dirty}), "
                            f"conf={dirty_match.group(0) if dirty_match else None}"
                        )
            elif has_ceph_block and oc_true:
                raise OperationFailedError(
                    f"nfs.{cluster_id} expected OC off in ganesha.conf "
                    "(no CEPH block or client_oc=false)"
                )
            log.info("ganesha.conf for nfs.%s:\n%s", cluster_id, conf)
            log.info(
                "OC ganesha.conf verify nfs.%s expect_on=%s OK",
                cluster_id,
                expect_on,
            )
            return
        except Exception as err:
            last_err = err
            log.info(
                "Waiting for nfs.%s OC conf expect_on=%s (%s)",
                cluster_id,
                expect_on,
                err,
            )
    if conf:
        log.error("ganesha.conf for nfs.%s (final):\n%s", cluster_id, conf)
    raise OperationFailedError(
        f"nfs.{cluster_id} OC conf verify timed out after {timeout}s "
        f"(expect_on={expect_on}): {last_err}"
    ) from last_err


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


def _load_nfs_orch_spec(client, nfs_name):
    """Return the first orch export dict for nfs.<name> (status/events stripped)."""
    out, err = client.exec_command(
        sudo=True,
        cmd=f"ceph orch ls --service-name nfs.{nfs_name} --export --format json",
        check_ec=False,
        timeout=120,
    )
    text = f"{out or ''}{err or ''}".strip()
    try:
        specs = json.loads(out) if out and str(out).strip().startswith("[") else []
    except json.JSONDecodeError as exc:
        raise OperationFailedError(
            f"nfs.{nfs_name} orch export JSON parse failed: {exc}: {text[:300]}"
        )
    if not specs:
        raise OperationFailedError(f"No orch export for nfs.{nfs_name}: {text[:300]}")
    spec = specs[0]
    spec.setdefault("spec", {})
    spec.pop("status", None)
    spec.pop("events", None)
    return spec


def _apply_nfs_orch_spec(client, installer, nfs_name, spec, timeout=300, restart=True):
    """Write *spec* via ``ceph orch apply -i``; optionally restart + wait.

    Returns (rc, combined_stdout_stderr). Does not raise on non-zero apply rc
    so callers can SoftSkip pre-fix builds.
    """
    remote = f"/tmp/nfs_{nfs_name}_orch_spec.json"
    rf = client.remote_file(sudo=True, file_name=remote, file_mode="w")
    try:
        rf.write(json.dumps(spec))
        rf.flush()
    finally:
        rf.close()
    apply_out, apply_err = client.exec_command(
        sudo=True,
        cmd=f"ceph orch apply -i {remote}",
        check_ec=False,
        timeout=180,
    )
    combined = f"{apply_out or ''}{apply_err or ''}"
    rc = getattr(client, "exit_status", 0)
    log.info("orch apply nfs.%s rc=%s: %s", nfs_name, rc, combined[:400])
    if rc != 0:
        return rc, combined
    if restart:
        sleep(5)
        client.exec_command(
            sudo=True,
            cmd=f"ceph orch restart nfs.{nfs_name}",
            check_ec=False,
            timeout=120,
        )
        sleep(10)
        wait_for_nfs_ganesha_daemons(installer, timeout=timeout, nfs_name=nfs_name)
    return 0, combined


def patch_nfs_orch_spec(client, installer, nfs_name, fields, timeout=300, restart=True):
    """Merge *fields* into nfs orch ``spec`` and apply. Returns (rc, text)."""
    spec = _load_nfs_orch_spec(client, nfs_name)
    spec["spec"].update(fields)
    return _apply_nfs_orch_spec(
        client, installer, nfs_name, spec, timeout=timeout, restart=restart
    )


def apply_clients_per_pool(client, installer, nfs_name, k, timeout=300, restart=True):
    """Set clients_per_pool via orch export/apply; optionally restart daemons."""
    return patch_nfs_orch_spec(
        client,
        installer,
        nfs_name,
        {"clients_per_pool": int(k)},
        timeout=timeout,
        restart=restart,
    )


def assert_cluster_info_clients_per_pool(client, nfs_name, expect_k):
    """Assert ceph nfs cluster info reports clients_per_pool == expect_k."""
    out, err = client.exec_command(
        sudo=True,
        cmd=f"ceph nfs cluster info {nfs_name} -f json",
        check_ec=False,
        timeout=60,
    )
    text = f"{out or ''}{err or ''}"
    try:
        info = json.loads(out) if out and str(out).strip() else {}
    except json.JSONDecodeError as exc:
        raise OperationFailedError(
            f"nfs.{nfs_name} cluster info JSON parse failed: {exc}: {text[:300]}"
        )
    # Live shape: { "<cluster_id>": { "clients_per_pool": N, "backend": [...] } }
    block = info.get(nfs_name, info) if isinstance(info, dict) else {}
    got = block.get("clients_per_pool") if isinstance(block, dict) else None
    if got is None or int(got) != int(expect_k):
        raise OperationFailedError(
            f"nfs.{nfs_name} cluster info clients_per_pool={got!r} "
            f"(expected {expect_k}); raw={text[:400]}"
        )
    log.info("nfs.%s cluster info clients_per_pool=%s OK", nfs_name, got)


def assert_export_client_pool(
    client, nfs_name, export_path, expect_users=None, timeout=0, interval=5
):
    """Assert export get client_pool.users presence/length.

    expect_users:
      int  -> client_pool.users must exist with that length
      None -> client_pool must be absent (PEC / non-pool export)
    timeout > 0 polls until ready (brownfield race after orch enable).
    """

    def _check():
        last_text = ""
        info = None
        for cmd in (
            f"ceph nfs export get {nfs_name} {export_path} -f json",
            f"ceph nfs export info {nfs_name} {export_path} -f json",
        ):
            out, err = client.exec_command(
                sudo=True, cmd=cmd, check_ec=False, timeout=60
            )
            last_text = f"{out or ''}{err or ''}"
            if getattr(client, "exit_status", 0) != 0 or not (out or "").strip():
                continue
            try:
                info = json.loads(out)
                break
            except json.JSONDecodeError:
                continue
        if info is None:
            raise OperationFailedError(
                f"nfs.{nfs_name} export detail for {export_path} unavailable: "
                f"{last_text[:300]}"
            )
        pool = info.get("client_pool")
        if expect_users is None:
            if pool:
                raise OperationFailedError(
                    f"nfs.{nfs_name} {export_path} unexpected client_pool={pool!r}"
                )
            log.info("nfs.%s %s has no client_pool (expected)", nfs_name, export_path)
            return
        users = (pool or {}).get("users") if isinstance(pool, dict) else None
        if not isinstance(users, list) or len(users) != int(expect_users):
            raise OperationFailedError(
                f"nfs.{nfs_name} {export_path} client_pool.users "
                f"expected length {expect_users}, got {users!r}"
            )
        log.info(
            "nfs.%s %s client_pool.users length=%s OK",
            nfs_name,
            export_path,
            len(users),
        )

    if timeout <= 0:
        _check()
        return
    last_err = None
    for _ in WaitUntil(timeout=timeout, interval=interval):
        try:
            _check()
            return
        except OperationFailedError as err:
            last_err = err
            log.info(
                "waiting for nfs.%s %s client_pool users=%s: %s",
                nfs_name,
                export_path,
                expect_users,
                err,
            )
    raise OperationFailedError(
        f"nfs.{nfs_name} {export_path} client_pool not ready: {last_err}"
    )


def ceph_users_object_name(cluster_id):
    return f"ceph-users-nfs.{cluster_id}"


def read_ceph_users(node, cluster_id):
    """Return RADOS CEPH_USERS object text (may be empty).

    Missing object -> empty string. Other rados failures raise so CFG-07
    does not treat auth/pool errors as absent.
    """
    obj = ceph_users_object_name(cluster_id)
    cmd = f"rados -N {cluster_id} -p .nfs get {obj} -"
    out, err = node.exec_command(sudo=True, cmd=cmd, check_ec=False, timeout=60)
    text = str(out or "")
    err_s = str(err or "")
    rc = getattr(node, "exit_status", 0)
    if rc == 0:
        return text
    if "No such file" in text or "No such file" in err_s or "does not exist" in err_s:
        return ""
    raise OperationFailedError(
        f"rados get {obj} failed rc={rc}: {err_s or text or '<empty>'}"
    )


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
    raw = read_ceph_users(node, cluster_id)
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


_ROTATE_KEY_TYPE = "aes256k"


def delete_nfs_cluster(client, nfs_name, timeout=60, interval=5):
    """``ceph nfs cluster rm`` then wait until orch no longer lists the service."""
    try:
        client.exec_command(
            sudo=True, cmd=f"ceph nfs cluster rm {nfs_name}", timeout=60
        )
    except Exception as err:
        log.warning("cluster rm %s: %s", nfs_name, err)
    for _ in WaitUntil(timeout=timeout, interval=interval):
        out, _ = client.exec_command(
            sudo=True,
            cmd=f"ceph orch ls --service-name nfs.{nfs_name}",
            check_ec=False,
        )
        text = str(out)
        if "No services reported" in text or f"nfs.{nfs_name}" not in text:
            return
    log.warning("nfs.%s still listed after cluster rm", nfs_name)


def delete_nfs_exports_and_subvols(client, nfs_name, export_paths, fs_name="cephfs"):
    """Delete NFS exports and matching ganeshagroup subvolumes."""
    if isinstance(export_paths, str):
        export_paths = [export_paths]
    for export_path in export_paths:
        try:
            Ceph(client).nfs.export.delete(nfs_name, export_path)
        except Exception as err:
            log.warning("export delete %s: %s", export_path, err)
        subvol_name = export_path.replace("/", "")
        if not subvol_name:
            continue
        client.exec_command(
            sudo=True,
            cmd=(
                f"ceph fs subvolume rm {fs_name} {subvol_name} "
                f"--group_name {GANESHA_SUBVOL_GROUP} --force"
            ),
            check_ec=False,
        )


def rm_nfs_mount_point(client, mount_point):
    """``rm -rf`` a mount dir only when it is not still a mountpoint."""
    mp = shlex.quote(mount_point)
    out, _ = client.exec_command(
        sudo=True,
        cmd=(
            f"if mountpoint -q {mp}; then echo STILL_MOUNTED; "
            f"else timeout 120 rm -rf -- {mp}; fi"
        ),
        check_ec=False,
        timeout=180,
    )
    if "STILL_MOUNTED" in str(out or ""):
        log.warning("skip rm -rf; still mounted: %s", mount_point)
        return False
    return True


def full_nfs_cleanup(client, nfs_name, export_paths, mount_points, fs_name="cephfs"):
    """Lazy umount + rm mounts, drop exports/subvols, then cluster rm + orch wait."""
    if isinstance(mount_points, str):
        mount_points = [mount_points]
    for mount_point in mount_points:
        try:
            mp = shlex.quote(mount_point)
            client.exec_command(
                sudo=True,
                cmd=f"timeout 60 umount -l {mp}",
                check_ec=False,
                timeout=90,
            )
            rm_nfs_mount_point(client, mount_point)
        except Exception:
            pass
    delete_nfs_exports_and_subvols(client, nfs_name, export_paths, fs_name=fs_name)
    delete_nfs_cluster(client, nfs_name)


def _rotate_key_available(installer):
    """True when ``ceph nfs cluster rotate-key`` is in help."""
    out, err = installer.exec_command(
        sudo=True,
        cmd="cephadm shell -- ceph nfs cluster --help",
        check_ec=False,
    )
    help_text = "\n".join(part for part in (out, err) if part)
    return "rotate-key" in help_text


def _snapshot_nfs_keys(installer, nfs_name, timeout=60, interval=6):
    """Return {entity: key} for client.nfs.<nfs_name>.* entities."""
    prefix = f"client.nfs.{nfs_name}"
    entities = []
    for _ in WaitUntil(timeout=timeout, interval=interval):
        raw = CephAdm(installer).ceph.auth.list()
        entities = []
        for line in raw.splitlines():
            line = line.strip()
            if line.startswith(prefix):
                entities.append(line.split()[0] if " " in line else line)
        if entities:
            break
    if not entities:
        raise OperationFailedError(
            f"No CephX entities for nfs.{nfs_name} after {timeout}s"
        )
    keys = {}
    for entity in entities:
        raw = CephAdm(installer).ceph.auth.get(entity)
        for line in raw.splitlines():
            line = line.strip()
            if line.startswith("key"):
                keys[entity] = line.split("=", 1)[1].strip()
                break
        else:
            raise OperationFailedError(f"Could not parse key for {entity!r}")
    return keys


def _assert_nfs_keys_rotated(before, after):
    shared = set(before) & set(after)
    if not shared:
        raise OperationFailedError(
            f"No overlapping NFS CephX entities: before={list(before)} after={list(after)}"
        )
    unchanged = [e for e in sorted(shared) if before[e] == after[e]]
    if unchanged:
        raise OperationFailedError(
            "Key value unchanged after rotate-key for: " + ", ".join(unchanged)
        )


def run_pool_rotate_key(ceph_cluster, config):
    """Rotate-key on a clients_per_pool NFS cluster (10.0+).

    SoftSkip when rotate-key is absent. Caller should gate Ceph >= 21 / pool
    flag and wrap with ``ensure_nfs_admin_socket``.
    """
    clients = ceph_cluster.get_nodes("client")
    if not clients:
        raise OperationFailedError("No client nodes")
    client = clients[0]
    installers = ceph_cluster.get_nodes(role="installer") or []
    installer = installers[0] if installers else client
    if not _rotate_key_available(installer):
        raise SoftSkip("ceph nfs cluster rotate-key not in help")

    nfs_nodes = (
        ceph_cluster.get_nodes("nfs")
        or ceph_cluster.get_nodes(role="installer")
        or ceph_cluster.get_nodes("mds")
    )
    if not nfs_nodes:
        raise OperationFailedError("No NFS nodes")
    nfs_server = nfs_nodes[0].hostname
    server_ip = nfs_nodes[0].ip_address

    k = int(config.get("clients_per_pool", 3))
    nfs_name = config.get("nfs_name", "cephfs-nfs-cephx-pool")
    fs_name = config.get("fs_name", "cephfs")
    nfs_port = str(config.get("port", "2071"))
    nfs_version = config.get("nfs_version", "4.2")
    exports = [f"/export_pool_rk_{i}" for i in range(k)]
    mounts = [f"/mnt/nfs_pool_rk_{i}" for i in range(k)]

    try:
        Ceph(client).mgr.module.enable(module="nfs", force=True)
        sleep(2)
        # Clear leftover cluster/asoks from aborted prior runs.
        delete_nfs_cluster(client, nfs_name)
        create_nfs_with_clients_per_pool(client, nfs_name, nfs_server, nfs_port, k)
        sleep(5)
        verify_nfs_ganesha_service(installer, timeout=300)
        for export in exports:
            Ceph(client).nfs.export.create(
                fs_name=fs_name, nfs_name=nfs_name, nfs_export=export, fs=fs_name
            )
        sleep(5)
        for export, mount in zip(exports, mounts):
            mount_nfs_export_with_wait(
                client,
                server_ip,
                nfs_port,
                export,
                mount,
                nfs_version,
                installer_node=installer,
                nfs_name=nfs_name,
            )

        verify_pool(ceph_cluster, client, nfs_name, expect_clients=k)
        probe = f"{mounts[0]}/pre_pool_rotate"
        client.exec_command(
            sudo=True,
            cmd=f"dd if=/dev/urandom of={probe} bs=1M count=4 conv=fsync status=none",
            timeout=60,
        )
        baseline = _snapshot_nfs_keys(installer, nfs_name)
        result = CephAdm(installer).ceph.nfs.cluster.rotate_key(
            nfs_name, key_type=_ROTATE_KEY_TYPE
        )
        log.info("pool rotate-key response: %s", json.dumps(result, indent=2))
        rotated = result.get("rotated") or []
        if not rotated:
            raise OperationFailedError("rotate-key returned empty rotated list")
        if result.get("service_redeployed") is not True:
            raise OperationFailedError(
                f"service_redeployed={result.get('service_redeployed')!r}"
            )
        updated = result.get("updated_exports") or []
        missing = [e for e in exports if e not in updated]
        if missing:
            raise OperationFailedError(
                f"exports missing from updated_exports: {missing}"
            )
        # K clients = 1 primary + (K-1) *.pool.N — only the extras use ".pool.".
        pool_ents = [e for e in rotated if ".pool." in e]
        need_pool = max(k - 1, 0)
        if len(pool_ents) < need_pool:
            raise OperationFailedError(
                f"expected >= {need_pool} pool.* keys in rotated "
                f"(K={k} => primary + {need_pool} pool.*), got {pool_ents}"
            )
        cluster_ents = [e for e in rotated if nfs_name in e]
        if len(cluster_ents) < k:
            raise OperationFailedError(
                f"expected >= {k} rotated keys for nfs.{nfs_name}, got {cluster_ents}"
            )
        sleep(10)
        verify_nfs_ganesha_service(installer, timeout=300)
        for export, mount in zip(exports, mounts):
            try:
                Unmount(client).unmount(mount)
            except Exception:
                pass
            mount_nfs_export_with_wait(
                client,
                server_ip,
                nfs_port,
                export,
                mount,
                nfs_version,
                installer_node=installer,
                nfs_name=nfs_name,
            )
        out, _ = client.exec_command(sudo=True, cmd=f"ls -lh {probe}", timeout=30)
        if "pre_pool_rotate" not in (out or ""):
            raise OperationFailedError(f"pre-rotate file missing after remount: {out}")
        client.exec_command(
            sudo=True,
            cmd=(
                f"dd if=/dev/urandom of={mounts[0]}/post_pool_rotate "
                f"bs=1M count=2 conv=fsync status=none"
            ),
            timeout=60,
        )
        _assert_nfs_keys_rotated(baseline, _snapshot_nfs_keys(installer, nfs_name))
        verify_pool(ceph_cluster, client, nfs_name, expect_clients=k)
        log.info("pool rotate-key OK for nfs.%s K=%s", nfs_name, k)
    finally:
        # Always cleanup: timed umount/rm, export+subvol delete, cluster rm + wait.
        full_nfs_cleanup(client, nfs_name, exports, mounts, fs_name=fs_name)
