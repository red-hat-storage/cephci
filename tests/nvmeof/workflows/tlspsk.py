"""
TLS-PSK workflow utilities for NVMe-oF tests.

Implements every step from:
  https://www.ibm.com/docs/en/storage-ceph/9.9.1?topic=gateways-configuring-tls-psk-authentication-nvme

Full procedure (in order)
--------------------------

**Phase 1 — Deploy the NVMe-oF gateway service with TLS-PSK support**
  (done by NVMeService.deploy() — openssl key + orch spec with enable_encryption: True)

**Phase 2 — Install and configure the TLS daemon on the initiator**

    dnf install ktls-utils -y

    # /etc/tlshd.conf:
    [authenticate]
    keyrings = .nvme

    systemctl enable --now tlshd

**Phase 3 — Generate and register the TLS key on the initiator**

    cat /etc/nvme/hostnqn                           # retrieve host NQN
    nvme gen-tls-key                                 # generate PSK → KEY_DATA
    modprobe nvme-tcp
    nvme check-tls-key --insert \\
        --hostnqn=HOST_NQN --subsysnqn=SUBSYSTEM_NQN \\
        --keydata=KEY_DATA --identity=0

**Phase 4 — Configure the NVMe-oF subsystem with secure listeners (gateway CLI)**

    ceph nvmeof subsystem add SUBSYSTEM_NQN --secure_listeners
    ceph nvmeof namespace add --nqn SUBSYSTEM_NQN ...
    ceph nvmeof listener add --nqn SUBSYSTEM_NQN --secure

**Phase 5 — Add host with PSK (gateway CLI)**

    ceph nvmeof host add --nqn SUBSYSTEM_NQN \\
        --host_nqn HOST_NQN --psk KEY_DATA

**Phase 6 — Connect from initiator**

    nvme connect --transport tcp --traddr GATEWAY_IP \\
        --trsvcid 4420 --nqn SUBSYSTEM_NQN --tls
"""

import time

from ceph.ceph import CommandFailed
from utility.log import Log

LOG = Log(__name__)

# Default path for the RSA encryption key on gateway nodes (Phase 1 / orch spec)
DEFAULT_ENCRYPTION_KEY_PATH = "/root/encryption.key"

# TCP-level failures. These mean the listener is not accepting connections.
# They are not a TLS handshake or NVMe connect rejection.
_LISTENER_DOWN_MARKERS = (
    "connection refused",
    "failed to connect to remote host",
    "failed to connect to all addresses",
    "is going down",
    "no route to host",
    "network is unreachable",
)

# tlshd config block required on the initiator
TLSHD_CONF_BLOCK = "[authenticate]\nkeyrings = .nvme\n"


# ---------------------------------------------------------------------------
# Phase 2 — TLS daemon setup on initiator node
# ---------------------------------------------------------------------------


def setup_tlshd(initiator_node):
    """Install ktls-utils, configure tlshd, and enable the daemon.

    Runs the exact steps from the doc on *initiator_node*:

        dnf install ktls-utils -y
        # write [authenticate] / keyrings = .nvme to /etc/tlshd.conf
        systemctl enable --now tlshd

    Args:
        initiator_node: CephNode for the initiator (RHEL 9.7+).
    """
    LOG.info("setup_tlshd: Installing ktls-utils on %s", initiator_node.hostname)
    initiator_node.exec_command(cmd="dnf install ktls-utils -y", sudo=True)

    LOG.info("setup_tlshd: Configuring /etc/tlshd.conf on %s", initiator_node.hostname)
    # Preserve any existing sections; append/replace [authenticate] block
    initiator_node.exec_command(
        cmd=(
            "grep -q '\\[authenticate\\]' /etc/tlshd.conf 2>/dev/null || "
            "echo -e '[authenticate]\\nkeyrings = .nvme' >> /etc/tlshd.conf"
        ),
        sudo=True,
    )
    # Ensure keyrings = .nvme is set under [authenticate]
    initiator_node.exec_command(
        cmd=(
            'python3 -c "'
            "import configparser, os; "
            "c = configparser.ConfigParser(); "
            "c.read('/etc/tlshd.conf'); "
            "c.setdefault('authenticate', {})['keyrings'] = '.nvme'; "
            "open('/etc/tlshd.conf', 'w').write('\\n'.join("
            "  f'[{s}]' + '\\n' + '\\n'.join(f'{k} = {v}' for k,v in c[s].items())"
            '  for s in c.sections()))"'
        ),
        sudo=True,
    )

    LOG.info("setup_tlshd: Enabling tlshd on %s", initiator_node.hostname)
    initiator_node.exec_command(cmd="systemctl enable --now tlshd", sudo=True)
    LOG.info("setup_tlshd: tlshd enabled on %s", initiator_node.hostname)


# ---------------------------------------------------------------------------
# Phase 3 — Generate and register TLS PSK on initiator
# ---------------------------------------------------------------------------


def get_host_nqn(initiator_node):
    """Read (or generate) the host NQN on the initiator.

    Strategy:
      1. Try ``nvme show-hostnqn`` (RHEL 10+ preferred command).
      2. Fall back to ``cat /etc/nvme/hostnqn`` (RHEL 9 and earlier).
      3. If both return empty (hostnqn not yet generated), run
         ``nvme gen-hostnqn`` and persist it to ``/etc/nvme/hostnqn``
         so that both ``nvme show-hostnqn`` and the nvme-cli tools
         find a stable NQN on subsequent calls.

    Args:
        initiator_node: CephNode for the initiator.

    Returns:
        str: Host NQN string (e.g. ``nqn.2014-08.org.nvmexpress:uuid:...``).
    """
    # Step 1 — try nvme show-hostnqn (RHEL 10+)
    try:
        out, _ = initiator_node.exec_command(cmd="nvme show-hostnqn", sudo=True)
        hostnqn = out.strip()
        if hostnqn:
            LOG.info(
                "get_host_nqn (nvme show-hostnqn): %s → %s",
                initiator_node.hostname,
                hostnqn,
            )
            return hostnqn
    except Exception:
        pass

    # Step 2 — fall back to the flat file (RHEL 9 and earlier)
    try:
        out, _ = initiator_node.exec_command(cmd="cat /etc/nvme/hostnqn", sudo=True)
        hostnqn = out.strip()
        if hostnqn:
            LOG.info(
                "get_host_nqn (/etc/nvme/hostnqn): %s → %s",
                initiator_node.hostname,
                hostnqn,
            )
            return hostnqn
    except Exception:
        pass

    # Step 3 — hostnqn not yet generated; create one and persist it.
    # This happens on fresh RHEL 10 nodes where /etc/nvme/hostnqn does not
    # exist and nvme show-host-nqn returns empty until a hostnqn is written.
    LOG.info(
        "get_host_nqn: no hostnqn found on %s — generating with nvme gen-hostnqn",
        initiator_node.hostname,
    )
    initiator_node.exec_command(
        cmd="mkdir -p /etc/nvme && nvme gen-hostnqn > /etc/nvme/hostnqn",
        sudo=True,
    )
    out, _ = initiator_node.exec_command(cmd="cat /etc/nvme/hostnqn", sudo=True)
    hostnqn = out.strip()
    LOG.info(
        "get_host_nqn (generated): %s → %s",
        initiator_node.hostname,
        hostnqn,
    )
    return hostnqn


def generate_tls_key(initiator_node):
    """Run ``nvme gen-tls-key`` and return the generated key data string.

    Doc step::

        nvme gen-tls-key

    Args:
        initiator_node: CephNode for the initiator.

    Returns:
        str: The TLS key data string produced by nvme gen-tls-key.

    Raises:
        RuntimeError: If no key data can be parsed from the output.
    """
    LOG.info(
        "generate_tls_key: Running nvme gen-tls-key on %s", initiator_node.hostname
    )
    out, _ = initiator_node.exec_command(cmd="nvme gen-tls-key", sudo=True)
    # nvme gen-tls-key output format:
    # NVMeTLSkey-1:01:<base64data>:
    # or just the key on stdout depending on version
    key_data = out.strip()
    if not key_data:
        raise RuntimeError(
            f"generate_tls_key: nvme gen-tls-key produced no output on {initiator_node.hostname}"
        )
    LOG.info("generate_tls_key: key_data=%s", key_data[:40] + "...")
    return key_data


def load_nvme_tcp_module(initiator_node):
    """Load the nvme-tcp kernel module on the initiator.

    Doc step::

        modprobe nvme-tcp

    Args:
        initiator_node: CephNode for the initiator.
    """
    LOG.info("load_nvme_tcp_module: modprobe nvme-tcp on %s", initiator_node.hostname)
    initiator_node.exec_command(cmd="modprobe nvme-tcp", sudo=True)


def insert_tls_key(initiator_node, hostnqn, subsysnqn, key_data, identity=0):
    """Insert the TLS PSK into the kernel keyring via nvme check-tls-key.

    Doc step::

        nvme check-tls-key --insert \\
            --hostnqn=HOST_NQN --subsysnqn=SUBSYSTEM_NQN \\
            --keydata=KEY_DATA --identity=0

    Args:
        initiator_node: CephNode for the initiator.
        hostnqn (str): Host NQN (from /etc/nvme/hostnqn).
        subsysnqn (str): Subsystem NQN.
        key_data (str): Key data string from nvme gen-tls-key.
        identity (int): TLS identity (0 or 1). Doc uses 0.

    Returns:
        str: Output from nvme check-tls-key (may include key serial/description).
    """
    cmd = (
        f"nvme check-tls-key --insert "
        f"--hostnqn={hostnqn} "
        f"--subsysnqn={subsysnqn} "
        f"--keydata={key_data} "
        f"--identity={identity}"
    )
    LOG.info("insert_tls_key: %s on %s", cmd, initiator_node.hostname)
    out, _ = initiator_node.exec_command(cmd=cmd, sudo=True)
    LOG.info("insert_tls_key: result=%s", out.strip())
    return out.strip()


def setup_initiator_tls_psk(initiator_node, subsysnqn):
    """Run all Phase 2 + Phase 3 steps on the initiator for one subsystem.

    Convenience wrapper that runs the complete initiator-side TLS-PSK setup:
      1. Install and configure tlshd (Phase 2).
      2. Get host NQN.
      3. Generate TLS key (nvme gen-tls-key).
      4. Load nvme-tcp module.
      5. Insert key into kernel keyring (nvme check-tls-key --insert).

    Args:
        initiator_node: CephNode for the initiator.
        subsysnqn (str): Subsystem NQN to associate the key with.

    Returns:
        dict: {
            "hostnqn": str,
            "key_data": str,
        }
    """
    setup_tlshd(initiator_node)
    hostnqn = get_host_nqn(initiator_node)
    key_data = generate_tls_key(initiator_node)
    load_nvme_tcp_module(initiator_node)
    insert_tls_key(initiator_node, hostnqn, subsysnqn, key_data, identity=0)
    return {"hostnqn": hostnqn, "key_data": key_data}


# ---------------------------------------------------------------------------
# Phase 4 — Secure listener on gateway (via gateway entities / CLI)
# ---------------------------------------------------------------------------


def add_secure_listener(gateway, nqn, traddr, trsvcid, host_name):
    """Add a TLS-secure listener to a subsystem.

    Idempotent: silently skips if a secure listener already exists at the same
    address:port (e.g. leftover OMAP from a prior run where cleanup failed).
    Raises if the clash is with an *insecure* listener — that indicates a YAML
    misconfiguration (``secure_listener: true`` missing from the subsystem block).

    Doc step::

        ceph nvmeof listener add --nqn SUBSYSTEM_NQN --secure

    Passes ``secure=True`` to the listener add CLI call.

    Args:
        gateway: NVMeGateway instance (has .listener attribute).
        nqn (str): Subsystem NQN.
        traddr (str): Gateway IP address.
        trsvcid (int|str): Listener port (default 4420).
        host_name (str): Gateway hostname.
    """
    LOG.info(
        "add_secure_listener: nqn=%s traddr=%s trsvcid=%s secure=True",
        nqn,
        traddr,
        trsvcid,
    )
    try:
        gateway.listener.add(
            **{
                "args": {
                    "subsystem": nqn,
                    "traddr": traddr,
                    "trsvcid": trsvcid,
                    "host-name": host_name,
                    "secure": True,
                }
            }
        )
    except Exception as exc:
        err_str = str(exc)
        if "already exists" in err_str.lower() or "Listener already exists" in err_str:
            # Secure listener already present (leftover OMAP) — treat as success
            LOG.info(
                "add_secure_listener: listener %s:%s already exists on %s — skipping (stale OMAP?)",
                traddr,
                trsvcid,
                nqn,
            )
        elif "clashes with the existing insecure listener" in err_str:
            # Insecure listener clash — this is a YAML misconfiguration: the subsystem
            # was added without --secure_listeners, which auto-created an insecure listener.
            # The subsystem block must have secure_listener: true in the YAML.
            raise RuntimeError(
                f"add_secure_listener: INSECURE listener clash on {nqn} at {traddr}:{trsvcid}. "
                f"Ensure 'secure_listener: true' is set in the subsystem config block so that "
                f"'subsystem add --secure_listeners True' is used (not the default insecure add). "
                f"Original error: {exc}"
            ) from exc
        else:
            raise


# ---------------------------------------------------------------------------
# Phase 5 — Add host with PSK on gateway CLI
# ---------------------------------------------------------------------------


def host_psk_args(nqn, hostnqn, key_data, dhchap_key=None):
    """Build ``host add`` arguments for a TLS-PSK host.

    A subsystem that already has a DH-HMAC-CHAP key rejects a host that does
    not have one. ``dhchap_key`` is that host key, passed as ``--dhchap-key``
    next to ``--psk``.
    """
    args = {"subsystem": nqn, "host": hostnqn, "psk": key_data}
    if dhchap_key:
        args["dhchap-key"] = dhchap_key
    return args


def add_host_with_psk(gateway, nqn, hostnqn, key_data, dhchap_key=None):
    """Register an initiator host with its TLS PSK on the gateway.

    Idempotent: if the host is already registered (EINVAL: Host is already added),
    the existing entry is deleted first then re-added with the new PSK.  This
    handles the case where a previous test run's cleanup failed and left stale
    OMAP state, or where the caller simply wants to update the PSK.

    When the subsystem has a DH-HMAC-CHAP key, pass the host's own key as
    ``dhchap_key``. The gateway rejects a PSK-only host in that case.

    Doc step::

        ceph nvmeof host add --nqn SUBSYSTEM_NQN \\
            --host_nqn HOST_NQN --psk TLS_PSK_KEY

    Args:
        gateway: NVMeGateway instance (has .host attribute).
        nqn (str): Subsystem NQN.
        hostnqn (str): Initiator host NQN.
        key_data (str): TLS PSK key data from nvme gen-tls-key.
        dhchap_key (str|None): Host DH-HMAC-CHAP key, required when the
            subsystem has a DHCHAP key.
    """
    LOG.info("add_host_with_psk: subsystem=%s host_nqn=%s psk=<key_data>", nqn, hostnqn)
    args = host_psk_args(nqn, hostnqn, key_data, dhchap_key)
    try:
        gateway.host.add(**{"args": args})
    except Exception as exc:
        err_str = str(exc)
        if "Host is already added" in err_str or "already added" in err_str.lower():
            LOG.warning(
                "add_host_with_psk: host %s already exists on %s — "
                "deleting stale entry and re-adding with new PSK (leftover from prior run?)",
                hostnqn,
                nqn,
            )
            # Delete the stale host entry, then add fresh with current PSK
            gateway.host.delete(
                **{
                    "args": {
                        "subsystem": nqn,
                        "host": hostnqn,
                    }
                }
            )
            gateway.host.add(**{"args": args})
            LOG.info(
                "add_host_with_psk: re-added host %s on %s with fresh PSK", hostnqn, nqn
            )
        else:
            raise


# ---------------------------------------------------------------------------
# Phase 6 — Connect from initiator with --tls
# ---------------------------------------------------------------------------


def connect_with_tls(
    initiator_node,
    traddr,
    trsvcid,
    nqn,
    expect_failure=False,
    keep_alive_tmo=None,
    ctrl_loss_tmo=None,
    reconnect_delay=None,
):
    """Connect to the NVMe-oF target using TLS.

    Doc step::

        nvme connect --transport tcp --traddr GATEWAY_IP \\
            --trsvcid 4420 --nqn SUBSYSTEM_NQN --tls

    Args:
        initiator_node: CephNode for the initiator.
        traddr (str): Gateway IP address.
        trsvcid (int|str): Listener port.
        nqn (str): Subsystem NQN.
        expect_failure (bool): When True, assert that the connect fails.
        keep_alive_tmo (int|None): ``--keep-alive-tmo`` value in seconds.
            When set, overrides the kernel NVMe driver default (~5 s) so the
            controller is not declared lost prematurely during a PSK rotation
            window.  Pass ``None`` (default) to omit the flag entirely.
        ctrl_loss_tmo (int|None): ``--ctrl-loss-tmo`` value in seconds.
            Controls how long the kernel driver keeps retrying before deleting
            the controller.  Use ``-1`` for infinite.  Pass ``None`` (default)
            to omit the flag.
        reconnect_delay (int|None): ``--reconnect-delay`` value in seconds.
            How frequently the driver retries within the ``ctrl_loss_tmo``
            window.  Pass ``None`` (default) to omit the flag.

    Returns:
        tuple: (stdout, stderr) on success; (None, str(exc)) on expected failure.

    Raises:
        AssertionError: If outcome does not match expect_failure.
    """
    cmd = (
        f"nvme connect --transport tcp "
        f"--traddr {traddr} --trsvcid {trsvcid} "
        f"--nqn {nqn} --tls"
    )
    if keep_alive_tmo is not None:
        cmd += f" --keep-alive-tmo {keep_alive_tmo}"
    if ctrl_loss_tmo is not None:
        cmd += f" --ctrl-loss-tmo {ctrl_loss_tmo}"
    if reconnect_delay is not None:
        cmd += f" --reconnect-delay {reconnect_delay}"
    LOG.info("connect_with_tls: %s (expect_failure=%s)", cmd, expect_failure)
    try:
        out, err = initiator_node.exec_command(cmd=cmd, sudo=True)
        if expect_failure:
            raise AssertionError(
                f"Expected TLS connect to {nqn} to fail, but it succeeded. "
                f"stdout={out!r} stderr={err!r}"
            )
        LOG.info("connect_with_tls: succeeded")
        return out, err
    except CommandFailed as exc:
        if not expect_failure:
            raise AssertionError(
                f"Expected TLS connect to {nqn} to succeed but got CommandFailed: {exc}"
            ) from exc
        LOG.info("connect_with_tls: correctly refused (expected): %s", exc)
        return None, str(exc)


def connect_error_is_stale_psk(exc):
    """Return True when a connect failed because the peer has an old host PSK.

    A gateway that just received ``host add`` applies the new key at once.
    The other gateways copy it from OMAP on ``state_update_interval_sec``.
    Until then ``nvme connect --tls`` fails with ``failed to write to
    nvme-fabrics device``. A closed listener is a different failure and must
    not be retried as key replication.

    Args:
        exc: Exception or message from ``nvme connect``.
    """
    if connect_failure_is_listener_down(exc):
        return False
    return "failed to write to nvme-fabrics" in str(exc).lower()


def connect_with_tls_until(
    initiator_node,
    traddr,
    trsvcid,
    nqn,
    timeout,
    interval=5,
    **connect_kwargs,
):
    """Retry ``nvme connect --tls`` until the peer gateway has the new host PSK.

    Args:
        initiator_node: CephNode for the initiator.
        traddr (str): Gateway IP address.
        trsvcid (int|str): Listener port.
        nqn (str): Subsystem NQN.
        timeout (int): Seconds to keep retrying a stale-PSK refusal.
        interval (int): Seconds between attempts.
        connect_kwargs: Forwarded to ``connect_with_tls``.

    Returns:
        tuple: (stdout, stderr) from the successful connect.

    Raises:
        AssertionError: The connect failed for another reason, or the stale-PSK
            refusal lasted through ``timeout``.
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            return connect_with_tls(
                initiator_node, traddr, trsvcid, nqn, **connect_kwargs
            )
        except AssertionError as exc:
            if not connect_error_is_stale_psk(exc) or time.monotonic() >= deadline:
                raise
            LOG.info(
                "connect_with_tls: %s still has the previous host PSK, "
                "retrying for up to %ss",
                traddr,
                timeout,
            )
            time.sleep(interval)


def nvmeof_daemons_serving(daemons):
    """Return True when every daemon is running and was not user-stopped.

    ``ceph orch daemon stop`` sets ``user_stopped`` and leaves the daemon
    stopped. A restart is only complete when every daemon is ``running``
    and ``user_stopped`` is false.

    Args:
        daemons (list): ``ceph orch ps --format json`` entries.

    Returns:
        bool
    """
    if not daemons:
        return False
    return all(
        d.get("status_desc") == "running" and not d.get("user_stopped") for d in daemons
    )


def nvmeof_daemons_restarted(before, after):
    """Return True when *after* is a new serving generation versus *before*.

    ``ceph orch daemon restart`` and ``ceph orch redeploy`` are asynchronous.
    The first ``orch ps`` after the command can still show the previous
    process as running. A restart counts only when every daemon is serving
    and the daemon name or ``started`` timestamp has changed.

    Args:
        before (list): ``ceph orch ps`` entries taken before the restart.
        after (list): Current ``ceph orch ps`` entries.

    Returns:
        bool
    """
    if not nvmeof_daemons_serving(after):
        return False
    before_started = {
        daemon.get("daemon_name"): daemon.get("started") for daemon in before or []
    }
    after_started = {
        daemon.get("daemon_name"): daemon.get("started") for daemon in after
    }
    return before_started != after_started


def connect_failure_is_listener_down(exc):
    """Return True when *exc* is a closed port, not an NVMe/TLS rejection.

    Args:
        exc: Exception or message from ``nvme connect``.
    """
    text = str(exc).lower()
    return any(marker in text for marker in _LISTENER_DOWN_MARKERS)


def assert_plain_connect_refused(initiator_node, traddr, trsvcid, nqn, context=""):
    """Assert a plain ``nvme connect`` (no ``--tls``) is refused by TLS.

    A successful connect means TLS enforcement is gone. ``connection refused``
    and the other listener-down markers mean the gateway port is not up; that
    is a test error, not evidence that TLS is enforced.

    Args:
        initiator_node: CephNode for the initiator.
        traddr (str): Gateway IP address.
        trsvcid (int|str): Listener port.
        nqn (str): Subsystem NQN.
        context (str): Short label included in assertion messages.

    Raises:
        AssertionError: Connect succeeded, or failed because the listener is down.
    """
    label = f"{context}: " if context else ""
    cmd = (
        f"nvme connect --transport tcp --traddr {traddr} "
        f"--trsvcid {trsvcid} --nqn {nqn}"
    )
    LOG.info("%splain connect (no --tls) must be refused: %s", label, cmd)
    try:
        initiator_node.exec_command(cmd=cmd, sudo=True)
    except CommandFailed as exc:
        if connect_failure_is_listener_down(exc):
            raise AssertionError(
                f"{label}plain connect failed because the listener is down, "
                f"not because TLS refused it: {exc}"
            ) from exc
        LOG.info("%splain connect refused by TLS: %s", label, exc)
        return
    raise AssertionError(f"{label}plain connect succeeded — TLS enforcement lost")


def disconnect_nqn(initiator_node, nqn):
    """Disconnect the initiator from *nqn*.

    Args:
        initiator_node: CephNode for the initiator.
        nqn (str): Subsystem NQN to disconnect.
    """
    LOG.info("disconnect_nqn: nvme disconnect --nqn %s", nqn)
    try:
        initiator_node.exec_command(cmd=f"nvme disconnect --nqn {nqn}", sudo=True)
    except CommandFailed as exc:
        LOG.warning("disconnect_nqn: %s (ignored)", exc)


# ---------------------------------------------------------------------------
# PSK key rotation helper
# ---------------------------------------------------------------------------


def rotate_psk(
    initiator_node,
    gateway,
    nqn,
    hostnqn,
    gw_nodes,
    new_key_path=DEFAULT_ENCRYPTION_KEY_PATH,
    trsvcid=4420,
):
    """Rotate the TLS-PSK for one subsystem/host pair.

    Steps:
        1. Generate new openssl RSA key; distribute to all GW nodes; apply spec.
        2. Re-run nvme gen-tls-key on initiator → new KEY_DATA.
        3. Load nvme-tcp module (idempotent).
        4. Insert new key into kernel keyring.
        5. Re-add host with new PSK on gateway.

    Args:
        initiator_node: CephNode for the initiator.
        gateway: NVMeGateway instance.
        nqn (str): Subsystem NQN.
        hostnqn (str): Initiator host NQN.
        gw_nodes (list): All CephNode instances for GW nodes.
        new_key_path (str): Path for the new RSA key on GW nodes.
        trsvcid (int): Listener port.

    Returns:
        str: New key_data.
    """
    # Step 1 — new openssl RSA key on GW1, distribute to rest
    LOG.info("rotate_psk: generating new RSA key on %s", gw_nodes[0].hostname)
    gw_nodes[0].exec_command(
        cmd=(
            f"openssl req -newkey rsa:512 -noenc -noout "
            f"-keyout {new_key_path} -batch 2>/dev/null"
        ),
        sudo=True,
    )
    out, _ = gw_nodes[0].exec_command(cmd=f"cat {new_key_path}", sudo=True)
    pem = out.strip()
    for node in gw_nodes[1:]:
        node.exec_command(
            cmd=(
                f"cat > {new_key_path} << 'TLSPSK_EOF'\n"
                f"{pem}\n"
                f"TLSPSK_EOF\n"
                f"chmod 600 {new_key_path}"
            ),
            sudo=True,
        )

    # Steps 2-4 — new TLS key on initiator
    new_key_data = generate_tls_key(initiator_node)
    load_nvme_tcp_module(initiator_node)
    insert_tls_key(initiator_node, hostnqn, nqn, new_key_data, identity=0)

    # Step 5 — update host PSK on gateway
    add_host_with_psk(gateway, nqn, hostnqn, new_key_data)

    LOG.info("rotate_psk: rotation complete for %s / %s", nqn, hostnqn)
    return new_key_data
