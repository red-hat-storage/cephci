"""
IBMCEPH-11530 lifecycle cases beyond the basic disable/re-enable toggle.

TC-VS12 — second toggle cycle; daemon count stable
TC-VS13 — multi-daemon Virtual_Server conf agrees
TC-VS14 — invalid enable_virtual_server value recovers
TC-VS15 — orch redeploy preserves disabled setting

Select with suite config ``tc: VS12|VS13|VS14|VS15``.
"""

from cli.exceptions import OperationFailedError
from tests.nfs.regional_mount import virtual_server as vs
from utility.log import Log

log = Log(__name__)

VALID_TCS = ("VS12", "VS13", "VS14", "VS15")


def _assert_spec_enabled(client, nfs_name, expected):
    value = vs.get_enable_virtual_server_from_spec(client, nfs_name)
    expected_bool = bool(expected)
    is_true = value is True or str(value).lower() == "true"
    if expected_bool and not is_true:
        raise OperationFailedError(
            f"nfs.{nfs_name} enable_virtual_server={value!r}, expected true"
        )
    if not expected_bool and is_true:
        raise OperationFailedError(
            f"nfs.{nfs_name} enable_virtual_server={value!r}, expected not true"
        )
    log.info(
        "nfs.%s enable_virtual_server=%r (expected true=%s)",
        nfs_name,
        value,
        expected_bool,
    )


def _mount_io_unmount(route, export_name, mount_path, version, port, sudo):
    vs.mount_nfs_for_route(route, export_name, mount_path, version, port, sudo=sudo)
    vs.run_nfs_io_for_route(route, mount_path, sudo=sudo)
    vs.unmount_nfs_route(route["client"], mount_path)


def _wait_for_nfs_daemons(client, nfs_name, min_count=1, timeout=180):
    from time import sleep, time

    end = time() + timeout
    while time() < end:
        daemons = vs.list_running_nfs_daemons(client, nfs_name)
        if len(daemons) >= min_count:
            return daemons
        sleep(10)
    raise OperationFailedError(
        f"Timed out waiting for >= {min_count} running nfs.{nfs_name} daemon(s)"
    )


def _setup_vs_cluster(ceph_cluster, config):
    """Create VS NFS + one export; return context dict."""
    clients = vs.get_clients(ceph_cluster, {**config, "clients": 1})
    client = clients[0]
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    nfs_node = nfs_nodes[0]
    # Prefer all NFS role hosts so VS13 can see multi-daemon conf.
    nfs_server = [n.hostname for n in nfs_nodes] or [
        vs.get_nfs_server_hostname(ceph_cluster)
    ]
    names = vs.default_nfs_names()
    version = config.get("nfs_version", "4.2")
    port = config.get("port", "2049")
    export_name = vs.export_name_for_index(names["nfs_export"], 0, single_export=True)
    sudo = vs.io_sudo_from_config(config)

    routes = vs.discover_and_verify_routes(nfs_node, client, config)

    # Reuse clusters often already have nfs.<name> without enable_virtual_server
    # in the orch spec. Delete first so create --enable-virtual-server applies.
    log.info("Pre-clean existing NFS cluster %s (if any)", names["nfs_name"])
    vs.cleanup_virtual_server(
        clients,
        names["nfs_name"],
        names["nfs_export"],
        nfs_nodes=nfs_nodes,
        fs_name=names["fs_name"],
    )

    vs.setup_virtual_server_exports(
        ceph_cluster,
        clients,
        nfs_server,
        config,
        single_export=True,
    )
    _wait_for_nfs_daemons(client, names["nfs_name"], min_count=1)

    # If create reused an old service somehow, force the flag + reconfig.
    flag = vs.get_enable_virtual_server_from_spec(client, names["nfs_name"])
    if flag is not True and str(flag).lower() != "true":
        log.warning(
            "enable_virtual_server=%r after create; applying true + reconfig",
            flag,
        )
        vs.set_virtual_server_enabled(client, names["nfs_name"], True)

    _assert_spec_enabled(client, names["nfs_name"], True)
    vs.assert_virtual_server_conf(nfs_nodes, names["nfs_name"], True)

    return {
        "clients": clients,
        "client": client,
        "nfs_nodes": nfs_nodes,
        "nfs_node": nfs_node,
        "names": names,
        "export_name": export_name,
        "routes": routes,
        "version": version,
        "port": port,
        "sudo": sudo,
    }


def _cleanup(ctx):
    names = ctx["names"]
    vs.cleanup_virtual_server(
        ctx["clients"],
        names["nfs_name"],
        names["nfs_export"],
        nfs_nodes=ctx["nfs_nodes"],
        fs_name=names["fs_name"],
    )


def _toggle_off_on(ctx, label, expected_count):
    """One disable → re-enable cycle with daemon-count checks (VS8/VS10 style)."""
    client = ctx["client"]
    nfs_name = ctx["names"]["nfs_name"]
    nfs_nodes = ctx["nfs_nodes"]
    export_name = ctx["export_name"]
    route0 = ctx["routes"][0]
    version, port, sudo = ctx["version"], ctx["port"], ctx["sudo"]

    log.info("%s: disable virtual server", label)
    vs.set_virtual_server_enabled(client, nfs_name, False)
    _assert_spec_enabled(client, nfs_name, False)
    vs.assert_virtual_server_conf(nfs_nodes, nfs_name, False)
    vs.assert_cluster_and_export_preserved(client, nfs_name, export_name)
    vs.assert_nfs_daemon_count(client, nfs_name, expected_count, label=f"{label} off")

    mount_off = vs.mount_path_for_route(
        ctx["names"]["nfs_mount"], f"{label}_off_{route0['route_id']}"
    )
    _mount_io_unmount(route0, export_name, mount_off, version, port, sudo)

    log.info("%s: re-enable virtual server", label)
    vs.set_virtual_server_enabled(client, nfs_name, True)
    _assert_spec_enabled(client, nfs_name, True)
    vs.assert_virtual_server_conf(nfs_nodes, nfs_name, True)
    vs.assert_cluster_and_export_preserved(client, nfs_name, export_name)
    vs.assert_nfs_daemon_count(client, nfs_name, expected_count, label=f"{label} on")

    for route in ctx["routes"][:2]:
        mount_on = vs.mount_path_for_route(
            ctx["names"]["nfs_mount"], f"{label}_on_{route['route_id']}"
        )
        _mount_io_unmount(route, export_name, mount_on, version, port, sudo)


def run_vs12(ctx):
    """TC-VS12: two full toggle cycles; daemon count stable."""
    client = ctx["client"]
    nfs_name = ctx["names"]["nfs_name"]
    expected = vs.expected_nfs_daemon_count(client, nfs_name)
    if expected < 1:
        raise OperationFailedError("No NFS daemons before VS12 toggle cycles")
    vs.assert_nfs_daemon_count(client, nfs_name, expected, label="VS12 baseline")

    _toggle_off_on(ctx, "VS12-cycle1", expected)
    _toggle_off_on(ctx, "VS12-cycle2", expected)

    vs.assert_cluster_and_export_preserved(
        client, nfs_name, ctx["export_name"]
    )
    vs.assert_nfs_daemon_count(client, nfs_name, expected, label="VS12 final")
    log.info("TC-VS12 passed: two toggle cycles; %d daemon(s) stable", expected)


def run_vs13(ctx):
    """TC-VS13: every NFS daemon host renders the same Virtual_Server value."""
    client = ctx["client"]
    nfs_name = ctx["names"]["nfs_name"]
    nfs_nodes = ctx["nfs_nodes"]
    daemons = vs.list_running_nfs_daemons(client, nfs_name)
    if len(daemons) < 2:
        log.warning(
            "TC-VS13 SKIP: need >=2 running NFS daemons; found %d. "
            "Lab has a single NFS placement — recorded as skip per plan.",
            len(daemons),
        )
        return

    log.info("TC-VS13: assert Virtual_Server=true on all %d daemons", len(daemons))
    vs.assert_virtual_server_conf(nfs_nodes, nfs_name, True)

    vs.set_virtual_server_enabled(client, nfs_name, False)
    _assert_spec_enabled(client, nfs_name, False)
    log.info("TC-VS13: assert Virtual_Server=false on all daemons after disable")
    vs.assert_virtual_server_conf(nfs_nodes, nfs_name, False)

    vs.set_virtual_server_enabled(client, nfs_name, True)
    vs.assert_virtual_server_conf(nfs_nodes, nfs_name, True)
    log.info("TC-VS13 passed: multi-daemon conf agreed true→false→true")


def run_vs14(ctx):
    """TC-VS14: orch accepts bad value; restore recovers mountable export."""
    client = ctx["client"]
    nfs_name = ctx["names"]["nfs_name"]
    nfs_nodes = ctx["nfs_nodes"]
    export_name = ctx["export_name"]
    route0 = ctx["routes"][0]
    version, port, sudo = ctx["version"], ctx["port"], ctx["sudo"]

    log.info("TC-VS14: apply enable_virtual_server='maybe' (expect orch accept)")
    _, out, err = vs.apply_enable_virtual_server_raw(
        client, nfs_name, "maybe", check_ec=False
    )
    combined = f"{out}\n{err}".lower()
    if "error" in combined and "traceback" in combined:
        raise OperationFailedError(
            f"Orch apply rejected 'maybe' unexpectedly: out={out!r} err={err!r}"
        )
    log.info("TC-VS14: orch apply accepted bad value (expected)")

    log.info("TC-VS14: reconfig after bad value (Ganesha-side failure expected OK)")
    try:
        vs.reconfig_nfs_daemons(client, nfs_name)
    except Exception as exc:
        log.info("TC-VS14: reconfig after 'maybe' raised (acceptable): %s", exc)

    log.info("TC-VS14: restore enable_virtual_server=true + reconfig")
    vs.set_virtual_server_enabled(client, nfs_name, True)
    _assert_spec_enabled(client, nfs_name, True)
    vs.assert_virtual_server_conf(nfs_nodes, nfs_name, True)
    vs.assert_cluster_and_export_preserved(client, nfs_name, export_name)

    mount_path = vs.mount_path_for_route(
        ctx["names"]["nfs_mount"], f"vs14_recover_{route0['route_id']}"
    )
    _mount_io_unmount(route0, export_name, mount_path, version, port, sudo)
    log.info("TC-VS14 passed: bad value recovered; export mountable")


def run_vs15(ctx):
    """TC-VS15: redeploy while disabled keeps Virtual_Server=false + mount works."""
    client = ctx["client"]
    nfs_name = ctx["names"]["nfs_name"]
    nfs_nodes = ctx["nfs_nodes"]
    export_name = ctx["export_name"]
    route0 = ctx["routes"][0]
    version, port, sudo = ctx["version"], ctx["port"], ctx["sudo"]

    log.info("TC-VS15: disable virtual server before redeploy")
    vs.set_virtual_server_enabled(client, nfs_name, False)
    _assert_spec_enabled(client, nfs_name, False)
    vs.assert_virtual_server_conf(nfs_nodes, nfs_name, False)

    log.info("TC-VS15: ceph orch redeploy nfs.%s", nfs_name)
    vs.redeploy_nfs_service(client, nfs_name)

    _assert_spec_enabled(client, nfs_name, False)
    vs.assert_virtual_server_conf(nfs_nodes, nfs_name, False)
    vs.assert_cluster_and_export_preserved(client, nfs_name, export_name)

    mount_path = vs.mount_path_for_route(
        ctx["names"]["nfs_mount"], f"vs15_redeploy_{route0['route_id']}"
    )
    _mount_io_unmount(route0, export_name, mount_path, version, port, sudo)
    log.info("TC-VS15 passed: redeploy preserved disable; export mountable")


_HANDLERS = {
    "VS12": run_vs12,
    "VS13": run_vs13,
    "VS14": run_vs14,
    "VS15": run_vs15,
}


def run(ceph_cluster, **kw):
    """
    Scenario 7 — IBMCEPH-11530 lifecycle (TC-VS12 … TC-VS15).

    Suite config must set ``tc`` to one of VS12, VS13, VS14, VS15.
    """
    config = kw.get("config") or {}
    tc = str(config.get("tc") or "").upper()
    if tc not in VALID_TCS:
        log.error("config.tc must be one of %s; got %r", VALID_TCS, config.get("tc"))
        return 1

    ctx = None
    try:
        ctx = _setup_vs_cluster(ceph_cluster, config)
        _HANDLERS[tc](ctx)
        log.info("Scenario 7 / %s passed", tc)
        return 0
    except Exception as exc:
        log.error("Scenario 7 / %s failed: %s", tc, exc)
        return 1
    finally:
        if ctx:
            _cleanup(ctx)
