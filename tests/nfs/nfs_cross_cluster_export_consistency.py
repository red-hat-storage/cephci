"""
NFS cross-cluster export consistency (IBMCEPHQE-25297 / IBMCEPH-11665 / BZ 2374369).

Problem
-------
A customer exported the same CephFS subvolume path on two independent NFS-Ganesha
clusters (``nfs1`` on one host and ``nfsganesha`` on another). Clients mounting
each cluster saw inconsistent directory listings: partial ``ls`` output, ``mkdir:
File exists`` for directories not shown in the listing, and stale file handles
after cross-cluster deletes.

Solution
--------
Ceph fixed metadata propagation when one subvolume path is exported through
multiple NFS clusters (IBM Ceph 9.0 / RHCS builds containing the BZ 2374369
fix). This test guards that regression by exercising create, list, and delete
across two clusters on the same subvolume.

Test flow
---------
1. Ensure the NFS mgr module is enabled, create an isolated subvolume, and
   create two NFS clusters on separate NFS nodes (when run in tier0 sanity,
   earlier NFS tests already enable the mgr module).
2. Poll each cluster until Ganesha daemons and ``ceph nfs cluster info`` backends
   are ready (:func:`wait_for_nfs_cluster_backend_endpoint`).
3. Export the same subvolume path on both clusters with different pseudo paths.
4. Mount cluster A on client A and cluster B on client B
   (:func:`mount_nfs_export_with_wait`).
5. Phase 1 — client A creates files and directories; client B must list all
   entries; ``mkdir`` on existing dirs on cluster B must fail with "exists".
6. Phase 2 — client B deletes directories; client A must see updated listings
   and must not retain stale handles to removed paths.
7. Phase 3 — reverse direction: client B creates; client A validates listing
   and delete visibility.
8. Cleanup: unmount clients, delete exports, delete clusters, remove subvolume.

Requirements
------------
- At least two NFS nodes (``nfs`` label) and two clients.
- Suite config: ``nfs_version`` (default 4.1), ``clients: 2``.
- Global conf: ``conf/tentacle/nfs/1admin-7node-3client.yaml`` (or equivalent).
"""

import traceback
from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.nfs_operations import (
    assert_mount_path_accessible,
    cleanup_nfs_cross_cluster_resources,
    create_files_and_dirs_on_mount,
    create_isolated_subvolume_and_getpath,
    create_nfs_export_for_path,
    list_mount_entry_names,
    mount_nfs_export_with_wait,
    nfs_log_parser,
    wait_for_mount_listing,
    wait_for_nfs_cluster_backend_endpoint,
)
from utility.log import Log

log = Log(__name__)

FS_NAME = "cephfs"
SUBVOL_NAME = "cross_cluster_sv"
CLUSTER_A = "nfs1"
CLUSTER_B = "nfsganesha"
EXPORT_A = "/nfs1"
EXPORT_B = "/ganesha1"
MOUNT_A = "/mnt/nfs_cross_a"
MOUNT_B = "/mnt/nfs_cross_b"
NFS_READY_TIMEOUT = 300

# IBMCEPH-11665 / BZ 2374369 — customer-visible symptoms for log comparison.
JIRA_SYMPTOM_PARTIAL_LS = "partial directory listing on peer NFS cluster"
JIRA_SYMPTOM_MKDIR_EXISTS = "mkdir: File exists for directories not shown in ls"
JIRA_SYMPTOM_STALE_FH = "stale file handle after cross-cluster delete"


def _log_mount_listings(phase, client_a, client_b):
    """
    Log ``ls`` output on both cross-cluster mounts for IBMCEPH-11665 triage.

    Compares what client A sees on cluster A vs client B on cluster B at each
    phase. Partial or mismatched listings indicate the JIRA regression.
    """
    listing_a = sorted(list_mount_entry_names(client_a, MOUNT_A))
    listing_b = sorted(list_mount_entry_names(client_b, MOUNT_B))
    log.info(
        "Cross-cluster listing snapshot [%s]: %s@%s=%s | %s@%s=%s",
        phase,
        client_a.hostname,
        MOUNT_A,
        listing_a,
        client_b.hostname,
        MOUNT_B,
        listing_b,
    )


def _log_jira_symptom_checklist(exc):
    """
    Log IBMCEPH-11665 symptom checklist alongside the actual failure.

    Lets Jenkins logs be compared to customer reports without re-reading JIRA.
    """
    log.error(
        "IBMCEPH-11665 / BZ 2374369 symptom checklist for log comparison: "
        "1) %s; 2) %s; 3) %s. Actual failure: %s",
        JIRA_SYMPTOM_PARTIAL_LS,
        JIRA_SYMPTOM_MKDIR_EXISTS,
        JIRA_SYMPTOM_STALE_FH,
        exc,
    )


def _collect_nfs_ganesha_diagnostics(client, nfs_nodes, cluster_names):
    """
    Collect orch status, container logs, and ganesha.conf after failure/cleanup.

    Mirrors the ``nfs_log_parser`` pattern used in ``cleanup_cluster`` and other
    NFS tests for post-mortem analysis.
    """
    for nfs_node, cluster_name in zip(nfs_nodes, cluster_names):
        log.info(
            "Collecting NFS-Ganesha diagnostics for cluster %s on %s",
            cluster_name,
            nfs_node.hostname,
        )
        nfs_log_parser(
            client=client,
            nfs_node=nfs_node,
            nfs_name=cluster_name,
        )


def run(ceph_cluster, **kw):
    """
    Run NFS cross-cluster export consistency validation.

    See module docstring for problem background, expected fix, and phase-by-phase
    flow. Returns 0 on pass, 1 on failure.
    """
    config = kw.get("config", {})
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")
    port = config.get("port", "2049")
    nfs_version = config.get("nfs_version", "4.1")
    no_clients = int(config.get("clients", 2))
    nfs_ready_timeout = int(config.get("nfs_ready_timeout", NFS_READY_TIMEOUT))

    if len(nfs_nodes) < 2:
        raise ConfigError("Test requires at least 2 NFS nodes for two clusters")
    if no_clients > len(clients):
        raise ConfigError("Test requires at least 2 clients")

    client_a, client_b = clients[0], clients[1]
    nfs_host_a = nfs_nodes[0].hostname
    nfs_host_b = nfs_nodes[1].hostname
    installer_node = ceph_cluster.get_nodes("installer")[0]
    clusters_created = []

    log.info(
        "Starting IBMCEPH-11665 cross-cluster export test: "
        "cluster_a=%s@%s export=%s, cluster_b=%s@%s export=%s, "
        "clients=%s/%s, nfs_version=%s",
        CLUSTER_A,
        nfs_host_a,
        EXPORT_A,
        CLUSTER_B,
        nfs_host_b,
        EXPORT_B,
        client_a.hostname,
        client_b.hostname,
        nfs_version,
    )

    try:
        log.info("Enabling NFS mgr module (idempotent if already enabled by suite)")
        Ceph(client_a).mgr.module.enable(module="nfs", force=True)
        sleep(3)

        subvol_path = create_isolated_subvolume_and_getpath(
            client_a,
            FS_NAME,
            SUBVOL_NAME,
            ceph_cluster=ceph_cluster,
        )
        log.info("Shared subvolume path for both exports: %s", subvol_path)

        log.info("Creating NFS cluster %s on %s", CLUSTER_A, nfs_host_a)
        Ceph(client_a).nfs.cluster.create(name=CLUSTER_A, nfs_server=[nfs_host_a])
        clusters_created.append(CLUSTER_A)
        ip_a, port_a = wait_for_nfs_cluster_backend_endpoint(
            client_a,
            installer_node,
            CLUSTER_A,
            timeout=nfs_ready_timeout,
        )

        log.info("Creating NFS cluster %s on %s", CLUSTER_B, nfs_host_b)
        Ceph(client_a).nfs.cluster.create(name=CLUSTER_B, nfs_server=[nfs_host_b])
        clusters_created.append(CLUSTER_B)
        ip_b, port_b = wait_for_nfs_cluster_backend_endpoint(
            client_a,
            installer_node,
            CLUSTER_B,
            timeout=nfs_ready_timeout,
        )

        create_nfs_export_for_path(client_a, FS_NAME, CLUSTER_A, EXPORT_A, subvol_path)
        create_nfs_export_for_path(client_a, FS_NAME, CLUSTER_B, EXPORT_B, subvol_path)

        log.info(
            "Cluster backend endpoints: %s=%s:%s, %s=%s:%s",
            CLUSTER_A,
            ip_a,
            port_a or port,
            CLUSTER_B,
            ip_b,
            port_b or port,
        )

        mount_nfs_export_with_wait(
            client_a,
            ip_a,
            port_a or port,
            EXPORT_A,
            MOUNT_A,
            nfs_version,
            installer_node=installer_node,
            nfs_name=CLUSTER_A,
            nfs_wait_timeout=nfs_ready_timeout,
        )
        mount_nfs_export_with_wait(
            client_b,
            ip_b,
            port_b or port,
            EXPORT_B,
            MOUNT_B,
            nfs_version,
            installer_node=installer_node,
            nfs_name=CLUSTER_B,
            nfs_wait_timeout=nfs_ready_timeout,
        )
        _log_mount_listings("after_mount", client_a, client_b)

        # Phase 1: client A creates; client B must see all entries (IBMCEPH-11665 repro)
        log.info("Phase 1: create on cluster A; peer listing on cluster B must match")
        created_a = create_files_and_dirs_on_mount(client_a, MOUNT_A, "c1")
        log.info("Phase 1 created on %s: %s", MOUNT_A, created_a)
        wait_for_mount_listing(client_b, MOUNT_B, created_a)
        _log_mount_listings("phase1_after_create", client_a, client_b)

        for dir_name in ["c1_dir1", "c1_dir2", "c1_dir3"]:
            out, rc = client_b.exec_command(
                sudo=True,
                cmd=f"mkdir {MOUNT_B}/{dir_name}",
                check_ec=False,
            )
            log.info(
                "Phase 1 mkdir probe on %s/%s: rc=%s out=%s (JIRA symptom: %s)",
                MOUNT_B,
                dir_name,
                rc,
                out.strip(),
                JIRA_SYMPTOM_MKDIR_EXISTS,
            )
            if rc == 0:
                _log_mount_listings(
                    "phase1_mkdir_should_have_failed", client_a, client_b
                )
                raise OperationFailedError(
                    f"mkdir {dir_name} on cluster B should fail (already exists), "
                    "but succeeded — matches IBMCEPH-11665 if dir missing from ls"
                )
            if "exists" not in out.lower() and "exist" not in out.lower():
                log.warning(
                    "mkdir returned rc=%s without 'exists' message: %s", rc, out
                )

        # Phase 2: client B deletes dirs; client A must observe deletes (no stale FH)
        log.info("Phase 2: delete on cluster B; cluster A must see updated listing")
        client_b.exec_command(
            sudo=True, cmd=f"rm -rf {MOUNT_B}/c1_dir3 {MOUNT_B}/c1_dir2"
        )
        remaining_a = ["c1_g1", "c1_g2", "c1_g3", "c1_dir1"]
        wait_for_mount_listing(client_a, MOUNT_A, remaining_a)
        _log_mount_listings("phase2_after_delete", client_a, client_b)
        assert_mount_path_accessible(client_a, MOUNT_A, "c1_dir2", should_exist=False)
        assert_mount_path_accessible(client_a, MOUNT_A, "c1_dir3", should_exist=False)
        log.info(
            "Phase 2 stale-FH check passed for c1_dir2/c1_dir3 on %s (JIRA: %s)",
            MOUNT_A,
            JIRA_SYMPTOM_STALE_FH,
        )

        # Phase 3: reverse direction — client B creates; client A validates
        log.info(
            "Phase 3: reverse create/delete — cluster B creates, cluster A validates"
        )
        created_b = create_files_and_dirs_on_mount(client_b, MOUNT_B, "c2")
        # c1_dir2/c1_dir3 were removed in phase 2; do not expect them in phase 3.
        wait_for_mount_listing(client_a, MOUNT_A, remaining_a + created_b)
        _log_mount_listings("phase3_after_create", client_a, client_b)

        client_b.exec_command(
            sudo=True, cmd=f"rm -rf {MOUNT_B}/c2_dir2 {MOUNT_B}/c2_dir3"
        )
        remaining_both = [
            "c1_g1",
            "c1_g2",
            "c1_g3",
            "c1_dir1",
            "c2_g1",
            "c2_g2",
            "c2_g3",
            "c2_dir1",
        ]
        wait_for_mount_listing(client_a, MOUNT_A, remaining_both)
        _log_mount_listings("phase3_after_delete", client_a, client_b)
        assert_mount_path_accessible(client_a, MOUNT_A, "c2_dir2", should_exist=False)

        log.info("NFS cross-cluster export consistency test passed (IBMCEPH-11665)")
        return 0

    except Exception as exc:
        log.error("NFS cross-cluster export consistency test failed: %s", exc)
        log.error(traceback.format_exc())
        _log_jira_symptom_checklist(exc)
        try:
            _log_mount_listings("failure", client_a, client_b)
        except Exception as listing_exc:
            log.warning("Could not capture mount listings on failure: %s", listing_exc)
        return 1

    finally:
        cleanup_nfs_cross_cluster_resources(
            client_a,
            [(client_a, MOUNT_A), (client_b, MOUNT_B)],
            [(CLUSTER_A, EXPORT_A), (CLUSTER_B, EXPORT_B)],
            clusters_created,
            fs_name=FS_NAME,
            subvol_name=SUBVOL_NAME,
        )
        if clusters_created:
            log.info("Post-cleanup NFS-Ganesha diagnostic collection")
            _collect_nfs_ganesha_diagnostics(
                client_a,
                nfs_nodes[: len(clusters_created)],
                clusters_created,
            )
