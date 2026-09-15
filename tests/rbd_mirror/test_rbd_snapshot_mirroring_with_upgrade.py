import random
import time

from ceph.parallel import parallel
from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.workflows.cephx_rotation import post_upgrade_rbd_cephx_workflow
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.execute import execute
from ceph.rbd.workflows.rbd_mirror import check_image_mirror_status
from ceph.rbd.workflows.snap_scheduling import (
    cleanup_upgrade_io_mounts,
    prepare_upgrade_io_mounts,
    run_io_on_existing_mount,
    run_io_on_existing_mounts,
)
from utility.log import Log

log = Log(__name__)


def verify_mirror_states(pool_types, rbd_primary, rbd_secondary, stage_label, **kw):
    """Verify mirror image states for all pool types on both clusters.

    Checks that primary images are 'up+stopped' and secondary images are
    'up+replaying'. Retries with a 60s interval up to 300s.

    Args:
        pool_types: List of pool type config keys to verify.
        rbd_primary: RBD object for the primary cluster.
        rbd_secondary: RBD object for the secondary cluster.
        stage_label: Human-readable label for log messages (e.g. "post-primary-upgrade").
        **kw: Additional keyword arguments passed to check_image_mirror_status.

    Returns:
        0 on success, 1 on failure.
    """
    log.info(f"Verify mirror image states: {stage_label}")
    retry_interval = 60
    max_wait = 300

    for pool_type in pool_types:
        # --- Primary: expect up+stopped ---
        log.info(
            f"[{stage_label}] Checking primary image state is up+stopped "
            f"for {pool_type}"
        )
        elapsed = 0
        primary_ok = False
        while elapsed <= max_wait:
            rc = check_image_mirror_status(
                status="up+stopped",
                pool_type=pool_type,
                rbd=rbd_primary,
                **kw,
            )
            if rc == 0:
                log.info(
                    f"[{stage_label}] Primary image state is up+stopped "
                    f"for {pool_type} (elapsed {elapsed}s)"
                )
                primary_ok = True
                break
            log.info(
                f"[{stage_label}] Primary image state not yet up+stopped "
                f"for {pool_type} (elapsed {elapsed}s), "
                f"retrying in {retry_interval}s ..."
            )
            time.sleep(retry_interval)
            elapsed += retry_interval

        if not primary_ok:
            log.error(
                f"[{stage_label}] Primary mirror image state is not "
                f"up+stopped after {max_wait}s for {pool_type}"
            )
            return 1

        # --- Secondary: expect up+replaying ---
        if rbd_secondary:
            log.info(
                f"[{stage_label}] Checking secondary image state is "
                f"up+replaying for {pool_type}"
            )
            elapsed = 0
            secondary_ok = False
            while elapsed <= max_wait:
                rc = check_image_mirror_status(
                    status="up+replaying",
                    pool_type=pool_type,
                    rbd=rbd_secondary,
                    **kw,
                )
                if rc == 0:
                    log.info(
                        f"[{stage_label}] Secondary image state is "
                        f"up+replaying for {pool_type} (elapsed {elapsed}s)"
                    )
                    secondary_ok = True
                    break
                log.info(
                    f"[{stage_label}] Secondary image state not yet "
                    f"up+replaying for {pool_type} (elapsed {elapsed}s), "
                    f"retrying in {retry_interval}s ..."
                )
                time.sleep(retry_interval)
                elapsed += retry_interval

            if not secondary_ok:
                log.error(
                    f"[{stage_label}] Secondary mirror image state is not "
                    f"up+replaying after {max_wait}s for {pool_type}"
                )
                return 1

    log.info(
        f"[{stage_label}] Mirror state verification successful: "
        "primary=up+stopped, secondary=up+replaying"
    )
    return 0


def test_cluster_upgrade(target_cluster, target_client, **kw):
    """Upgrade a cluster while running fio on existing primary mounts.

    IO uses already-mapped/mounted primary RBD filesystems from
    kw['io_contexts']. Snapshot schedule verification is intentionally NOT
    run inside the parallel upgrade workers so snap-interval sleeps do not
    serialize with the rolling upgrade. Snap verification remains in the
    surrounding test stages.

    Args:
        target_cluster: The ceph cluster object to upgrade.
        target_client: The client node on the target cluster (osd flags / health).
        **kw: Must include io_contexts (list of persistent mount contexts),
             config, etc. kw['ceph_cluster'] must already be set to
             target_cluster before calling this function.
    """
    try:
        mon_node = target_cluster.get_nodes("installer")[0]
        rc = target_cluster.check_health(
            kw["config"]["installed_version"],
            client=mon_node,
            timeout=300,
        )

        if rc != 0:
            log.error("Ceph health not OK")
            return 1

        target_client.exec_command(cmd="ceph osd set noout", sudo=True)
        target_client.exec_command(cmd="ceph osd set noscrub", sudo=True)
        target_client.exec_command(cmd="ceph osd set nodeep-scrub", sudo=True)

        io_contexts = kw.get("io_contexts") or []
        with parallel() as p:
            p.spawn(
                execute,
                mod_file_name="tests.cephadm.test_cephadm_upgrade",
                args=kw,
                test_name="Upgrade cluster",
                raise_exception=True,
            )
            for mount_ctx in io_contexts:
                log.info(
                    "Reusing existing RBD mount during cluster upgrade: "
                    f"device={mount_ctx['device_name']} "
                    f"mount={mount_ctx['mount_path']} "
                    f"file={mount_ctx['mount_path']}/file_during_upgrade"
                )
                p.spawn(
                    run_io_on_existing_mount,
                    mount_ctx,
                    file_name="file_during_upgrade",
                    verify_snap_schedule=False,
                    raise_exception=True,
                )

    except Exception as e:
        log.error(f"Cluster upgrade failed with error {e}")
        return 1
    finally:
        target_client.exec_command(cmd="ceph osd unset noout", sudo=True)
        target_client.exec_command(cmd="ceph osd unset noscrub", sudo=True)
        target_client.exec_command(cmd="ceph osd unset nodeep-scrub", sudo=True)
    return 0


def run(**kw):
    """CEPH-83574895 - Configure two-way rbd-mirror on Stand alone
    CEPH cluster on image (with replicated and ec pools)with snapshot
    based mirroring and perform rolling upgrade (real customer upgrade path).
    Pre-requisites :
    We need atleast one client node with ceph-common, fio and rbd-nbd packages,
    conf and keyring files in both clusters with snapshot based RBD mirroring
    enabled between the clusters.
    kw:
        clusters:
            ceph-rbd1:
            config:
                rep_pool_config:
                num_pools: 1
                num_images: 5
                size: 10G
                mode: image # compulsory argument if mirroring needs to be setup
                mirrormode: snapshot
                snap_schedule_levels:
                    - image
                snap_schedule_intervals: #one value for each level specified above
                    - 5m
                io_percentage: 30 #percentage of space in each image to be filled
                ec_pool_config:
                num_pools: 1
                num_images: 5
                mode: image # compulsory argument if mirroring needs to be setup
                mirrormode: snapshot
                snap_schedule_levels:
                    - image
                snap_schedule_intervals:
                    - 5m
                io_percentage: 30
                command: start
                service: upgrade
                verify_cluster_health: true
            ceph-rbd2:
            config:
                rep_pool_config:
                num_pools: 1
                num_images: 5
                size: 10G
                mode: image # compulsory argument if mirroring needs to be setup
                mirrormode: snapshot
                snap_schedule_levels:
                    - image
                snap_schedule_intervals: #one value for each level specified above
                    - 5m
                io_percentage: 30 #percentage of space in each image to be filled
                ec_pool_config:
                num_pools: 1
                num_images: 5
                mode: image # compulsory argument if mirroring needs to be setup
                mirrormode: snapshot
                snap_schedule_levels:
                    - image
                snap_schedule_intervals:
                    - 5m
                io_percentage: 30
                command: start
                service: upgrade
                verify_cluster_health: true
    Test Case Flow (Rolling Upgrade - Real Customer Upgrade Path)
    1. Bootstrap two CEPH clusters and setup snapshot based mirroring
    2. Create pools and images, enable snapshot mirroring, schedule snapshots
    3. Map/mount primary images once, run IOs and verify mirroring (pre-upgrade)
    4. Upgrade primary cluster while fio continues on existing mounts
    5. Verify mirroring works across mixed-version clusters
    6. Run IOs on the same mounts after primary upgrade
    7. Upgrade secondary cluster while primary client IO continues
    8. Verify mirroring immediately after upgrade before CephCI CephX migration
    9. Continue IO on the same mounts before any CephX rotation
    10. Perform RBD-specific CephX rotation when required
    11. Verify mirroring after CephX migration
    12. Run final IOs on the same mounts and cleanup map/mount once
    """
    pool_types = ["rep_pool_config", "ec_pool_config"]
    log.info(
        "Running rbd mirror rolling cluster upgrade with snapshot " "mirroring enabled"
    )

    # Save original ceph_cluster so we can restore it after swapping
    original_ceph_cluster = kw.get("ceph_cluster")
    mirror_obj = None
    primary_io_contexts = []

    try:
        # ---- Step 1: Setup mirroring between two clusters ----
        config = kw.get("config", {})
        config["do_not_run_io"] = True
        # Run upgrade coverage on either replicated or EC pool (not both)
        pool_type = random.choice(pool_types)
        log.info("Running upgrade suite on %s only", pool_type)
        if pool_type == "rep_pool_config":
            config["rep-pool-only"] = True
        else:
            config["ec-pool-only"] = True

        mirror_obj = initial_mirror_config(**kw)
        mirror_obj.pop("output", [])

        # ---- Step 2: Extract both clusters' info ----
        rbd_primary = None
        rbd_secondary = None
        client_primary = None
        client_secondary = None
        cluster_primary = None
        cluster_secondary = None

        for val in mirror_obj.values():
            if not val.get("is_secondary", False):
                rbd_primary = val.get("rbd")
                client_primary = val.get("client")
                cluster_primary = val.get("cluster")
            else:
                rbd_secondary = val.get("rbd")
                client_secondary = val.get("client")
                cluster_secondary = val.get("cluster")

        log.info(
            f"Initial configuration complete. "
            f"Primary cluster: {cluster_primary.name}, "
            f"Secondary cluster: {cluster_secondary.name}"
        )

        pool_types = list(mirror_obj.values())[0].get("pool_types")

        # ---- Step 3: Map/mount once, then pre-upgrade IO on existing mounts ----
        log.info(
            "Step 3: Prepare persistent primary RBD mounts, run pre-upgrade IO "
            "and verify snapshot schedule"
        )
        primary_io_contexts, rc = prepare_upgrade_io_mounts(
            rbd=rbd_primary,
            client=client_primary,
            pool_types=pool_types,
            **kw,
        )
        if rc:
            log.error("Failed to prepare persistent primary RBD mounts")
            return 1

        rc = run_io_on_existing_mounts(
            primary_io_contexts,
            file_name="file_pre_upgrade",
            verify_snap_schedule=True,
        )
        if rc:
            log.error("Pre-upgrade IOs failed on persistent primary mounts")
            return 1

        # Verify mirroring works before any upgrade
        rc = verify_mirror_states(
            pool_types, rbd_primary, rbd_secondary, "pre-upgrade", **kw
        )
        if rc:
            return 1

        # ---- Step 4: Upgrade primary cluster (cluster 1) ----
        log.info(f"Step 4: Upgrading PRIMARY cluster: {cluster_primary.name}")
        # Swap ceph_cluster to point to primary for the upgrade module
        kw["ceph_cluster"] = cluster_primary
        rc = test_cluster_upgrade(
            target_cluster=cluster_primary,
            target_client=client_primary,
            io_contexts=primary_io_contexts,
            **kw,
        )
        if rc:
            log.error(f"Primary cluster upgrade failed for {cluster_primary.name}")
            return 1
        log.info(f"Primary cluster {cluster_primary.name} upgraded successfully")

        # ---- Step 5: Verify mirroring across mixed-version clusters ----
        log.info(
            "Step 5: Verifying mirroring works across mixed-version clusters "
            "(primary upgraded, secondary old version)"
        )
        rc = verify_mirror_states(
            pool_types,
            rbd_primary,
            rbd_secondary,
            "post-primary-upgrade-mixed-version",
            **kw,
        )
        if rc:
            return 1

        # ---- Step 6: Run IOs on primary after upgrade (same mounts) ----
        log.info("Step 6: Running post-primary-upgrade IO using existing RBD mounts")
        # Allow remount in case rbd-nbd was restarted during the primary upgrade
        for ctx in primary_io_contexts:
            ctx["remount_after_upgrade"] = True
        rc = run_io_on_existing_mounts(
            primary_io_contexts,
            file_name="file_after_primary_upgrade",
            verify_snap_schedule=True,
        )
        for ctx in primary_io_contexts:
            ctx["remount_after_upgrade"] = False
        if rc:
            log.error("Post-primary-upgrade IOs failed on persistent mounts")
            return 1

        # ---- Step 7: Upgrade secondary; keep primary client IO connected ----
        log.info(f"Step 7: Upgrading SECONDARY cluster: {cluster_secondary.name}")
        # Swap ceph_cluster to point to secondary for the upgrade module
        kw["ceph_cluster"] = cluster_secondary
        # IO continues on primary mounts/client — do not map secondary images.
        rc = test_cluster_upgrade(
            target_cluster=cluster_secondary,
            target_client=client_secondary,
            io_contexts=primary_io_contexts,
            **kw,
        )
        if rc:
            log.error(
                f"Secondary cluster upgrade failed for " f"{cluster_secondary.name}"
            )
            return 1
        log.info(
            f"Secondary cluster {cluster_secondary.name} upgraded " f"successfully"
        )

        # ---- Step 8: Customer scenario 1 - mirroring works before CephCI CephX migration ----
        log.info(
            "Step 8: Verifying mirroring immediately after upgrade before "
            "CephCI performs any RBD-specific CephX rotation"
        )
        rc = verify_mirror_states(
            pool_types,
            rbd_primary,
            rbd_secondary,
            "post-both-clusters-upgraded-pre-cephx",
            **kw,
        )
        if rc:
            return 1
        log.info(
            "Customer scenario 1 validated: RBD mirroring continues to work "
            "after upgrade without requiring immediate Ceph managed CephX migration"
        )

        # ---- Step 9: Continue IO before optional CephX migration ----
        log.info(
            "Step 9: Continue IO and mirror verification before RBD CephX rotation"
        )
        kw["ceph_cluster"] = cluster_primary
        # Allow remount in case rbd-nbd was restarted during the secondary upgrade
        for ctx in primary_io_contexts:
            ctx["remount_after_upgrade"] = True
        rc = run_io_on_existing_mounts(
            primary_io_contexts,
            file_name="file_post_upgrade_pre_cephx",
            verify_snap_schedule=True,
        )
        for ctx in primary_io_contexts:
            ctx["remount_after_upgrade"] = False
        if rc:
            log.error("Post-upgrade pre-CephX IOs failed on persistent mounts")
            return 1

        rc = verify_mirror_states(
            pool_types,
            rbd_primary,
            rbd_secondary,
            "post-upgrade-io-before-cephx",
            **kw,
        )
        if rc:
            return 1

        # ---- Step 10: Customer scenario 2 - RBD CephX migration ----
        log.info(
            "Step 10: Performing RBD-specific CephX rotation when required "
            "(customer scenario 2: migrate RBD credentials to aes256k)"
        )
        rc = post_upgrade_rbd_cephx_workflow(
            rbd_primary=rbd_primary,
            rbd_secondary=rbd_secondary,
            client_primary=client_primary,
            client_secondary=client_secondary,
            cluster_primary_name=cluster_primary.name,
            cluster_secondary_name=cluster_secondary.name,
            pool_types=pool_types,
            config=kw.get("config", {}),
        )
        if rc:
            log.error("Post-upgrade RBD CephX rotation workflow failed")
            return 1
        log.info(
            "Customer scenario 2 validated: RBD CephX credentials migrated "
            "to aes256k when required"
        )

        # ---- Step 11: Verify mirroring after CephX migration ----
        log.info("Step 11: Verifying mirroring after RBD CephX migration")
        rc = verify_mirror_states(
            pool_types,
            rbd_primary,
            rbd_secondary,
            "post-cephx-migration",
            **kw,
        )
        if rc:
            return 1

        # ---- Step 12: Final IO and data integrity verification ----
        log.info(
            "Step 12: Final IO verification on fully upgraded clusters "
            "using existing RBD mounts"
        )
        rc = run_io_on_existing_mounts(
            primary_io_contexts,
            file_name="file_final_verify",
            verify_snap_schedule=True,
        )
        if rc:
            log.error("Final IO verification failed on persistent mounts")
            return 1

        log.info(
            "Rolling upgrade test completed successfully. "
            "Mirroring verified at all stages: pre-upgrade, mixed-version, "
            "post-upgrade with legacy keys, and post-CephX migration."
        )
    except Exception as e:
        log.error(
            f"Rbd mirror rolling cluster upgrade with snapshot mirroring "
            f"enabled failed with error {str(e)}"
        )
        return 1
    finally:
        if primary_io_contexts:
            log.info("Cleaning up persistent primary RBD mounts from upgrade test")
            cleanup_upgrade_io_mounts(primary_io_contexts)
        # Restore original ceph_cluster before cleanup
        kw["ceph_cluster"] = original_ceph_cluster
        if mirror_obj is not None:
            cleanup(pool_types=pool_types, multi_cluster_obj=mirror_obj, **kw)
    return 0
