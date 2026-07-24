import time
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_mirroring.cephfs_mirroring_utils import CephfsMirroringUtils
from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    CEPH-83632391- Validate cephfs-mirror daemon status shows correct fsid and mon list
     of the remote cluster, and verify the same after adding intra-cluster peered
     filesystems.

     Test Workflow:
         1. Validates pre-requisites by ensuring there are clients available on both
            the source and target clusters.
         2. Prepares clients, authenticates them, and ensures the CephFS filesystem
            is available on both clusters.
         3. Configures CephFS mirroring between the source and target Ceph clusters
            using peer_bootstrap.
         4. Verifies successful mirror peer addition and ensures the peer is properly
            registered.
         5a. Checks cephfs mirror daemon status and validates the presence of fsid
             and mon list of the remote cluster.
         5b. Creates two additional filesystems (cephfs2, cephfs3) on the primary
             cluster, peers them with each other (intra-cluster), and validates that
             daemon status shows correct fsid and mon list for all peers.

     Args:
         ceph_cluster: The Ceph cluster to perform the mirroring tests on.
         **kw: Additional keyword arguments.

     Returns:
         int: 0 if the test is successful, 1 if there's an error.

     Raises:
         Exception: Any unexpected exceptions that might occur during the test.
    """
    intra_fs_source = None
    intra_fs_target = None
    intra_user = None
    intra_peer_uuid = None
    peer_uuid = None
    try:
        config = kw.get("config")
        ceph_cluster_dict = kw.get("ceph_cluster_dict")
        test_data = kw.get("test_data")
        erasure = (
            FsUtils.get_custom_config_value(test_data, "erasure")
            if test_data
            else False
        )
        fs_util_ceph1 = FsUtils(ceph_cluster_dict.get("ceph1"), test_data=test_data)
        fs_util_ceph2 = FsUtils(ceph_cluster_dict.get("ceph2"), test_data=test_data)
        fs_mirroring_utils = CephfsMirroringUtils(
            ceph_cluster_dict.get("ceph1"), ceph_cluster_dict.get("ceph2")
        )
        build = config.get("build", config.get("rhbuild"))
        source_clients = ceph_cluster_dict.get("ceph1").get_ceph_objects("client")
        target_clients = ceph_cluster_dict.get("ceph2").get_ceph_objects("client")

        log.info("checking Pre-requisites")
        if not source_clients or not target_clients:
            log.error(
                "This test requires a minimum of 1 client node on both ceph1 and ceph2."
            )
            return 1

        log.info("Preparing Clients...")
        fs_util_ceph1.prepare_clients(source_clients, build)
        fs_util_ceph2.prepare_clients(target_clients, build)
        fs_util_ceph1.auth_list(source_clients)
        fs_util_ceph2.auth_list(target_clients)

        source_fs = "cephfs" if not erasure else "cephfs-ec"
        target_fs = "cephfs" if not erasure else "cephfs-ec"
        fs_details_source = fs_util_ceph1.get_fs_info(source_clients[0], source_fs)
        if not fs_details_source:
            fs_util_ceph1.create_fs(source_clients[0], source_fs)
        fs_util_ceph1.wait_for_mds_process(source_clients[0], source_fs)
        fs_details_target = fs_util_ceph2.get_fs_info(target_clients[0], target_fs)
        if not fs_details_target:
            fs_util_ceph2.create_fs(target_clients[0], target_fs)
        fs_util_ceph2.wait_for_mds_process(target_clients[0], target_fs)

        target_user = "mirror_remote"
        target_site_name = "remote_site"

        log.info("Deploy CephFS Mirroring Configuration using peer_bootstrap")
        fs_mirroring_utils.deploy_cephfs_mirroring(
            source_fs,
            source_clients[0],
            target_fs,
            target_clients[0],
            target_user,
            target_site_name,
        )

        log.info("Verify peer addition via peer_list")
        peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], source_fs
        )
        if not peer_uuid:
            log.error("Peer UUID not found after deploying mirroring for %s", source_fs)
            raise CommandFailed("Peer UUID not found for %s" % source_fs)
        log.info("Peer UUID for %s: %s", source_fs, peer_uuid)

        validate_rc = fs_mirroring_utils.validate_peer_connection(
            source_clients[0], source_fs, target_site_name, target_user, target_fs
        )
        if validate_rc != 0:
            raise CommandFailed("Peer connection validation failed for %s" % source_fs)

        log.info(
            "Step 5a: Check daemon status for %s - validate fsid and mon_host of remote cluster",
            source_fs,
        )
        target_fsid = fs_util_ceph2.get_fsid(target_clients[0])
        target_mon_ips = sorted(fs_util_ceph2.get_mon_node_ips())
        log.info("Expected target cluster fsid: %s", target_fsid)
        log.info("Expected target cluster mon IPs: %s", target_mon_ips)

        fs_mirroring_utils.validate_daemon_status_for_fs(
            source_clients[0],
            source_fs,
            target_fsid,
            target_mon_ips,
        )
        log.info(
            "Daemon status validation passed for %s with correct fsid and mon list",
            source_fs,
        )

        log.info(
            "Step 5b: Create cephfs2 and cephfs3 on the primary cluster "
            "and peer them intra-cluster"
        )
        intra_fs_source = "cephfs2" if not erasure else "cephfs2-ec"
        intra_fs_target = "cephfs3" if not erasure else "cephfs3-ec"
        intra_user = "mirror_intra"
        intra_site_name = "intra_site"

        mds_nodes = ceph_cluster_dict.get("ceph1").get_ceph_objects("mds")
        mds_names = [mds.node.hostname for mds in mds_nodes]

        hosts_fs2 = mds_names[-4:-2]
        mds_placement_fs2 = " ".join(hosts_fs2) + " "
        log.info(
            "Creating %s on primary cluster MDS hosts: %s",
            intra_fs_source,
            mds_placement_fs2,
        )
        fs_util_ceph1.create_fs(
            source_clients[0],
            intra_fs_source,
            placement=f"2 {mds_placement_fs2}",
        )
        fs_util_ceph1.wait_for_mds_process(source_clients[0], intra_fs_source)

        hosts_fs3 = mds_names[-2:]
        mds_placement_fs3 = " ".join(hosts_fs3) + " "
        log.info(
            "Creating %s on primary cluster MDS hosts: %s",
            intra_fs_target,
            mds_placement_fs3,
        )
        fs_util_ceph1.create_fs(
            source_clients[0],
            intra_fs_target,
            placement=f"2 {mds_placement_fs3}",
        )
        fs_util_ceph1.wait_for_mds_process(source_clients[0], intra_fs_target)

        log.info(
            "Deploy intra-cluster mirroring: %s -> %s",
            intra_fs_source,
            intra_fs_target,
        )
        fs_mirroring_utils.deploy_cephfs_mirroring(
            intra_fs_source,
            source_clients[0],
            intra_fs_target,
            source_clients[0],
            intra_user,
            intra_site_name,
        )

        intra_peer_uuid = fs_mirroring_utils.get_peer_uuid_by_name(
            source_clients[0], intra_fs_source
        )
        if not intra_peer_uuid:
            log.error(
                "Peer UUID not found for intra-cluster peering on %s",
                intra_fs_source,
            )
            raise CommandFailed("Peer UUID not found for %s" % intra_fs_source)
        log.info(
            "Intra-cluster peer UUID for %s: %s",
            intra_fs_source,
            intra_peer_uuid,
        )

        validate_rc_intra = fs_mirroring_utils.validate_peer_connection(
            source_clients[0],
            intra_fs_source,
            intra_site_name,
            intra_user,
            intra_fs_target,
        )
        if validate_rc_intra != 0:
            raise CommandFailed(
                "Intra-cluster peer connection validation failed for %s"
                % intra_fs_source
            )

        log.info(
            "Validate daemon status for intra-cluster peer shows correct "
            "primary cluster fsid and mon list"
        )
        primary_fsid = fs_util_ceph1.get_fsid(source_clients[0])
        primary_mon_ips = sorted(fs_util_ceph1.get_mon_node_ips())
        log.info("Expected primary cluster fsid: %s", primary_fsid)
        log.info("Expected primary cluster mon IPs: %s", primary_mon_ips)

        fs_mirroring_utils.validate_daemon_status_for_fs(
            source_clients[0],
            intra_fs_source,
            primary_fsid,
            primary_mon_ips,
        )
        log.info(
            "Daemon status validation passed for intra-cluster peer %s "
            "with correct primary cluster fsid and mon list",
            intra_fs_source,
        )

        log.info(
            "Re-validate daemon status for cross-cluster peer %s is still correct",
            source_fs,
        )
        fs_mirroring_utils.validate_daemon_status_for_fs(
            source_clients[0],
            source_fs,
            target_fsid,
            target_mon_ips,
        )
        log.info(
            "Daemon status re-validation passed for cross-cluster peer %s",
            source_fs,
        )

        log.info(
            "Test Completed Successfully. Daemon status shows correct fsid and "
            "mon list for both cross-cluster (%s) and intra-cluster (%s) peers.",
            source_fs,
            intra_fs_source,
        )
        return 0
    except Exception as e:
        log.error(e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Clean up the system")

        if intra_fs_source and intra_fs_target:
            log.info(
                "Destroy intra-cluster mirroring for %s -> %s",
                intra_fs_source,
                intra_fs_target,
            )
            try:
                fs_mirroring_utils.destroy_cephfs_mirroring(
                    intra_fs_source,
                    source_clients[0],
                    intra_fs_target,
                    source_clients[0],
                    intra_user,
                    intra_peer_uuid,
                )
            except Exception as e:
                log.warning("Cleanup of intra-cluster mirroring failed: %s", e)

            log.info(
                "Remove intra-cluster filesystems %s and %s",
                intra_fs_source,
                intra_fs_target,
            )
            try:
                source_clients[0].exec_command(
                    sudo=True, cmd="ceph config set mon mon_allow_pool_delete true"
                )
                for fs_name in [intra_fs_source, intra_fs_target]:
                    source_clients[0].exec_command(
                        sudo=True,
                        cmd=f"ceph fs volume rm {fs_name} --yes-i-really-mean-it",
                        check_ec=False,
                    )
                time.sleep(30)
            except Exception as e:
                log.warning("Cleanup of intra-cluster FS volumes failed: %s", e)

        log.info("Destroy cross-cluster mirroring for %s -> %s", source_fs, target_fs)
        try:
            fs_mirroring_utils.destroy_cephfs_mirroring(
                source_fs,
                source_clients[0],
                target_fs,
                target_clients[0],
                target_user,
                peer_uuid,
            )
        except Exception as e:
            log.warning("Cleanup of cross-cluster mirroring failed: %s", e)
