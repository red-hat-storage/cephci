############################################################################
# Supported Ceph Version    : 9.0 onwards
# Test Case Covered         : CEPH-83630825
# Bugzilla Link             : https://bugzilla.redhat.com/show_bug.cgi?id=2355303
##############################################################################
import traceback

from cli.ceph.ceph import Ceph
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.exceptions import CharMapSetError, SubvolumeGroupCreateError
from tests.cephfs.lib.cephfs_attributes_lib import CephFSAttributeUtilities
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from utility.log import Log

log = Log(__name__)


def extract_case_and_normalization(info):
    """Extracts case sensitivity and normalization attributes from subvolume info.
    Args:
        info (dict): Subvolume info dictionary.
    Returns:
        dict: A dictionary containing 'casesensitive' and 'normalization' attributes.
    """
    return {
        "casesensitive": info.get("casesensitive", ""),
        "normalization": info.get("normalization", ""),
    }


def create_svgroup_case_charmap_test():
    """Create subvolume group and validate different charmap settings"""
    Ceph(client1).fs.sub_volume_group.create(
        cephfs_vol,
        cephfs_subvol_group1,
        **{"casesensitive=": "false"},
    )

    get_charmap = Ceph(client1).fs.sub_volume_group.charmap.get(
        cephfs_vol, cephfs_subvol_group1
    )

    attr_util.validate_charmap_with_values(
        get_charmap,
        {
            "casesensitive": False,
            "normalization": "nfd",
            "encoding": "utf8",
        },
    )

    get_subvolume = Ceph(client1).fs.sub_volume_group.info(
        cephfs_vol,
        cephfs_subvol_group1,
    )

    attr_util.validate_charmap_with_values(
        extract_case_and_normalization(get_subvolume),
        {"casesensitive": False, "normalization": "nfd"},
    )


def create_svgroup_norm_charmap_test():
    """Create subvolume group and validate different charmap settings"""
    Ceph(client1).fs.sub_volume_group.create(
        cephfs_vol,
        cephfs_subvol_group2,
        **{"normalization=": "nfkd"},
    )

    get_charmap = Ceph(client1).fs.sub_volume_group.charmap.get(
        cephfs_vol, cephfs_subvol_group2
    )

    attr_util.validate_charmap_with_values(
        get_charmap,
        {
            "casesensitive": True,
            "normalization": "nfkd",
            "encoding": "utf8",
        },
    )

    get_subvolume = Ceph(client1).fs.sub_volume_group.info(
        cephfs_vol,
        cephfs_subvol_group2,
    )

    attr_util.validate_charmap_with_values(
        extract_case_and_normalization(get_subvolume),
        {"casesensitive": True, "normalization": "nfkd"},
    )


def create_subvolumes_and_validate_charmap():
    """Create subvolumes under subvolume groups and validate charmap settings."""
    Ceph(client1).fs.sub_volume.create(
        cephfs_vol,
        cephfs_subvol,
        **{"group-name": cephfs_subvol_group1, "mode": "0777"},
    )

    sv1_info = Ceph(client1).fs.sub_volume.info(
        cephfs_vol, cephfs_subvol, **{"group-name": cephfs_subvol_group1}
    )
    log.info("Subvolume {} info: {}".format(cephfs_subvol, sv1_info))

    sv1_charmap = extract_case_and_normalization(sv1_info)
    log.info("Subvolume {} charmap: {}".format(cephfs_subvol, sv1_charmap))

    attr_util.validate_charmap_with_values(
        sv1_charmap,
        {
            "casesensitive": False,
            "normalization": "nfd",
        },
    )

    Ceph(client1).fs.sub_volume.create(
        cephfs_vol,
        cephfs_subvol,
        **{"group-name": cephfs_subvol_group2, "mode": "0777"},
    )

    sv2_info = Ceph(client1).fs.sub_volume.info(
        cephfs_vol, cephfs_subvol, **{"group-name": cephfs_subvol_group2}
    )
    log.info("Subvolume {} info: {}".format(cephfs_subvol, sv2_info))

    sv2_charmap = extract_case_and_normalization(sv2_info)
    log.info("Subvolume {} charmap: {}".format(cephfs_subvol, sv2_charmap))

    attr_util.validate_charmap_with_values(
        sv2_charmap,
        {
            "casesensitive": True,
            "normalization": "nfkd",
        },
    )


def modify_and_validate_charmap_sv1():
    """Modify charmap values of subvolumes under svg and validate."""
    Ceph(client1).fs.sub_volume.charmap.set(
        cephfs_vol,
        cephfs_subvol,
        {
            "casesensitive": "true",
            "normalization": "nfkc",
        },
        **{"group-name": cephfs_subvol_group1},
    )

    sv1_info = Ceph(client1).fs.sub_volume.info(
        cephfs_vol, cephfs_subvol, **{"group-name": cephfs_subvol_group1}
    )
    log.info("Subvolume {} info after modification: {}".format(cephfs_subvol, sv1_info))

    sv1_charmap = extract_case_and_normalization(sv1_info)
    log.info(
        "Subvolume {} charmap after modification: {}".format(cephfs_subvol, sv1_charmap)
    )

    attr_util.validate_charmap_with_values(
        sv1_charmap,
        {
            "casesensitive": True,
            "normalization": "nfkc",
        },
    )

    Ceph(client1).fs.sub_volume.charmap.set(
        cephfs_vol,
        cephfs_subvol,
        {
            "casesensitive": "false",
            "normalization": "nfc",
        },
        **{"group-name": cephfs_subvol_group2},
    )

    get_charmap = Ceph(client1).fs.sub_volume.charmap.get(
        cephfs_vol,
        cephfs_subvol,
        **{"group-name": cephfs_subvol_group2},
    )

    attr_util.validate_charmap_with_values(
        get_charmap,
        {
            "casesensitive": False,
            "normalization": "nfc",
            "encoding": "utf8",
        },
    )


def modify_charmap_svg1():
    """[Negative] Try modifying charmaps of subvolume group 1 and validate."""
    try:
        Ceph(client1).fs.sub_volume_group.charmap.set(
            cephfs_vol,
            cephfs_subvol_group1,
            **{"casesensitive": "false"},
        )
        log.error(
            "FAILED: Modified charmap of subvolume group which should be immutable"
        )
        return 1

    except CharMapSetError as e:
        log.info(
            "Expected failure while modifying charmap of subvolume group {}: {}".format(
                cephfs_subvol_group1, e
            )
        )


def sv_mount_io():
    """Create subvolume under subvolume group 3 and mount and run IO."""
    global fuse_mounting_dir, nfs_mounting_dir, smb_mounting_dir, nfs_params, smb_params, sub_vol_path_1

    sub_vol_path_1 = common_util.subvolume_get_path(
        client1,
        cephfs_vol,
        subvolume_name=cephfs_subvol,
        subvolume_group=cephfs_subvol_group3,
    )

    log.info("*** Mounting Subvolume via FUSE ***")
    fuse_mounting_dir = common_util.setup_fuse_mount(
        client1,
        cephfs_vol,
        mount_path="/mnt/fuse_{}/".format(common_util.generate_mount_dir()),
        subvolume_name=cephfs_subvol,
        subvol_path=sub_vol_path_1,
    )
    log.info("Mounting dir for FUSE: {}".format(fuse_mounting_dir))

    fs_util.run_ios_V1(client1, fuse_mounting_dir, ["dd"])

    attr_util.validate_charmap(
        client1,
        fuse_mounting_dir,
        {"normalization": "nfkd", "encoding": "utf8", "casesensitive": False},
    )


def validate_charmap_sv1():
    """[Negative] Set invalid charmap values to subvolume and validate."""
    try:
        Ceph(client1).fs.sub_volume_group.create(
            cephfs_vol,
            cephfs_subvol_group3,
            **{"normalization=": "norm"},
        )
        log.error("Subvolume group create should have failed with invalid value")
        return 1

    except SubvolumeGroupCreateError as e:
        log.info("Passed: Failed as expected with invalid value: {}".format(str(e)))

    try:
        Ceph(client1).fs.sub_volume_group.create(
            cephfs_vol,
            cephfs_subvol_group3,
            **{"casesensitive=": "falsey"},
        )
        log.error("Subvolume group create should have failed with invalid value")
        return 1

    except SubvolumeGroupCreateError as e:
        log.info("Passed: Failed as expected with invalid value: {}".format(str(e)))


def run(ceph_cluster, **kw):
    try:
        test_data = kw.get("test_data")
        config = kw.get("config")
        clients = ceph_cluster.get_ceph_objects("client")
        build = config.get("build", config.get("rhbuild"))

        log.info("checking Pre-requisites")
        if len(clients) < 1:
            log.error(
                "This test requires minimum 1 client nodes. This has only {} clients".format(
                    len(clients)
                )
            )
            return 1

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Pre-Requisite : Create volume and define global params "
            "\n---------------***************-----------------------------"
        )
        global client1, installer, common_util, attr_util, fs_util
        global cephfs_vol, cephfs_subvol, cephfs_subvol_group1, cephfs_subvol_group2, cephfs_subvol_group3

        attr_util = CephFSAttributeUtilities(ceph_cluster)
        common_util = CephFSCommonUtils(ceph_cluster)
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        fs_util.auth_list(clients)
        client1 = clients[0]
        installer = ceph_cluster.get_nodes(role="installer")[0]
        fs_util.prepare_clients([client1], build)

        cephfs_vol = "cs-volume-1"
        cephfs_subvol = "subvol1"
        cephfs_subvol_group1 = "subvolgroup1"
        cephfs_subvol_group2 = "subvolgroup2"
        cephfs_subvol_group3 = "subvolgroup3"

        Ceph(client1).fs.volume.create(cephfs_vol)

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 1: Create subvolume group1 and validate charmap settings using charmap set/get and info "
            "\n---------------***************-----------------------------"
        )

        log.info("Check Ceph health")
        if common_util.wait_for_healthy_ceph(client1, 300):
            log.error("Cluster health is not OK even after waiting for 300secs")
            return 1

        create_svgroup_case_charmap_test()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 2: Create subvolume group2 and validate charmap settings using charmap set/get and info "
            "\n---------------***************-----------------------------"
        )
        create_svgroup_norm_charmap_test()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 3: Create SubVolumes under subvolume group1,2 and validate charmap settings "
            "\n---------------***************-----------------------------"
        )
        create_subvolumes_and_validate_charmap()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 4: Modify charmap values of subvolumes under svg and validate "
            "\n---------------***************-----------------------------"
        )
        modify_and_validate_charmap_sv1()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 5: [Negative] Try modifying charmaps of subvolume group 1 and validate "
            "\n---------------***************-----------------------------"
        )
        modify_charmap_svg1()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 6: [Negative] Set invalid charmap values to subvolume and validate "
            "\n---------------***************-----------------------------"
        )

        validate_charmap_sv1()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 7: Create subvolume1 under subvolume group 3, mount and run IO "
            "\n---------------***************-----------------------------"
        )
        log.info("Creating subvolume under subvolume group 3")

        Ceph(client1).fs.sub_volume_group.create(
            cephfs_vol,
            cephfs_subvol_group3,
            **{"normalization=": "nfkd", "casesensitive=": "false"},
        )

        Ceph(client1).fs.sub_volume.create(
            cephfs_vol,
            cephfs_subvol,
            **{"group-name": cephfs_subvol_group3, "mode": "0777"},
        )

        log.info("Declaring global variables for reuse")
        global fuse_mounting_dir
        fuse_mounting_dir = ""

        log.info("Mounting Subvolume 1 via FUSE and running IO")
        sv_mount_io()

        log.info("*** Passed: Completed SubvolumeGroup Charmap tests successfully ***")
        return 0

    except Exception as e:
        log.error("Test execution failed: {}".format(str(e)))
        log.error(traceback.format_exc())
        return 1

    finally:
        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Cleaning up the file system and subvolumes created "
            "\n---------------***************-----------------------------"
        )

        log.info("Check Ceph health")
        if common_util.wait_for_healthy_ceph(client1, 300):
            log.error("Cluster health is not OK even after waiting for 300secs")
            return 1

        log.info("Unmounting and cleaning up the FUSE mounts")
        fs_util.client_clean_up(
            "umount", fuse_clients=[client1], mounting_dir=fuse_mounting_dir
        )

        log.info("Removing Volume")
        fs_util.remove_fs(client1, cephfs_vol)
        log.info("Cleanup completed successfully.")
