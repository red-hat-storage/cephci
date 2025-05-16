import os
import traceback

from cli.ceph.ceph import Ceph
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.exceptions import (
    CaseSensitivityValidationError,
    CharMapGetError,
    CharMapRemoveError,
    CharMapSetError,
    NormalizationValidationError,
)
from tests.cephfs.lib.cephfs_attributes_lib import CephFSAttributeUtilities
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils

# from tests.cephfs.lib.cephfs_recovery_lib import FSRecovery
from tests.smb.smb_operations import (
    check_smb_cluster,
    create_smb_cluster,
    create_smb_share,
    remove_smb_cluster,
    remove_smb_share,
    smb_cifs_mount,
    smbclient_check_shares,
    verify_smb_service,
)
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


def create_svgroup_charmap_test():
    """Create subvolume group and validate different charmap settings using charmap set/get."""
    Ceph(client1).fs.sub_volume_group.create(
        cephfs_vol,
        cephfs_subvol_group1,
    )

    Ceph(client1).fs.sub_volume_group.charmap.set(
        cephfs_vol,
        cephfs_subvol_group1,
        **{
            "casesensitive": "false",
        },
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

    Ceph(client1).fs.sub_volume_group.create(
        cephfs_vol,
        cephfs_subvol_group2,
    )

    Ceph(client1).fs.sub_volume_group.charmap.set(
        cephfs_vol,
        cephfs_subvol_group2,
        **{
            "normalization": "nfkc",
        },
    )

    get_charmap = Ceph(client1).fs.sub_volume_group.charmap.get(
        cephfs_vol, cephfs_subvol_group2
    )

    attr_util.validate_charmap_with_values(
        get_charmap,
        {
            "casesensitive": True,
            "normalization": "nfkc",
            "encoding": "utf8",
        },
    )


def validate_svg_unsupported_charmaps():
    """Validate unsupported charmaps for subvolume group."""
    try:
        Ceph(client1).fs.sub_volume_group.charmap.set(
            cephfs_vol,
            cephfs_subvol_group1,
            **{
                "casesensitive": "falsey",
            },
        )
        log.error("Charmap set should have failed with invalid value")
        return 1

    except CharMapSetError as e:
        log.info("Passed: Failed as expected with invalid value: {}".format(str(e)))

    try:
        Ceph(client1).fs.sub_volume_group.charmap.set(
            cephfs_vol,
            cephfs_subvol_group1,
            **{
                "casesensitive": "none",
            },
        )
        log.error("Charmap set should have failed with invalid value")
        return 1
    except CharMapSetError as e:
        log.info("Passed: Failed as expected with invalid value: {}".format(str(e)))

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


def test_sv_creation_with_charmap():
    """Create subvolume under subvolume group 2 where charmap set."""
    Ceph(client1).fs.sub_volume.create(
        cephfs_vol,
        cephfs_subvol,
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
            "casesensitive": True,
            "normalization": "nfkc",
            "encoding": "utf8",
        },
    )

    get_subvolume = Ceph(client1).fs.sub_volume.info(
        cephfs_vol,
        cephfs_subvol,
        **{"group-name": cephfs_subvol_group2},
    )

    attr_util.validate_charmap_with_values(
        extract_case_and_normalization(get_subvolume),
        {"casesensitive": True, "normalization": "nfkc"},
    )


def modify_charmap_svg2():
    """Modify charmaps of subvolume group 2 and validate."""
    try:
        Ceph(client1).fs.sub_volume_group.charmap.set(
            cephfs_vol,
            cephfs_subvol_group2,
            **{
                "normalization": "nfd",
                "casesensitive": "false",
            },
        )
        log.error("Charmap set should have failed since subvol group is not empty")
        return 1
    except CharMapSetError as e:
        log.info("Passed: Failed as expected with error: {}".format(str(e)))

    get_charmap = Ceph(client1).fs.sub_volume_group.charmap.get(
        cephfs_vol, cephfs_subvol_group2
    )

    attr_util.validate_charmap_with_values(
        get_charmap,
        {
            "casesensitive": True,
            "normalization": "nfkc",
            "encoding": "utf8",
        },
    )


def sv1_mount(mount_type="fuse"):
    """Create subvolume1 under subvolume group 1 and mount across different clients."""
    global fuse_mounting_dir, nfs_mounting_dir, smb_mounting_dir, nfs_params, smb_params, sub_vol_path_1

    sub_vol_path_1 = common_util.subvolume_get_path(
        client1,
        cephfs_vol,
        subvolume_name=cephfs_subvol,
        subvolume_group=cephfs_subvol_group1,
    )

    if ibm_build and mount_type == "smb":
        log.info("*** Mounting via SMB ***")

        log.info("Removing the earmark from subvolume as a pre-requisite")
        Ceph(client1).fs.sub_volume.earmark.remove(
            volume=cephfs_vol,
            subvolume_name=cephfs_subvol,
            group_name=cephfs_subvol_group1,
        )

        smb_mounting_dir = "/mnt/smb_{}/".format(common_util.generate_mount_dir())
        smb_params = {
            "smb_cluster_id": "smb-sv-1",
            "smb_shares": ["share-sv-1"],
            "installer": installer,
        }

        # Create smb cluster with auth_mode
        create_smb_cluster(
            installer,
            smb_cluster_id=smb_params.get("smb_cluster_id"),
            auth_mode="user",
            domain_realm="",
            smb_user_name="user1",
            smb_user_password="passwd",
            custom_dns="",
            clustering="default",
        )

        # Check smb cluster
        check_smb_cluster(installer, smb_cluster_id=smb_params.get("smb_cluster_id"))

        # Create smb share
        create_smb_share(
            installer,
            smb_params.get("smb_shares"),
            smb_params.get("smb_cluster_id"),
            cephfs_vol,
            "/",
            cephfs_subvol_group1,
            [cephfs_subvol],
        )

        # Check smb service
        verify_smb_service(installer, service_name="smb")

        smbclient_check_shares(
            smb_nodes,
            client1,
            smb_params.get("smb_shares"),
            smb_user_name="user1",
            smb_user_password="passwd",
            auth_mode="user",
            domain_realm="",
        )

        smb_cifs_mount(
            smb_nodes[0],
            client1,
            smb_params.get("smb_shares")[0],
            smb_user_name="user1",
            smb_user_password="passwd",
            auth_mode="user",
            domain_realm="",
            cifs_mount_point=smb_mounting_dir,
        )

    if mount_type == "fuse":
        log.info("*** Mounting Subvolume via FUSE ***")
        fuse_mounting_dir = common_util.setup_fuse_mount(
            client1,
            cephfs_vol,
            mount_path="/mnt/fuse_{}/".format(common_util.generate_mount_dir()),
            subvolume_name=cephfs_subvol,
            subvol_path=sub_vol_path_1,
        )
        log.info("Mounting dir for FUSE: {}".format(fuse_mounting_dir))

    if mount_type == "nfs":
        log.info("*** Mounting via NFS ***")

        mds = fs_util.get_active_mdss(client1, cephfs_vol)
        active_mds_hostnames = [i.split(".")[1] for i in mds]
        nfs_server_name = active_mds_hostnames[0]
        log.debug("Active MDS Hostname: {}".format(nfs_server_name))

        nfs_params = {
            "nfs_cluster_name": "nfs-sv-1",
            "nfs_server_name": nfs_server_name,
            "binding": "/export_binding_nfs_sv_1",
        }
        nfs_mounting_dir = common_util.setup_nfs_mount(
            client1,
            cephfs_vol,
            mount_path="/mnt/nfs_{}/".format(common_util.generate_mount_dir()),
            subvol_path=sub_vol_path_1,
            subvolume_name=cephfs_subvol,
            nfs_server_name=nfs_params.get("nfs_server_name"),
            nfs_cluster_name=nfs_params.get("nfs_cluster_name"),
            binding=nfs_params.get("binding"),
        )
        log.info("NFS Mounting dir: {}".format(nfs_mounting_dir))


def validate_charmap_sv1():
    """Validate attribute of subvolume1 created under subvolume group 1."""
    get_charmap = Ceph(client1).fs.sub_volume.charmap.get(
        cephfs_vol,
        cephfs_subvol,
        **{"group-name": cephfs_subvol_group1},
    )

    attr_util.validate_charmap_with_values(
        get_charmap,
        {
            "casesensitive": False,
            "normalization": "nfd",
            "encoding": "utf8",
        },
    )

    get_subvolume = Ceph(client1).fs.sub_volume.info(
        cephfs_vol, cephfs_subvol, **{"group-name": cephfs_subvol_group1}
    )

    attr_util.validate_charmap_with_values(
        extract_case_and_normalization(get_subvolume),
        {"casesensitive": False, "normalization": "nfd"},
    )

    attr_util.validate_charmap(
        client1,
        fuse_mounting_dir,
        {"normalization": "nfd", "encoding": "utf8", "casesensitive": False},
    )


def modify_and_validate_charmap_sv1():
    """Modify and validate attribute of subvolume1 created under subvolume group 1."""
    Ceph(client1).fs.sub_volume.charmap.set(
        cephfs_vol,
        cephfs_subvol,
        {
            "casesensitive": "false",
            "normalization": "nfkd",
        },
        **{"group-name": cephfs_subvol_group1},
    )

    attr_util.validate_charmap(
        client1,
        fuse_mounting_dir,
        {"normalization": "nfkd", "encoding": "utf8", "casesensitive": False},
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


def snap_clone_sv_and_validate_charmap():
    """Create snapshot and clone subvolume, then validate charmaps."""
    fs_util.create_snapshot(
        client1,
        cephfs_vol,
        cephfs_subvol,
        cephfs_snap1,
        **{"group_name": cephfs_subvol_group1},
    )

    clone_status_1 = {
        "vol_name": cephfs_vol,
        "subvol_name": cephfs_subvol,
        "snap_name": cephfs_snap1,
        "target_subvol_name": cephfs_clone_subvol,
        "group_name": cephfs_subvol_group1,
        "target_group_name": cephfs_subvol_group1,
    }

    fs_util.create_clone(client1, **clone_status_1)
    fs_util.validate_clone_state(client1, clone_status_1)

    get_charmap = Ceph(client1).fs.sub_volume.charmap.get(
        cephfs_vol,
        cephfs_clone_subvol,
        **{
            "group-name": cephfs_subvol_group1,
        },
    )

    attr_util.validate_charmap_with_values(
        get_charmap,
        {
            "casesensitive": False,
            "normalization": "nfkd",
            "encoding": "utf8",
        },
    )

    get_subvolume = Ceph(client1).fs.sub_volume.info(
        cephfs_vol, cephfs_clone_subvol, **{"group-name": cephfs_subvol_group1}
    )

    attr_util.validate_charmap_with_values(
        extract_case_and_normalization(get_subvolume),
        {"casesensitive": False, "normalization": "nfkd"},
    )


def resize_clone_sv_and_validate_charmap():
    """Resize cloned subvolume and validate charmaps."""
    Ceph(client1).fs.sub_volume.resize(
        cephfs_vol,
        cephfs_clone_subvol,
        "1073741824",
        **{"group-name": cephfs_subvol_group1},
    )

    get_charmap = Ceph(client1).fs.sub_volume.charmap.get(
        cephfs_vol,
        cephfs_clone_subvol,
        **{
            "group-name": cephfs_subvol_group1,
        },
    )

    attr_util.validate_charmap_with_values(
        get_charmap,
        {
            "casesensitive": False,
            "normalization": "nfkd",
            "encoding": "utf8",
        },
    )

    get_subvolume = Ceph(client1).fs.sub_volume.info(
        cephfs_vol, cephfs_clone_subvol, **{"group-name": cephfs_subvol_group1}
    )

    attr_util.validate_charmap_with_values(
        extract_case_and_normalization(get_subvolume),
        {"casesensitive": False, "normalization": "nfkd"},
    )


def rename_clone_sv_and_validate_charmap():
    """Rename cloned subvolume and validate charmaps."""
    fs_util.rename_volume(client1, cephfs_vol, cephfs_rename_vol)

    get_charmap = Ceph(client1).fs.sub_volume.charmap.get(
        cephfs_rename_vol,
        cephfs_clone_subvol,
        **{
            "group-name": cephfs_subvol_group1,
        },
    )

    attr_util.validate_charmap_with_values(
        get_charmap,
        {
            "casesensitive": False,
            "normalization": "nfkd",
            "encoding": "utf8",
        },
    )

    get_subvolume = Ceph(client1).fs.sub_volume.info(
        cephfs_rename_vol, cephfs_clone_subvol, **{"group-name": cephfs_subvol_group1}
    )

    attr_util.validate_charmap_with_values(
        extract_case_and_normalization(get_subvolume),
        {"casesensitive": False, "normalization": "nfkd"},
    )

    fs_util.rename_volume(client1, cephfs_rename_vol, cephfs_vol)

    get_charmap = Ceph(client1).fs.sub_volume.charmap.get(
        cephfs_vol,
        cephfs_clone_subvol,
        **{
            "group-name": cephfs_subvol_group1,
        },
    )

    attr_util.validate_charmap_with_values(
        get_charmap,
        {
            "casesensitive": False,
            "normalization": "nfkd",
            "encoding": "utf8",
        },
    )

    get_subvolume = Ceph(client1).fs.sub_volume.info(
        cephfs_vol, cephfs_clone_subvol, **{"group-name": cephfs_subvol_group1}
    )

    attr_util.validate_charmap_with_values(
        extract_case_and_normalization(get_subvolume),
        {"casesensitive": False, "normalization": "nfkd"},
    )


def run_io_sv_and_validate_charmap():
    """Run IO on subvolume and validate charmaps."""
    fs_util.run_ios_V1(client1, fuse_mounting_dir, ["dd"])

    attr_util.validate_charmap(
        client1,
        fuse_mounting_dir,
        {"normalization": "nfkd", "encoding": "utf8", "casesensitive": False},
    )

    get_subvolume = Ceph(client1).fs.sub_volume.info(
        cephfs_vol, cephfs_subvol, **{"group-name": cephfs_subvol_group1}
    )

    attr_util.validate_charmap_with_values(
        extract_case_and_normalization(get_subvolume),
        {"casesensitive": False, "normalization": "nfkd"},
    )


def sv_default_validate_charmap():
    """Create subvolume with default charmaps and validate."""
    Ceph(client1).fs.sub_volume.create(
        cephfs_vol,
        cephfs_subvol_default,
    )

    Ceph(client1).fs.sub_volume.charmap.set(
        cephfs_vol,
        cephfs_subvol_default,
        {
            "normalization": "nfkc",
            "casesensitive": "false",
        },
    )

    get_charmap = Ceph(client1).fs.sub_volume.charmap.get(
        cephfs_vol, cephfs_subvol_default
    )

    attr_util.validate_charmap_with_values(
        get_charmap,
        {
            "casesensitive": False,
            "normalization": "nfkc",
            "encoding": "utf8",
        },
    )

    Ceph(client1).fs.sub_volume.charmap.set(
        cephfs_vol,
        cephfs_subvol_default,
        {
            "normalization": "nfc",
        },
    )

    get_charmap = Ceph(client1).fs.sub_volume.charmap.get(
        cephfs_vol, cephfs_subvol_default
    )

    attr_util.validate_charmap_with_values(
        get_charmap,
        {
            "casesensitive": False,
            "normalization": "nfc",
            "encoding": "utf8",
        },
    )

    Ceph(client1).fs.sub_volume.charmap.set(
        cephfs_vol,
        cephfs_subvol_default,
        {
            "normalization": "nfd",
        },
    )

    get_charmap = Ceph(client1).fs.sub_volume.charmap.get(
        cephfs_vol, cephfs_subvol_default
    )

    attr_util.validate_charmap_with_values(
        get_charmap,
        {
            "casesensitive": False,
            "normalization": "nfd",
            "encoding": "utf8",
        },
    )

    Ceph(client1).fs.sub_volume.charmap.set(
        cephfs_vol,
        cephfs_subvol_default,
        {
            "normalization": "nfkd",
        },
    )

    get_charmap = Ceph(client1).fs.sub_volume.charmap.get(
        cephfs_vol, cephfs_subvol_default
    )

    attr_util.validate_charmap_with_values(
        get_charmap,
        {
            "casesensitive": False,
            "normalization": "nfkd",
            "encoding": "utf8",
        },
    )


def remove_charmap_and_validate():
    """Remove charmaps from subvolume, subvolumegroup and validate."""
    try:
        log.info("Try removing charmaps from subvolume where dir exists")
        Ceph(client1).fs.sub_volume.charmap.remove(
            cephfs_vol, cephfs_subvol, **{"group-name": cephfs_subvol_group1}
        )
        log.error("Charmap remove should have failed since subvolume exists")
        return 1
    except CharMapRemoveError as e:
        log.info("Passed: Failed as expected with error: {}".format(str(e)))

    log.info("Removing snapshot, files and re-test")
    fs_util.remove_snapshot(
        client1,
        cephfs_vol,
        cephfs_subvol,
        cephfs_snap1,
        **{"group_name": cephfs_subvol_group1},
    )

    log.info("Removing files..")
    client1.exec_command(
        sudo=True, cmd=f"rm -rf {fuse_mounting_dir}*", long_running=True, timeout=3600
    )

    Ceph(client1).fs.sub_volume.charmap.remove(
        cephfs_vol, cephfs_subvol, **{"group-name": cephfs_subvol_group1}
    )
    log.info("Charmap removed successfully after deleting the snapshot")

    log.info("Removing charmaps from subvolume where dir does not exist")
    Ceph(client1).fs.sub_volume.charmap.remove(cephfs_vol, cephfs_subvol_default)

    try:
        get_charmap = Ceph(client1).fs.sub_volume.charmap.get(
            cephfs_vol, cephfs_subvol_default
        )
        log.error(
            "Charmap get should have failed after removing charmaps {}".format(
                get_charmap
            )
        )
        return 1
    except CharMapGetError as e:
        log.info("Passed: Failed as expected with error: {}".format(str(e)))

    get_subvolume = Ceph(client1).fs.sub_volume.info(cephfs_vol, cephfs_subvol_default)

    attr_util.validate_charmap_with_values(
        extract_case_and_normalization(get_subvolume),
        {"casesensitive": True, "normalization": "none"},
    )

    try:
        log.info("Try removing charmaps from subvolumegroup where dir exists")
        Ceph(client1).fs.sub_volume_group.charmap.remove(
            cephfs_vol, cephfs_subvol_group1
        )
        log.error("Charmap remove should have failed since subvolumegroup exists")
        return 1
    except CharMapRemoveError as e:
        log.info("Passed: Failed as expected with error: {}".format(str(e)))

    log.info("Removing charmaps from subvolumegroup where dir does not exist")
    Ceph(client1).fs.sub_volume.rm(cephfs_vol, cephfs_subvol, cephfs_subvol_group2)
    Ceph(client1).fs.sub_volume_group.charmap.remove(cephfs_vol, cephfs_subvol_group2)

    try:
        get_charmap = Ceph(client1).fs.sub_volume_group.charmap.get(
            cephfs_vol, cephfs_subvol_group2
        )
        log.error(
            "Charmap get should have failed after removing charmaps {}".format(
                get_charmap
            )
        )
        return 1
    except CharMapGetError as e:
        log.info("Passed: Failed as expected with error: {}".format(str(e)))


def test_sv_creation_with_casesensitive_flags():
    """Create subvolume using flags"""
    log.info(" ** Creating subvolume with only casesensitive flag true **")
    Ceph(client1).fs.sub_volume.create(
        cephfs_vol,
        cephfs_subvol_2,
        **{"casesensitive=": "true"},
    )

    get_subvolume = Ceph(client1).fs.sub_volume.info(
        cephfs_vol,
        cephfs_subvol_2,
    )

    attr_util.validate_charmap_with_values(
        extract_case_and_normalization(get_subvolume),
        {"casesensitive": True, "normalization": "none"},
    )

    Ceph(client1).fs.sub_volume.rm(cephfs_vol, cephfs_subvol_2)

    log.info(" ** Creating subvolume with only casesensitive flag false **")
    Ceph(client1).fs.sub_volume.create(
        cephfs_vol,
        cephfs_subvol_2,
        **{"casesensitive=": "false"},
    )

    get_charmap = Ceph(client1).fs.sub_volume.charmap.get(
        cephfs_vol,
        cephfs_subvol_2,
    )

    attr_util.validate_charmap_with_values(
        get_charmap,
        {
            "casesensitive": False,
            "normalization": "nfd",
            "encoding": "utf8",
        },
    )

    get_subvolume = Ceph(client1).fs.sub_volume.info(
        cephfs_vol,
        cephfs_subvol_2,
    )

    attr_util.validate_charmap_with_values(
        extract_case_and_normalization(get_subvolume),
        {"casesensitive": False, "normalization": "nfd"},
    )

    Ceph(client1).fs.sub_volume.rm(cephfs_vol, cephfs_subvol_2)


def test_sv_creation_with_normalization_flags():
    log.info(" ** Creating subvolume with only normalization flag **")
    Ceph(client1).fs.sub_volume.create(
        cephfs_vol,
        cephfs_subvol_2,
        **{"normalization": "nfkd"},
    )

    get_charmap = Ceph(client1).fs.sub_volume.charmap.get(
        cephfs_vol,
        cephfs_subvol_2,
    )

    attr_util.validate_charmap_with_values(
        get_charmap,
        {
            "casesensitive": True,
            "normalization": "nfkd",
            "encoding": "utf8",
        },
    )

    get_subvolume = Ceph(client1).fs.sub_volume.info(
        cephfs_vol,
        cephfs_subvol_2,
    )

    attr_util.validate_charmap_with_values(
        extract_case_and_normalization(get_subvolume),
        {"casesensitive": True, "normalization": "nfkd"},
    )

    sub_vol_path_2 = common_util.subvolume_get_path(
        client1,
        cephfs_vol,
        subvolume_name=cephfs_subvol_2,
    )

    log.info("*** Mounting Subvolume 2 via FUSE and validating the normalization ***")
    fuse_mounting_dir_2 = common_util.setup_fuse_mount(
        client1,
        cephfs_vol,
        mount_path="/mnt/fuse_{}/".format(common_util.generate_mount_dir()),
        subvolume_name=cephfs_subvol_2,
        subvol_path=sub_vol_path_2,
    )
    log.info("Mounting dir for FUSE: {}".format(fuse_mounting_dir_2))

    unicode_name = attr_util.generate_random_unicode_names()[0]
    log.info("Unicode Dir Name: %s", unicode_name)

    child_dir_path = os.path.join(fuse_mounting_dir_2, "test-1", unicode_name)
    rel_child_dir = os.path.relpath(child_dir_path, fuse_mounting_dir_2)

    # Removing the first slash for the subvolumes
    actual_child_dir_root = os.path.join(
        sub_vol_path_2.strip().lstrip("/"), rel_child_dir.split("/")[0]
    )

    attr_util.create_directory(client1, child_dir_path, force=True)
    attr_util.validate_charmap(
        client1,
        fuse_mounting_dir_2,
        {"casesensitive": True, "normalization": "nfkd", "encoding": "utf8"},
    )
    log.debug("Root Child Dir Path: {}".format(actual_child_dir_root))
    log.debug("Unicode Name: {}".format(unicode_name))

    if not attr_util.validate_normalization(
        client1,
        cephfs_vol,
        actual_child_dir_root,
        unicode_name,
        "NFKD",
    ):
        raise NormalizationValidationError(
            "Normalization validation failed for {}".format(
                os.path.join(actual_child_dir_root, unicode_name)
            )
        )

    log.info("** Cleanup ** ")

    attr_util.delete_directory(client1, child_dir_path, recursive=True)
    fs_util.client_clean_up(
        "umount", fuse_clients=[client1], mounting_dir=fuse_mounting_dir_2
    )
    Ceph(client1).fs.sub_volume.rm(cephfs_vol, cephfs_subvol_2)


def test_case_sensitive_nfs_smb(mount_type):
    """Validate case sensitive flags across NFS and SMB."""

    log.debug("FUSE Mouunt dir: {}".format(fuse_mounting_dir))
    log.debug("NFS Mount dir: {}".format(nfs_mounting_dir))
    log.debug("SMB Mount dir: {}".format(smb_mounting_dir))

    if mount_type == "nfs":
        mount_dir = nfs_mounting_dir
    elif mount_type == "smb":
        mount_dir = smb_mounting_dir

    log.info("\n ** Validating for case sensitivity as True **\n")
    Ceph(client1).fs.sub_volume.charmap.set(
        cephfs_vol,
        cephfs_subvol,
        {
            "casesensitive": "true",
            "normalization": "nfd",
        },
        **{"group-name": cephfs_subvol_group1},
    )

    get_subvolume = Ceph(client1).fs.sub_volume.info(
        cephfs_vol, cephfs_subvol, **{"group-name": cephfs_subvol_group1}
    )

    attr_util.validate_charmap_with_values(
        extract_case_and_normalization(get_subvolume),
        {"casesensitive": True, "normalization": "nfd"},
    )

    log.info("Creating directories with case-sensitive names")
    folder_name_1 = "step-1"
    folder_name_2 = "Step-1"
    dir_path_1 = os.path.join(fuse_mounting_dir, folder_name_1)
    attr_util.create_directory(client1, dir_path_1)
    dir_path_2 = os.path.join(fuse_mounting_dir, folder_name_2)
    attr_util.create_directory(client1, dir_path_2)

    log.info("Validate the name exists in the {} directoy".format(mount_type))
    if not os.path.isdir(os.path.join(mount_dir, folder_name_1)) and os.path.isdir(
        os.path.join(mount_dir, folder_name_2)
    ):
        raise CaseSensitivityValidationError("Failed: NFS mount is not case sensitive")
    log.info("Passed: Both directories exist in the {} mount".format(mount_type))

    log.info("Removing the directories")
    attr_util.delete_directory(client1, dir_path_1, recursive=True)
    attr_util.delete_directory(client1, dir_path_2, recursive=True)

    log.info("\n ** Validating for case sensitivity as False **\n")
    Ceph(client1).fs.sub_volume.charmap.set(
        cephfs_vol,
        cephfs_subvol,
        {
            "casesensitive": "false",
            "normalization": "nfd",
        },
        **{"group-name": cephfs_subvol_group1},
    )

    get_subvolume = Ceph(client1).fs.sub_volume.info(
        cephfs_vol, cephfs_subvol, **{"group-name": cephfs_subvol_group1}
    )

    attr_util.validate_charmap_with_values(
        extract_case_and_normalization(get_subvolume),
        {"casesensitive": False, "normalization": "nfd"},
    )

    attr_util.create_directory(client1, dir_path_1)
    try:
        attr_util.create_directory(client1, dir_path_2)
        log.error("Expected to fail as case sensitivity is False")
        return 1
    except Exception as e:
        log.info("Passed: Failed as expected with error: {}".format(str(e)))

    if not attr_util.check_ls_case_sensitivity(
        client1, os.path.join(mount_dir, folder_name_1)
    ):
        raise CaseSensitivityValidationError(
            "Failed: Folder does not behave in non-case sensitive manner"
        )

    log.info("Removing the directories")
    attr_util.delete_directory(client1, dir_path_1, recursive=True)

    log.info(
        "Passed: Case sensitivity is working as expected for {} mount".format(
            mount_type
        )
    )


def test_normalization_nfs_smb(mount_type):
    """Validate normalization flags across NFS and SMB."""
    log.debug("FUSE Mouunt dir: {}".format(fuse_mounting_dir))
    log.debug("NFS Mount dir: {}".format(nfs_mounting_dir))
    log.debug("SMB Mount dir: {}".format(smb_mounting_dir))

    if mount_type == "nfs":
        mount_dir = nfs_mounting_dir
    elif mount_type == "smb":
        mount_dir = smb_mounting_dir

    log.info("\n ** Validating for normalization as NFC **\n")
    Ceph(client1).fs.sub_volume.charmap.set(
        cephfs_vol,
        cephfs_subvol,
        {
            "casesensitive": "true",
            "normalization": "nfc",
        },
        **{"group-name": cephfs_subvol_group1},
    )

    get_subvolume = Ceph(client1).fs.sub_volume.info(
        cephfs_vol, cephfs_subvol, **{"group-name": cephfs_subvol_group1}
    )

    attr_util.validate_charmap_with_values(
        extract_case_and_normalization(get_subvolume),
        {"casesensitive": True, "normalization": "nfc"},
    )

    log.info("Creating directories with normalization names")
    unicode_name = attr_util.generate_random_unicode_names()[0]
    log.info("Unicode Dir Name: %s", unicode_name)

    child_dir_path = os.path.join(mount_dir, "test-1", unicode_name)
    rel_child_dir = os.path.relpath(child_dir_path, mount_dir)

    # Removing the first slash for the subvolumes
    actual_child_dir_root = os.path.join(
        sub_vol_path_1.strip().lstrip("/"), rel_child_dir.split("/")[0]
    )

    attr_util.create_directory(client1, child_dir_path, force=True)
    log.debug("Root Child Dir Path: {}".format(actual_child_dir_root))
    log.debug("Unicode Name: {}".format(unicode_name))

    if not attr_util.validate_normalization(
        client1,
        cephfs_vol,
        actual_child_dir_root,
        unicode_name,
        "NFC",
    ):
        raise NormalizationValidationError(
            "Normalization validation failed for {}".format(
                os.path.join(actual_child_dir_root, unicode_name)
            )
        )

    log.info("Removing the directories")
    attr_util.delete_directory(client1, child_dir_path, recursive=True)


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
        global client1, installer, common_util, attr_util, fs_util, smb_nodes, ibm_build
        global cephfs_vol, cephfs_subvol, cephfs_subvol_2, cephfs_clone_subvol, cephfs_subvol_group1
        global cephfs_subvol_group2, cephfs_snap1, cephfs_rename_vol, cephfs_subvol_default

        attr_util = CephFSAttributeUtilities(ceph_cluster)
        common_util = CephFSCommonUtils(ceph_cluster)
        fs_util = FsUtils(ceph_cluster, test_data=test_data)
        fs_util.auth_list(clients)
        client1 = clients[0]
        installer = ceph_cluster.get_nodes(role="installer")[0]
        ibm_build = config.get("ibm_build", False)
        smb_nodes = ceph_cluster.get_nodes("smb")
        fs_util.prepare_clients([client1], build)

        cephfs_vol = "cs-volume-1"
        cephfs_rename_vol = "cs-volume-1-renamed"
        cephfs_subvol = "subvol1"
        cephfs_subvol_2 = "subvol2"
        cephfs_subvol_default = "subvol2"
        cephfs_clone_subvol = "subvol-clone1"
        cephfs_subvol_group1 = "subvolgroup1"
        cephfs_subvol_group2 = "subvolgroup2"
        cephfs_snap1 = "sv1-snap-1"

        Ceph(client1).fs.volume.create(cephfs_vol)

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 1: Create subvolume group1 and validate different charmap settings using charmap set/get "
            "\n---------------***************-----------------------------"
        )

        create_svgroup_charmap_test()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 2: [Negative] Try invalid/unsupported values for subvolumegroup charmap set "
            "\n---------------***************-----------------------------"
        )
        validate_svg_unsupported_charmaps()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 3: Create subvolume under subvolume group 2 where charmap set "
            "\n---------------***************-----------------------------"
        )
        test_sv_creation_with_charmap()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 4: [Negative] Try modifying charmaps of subvolume group 2 "
            "\n---------------***************-----------------------------"
        )
        modify_charmap_svg2()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 5: Create subvolume1 under subvolume group 1 and mount across different clients "
            "\n---------------***************-----------------------------"
        )
        log.info("Creating subvolume under subvolume group 1")
        Ceph(client1).fs.sub_volume.create(
            cephfs_vol,
            cephfs_subvol,
            **{"group-name": cephfs_subvol_group1, "mode": "0777"},
        )

        log.info("Declaring global variables for reuse")
        global fuse_mounting_dir, nfs_mounting_dir, smb_mounting_dir, nfs_params, smb_params
        fuse_mounting_dir = nfs_mounting_dir = smb_mounting_dir = ""

        log.info("Mounting Subvolume 1 via FUSE")
        sv1_mount()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 6: Validate attribute of subvolume1 created under subvolume group 1 "
            "\n---------------***************-----------------------------"
        )
        validate_charmap_sv1()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 7: Modify and validate attribute of subvolume1 created under subvolume group 1 "
            "\n---------------***************-----------------------------"
        )
        modify_and_validate_charmap_sv1()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 8: Snap, Clone subvolume1 and validate charmaps "
            "\n---------------***************-----------------------------"
        )
        snap_clone_sv_and_validate_charmap()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 9: Resize cloned subvolume and validate charmaps "
            "\n---------------***************-----------------------------"
        )
        resize_clone_sv_and_validate_charmap()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 10: Rename cloned subvolume and validate charmaps "
            "\n---------------***************-----------------------------"
        )
        rename_clone_sv_and_validate_charmap()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 11: Run IO on the subvolume and validate charmaps "
            "\n---------------***************-----------------------------"
        )
        run_io_sv_and_validate_charmap()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 12: Create subvolume in default group and validate normalization values "
            "\n---------------***************-----------------------------"
        )
        sv_default_validate_charmap()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 13: Remove charmap attribute from subvolume "
            "\n---------------***************-----------------------------"
        )
        remove_charmap_and_validate()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 14: Create subvolume using only case-sensitive flags and validate "
            "\n---------------***************-----------------------------"
        )

        test_sv_creation_with_casesensitive_flags()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 15: Create subvolume using only normalization flags and validate "
            "\n---------------***************-----------------------------"
        )

        test_sv_creation_with_normalization_flags()

        log.info(
            "\n"
            "\n---------------***************-----------------------------"
            "\n  Usecase 16: Validate case sensitive and normalization across NFS"
            "\n---------------***************-----------------------------"
        )

        sv1_mount(mount_type="nfs")

        log.info("\n ** TC: Validate case sensitive in NFS directories ** ")
        test_case_sensitive_nfs_smb(mount_type="nfs")

        log.info("\n ** TC: Validate normalization in NFS directories ** ")
        test_normalization_nfs_smb(mount_type="nfs")

        log.info("Unmounting and cleaning up the NFS mounts")
        fs_util.client_clean_up(
            "umount", kernel_clients=[client1], mounting_dir=nfs_mounting_dir
        )

        fs_util.remove_nfs_export(
            client1,
            nfs_params.get("nfs_cluster_name"),
            nfs_params.get("binding"),
        )

        fs_util.remove_nfs_cluster(client1, nfs_params.get("nfs_cluster_name"))

        if ibm_build:
            log.info(
                "\n"
                "\n---------------***************-----------------------------"
                "\n  Usecase 17: Validate case sensitive and normalization across SMB "
                "\n---------------***************-----------------------------"
            )
            sv1_mount(mount_type="smb")

            log.info("\n ** TC: Validate case sensitive in SMB directories ** ")
            test_case_sensitive_nfs_smb(mount_type="smb")

            log.info("\n ** TC: Validate normalization in SMB directories ** ")
            test_normalization_nfs_smb(mount_type="smb")

        log.info("*** Passed: Completed Subvolume Charmap tests successfully ***")
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

        log.info("Unmounting and cleaning up the FUSE mounts")
        fs_util.client_clean_up(
            "umount", fuse_clients=[client1], mounting_dir=fuse_mounting_dir
        )

        if ibm_build and smb_mounting_dir != "":
            log.info("Unmounting and cleaning up the SMB mounts")
            client1.exec_command(
                sudo=True,
                cmd=f"rm -rf {smb_mounting_dir}/*",
            )

            client1.exec_command(
                sudo=True,
                cmd=f"umount {smb_mounting_dir}",
            )

            remove_smb_share(
                smb_params.get("installer"),
                smb_params.get("smb_shares"),
                smb_params.get("smb_cluster_id"),
            )

            remove_smb_cluster(
                smb_params.get("installer"), smb_params.get("smb_cluster_id")
            )

        log.info("Removing Volume")
        fs_util.remove_fs(client1, cephfs_vol)
        log.info("Cleanup completed successfully.")
