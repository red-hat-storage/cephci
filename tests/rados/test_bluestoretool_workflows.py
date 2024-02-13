"""
Test Module to perform specific functionalities of ceph-bluestore-tool.
 - ceph-bluestore-tool fsck|repair --path osd path [ --deep ]
 - ceph-bluestore-tool qfsck --path osd path
 - ceph-bluestore-tool allocmap --path osd path
 - ceph-bluestore-tool show-label --dev device
 - ceph-bluestore-tool prime-osd-dir --dev device --path osd path
 - ceph-bluestore-tool bluefs-export --path osd path --out-dir dir
 - ceph-bluestore-tool bluefs-bdev-sizes --path osd path
 - ceph-bluestore-tool bluefs-bdev-expand --path osd path
 - ceph-bluestore-tool free-dump|free-score --path osd path [--allocator block/bluefs-wal/bluefs-db/bluefs-slow]
 - ceph-bluestore-tool show-sharding --path osd path
 - ceph-bluestore-tool bluefs-stats --path osd path
"""
import json
import random
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.bluestoretool_workflows import BluestoreToolWorkflows
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    # CEPH-83571692
    Test to perform +ve workflows for the ceph-bluestore-tool utility
    Returns:
        1 -> Fail, 0 -> Pass
    *** Currently, covers commands/workflows valid only for collocated OSDs
    Commands reserved for future coverage with non-collocated OSDs:
    - ceph-bluestore-tool reshard --path osd path --sharding new sharding [ --sharding-ctrl control string ]
    - ceph-bluestore-tool bluefs-bdev-new-db --path osd path --dev-target new-device
    - ceph-bluestore-tool bluefs-bdev-migrate --path osd path --dev-target new-device --devs-source device1
    - ceph-bluestore-tool restore_cfb --path osd path
    """
    log.info(run.__doc__)
    config = kw["config"]
    rhbuild = config.get("rhbuild")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    bluestore_obj = BluestoreToolWorkflows(node=cephadm)
    client = ceph_cluster.get_nodes(role="client")[0]

    out, _ = cephadm.shell(args=["ceph osd ls"])
    osd_list = out.strip().split("\n")

    try:
        if config.get("non-collocated"):
            # Execute ceph-bluestore-tool --help
            osd_id = random.choice(osd_list)
            log.info(
                f"\n --------------------"
                f"\n Running cbt help for OSD {osd_id}"
                f"\n --------------------"
            )
            out = bluestore_obj.help(osd_id=osd_id)
            log.info(out)

            # Add a new dedicated WAL device to existing collocated OSD
            # ceph-bluestore-tool bluefs-bdev-new-wal --path osd path --dev-target new-device
            osd_id = random.choice(osd_list)
            log.info(
                f"\n --------------------"
                f"\n Adding a dedicated WAL device for OSD.{osd_id}"
                f"\n --------------------"
            )
            osd_host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
            empty_devices = rados_obj.get_available_devices(
                node=osd_host, device_type="hdd"
            )
            log.info(
                f"List of available devices on osd host {osd_host}: {empty_devices}"
            )
            log.info(f"WAL will be added on device: {empty_devices[0]}")

            out = bluestore_obj.add_wal_device(
                osd_id=osd_id, new_device=empty_devices[0]
            )
            assert f"WAL device added {empty_devices[0]}" in out
            osd_metadata = ceph_cluster.get_osd_metadata(
                osd_id=int(osd_id), client=client
            )
            assert int(osd_metadata["bluefs_dedicated_wal"]) == 1
            assert (
                osd_metadata["bluefs_wal_dev_node"]
                == osd_metadata["bluefs_wal_partition_path"]
                == empty_devices[0]
            )
        else:
            # Execute ceph-bluestore-tool --help
            osd_id = random.choice(osd_list)
            log.info(
                f"\n --------------------"
                f"\n Running cbt help for OSD {osd_id}"
                f"\n --------------------"
            )
            out = bluestore_obj.help(osd_id=osd_id)
            log.info(out)

            # Execute ceph-bluestore-tool fsck --path <osd_path>
            osd_id = random.choice(osd_list)
            log.info(
                f"\n --------------------"
                f"\n Running consistency check for OSD {osd_id}"
                f"\n --------------------"
            )
            out = bluestore_obj.run_consistency_check(osd_id=osd_id)
            log.info(out)
            assert "success" in out

            # Execute ceph-bluestore-tool fsck --deep --path <osd_path>
            osd_id = random.choice(osd_list)
            log.info(
                f"\n --------------------"
                f"\n Running deep consistency check for OSD {osd_id}"
                f"\n --------------------"
            )
            out = bluestore_obj.run_consistency_check(osd_id=osd_id, deep=True)
            log.info(out)
            assert "success" in out

            if rhbuild.split(".")[0] >= "6":
                # Execute ceph-bluestore-tool qfsck --path <osd_path>
                osd_id = random.choice(osd_list)
                log.info(
                    f"\n --------------------"
                    f"\n Running quick consistency check for OSD {osd_id}"
                    f"\n --------------------"
                )
                out = bluestore_obj.run_quick_consistency_check(osd_id=osd_id)
                log.info(out)
                assert "success" in out

                # Execute ceph-bluestore-tool allocmap --path <osd_path>
                osd_id = random.choice(osd_list)
                log.info(
                    f"\n --------------------"
                    f"\n Fetching allocmap for OSD {osd_id}"
                    f"\n ---------------------"
                )
                out = bluestore_obj.fetch_allocmap(osd_id=osd_id)
                log.info(out)
                assert "success" in out

            # Execute ceph-bluestore-tool repair --path <osd_path>
            osd_id = random.choice(osd_list)
            log.info(
                f"\n --------------------"
                f"\n Run BlueFS repair for OSD {osd_id}"
                f"\n --------------------"
            )
            out = bluestore_obj.repair(osd_id=osd_id)
            log.info(out)
            assert "success" in out

            """
            # Execute ceph-bluestore-tool restore_cfb --path <osd_path>
            osd_id = random.choice(osd_list)
            log.info(f"\n --------------------"
                     f"\n Restoring Column-Family B for OSD {osd_id}"
                     f"\n --------------------")
            out = bluestore_obj.restore_cfb(osd_id=osd_id)
            log.info(out)
    
            Execution failed with below msg -
            restore_cfb failed: (1) Operation not permitted
            7fe3c5c80600 -1 bluestore::NCB::push_allocation_to_rocksdb::cct->_conf->bluestore_allocation_from_file
            must be cleared first
            7fe3c5c80600 -1 bluestore::NCB::push_allocation_to_rocksdb::please change default to false in ceph.conf file>
            *** Needs further investigation as upstream documentation says this command is supposed to reserve changes
            done by the new NCB code | restore_cfb: Reverses changes done by the new NCB code (either through
             ceph restart or when running allocmap command) and restores RocksDB B Column-Family (allocator-map).
            The failure may only be applicable in certain scenarios, BZ will be raised if found otherwise.
            https://docs.ceph.com/en/quincy/man/8/ceph-bluestore-tool/#commands
            """

            # Execute ceph-bluestore-tool bluefs-export --out-dir <dir> --path <osd_path>
            osd_id = random.choice(osd_list)
            log.info(
                f"\n --------------------"
                f"\n Exporting BlueFS contents to an output directory for OSD {osd_id}"
                f"\n --------------------"
            )
            out = bluestore_obj.do_bluefs_export(osd_id=osd_id, output_dir="/tmp/")
            log.info(out)
            assert f"/var/lib/ceph/osd/ceph-{osd_id}/" in out

            # Execute ceph-bluestore-tool bluefs-bdev-sizes --path <osd_path>
            osd_id = random.choice(osd_list)
            log.info(
                f"\n --------------------"
                f"\n Print the device sizes, as understood by BlueFS, to stdout for OSD {osd_id}"
                f"\n --------------------"
            )
            out = bluestore_obj.block_device_sizes(osd_id=osd_id)
            log.info(out)
            assert "device size" in out

            # Execute ceph-bluestore-tool bluefs-bdev-expand --path <osd_path>
            osd_id = random.choice(osd_list)
            log.info(
                f"\n --------------------"
                f"\n Checking if size of block device is expanded for OSD {osd_id}"
                f"\n --------------------"
            )
            out = bluestore_obj.block_device_expand(osd_id=osd_id)
            log.info(out)
            assert "device size" in out and "Expanding" in out

            # Execute ceph-bluestore-tool show-label
            log.info(
                f"\n --------------------"
                f"\n Dump label content for block device for OSD {osd_id}"
                f"\n --------------------"
            )
            out = bluestore_obj.show_label(osd_id=osd_id)
            log.info(out)
            assert f"/var/lib/ceph/osd/ceph-{osd_id}/" in out

            # Execute ceph-bluestore-tool show-label --dev <device>
            for device in ["block", "db", "wal"]:
                osd_id = random.choice(osd_list)
                osd_node = rados_obj.fetch_host_node(
                    daemon_type="osd", daemon_id=str(osd_id)
                )
                lvm_list, _ = osd_node.exec_command(
                    sudo=True,
                    cmd=f"cephadm shell -- ceph-volume lvm list {osd_id} --format json",
                )
                lvm_json = json.loads(lvm_list)
                dev = lvm_json[f"{osd_id}"][0]["tags"]["ceph.block_device"]
                device_type = lvm_json[f"{osd_id}"][0]["type"]
                if device in device_type:
                    log.info(
                        f"\n --------------------"
                        f"\n Dump label content for {device_type} device {dev} for OSD {osd_id}"
                        f"\n --------------------"
                    )
                    out = bluestore_obj.show_label(osd_id=osd_id, device=dev)
                    log.info(out)
                    assert dev in out

            # Execute ceph-bluestore-tool prime-osd-dir --dev <main_device> --path <osd_path>
            osd_id = random.choice(osd_list)
            osd_node = rados_obj.fetch_host_node(
                daemon_type="osd", daemon_id=str(osd_id)
            )
            lvm_list, _ = osd_node.exec_command(
                sudo=True,
                cmd=f"cephadm shell -- ceph-volume lvm list {osd_id} --format json",
            )
            lvm_json = json.loads(lvm_list)
            dev = lvm_json[f"{osd_id}"][0]["tags"]["ceph.block_device"]
            log.info(
                f"\n --------------------"
                f"\n Generate the content for an OSD data directory "
                f"that can start up a BlueStore OSD for OSD {osd_id}"
                f"\n --------------------"
            )
            out = bluestore_obj.generate_prime_osd_dir(osd_id=osd_id, device=dev)
            log.info(out)

            # Execute ceph-bluestore-tool free-dump --path <osd_path> [--allocator block/bluefs-wal/bluefs-db/bluefs-slow]
            osd_id = random.choice(osd_list)
            log.info(
                f"\n --------------------"
                f"\n Dump all free regions in allocator for OSD {osd_id}"
                f"\n --------------------"
            )
            out = (
                bluestore_obj.get_free_dump(osd_id=osd_id)
                if rhbuild.split(".")[0] >= "6"
                else bluestore_obj.get_free_dump(osd_id=osd_id, allocator_type="block")
            )
            log.debug(out)
            assert "alloc_name" in out and "extents" in out

            # Execute ceph-bluestore-tool free-score --path <osd_path> [--allocator block/bluefs-wal/bluefs-db/bluefs-slow]
            osd_id = random.choice(osd_list)
            log.info(
                f"\n --------------------"
                f"\n Get fragmentation score for OSD {osd_id}"
                f"\n --------------------"
            )
            out = (
                bluestore_obj.get_free_score(osd_id=osd_id)
                if rhbuild.split(".")[0] >= "6"
                else bluestore_obj.get_free_score(osd_id=osd_id, allocator_type="block")
            )
            log.info(out)
            assert "fragmentation_rating" in out

            # Execute ceph-bluestore-tool show-sharding --path <osd_path>
            osd_id = random.choice(osd_list)
            log.info(
                f"\n --------------------"
                f"\n Show sharding that is currently applied to "
                f"BlueStore's RocksDB for OSD {osd_id}"
                f"\n --------------------"
            )
            out = bluestore_obj.show_sharding(osd_id=osd_id)
            log.info(out)
            assert "block_cache" in out

            # Execute ceph-bluestore-tool bluefs-stats --path <osd_path>
            osd_id = random.choice(osd_list)
            log.info(
                f"\n --------------------"
                f"\n Display BlueFS statistics for OSD {osd_id}"
                f"\n --------------------"
            )
            out = bluestore_obj.show_bluefs_stats(osd_id=osd_id)
            log.info(out)
            if rhbuild.split(".")[0] >= "7":
                for pattern in [
                    "device size",
                    "DEV/LEV",
                    "LOG",
                    "WAL",
                    "DB",
                    "SLOW",
                    "MAXIMUMS",
                    "TOTAL",
                ]:
                    assert pattern in out
            else:
                for pattern in ["device size", "wal_total", "db_total", "slow_total"]:
                    assert pattern in out

            # restart OSD services
            osd_services = rados_obj.list_orch_services(service_type="osd")
            for osd_service in osd_services:
                cephadm.shell(args=[f"ceph orch restart {osd_service}"])
            time.sleep(30)

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        return 1
    log.info("Completed verification of Ceph-BlueStore-Tool commands.")
    return 0
