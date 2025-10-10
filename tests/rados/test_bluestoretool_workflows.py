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
 - ceph-bluestore-tool reshard --path osd path --sharding new sharding [ --sharding-ctrl control string ]
 - ceph-bluestore-tool bluefs-bdev-new-wal --path osd path --dev-target new-device
 - ceph-bluestore-tool bluefs-bdev-new-db --path osd path --dev-target new-device
"""

import datetime
import math
import random
import time

from ceph.ceph_admin import CephAdmin
from ceph.rados.bluestoretool_workflows import BluestoreToolWorkflows
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.rados_bench import RadosBench
from ceph.rados.serviceability_workflows import ServiceabilityMethods
from ceph.utils import get_node_by_id
from tests.misc_env.lvm_deployer import create_lvms
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
    - ceph-bluestore-tool restore_cfb --path osd path
    - ceph-bluestore-tool bluefs-bdev-migrate --path osd path --dev-target new-device --devs-source device1
    """
    log.info(run.__doc__)
    config = kw["config"]
    rhbuild = config.get("rhbuild")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    bluestore_obj = BluestoreToolWorkflows(node=cephadm)
    bluestore_obj_live = BluestoreToolWorkflows(node=cephadm, nostop=True)
    client = ceph_cluster.get_nodes(role="client")[0]
    bench_obj = RadosBench(mon_node=cephadm, clients=client)
    service_obj = ServiceabilityMethods(cluster=ceph_cluster, **config)

    try:
        osd_list = rados_obj.get_osd_list(status="up")
        log.info(f"List of OSDs: \n{osd_list}")

        if config.get("non-collocated"):
            log.info(
                "\n\n Execution begins for CBT non-collocated scenarios ************ \n\n"
            )
            # Execute ceph-bluestore-tool --help
            osd_id = random.choice(osd_list)
            log.info(
                f"\n ---------------------------------"
                f"\n Running cbt help for OSD {osd_id}"
                f"\n ---------------------------------"
            )
            out = bluestore_obj.help(osd_id=osd_id)
            log.info(out)

            # Add a new dedicated WAL device to existing collocated OSD
            # ceph-bluestore-tool bluefs-bdev-new-wal --path osd path --dev-target new-device
            osd_id = random.choice(osd_list)
            log.info(
                f"\n -------------------------------------------"
                f"\n Adding a dedicated WAL device for OSD.{osd_id}"
                f"\n -------------------------------------------"
            )
            osd_host = rados_obj.fetch_host_node(daemon_type="osd", daemon_id=osd_id)
            empty_devices = rados_obj.get_available_devices(
                node_name=osd_host.hostname, device_type="hdd"
            )
            if not empty_devices:
                log.error(f"No spare disks available on OSD host {osd_host.hostname}")
                raise Exception(
                    f"No spare disks available on OSD host {osd_host.hostname}"
                )

            # determine size of db and wal lvms
            osd_df_stats = rados_obj.get_osd_df_stats(
                tree=False, filter_by="name", filter=f"osd.{osd_id}"
            )
            osd_size = osd_df_stats["nodes"][0]["kb"]
            log.info(osd_size)
            db_size = int(int(osd_size) * 1024 * 0.05)
            log.info(db_size)
            # covert bytes to GB
            wal_db_size = f"{math.ceil(db_size/1073741824)}G"
            log.info(wal_db_size)

            log.info(
                f"List of available devices on osd host {osd_host.hostname}: {empty_devices}"
            )

            lvm_list = create_lvms(
                node=osd_host, count=6, size=wal_db_size, devices=[empty_devices[0]]
            )
            log.info(f"List of LVMs created on {osd_host.hostname}: {lvm_list}")

            wal_target = lvm_list.pop()
            log.info(f"WAL will be added on device: {wal_target}")

            out = bluestore_obj.add_wal_device(osd_id=osd_id, new_device=wal_target)
            log.info(out)
            assert "WAL device added" in out

            for _ in range(3):
                osd_metadata = ceph_cluster.get_osd_metadata(
                    osd_id=int(osd_id), client=client
                )
                log.debug(f"OSD metadata for osd.{osd_id}: \n {osd_metadata}")

                if int(osd_metadata["bluefs_dedicated_wal"]) == 1:
                    log_info_msg = "'bluefs_dedicated_wal' entry in OSD metadata is 1"
                    log.info(log_info_msg)
                    break

                log_info_msg = "'bluefs_dedicated_wal' entry in OSD metadata is not 1. Retrying after 30 seconds."
                log.info(log_info_msg)
                time.sleep(30)
            else:
                log.error("'bluefs_dedicated_wal' entry in OSD metadata is not 1")
                raise AssertionError(
                    "'bluefs_dedicated_wal' entry in OSD metadata is not 1"
                )

            if not (
                osd_metadata["bluefs_wal_dev_node"]
                == osd_metadata["bluefs_wal_partition_path"]
            ):
                log.error(
                    "bluefs wal device node and bluefs wal partition path do not match"
                )
                raise AssertionError(
                    "bluefs wal device node and bluefs wal partition path do not match"
                )

            if not osd_metadata["bluefs_wal_devices"] in empty_devices[0]:
                log.error(f"bluefs WAL device is not pointing to {empty_devices[0]}")
                raise AssertionError(
                    f"bluefs WAL device is not pointing to {empty_devices[0]}"
                )

            log.info(
                f"Dedicated WAL device successfully added for OSD {osd_id} on {empty_devices[0]}"
            )

            # Add a new DB device to existing collocated OSD
            # ceph-bluestore-tool bluefs-bdev-new-db --path osd path --dev-target new-device
            log.info(
                f"\n -----------------------------------------------"
                f"\n Adding a dedicated DB device for OSD.{osd_id}"
                f"\n -----------------------------------------------"
            )
            db_target = lvm_list.pop()
            log.info(f"DB will be added on device: {db_target}")

            out = bluestore_obj.add_db_device(
                osd_id=osd_id, new_device=db_target, db_size=db_size
            )
            log.info(out)
            assert "DB device added" in out

            for _ in range(3):
                osd_metadata = ceph_cluster.get_osd_metadata(
                    osd_id=int(osd_id), client=client
                )
                log.debug(f"OSD metadata for osd.{osd_id}: \n {osd_metadata}")

                if int(osd_metadata["bluefs_dedicated_db"]) == 1:
                    log_info_msg = "'bluefs_dedicated_db' entry in OSD metadata is 1"
                    log.info(log_info_msg)
                    break

                log_info_msg = "'bluefs_dedicated_db' entry in OSD metadata is not 1. Retrying after 30 seconds."
                log.info(log_info_msg)
                time.sleep(30)
            else:
                log.error("'bluefs_dedicated_db' entry in OSD metadata is not 1")
                raise AssertionError(
                    "'bluefs_dedicated_db' entry in OSD metadata is not 1"
                )

            if not (
                osd_metadata["bluefs_db_dev_node"]
                == osd_metadata["bluefs_db_partition_path"]
            ):
                log.error(
                    "bluefs DB device node and bluefs DB partition path do not match"
                )
                raise AssertionError(
                    "bluefs DB device node and bluefs DB partition path do not match"
                )

            if not osd_metadata["bluefs_db_devices"] in empty_devices[0]:
                log.error(f"bluefs DB device is not pointing to {empty_devices[0]}")
                raise AssertionError(
                    f"bluefs DB device is not pointing to {empty_devices[0]}"
                )

            log.info(
                f"Dedicated DB device successfully added for OSD {osd_id} on {empty_devices[0]}"
            )

            # Migrating new DB device to existing collocated OSD
            # ceph-bluestore-tool bluefs-bdev-migrate --dev-target {new_device} --devs-source {device_source}
            # Not part of execution due to blocker BZ-2269101
            if not True:
                log.info(
                    f"\n -----------------------------------------------"
                    f"\n Migrating a dedicated DB device for OSD.{osd_id}"
                    f"\n -----------------------------------------------"
                )
                existing_db_device = f"dev/{osd_metadata['bluefs_db_devices']}"
                empty_devices = rados_obj.get_available_devices(
                    node_name=osd_host.hostname, device_type="hdd"
                )
                log.info(
                    f"List of available devices on osd host {osd_host.hostname}: {empty_devices}"
                )
                log.info(f"Existing DB will be migrated to device: {empty_devices[0]}")
                out = bluestore_obj.block_device_migrate(
                    osd_id=osd_id,
                    new_device=empty_devices[0],
                    device_source=existing_db_device,
                )
                log.info(out)
                log.debug(f"OSD metadata for osd.{osd_id}: \n {osd_metadata}")
        if config.get("bluefs-spillover"):
            log.info(
                "\n\n ******** Running test to check BlueFS spillover **********"
                "\n CEPH-83595766"
                "\n BZ-2129414 \n\n"
            )

            # addition of small device as wal/db fails with CBT
            # BZ - https://bugzilla.redhat.com/show_bug.cgi?id=2309610
            if False:
                osd_id = random.choice(osd_list)

                log.info(
                    f"\n -------------------------------------------"
                    f"\n Adding a dedicated WAL/DB device for OSD.{osd_id}"
                    f"\n -------------------------------------------"
                )
                osd_metadata = ceph_cluster.get_osd_metadata(
                    osd_id=int(osd_id), client=client
                )
                log.debug(f"OSD metadata for osd.{osd_id}: \n {osd_metadata}")

                if (
                    int(osd_metadata["bluefs_dedicated_wal"]) == 1
                    or int(osd_metadata["bluefs_dedicated_db"]) == 1
                ):
                    log.info(
                        f"OSD.{osd_id} already has WAL/DB partition, choosing another OSD"
                    )

                    osd_id = random.choice(osd_list)
                    log.info(
                        f"\n -------------------------------------------"
                        f"\n Adding a dedicated WAL/DB device for OSD.{osd_id}"
                        f"\n -------------------------------------------"
                    )

                osd_host = rados_obj.fetch_host_node(
                    daemon_type="osd", daemon_id=osd_id
                )
                empty_devices = rados_obj.get_available_devices(
                    node_name=osd_host.hostname, device_type="hdd"
                )
                if not empty_devices:
                    log.error(
                        f"No spare disks available on OSD host {osd_host.hostname}"
                    )
                    raise Exception(
                        f"No spare disks available on OSD host {osd_host.hostname}"
                    )

                db_size = 4 << 20
                log.info(db_size)
                wal_db_size = "4M"
                log.info(wal_db_size)

                log.info(
                    f"List of available devices on osd host {osd_host.hostname}: {empty_devices}"
                )

                lvm_list = create_lvms(
                    node=osd_host, count=2, size=wal_db_size, devices=[empty_devices[0]]
                )
                log.info(f"List of LVMs created on {osd_host.hostname}: {lvm_list}")
                db_target = lvm_list.pop()
                log.info(f"DB will be added on device: {db_target}")
                out = bluestore_obj.add_db_device(
                    osd_id=osd_id, new_device=db_target, db_size=db_size
                )
                log.info(out)
                assert "DB device added" in out
                osd_metadata = ceph_cluster.get_osd_metadata(
                    osd_id=int(osd_id), client=client
                )
                log.debug(f"OSD metadata for osd.{osd_id}: \n {osd_metadata}")

                if not int(osd_metadata["bluefs_dedicated_db"]) == 1:
                    log.error("'bluefs_dedicated_db' entry in OSD metadata is not 1")
                    raise AssertionError(
                        "'bluefs_dedicated_db' entry in OSD metadata is not 1"
                    )

                wal_target = lvm_list.pop()
                log.info(f"WAL will be added on device: {wal_target}")

                out = bluestore_obj.add_wal_device(osd_id=osd_id, new_device=wal_target)
                log.info(out)
                assert "WAL device added" in out
                osd_metadata = ceph_cluster.get_osd_metadata(
                    osd_id=int(osd_id), client=client
                )
                log.debug(f"OSD metadata for osd.{osd_id}: \n {osd_metadata}")

                if not int(osd_metadata["bluefs_dedicated_wal"]) == 1:
                    log.error("'bluefs_dedicated_wal' entry in OSD metadata is not 1")
                    raise AssertionError(
                        "'bluefs_dedicated_wal' entry in OSD metadata is not 1"
                    )

            # using additional host node13 to add OSD
            node13_obj = get_node_by_id(ceph_cluster, "node13")
            empty_devices = rados_obj.get_available_devices(
                node_name=node13_obj.hostname, device_type="hdd"
            )
            if len(empty_devices) < 2:
                log.error(
                    f"Need at least 2 spare disks available on host {node13_obj.hostname}"
                )
                raise Exception(
                    f"Two spare disks not available on host {node13_obj.hostname}"
                )

            wal_db_size = "4M"
            log.info(wal_db_size)

            log.info(
                f"List of available devices on osd host {node13_obj.hostname}: {empty_devices}"
            )

            lvm_list = create_lvms(
                node=node13_obj, count=2, size=wal_db_size, devices=[empty_devices[0]]
            )
            log.info(f"List of LVMs created on {node13_obj.hostname}: {lvm_list}")

            # manually add OSD with wal/db on node13
            db_target = lvm_list.pop()
            log.info(f"DB will be added on device: {db_target}")
            data_dev = empty_devices[1]
            log.info(f"Data device will be on disk: {data_dev}")
            add_cmd = f"ceph orch daemon add osd {node13_obj.hostname}:data_devices={data_dev},db_devices={db_target}"
            out, err = cephadm.shell(args=[add_cmd])

            if not ("Created osd" in out and f"on host '{node13_obj.hostname}" in out):
                log.error(f"OSD addition on {node13_obj.hostname} failed")
                log.error(err)
                raise Exception(f"OSD addition on {node13_obj.hostname} failed")
            time.sleep(30)
            assert service_obj.add_osds_to_managed_service()

            node13_osds = rados_obj.collect_osd_daemon_ids(osd_node=node13_obj)
            log.info(f"OSD IDs on {node13_obj.hostname}: {node13_osds}")

            # create a data pool and fill cluster til 20% capacity
            assert rados_obj.create_pool(pool_name="db-spillover")

            # determine the number of objects to be written to the pool
            # to achieve 20% utilization
            bench_obj_size_kb = 16384

            # determine how much % cluster is already filled
            current_fill_ratio = rados_obj.get_cephdf_stats()["stats"][
                "total_used_raw_ratio"
            ]

            osd_df_stats = rados_obj.get_osd_df_stats(
                tree=False, filter_by="name", filter="osd.0"
            )
            osd_size = osd_df_stats["nodes"][0]["kb"]
            num_objs = int(
                (osd_size * (0.2 - current_fill_ratio) / bench_obj_size_kb) + 1
            )

            # fill the cluster till 20% capacity
            bench_config = {
                "seconds": 300,
                "b": f"{bench_obj_size_kb}KB",
                "no-cleanup": True,
                "max-objects": num_objs,
            }
            bench_obj.write(client=client, pool_name="db-spillover", **bench_config)
            time.sleep(10)

            # log the cluster and pool fill %
            cluster_fill = (
                int(rados_obj.get_cephdf_stats()["stats"]["total_used_raw_ratio"]) * 100
            )
            pool_fill = (
                int(
                    rados_obj.get_cephdf_stats(pool_name="db-spillover")["stats"][
                        "percent_used"
                    ]
                )
                * 100
            )

            log.info(f"Cluster fill %: {cluster_fill}")
            log.info(f"Pool db-spillover fill %: {pool_fill}")

            # monitor ceph health for db spillover warning
            # smart wait for 240 secs to check BlueFS spillover warning
            timeout_time = datetime.datetime.now() + datetime.timedelta(seconds=240)
            while datetime.datetime.now() < timeout_time:
                health_detail, _ = cephadm.shell(args=["ceph health detail"])
                log.info(f"Health warning: \n {health_detail}")
                if "experiencing BlueFS spillover" not in health_detail:
                    log.error("BlueFS spillover yet to be generated")
                    log.info("sleeping for 30 secs")
                    time.sleep(30)
                    continue
                break
            else:
                log.error("Expected BlueFS spillover did not show up within timeout")
                raise Exception(
                    "Expected BlueFS spillover did not show up within timeout"
                )

            log.info("BlueFS spillover warning successfully generated")
        else:
            log.info(
                "\n\n Execution begins for CBT collocated scenarios ************ \n\n"
            )
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
            7fe3c5c80600 -1 bluestore::NCB::push_allocation_to_rocksdb::please
             change default to false in ceph.conf file>
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
            osd_id = random.choice(osd_list)
            log.info(
                f"\n --------------------"
                f"\n Dump label content for block device for live OSD {osd_id}"
                f"\n --------------------"
            )
            out = bluestore_obj_live.show_label(osd_id=osd_id)
            log.info(out)
            assert f"/var/lib/ceph/osd/ceph-{osd_id}/" in out

            log.info(
                f"\n --------------------"
                f"\n Dump label content for block device for stopped OSD {osd_id}"
                f"\n --------------------"
            )
            out = bluestore_obj.show_label(osd_id=osd_id)
            log.info(out)
            assert f"/var/lib/ceph/osd/ceph-{osd_id}/" in out

            osd_id = random.choice(osd_list)
            osd_metadata = ceph_cluster.get_osd_metadata(
                osd_id=int(osd_id), client=client
            )
            osd_meta_devices = [
                value
                for key, value in osd_metadata.items()
                if key
                in [
                    "bluestore_bdev_partition_path",
                    "bluefs_db_partition_path",
                    "bluefs_wal_partition_path",
                ]
                and value
            ]

            # Execute ceph-bluestore-tool show-label --dev <device>
            for dev in osd_meta_devices:
                log.info(
                    f"\n ----------------------------------------"
                    f"\n Dump label content for device {dev} for OSD {osd_id}"
                    f"\n ----------------------------------------"
                )
                out = bluestore_obj.show_label(osd_id=osd_id, device=dev)
                log.info(out)
                assert dev in out

            # Execute ceph-bluestore-tool prime-osd-dir --dev <main_device> --path <osd_path>
            osd_id = random.choice(osd_list)
            osd_node = rados_obj.fetch_host_node(
                daemon_type="osd", daemon_id=str(osd_id)
            )

            osd_metadata = ceph_cluster.get_osd_metadata(
                osd_id=int(osd_id), client=client
            )
            dev = f"{osd_metadata['osd_data']}/block"
            log.info(
                f"\n --------------------"
                f"\n Generate the content for an OSD data directory "
                f"that can start up a BlueStore OSD for OSD {osd_id}"
                f"\n --------------------"
            )
            out = bluestore_obj_live.generate_prime_osd_dir(osd_id=osd_id, device=dev)
            log.info(out)
            out = bluestore_obj.generate_prime_osd_dir(osd_id=osd_id, device=dev)
            log.info(out)

            # ceph-bluestore-tool free-dump --path <osd_path> [--allocator block/bluefs-wal/bluefs-db/bluefs-slow]
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

            # ceph-bluestore-tool free-score --path <osd_path> [--allocator block/bluefs-wal/bluefs-db/bluefs-slow]
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

            # Execute ceph-bluestore-tool --sharding="<>" reshard --path <osd_path>
            osd_id = random.choice(osd_list)
            log.info(
                f"\n -----------------------------------------"
                f"\n Reshard with custom value for for OSD {osd_id}"
                f"\n -----------------------------------------"
            )
            new_shard = "m(3) p(3,0-12) O(3,0-13)=block_cache={type=binned_lru} L P"
            log.info(f"New shard: {new_shard}")
            out = bluestore_obj.do_reshard(osd_id=osd_id, new_shard=new_shard)
            log.info(out)
            assert "reshard success" in out
            out = bluestore_obj.show_sharding(osd_id=osd_id)
            log.info(f"Output of show_sharding after new shard was applied: \n {out}")
            assert new_shard in out

            # Execute ceph-bluestore-tool bluefs-stats --path <osd_path>
            osd_id = random.choice(osd_list)
            log.info(
                f"\n --------------------"
                f"\n Display BlueFS statistics for OSD {osd_id}"
                f"\n --------------------"
            )
            out = bluestore_obj.show_bluefs_stats(osd_id=osd_id)
            log.info(out)
            if rhbuild.startswith("6"):
                log.info(
                    "Check if the chosen OSD has a dedicated DB device. Fetching metadata..."
                )
                osd_metadata = ceph_cluster.get_osd_metadata(
                    osd_id=int(osd_id), client=client
                )
                # 1 if dedicated DB device is present, 0 otherwise
                dedicated_db = int(osd_metadata["bluefs_dedicated_db"])

            # default bluefs-stats for releases older than Reef
            pattern_list = ["device size", "wal_total", "db_total", "slow_total"]

            # verbose updated stats for Reef and above
            if rhbuild.split(".")[0] > "6" or (
                rhbuild.startswith("6") and dedicated_db
            ):
                pattern_list = [
                    "Settings",
                    "device size",
                    "LOG",
                    "WAL",
                    "DB",
                    "SLOW",
                    "MAXIMUMS",
                    "TOTAL",
                    "SIZE",
                ]
                (
                    pattern_list.append("LEV/DEV")
                    if rhbuild.split(".")[0] > "8"
                    else pattern_list.append("DEV/LEV")
                )
            for pattern in pattern_list:
                assert (
                    pattern in out
                ), f"{pattern} not found in bluefs stats output for build {rhbuild}"

            # restart OSD services
            osd_services = rados_obj.list_orch_services(service_type="osd")
            for osd_service in osd_services:
                cephadm.shell(args=[f"ceph orch restart {osd_service}"])
            time.sleep(30)

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        # log cluster health
        rados_obj.log_cluster_health()
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )
        # fail-safe removal of all OSDs from node13
        if "node13_osds" in locals() or "node13_osds" in globals():
            service_obj.remove_osds_from_host(host_obj=node13_obj)
            # remove lvs, vgs, and pvs
            for lvm in lvm_list:
                node13_obj.exec_command(cmd=f"lvremove -y {lvm}", sudo=True)
            vgname = lvm.split("/")[2]
            node13_obj.exec_command(cmd=f"vgremove -y {vgname}", sudo=True)
            node13_obj.exec_command(cmd=f"pvremove -y {empty_devices[0]}", sudo=True)

        # delete all rados pools
        rados_obj.rados_pool_cleanup()
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info("Completed verification of Ceph-BlueStore-Tool commands.")
    return 0
