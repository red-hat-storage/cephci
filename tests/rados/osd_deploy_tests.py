"""
Module to verify OSD deployment scenarios using spec files and raw devices.

This module tests the behavior of OSD deployment with various configurations including:
- OSD deployment using spec file with raw device
- OSD deployment using spec file with lvm device
- OSD deployment using spec file with raw data and db device
- OSD deployment using spec file with lvm data and db device
- OSD deployment using spec file with raw data,db and wal device
- OSD deployment using spec file with lvm data,db and wal device
- Verification of OSD deployment using bluestore tool (show-label)
- Verification using ceph-volume lvm list
- Validation of spec attributes in ceph orch ls output
- Validation of OSD metadata for bluefs_dedicated_db and bluefs_dedicated_wal
"""

import json
import time

import yaml

from ceph.ceph_admin import CephAdmin
from ceph.rados.bluestoretool_workflows import BluestoreToolWorkflows
from ceph.rados.cephvolume_workflows import CephVolumeWorkflows
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonConfigMethods
from tests.rados.stretch_cluster import wait_for_clean_pg_sets
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):

    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    cbt_obj = BluestoreToolWorkflows(node=cephadm, nostop=True, nostart=True)
    client_node = ceph_cluster.get_nodes(role="client")[0]
    cv_obj = CephVolumeWorkflows(node=cephadm)
    start_time = get_cluster_timestamp(rados_obj.node)
    encrypted = config.get("encrypted", False)
    compressed = config.get("compressed", False)
    log.debug(f"Test workflow started. Start time: {start_time}")

    # Get list of OSD IDs already used by previous test runs

    try:

        steps = """
        Test OSD deployment using spec file and raw/lvm device method.

        Pre-Steps:
            1. Select the osd-bak host from the cluster
            2. Create a test pool for OSD deployment verification
            3. Write data to the pool using rados bench (10K objects, 64KB)

        Test Steps:
            1. Create a YAML spec file for OSD deployment (scenario-based)
            2. Deploy the OSD using the created spec file via ceph orch apply
            3. Wait for new OSD(s) to be deployed and come up
            4. Set compression config on newly added OSDs (if compressed flag is set)

        Validation Steps:
            1. Verify OSD deployment using ceph-bluestore-tool show-label
            2. Validate OSD deployment using ceph-volume lvm list (lvm method only)
            3. Validate OSD spec in 'ceph orch ls --export' output
            4. Validate OSD metadata

        OSD functionality validation steps:
            1. Verify cluster is active+clean post OSD deployment
            2. Write IOs post OSD addition (10K objects, 64KB)
            3. Verify new OSDs are part of PGs using ceph pg dump
               - PG_SUM > 0
               - HB_PEERS > 0
               - USED > 0

        Cleanup Steps:
            1. Delete the created test pool
            2. Set all OSD services as unmanaged
            3. Drain the OSD host to remove deployed OSDs
            4. Remove drain-related labels (_no_schedule, _no_conf_keyring) from host
            5. Remove compression config from OSDs (if compressed flag is set)
        """
        log.info(steps)

        # Get the osd-bak host and its available device paths
        osd_bak_node = ceph_cluster.get_nodes(role="osd-bak")[0]
        target_hostname = osd_bak_node.hostname
        host_node = osd_bak_node

        device_paths = rados_obj.get_available_devices(
            node_name=target_hostname,
            device_type="hdd",
        )
        log.info(f"Using osd-bak host: {target_hostname}")
        log.info(f"Available device paths: {device_paths}")

        initial_osd_list = rados_obj.get_osd_list(status="up")
        log.info(f"Initial OSD list: {initial_osd_list}")

        # Pre-steps: Create pool and write data
        log.info(
            "\n ------------------------------------------- \n"
            "Pre-Step 1: Creating test pool for OSD deployment verification"
            "\n ------------------------------------------- \n"
        )
        pool_name = "osd_deploy_test_pool"
        if not rados_obj.create_pool(pool_name=pool_name):
            log.error(f"Failed to create pool: {pool_name}")
            raise Exception(f"Failed to create pool: {pool_name}")
        log.info(f"Successfully created pool: {pool_name}")

        log.info(
            "\n ------------------------------------------- \n"
            "Pre-Step 2: Writing data using rados bench (10K objects, 64KB size)"
            "\n ------------------------------------------- \n"
        )
        if not rados_obj.bench_write(
            pool_name=pool_name, byte_size="64K", max_objs=10000, verify_stats=False
        ):
            log.error(f"Failed to write data to pool: {pool_name}")
            raise Exception(f"Failed to write data to pool: {pool_name}")
        log.info(f"Successfully wrote 10K objects (64KB each) to pool: {pool_name}")

        if "scenario1" in config.get("test_scenarios", []):

            log.info(
                "\n =========================================== \n"
                "   Scenario 1 : Starting OSD deployment using spec file and raw data device"
                "\n =========================================== \n"
            )

            log.info(
                "\n ------------------------------------------- \n"
                "Step 1: Creating OSD spec file with raw device method"
                "\n ------------------------------------------- \n"
            )

            service_id = "osd_scenario_1"
            osd_spec = {
                "service_type": "osd",
                "service_id": service_id,
                "placement": {"hosts": [target_hostname]},
                "data_devices": {"paths": [device_paths[0]]},
                "method": "raw",
            }

            if encrypted:
                osd_spec["encrypted"] = "true"

            spec_file_path = f"/tmp/{service_id}.yaml"
            spec_content = yaml.dump(osd_spec, default_flow_style=False)

            log.info(f"OSD spec content:\n{spec_content}")

            # Write spec file to client node
            client_node.exec_command(
                sudo=True, cmd=f"cat > {spec_file_path} << 'EOF'\n{spec_content}EOF"
            )
            log.info(f"Created OSD spec file at: {spec_file_path}")

            # Step 2: Deploy the OSD using the created spec file
            log.info(
                "\n ------------------------------------------- \n"
                "Step 2: Deploying OSD using spec file via ceph orch apply"
                "\n ------------------------------------------- \n"
            )

            apply_cmd = f"ceph orch apply -i {spec_file_path}"
            client_node.exec_command(sudo=True, cmd=apply_cmd, verbose=True)

            # Wait for OSD deployment to complete
            new_osds = wait_for_osd_deployment(
                rados_obj=rados_obj,
                osd_list_before=initial_osd_list,
            )
            if new_osds is None:
                return 1

            # Select the first newly deployed OSD for verification
            test_osd_id = new_osds[0]
            log.info(f"Selected OSD {test_osd_id} for verification")

            # Get OSD host for verification commands
            osd_host = rados_obj.fetch_host_node(
                daemon_type="osd", daemon_id=str(test_osd_id)
            )
            log.info(f"OSD {test_osd_id} is on host: {osd_host.hostname}")

            # Step 3: Verify OSD deployment using bluestore tool show-label
            log.info(
                "\n ------------------------------------------- \n"
                "Step 3: Verifying OSD deployment using ceph-bluestore-tool show-label"
                "\n ------------------------------------------- \n"
            )
            if not validate_bluestore_label(
                cbt_obj=cbt_obj,
                osd_id=test_osd_id,
                service_id=service_id,
                data_devices=True,
                db_device=False,
                wal_device=False,
            ):
                log.error("Bluestore label validation failed")
                return 1

            # Step 4: Validate OSD spec in 'ceph orch ls' output
            log.info(
                "\n ------------------------------------------- \n"
                "Step 4: Validating OSD spec in 'ceph orch ls' output"
                "\n ------------------------------------------- \n"
            )

            if not validate_orch_ls_spec(
                rados_obj=rados_obj,
                service_id=service_id,
                method="raw",
                data_devices=True,
                db_device=False,
                wal_device=False,
            ):
                log.error("Orch ls spec validation failed")
                return 1

            # Step 5: Validate OSD metadata for bluefs_dedicated_db and bluefs_dedicated_wal
            log.info(
                "\n ------------------------------------------- \n"
                "Step 5: Validating OSD metadata for bluefs_dedicated_db and bluefs_dedicated_wal"
                "\n ------------------------------------------- \n"
            )

            if not validate_osd_metadata(
                rados_obj=rados_obj,
                osd_id=test_osd_id,
                service_id=service_id,
                db_device=False,
                wal_device=False,
            ):
                log.error("OSD metadata validation failed")
                return 1

        if "scenario2" in config.get("test_scenarios", []):

            log.info(
                "\n =========================================== \n"
                "   Scenario 2 : Starting OSD deployment using spec file and lvm data device"
                "\n =========================================== \n"
            )

            log.info(
                "\n ------------------------------------------- \n"
                "Step 1: Creating OSD spec file with lvm device method"
                "\n ------------------------------------------- \n"
            )

            # Create PV, VG and LV on the data device
            data_device_path = device_paths[0]
            vg_name = "data_vg"
            lv_name = "data_lv"

            log.info(f"Creating PV on {data_device_path}")
            host_node.exec_command(sudo=True, cmd=f"pvcreate -f {data_device_path}")

            log.info(f"Creating VG '{vg_name}' on {data_device_path}")
            host_node.exec_command(
                sudo=True, cmd=f"vgcreate {vg_name} {data_device_path}"
            )

            log.info(f"Creating LV '{lv_name}' in VG '{vg_name}'")
            host_node.exec_command(
                sudo=True, cmd=f"lvcreate -l 100%FREE -n {lv_name} {vg_name}"
            )

            lv_path = f"/dev/{vg_name}/{lv_name}"
            log.info(f"LV created at: {lv_path}")

            service_id = "osd_scenario_2"
            osd_spec = {
                "service_type": "osd",
                "service_id": service_id,
                "placement": {"hosts": [target_hostname]},
                "data_devices": {"paths": [lv_path]},
            }
            if encrypted:
                osd_spec["encrypted"] = "true"

            spec_file_path = f"/tmp/{service_id}.yaml"
            spec_content = yaml.dump(osd_spec, default_flow_style=False)

            log.info(f"OSD spec content:\n{spec_content}")

            # Write spec file to client node
            client_node.exec_command(
                sudo=True, cmd=f"cat > {spec_file_path} << 'EOF'\n{spec_content}EOF"
            )
            log.info(f"Created OSD spec file at: {spec_file_path}")

            # Step 2: Deploy the OSD using the created spec file
            log.info(
                "\n ------------------------------------------- \n"
                "Step 2: Deploying OSD using spec file via ceph orch apply"
                "\n ------------------------------------------- \n"
            )

            apply_cmd = f"ceph orch apply -i {spec_file_path}"
            client_node.exec_command(sudo=True, cmd=apply_cmd, verbose=True)

            # Wait for OSD deployment to complete
            new_osds = wait_for_osd_deployment(
                rados_obj=rados_obj,
                osd_list_before=initial_osd_list,
            )
            if new_osds is None:
                return 1

            # Select the first newly deployed OSD for verification
            test_osd_id = new_osds[0]
            log.info(f"Selected OSD {test_osd_id} for verification")

            # Get OSD host for verification commands
            osd_host = rados_obj.fetch_host_node(
                daemon_type="osd", daemon_id=str(test_osd_id)
            )
            log.info(f"OSD {test_osd_id} is on host: {osd_host.hostname}")

            # Step 3: Verify OSD deployment using bluestore tool show-label
            log.info(
                "\n ------------------------------------------- \n"
                "Step 3: Verifying OSD deployment using ceph-bluestore-tool show-label"
                "\n ------------------------------------------- \n"
            )
            if not validate_bluestore_label(
                cbt_obj=cbt_obj,
                osd_id=test_osd_id,
                service_id=service_id,
                data_devices=True,
                db_device=False,
                wal_device=False,
            ):
                log.error("Bluestore label validation failed")
                return 1

            # Step 4: Validate OSD spec in 'ceph orch ls' output
            log.info(
                "\n ------------------------------------------- \n"
                "Step 4: Validating OSD spec in 'ceph orch ls' output"
                "\n ------------------------------------------- \n"
            )

            if not validate_orch_ls_spec(
                rados_obj=rados_obj,
                service_id=service_id,
                method="lvm",
                data_devices=True,
                db_device=False,
                wal_device=False,
            ):
                log.error("Orch ls spec validation failed")
                return 1

            # Step 5: Validate OSD metadata for bluefs_dedicated_db and bluefs_dedicated_wal
            log.info(
                "\n ------------------------------------------- \n"
                "Step 5: Validating OSD metadata for bluefs_dedicated_db and bluefs_dedicated_wal"
                "\n ------------------------------------------- \n"
            )

            if not validate_osd_metadata(
                rados_obj=rados_obj,
                osd_id=test_osd_id,
                service_id=service_id,
                db_device=False,
                wal_device=False,
            ):
                log.error("OSD metadata validation failed")
                return 1

            # Step 6: Validate OSD deployment using ceph-volume lvm list
            log.info(
                "\n ------------------------------------------- \n"
                "Step 6: Validating OSD deployment using ceph-volume lvm list"
                "\n ------------------------------------------- \n"
            )

            if not validate_lvm_list(
                cv_obj=cv_obj,
                osd_host=osd_host,
                osd_id=test_osd_id,
                service_id=service_id,
                data_devices=True,
                db_device=False,
                wal_device=False,
            ):
                log.error("LVM list validation failed")
                return 1

        if "scenario3" in config.get("test_scenarios", []):

            log.info(
                "\n =========================================== \n"
                "   Scenario 3 : Starting OSD deployment using lvm data&db device - spec file"
                "\n =========================================== \n"
            )

            log.info(
                "\n ------------------------------------------- \n"
                "Step 1: Creating OSD spec file with lvm device method"
                "\n ------------------------------------------- \n"
            )

            # Create PV, VG and LV on the data device
            data_device_path = device_paths[0]
            vg_name = "data_vg"
            lv_name_1 = "data_lv"
            lv_name_2 = "db_lv"

            log.info(f"Creating PV on {data_device_path}")
            host_node.exec_command(sudo=True, cmd=f"pvcreate -f {data_device_path}")

            log.info(f"Creating VG '{vg_name}' on {data_device_path}")
            host_node.exec_command(
                sudo=True, cmd=f"vgcreate {vg_name} {data_device_path}"
            )

            log.info(f"Creating LV '{lv_name_1}' in VG '{vg_name}'")
            host_node.exec_command(
                sudo=True, cmd=f"lvcreate -l 90%FREE -n {lv_name_1} {vg_name}"
            )

            log.info(f"Creating LV '{lv_name_2}' in VG '{vg_name}'")
            host_node.exec_command(
                sudo=True, cmd=f"lvcreate -l 10%FREE -n {lv_name_2} {vg_name}"
            )

            lv_path_1 = f"/dev/{vg_name}/{lv_name_1}"
            lv_path_2 = f"/dev/{vg_name}/{lv_name_2}"
            log.info(f"LV created at: {lv_path_1} and {lv_path_2}")

            service_id = "osd_scenario_3"
            osd_spec = {
                "service_type": "osd",
                "service_id": service_id,
                "placement": {"hosts": [target_hostname]},
                "data_devices": {"paths": [lv_path_1]},
                "db_devices": {"paths": [lv_path_2]},
            }
            if encrypted:
                osd_spec["encrypted"] = "true"

            spec_file_path = f"/tmp/{service_id}.yaml"
            spec_content = yaml.dump(osd_spec, default_flow_style=False)

            log.info(f"OSD spec content:\n{spec_content}")

            # Write spec file to client node
            client_node.exec_command(
                sudo=True, cmd=f"cat > {spec_file_path} << 'EOF'\n{spec_content}EOF"
            )
            log.info(f"Created OSD spec file at: {spec_file_path}")

            # Step 2: Deploy the OSD using the created spec file
            log.info(
                "\n ------------------------------------------- \n"
                "Step 2: Deploying OSD using spec file via ceph orch apply"
                "\n ------------------------------------------- \n"
            )

            apply_cmd = f"ceph orch apply -i {spec_file_path}"
            client_node.exec_command(sudo=True, cmd=apply_cmd, verbose=True)

            # Wait for OSD deployment to complete
            new_osds = wait_for_osd_deployment(
                rados_obj=rados_obj,
                osd_list_before=initial_osd_list,
            )
            if new_osds is None:
                return 1

            # Select the first newly deployed OSD for verification
            test_osd_id = new_osds[0]
            log.info(f"Selected OSD {test_osd_id} for verification")

            # Get OSD host for verification commands
            osd_host = rados_obj.fetch_host_node(
                daemon_type="osd", daemon_id=str(test_osd_id)
            )
            log.info(f"OSD {test_osd_id} is on host: {osd_host.hostname}")

            # Step 3: Verify OSD deployment using bluestore tool show-label
            log.info(
                "\n ------------------------------------------- \n"
                "Step 3: Verifying OSD deployment using ceph-bluestore-tool show-label"
                "\n ------------------------------------------- \n"
            )
            if not validate_bluestore_label(
                cbt_obj=cbt_obj,
                osd_id=test_osd_id,
                service_id=service_id,
                data_devices=True,
                db_device=True,
                wal_device=False,
            ):
                log.error("Bluestore label validation failed")
                return 1

            # Step 4: Validate OSD spec in 'ceph orch ls' output
            log.info(
                "\n ------------------------------------------- \n"
                "Step 4: Validating OSD spec in 'ceph orch ls' output"
                "\n ------------------------------------------- \n"
            )

            if not validate_orch_ls_spec(
                rados_obj=rados_obj,
                service_id=service_id,
                method="raw",
                data_devices=True,
                db_device=True,
                wal_device=False,
            ):
                log.error("Orch ls spec validation failed")
                return 1

            # Step 5: Validate OSD metadata for bluefs_dedicated_db and bluefs_dedicated_wal
            log.info(
                "\n ------------------------------------------- \n"
                "Step 5: Validating OSD metadata for bluefs_dedicated_db and bluefs_dedicated_wal"
                "\n ------------------------------------------- \n"
            )

            if not validate_osd_metadata(
                rados_obj=rados_obj,
                osd_id=test_osd_id,
                service_id=service_id,
                db_device=True,
                wal_device=False,
            ):
                log.error("OSD metadata validation failed")
                return 1

            # Step 6: Validate OSD deployment using ceph-volume lvm list
            log.info(
                "\n ------------------------------------------- \n"
                "Step 6: Validating OSD deployment using ceph-volume lvm list"
                "\n ------------------------------------------- \n"
            )

            if not validate_lvm_list(
                cv_obj=cv_obj,
                osd_host=osd_host,
                osd_id=test_osd_id,
                service_id=service_id,
                data_devices=True,
                db_device=True,
                wal_device=False,
            ):
                log.error("LVM list validation failed")
                return 1

        if "scenario4" in config.get("test_scenarios", []):

            log.info(
                "\n =========================================== \n"
                "   Scenario 4 : Starting OSD deployment using lvm data,db and wal device - spec file"
                "\n =========================================== \n"
            )

            log.info(
                "\n ------------------------------------------- \n"
                "Step 1: Creating OSD spec file with lvm device method"
                "\n ------------------------------------------- \n"
            )

            # Create PV, VG and LV on the data device
            data_device_path = device_paths[0]
            vg_name = "data_vg"
            lv_name_1 = "data_lv"
            lv_name_2 = "db_lv"
            lv_name_3 = "wal_lv"

            log.info(f"Creating PV on {data_device_path}")
            host_node.exec_command(sudo=True, cmd=f"pvcreate -f {data_device_path}")

            log.info(f"Creating VG '{vg_name}' on {data_device_path}")
            host_node.exec_command(
                sudo=True, cmd=f"vgcreate {vg_name} {data_device_path}"
            )

            log.info(f"Creating LV '{lv_name_1}' in VG '{vg_name}'")
            host_node.exec_command(
                sudo=True, cmd=f"lvcreate -l 80%FREE -n {lv_name_1} {vg_name}"
            )

            log.info(f"Creating LV '{lv_name_2}' in VG '{vg_name}'")
            host_node.exec_command(
                sudo=True, cmd=f"lvcreate -l 10%FREE -n {lv_name_2} {vg_name}"
            )

            log.info(f"Creating LV '{lv_name_3}' in VG '{vg_name}'")
            host_node.exec_command(
                sudo=True, cmd=f"lvcreate -l 10%FREE -n {lv_name_3} {vg_name}"
            )

            lv_path_1 = f"/dev/{vg_name}/{lv_name_1}"
            lv_path_2 = f"/dev/{vg_name}/{lv_name_2}"
            lv_path_3 = f"/dev/{vg_name}/{lv_name_3}"
            log.info(f"LV created at: {lv_path_1}, {lv_path_2} and {lv_path_3}")

            service_id = "osd_scenario_4"
            osd_spec = {
                "service_type": "osd",
                "service_id": service_id,
                "placement": {"hosts": [target_hostname]},
                "data_devices": {"paths": [lv_path_1]},
                "db_devices": {"paths": [lv_path_2]},
                "wal_devices": {"paths": [lv_path_3]},
            }

            if encrypted:
                osd_spec["encrypted"] = "true"

            spec_file_path = f"/tmp/{service_id}.yaml"
            spec_content = yaml.dump(osd_spec, default_flow_style=False)

            log.info(f"OSD spec content:\n{spec_content}")

            # Write spec file to client node
            client_node.exec_command(
                sudo=True, cmd=f"cat > {spec_file_path} << 'EOF'\n{spec_content}EOF"
            )
            log.info(f"Created OSD spec file at: {spec_file_path}")

            # Step 2: Deploy the OSD using the created spec file
            log.info(
                "\n ------------------------------------------- \n"
                "Step 2: Deploying OSD using spec file via ceph orch apply"
                "\n ------------------------------------------- \n"
            )

            apply_cmd = f"ceph orch apply -i {spec_file_path}"
            client_node.exec_command(sudo=True, cmd=apply_cmd, verbose=True)

            # Wait for OSD deployment to complete
            new_osds = wait_for_osd_deployment(
                rados_obj=rados_obj,
                osd_list_before=initial_osd_list,
            )
            if new_osds is None:
                return 1

            # Select the first newly deployed OSD for verification
            test_osd_id = new_osds[0]
            log.info(f"Selected OSD {test_osd_id} for verification")

            # Get OSD host for verification commands
            osd_host = rados_obj.fetch_host_node(
                daemon_type="osd", daemon_id=str(test_osd_id)
            )
            log.info(f"OSD {test_osd_id} is on host: {osd_host.hostname}")

            # Step 3: Verify OSD deployment using bluestore tool show-label
            log.info(
                "\n ------------------------------------------- \n"
                "Step 3: Verifying OSD deployment using ceph-bluestore-tool show-label"
                "\n ------------------------------------------- \n"
            )
            if not validate_bluestore_label(
                cbt_obj=cbt_obj,
                osd_id=test_osd_id,
                service_id=service_id,
                data_devices=True,
                db_device=True,
                wal_device=True,
            ):
                log.error("Bluestore label validation failed")
                return 1

            # Step 4: Validate OSD spec in 'ceph orch ls' output
            log.info(
                "\n ------------------------------------------- \n"
                "Step 4: Validating OSD spec in 'ceph orch ls' output"
                "\n ------------------------------------------- \n"
            )

            if not validate_orch_ls_spec(
                rados_obj=rados_obj,
                service_id=service_id,
                method="lvm",
                data_devices=True,
                db_device=True,
                wal_device=True,
            ):
                log.error("Orch ls spec validation failed")
                return 1

            # Step 5: Validate OSD metadata for bluefs_dedicated_db and bluefs_dedicated_wal
            log.info(
                "\n ------------------------------------------- \n"
                "Step 5: Validating OSD metadata for bluefs_dedicated_db and bluefs_dedicated_wal"
                "\n ------------------------------------------- \n"
            )

            if not validate_osd_metadata(
                rados_obj=rados_obj,
                osd_id=test_osd_id,
                service_id=service_id,
                db_device=True,
                wal_device=True,
            ):
                log.error("OSD metadata validation failed")
                return 1

            # Step 6: Validate OSD deployment using ceph-volume lvm list
            log.info(
                "\n ------------------------------------------- \n"
                "Step 6: Validating OSD deployment using ceph-volume lvm list"
                "\n ------------------------------------------- \n"
            )

            if not validate_lvm_list(
                cv_obj=cv_obj,
                osd_host=osd_host,
                osd_id=test_osd_id,
                service_id=service_id,
                data_devices=True,
                db_device=True,
                wal_device=True,
            ):
                log.error("LVM list validation failed")
                return 1

        if "scenario5" in config.get("test_scenarios", []):

            log.info(
                "\n =========================================== \n"
                "   Scenario 5 : Starting OSD deployment using raw data and db device - spec file"
                "\n =========================================== \n"
            )

            log.info(
                "\n ------------------------------------------- \n"
                "Step 1: Creating OSD spec file with raw device method"
                "\n ------------------------------------------- \n"
            )

            service_id = "osd_scenario_5"
            osd_spec = {
                "service_type": "osd",
                "service_id": service_id,
                "placement": {"hosts": [target_hostname]},
                "data_devices": {"paths": [device_paths[0]]},
                "db_devices": {"paths": [device_paths[1]]},
            }
            if encrypted:
                osd_spec["encrypted"] = "true"

            spec_file_path = f"/tmp/{service_id}.yaml"
            spec_content = yaml.dump(osd_spec, default_flow_style=False)

            log.info(f"OSD spec content:\n{spec_content}")

            # Write spec file to client node
            client_node.exec_command(
                sudo=True, cmd=f"cat > {spec_file_path} << 'EOF'\n{spec_content}EOF"
            )
            log.info(f"Created OSD spec file at: {spec_file_path}")

            # Step 2: Deploy the OSD using the created spec file
            log.info(
                "\n ------------------------------------------- \n"
                "Step 2: Deploying OSD using spec file via ceph orch apply"
                "\n ------------------------------------------- \n"
            )

            apply_cmd = f"ceph orch apply -i {spec_file_path}"
            client_node.exec_command(sudo=True, cmd=apply_cmd, verbose=True)

            # Wait for OSD deployment to complete
            new_osds = wait_for_osd_deployment(
                rados_obj=rados_obj,
                osd_list_before=initial_osd_list,
            )
            if new_osds is None:
                return 1

            # Select the first newly deployed OSD for verification
            test_osd_id = new_osds[0]
            log.info(f"Selected OSD {test_osd_id} for verification")

            # Get OSD host for verification commands
            osd_host = rados_obj.fetch_host_node(
                daemon_type="osd", daemon_id=str(test_osd_id)
            )
            log.info(f"OSD {test_osd_id} is on host: {osd_host.hostname}")

            # Step 3: Verify OSD deployment using bluestore tool show-label
            log.info(
                "\n ------------------------------------------- \n"
                "Step 3: Verifying OSD deployment using ceph-bluestore-tool show-label"
                "\n ------------------------------------------- \n"
            )
            if not validate_bluestore_label(
                cbt_obj=cbt_obj,
                osd_id=test_osd_id,
                service_id=service_id,
                data_devices=True,
                db_device=True,
                wal_device=False,
            ):
                log.error("Bluestore label validation failed")
                return 1

            # Step 4: Validate OSD spec in 'ceph orch ls' output
            log.info(
                "\n ------------------------------------------- \n"
                "Step 4: Validating OSD spec in 'ceph orch ls' output"
                "\n ------------------------------------------- \n"
            )

            if not validate_orch_ls_spec(
                rados_obj=rados_obj,
                service_id=service_id,
                method="lvm",
                data_devices=True,
                db_device=True,
                wal_device=False,
            ):
                log.error("Orch ls spec validation failed")
                return 1

            # Step 5: Validate OSD metadata for bluefs_dedicated_db and bluefs_dedicated_wal
            log.info(
                "\n ------------------------------------------- \n"
                "Step 5: Validating OSD metadata for bluefs_dedicated_db and bluefs_dedicated_wal"
                "\n ------------------------------------------- \n"
            )

            if not validate_osd_metadata(
                rados_obj=rados_obj,
                osd_id=test_osd_id,
                service_id=service_id,
                db_device=True,
                wal_device=False,
            ):
                log.error("OSD metadata validation failed")
                return 1

            # Step 6: Validate OSD deployment using ceph-volume lvm list
            log.info(
                "\n ------------------------------------------- \n"
                "Step 6: Validating OSD deployment using ceph-volume lvm list"
                "\n ------------------------------------------- \n"
            )

            if not validate_lvm_list(
                cv_obj=cv_obj,
                osd_host=osd_host,
                osd_id=test_osd_id,
                service_id=service_id,
                data_devices=True,
                db_device=True,
                wal_device=False,
            ):
                log.error("LVM list validation failed")
                return 1

        if "scenario6" in config.get("test_scenarios", []):

            log.info(
                "\n =========================================== \n"
                "   Scenario 6 : Starting OSD deployment using raw data,db and wal device - spec file"
                "\n =========================================== \n"
            )

            log.info(
                "\n ------------------------------------------- \n"
                "Step 1: Creating OSD spec file with lvm device method"
                "\n ------------------------------------------- \n"
            )

            service_id = "osd_scenario_6"
            osd_spec = {
                "service_type": "osd",
                "service_id": service_id,
                "placement": {"hosts": [target_hostname]},
                "data_devices": {"paths": [device_paths[0]]},
                "db_devices": {"paths": [device_paths[1]]},
                "wal_devices": {"paths": [device_paths[2]]},
            }
            if encrypted:
                osd_spec["encrypted"] = "true"

            spec_file_path = f"/tmp/{service_id}.yaml"
            spec_content = yaml.dump(osd_spec, default_flow_style=False)

            log.info(f"OSD spec content:\n{spec_content}")

            # Write spec file to client node
            client_node.exec_command(
                sudo=True, cmd=f"cat > {spec_file_path} << 'EOF'\n{spec_content}EOF"
            )
            log.info(f"Created OSD spec file at: {spec_file_path}")

            # Step 2: Deploy the OSD using the created spec file
            log.info(
                "\n ------------------------------------------- \n"
                "Step 2: Deploying OSD using spec file via ceph orch apply"
                "\n ------------------------------------------- \n"
            )

            apply_cmd = f"ceph orch apply -i {spec_file_path}"
            client_node.exec_command(sudo=True, cmd=apply_cmd, verbose=True)

            # Wait for OSD deployment to complete
            new_osds = wait_for_osd_deployment(
                rados_obj=rados_obj,
                osd_list_before=initial_osd_list,
            )
            if new_osds is None:
                return 1

            # Select the first newly deployed OSD for verification
            test_osd_id = new_osds[0]
            log.info(f"Selected OSD {test_osd_id} for verification")

            # Get OSD host for verification commands
            osd_host = rados_obj.fetch_host_node(
                daemon_type="osd", daemon_id=str(test_osd_id)
            )
            log.info(f"OSD {test_osd_id} is on host: {osd_host.hostname}")

            # Step 3: Verify OSD deployment using bluestore tool show-label
            log.info(
                "\n ------------------------------------------- \n"
                "Step 3: Verifying OSD deployment using ceph-bluestore-tool show-label"
                "\n ------------------------------------------- \n"
            )
            if not validate_bluestore_label(
                cbt_obj=cbt_obj,
                osd_id=test_osd_id,
                service_id=service_id,
                data_devices=True,
                db_device=True,
                wal_device=True,
            ):
                log.error("Bluestore label validation failed")
                return 1

            # Step 4: Validate OSD spec in 'ceph orch ls' output
            log.info(
                "\n ------------------------------------------- \n"
                "Step 4: Validating OSD spec in 'ceph orch ls' output"
                "\n ------------------------------------------- \n"
            )

            if not validate_orch_ls_spec(
                rados_obj=rados_obj,
                service_id=service_id,
                method="raw",
                data_devices=True,
                db_device=True,
                wal_device=True,
            ):
                log.error("Orch ls spec validation failed")
                return 1

            # Step 5: Validate OSD metadata for bluefs_dedicated_db and bluefs_dedicated_wal
            log.info(
                "\n ------------------------------------------- \n"
                "Step 5: Validating OSD metadata for bluefs_dedicated_db and bluefs_dedicated_wal"
                "\n ------------------------------------------- \n"
            )

            if not validate_osd_metadata(
                rados_obj=rados_obj,
                osd_id=test_osd_id,
                service_id=service_id,
                db_device=True,
                wal_device=True,
            ):
                log.error("OSD metadata validation failed")
                return 1

            # Step 6: Validate OSD deployment using ceph-volume lvm list
            log.info(
                "\n ------------------------------------------- \n"
                "Step 6: Validating OSD deployment using ceph-volume lvm list"
                "\n ------------------------------------------- \n"
            )

            if not validate_lvm_list(
                cv_obj=cv_obj,
                osd_host=osd_host,
                osd_id=test_osd_id,
                service_id=service_id,
                data_devices=True,
                db_device=True,
                wal_device=True,
            ):
                log.error("LVM list validation failed")
                return 1

        if compressed:
            log.info(
                "\n =========================================== \n"
                "   Setting compression config on newly added OSDs"
                "\n =========================================== \n"
            )
            # Get current OSD list to identify newly deployed OSDs
            current_osd_list = rados_obj.get_osd_list(status="up")
            new_osd_ids = [
                osd for osd in current_osd_list if osd not in initial_osd_list
            ]
            log.info(f"Newly deployed OSDs for compression config: {new_osd_ids}")

            mon_config_obj = MonConfigMethods(rados_obj=rados_obj)
            for osd_id in new_osd_ids:
                # Set bluestore_compression_algorithm to snappy
                log.info(
                    f"Setting bluestore_compression_algorithm to snappy on OSD {osd_id}"
                )
                if not mon_config_obj.set_config(
                    section=f"osd.{osd_id}",
                    name="bluestore_compression_algorithm",
                    value="snappy",
                ):
                    log.error(
                        f"Failed to set bluestore_compression_algorithm on OSD {osd_id}"
                    )
                    return 1

                # Set bluestore_compression_mode to force
                log.info(f"Setting bluestore_compression_mode to force on OSD {osd_id}")
                if not mon_config_obj.set_config(
                    section=f"osd.{osd_id}",
                    name="bluestore_compression_mode",
                    value="force",
                ):
                    log.error(
                        f"Failed to set bluestore_compression_mode on OSD {osd_id}"
                    )
                    return 1

            log.info("Compression config set successfully on all newly added OSDs")

        # Validations for successful OSD deployment
        log.info(
            "\n =========================================== \n"
            "   Post-Deployment OSD functionality Validations"
            "\n =========================================== \n"
        )

        # Validation 1: Verify cluster is active+clean post addition of new OSDs
        log.info(
            "\n ------------------------------------------- \n"
            "Validation 1: Verifying cluster is active+clean post OSD addition"
            "\n ------------------------------------------- \n"
        )
        if not wait_for_clean_pg_sets(rados_obj, timeout=1800):
            log.error("Cluster did not reach active+clean state after OSD deployment")
            return 1
        log.info("Verified: Cluster is active+clean post OSD addition")

        # Validation 2: Write IOs post addition
        log.info(
            "\n ------------------------------------------- \n"
            "Validation 2: Writing data to pool post OSD addition (10K objects, 64KB size)"
            "\n ------------------------------------------- \n"
        )
        if not rados_obj.bench_write(
            pool_name=pool_name, byte_size="64K", max_objs=10000, verify_stats=False
        ):
            log.error(f"Failed to write data to pool: {pool_name} post OSD addition")
            return 1
        log.info(f"Successfully wrote 10K objects (64KB each) to pool: {pool_name}")

        # Validation 3: Verify new OSDs are part of PGs as both primaries and secondaries
        log.info(
            "\n ------------------------------------------- \n"
            "Validation 3: Verifying new OSDs are part of PGs (primary and secondary)"
            "\n ------------------------------------------- \n"
        )

        # Get the current OSD list to identify newly deployed OSDs
        current_osd_list = rados_obj.get_osd_list(status="up")
        new_osd_ids = [osd for osd in current_osd_list if osd not in initial_osd_list]
        log.info(f"Newly deployed OSDs: {new_osd_ids}")

        if new_osd_ids:
            # Get OSD stats from ceph pg dump
            cmd = "ceph pg dump -f json"
            out, _ = rados_obj.client.exec_command(sudo=True, cmd=cmd)
            pg_dump = json.loads(out)
            pg_map = pg_dump.get("pg_map", [])

            # Build a dictionary of OSD stats keyed by OSD ID
            osd_stats = {stat["osd"]: stat for stat in pg_map.get("osd_stats", [])}

            for new_osd_id in new_osd_ids:
                if new_osd_id not in osd_stats:
                    log.error(f"OSD {new_osd_id} not found in pg dump osd_stats")
                    raise Exception(
                        f"OSD {new_osd_id} not found in ceph pg dump osd_stats"
                    )

                osd_stat = osd_stats[new_osd_id]
                log.info(json.dumps(osd_stat, indent=4))
                pg_sum = osd_stat.get("num_pgs", 0)
                # primary PG sum is not reported in ceph pg dump -fjson
                hb_peers = osd_stat.get("hb_peers", [])
                kb_used = osd_stat.get("kb_used", 0)

                log.info(
                    f"OSD {new_osd_id} stats from pg dump: "
                    f"PG_SUM={pg_sum} "
                    f"HB_PEERS={len(hb_peers)}, USED={kb_used} KB"
                )

                # Verify PG_SUM > 0
                if pg_sum == 0:
                    log.error(f"OSD {new_osd_id} has PG_SUM={pg_sum}, expected > 0")
                    raise Exception(
                        f"OSD {new_osd_id} PG_SUM validation failed: {pg_sum} == 0"
                    )
                log.info(f"OSD {new_osd_id}: PG_SUM={pg_sum} > 0 - PASSED")

                # Verify HB_PEERS count > 0
                if len(hb_peers) == 0:
                    log.error(
                        f"OSD {new_osd_id} has HB_PEERS count={len(hb_peers)}, expected > 0"
                    )
                    raise Exception(
                        f"OSD {new_osd_id} HB_PEERS validation failed: "
                        f"{len(hb_peers)} == 0"
                    )
                log.info(
                    f"OSD {new_osd_id}: HB_PEERS count={len(hb_peers)} > 0 - PASSED"
                )

                # Verify USED > 0
                if kb_used == 0:
                    log.error(f"OSD {new_osd_id} has USED={kb_used} KB, expected > 0")
                    raise Exception(
                        f"OSD {new_osd_id} USED validation failed: {kb_used} == 0"
                    )
                log.info(f"OSD {new_osd_id}: USED={kb_used} KB > 0 - PASSED")

            log.info("Verified: All new OSDs have PG_SUM, HB_PEERS and USED > 0")
        else:
            log.error("No new OSDs were deployed to verify PG participation")
            return 1

        log.info(
            "\n =========================================== \n"
            "   All Post-Deployment Validations Passed Successfully!"
            "\n =========================================== \n"
        )

    except Exception as e:
        log.error(f"Failed with exception: {e.__doc__}")
        log.exception(e)
        rados_obj.log_cluster_health()
        return 1
    finally:
        log.info(
            "\n \n ************** Execution of finally block begins here *************** \n \n"
        )

        # Delete the created pool
        log.info(
            "\n ------------------------------------------- \n"
            "Cleanup Step 1: Deleting test pool"
            "\n ------------------------------------------- \n"
        )
        if rados_obj.delete_pool(pool=pool_name):
            log.info(f"Successfully deleted pool: {pool_name}")
        else:
            log.warning(f"Failed to delete pool: {pool_name}")

        # Set all OSD services as unmanaged
        log.info(
            "\n ------------------------------------------- \n"
            "Cleanup Step 2: Setting all OSD services as unmanaged"
            "\n ------------------------------------------- \n"
        )
        osd_services = rados_obj.list_orch_services(service_type="osd")
        for service in osd_services:
            rados_obj.set_unmanaged_flag(service_type="osd", service_name=service)
            log.info(f"Set OSD service '{service}' as unmanaged")

        # Drain the host to remove OSDs
        log.info(
            "\n ------------------------------------------- \n"
            "Cleanup Step 3: Draining OSD host to remove deployed OSDs"
            "\n ------------------------------------------- \n"
        )
        drain_cmd = f"ceph orch host drain {target_hostname} --force --zap-osd-devices"
        rados_obj.client.exec_command(sudo=True, cmd=drain_cmd)
        log.info(f"Initiated drain on host: {target_hostname}")

        # Wait for drain to complete by checking osd rm status
        import datetime

        end_time = datetime.datetime.now() + datetime.timedelta(seconds=600)
        log.info(f"Waiting up to 600s for drain to complete on {target_hostname}")
        while datetime.datetime.now() < end_time:
            status_cmd = "ceph orch osd rm status -f json"
            out, _ = rados_obj.client.exec_command(sudo=True, cmd=status_cmd)
            try:
                status = json.loads(out)
                log.debug(f"OSD removal still in progress: {status}")
            except json.JSONDecodeError:
                log.debug("No OSDs in removal queue, drain may be complete")
                break
            time.sleep(10)
        else:
            log.warning(f"Drain operation timed out for host: {target_hostname}")

        out = rados_obj.run_ceph_command(cmd="ceph osd tree", print_output=True)
        log.info(out)

        out = rados_obj.run_ceph_command(cmd="ceph orch ps", print_output=True)
        log.info(out)

        # Remove the drain related labels which are added to the host during drain
        log.info(
            "\n ------------------------------------------- \n"
            "Cleanup Step 4: Removing drain-related labels from host"
            "\n ------------------------------------------- \n"
        )
        host_labels = rados_obj.get_host_label(host_name=target_hostname)
        log.info(f"Current labels on {target_hostname}: {host_labels}")

        drain_labels = ["_no_schedule", "_no_conf_keyring"]
        for label in drain_labels:
            if label in host_labels:
                rados_obj.remove_host_label(host_name=target_hostname, label=label)
                log.info(f"Removed label '{label}' from host: {target_hostname}")
            else:
                log.debug(f"Label '{label}' not present on host: {target_hostname}")

        # Remove compression config if it was enabled
        if compressed:
            log.info(
                "\n ------------------------------------------- \n"
                "Cleanup Step 5: Removing compression config from OSDs"
                "\n ------------------------------------------- \n"
            )
            # Get the list of OSDs that were deployed during the test
            current_osd_list = rados_obj.get_osd_list(status="up")
            new_osd_ids = [
                osd for osd in current_osd_list if osd not in initial_osd_list
            ]
            log.info(f"Removing compression config from OSDs: {new_osd_ids}")

            mon_config_obj = MonConfigMethods(rados_obj=rados_obj)
            for osd_id in new_osd_ids:
                # Remove bluestore_compression_algorithm config
                log.info(f"Removing bluestore_compression_algorithm from OSD {osd_id}")
                mon_config_obj.remove_config(
                    section=f"osd.{osd_id}",
                    name="bluestore_compression_algorithm",
                    verify_rm=False,
                )

                # Remove bluestore_compression_mode config
                log.info(f"Removing bluestore_compression_mode from OSD {osd_id}")
                mon_config_obj.remove_config(
                    section=f"osd.{osd_id}",
                    name="bluestore_compression_mode",
                    verify_rm=False,
                )

            log.info("Compression config removed successfully from all OSDs")

        rados_obj.remove_empty_service_spec(service_type="osd")

        # Log cluster health
        rados_obj.log_cluster_health()

        # Check for crashes after test execution
        test_end_time = get_cluster_timestamp(rados_obj.node)
        log.debug(
            f"Test workflow completed. Start time: {start_time}, End time: {test_end_time}"
        )
        if rados_obj.check_crash_status(start_time=start_time, end_time=test_end_time):
            log.error("Test failed due to crash at the end of test")
            return 1

    log.info(
        "Verification of OSD deployment using spec file and raw device completed successfully"
    )
    return 0


def get_device_path_from_metadata(rados_obj, osd_id):
    """
    Fetch the device path of an OSD using ceph osd metadata command.

    This method executes 'ceph osd metadata <osd_id>' command, parses the JSON
    output and returns the bluestore_bdev_dev_node value which represents
    the device path where the OSD is deployed.

    Args:
        rados_obj: RadosOrchestrator object for executing ceph commands
        osd_id: The OSD ID to fetch the device path for

    Returns:
        str: The device path (bluestore_bdev_dev_node) of the OSD
        None: If the command fails or the key is not found
    """
    cmd = f"ceph osd metadata {osd_id} -f json"
    out, _ = rados_obj.client.exec_command(sudo=True, cmd=cmd)
    metadata = json.loads(out)
    device_path = metadata.get("bluestore_bdev_devices")
    if device_path:
        log.info(f"OSD {osd_id} device path from metadata: /dev/{device_path}")
        return f"/dev/{device_path}"
    else:
        log.error(f"bluestore_bdev_dev_node not found in OSD {osd_id} metadata")
        raise Exception(f"bluestore_bdev_dev_node not found in OSD {osd_id} metadata")


def validate_bluestore_label(
    cbt_obj, osd_id, service_id, data_devices=True, db_device=False, wal_device=False
):
    """
    Validate OSD deployment using ceph-bluestore-tool show-label output.

    This method runs the show-label command and validates the presence of
    block, block.db, and block.wal entries based on the provided flags.

    Args:
        cbt_obj: BluestoreToolWorkflows object for running bluestore commands
        osd_id: The OSD ID to validate
        service_id: The expected osdspec_affinity service ID
        data_devices: If True, validate block entry exists (default: True)
        db_device: If True, validate block.db entry exists (default: False)
        wal_device: If True, validate block.wal entry exists (default: False)

    Returns:
        bool: True if all validations pass, False otherwise
    """
    show_label_output = cbt_obj.show_label(osd_id=osd_id)
    log.info(f"ceph-bluestore-tool show-label output:\n{show_label_output}")

    # Validate block entry (data_devices)
    if data_devices:
        expected_block_path = f"/var/lib/ceph/osd/ceph-{osd_id}/block"
        if expected_block_path not in show_label_output:
            log.error(
                f"Expected block path '{expected_block_path}' not found in show-label output"
            )
            return False
        log.info(
            f"Verified: Block entry '{expected_block_path}' exists in show-label output"
        )

    # Validate block.db entry (db_device)
    if db_device:
        expected_db_path = f"/var/lib/ceph/osd/ceph-{osd_id}/block.db"
        if expected_db_path not in show_label_output:
            log.error(
                f"Expected block.db path '{expected_db_path}' not found in show-label output"
            )
            return False
        log.info(
            f"Verified: Block.db entry '{expected_db_path}' exists in show-label output"
        )

    # Validate block.wal entry (wal_device)
    if wal_device:
        expected_wal_path = f"/var/lib/ceph/osd/ceph-{osd_id}/block.wal"
        if expected_wal_path not in show_label_output:
            log.error(
                f"Expected block.wal path '{expected_wal_path}' not found in show-label output"
            )
            return False
        log.info(
            f"Verified: Block.wal entry '{expected_wal_path}' exists in show-label output"
        )

    # Validate osdspec_affinity matches service_id
    if f'"osdspec_affinity": "{service_id}"' in show_label_output:
        log.info(f"Verified: osdspec_affinity matches service_id '{service_id}'")
    else:
        log.warning(
            f"osdspec_affinity may not match service_id '{service_id}' in show-label output"
        )

    return True


def validate_orch_ls_spec(
    rados_obj,
    service_id,
    method,
    data_devices=True,
    db_device=False,
    wal_device=False,
):
    """
    Validate OSD spec in 'ceph orch ls --export' output.

    This method fetches the orch ls export and validates the presence of
    data_devices, db_devices, wal_devices entries and method based on the provided flags.

    Args:
        rados_obj: RadosOrchestrator object for running ceph commands
        service_id: The expected service_id to find in the spec
        method: The expected method (raw/lvm) in the spec
        data_devices: If True, validate data_devices exists (default: True)
        db_device: If True, validate db_devices exists; if False, validate it does NOT exist
        wal_device: If True, validate wal_devices exists; if False, validate it does NOT exist

    Returns:
        bool: True if all validations pass, False otherwise
    """
    orch_ls_export = rados_obj.list_orch_services(service_type="osd", export=True)
    log.info(f"ceph orch ls --export output:\n{json.dumps(orch_ls_export, indent=2)}")

    spec_found = False
    for svc_spec in orch_ls_export:
        if svc_spec.get("service_id") == service_id:
            spec_found = True
            log.info(f"Found OSD spec with service_id '{service_id}'")

            # Validate data_devices in spec
            if data_devices:
                if "data_devices" in svc_spec.get("spec", {}):
                    log.info("Verified: 'data_devices' exists in the spec")
                else:
                    log.error("'data_devices' not found in the spec")
                    return False

            # Validate db_devices based on db_device flag
            if db_device:
                if "db_devices" in svc_spec.get("spec", {}):
                    log.info("Verified: 'db_devices' exists in the spec")
                else:
                    log.error("'db_devices' not found in the spec but was expected")
                    return False
            else:
                if "db_devices" not in svc_spec.get("spec", {}):
                    log.info(
                        "Verified: 'db_devices' does not exist in the spec (as expected)"
                    )
                else:
                    log.error(
                        "'db_devices' should not exist in the spec when db devices are not specified"
                    )
                    return False

            # Validate wal_devices based on wal_device flag
            if wal_device:
                if "wal_devices" in svc_spec.get("spec", {}):
                    log.info("Verified: 'wal_devices' exists in the spec")
                else:
                    log.error("'wal_devices' not found in the spec but was expected")
                    return False
            else:
                if "wal_devices" not in svc_spec.get("spec", {}):
                    log.info(
                        "Verified: 'wal_devices' does not exist in the spec (as expected)"
                    )
                else:
                    log.error(
                        "'wal_devices' should not exist in the spec when wal devices are not specified"
                    )
                    return False

            # Validate method is raw/lvm
            spec_method = svc_spec.get("spec", {}).get("method")
            if spec_method == method:
                log.info(f"Verified: 'method' is '{method}' in the spec")
            else:
                log.warning(
                    f"Method in spec is '{spec_method}', expected '{method}'. "
                    "This may be expected if method is at root level."
                )

            break

    if not spec_found:
        log.error(
            f"OSD spec with service_id '{service_id}' not found in orch ls export. "
        )
        return False

    return True


def validate_osd_metadata(
    rados_obj,
    osd_id,
    service_id,
    db_device=False,
    wal_device=False,
):
    """
    Validate OSD metadata for bluefs_dedicated_db, bluefs_dedicated_wal, and osdspec_affinity.

    This method fetches the OSD metadata and validates the bluefs configuration
    based on whether dedicated DB and WAL devices are expected.

    Args:
        rados_obj: RadosOrchestrator object for running ceph commands
        osd_id: The OSD ID to validate
        service_id: The expected osdspec_affinity service ID
        db_device: If True, expect bluefs_dedicated_db=1; if False, expect bluefs_dedicated_db=0
        wal_device: If True, expect bluefs_dedicated_wal=1; if False, expect bluefs_dedicated_wal=0

    Returns:
        bool: True if all validations pass, False otherwise
    """
    osd_metadata = rados_obj.get_daemon_metadata(
        daemon_type="osd", daemon_id=str(osd_id)
    )
    log.info(f"OSD {osd_id} metadata: {json.dumps(osd_metadata, indent=2)}")

    if osd_metadata is None:
        log.error(f"Failed to retrieve metadata for OSD {osd_id}")
        return False

    # Validate bluefs_dedicated_db based on db_device flag
    bluefs_dedicated_db = osd_metadata.get("bluefs_dedicated_db")
    expected_db_value = "1" if db_device else "0"
    if bluefs_dedicated_db == expected_db_value:
        if db_device:
            log.info("Verified: bluefs_dedicated_db is 1 (dedicated DB device)")
        else:
            log.info("Verified: bluefs_dedicated_db is 0 (no dedicated DB device)")
    else:
        log.error(
            f"Expected bluefs_dedicated_db to be '{expected_db_value}', "
            f"but got '{bluefs_dedicated_db}'"
        )
        return False

    # Validate bluefs_dedicated_wal based on wal_device flag
    bluefs_dedicated_wal = osd_metadata.get("bluefs_dedicated_wal")
    expected_wal_value = "1" if wal_device else "0"
    if bluefs_dedicated_wal == expected_wal_value:
        if wal_device:
            log.info("Verified: bluefs_dedicated_wal is 1 (dedicated WAL device)")
        else:
            log.info("Verified: bluefs_dedicated_wal is 0 (no dedicated WAL device)")
    else:
        log.error(
            f"Expected bluefs_dedicated_wal to be '{expected_wal_value}', "
            f"but got '{bluefs_dedicated_wal}'"
        )
        return False

    # Additional validation: bluefs_single_shared_device
    # Should be 1 if no dedicated DB or WAL device, 0 otherwise
    bluefs_single_shared_device = osd_metadata.get("bluefs_single_shared_device")
    expected_shared_value = "0" if (db_device or wal_device) else "1"
    if bluefs_single_shared_device == expected_shared_value:
        if expected_shared_value == "1":
            log.info(
                "Verified: bluefs_single_shared_device is 1 (data, DB, WAL on same device)"
            )
        else:
            log.info(
                "Verified: bluefs_single_shared_device is 0 (separate DB/WAL devices)"
            )
    else:
        log.warning(
            f"bluefs_single_shared_device is '{bluefs_single_shared_device}', "
            f"expected '{expected_shared_value}'"
        )

    # Validate osdspec_affinity in metadata
    metadata_osdspec_affinity = osd_metadata.get("osdspec_affinity")
    if metadata_osdspec_affinity == service_id:
        log.info(
            f"Verified: osdspec_affinity in metadata is '{metadata_osdspec_affinity}'"
        )
    else:
        log.warning(
            f"osdspec_affinity in metadata is '{metadata_osdspec_affinity}', "
            f"expected '{service_id}'"
        )

    return True


def wait_for_osd_deployment(
    rados_obj,
    osd_list_before,
    max_retries=12,
    retry_interval=30,
):
    """
    Wait for new OSD(s) to be deployed and come up.

    This method polls the cluster for new OSDs that were not present in the
    osd_list_before and returns the list of newly deployed OSDs.

    Args:
        rados_obj: RadosOrchestrator object for running ceph commands
        osd_list_before: List of OSD IDs that existed before deployment
        max_retries: Maximum number of retry attempts (default: 12)
        retry_interval: Seconds to wait between retries (default: 30)

    Returns:
        list: List of newly deployed OSD IDs if successful
        None: If no new OSDs were deployed within the timeout
    """
    log.info("Waiting for OSD deployment to complete...")

    for attempt in range(max_retries):
        current_osd_list = rados_obj.get_osd_list(status="up")
        new_osds = [osd for osd in current_osd_list if osd not in osd_list_before]

        if new_osds:
            log.info(f"New OSDs deployed: {new_osds}")
            return new_osds

        log.info(
            f"Attempt {attempt + 1}/{max_retries}: Waiting for new OSDs to come up..."
        )
        time.sleep(retry_interval)

    log.error("No new OSDs were deployed after applying the spec file")
    return None


def validate_lvm_list(
    cv_obj,
    osd_host,
    osd_id,
    service_id,
    data_devices=True,
    db_device=False,
    wal_device=False,
):
    """
    Validate OSD deployment using ceph-volume lvm list output.

    This method runs the lvm list command and validates the presence of
    block, block.db, and block.wal entries based on the provided flags.

    Args:
        cv_obj: CephVolumeWorkflows object for running ceph-volume commands
        osd_host: The host node where the OSD is deployed
        osd_id: The OSD ID to validate
        service_id: The expected osdspec_affinity service ID
        data_devices: If True, validate block entry exists (default: True)
        db_device: If True, validate block.db entry exists (default: False)
        wal_device: If True, validate block.wal entry exists (default: False)

    Returns:
        bool: True if all validations pass, False otherwise
    """
    lvm_list_output = cv_obj.lvm_list(host=osd_host, osd_id=str(osd_id))
    log.info(f"ceph-volume lvm list output:\n{lvm_list_output}")

    try:
        lvm_list_json = json.loads(lvm_list_output)
    except json.JSONDecodeError as e:
        log.error(f"Error parsing ceph-volume lvm list output: {e}")
        return False

    # Validate OSD exists in lvm list
    if str(osd_id) not in lvm_list_json:
        log.error(f"OSD {osd_id} not found in ceph-volume lvm list output")
        return False

    osd_lvm_info = lvm_list_json[str(osd_id)]

    # Validate block entry (data_devices)
    if data_devices:
        block_entry_found = False
        for device_info in osd_lvm_info:
            if device_info.get("type") == "block":
                block_entry_found = True
                block_device = device_info.get("lv_path")
                log.info(f"Verified: Block device entry found: {block_device}")

                # Validate osdspec affinity
                osdspec_affinity = device_info.get("tags", {}).get(
                    "ceph.osdspec_affinity"
                )
                if osdspec_affinity == service_id:
                    log.info(
                        f"Verified: osdspec_affinity '{osdspec_affinity}' matches service_id"
                    )
                break

        if not block_entry_found:
            log.error("Block device entry not found in ceph-volume lvm list")
            return False

    # Validate block.db entry (db_device)
    if db_device:
        db_entry_found = False
        for device_info in osd_lvm_info:
            if device_info.get("type") == "db":
                db_entry_found = True
                db_device_path = device_info.get("lv_path")
                log.info(f"Verified: Block.db device entry found: {db_device_path}")
                break

        if not db_entry_found:
            log.error("Block.db device entry not found in ceph-volume lvm list")
            return False

    # Validate block.wal entry (wal_device)
    if wal_device:
        wal_entry_found = False
        for device_info in osd_lvm_info:
            if device_info.get("type") == "wal":
                wal_entry_found = True
                wal_device_path = device_info.get("lv_path")
                log.info(f"Verified: Block.wal device entry found: {wal_device_path}")
                break

        if not wal_entry_found:
            log.error("Block.wal device entry not found in ceph-volume lvm list")
            return False

    return True
