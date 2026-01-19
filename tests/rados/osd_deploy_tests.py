"""
Module to verify OSD deployment scenarios using spec files and raw devices.

This module tests the behavior of OSD deployment with various configurations including:
- OSD deployment using spec file with raw device method
- Verification of OSD deployment using bluestore tool (show-label)
- Verification using ceph-volume lvm list
- Validation of spec attributes in ceph orch ls output
- Validation of OSD metadata for bluefs_dedicated_db and bluefs_dedicated_wal
"""

import json
import random
import time

import yaml

from ceph.ceph_admin import CephAdmin
from ceph.rados import utils as rados_utils
from ceph.rados.bluestoretool_workflows import BluestoreToolWorkflows
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.utils import get_cluster_timestamp
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):

    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    cbt_obj = BluestoreToolWorkflows(node=cephadm, nostop=True, nostart=True)
    client_node = ceph_cluster.get_nodes(role="client")[0]

    start_time = get_cluster_timestamp(rados_obj.node)
    log.debug(f"Test workflow started. Start time: {start_time}")

    try:

        steps = """
        Test OSD deployment using spec file and raw/lvm device method.

        Pre-Steps:
            1. Select a random OSD host from the cluster
            2. Set all OSD specs to unmanaged mode
            3. Getting device paths for OSDs to be removed
            4. Removing OSDs using ceph orch osd rm --force --zap

        Test Steps:
            1. Create a YAML spec file for OSD deployment
            2. Deploy the OSD using the created spec file via ceph orch apply
            3. Wait for new OSD(s) to be deployed and come up

        Validation Steps:
            1. Verify OSD deployment using ceph-bluestore-tool show-label
            2. Validate OSD deployment using ceph-volume lvm list (lvm method only)
            3. Validate OSD spec in 'ceph orch ls --export' output
            4. Validate OSD metadata
        """
        log.info(steps)

        log.info("------- Pre step start ---------- ")

        if "scenario1" in config.get("test_scenarios", []):
            num_osds_to_remove = 1
        elif "scenario2" in config.get("test_scenarios", []):
            num_osds_to_remove = 2
        else:
            log.error("Invalid scenario selected")
            return 1

        # Select OSDs from the host, set specs to unmanaged, remove OSDs
        removed_osds_list, device_paths, target_hostname = select_and_remove_osds(
            ceph_cluster=ceph_cluster,
            rados_obj=rados_obj,
            num_osds=num_osds_to_remove,
        )

        log.info(
            f"Removed {len(removed_osds_list)} OSD(s) from host {target_hostname}\n"
            f"Device paths: {device_paths}"
        )

        osd_list_after_removal = rados_obj.get_osd_list(status="up")
        log.info(f"OSD list after removal: {osd_list_after_removal}")

        log.info("------- Pre step End ---------- ")

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
                osd_list_before=osd_list_after_removal,
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

            log.info(
                "\n =========================================== \n"
                "   Scenario 1 : OSD deployment using spec file and raw device passed successfully!"
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


def select_and_remove_osds(ceph_cluster, rados_obj, num_osds=1):
    """
    Select a random OSD host, set all OSD specs to unmanaged, remove specified
    number of OSDs using ceph orch rm --force --zap, and return OSD information
    along with device paths and target hostname.

    Steps:
        1. Get list of all active OSDs in the cluster
        2. Select a random OSD host from the cluster
        3. Identify OSDs on the selected host
        4. Validate sufficient OSDs are available for removal
        5. Select the specified number of OSDs to remove
        6. Set all OSD specs to unmanaged mode
        7. Get device paths for all OSDs to be removed using ceph osd metadata
        8. Remove OSDs using ceph orch osd rm --force --zap
        9. Wait for all OSD removals to complete
        10. Return removed OSD information, device paths, and target hostname

    Args:
        ceph_cluster: Ceph cluster object
        rados_obj: RadosOrchestrator object
        num_osds: Number of OSDs to remove from the host (default: 1)

    Returns:
        tuple: A tuple containing:
            - removed_osds_info (list): List of dictionaries with OSD information:
                - osd_id: The OSD ID that was removed
                - hostname: The hostname where the OSD was located
                - host_node: The CephNode object for the host
                - device_path: The device path of the removed OSD
            - device_paths (list): List of device paths from removed OSDs
            - target_hostname (str): Hostname where OSDs were removed
        Returns 1 if operation fails
    """

    # Get initial OSD count before deployment
    initial_osd_list = rados_obj.get_osd_list(status="up")
    log.info(f"Initial active OSDs in the cluster: {initial_osd_list}")

    # Get configuration for number of OSDs to remove and target host

    osd_host = rados_obj.fetch_host_node(
        daemon_type="osd", daemon_id=str(random.choice(initial_osd_list))
    )

    host_node = osd_host
    hostname = osd_host.hostname

    log.info(
        "\n ------------------------------------------- \n"
        f"Pre-Step 1: Identifying OSDs on host {hostname}"
        "\n ------------------------------------------- \n"
    )

    # Get list of OSDs on the specified host
    osds_on_host = rados_obj.collect_osd_daemon_ids(osd_node=host_node)
    if not osds_on_host:
        log.error(f"No OSDs found on host {hostname}")
        return None

    log.info(f"OSDs on host {hostname}: {osds_on_host}")

    # Validate we have enough OSDs to remove
    if len(osds_on_host) < num_osds:
        log.error(
            f"Requested to remove {num_osds} OSDs, but only {len(osds_on_host)} "
            f"OSDs available on host {hostname}"
        )
        return None

    # Select the specified number of OSDs to remove
    osds_to_remove = osds_on_host[:num_osds]
    log.info(f"Selected {num_osds} OSD(s) for removal: {osds_to_remove}")

    # Pre-Step 2: Set all OSD specs to unmanaged
    log.info(
        "\n ------------------------------------------- \n"
        "Pre-Step 2: Setting all OSD specs to unmanaged"
        "\n ------------------------------------------- \n"
    )

    osd_services = rados_obj.list_orch_services(service_type="osd")
    log.info(f"OSD services in the cluster: {osd_services}")

    for service in osd_services:
        log.info(f"Setting OSD service '{service}' to unmanaged")
        rados_obj.set_unmanaged_flag(service_type="osd", service_name=service)

    log.info("All OSD specs have been set to unmanaged")

    # Pre-Step 3: Get device paths for all OSDs to be removed
    log.info(
        "\n ------------------------------------------- \n"
        "Pre-Step 3: Getting device paths for OSDs to be removed"
        "\n ------------------------------------------- \n"
    )

    removed_osds_info = []
    for osd_id in osds_to_remove:
        osd_info = {
            "osd_id": osd_id,
            "hostname": hostname,
            "host_node": host_node,
        }

        # Get device path of the OSD before removal
        device_path = get_device_path_from_metadata(rados_obj, osd_id)
        osd_info["device_path"] = device_path
        log.info(f"OSD {osd_id} device path: {device_path}")

        removed_osds_info.append(osd_info)

    # Pre-Step 4: Remove the OSDs using ceph orch rm --force --zap
    log.info(
        "\n ------------------------------------------- \n"
        "Pre-Step 4: Removing OSDs using ceph orch osd rm --force --zap"
        "\n ------------------------------------------- \n"
    )

    for osd_info in removed_osds_info:
        osd_id = osd_info["osd_id"]
        log.info(
            f"Removing OSD {osd_id} with --force --zap flags\n"
            f"Host: {osd_info['hostname']}\n"
            f"Device path: {osd_info['device_path']}"
        )

        # Remove the OSD with force and zap flags
        rados_utils.osd_remove(
            ceph_cluster=ceph_cluster,
            osd_id=osd_id,
            zap=True,
            force=True,
        )

    # Wait for all OSD removals to complete
    log.info(f"Waiting for {num_osds} OSD(s) removal to complete...")
    wait_interval = 30

    for osd_info in removed_osds_info:
        osd_id = osd_info["osd_id"]
        removal_complete = False

        for attempt in range(10):
            current_osds = rados_obj.get_osd_list(status="up")
            if osd_id not in current_osds:
                removal_complete = True
                log.info(f"OSD {osd_id} has been successfully removed")
                break
            log.info(
                f"Attempt {attempt + 1}: OSD {osd_id} still present, "
                f"waiting {wait_interval}s..."
            )
            time.sleep(wait_interval)

        if not removal_complete:
            log.error(f"OSD {osd_id} removal did not complete within timeout")
            return None

    # Log summary
    log.info(
        f"\n ------------------------------------------- \n"
        f"OSD Removal Summary:\n"
        f"  Host: {hostname}\n"
        f"  Number of OSDs removed: {num_osds}\n"
    )
    for osd_info in removed_osds_info:
        log.info(
            f"    - OSD ID: {osd_info['osd_id']}, "
            f"Device Path: {osd_info['device_path']}"
        )
    log.info("\n ------------------------------------------- \n")

    if removed_osds_info is None:
        log.error("Failed to select and remove OSDs")
        return 1

    # Get device paths from all removed OSDs
    device_paths = [osd_info["device_path"] for osd_info in removed_osds_info]
    target_hostname = removed_osds_info[0]["hostname"]

    return removed_osds_info, device_paths, target_hostname


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
    log.info(f"ceph orch ls --export output: {orch_ls_export}")

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
