import csv
import os
import traceback

from utility.log import Log

log = Log(__name__)

log_base_dir = os.path.dirname(log.logger.handlers[0].baseFilename)
log_dir = f"{log_base_dir}/SnapDiff_Results/"
log.info(f"Log Dir : {log_dir}")


def read_csv(file_path):
    """
    Reads the CSV file from the provided file path and extracts the 'Snapshot Name' and 'Sync Duration' values.

    :param file_path: Path to the CSV file.
    :return: Dictionary where the key is the snapshot name and the value is the sync duration as a float.
             Returns None if an error occurs while reading the CSV file.
    """
    data = {}
    try:
        with open(file_path, mode="r") as file:
            reader = csv.DictReader(file)
            for row in reader:
                data[row["Snapshot Name"]] = float(row["Sync Duration"])
        log.info(f"Successfully read CSV file: {file_path}")
    except Exception as e:
        log.error(f"Failed to read CSV file: {file_path}. Error: {e}")
        log.error(traceback.format_exc())
        return None
    return data


def compare_sync_duration(data_v7, data_v8):
    """
    Compares the sync duration values between two versions of snapshot data (v7 and v8).

    If the sync duration in version 8 is greater than in version 7, a warning is logged.
    If the sync duration in version 8 is smaller than in version 7, an info log is added.
    If the durations are equal, an info log is added.
    If a snapshot is missing in version 8, a warning is logged.

    :param data_v7: Dictionary of sync durations for version 7.
    :param data_v8: Dictionary of sync durations for version 8.
    :return: None
    """
    for snapshot_name, sync_duration_v7 in data_v7.items():
        sync_duration_v8 = data_v8.get(snapshot_name)

        if sync_duration_v8 is not None:
            if sync_duration_v8 > sync_duration_v7:
                log.warning(
                    f"Sync duration increased for {snapshot_name}: {sync_duration_v7} -> {sync_duration_v8}. "
                    f"Validate the results manually"
                )
            elif sync_duration_v8 < sync_duration_v7:
                log.info(
                    f"Sync duration decreased for {snapshot_name}: {sync_duration_v7} -> {sync_duration_v8}. "
                    f"Performance improvements are seen."
                )
            else:
                log.info(f"No change in sync duration for {snapshot_name}.")
        else:
            log.warning(f"Snapshot {snapshot_name} not found in the second CSV.")


def run(ceph_cluster, **kw):
    try:
        config = kw.get("config")
        csv_version_7 = config.get("result_filev7")
        csv_version_8 = config.get("result_filev8")

        v7_path = os.path.join(log_dir, csv_version_7)
        v8_path = os.path.join(log_dir, csv_version_8)

        result_v7 = read_csv(v7_path)
        result_v8 = read_csv(v8_path)

        if result_v7 is None or result_v8 is None:
            log.error("Error reading one or both CSV files. Exiting.")
            return 1

        compare_sync_duration(result_v7, result_v8)
        log.info("Validation completed successfully.")

        return 0
    except Exception as e:
        log.error(f"An error occurred during execution: {e}")
        log.error(traceback.format_exc())
        return 1
