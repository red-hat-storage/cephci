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
            next(file)  # Skip "Ceph Version: ..."
            next(file)  # Skip blank line
            reader = csv.DictReader(file)
            for row in reader:
                data[row["Snapshot Name"]] = float(row["Sync Duration"])
        log.info(f"Successfully read CSV file: {file_path}")
    except Exception as e:
        log.error(f"Failed to read CSV file: {file_path}. Error: {e}")
        log.error(traceback.format_exc())
        return None
    return data


def compare_sync_duration(data_v_n_1, data_v_n):
    """
    Compares the sync duration values between two versions of snapshot data (v_n_1 and v_n).

    If the sync duration in v_n is greater than in v_n_1, a warning is logged.
    If the sync duration in v_n is smaller than in v_n_1, an info log is added.
    If the durations are equal, an info log is added.
    If a snapshot is missing in v_n, a warning is logged.

    :param data_v_n_1: Dictionary of sync durations for version n-1.
    :param data_v_n: Dictionary of sync durations for version n.
    :return: None
    """
    for snapshot_name, sync_duration_v_n_1 in data_v_n_1.items():
        sync_duration_v_n = data_v_n.get(snapshot_name)

        if sync_duration_v_n is not None:
            if sync_duration_v_n > sync_duration_v_n_1:
                log.warning(
                    f"Sync duration increased for {snapshot_name}: {sync_duration_v_n_1} -> {sync_duration_v_n}. "
                    f"Validate the results manually"
                )
            elif sync_duration_v_n < sync_duration_v_n_1:
                log.info(
                    f"Sync duration decreased for {snapshot_name}: {sync_duration_v_n_1} -> {sync_duration_v_n}. "
                    f"Performance improvements are seen."
                )
            else:
                log.info(f"No change in sync duration for {snapshot_name}.")
        else:
            log.warning(f"Snapshot {snapshot_name} not found in the second CSV.")


def run(ceph_cluster, **kw):
    try:
        config = kw.get("config")
        csv_file_v_n_1 = config.get("result_file_v_n_1")
        csv_file_v_n = config.get("result_file_v_n")

        path_v_n_1 = os.path.join(log_dir, csv_file_v_n_1)
        path_v_n = os.path.join(log_dir, csv_file_v_n)

        result_v_n_1 = read_csv(path_v_n_1)
        result_v_n = read_csv(path_v_n)

        if result_v_n_1 is None or result_v_n is None:
            log.error("Error reading one or both CSV files. Exiting.")
            return 1

        compare_sync_duration(result_v_n_1, result_v_n)
        log.info("Validation completed successfully.")

        return 0
    except Exception as e:
        log.error(f"An error occurred during execution: {e}")
        log.error(traceback.format_exc())
        return 1
