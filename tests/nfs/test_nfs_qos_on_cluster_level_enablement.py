import json
import re
from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)


def _maybe_json_loads(value):
    """Return dict/list if value is a JSON string; otherwise return as-is."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


def _extract_first_float(text):
    if text is None:
        return None
    match = re.search(r"\d+(?:\.\d+)?", str(text))
    return float(match.group(0)) if match else None


def _within_qos_limit(limit_mb, actual_mbps, slack_ratio=0.2, slack_abs=0.5):
    """Return True if actual is within limit allowing some measurement slack."""
    limit = _extract_first_float(limit_mb)
    actual = _extract_first_float(actual_mbps)
    if limit is None or actual is None:
        return False
    allowed = max(limit * (1.0 + slack_ratio), limit + slack_abs)
    return actual <= allowed


def _dd_speed_to_mbps(dd_output: str):
    """Parse dd throughput and return MB/s (decimal) as float.

    Supports common dd units: B/s, kB/s, KB/s, KiB/s, MB/s, MiB/s, GB/s, GiB/s.
    Returns None if no throughput token is found.
    """

    matches = re.findall(
        r"(\d+(?:\.\d+)?)\s*(B/s|kB/s|KB/s|KiB/s|MB/s|MiB/s|GB/s|GiB/s)",
        dd_output,
    )
    if not matches:
        return None

    value_str, unit = matches[-1]
    value = float(value_str)

    if unit == "B/s":
        bytes_per_sec = value
    elif unit in ("kB/s", "KB/s"):
        bytes_per_sec = value * 1000
    elif unit == "KiB/s":
        bytes_per_sec = value * 1024
    elif unit == "MB/s":
        bytes_per_sec = value * 1_000_000
    elif unit == "MiB/s":
        bytes_per_sec = value * 1_048_576
    elif unit == "GB/s":
        bytes_per_sec = value * 1_000_000_000
    elif unit == "GiB/s":
        bytes_per_sec = value * 1_073_741_824
    else:
        return None

    return bytes_per_sec / 1_000_000


def capture_copy_details(client, nfs_mount, file_name, size="100"):
    """
    Captures the output of the dd command executed remotely.
    :param nfs_mount: The NFS mount path where the file will be created.
    :param size: The size of the file to create (default is "100M").
    :return: A tuple containing (stdout, stderr) from the dd command.
    """
    drop_cache_cmd = "echo 3 > /proc/sys/vm/drop_caches"
    write_speed, read_speed = "", ""
    cmd = "touch {0}/{1}".format(nfs_mount, file_name)
    client.exec_command(
        sudo=True,
        cmd=cmd,
    )

    client.exec_command(
        sudo=True,
        cmd=drop_cache_cmd,
    )
    log.info("Cache dropped successfully")
    write_cmd = "dd if=/dev/urandom of={0}/{1} bs={2}M count=1 oflag=direct".format(
        nfs_mount, file_name, size
    )
    write_results = client.exec_command(sudo=True, cmd=write_cmd)[1]

    write_mbps = _dd_speed_to_mbps(write_results)
    if write_mbps is not None:
        write_speed = f"{write_mbps:.3f} MB/s"
        log.info("File created successfully on {0}".format(client.hostname))
        log.info("write speed is {0}".format(write_speed))

    client.exec_command(
        sudo=True,
        cmd=drop_cache_cmd,
    )
    log.info("Cache dropped successfully")

    # Use /dev/null as sink for read throughput; /dev/urandom can distort timing.
    # Specify bs/count to avoid dd defaulting to tiny blocks (can time out with direct I/O).
    read_cmd = "dd if={0}/{1} of=/dev/null bs={2}M count=1 iflag=direct".format(
        nfs_mount, file_name, size
    )
    read_results = client.exec_command(sudo=True, cmd=read_cmd)[1]

    read_mbps = _dd_speed_to_mbps(read_results)
    if read_mbps is not None:
        read_speed = f"{read_mbps:.3f} MB/s"
        log.info("read speed is {0}".format(read_speed))

    rm_cmd = "rm -rf {0}/{1}".format(nfs_mount, file_name)
    client.exec_command(
        sudo=True,
        cmd=rm_cmd,
    )

    if write_speed and read_speed:
        log.info(
            'Read and write speed captured successfully on {0} "write_speed": {1}, "read_speed": {2}'.format(
                client.hostname, write_speed, read_speed
            ),
        )
        return {"write_speed": write_speed, "read_speed": read_speed}
    else:
        raise OperationFailedError(
            "Failed to capture read/write speed on {0}".format(client.hostname)
        )


def validate_qos_operation(operation_key, qos_type, cluster_name, qos_data):
    """Validate QoS operation result and handle logging."""
    expected_key = True if operation_key == "enable" else False
    qos_data = json.loads(qos_data)

    log.info("QoS data: {0}".format(qos_data))

    if qos_data.get("enable_bw_control") == expected_key:
        log.info(
            "QoS {0} for {1} cluster {2} is successful".format(
                operation_key, qos_type, cluster_name
            )
        )
    else:
        raise OperationFailedError(
            "QoS {0} for {1} cluster {2} failed".format(
                operation_key, qos_type, cluster_name
            )
        )


def enable_disable_qos_for_cluster(
    enable_flag,
    ceph_cluster_nfs_obj,
    cluster_name,
    qos_type=None,
    **qos_parameters,
):
    # Common validation
    if enable_flag and not qos_type:
        raise ValueError("qos_type is required when enabling QoS")

    operation_key = "enable" if enable_flag else "disable"

    try:
        if enable_flag:
            if qos_type == "PerShare":
                if "max_export_combined_bw" in qos_parameters:
                    ceph_cluster_nfs_obj.qos.enable_per_share(
                        qos_type=qos_type,
                        cluster_id=cluster_name,
                        max_export_combined_bw=qos_parameters.get(
                            "max_export_combined_bw"
                        ),
                    )
                else:
                    ceph_cluster_nfs_obj.qos.enable_per_share(
                        qos_type=qos_type,
                        cluster_id=cluster_name,
                        max_export_read_bw=qos_parameters.get("max_export_read_bw"),
                        max_export_write_bw=qos_parameters.get("max_export_write_bw"),
                    )
            elif qos_type == "PerClient":
                if "max_client_combined_bw" in qos_parameters:
                    ceph_cluster_nfs_obj.qos.enable_per_client(
                        qos_type=qos_type,
                        cluster_id=cluster_name,
                        max_client_combined_bw=qos_parameters.get(
                            "max_client_combined_bw"
                        ),
                    )
                else:
                    ceph_cluster_nfs_obj.qos.enable_per_client(
                        qos_type=qos_type,
                        cluster_id=cluster_name,
                        max_client_read_bw=qos_parameters.get("max_client_read_bw"),
                        max_client_write_bw=qos_parameters.get("max_client_write_bw"),
                    )
            elif qos_type == "PerShare_PerClient":
                if "max_client_combined_bw" in qos_parameters:
                    ceph_cluster_nfs_obj.qos.enable_per_share_per_client(
                        qos_type=qos_type,
                        cluster_id=cluster_name,
                        max_export_combined_bw=qos_parameters.get(
                            "max_export_combined_bw"
                        ),
                        max_client_combined_bw=qos_parameters.get(
                            "max_client_combined_bw"
                        ),
                    )
                else:
                    ceph_cluster_nfs_obj.qos.enable_per_share_per_client(
                        qos_type=qos_type,
                        cluster_id=cluster_name,
                        max_export_read_bw=qos_parameters.get("max_export_read_bw"),
                        max_export_write_bw=qos_parameters.get("max_export_write_bw"),
                        max_client_read_bw=qos_parameters.get("max_client_read_bw"),
                        max_client_write_bw=qos_parameters.get("max_client_write_bw"),
                    )
        else:
            ceph_cluster_nfs_obj.qos.disable(cluster_id=cluster_name)

        qos_data = ceph_cluster_nfs_obj.qos.get(cluster_id=cluster_name, format="json")
        validate_qos_operation(
            operation_key=operation_key,
            qos_type=qos_type,
            cluster_name=cluster_name,
            qos_data=qos_data,
        )
        log.info(
            "QoS {0} {1} for cluster {2}".format(qos_data, operation_key, cluster_name)
        )

    except Exception as e:
        raise RuntimeError(
            "QoS {0} failed for {1} cluster {2}".format(
                operation_key, qos_type, cluster_name
            )
        ) from e


def _parse_exec_result(res):
    """
    Normalize client.exec_command return to (rc, stdout, stderr).
    Supports (rc,out,err), (out,err), (out,), single string, or None.
    """
    rc = 0
    stdout = ""
    stderr = ""
    if res is None:
        return rc, stdout, stderr
    if isinstance(res, (tuple, list)):
        if len(res) == 3:
            rc, stdout, stderr = res
        elif len(res) == 2:
            stdout, stderr = res
        elif len(res) == 1:
            stdout = res[0] or ""
    else:
        stdout = str(res or "")
    # normalize types
    try:
        rc = int(rc or 0)
    except Exception:
        rc = 0
    return rc, (stdout or ""), (stderr or "")


def _wait_for_orch_service_running(client, service_name, timeout=180, interval=5):
    """Wait until `ceph orch ps --service_name <service>` reports all daemons as running."""
    remaining = int(timeout)
    while remaining > 0:
        cmd = f"ceph orch ps --service_name {service_name} --format json"
        rc, stdout, stderr = _parse_exec_result(client.exec_command(sudo=True, cmd=cmd))
        payload = (stdout or "").strip() or (stderr or "").strip()
        try:
            ps = json.loads(payload) if payload else []
        except Exception:
            ps = []

        if (
            rc == 0
            and ps
            and all(
                str(d.get("status_desc", "")).strip().lower() == "running" for d in ps
            )
        ):
            log.info(f"Service {service_name} is running: {ps}")
            return True

        sleep(interval)
        remaining -= interval

    raise OperationFailedError(
        f"Timed out waiting for {service_name} to become running via 'ceph orch ps'"
    )


def _wait_for_qos_persisted(
    ceph_nfs_client, cluster_name, qos_type, timeout=120, interval=5
):
    """Poll `ceph nfs cluster qos get` until QoS is enabled and matches expected qos_type."""
    remaining = int(timeout)
    while remaining > 0:
        qos_data = ceph_nfs_client.cluster.qos.get(
            cluster_id=cluster_name, format="json"
        )
        try:
            qos = json.loads(qos_data)
        except Exception:
            qos = {}

        if qos.get("enable_bw_control") is True and qos.get("qos_type") == qos_type:
            return qos

        sleep(interval)
        remaining -= interval

    raise OperationFailedError(
        f"Timed out waiting for QoS to persist after restart: cluster={cluster_name}, qos_type={qos_type}"
    )


def _format_cli_cmd(base, *args):
    parts = [base] + [str(a) for a in args if a is not None and a != ""]
    return " ".join(parts)


def _qos_cli_token(qos_type):
    """Normalize user-provided qos_type to Ceph CLI token."""
    if not qos_type:
        return None
    qt = str(qos_type).strip().lower()
    if qt in ("pershare", "per_share", "per-share"):
        return "PerShare"
    if qt in ("perclient", "per_client", "per-client"):
        return "PerClient"
    if qt in (
        "pershare_perclient",
        "perclient_pershare",
        "per_share_per_client",
        "per-share-per-client",
        "pershare-perclient",
    ):
        return "PerShare_PerClient"
    return qos_type  # assume already canonical


def enable_cluster_ops_control(client, cluster_name, qos_type, ops_config):
    if not qos_type:
        raise ConfigError("qos_type is required to enable cluster ops control")
    qtoken = _qos_cli_token(qos_type)
    if not qtoken:
        raise ConfigError("invalid qos_type provided")
    base = f"ceph nfs cluster qos enable ops_control {cluster_name} {qtoken}"

    if qtoken == "PerShare":
        if "max_export_iops" not in ops_config:
            raise ConfigError("max_export_iops required for pershare ops control")
        cmd = _format_cli_cmd(base, ops_config["max_export_iops"])
    elif qtoken == "PerClient":
        if "max_client_iops" not in ops_config:
            raise ConfigError("max_client_iops required for perclient ops control")
        cmd = _format_cli_cmd(base, ops_config["max_client_iops"])
    elif qtoken == "PerShare_PerClient":
        if not all(k in ops_config for k in ("max_export_iops", "max_client_iops")):
            raise ConfigError("Both max_export_iops and max_client_iops required")
        cmd = _format_cli_cmd(
            base, ops_config["max_export_iops"], ops_config["max_client_iops"]
        )
    else:
        raise ConfigError(f"Invalid ops control type: {qos_type}")

    try:
        res = client.exec_command(sudo=True, cmd=cmd)
        rc, out, err = _parse_exec_result(res)
        if rc != 0 or (err and err.strip()):
            raise OperationFailedError(
                f"Failed to enable cluster ops_control (cmd: '{cmd}'). rc={rc}, stdout='{out}', stderr='{err}'"
            )
        log.info(f"Cluster ops_control enabled via CLI: {cmd}")
        return cmd
    except Exception as e:
        log.error(f"enable_cluster_ops_control error: {e}")
        raise


def disable_cluster_ops_control(client, cluster_name):
    """Disable cluster-level QoS using CLI"""
    cmd = f"ceph nfs cluster qos disable {cluster_name}"
    try:
        res = client.exec_command(sudo=True, cmd=cmd)
        rc, out, err = _parse_exec_result(res)
        if rc != 0 or (err and err.strip()):
            raise OperationFailedError(
                f"Failed to disable cluster ops_control (cmd: '{cmd}'). rc={rc}, stdout='{out}', stderr='{err}'"
            )
        log.info(f"Cluster ops_control disabled via CLI: {cmd}")
        return cmd
    except Exception as e:
        log.error(f"disable_cluster_ops_control error: {e}")
        raise


def enable_export_ops_control(client, cluster_name, pseudo_path, qos_type, ops_config):
    if not qos_type:
        raise ConfigError("qos_type is required to enable export ops control")
    qtoken = _qos_cli_token(qos_type)
    if not qtoken:
        raise ConfigError("invalid qos_type provided")
    # NOTE: export CLI does NOT accept a qos token; it accepts only numeric limits.
    base = f"ceph nfs export qos enable ops_control {cluster_name} {pseudo_path}"

    # Build command based on provided limits (export CLI expects [<max_export_iops>] [<max_client_iops>])
    if "max_export_iops" in ops_config and "max_client_iops" in ops_config:
        cmd = _format_cli_cmd(
            base, ops_config["max_export_iops"], ops_config["max_client_iops"]
        )
    elif "max_export_iops" in ops_config:
        cmd = _format_cli_cmd(base, ops_config["max_export_iops"])
    elif "max_client_iops" in ops_config:
        # exporting only per-client limit at export-level is not meaningful if server expects export limit first
        raise ConfigError(
            "max_export_iops required when specifying export-level max_client_iops"
        )
    else:
        raise ConfigError(
            "At least max_export_iops (or both max_export_iops and "
            "max_client_iops) required for export ops control"
        )

    try:
        res = client.exec_command(sudo=True, cmd=cmd)
        rc, out, err = _parse_exec_result(res)
        if rc != 0 or (err and err.strip()):
            raise OperationFailedError(
                f"Failed to enable export ops_control (cmd: '{cmd}'). rc={rc}, stdout='{out}', stderr='{err}'"
            )
        log.info(f"Export ops_control enabled via CLI: {cmd}")
        return cmd
    except Exception as e:
        log.error(f"enable_export_ops_control error: {e}")
        raise


def verify_ops_control_settings(client, cluster_name, export_path=None):
    """Verify ops control settings at cluster and export level"""
    try:
        # cluster settings
        res = client.exec_command(
            sudo=True, cmd=f"ceph nfs cluster qos get {cluster_name} --format json"
        )
        rc, stdout, stderr = _parse_exec_result(res)
        payload = (stdout or "").strip() or (stderr or "").strip()
        if rc != 0 and not payload:
            raise OperationFailedError(
                (
                    f"Failed to fetch cluster ops control settings for {cluster_name}. "
                    f"rc={rc}, stdout='{stdout}', stderr='{stderr}'"
                )
            )
        try:
            cluster_settings = json.loads(payload)
        except Exception as e:
            raise OperationFailedError(
                f"Failed to parse cluster qos json: {e}; payload='{payload}'"
            )
        log.info(f"Current cluster ops control settings: {cluster_settings}")

        # export settings if provided
        if export_path:
            res = client.exec_command(
                sudo=True,
                cmd=f"ceph nfs export qos get {cluster_name} {export_path} --format json",
            )
            rc, stdout, stderr = _parse_exec_result(res)
            payload = (stdout or "").strip() or (stderr or "").strip()
            if rc != 0 and not payload:
                raise OperationFailedError(
                    (
                        f"Failed to fetch export ops control settings for {export_path}. "
                        f"rc={rc}, stdout='{stdout}', stderr='{stderr}'"
                    )
                )
            try:
                export_settings = json.loads(payload)
            except Exception as e:
                raise OperationFailedError(
                    f"Failed to parse export qos json: {e}; payload='{payload}'"
                )
            log.info(
                f"Current export ops control settings for {export_path}: {export_settings}"
            )
            return cluster_settings, export_settings

        return cluster_settings, None
    except Exception as e:
        log.error(f"Failed to get ops control settings: {str(e)}")
        return None, None


def extract_dd_time(dd_output):
    """Extract time taken from dd command output"""
    try:
        if not dd_output:
            return None
        # Get the last line containing 'copied' and parse the time token before 's,'
        for line in reversed(dd_output.splitlines()):
            if "copied" in line:
                # typical: "67108864 bytes (67 MB, 64 MiB) copied, 4.47958 s, 15.0 MB/s"
                parts = line.split("copied,")
                if len(parts) > 1:
                    after = parts[1]
                    # find "<num> s" token
                    m = re.search(r"(\d+\.\d+)\s*s", after)
                    if m:
                        return float(m.group(1))
                    # fallback to last float in the 'after' part
                    fm = re.findall(r"(\d+\.\d+)", after)
                    if fm:
                        return float(fm[-1])
        return None
    except Exception as e:
        log.error(f"Failed to extract time from dd output: {str(e)}")
        return None


def validate_ops_limit(dd_time, expected_limit=None):
    """
    Preserve earlier behaviour: compute ops limit using formula ops_limit = int(278/dd_time).
    Do NOT enforce strict equality here — return success and the calculated value for callers to decide.
    """
    if not dd_time:
        return False, None
    try:
        calculated_limit = int(278 / float(dd_time))
    except Exception as e:
        log.error(f"validate_ops_limit failed: {e}")
        return False, None

    log.info("Validation Summary:")
    log.info(f"- Time taken: {dd_time} seconds")
    log.info(f"- Calculated ops limit: {calculated_limit} (278/{dd_time})")
    if expected_limit is not None:
        log.info(f"- Expected ops limit (not enforced here): {expected_limit}")

    return True, calculated_limit


def validate_ops_control(client, nfs_mount, file_name, dd_params):
    """Validate ops control by measuring IO operations (wrapped with try/except)."""
    try:
        # create test file
        cmd = f"touch {nfs_mount}/{file_name}"
        client.exec_command(sudo=True, cmd=cmd)

        # write test (redirect stderr to stdout to capture dd progress)
        write_cmd = (
            f"dd if={dd_params.get('input_file', '/dev/zero')} of={nfs_mount}/{file_name} "
            f"bs={dd_params['block_size']} count={dd_params['count']} status=progress 2>&1"
        )
        res = client.exec_command(sudo=True, cmd=write_cmd)
        rc, write_results, write_err = _parse_exec_result(res)
        if rc != 0:
            raise OperationFailedError(
                f"Write dd failed. rc={rc}, stdout='{write_results}', stderr='{write_err}'"
            )
        log.info(f"Write test results: {write_results}")

        # extract write time
        write_time = extract_dd_time(write_results)
        log.info(f"Write operation took {write_time} seconds")

        # drop caches
        client.exec_command(sudo=True, cmd="echo 3 > /proc/sys/vm/drop_caches")
        log.info("Cache dropped successfully")

        # read test (redirect stderr to stdout to capture dd progress)
        read_cmd = (
            f"dd if={nfs_mount}/{file_name} of=/dev/null "
            f"bs={dd_params['block_size']} count={dd_params['count']} status=progress 2>&1"
        )
        res = client.exec_command(sudo=True, cmd=read_cmd)
        rc, read_results, read_err = _parse_exec_result(res)
        if rc != 0:
            raise OperationFailedError(
                f"Read dd failed. rc={rc}, stdout='{read_results}', stderr='{read_err}'"
            )
        log.info(f"Read test results: {read_results}")

        # extract read time
        read_time = extract_dd_time(read_results)
        log.info(f"Read operation took {read_time} seconds")

        return {
            "write_results": write_results,
            "read_results": read_results,
            "write_time": write_time,
            "read_time": read_time,
        }

    except Exception as e:
        log.error(f"validate_ops_control failed: {e}")
        raise
    finally:
        # best-effort cleanup of test file
        try:
            client.exec_command(sudo=True, cmd=f"rm -rf {nfs_mount}/{file_name}")
        except Exception as cleanup_err:
            log.debug(f"validate_ops_control cleanup failed: {cleanup_err}")


def run(ceph_cluster, **kw):
    """Verify QoS operations on NFS cluster"""
    config = kw.get("config")
    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    cluster_name = config["cluster_name"]
    operation = config.get("operation", None)
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.2")
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    subvolume_group = "ganeshagroup"

    if not nfs_nodes:
        raise OperationFailedError("No NFS nodes found in cluster")

    nfs_node = nfs_nodes[0]
    qos_type = config.get("qos_type", None)
    no_clients = int(config.get("clients", "1"))

    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]
    client = clients[0]

    _orig_exec = client.exec_command
    ceph_nfs_client = Ceph(client).nfs
    Ceph(client).fs.sub_volume_group.create(volume=fs_name, group=subvolume_group)

    try:
        # Setup nfs cluster
        setup_nfs_cluster(
            clients,
            nfs_node.hostname,
            port,
            version,
            nfs_name,
            nfs_mount,
            fs_name,
            nfs_export,
            fs,
            ceph_cluster=ceph_cluster,
        )

        # Process QoS operations
        enable_disable_qos_for_cluster(
            enable_flag=True,
            qos_type=qos_type,
            ceph_cluster_nfs_obj=ceph_nfs_client.cluster,
            cluster_name=cluster_name,
            **{
                k: config[k]
                for k in [
                    "max_export_write_bw",
                    "max_export_read_bw",
                    "max_client_write_bw",
                    "max_client_read_bw",
                    "max_export_combined_bw",
                    "max_client_combined_bw",
                ]
                if k in config
            },
        )

        if operation == "restart":
            data = json.loads(Ceph(client).orch.ls(format="json"))
            [service_name] = [
                x["service_name"] for x in data if x.get("service_id") == cluster_name
            ]

            Ceph(client).orch.restart(service_name)
            if cluster_name not in [x["service_name"] for x in data]:
                sleep(1)
            # Waiting for QoS settings to load after cluster restart
            sleep(20)
            qos_data_after_restart = ceph_nfs_client.cluster.qos.get(
                cluster_id=cluster_name, format="json"
            )
            qos_data_after_restart = _maybe_json_loads(qos_data_after_restart)
            if (
                isinstance(qos_data_after_restart, dict)
                and qos_data_after_restart.get("qos_type") == qos_type
            ):
                log.info(
                    "Qos data for {0} persists even after the nfs cluster restarted".format(
                        qos_type
                    )
                )
            else:
                raise OperationFailedError(
                    "Qos data for {0} did not persists after the nfs cluster restarted, after restart {1}".format(
                        qos_type, qos_data_after_restart
                    )
                )
        speed = capture_copy_details(client, nfs_mount, "sample.txt")
        log.info(
            "Transfer speed is {0} for QoS {1} enabled in cluster level".format(
                speed, qos_type
            )
        )

        write_speed = speed.get("write_speed")
        read_speed = speed.get("read_speed")

        max_export_write_bw = config.get("max_export_write_bw")
        max_export_read_bw = config.get("max_export_read_bw")
        max_client_write_bw = config.get("max_client_write_bw")
        max_client_read_bw = config.get("max_client_read_bw")
        max_export_combined_bw = config.get("max_export_combined_bw")
        if max_export_combined_bw:
            max_export_write_bw = max_export_combined_bw
            max_export_read_bw = max_export_combined_bw
        max_client_combined_bw = config.get("max_client_combined_bw")
        if max_client_combined_bw:
            max_client_write_bw = max_client_combined_bw
            max_client_read_bw = max_client_combined_bw

        export_ok = False
        if max_export_write_bw is not None or max_export_read_bw is not None:
            export_ok = True
            if max_export_write_bw is not None:
                export_ok = export_ok and _within_qos_limit(
                    max_export_write_bw, write_speed
                )
            if max_export_read_bw is not None:
                export_ok = export_ok and _within_qos_limit(
                    max_export_read_bw, read_speed
                )

        client_ok = False
        if max_client_write_bw is not None or max_client_read_bw is not None:
            client_ok = True
            if max_client_write_bw is not None:
                client_ok = client_ok and _within_qos_limit(
                    max_client_write_bw, write_speed
                )
            if max_client_read_bw is not None:
                client_ok = client_ok and _within_qos_limit(
                    max_client_read_bw, read_speed
                )

        if export_ok or client_ok:
            log.info(
                "Test passed: QoS {0} enabled successfully in cluster level write speed is {1}"
                " , max_export_write_bw is {2} and read speed is {3}"
                " and max_export_read_bw is {4}".format(
                    qos_type,
                    write_speed,
                    max_export_write_bw,
                    read_speed,
                    max_export_read_bw,
                )
            )
        else:
            raise OperationFailedError(
                "Test failed: QoS {0} enabled successfully in cluster level write speed is {1}"
                " and read speed is {2} config is {3}".format(
                    qos_type, write_speed, read_speed, config
                )
            )

        enable_disable_qos_for_cluster(
            enable_flag=False,
            qos_type=qos_type,
            ceph_cluster_nfs_obj=ceph_nfs_client.cluster,
            cluster_name=cluster_name,
        )
        return 0
    except (ConfigError, OperationFailedError, RuntimeError) as e:
        log.error("Test failed: {0}".format(e))
        return 1
    finally:
        log.info("Cleanup in progress")
        log.debug("deleting NFS cluster {0}".format(cluster_name))
        # restore original exec_command so other tests are unaffected
        try:
            client.exec_command = _orig_exec
        except Exception:
            pass
        cleanup_cluster(client, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_node)
