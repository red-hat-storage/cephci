"""Shared constants for NFS Multi-Active suite tests."""

# Timeouts (seconds)
DEPLOY_TIMEOUT = 300
IO_RESUME_TIMEOUT = 300
IO_JOIN_TIMEOUT = 120
MARKER_IO_TIMEOUT = 60
DEFAULT_HEALTH_WAIT = 1200
DEFAULT_TC3_SERVICE_WAIT_TIMEOUT = DEPLOY_TIMEOUT

# Short delays / polling (seconds)
POLL_INTERVAL_SEC = 5
IO_WARMUP_SEC = POLL_INTERVAL_SEC
BG_IO_START_DELAY = POLL_INTERVAL_SEC

# Mount / export
NFS_MOUNT = "/mnt/nfs"
NFS_MOUNT_A = "/mnt/nfs_a"
NFS_MOUNT_B = "/mnt/nfs_b"
EXPORT_NAME = "/export_0"
FS_NAME = "cephfs"
NFS_VERSION = "4"

# Cluster sizing
NFS_DAEMON_COUNT = 2
MIN_NFS_NODES = 4
HEALTH_CHECK_INTERVAL = "5m"

# Background IO — long enough to outlive failover + IO-resume wait
BG_IO_RUNTIME = 900
DD_RUNTIME = BG_IO_RUNTIME
FIO_RUNTIME = BG_IO_RUNTIME
IO_BLOCK_SIZE = "4M"
DD_BS = IO_BLOCK_SIZE
FIO_BS = IO_BLOCK_SIZE
DD_COUNT = 16
FIO_SIZE = "256M"
FIO_IODEPTH = 4
FIO_JOB_PREFIX = "nfs_ma"

# IO wait aliases (failover / colocation / daemon_disruption tests)
BG_IO_RESUME_TIMEOUT = IO_RESUME_TIMEOUT
BG_IO_JOIN_TIMEOUT = IO_JOIN_TIMEOUT


def log_section(logger, title):
    logger.info("\n" + "*" * 70 + "  %s\n", title)


# Service ID prefixes per suite module
SERVICE_ID_PREFIX = {
    "functional": "cephfs-nfs",
    "failover": "cephfs-nfs",
    "colocation": "cephfs-nfs-coloc",
    "integration": "cephfs-nfs",
    "negative": "cephfs-nfs-neg",
    "daemon_disruption": "cephfs-nfs",
}

# QoS defaults (integration TC-2 / TC-4 colocation)
DEFAULT_EXPORT_BW = {"max_export_combined_bw": "8MB"}
DEFAULT_CLUSTER_BW = {"max_export_combined_bw": "100MB"}
DEFAULT_CLIENT_BW = {"max_client_combined_bw": "8MB"}
DEFAULT_TC4_CLUSTER_BW = {
    "max_client_combined_bw": "100MB",
    "max_export_combined_bw": "100MB",
}
DEFAULT_QOS_TYPE = "PerShare"
DEFAULT_TC4_QOS_TYPE = "PerShare_PerClient"
DEFAULT_QOS_OPERATION = "bandwidth_control"
QOS_BLOCK_POLL_TIMEOUT = 30
QOS_BLOCK_POLL_INTERVAL = 2
QOS_SETTLE_SEC = 10
QOS_BW_KEYS = (
    "max_export_write_bw",
    "max_export_read_bw",
    "max_export_combined_bw",
    "max_export_iops",
    "max_client_write_bw",
    "max_client_read_bw",
    "max_client_combined_bw",
    "max_client_iops",
)
DEFAULT_TC4_FIO_PARAMS = {
    "bs": "1M",
    "iodepth": 1,
    "numjobs": 1,
    "runtime": 15,
    "size": FIO_SIZE,
}

# Integration workflow presets
INTEGRATION_PORTS = (17792, 17892, 17992)
TC4_NFS_COUNT = 4
TC4_COLOCATION = {
    "port": 18692,
    "monitoring_port": 19692,
    "colocation_ports": [
        {"data_port": 18792, "monitoring_port": 19792, "cluster_qos_port": 32711},
        {"data_port": 18892, "monitoring_port": 19892, "cluster_qos_port": 33711},
        {"data_port": 18992, "monitoring_port": 19992, "cluster_qos_port": 34711},
    ],
}
TC4_INGRESS_PORTS = (18801, 18901)
TC4_LABELS = {"nfs": "nfs-ma-int-tc4-nfs", "ingress": "nfs-ma-int-tc4-ingress"}
DEFAULT_TC3_REDEPLOY_WAIT = 10
DEFAULT_TC3_DELEGATION_HOLD_SEC = 300
DEFAULT_TC3_DELEGATION_HOLD_READY_SEC = 8
DEFAULT_TC3_DELEGATION_RELEASE_WAIT_SEC = 8
