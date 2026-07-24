"""QoS helpers for NFS Multi-Active integration tests (TC-2 / TC-4)."""

from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import OperationFailedError
from tests.nfs.lib.multi_active.constants import (
    DEFAULT_CLIENT_BW,
    DEFAULT_CLUSTER_BW,
    DEFAULT_EXPORT_BW,
    DEFAULT_TC4_CLUSTER_BW,
    DEFAULT_TC4_QOS_TYPE,
    EXPORT_NAME,
    QOS_BLOCK_POLL_INTERVAL,
    QOS_BLOCK_POLL_TIMEOUT,
    QOS_BW_KEYS,
)
from tests.nfs.qos.test_nfs_qos_on_cluster_level_enablement import (
    _maybe_json_loads,
    _within_qos_limit,
    capture_copy_details,
    enable_disable_qos_for_cluster,
)
from tests.nfs.qos.test_nfs_qos_on_export_level_enablement import (
    enable_disable_qos_for_export,
)
from utility.log import Log

log = Log(__name__)


class NfsMultiActiveQos:
    """Export/cluster QoS enablement, polling, and speed validation."""

    @staticmethod
    def export_bw_config(config):
        export_bw = config.get("export_bw")
        if isinstance(export_bw, list):
            export_bw = export_bw[0]
        if not export_bw:
            export_bw = DEFAULT_EXPORT_BW
        return dict(export_bw)

    @staticmethod
    def cluster_bw_config(config):
        cluster_bw = config.get("cluster_bw")
        if isinstance(cluster_bw, list):
            cluster_bw = cluster_bw[0]
        if not cluster_bw:
            cluster_bw = DEFAULT_CLUSTER_BW
        return dict(cluster_bw)

    @staticmethod
    def client_bw_config(config):
        client_bw = config.get("tc4_client_bw") or config.get("client_bw")
        if isinstance(client_bw, list):
            client_bw = client_bw[0]
        if not client_bw:
            export_bw = config.get("tc4_export_bw") or config.get("export_bw")
            if isinstance(export_bw, list):
                export_bw = export_bw[0]
            if export_bw:
                client_bw = {
                    k: export_bw[k]
                    for k in (
                        "max_client_write_bw",
                        "max_client_read_bw",
                        "max_client_combined_bw",
                        "max_client_iops",
                    )
                    if k in export_bw
                }
        if not client_bw:
            client_bw = DEFAULT_CLIENT_BW
        return dict(client_bw)

    @staticmethod
    def tc4_cluster_bw_config(config):
        cluster_bw = config.get("tc4_cluster_bw") or config.get("cluster_bw")
        if isinstance(cluster_bw, list):
            cluster_bw = cluster_bw[0]
        if not cluster_bw:
            cluster_bw = dict(DEFAULT_TC4_CLUSTER_BW)
        else:
            cluster_bw = dict(cluster_bw)
        qos_type = config.get(
            "tc4_qos_type", config.get("qos_type", DEFAULT_TC4_QOS_TYPE)
        )
        if qos_type == "PerShare_PerClient":
            cluster_bw.setdefault("max_export_combined_bw", "100MB")
            cluster_bw.setdefault("max_client_combined_bw", "100MB")
        return cluster_bw

    @staticmethod
    def tc4_export_bw_config(config):
        export_bw = dict(NfsMultiActiveQos.client_bw_config(config))
        cluster_bw = NfsMultiActiveQos.tc4_cluster_bw_config(config)
        export_bw.setdefault(
            "max_export_combined_bw",
            cluster_bw.get("max_export_combined_bw", "100MB"),
        )
        return export_bw

    @staticmethod
    def bw_params(bw_config):
        return {k: bw_config[k] for k in QOS_BW_KEYS if k in bw_config}

    @staticmethod
    def limit_key(bw_config):
        for key in (
            "max_client_combined_bw",
            "max_client_write_bw",
            "max_client_read_bw",
            "max_export_combined_bw",
            "max_export_write_bw",
            "max_export_read_bw",
        ):
            if bw_config.get(key):
                return key, bw_config[key]
        return None, None

    @staticmethod
    def enable(session, cluster_bw, export_bw, qos_type, qos_operation):
        """Enable cluster QoS; export QoS for PerShare / PerShare_PerClient."""
        client = session.client1
        ceph_nfs = Ceph(client).nfs
        params = NfsMultiActiveQos.bw_params
        if qos_type == "PerClient":
            enable_disable_qos_for_cluster(
                enable_flag=True,
                ceph_cluster_nfs_obj=ceph_nfs.cluster,
                cluster_name=session.nfs_service_id,
                qos_type=qos_type,
                operation=qos_operation,
                **params(export_bw),
            )
            session.cluster_qos_enabled = True
            return

        enable_disable_qos_for_cluster(
            enable_flag=True,
            ceph_cluster_nfs_obj=ceph_nfs.cluster,
            cluster_name=session.nfs_service_id,
            qos_type=qos_type,
            operation=qos_operation,
            **params(cluster_bw),
        )
        session.cluster_qos_enabled = True
        enable_disable_qos_for_export(
            enable_flag=True,
            ceph_export_nfs_obj=ceph_nfs.export,
            cluster_name=session.nfs_service_id,
            qos_type=qos_type,
            operation=qos_operation,
            nfs_name=session.nfs_service_id,
            export=EXPORT_NAME,
            **params(export_bw),
        )
        session.qos_enabled = True

    @staticmethod
    def disable(session):
        client = session.client1
        if session.qos_enabled:
            enable_disable_qos_for_export(
                enable_flag=False,
                ceph_export_nfs_obj=Ceph(client).nfs.export,
                cluster_name=session.nfs_service_id,
                nfs_name=session.nfs_service_id,
                export=EXPORT_NAME,
            )
            session.qos_enabled = False
        if session.cluster_qos_enabled:
            enable_disable_qos_for_cluster(
                enable_flag=False,
                ceph_cluster_nfs_obj=Ceph(client).nfs.cluster,
                cluster_name=session.nfs_service_id,
            )
            session.cluster_qos_enabled = False

    @staticmethod
    def wait_ready(client, nfs_service_id, bw_config, qos_type, stage, config=None):
        if qos_type == "PerClient":
            return NfsMultiActiveQos._wait_cluster_ready(
                client, nfs_service_id, bw_config, qos_type, stage, config
            )
        return NfsMultiActiveQos._wait_export_ready(
            client, nfs_service_id, bw_config, qos_type, stage, config
        )

    @staticmethod
    def validate_persisted(
        client, nfs_service_id, bw_config, qos_type, baseline_get, baseline_block, stage
    ):
        if qos_type == "PerClient":
            return NfsMultiActiveQos._validate_cluster_persisted(
                client, nfs_service_id, bw_config, qos_type, baseline_get, stage
            )
        return NfsMultiActiveQos._validate_export_persisted(
            client,
            nfs_service_id,
            bw_config,
            qos_type,
            baseline_get,
            baseline_block,
            stage,
        )

    @staticmethod
    def validate_speeds(client, nfs_mount, export_bw, qos_type, file_tag):
        speed = capture_copy_details(client, nfs_mount, f"tc2_qos_{file_tag}")
        log.info(
            "TC-2 QoS speed (%s): write=%s read=%s type=%s limits=%s",
            file_tag,
            speed.get("write_speed"),
            speed.get("read_speed"),
            qos_type,
            export_bw,
        )
        write_speed = speed.get("write_speed")
        read_speed = speed.get("read_speed")
        max_export_write_bw = export_bw.get("max_export_write_bw")
        max_export_read_bw = export_bw.get("max_export_read_bw")
        max_export_combined_bw = export_bw.get("max_export_combined_bw")
        if max_export_combined_bw:
            max_export_write_bw = max_export_combined_bw
            max_export_read_bw = max_export_combined_bw

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

        if not export_ok:
            raise OperationFailedError(
                f"QoS limits violated ({file_tag}): write={write_speed} "
                f"read={read_speed} limits={export_bw}"
            )
        log.info("TC-2 QoS speed validation passed (%s)", file_tag)

    @staticmethod
    def validate_perclient_speeds(
        clients, nfs_mount, client_bw, qos_type, file_tag, session=None
    ):
        """Validate per-client bandwidth limits on the VIP export mount."""
        for client in clients:
            if session:
                backend_host, backend_port = (
                    session.haproxy.get_client_backend_endpoint(client)
                )
                log.info(
                    "TC-4 QoS via VIP %s on %s (pinned %s:%s, %s)",
                    nfs_mount,
                    client.hostname,
                    backend_host,
                    backend_port,
                    file_tag,
                )
            speed = capture_copy_details(
                client, nfs_mount, f"tc4_qos_{file_tag}_{client.hostname}"
            )
            log.info(
                "TC-4 per-client QoS speed (%s_%s): write=%s read=%s type=%s limits=%s",
                file_tag,
                client.hostname,
                speed.get("write_speed"),
                speed.get("read_speed"),
                qos_type,
                client_bw,
            )
            write_speed = speed.get("write_speed")
            read_speed = speed.get("read_speed")
            max_client_combined_bw = client_bw.get("max_client_combined_bw")
            max_client_write_bw = client_bw.get("max_client_write_bw")
            max_client_read_bw = client_bw.get("max_client_read_bw")
            if max_client_combined_bw:
                max_client_write_bw = max_client_combined_bw
                max_client_read_bw = max_client_combined_bw
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
            if not client_ok:
                raise OperationFailedError(
                    f"Per-client QoS limits violated ({file_tag}_{client.hostname}): "
                    f"write={write_speed} read={read_speed} limits={client_bw}"
                )
            log.info(
                "TC-4 per-client QoS speed validation passed (%s_%s)",
                file_tag,
                client.hostname,
            )

    @staticmethod
    def assert_colocated_client_isolation(session, case_id):
        endpoint1 = session.haproxy.get_client_backend_endpoint(session.client1)
        endpoint2 = session.haproxy.get_client_backend_endpoint(session.client2)
        if endpoint1 == endpoint2:
            raise OperationFailedError(
                f"{case_id} clients are pinned to the same NFS backend instance "
                f"{endpoint1}; expected distinct colocated backends"
            )
        log.info(
            "%s colocated backend isolation: client1=%s client2=%s",
            case_id,
            endpoint1,
            endpoint2,
        )
        return endpoint1, endpoint2

    @staticmethod
    def _read_cluster_get(client, nfs_service_id):
        qos_data = Ceph(client).nfs.cluster.qos.get(
            cluster_id=nfs_service_id,
            format="json",
        )
        return _maybe_json_loads(qos_data)

    @staticmethod
    def _read_export_block(client, nfs_service_id):
        export_data = Ceph(client).nfs.export.get(
            nfs_name=nfs_service_id,
            nfs_export=EXPORT_NAME,
        )
        export_data = _maybe_json_loads(export_data)
        if isinstance(export_data, dict):
            return export_data.get("qos_block") or {}
        return {}

    @staticmethod
    def _read_export_get(client, nfs_service_id):
        return Ceph(client).nfs.export.qos.get(
            nfs_name=nfs_service_id,
            export=EXPORT_NAME,
        )

    @staticmethod
    def _cluster_is_ready(qos_get, bw_config):
        if not isinstance(qos_get, dict):
            return False
        if qos_get.get("enable_bw_control") is not True:
            return False
        limit_key, _ = NfsMultiActiveQos.limit_key(bw_config)
        return bool(limit_key and qos_get.get(limit_key))

    @staticmethod
    def _export_is_ready(qos_get, export_bw):
        if not isinstance(qos_get, dict):
            return False
        if qos_get.get("enable_bw_control") is not True:
            return False
        limit_key, _ = NfsMultiActiveQos.limit_key(export_bw)
        return bool(limit_key and qos_get.get(limit_key))

    @staticmethod
    def _wait_export_ready(
        client, nfs_service_id, export_bw, qos_type, stage, config=None
    ):
        cfg = config or {}
        timeout = int(cfg.get("qos_poll_timeout", QOS_BLOCK_POLL_TIMEOUT))
        interval = int(cfg.get("qos_poll_interval", QOS_BLOCK_POLL_INTERVAL))
        remaining = timeout
        while remaining > 0:
            qos_get = NfsMultiActiveQos._read_export_get(client, nfs_service_id)
            qos_block = NfsMultiActiveQos._read_export_block(client, nfs_service_id)
            if NfsMultiActiveQos._export_is_ready(qos_get, export_bw):
                log.info(
                    "TC-2 export QoS ready at %s: get=%s block=%s (%s)",
                    stage,
                    qos_get,
                    qos_block,
                    qos_type,
                )
                return qos_get, qos_block
            sleep(interval)
            remaining -= interval
        raise OperationFailedError(
            f"Timed out waiting for export QoS at {stage} on {EXPORT_NAME}"
        )

    @staticmethod
    def _wait_cluster_ready(
        client, nfs_service_id, bw_config, qos_type, stage, config=None
    ):
        cfg = config or {}
        timeout = int(cfg.get("qos_poll_timeout", QOS_BLOCK_POLL_TIMEOUT))
        interval = int(cfg.get("qos_poll_interval", QOS_BLOCK_POLL_INTERVAL))
        remaining = timeout
        while remaining > 0:
            qos_get = NfsMultiActiveQos._read_cluster_get(client, nfs_service_id)
            if NfsMultiActiveQos._cluster_is_ready(qos_get, bw_config):
                log.info(
                    "Cluster QoS ready at %s: get=%s (%s)",
                    stage,
                    qos_get,
                    qos_type,
                )
                return qos_get, {}
            sleep(interval)
            remaining -= interval
        raise OperationFailedError(
            f"Timed out waiting for cluster QoS at {stage} on {nfs_service_id}"
        )

    @staticmethod
    def _validate_cluster_persisted(
        client, nfs_service_id, bw_config, qos_type, baseline_get, stage
    ):
        qos_get = NfsMultiActiveQos._read_cluster_get(client, nfs_service_id)
        if not NfsMultiActiveQos._cluster_is_ready(qos_get, bw_config):
            raise OperationFailedError(f"Cluster QoS not enabled at {stage}: {qos_get}")
        limit_key, _ = NfsMultiActiveQos.limit_key(bw_config)
        if baseline_get:
            for key in (
                "enable_bw_control",
                "enable_qos",
                "combined_rw_bw_control",
                "qos_type",
                limit_key,
            ):
                if key and baseline_get.get(key) != qos_get.get(key):
                    raise OperationFailedError(
                        f"Cluster QoS {key!r} changed at {stage}: "
                        f"before={baseline_get.get(key)!r} after={qos_get.get(key)!r}"
                    )
        log.info(
            "Cluster QoS persisted at %s: get=%s (%s)",
            stage,
            qos_get,
            qos_type,
        )

    @staticmethod
    def _validate_export_persisted(
        client,
        nfs_service_id,
        export_bw,
        qos_type,
        baseline_get,
        baseline_block,
        stage,
    ):
        qos_get = NfsMultiActiveQos._read_export_get(client, nfs_service_id)
        qos_block = NfsMultiActiveQos._read_export_block(client, nfs_service_id)
        if not NfsMultiActiveQos._export_is_ready(qos_get, export_bw):
            raise OperationFailedError(f"Export QoS not enabled at {stage}: {qos_get}")
        limit_key, _ = NfsMultiActiveQos.limit_key(export_bw)
        if baseline_get:
            for key in (
                "enable_bw_control",
                "enable_qos",
                "combined_rw_bw_control",
                limit_key,
            ):
                if key and baseline_get.get(key) != qos_get.get(key):
                    raise OperationFailedError(
                        f"Export QoS {key!r} changed at {stage}: "
                        f"before={baseline_get.get(key)!r} after={qos_get.get(key)!r}"
                    )
        if baseline_block and qos_block != baseline_block:
            raise OperationFailedError(
                f"Export qos_block changed at {stage} for {qos_type}:\n"
                f"before={baseline_block}\nafter={qos_block}"
            )
        log.info(
            "TC-2 QoS persisted at %s: get=%s block=%s (%s)",
            stage,
            qos_get,
            qos_block,
            qos_type,
        )
