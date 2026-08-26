"""
Run real database engines on NVMeoF namespaces (filesystem on device + podman).

Config example::

    databases:
      - engine: postgresql
        duration: 60
        scale: 10
      - engine: mysql
      - engine: mariadb
      - engine: mongodb
      - engine: redis
      - engine: cassandra
      # oracle: always skipped (license)
      # mssql: requires accept_eula: true

    subsystems:   # provide at least one NS per DB entry
      - nqn: nqn.2016-06.io.spdk:cnode_db
        bdevs: [{count: 6, size: 50G, ns_create_image: true}]
    initiators:
      - nqn: nqn.2016-06.io.spdk:cnode_db
        node: node9
"""

from ceph.ceph import Ceph
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.db_workloads import (
    UNSUPPORTED,
    ensure_podman,
    podman_rm,
    run_database_workload,
    umount_quiet,
)
from tests.nvmeof.workflows.gateway_entities import (
    configure_gw_entities,
    disconnect_initiators,
)
from tests.nvmeof.workflows.initiator import prepare_io_execution
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

LOG = Log(__name__)


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Configure DB namespaces, mount, run engines, cleanup."""
    config = kwargs["config"]
    databases = config.get("databases") or []
    if not databases:
        LOG.error("config.databases list is required")
        return 1

    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
    custom_config = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)

    nvme_service = None
    clients = []
    results = []
    rc = 1
    try:
        nvme_service = NVMeService(config, ceph_cluster)
        if config.get("install"):
            nvme_service.deploy()
        nvme_service.init_gateways()
        configure_gw_entities(nvme_service, rbd_obj=rbd_obj, cluster=ceph_cluster)

        initiators = config.get("initiators")
        if not initiators:
            raise ValueError("initiators required for DB workloads")

        clients = prepare_io_execution(
            initiators,
            gateways=nvme_service.gateways,
            cluster=ceph_cluster,
            return_clients=True,
        )
        if not clients:
            raise RuntimeError("Failed to prepare NVMe initiators for DB workloads")

        client = clients[0]
        node = client.node
        ensure_podman(node)
        paths = client.list_spdk_drives() or client.list_devices()
        if not paths:
            raise RuntimeError(f"No NVMe devices on {node.hostname}")

        LOG.info(
            "DB workload devices on %s: %s (engines=%s)",
            node.hostname,
            paths,
            [d.get("engine") for d in databases],
        )

        errors = []
        # Assign devices only to runnable engines (skipped engines do not consume NS)
        device_idx = 0
        for db_cfg in databases:
            engine = (db_cfg.get("engine") or "").lower()
            if engine in UNSUPPORTED:
                LOG.warning("Skip %s: %s", engine, UNSUPPORTED[engine])
                results.append(
                    {"engine": engine, "skipped": True, "reason": UNSUPPORTED[engine]}
                )
                continue

            if device_idx >= len(paths):
                msg = (
                    f"Not enough NVMe namespaces for {engine}; "
                    f"need index {device_idx}, have {len(paths)} device(s). "
                    f"Increase subsystem bdevs.count."
                )
                LOG.error(msg)
                errors.append(msg)
                continue

            device = paths[device_idx]
            device_idx += 1
            db_cfg = dict(db_cfg)
            slot = device_idx  # 1-based for naming after increment
            db_cfg.setdefault("mount_point", f"/mnt/nvmeof-db-{engine}-{slot}")
            db_cfg.setdefault("container_name", f"nvmeof-db-{engine}-{slot}")
            if engine in ("mysql", "mariadb") and "port" not in db_cfg:
                db_cfg["port"] = 3306 + slot
            if engine in ("postgresql", "postgres") and "port" not in db_cfg:
                db_cfg["port"] = 5432 + slot

            try:
                results.append(run_database_workload(node, device, db_cfg))
            except Exception as err:
                LOG.error("DB workload %s failed: %s", engine, err)
                errors.append(f"{engine}: {err}")
                podman_rm(
                    node, db_cfg.get("container_name", f"nvmeof-db-{engine}-{slot}")
                )
                umount_quiet(node, db_cfg["mount_point"])

        unused = len(paths) - device_idx
        if unused > 0:
            LOG.info("%s spare NVMe namespace(s) unused after DB mapping", unused)
        LOG.info("DB workload results: %s", results)
        if errors:
            raise RuntimeError(f"Database workload failures: {errors}")
        rc = 0
    except Exception as err:
        LOG.error(err)
        rc = 1
    finally:
        if config.get("cleanup") and clients:
            for client in clients:
                try:
                    client.node.exec_command(
                        sudo=True,
                        cmd=(
                            "podman ps -a --format '{{.Names}}' | "
                            "grep '^nvmeof-db-' | xargs -r podman rm -f || true"
                        ),
                        check_ec=False,
                    )
                except Exception:
                    pass
            if nvme_service is not None:
                for initiator_cfg in config.get("initiators", []):
                    node = get_node_by_id(ceph_cluster, initiator_cfg["node"])
                    disconnect_initiators(nvme_service, node=node)
    return rc
