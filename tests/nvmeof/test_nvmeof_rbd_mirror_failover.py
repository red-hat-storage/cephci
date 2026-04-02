"""
Planned RBD mirror failover / failback with NVMeoF namespace read-write checks.

1. Mirror + NVMe setup (same pattern as test_nvmeof_rbd_mirror).
2. FIO (librbd) on primary for each image, then mirror snapshot + wait so the
   secondary catches up (snapshot mirroring).
3. MD5 each image on primary; failover; MD5 each image on secondary — must match.
4. FIO on secondary (librbd), optional NVMe R/W, then MD5 on secondary.
5. Failback; MD5 each image on primary — must match secondary post-IO MD5s.
6. After failback: remove mirror snapshot schedules for all images, FIO again,
   manual user snapshot + protect on primary, mirror image snapshot + wait,
   verify user snapshot on secondary, clone from that snap on secondary, MD5
   primary snap vs secondary clone.

Requires image_config.secondary_count: 0 so all images start as primary on ceph-rbd1.
"""

import json
import time
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.utils import (
    check_data_integrity,
    get_md5sum_rbd_image,
    getdict,
    random_string,
)
from ceph.rbd.workflows.cleanup import cleanup as rbd_mirror_pool_cleanup
from ceph.rbd.workflows.rbd_mirror import wait_for_status
from ceph.rbd.workflows.snap_clone_operations import snap_exists
from ceph.rbd.workflows.snap_scheduling import remove_snapshot_scheduling
from ceph.utils import get_node_by_id
from tests.nvmeof.test_nvmeof_rbd_mirror import (
    _build_rep_pool_config_for_nvme_mirror,
    _ensure_rbd_pool_cleanup,
    _list_mirror_image_names,
    configure_nvme_namespaces_for_mirrored_images,
    deploy_nvme_service_on_cluster,
    nvme_rbd_mirror_cleanup_requested,
)
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from utility.log import Log
from utility.utils import run_fio

LOG = Log(__name__)


def _check_cli_err(err, operation):
    if err and err.strip() and "100% complete" not in err:
        LOG.warning(f"{operation} stderr: {err}")


def _failover_fio_defaults(ft):
    """Defaults for failover_test FIO / sync tunables."""
    return {
        "pre_size": ft.get("pre_failover_fio_size", "256M"),
        "pre_runtime": int(ft.get("pre_failover_fio_runtime_sec", 60)),
        "sec_size": ft.get("secondary_fio_size", "128M"),
        "sec_runtime": int(ft.get("secondary_fio_runtime_sec", 45)),
        "sync_wait": int(ft.get("mirror_sync_wait_sec", 120)),
        "post_failback_wait": int(ft.get("post_failback_md5_wait_sec", 45)),
        "clone_io_size": ft.get(
            "clone_verify_fio_size", ft.get("pre_failover_fio_size", "256M")
        ),
        "clone_io_runtime": int(
            ft.get(
                "clone_verify_fio_runtime_sec",
                ft.get("pre_failover_fio_runtime_sec", 60),
            )
        ),
        "user_snap_wait": int(
            ft.get("user_snap_mirror_wait_sec", ft.get("mirror_sync_wait_sec", 120))
        ),
    }


def _run_fio_rbd_images(client, pool, images, size, runtime_sec, io_type, test_prefix):
    """Run librbd FIO on each image from the given client node."""
    for img in images:
        LOG.info(
            f"FIO ({test_prefix}) pool={pool} image={img} size={size} runtime={runtime_sec}s"
        )
        run_fio(
            pool_name=pool,
            image_name=img,
            client_node=client,
            size=size,
            run_time=runtime_sec,
            io_type=io_type,
            long_running=True,
            cmd_timeout="notimeout",
            test_name=f"{test_prefix}-{img}",
            num_jobs=1,
            verbose=True,
        )


def _trigger_mirror_snapshots_and_wait(rbd_primary, pool, images, wait_sec):
    """Force snapshot-based sync from primary, then wait for propagation."""
    for img in images:
        spec = f"{pool}/{img}"
        out, err = rbd_primary.mirror.image.snapshot(**{"image-spec": spec})
        _check_cli_err(err, f"mirror image snapshot {spec}")
        LOG.info(f"mirror image snapshot {spec}: {out}")
    LOG.info(f"Waiting {wait_sec}s for mirror sync after snapshot(s)")
    time.sleep(wait_sec)


def _md5_rbd_image(rbd, client, pool, image):
    path = f"/tmp/nvme_mirror_fail_md5_{image}_{random_string(len=6)}"
    digest = get_md5sum_rbd_image(
        image_spec=f"{pool}/{image}",
        file_path=path,
        rbd=rbd,
        client=client,
    )
    if not digest:
        raise RuntimeError(f"Failed to compute md5 for {pool}/{image}")
    return digest


def _md5_all_images(rbd, client, pool, images):
    return {img: _md5_rbd_image(rbd, client, pool, img) for img in images}


def _assert_md5_equal(left, right, phase):
    for img in left:
        if left[img] != right[img]:
            raise RuntimeError(
                f"{phase}: MD5 mismatch for {img}: {left[img]} vs {right[img]}"
            )
    LOG.info(f"{phase}: MD5 verified for {len(left)} image(s)")


def _state_after_image_demote(ceph_version_major):
    """Mirror image state on both clusters after demote (matches CEPH-9471 style)."""
    return "up+stopped" if ceph_version_major < 3 else "up+unknown"


def _planned_failover_images(
    rbd_primary,
    rbd_secondary,
    primary_cname,
    secondary_cname,
    pool,
    images,
    ceph_version_major,
):
    """For each image: demote on primary, wait, promote on secondary, wait for primary role."""
    demoted_state = _state_after_image_demote(ceph_version_major)
    for img in images:
        spec = f"{pool}/{img}"
        LOG.info(f"Failover image {spec}")
        out, err = rbd_primary.mirror.image.demote(**{"image-spec": spec})
        _check_cli_err(err, f"mirror image demote (primary) {spec}")
        LOG.info(out)
        time.sleep(10)
        wait_for_status(
            rbd=rbd_primary,
            cluster_name=primary_cname,
            imagespec=spec,
            state_pattern=demoted_state,
        )
        wait_for_status(
            rbd=rbd_secondary,
            cluster_name=secondary_cname,
            imagespec=spec,
            state_pattern=demoted_state,
        )

        out, err = rbd_secondary.mirror.image.promote(**{"image-spec": spec})
        _check_cli_err(err, f"mirror image promote (secondary) {spec}")
        LOG.info(out)
        time.sleep(10)
        wait_for_status(
            rbd=rbd_secondary,
            cluster_name=secondary_cname,
            imagespec=spec,
            description="local image is primary",
        )


def _planned_failback_images(
    rbd_primary,
    rbd_secondary,
    primary_cname,
    secondary_cname,
    pool,
    images,
    ceph_version_major,
):
    """Reverse: demote each image on secondary, promote on primary."""
    demoted_state = _state_after_image_demote(ceph_version_major)
    for img in images:
        spec = f"{pool}/{img}"
        LOG.info(f"Failback image {spec}")
        out, err = rbd_secondary.mirror.image.demote(**{"image-spec": spec})
        _check_cli_err(err, f"mirror image demote (secondary) {spec}")
        LOG.info(out)
        time.sleep(10)
        wait_for_status(
            rbd=rbd_secondary,
            cluster_name=secondary_cname,
            imagespec=spec,
            state_pattern=demoted_state,
        )
        wait_for_status(
            rbd=rbd_primary,
            cluster_name=primary_cname,
            imagespec=spec,
            state_pattern=demoted_state,
        )

        out, err = rbd_primary.mirror.image.promote(**{"image-spec": spec})
        _check_cli_err(err, f"mirror image promote (primary) {spec}")
        LOG.info(out)
        time.sleep(10)
        wait_for_status(
            rbd=rbd_primary,
            cluster_name=primary_cname,
            imagespec=spec,
            description="local image is primary",
        )


def _nvme_connect_subsystem(initiator, gateway, listener_port):
    """Discover and connect to the subsystem on the given gateway listener."""
    cmd_args = {"transport": "tcp", "traddr": gateway.node.ip_address}
    json_format = {"output-format": "json"}
    discovery_port = {"trsvcid": 8009}
    sub_nqns, _ = initiator.discover(**{**cmd_args, **discovery_port, **json_format})
    listener_port = str(listener_port)
    for nqn in json.loads(sub_nqns)["records"]:
        if nqn["trsvcid"] == listener_port:
            cmd_args["nqn"] = nqn["subnqn"]
            break
    else:
        raise RuntimeError(f"No subsystem found on listener port {listener_port}")
    conn_port = {"trsvcid": listener_port}
    LOG.info(initiator.connect(**{**cmd_args, **conn_port}))


def _remove_mirror_snap_schedules_for_images(rbd_primary, poolname, pool_cfg, images):
    """Remove mirror snapshot schedules added during initial_mirror_config (per-image)."""
    for img in images:
        img_cfg = getdict(pool_cfg).get(img) or {}
        levels = img_cfg.get("snap_schedule_levels") or []
        intervals = img_cfg.get("snap_schedule_intervals") or []
        for level, interval in zip(levels, intervals):
            out, err = remove_snapshot_scheduling(
                rbd_primary,
                pool=poolname,
                image=img,
                level=level,
                interval=interval,
            )
            LOG.info(
                f"Removed mirror snap schedule level={level} interval={interval} "
                f"for {poolname}/{img}: out={out!r} err={err!r}"
            )


def _wait_user_snap_on_secondary(
    rbd_secondary, pool, image, snap_name, timeout_sec, poll_sec=15
):
    """Poll until a user snapshot name appears on the secondary image after mirror sync."""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if snap_exists(rbd=rbd_secondary, pool=pool, image=image, snap_name=snap_name):
            LOG.info(f"Secondary has user snapshot {pool}/{image}@{snap_name}")
            return
        LOG.info(
            f"Waiting for {snap_name} on secondary {pool}/{image} "
            f"({int(deadline - time.time())}s left)"
        )
        time.sleep(poll_sec)
    raise RuntimeError(
        f"Timed out: user snapshot {snap_name} not seen on secondary {pool}/{image}"
    )


def _post_failback_user_snap_clone_verify(
    rbd_primary,
    rbd_secondary,
    client_primary,
    client_secondary,
    poolname,
    pool_cfg,
    images,
    fio_opts,
    clone_names_out,
):
    """
    After failback: drop schedules, write I/O, user snapshot + protect on primary,
    mirror snapshot, confirm user snap on secondary, clone on secondary, MD5(snap)==MD5(clone).
    """
    _remove_mirror_snap_schedules_for_images(rbd_primary, poolname, pool_cfg, images)

    LOG.info("Post-failback: FIO on all images before manual snapshot")
    _run_fio_rbd_images(
        client_primary,
        poolname,
        images,
        fio_opts["clone_io_size"],
        fio_opts["clone_io_runtime"],
        "rw",
        "post-failback-clone-verify",
    )

    snap_by_image = {}
    for img in images:
        snap_name = f"manual_{random_string(len=6)}"
        snap_by_image[img] = snap_name
        spec = f"{poolname}/{img}"
        out, err = rbd_primary.snap.create(
            **{"pool": poolname, "image": img, "snap": snap_name}
        )
        if "100% complete" not in (out or "") + (err or ""):
            raise RuntimeError(
                f"snap create failed {spec}@{snap_name}: out={out!r} err={err!r}"
            )
        LOG.info(f"User snapshot {spec}@{snap_name}: {out}")
        out, err = rbd_primary.snap.protect(**{"snap-spec": f"{spec}@{snap_name}"})
        combined = (out or "") + (err or "")
        # rbd snap protect often succeeds with exit 0 and no stdout/stderr.
        if combined.strip():
            if (
                "100% complete" not in combined
                and "already protected" not in combined.lower()
            ):
                raise RuntimeError(
                    f"snap protect failed {spec}@{snap_name}: out={out!r} err={err!r}"
                )
        else:
            LOG.info(f"snap protect succeeded with no output: {spec}@{snap_name}")
        LOG.info(f"Protected user snapshot {spec}@{snap_name}: {out or '(no stdout)'}")

    _trigger_mirror_snapshots_and_wait(
        rbd_primary,
        poolname,
        images,
        fio_opts["sync_wait"],
    )

    for img in images:
        snap_name = snap_by_image[img]
        _wait_user_snap_on_secondary(
            rbd_secondary,
            poolname,
            img,
            snap_name,
            fio_opts["user_snap_wait"],
        )

    for img in images:
        snap_name = snap_by_image[img]
        clone_name = f"clone_manual_{img}_{random_string(len=5)}"
        src = f"{poolname}/{img}@{snap_name}"
        dest = f"{poolname}/{clone_name}"
        _, err = rbd_secondary.clone(
            **{"source-snap-spec": src, "dest-image-spec": dest}
        )
        if err:
            raise RuntimeError(f"Clone on secondary failed {src} -> {dest}: {err}")
        LOG.info(f"Created secondary clone {dest} from {src}")
        clone_names_out.append(clone_name)

    for img, clone_name in zip(images, clone_names_out):
        snap_name = snap_by_image[img]
        tag = random_string(len=5)
        rc = check_data_integrity(
            first={
                "image_spec": f"{poolname}/{img}@{snap_name}",
                "file_path": f"/tmp/nvme_fail_pri_snap_{img}_{tag}",
                "rbd": rbd_primary,
                "client": client_primary,
            },
            second={
                "image_spec": f"{poolname}/{clone_name}",
                "file_path": f"/tmp/nvme_fail_sec_clone_{img}_{tag}",
                "rbd": rbd_secondary,
                "client": client_secondary,
            },
        )
        if rc != 0:
            raise RuntimeError(
                f"MD5 mismatch: primary snap {poolname}/{img}@{snap_name} vs "
                f"secondary clone {poolname}/{clone_name}"
            )
        LOG.info(
            f"MD5 OK: primary {poolname}/{img}@{snap_name} vs secondary clone {clone_name}"
        )


def _nvme_rw_verify(ceph_cluster, nvme_service, initiator_cfg):
    """Connect initiator to gateway and run short mixed read-write FIO on all NVMe paths."""
    gateway = nvme_service.gateways[0]
    client = get_node_by_id(ceph_cluster, initiator_cfg["node"])
    initiator = NVMeInitiator(client)
    listener_port = initiator_cfg.get("listener_port", 4420)
    try:
        _nvme_connect_subsystem(initiator, gateway, listener_port)
        paths = initiator.list_devices()
        if not paths:
            raise RuntimeError("No NVMe devices visible after connect")
        LOG.info(f"NVMe paths for R/W check: {paths}")
        initiator.start_fio(
            io_size="16M",
            runtime=15,
            execute_blkdiscard=False,
            io_type="rw",
            iodepth=16,
            test_name="nvmeof-mirror-failover-rw",
        )
        LOG.info("NVMe read/write verification completed successfully")
    finally:
        try:
            initiator.disconnect_all()
        except Exception as e:
            LOG.warning(f"Initiator disconnect: {e}")


def run(ceph_cluster: Ceph, **kwargs) -> int:
    config = kwargs["config"]
    mirror_obj = None
    mirror_kwargs = None
    rbd_primary = None
    rbd_secondary = None
    poolname = config.get("poolname", "rbd")
    secondary_clone_names = []

    if config.get("image_config", {}).get("secondary_count", 0):
        LOG.error(
            "failover test requires image_config.secondary_count: 0 "
            "(mixed primary sites are not handled by pool-level promote/demote here)"
        )
        return 1

    try:
        ceph_cluster_dict = kwargs.get("ceph_cluster_dict", {})
        primary_cluster = ceph_cluster_dict.get("ceph-rbd1")
        secondary_cluster = ceph_cluster_dict.get("ceph-rbd2")
        if not primary_cluster or not secondary_cluster:
            raise ValueError(
                "Clusters ceph-rbd1 and/or ceph-rbd2 not found in ceph_cluster_dict"
            )

        mirror_kwargs = deepcopy(kwargs)
        mirror_kwargs["ceph_cluster"] = primary_cluster
        mirror_kwargs["ceph_cluster_dict"] = ceph_cluster_dict
        mirror_kwargs["config"] = deepcopy(config)
        mirror_kwargs["config"]["do_not_run_io"] = True
        mirror_kwargs["config"]["rep_pool_config"] = (
            _build_rep_pool_config_for_nvme_mirror(mirror_kwargs["config"])
        )

        _rh = mirror_kwargs["config"].get("rhbuild", "5")
        ceph_version = int(str(_rh)[0])

        LOG.info("initial_mirror_config: pools, images, mirroring")
        mirror_obj = initial_mirror_config(**mirror_kwargs)
        mirror_obj.pop("output", None)
        client_primary = client_secondary = None
        primary_cluster = secondary_cluster = None
        for val in mirror_obj.values():
            if not val.get("is_secondary", False):
                rbd_primary = val.get("rbd")
                client_primary = val.get("client")
                primary_cluster = val.get("cluster")
            else:
                rbd_secondary = val.get("rbd")
                client_secondary = val.get("client")
                secondary_cluster = val.get("cluster")

        _ensure_rbd_pool_cleanup(rbd_primary, client_primary, ceph_version)
        _ensure_rbd_pool_cleanup(rbd_secondary, client_secondary, ceph_version)

        pool_cfg = mirror_kwargs["config"]["rep_pool_config"][poolname]
        created_images = _list_mirror_image_names(pool_cfg)
        primary_cname = primary_cluster.name
        secondary_cname = secondary_cluster.name

        # NVMe deploy + namespaces (both sites)
        primary_nvme_config = deepcopy(config.get("primary_nvme_config", config))
        primary_nvme_config["rbd_pool"] = poolname
        check_and_set_nvme_cli_image(
            primary_cluster, config=kwargs.get("test_data", {}).get("custom-config")
        )
        primary_nvme_service = deploy_nvme_service_on_cluster(
            primary_cluster, primary_nvme_config, rbd_primary
        )

        secondary_nvme_config = deepcopy(config.get("secondary_nvme_config", config))
        secondary_nvme_config["rbd_pool"] = poolname
        check_and_set_nvme_cli_image(
            secondary_cluster, config=kwargs.get("test_data", {}).get("custom-config")
        )
        secondary_nvme_service = deploy_nvme_service_on_cluster(
            secondary_cluster, secondary_nvme_config, rbd_secondary
        )

        primary_subsystem_config = primary_nvme_config.get("subsystems", [])
        if primary_subsystem_config:
            configure_gw_entities(
                primary_nvme_service, rbd_obj=rbd_primary, cluster=primary_cluster
            )
            for subsys_cfg in primary_subsystem_config:
                configure_nvme_namespaces_for_mirrored_images(
                    primary_nvme_service,
                    rbd_primary,
                    poolname,
                    created_images,
                    subsys_cfg,
                )

        secondary_subsystem_config = secondary_nvme_config.get("subsystems", [])
        if secondary_subsystem_config:
            configure_gw_entities(
                secondary_nvme_service, rbd_obj=rbd_secondary, cluster=secondary_cluster
            )
            for subsys_cfg in secondary_subsystem_config:
                configure_nvme_namespaces_for_mirrored_images(
                    secondary_nvme_service,
                    rbd_secondary,
                    poolname,
                    created_images,
                    subsys_cfg,
                )

        ft = config.get("failover_test", {})
        fio_opts = _failover_fio_defaults(ft)
        sec_init = ft.get("secondary_initiator")
        pri_init = ft.get("primary_initiator")
        subsys_nqn = primary_nvme_config.get("subsystems", [{}])[0].get(
            "nqn"
        ) or primary_nvme_config.get("subsystems", [{}])[0].get("subnqn")
        pre_io = ft.get("pre_failover_io_type", "rw")
        sec_io = ft.get("secondary_io_type", "rw")

        LOG.info("Pre-failover FIO on primary (librbd on client)")
        _run_fio_rbd_images(
            client_primary,
            poolname,
            created_images,
            fio_opts["pre_size"],
            fio_opts["pre_runtime"],
            pre_io,
            "pre-failover-primary",
        )
        _trigger_mirror_snapshots_and_wait(
            rbd_primary,
            poolname,
            created_images,
            fio_opts["sync_wait"],
        )

        LOG.info("MD5 on primary before failover")
        md5_primary_before = _md5_all_images(
            rbd_primary, client_primary, poolname, created_images
        )
        LOG.info(f"Primary MD5 before failover: {md5_primary_before}")

        LOG.info(
            "Planned failover: image demote on primary, image promote on secondary"
        )
        _planned_failover_images(
            rbd_primary,
            rbd_secondary,
            primary_cname,
            secondary_cname,
            poolname,
            created_images,
            ceph_version,
        )

        LOG.info(
            "MD5 on secondary after failover (expect match to primary pre-failover)"
        )
        md5_secondary_post_failover = _md5_all_images(
            rbd_secondary, client_secondary, poolname, created_images
        )
        _assert_md5_equal(
            md5_primary_before,
            md5_secondary_post_failover,
            "Primary vs secondary after failover",
        )

        LOG.info("Pre-failback FIO on secondary (librbd on secondary client)")
        _run_fio_rbd_images(
            client_secondary,
            poolname,
            created_images,
            fio_opts["sec_size"],
            fio_opts["sec_runtime"],
            sec_io,
            "pre-failback-secondary",
        )

        if sec_init and subsys_nqn:
            LOG.info("NVMe read/write on secondary after failover")
            _nvme_rw_verify(secondary_cluster, secondary_nvme_service, sec_init)
        else:
            LOG.warning(
                "failover_test.secondary_initiator not set; skipping NVMe R/W on secondary"
            )

        _trigger_mirror_snapshots_and_wait(
            rbd_secondary,
            poolname,
            created_images,
            fio_opts["sync_wait"],
        )

        LOG.info("MD5 on secondary after I/O (before failback)")
        md5_secondary_after_io = _md5_all_images(
            rbd_secondary, client_secondary, poolname, created_images
        )
        LOG.info(f"Secondary MD5 after I/O: {md5_secondary_after_io}")

        LOG.info(
            "Planned failback: image demote on secondary, image promote on primary"
        )
        _planned_failback_images(
            rbd_primary,
            rbd_secondary,
            primary_cname,
            secondary_cname,
            poolname,
            created_images,
            ceph_version,
        )

        LOG.info(
            f"Waiting {fio_opts['post_failback_wait']}s before final MD5 on primary"
        )
        time.sleep(fio_opts["post_failback_wait"])

        LOG.info("MD5 on primary after failback (expect match to secondary post-I/O)")
        md5_primary_after = _md5_all_images(
            rbd_primary, client_primary, poolname, created_images
        )
        _assert_md5_equal(
            md5_secondary_after_io,
            md5_primary_after,
            "Secondary post-I/O vs primary after failback",
        )

        if pri_init and subsys_nqn:
            LOG.info("NVMe read/write on primary after failback")
            _nvme_rw_verify(primary_cluster, primary_nvme_service, pri_init)
        else:
            LOG.warning(
                "failover_test.primary_initiator not set; skipping NVMe R/W on primary"
            )

        LOG.info(
            "Post-failback: remove schedules, I/O, user snapshot, mirror snapshot, "
            "verify snap on secondary, clone + MD5(snap vs clone)"
        )
        _post_failback_user_snap_clone_verify(
            rbd_primary,
            rbd_secondary,
            client_primary,
            client_secondary,
            poolname,
            mirror_kwargs["config"]["rep_pool_config"][poolname],
            created_images,
            fio_opts,
            secondary_clone_names,
        )

        LOG.info("NVMeoF RBD mirror planned failover/failback test completed")
        return 0

    except Exception as err:
        LOG.error(f"Test failed: {err}")
        import traceback

        LOG.error(traceback.format_exc())
        return 1
    finally:
        if nvme_rbd_mirror_cleanup_requested(config):
            LOG.info(
                "Cleanup: secondary clones, NVMe (subsystems/gateway/pool), mirror pools"
            )
            try:
                if rbd_secondary is not None and secondary_clone_names:
                    for cname in secondary_clone_names:
                        spec = f"{poolname}/{cname}"
                        out, err = rbd_secondary.rm(**{"image-spec": spec})
                        LOG.info(f"Removed clone image {spec}: {out} err={err!r}")
                if "primary_nvme_service" in locals() and rbd_primary is not None:
                    teardown(primary_nvme_service, rbd_primary)
                if "secondary_nvme_service" in locals() and rbd_secondary is not None:
                    teardown(secondary_nvme_service, rbd_secondary)
                if mirror_obj is not None and mirror_kwargs is not None:
                    try:
                        rbd_mirror_pool_cleanup(
                            ["rep_pool_config"],
                            mirror_obj,
                            config=mirror_kwargs["config"],
                        )
                    except Exception as pool_cleanup_err:
                        LOG.warning(
                            f"Rep pool cleanup (post-NVMe) skipped or failed: {pool_cleanup_err}"
                        )
            except Exception as cleanup_err:
                LOG.warning(f"Cleanup error: {cleanup_err}")
