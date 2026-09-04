"""RBD native import-only migration tests using mon_host credentials.

Test cases covered (dispatched via config.operation, same pattern as
test_namespace_mirror_operations.py) -
CEPH-83632846 - Native import with mon_host and inline CephX key
CEPH-83632847 - Native import with mon_host and config:// credential reference
CEPH-83632851 - Source-backed reads from NVMe-oF gateway Client-B without
                local source ceph.conf/keyring (persisted migration metadata)
CEPH-83632848 - Sparse native import-only migration preserves used size and
                zero-filled holes across prepare / execute / commit
CEPH-83632849 - Native import of LUKS/LUKS2 encrypted images with mon_host
                and inline CephX key (encryption preserved across prepare /
                execute / commit)
CEPH-83632850 - Negative validation of mon_host/key native source-spec
                (missing/invalid fields, mutual exclusion, auth/config-key
                failures, insufficient caps, stale-state checks)

Pre-requisites:
- Two Ceph clusters deployed and accessible (ceph-rbd1 and ceph-rbd2).
- Each cluster has a client node with ceph-common and fio installed.
- For CEPH-83632851, destination NVMe-oF gateway is deployed by the test
  (or via suite) on configured gw_nodes.
- Ceph/RHCS 9.2 or later.
"""

import json
import random
from datetime import datetime
from time import sleep

from looseversion import LooseVersion

from ceph.parallel import parallel
from ceph.rbd.io_utils import (
    assert_image_is_sparse,
    assert_used_size_close,
    filter_profiles_by_role,
    get_block_io_profiles,
    get_effective_used_size,
    get_rbd_du_exact,
    get_sparse_io_profiles,
    run_profile_fio,
)
from ceph.rbd.utils import copy_file, create_map_options, exec_cmd, random_string
from ceph.rbd.workflows.cleanup import pool_cleanup
from ceph.rbd.workflows.cluster_operations import (
    get_ceph_major_version,
    validate_cluster_health_and_daemons,
    validate_min_ceph_version,
)
from ceph.rbd.workflows.encryption import create_passphrase_file
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.rbd.workflows.migration import (
    assert_no_source_cluster_config,
    attempt_migration_prepare_import,
    create_source_cephx_client,
    get_source_mon_host,
    prepare_gateway_like_client,
    prepare_native_source_spec_with_config_key,
    prepare_native_source_spec_with_key,
    remove_config_key,
    resolve_gateway_like_client,
    simulate_librbd_consumer_restart,
    store_source_key_in_config_key,
    verify_config_key,
    verify_gateway_like_logs,
    verify_key_not_logged,
    verify_migration_state,
    verify_no_stale_migration_target,
    write_native_source_spec,
)
from ceph.rbd.workflows.rbd import (
    create_single_pool_and_images,
    get_checksum_rbd_image,
    run_rbd_fio,
)
from ceph.utils import get_node_by_id
from cli.rbd.rbd import Rbd
from tests.nvmeof.workflows.gateway_entities import (
    configure_hosts,
    configure_listeners,
    configure_subsystems,
    teardown,
)
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from utility.log import Log
from utility.utils import get_ceph_version_from_cluster

log = Log(__name__)


def _ensure_rbd_nbd(client):
    """Install rbd-nbd if missing (required for encrypted device map)."""
    out = exec_cmd(
        cmd="rpm -qa|grep rbd-nbd", node=client, sudo=True, output=True, check_ec=False
    )
    if not out or out == 1 or "rbd-nbd" not in str(out):
        exec_cmd(cmd="dnf install rbd-nbd -y", node=client, sudo=True)


def _device_path_from_map_out(out):
    """Normalize rbd device map stdout to a device path string."""
    if isinstance(out, tuple):
        out = out[0]
    return str(out).strip()


def map_encrypted_image(rbd, image_spec, encryption_config, device_type="nbd", **kw):
    """Map an encrypted image/snap and return the device path."""
    options = create_map_options(encryption_config)
    map_config = {
        "image-or-snap-spec": image_spec,
        "device-type": device_type,
        "options": options,
    }
    if kw.get("read_only"):
        map_config["read-only"] = True
    out, err = rbd.device.map(**map_config)
    if err:
        raise Exception(f"Encrypted map failed for {image_spec}: {err}")
    return _device_path_from_map_out(out), options


def unmap_encrypted_device(rbd, device, options=None, device_type="nbd"):
    """Unmap an encrypted nbd/krbd device."""
    unmap_config = {
        "image-snap-or-device-spec": device,
        "device-type": device_type,
    }
    if options:
        unmap_config["options"] = options
    out, err = rbd.device.unmap(**unmap_config)
    if err and "not mapped" not in str(err).lower():
        log.error(f"Encrypted unmap failed for {device}: {err}")
        return 1
    return 0


def get_encrypted_file_checksum(
    rbd, client, image_spec, encryption_config, file_path, read_only=False
):
    """Map encrypted image, mount filesystem, return md5 of file_path, then cleanup."""
    _ensure_rbd_nbd(client)
    device, options = map_encrypted_image(
        rbd, image_spec, encryption_config, read_only=read_only
    )
    mount_point = file_path.rsplit("/", 1)[0]
    try:
        mount_cmd = f"mkdir -p {mount_point}; mount"
        if read_only:
            mount_cmd += " -o ro,noload"
        mount_cmd += f" {device} {mount_point}"
        out, err = exec_cmd(cmd=mount_cmd, node=client, all=True)
        if err and out == 1:
            raise Exception(
                f"Mount failed for encrypted {image_spec} on {device}: {err}"
            )
        md5 = exec_cmd(node=client, cmd=f"md5sum {file_path}", output=True).split()[0]
        log.info(f"Plaintext md5 for {file_path} on {image_spec}: {md5}")
        return md5
    finally:
        exec_cmd(cmd=f"umount {mount_point}", node=client, check_ec=False)
        unmap_encrypted_device(rbd, device, options=options)


def verify_wrong_passphrase_rejected(rbd, client, image_spec, encryption_type, workdir):
    """Verify map with wrong passphrase does not expose plaintext data.

    Returns:
        0 if map correctly fails (or mount of raw ciphertext fails), else 1.
    """
    _ensure_rbd_nbd(client)
    wrong_pass = f"{workdir}/wrong_passphrase_{random_string(len=4)}.bin"
    create_passphrase_file(client, wrong_pass)
    options = create_map_options(
        [
            {"encryption-format": encryption_type},
            {"encryption-passphrase-file": wrong_pass},
        ]
    )
    map_config = {
        "image-or-snap-spec": image_spec,
        "device-type": "nbd",
        "options": options,
    }
    try:
        out, err = rbd.device.map(**map_config)
        if err:
            log.info(
                f"Map with wrong passphrase correctly failed for {image_spec}: {err}"
            )
            return 0
        # Unexpected success — ensure ciphertext is not a usable plaintext FS
        device = _device_path_from_map_out(out)
        log.warning(
            f"Map with wrong passphrase unexpectedly succeeded for {image_spec}; "
            "verifying plaintext is not readable"
        )
        try:
            mount_point = f"/tmp/wrong_pass_mnt_{random_string(len=4)}"
            out, err = exec_cmd(
                cmd=f"mkdir -p {mount_point}; mount {device} {mount_point}",
                node=client,
                all=True,
            )
            if err and out == 1:
                log.info(
                    "Wrong-passphrase device is not mountable as plaintext filesystem"
                )
                return 0
            log.error("Wrong-passphrase map exposed a mountable plaintext filesystem")
            exec_cmd(cmd=f"umount {mount_point}", node=client, check_ec=False)
            return 1
        finally:
            unmap_encrypted_device(rbd, device, options=options)
    except Exception as error:
        log.info(
            f"Map with wrong passphrase correctly failed for {image_spec}: {error}"
        )
        return 0
    finally:
        exec_cmd(node=client, cmd=f"rm -f {wrong_pass}", check_ec=False)


def verify_no_passphrase_hides_plaintext(rbd, client, image_spec, expected_file_path):
    """Map without encryption options; plaintext file must not be readable."""
    _ensure_rbd_nbd(client)
    out, err = rbd.device.map(
        **{"image-or-snap-spec": image_spec, "device-type": "nbd"}
    )
    if err:
        log.info(f"Unencrypted map of encrypted image failed as expected: {err}")
        return 0
    device = _device_path_from_map_out(out)
    mount_point = expected_file_path.rsplit("/", 1)[0]
    try:
        out, err = exec_cmd(
            cmd=f"mkdir -p {mount_point}; mount {device} {mount_point}",
            node=client,
            all=True,
        )
        if err and out == 1:
            log.info("Encrypted image without passphrase is not mountable as plaintext")
            return 0
        # If mount somehow works, the known data file must not match
        exists = exec_cmd(
            node=client,
            cmd=f"test -f {expected_file_path}",
            check_ec=False,
        )
        if exists == 0:
            log.error("Plaintext data file readable without encryption passphrase")
            return 1
        log.info("Plaintext data file not present without encryption passphrase")
        return 0
    finally:
        exec_cmd(cmd=f"umount {mount_point}", node=client, check_ec=False)
        unmap_encrypted_device(rbd, device)


def _default_nvme_subsystem(nvme_config):
    """Return the single subsystem dict used by this migration test."""
    subsystems = nvme_config.get("subsystems") or []
    if subsystems:
        return subsystems[0]
    # Backward-compatible flat keys from older suite shape.
    return {
        "nqn": nvme_config.get(
            "subsystem_nqn", "nqn.2016-06.io.spdk:rbd-native-import"
        ),
        "serial": nvme_config.get("serial", "1"),
        "max_ns": nvme_config.get("max_ns", 32),
        "listener_port": nvme_config.get("listener_port", 4420),
        "allow_host": nvme_config.get("allow_host", "*"),
    }


def deploy_basic_nvme_service(destination_cluster, nvme_config, client=None, **kw):
    """Basic single-GW NVMe-oF deploy (same pattern as tentacle NVMe BVT).

    check_and_set_nvme_cli_image → NVMeService.deploy() → init_gateways()
    """
    nvme_config.setdefault("rbd_pool", "rbd")
    nvme_config.setdefault("nvme_metadata_pool", nvme_config["rbd_pool"])
    nvme_config.setdefault("gw_group", "gw_group1")
    nvme_config.setdefault("gw_nodes", ["node6"])
    if "gw_node" not in nvme_config and nvme_config.get("gw_nodes"):
        nvme_config.setdefault("gw_node", nvme_config["gw_nodes"][0])
    nvme_config.setdefault("install", True)
    nvme_config.setdefault("cleanup", ["subsystems", "gateway"])
    if not nvme_config.get("subsystems"):
        nvme_config["subsystems"] = [_default_nvme_subsystem(nvme_config)]

    rbd_pool = nvme_config["rbd_pool"]
    if client:
        for cmd in (
            f"ceph osd pool create {rbd_pool}",
            f"ceph osd pool application enable {rbd_pool} rbd",
            f"rbd pool init {rbd_pool}",
        ):
            exec_cmd(node=client, cmd=cmd, check_ec=False)

    check_and_set_nvme_cli_image(
        destination_cluster,
        config=kw.get("test_data", {}).get("custom-config"),
    )
    nvme_service = NVMeService(nvme_config, destination_cluster)
    if nvme_config.get("install", True):
        log.info(
            "Deploying basic NVMe-oF gateway "
            f"(pool={rbd_pool}, group={nvme_config.get('gw_group')}, "
            f"nodes={nvme_config.get('gw_nodes')})"
        )
        nvme_service.deploy()
    nvme_service.init_gateways()
    if not nvme_service.gateways:
        raise Exception("NVMeService.init_gateways returned no gateways")
    log.info(
        f"NVMe-oF gateway ready on {nvme_service.gateways[0].node.hostname} "
        f"(service={getattr(nvme_service, 'service_name', None)})"
    )
    return nvme_service


def configure_nvme_for_existing_image(nvme_service, destination_cluster, pool, image):
    """Configure subsystem/host and attach an already-prepared RBD image.

    Uses gateway_entities helpers for subsystem/listener/host. Namespace is
    added manually because configure_namespaces always creates new images.
    """
    if not nvme_service.config.get("subsystems"):
        nvme_service.config["subsystems"] = [
            _default_nvme_subsystem(nvme_service.config)
        ]
    sub_cfg = nvme_service.config["subsystems"][0]
    nqn = sub_cfg.get("nqn") or sub_cfg.get("subnqn")
    gateway = nvme_service.gateways[0]

    configure_subsystems(nvme_service, ceph_cluster=destination_cluster)
    ceph_version = get_ceph_version_from_cluster(
        destination_cluster.get_nodes(role="client")[0]
    )
    # Match gateway_entities.configure_gw_entities listener gate.
    if LooseVersion(ceph_version) <= LooseVersion("20.2.1"):
        configure_listeners(nvme_service.gateways, nvme_service.config)
    configure_hosts(gateway, nvme_service.config, ceph_cluster=destination_cluster)
    gateway.namespace.add(
        **{"args": {"subsystem": nqn, "rbd-pool": pool, "rbd-image": image}}
    )
    log.info(
        f"Attached existing image {pool}/{image} to NVMe subsystem {nqn} "
        f"on {gateway.node.hostname}"
    )
    return nqn


def wait_nvme_daemons_running(client, timeout=300):
    """Poll until all nvmeof orch daemons report running."""
    elapsed = 0
    while elapsed < timeout:
        status_out = exec_cmd(
            node=client,
            cmd="ceph orch ps --daemon_type nvmeof --format json",
            output=True,
            check_ec=False,
        )
        try:
            status = json.loads(status_out) if status_out else []
        except Exception:
            status = []
        if status and all(d.get("status_desc") == "running" for d in status):
            log.info("NVMe-oF daemons running")
            return 0
        sleep(10)
        elapsed += 10
    log.error(f"NVMe-oF daemons not running after {timeout}s")
    return 1


def test_native_import_mon_host_inline_key(
    source_client, destination_client, is_ec_pool=False, **kw
):
    """Execute the RBD native import test using mon_host and inline key.

    This test validates that an RBD image can be migrated across two
    independent Ceph clusters using only the source cluster's monitor
    addresses and an inline CephX key — without deploying source
    ceph.conf or keyring files on the destination client.

    Args:
        source_client: Source cluster CephNode client.
        destination_client: Destination cluster CephNode client.
        is_ec_pool: If True, create EC pools instead of replicated pools.
        kw: Test configuration keyword arguments.

    Returns:
        0 on success, 1 on failure.
    """
    config = kw.get("config", {})
    src_pool = config.get("src_pool", "src_pool")
    dst_pool = config.get("dst_pool", "dst_pool")
    src_image = config.get("src_image", "src_image")
    target_image = config.get("target_image", f"target_image_{random_string(len=5)}")
    snap_name = config.get("src_snap", "snap1")
    image_size = config.get("image_size", "10G")
    source_cephx_client = config.get("source_client_name", "client.rbd-migration")
    workdir = config.get("workdir", "/tmp/rbd-native-import-test")
    source_spec_path = config.get(
        "source_spec_path", f"{workdir}/native-inline-key.json"
    )
    target_spec = f"{dst_pool}/{target_image}"
    ceph_version = get_ceph_major_version(config)

    source_rbd = Rbd(source_client)
    destination_rbd = Rbd(destination_client)
    source_entity = None
    source_key = None

    try:
        # --- Setup workdir ---
        exec_cmd(
            node=destination_client, cmd=f"mkdir -p {workdir} && chmod 700 {workdir}"
        )

        # --- Step 1: Validate cluster versions and health ---
        if validate_min_ceph_version(
            config,
            9,
            2,
            ("source", source_client),
            ("destination", destination_client),
        ):
            return 1
        if validate_cluster_health_and_daemons(source_client, "source"):
            return 1
        if validate_cluster_health_and_daemons(destination_client, "destination"):
            return 1

        # --- Step 2: Create source and destination pools ---
        for label, client, rbd, pool in (
            ("source", source_client, source_rbd, src_pool),
            ("destination", destination_client, destination_rbd, dst_pool),
        ):
            rc = create_single_pool_and_images(
                config=config,
                pool=pool,
                pool_config={
                    "pg_num": config.get("pg_num", 32),
                    "pgp_num": config.get("pgp_num", 32),
                },
                client=client,
                cluster="ceph",
                rbd=rbd,
                ceph_version=ceph_version,
                is_ec_pool=False,
                is_secondary=False,
                do_not_create_image=True,
            )
            if rc:
                log.error(f"{label} pool creation failed")
                return 1

        # --- Step 3: Create source image and write data patterns ---
        out, err = source_rbd.create(
            **{"image-spec": f"{src_pool}/{src_image}", "size": image_size}
        )
        if out or err:
            log.error(f"Source image creation failed: {out} {err}")
            return 1

        # Write pattern A at offset 0
        run_rbd_fio(
            client=source_client,
            pool=src_pool,
            image=src_image,
            rw="write",
            offset=config.get("source_pattern_a_offset", "0"),
            size=config.get("source_pattern_size", "1G"),
            pattern=config.get("source_pattern_a", "0xAA"),
            name="source-pattern-a",
        )
        # Write pattern B at offset 6G
        run_rbd_fio(
            client=source_client,
            pool=src_pool,
            image=src_image,
            rw="write",
            offset=config.get("source_pattern_b_offset", "6G"),
            size=config.get("source_pattern_size", "1G"),
            pattern=config.get("source_pattern_b", "0xBB"),
            name="source-pattern-b",
        )
        # Flush/verify source image is readable
        exec_cmd(
            node=source_client,
            cmd=f"rbd export {src_pool}/{src_image} - >/dev/null",
            long_running=True,
            timeout=7200,
        )

        # --- Step 3b: Create source snapshot and baseline checksum ---
        out, err = source_rbd.snap.create(
            **{"snap-spec": f"{src_pool}/{src_image}@{snap_name}"}
        )
        if err and "error" in err.lower():
            log.error(f"Source snapshot creation failed: {out} {err}")
            return 1
        log.info(f"Source snapshot created: {src_pool}/{src_image}@{snap_name}")
        source_rbd.snap.ls(**{"image-spec": f"{src_pool}/{src_image}"})

        baseline_checksum = get_checksum_rbd_image(
            source_client, f"{src_pool}/{src_image}@{snap_name}"
        )
        log.info(f"Source snapshot baseline sha256: {baseline_checksum}")

        # --- Step 4: Create CephX client and build native source spec ---
        source_entity, source_key = create_source_cephx_client(
            client=source_client,
            pool=src_pool,
            client_name=source_cephx_client,
        )
        mon_host = get_source_mon_host(source_client)
        spec = prepare_native_source_spec_with_key(
            client=destination_client,
            spec_path=source_spec_path,
            mon_host=mon_host,
            client_name=source_entity,
            key=source_key,
            pool_name=src_pool,
            image_name=src_image,
            snap_name=snap_name,
        )

        # Verify destination resolves to a different cluster than source
        source_fsid = exec_cmd(node=source_client, cmd="ceph fsid", output=True).strip()
        destination_fsid = exec_cmd(
            node=destination_client, cmd="ceph fsid", output=True
        ).strip()
        if source_fsid == destination_fsid:
            log.error(
                f"Source and destination resolve to the same cluster "
                f"fsid {source_fsid}"
            )
            return 1
        if "cluster_name" in spec:
            log.error("mon_host/key source spec must not include cluster_name")
            return 1
        log.info(
            f"Destination fsid {destination_fsid}, source fsid {source_fsid} "
            f"(confirmed different clusters)"
        )

        # --- Step 5: Prepare import-only migration ---
        destination_rbd.migration.prepare_import(
            source_spec_path=source_spec_path,
            dest_spec=target_spec,
        )

        # Verify target image exists in destination pool
        out, err = destination_rbd.ls(**{"pool-spec": dst_pool})
        if target_image not in out.split():
            log.error(f"Target image {target_spec} not listed in destination pool")
            return 1
        destination_rbd.info(**{"image-or-snap-spec": target_spec})

        if verify_migration_state(
            action="prepare",
            image_spec=target_spec,
            client=destination_client,
        ):
            log.error("Migration prepare state verification failed")
            return 1

        # --- Step 6: Verify reads from prepared image match source ---
        prepared_checksum = get_checksum_rbd_image(destination_client, target_spec)
        if prepared_checksum != baseline_checksum:
            log.error(
                f"Prepared target checksum mismatch: source={baseline_checksum} "
                f"destination={prepared_checksum}"
            )
            return 1
        log.info("Prepared target checksum matches source baseline")

        # --- Step 7: Target-side writes (while prepared) ---
        run_rbd_fio(
            client=destination_client,
            pool=dst_pool,
            image=target_image,
            rw="write",
            offset=config.get("target_write_offset", "2G"),
            size=config.get("target_write_size", "256M"),
            pattern=config.get("target_write_pattern", "0xCC"),
            name="target-post-prepare-write",
        )
        run_rbd_fio(
            client=destination_client,
            pool=dst_pool,
            image=target_image,
            rw="read",
            offset=config.get("target_write_offset", "2G"),
            size=config.get("target_write_size", "256M"),
            pattern=config.get("target_write_pattern", "0xCC"),
            name="target-post-prepare-read-verify",
        )

        # Verify source snapshot is unchanged (immutability check)
        source_checksum_after = get_checksum_rbd_image(
            source_client, f"{src_pool}/{src_image}@{snap_name}"
        )
        if source_checksum_after != baseline_checksum:
            log.error("Source snapshot changed after target-side writes")
            return 1

        # --- Step 8: Execute migration with concurrent I/O ---
        def _execute_migration():
            destination_rbd.migration.action(
                action="execute",
                dest_spec=target_spec,
            )

        def _run_active_io():
            run_rbd_fio(
                client=destination_client,
                pool=dst_pool,
                image=target_image,
                rw="write",
                offset=config.get("active_io_offset", "4G"),
                size=config.get("active_io_size", "1G"),
                pattern=config.get("active_io_pattern", "0xDD"),
                rate=config.get("active_io_rate", "64M"),
                name="target-active-io",
                timeout=7200,
            )

        with parallel(timeout=7200, max_workers=2) as p:
            p.spawn(_run_active_io)
            sleep(config.get("active_io_start_delay", 5))
            p.spawn(_execute_migration)

        if verify_migration_state(
            action="execute",
            image_spec=target_spec,
            client=destination_client,
        ):
            log.error("Migration execute state verification failed")
            return 1

        # --- Step 9: Verify all data regions after execute ---
        verify_regions = [
            (
                "verify-source-backed-a",
                "source_pattern_a_offset",
                "0",
                "source_pattern_size",
                "1G",
                "source_pattern_a",
                "0xAA",
            ),
            (
                "verify-source-backed-b",
                "source_pattern_b_offset",
                "6G",
                "source_pattern_size",
                "1G",
                "source_pattern_b",
                "0xBB",
            ),
            (
                "verify-target-write",
                "target_write_offset",
                "2G",
                "target_write_size",
                "256M",
                "target_write_pattern",
                "0xCC",
            ),
            (
                "verify-active-io",
                "active_io_offset",
                "4G",
                "active_io_size",
                "1G",
                "active_io_pattern",
                "0xDD",
            ),
        ]
        for name, off_key, off_def, sz_key, sz_def, pat_key, pat_def in verify_regions:
            run_rbd_fio(
                client=destination_client,
                pool=dst_pool,
                image=target_image,
                rw="read",
                offset=config.get(off_key, off_def),
                size=config.get(sz_key, sz_def),
                pattern=config.get(pat_key, pat_def),
                name=f"{name}-after-execute",
            )

        # --- Step 10: Commit migration and final verification ---
        destination_rbd.migration.action(
            action="commit",
            dest_spec=target_spec,
        )

        if verify_migration_state(
            action="commit",
            image_spec=target_spec,
            client=destination_client,
        ):
            log.error("Migration commit state verification failed")
            return 1

        # Final data integrity verification for all four regions
        exec_cmd(
            node=destination_client,
            cmd=f"rbd export {target_spec} - >/dev/null",
            long_running=True,
            timeout=7200,
        )
        for name, off_key, off_def, sz_key, sz_def, pat_key, pat_def in verify_regions:
            run_rbd_fio(
                client=destination_client,
                pool=dst_pool,
                image=target_image,
                rw="read",
                offset=config.get(off_key, off_def),
                size=config.get(sz_key, sz_def),
                pattern=config.get(pat_key, pat_def),
                name=f"final-{name}",
            )
        log.info("Reads at all offsets completed successfully for the migrated image")

        # Verify source snapshot immutability throughout the migration lifecycle
        final_source_checksum = get_checksum_rbd_image(
            source_client, f"{src_pool}/{src_image}@{snap_name}"
        )
        if final_source_checksum != baseline_checksum:
            log.error("Source snapshot changed by migration lifecycle")
            return 1

        # Final cluster health checks
        if validate_cluster_health_and_daemons(source_client, "source"):
            return 1
        if validate_cluster_health_and_daemons(destination_client, "destination"):
            return 1

        # --- Step 11: Security check — inline key not leaked ---
        test_start = kw.get("test_start")
        if (
            test_start
            and source_key
            and verify_key_not_logged(
                destination_client, source_key, workdir, test_start
            )
        ):
            return 1

        log.info("RBD native import with mon_host and inline key passed")
        return 0

    except Exception as error:
        log.error(f"RBD native import with mon_host and inline key failed: {error}")
        return 1

    finally:
        log.info("Cleaning up RBD native import test resources")

        # Remove spec file from destination
        exec_cmd(
            node=destination_client,
            cmd=f"rm -f {source_spec_path}",
            check_ec=False,
        )

        # Final key leak check during cleanup
        if source_key:
            verify_key_not_logged(
                destination_client,
                source_key,
                workdir,
                kw.get("test_start", datetime.utcnow().isoformat()),
            )

        # Abort migration if still in progress, then remove target image
        exec_cmd(
            node=destination_client,
            cmd=f"rbd migration abort {target_spec}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        exec_cmd(
            node=destination_client,
            cmd=f"rbd rm {target_spec}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )

        # Remove source snapshot and image
        exec_cmd(
            node=source_client,
            cmd=f"rbd snap rm {src_pool}/{src_image}@{snap_name}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        exec_cmd(
            node=source_client,
            cmd=f"rbd rm {src_pool}/{src_image}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )

        # Remove CephX client from source cluster
        if source_entity:
            exec_cmd(
                node=source_client,
                cmd=f"ceph auth rm {source_entity}",
                check_ec=False,
            )

        # Cleanup pools
        pool_cleanup(
            client=destination_client,
            pools=[dst_pool],
            ceph_version=ceph_version,
        )
        pool_cleanup(
            client=source_client,
            pools=[src_pool],
            ceph_version=ceph_version,
        )

        # Remove work directory
        exec_cmd(
            node=destination_client,
            cmd=f"rm -rf {workdir}",
            check_ec=False,
        )


def test_native_import_mon_host_config_key(
    source_client, destination_client, is_ec_pool=False, **kw
):
    """Execute RBD native import using mon_host and config:// key reference.

    Validates that an RBD image can be migrated across two independent Ceph
    clusters using source mon_host addresses and a CephX key stored in the
    destination MON config-key store (referenced via config://), without
    deploying source ceph.conf or keyring files on the destination client.

    Args:
        source_client: Source cluster CephNode client.
        destination_client: Destination cluster CephNode client.
        is_ec_pool: If True, create EC pools instead of replicated pools.
        kw: Test configuration keyword arguments.

    Returns:
        0 on success, 1 on failure.
    """
    config = kw.get("config", {})
    src_pool = config.get("src_pool", "src_pool")
    dst_pool = config.get("dst_pool", "dst_pool")
    src_image = config.get("src_image", "src_image")
    target_image = config.get("target_image", f"target_image_{random_string(len=5)}")
    snap_name = config.get("src_snap", "snap1")
    image_size = config.get("image_size", "10G")
    source_cephx_client = config.get("source_client_name", "client.rbd-migration")
    config_key_path = config.get("config_key_path", "rbd/native/source_client_key")
    workdir = config.get("workdir", "/tmp/rbd-native-import-config-key-test")
    source_spec_path = config.get(
        "source_spec_path", f"{workdir}/native-config-key.json"
    )
    target_spec = f"{dst_pool}/{target_image}"
    ceph_version = get_ceph_major_version(config)

    source_rbd = Rbd(source_client)
    destination_rbd = Rbd(destination_client)
    source_entity = None
    source_key = None
    config_key_stored = False

    try:
        # --- Setup workdir ---
        for client in (source_client, destination_client):
            exec_cmd(node=client, cmd=f"mkdir -p {workdir} && chmod 700 {workdir}")

        # --- Step 1: Validate cluster versions and health ---
        if validate_min_ceph_version(
            config,
            9,
            2,
            ("source", source_client),
            ("destination", destination_client),
        ):
            return 1
        if validate_cluster_health_and_daemons(source_client, "source"):
            return 1
        if validate_cluster_health_and_daemons(destination_client, "destination"):
            return 1

        # --- Step 2: Create source and destination pools ---
        for label, client, rbd, pool in (
            ("source", source_client, source_rbd, src_pool),
            ("destination", destination_client, destination_rbd, dst_pool),
        ):
            rc = create_single_pool_and_images(
                config=config,
                pool=pool,
                pool_config={
                    "pg_num": config.get("pg_num", 32),
                    "pgp_num": config.get("pgp_num", 32),
                },
                client=client,
                cluster="ceph",
                rbd=rbd,
                ceph_version=ceph_version,
                is_ec_pool=False,
                is_secondary=False,
                do_not_create_image=True,
            )
            if rc:
                log.error(f"{label} pool creation failed")
                return 1

        # --- Step 3: Create source image and write data patterns ---
        out, err = source_rbd.create(
            **{"image-spec": f"{src_pool}/{src_image}", "size": image_size}
        )
        if out or err:
            log.error(f"Source image creation failed: {out} {err}")
            return 1

        run_rbd_fio(
            client=source_client,
            pool=src_pool,
            image=src_image,
            rw="write",
            offset=config.get("source_pattern_a_offset", "0"),
            size=config.get("source_pattern_size", "1G"),
            pattern=config.get("source_pattern_a", "0xAA"),
            name="source-pattern-a",
        )
        run_rbd_fio(
            client=source_client,
            pool=src_pool,
            image=src_image,
            rw="write",
            offset=config.get("source_pattern_b_offset", "6G"),
            size=config.get("source_pattern_size", "1G"),
            pattern=config.get("source_pattern_b", "0xBB"),
            name="source-pattern-b",
        )
        exec_cmd(
            node=source_client,
            cmd=f"rbd export {src_pool}/{src_image} - >/dev/null",
            long_running=True,
            timeout=7200,
        )

        # --- Step 3b: Create source snapshot and baseline checksum ---
        out, err = source_rbd.snap.create(
            **{"snap-spec": f"{src_pool}/{src_image}@{snap_name}"}
        )
        if err and "error" in err.lower():
            log.error(f"Source snapshot creation failed: {out} {err}")
            return 1
        log.info(f"Source snapshot created: {src_pool}/{src_image}@{snap_name}")
        source_rbd.snap.ls(**{"image-spec": f"{src_pool}/{src_image}"})

        baseline_checksum = get_checksum_rbd_image(
            source_client, f"{src_pool}/{src_image}@{snap_name}"
        )
        log.info(f"Source snapshot baseline sha256: {baseline_checksum}")

        # --- Steps 4-5: Create CephX client and fetch key ---
        source_entity, source_key = create_source_cephx_client(
            client=source_client,
            pool=src_pool,
            client_name=source_cephx_client,
        )

        # --- Steps 6-7: Store and verify key in destination config-key ---
        store_source_key_in_config_key(
            client=destination_client,
            config_key_path=config_key_path,
            key=source_key,
            workdir=workdir,
        )
        config_key_stored = True
        if verify_config_key(destination_client, config_key_path, source_key):
            return 1

        # --- Steps 8-10: Build source-spec with config:// reference ---
        mon_host = get_source_mon_host(source_client)
        spec = prepare_native_source_spec_with_config_key(
            client=destination_client,
            spec_path=source_spec_path,
            mon_host=mon_host,
            client_name=source_entity,
            config_key_path=config_key_path,
            pool_name=src_pool,
            image_name=src_image,
            snap_name=snap_name,
        )

        expected_key_ref = f"config://{config_key_path}"
        if spec.get("key") != expected_key_ref:
            log.error(
                f"Source-spec key must be {expected_key_ref}, got {spec.get('key')}"
            )
            return 1
        # Ensure raw key is not embedded in the source-spec file
        spec_contents = exec_cmd(
            node=destination_client,
            cmd=f"cat {source_spec_path}",
            output=True,
        )
        if source_key in spec_contents:
            log.error("Raw source CephX key must not appear in source-spec file")
            return 1
        if "cluster_name" in spec:
            log.error("mon_host/config:// source spec must not include cluster_name")
            return 1

        source_fsid = exec_cmd(node=source_client, cmd="ceph fsid", output=True).strip()
        destination_fsid = exec_cmd(
            node=destination_client, cmd="ceph fsid", output=True
        ).strip()
        if source_fsid == destination_fsid:
            log.error(
                f"Source and destination resolve to the same cluster "
                f"fsid {source_fsid}"
            )
            return 1
        log.info(
            f"Destination fsid {destination_fsid}, source fsid {source_fsid} "
            f"(confirmed different clusters)"
        )

        # --- Step 11-12: Prepare import-only migration ---
        destination_rbd.migration.prepare_import(
            source_spec_path=source_spec_path,
            dest_spec=target_spec,
        )

        out, err = destination_rbd.ls(**{"pool-spec": dst_pool})
        if target_image not in out.split():
            log.error(f"Target image {target_spec} not listed in destination pool")
            return 1
        destination_rbd.info(**{"image-or-snap-spec": target_spec})

        if verify_migration_state(
            action="prepare",
            image_spec=target_spec,
            client=destination_client,
        ):
            log.error("Migration prepare state verification failed")
            return 1

        # --- Steps 13-14: Immediate reads match source snapshot ---
        prepared_checksum = get_checksum_rbd_image(destination_client, target_spec)
        if prepared_checksum != baseline_checksum:
            log.error(
                f"Prepared target checksum mismatch: source={baseline_checksum} "
                f"destination={prepared_checksum}"
            )
            return 1
        log.info(
            "Prepared target checksum matches source baseline "
            "(config:// key resolved from destination MON config-key store)"
        )

        # --- Steps 15-16: Target-side writes; source snap unchanged ---
        run_rbd_fio(
            client=destination_client,
            pool=dst_pool,
            image=target_image,
            rw="write",
            offset=config.get("target_write_offset", "2G"),
            size=config.get("target_write_size", "256M"),
            pattern=config.get("target_write_pattern", "0xCC"),
            name="target-post-prepare-write",
        )
        run_rbd_fio(
            client=destination_client,
            pool=dst_pool,
            image=target_image,
            rw="read",
            offset=config.get("target_write_offset", "2G"),
            size=config.get("target_write_size", "256M"),
            pattern=config.get("target_write_pattern", "0xCC"),
            name="target-post-prepare-read-verify",
        )

        source_checksum_after = get_checksum_rbd_image(
            source_client, f"{src_pool}/{src_image}@{snap_name}"
        )
        if source_checksum_after != baseline_checksum:
            log.error("Source snapshot changed after target-side writes")
            return 1

        # --- Step 17: Execute migration with concurrent I/O ---
        def _execute_migration():
            destination_rbd.migration.action(
                action="execute",
                dest_spec=target_spec,
            )

        def _run_active_io():
            run_rbd_fio(
                client=destination_client,
                pool=dst_pool,
                image=target_image,
                rw="write",
                offset=config.get("active_io_offset", "4G"),
                size=config.get("active_io_size", "1G"),
                pattern=config.get("active_io_pattern", "0xDD"),
                rate=config.get("active_io_rate", "64M"),
                name="target-active-io",
                timeout=7200,
            )

        with parallel(timeout=7200, max_workers=2) as p:
            p.spawn(_run_active_io)
            sleep(config.get("active_io_start_delay", 5))
            p.spawn(_execute_migration)

        if verify_migration_state(
            action="execute",
            image_spec=target_spec,
            client=destination_client,
        ):
            log.error("Migration execute state verification failed")
            return 1

        verify_regions = [
            (
                "verify-source-backed-a",
                "source_pattern_a_offset",
                "0",
                "source_pattern_size",
                "1G",
                "source_pattern_a",
                "0xAA",
            ),
            (
                "verify-source-backed-b",
                "source_pattern_b_offset",
                "6G",
                "source_pattern_size",
                "1G",
                "source_pattern_b",
                "0xBB",
            ),
            (
                "verify-target-write",
                "target_write_offset",
                "2G",
                "target_write_size",
                "256M",
                "target_write_pattern",
                "0xCC",
            ),
            (
                "verify-active-io",
                "active_io_offset",
                "4G",
                "active_io_size",
                "1G",
                "active_io_pattern",
                "0xDD",
            ),
        ]
        for name, off_key, off_def, sz_key, sz_def, pat_key, pat_def in verify_regions:
            run_rbd_fio(
                client=destination_client,
                pool=dst_pool,
                image=target_image,
                rw="read",
                offset=config.get(off_key, off_def),
                size=config.get(sz_key, sz_def),
                pattern=config.get(pat_key, pat_def),
                name=f"{name}-after-execute",
            )

        # --- Step 18: Commit migration ---
        destination_rbd.migration.action(
            action="commit",
            dest_spec=target_spec,
        )

        if verify_migration_state(
            action="commit",
            image_spec=target_spec,
            client=destination_client,
        ):
            log.error("Migration commit state verification failed")
            return 1

        # --- Steps 19-20: Reopen target, final verify, health ---
        exec_cmd(
            node=destination_client,
            cmd=f"rbd export {target_spec} - >/dev/null",
            long_running=True,
            timeout=7200,
        )
        for name, off_key, off_def, sz_key, sz_def, pat_key, pat_def in verify_regions:
            run_rbd_fio(
                client=destination_client,
                pool=dst_pool,
                image=target_image,
                rw="read",
                offset=config.get(off_key, off_def),
                size=config.get(sz_key, sz_def),
                pattern=config.get(pat_key, pat_def),
                name=f"final-{name}",
            )

        final_source_checksum = get_checksum_rbd_image(
            source_client, f"{src_pool}/{src_image}@{snap_name}"
        )
        if final_source_checksum != baseline_checksum:
            log.error("Source snapshot changed by migration lifecycle")
            return 1

        if validate_cluster_health_and_daemons(source_client, "source"):
            return 1
        if validate_cluster_health_and_daemons(destination_client, "destination"):
            return 1

        test_start = kw.get("test_start")
        if (
            test_start
            and source_key
            and verify_key_not_logged(
                destination_client, source_key, workdir, test_start
            )
        ):
            return 1

        log.info(
            "RBD native import with mon_host and config:// credential reference passed"
        )
        return 0

    except Exception as error:
        log.error(f"RBD native import with mon_host and config:// key failed: {error}")
        return 1

    finally:
        log.info("Cleaning up RBD native import config-key test resources")

        exec_cmd(
            node=destination_client,
            cmd=f"rm -f {source_spec_path}",
            check_ec=False,
        )

        if source_key:
            verify_key_not_logged(
                destination_client,
                source_key,
                workdir,
                kw.get("test_start", datetime.utcnow().isoformat()),
            )

        exec_cmd(
            node=destination_client,
            cmd=f"rbd migration abort {target_spec}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        exec_cmd(
            node=destination_client,
            cmd=f"rbd rm {target_spec}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )

        exec_cmd(
            node=source_client,
            cmd=f"rbd snap rm {src_pool}/{src_image}@{snap_name}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        exec_cmd(
            node=source_client,
            cmd=f"rbd rm {src_pool}/{src_image}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )

        if config_key_stored:
            remove_config_key(destination_client, config_key_path)

        if source_entity:
            exec_cmd(
                node=source_client,
                cmd=f"ceph auth rm {source_entity}",
                check_ec=False,
            )

        pool_cleanup(
            client=destination_client,
            pools=[dst_pool],
            ceph_version=ceph_version,
        )
        pool_cleanup(
            client=source_client,
            pools=[src_pool],
            ceph_version=ceph_version,
        )

        exec_cmd(
            node=destination_client,
            cmd=f"rm -rf {workdir}",
            check_ec=False,
        )
        exec_cmd(
            node=source_client,
            cmd=f"rm -rf {workdir}",
            check_ec=False,
        )


def test_native_import_gateway_like_client(
    source_client, client_a, client_b, is_ec_pool=False, **kw
):
    """Execute CEPH-83632851 NVMe-oF gateway Client-B native import validation.

    Deploys an NVMe-oF gateway on the destination cluster, uses the gateway
    node as Client-B (destination-only config), prepares import-only migration
    on Client-A, exposes the target via NVMe namespace, validates source-backed
    reads, restarts the NVMe-oF daemon, and completes execute/commit.

    Args:
        source_client: Source cluster CephNode client.
        client_a: Destination Client-A used for prepare/execute/commit.
        client_b: Destination NVMe-oF gateway node (Client-B).
        is_ec_pool: If True, create EC pools instead of replicated pools.
        kw: Test configuration keyword arguments. Expects optional
            ``destination_cluster`` and ``nvme_config``.

    Returns:
        0 on success, 1 on failure.
    """
    config = kw.get("config", {})
    src_pool = config.get("src_pool", "src_pool")
    dst_pool = config.get("dst_pool", "dst_pool")
    src_image = config.get("src_image", "src_image")
    target_image = config.get("target_image", f"target_image_{random_string(len=5)}")
    snap_name = config.get("src_snap", "snap1")
    image_size = config.get("image_size", "10G")
    source_cephx_client = config.get("source_client_name", "client.rbd-migration")
    workdir = config.get("workdir", "/tmp/rbd-native-import-gateway-like-test")
    source_spec_path = config.get(
        "source_spec_path", f"{workdir}/native-gateway-like.json"
    )
    target_spec = f"{dst_pool}/{target_image}"
    ceph_version = get_ceph_major_version(config)
    destination_cluster = kw.get("destination_cluster")
    nvme_config = dict(kw.get("nvme_config") or config.get("nvme_config") or {})
    # Basic single-GW defaults (aligned with tentacle NVMe BVT shape).
    nvme_config.setdefault("gw_nodes", ["node6"])
    nvme_config.setdefault("gw_group", "gw_group1")
    nvme_config.setdefault("rbd_pool", "rbd")
    nvme_config.setdefault("nvme_metadata_pool", "rbd")
    nvme_config.setdefault("install", True)
    nvme_config.setdefault("cleanup", ["subsystems", "gateway"])
    if not nvme_config.get("subsystems"):
        nvme_config["subsystems"] = [_default_nvme_subsystem(nvme_config)]

    source_rbd = Rbd(source_client)
    client_a_rbd = Rbd(client_a)
    client_b_rbd = Rbd(client_b)
    source_entity = None
    source_key = None
    mon_host = None
    source_fsid = None
    nvmegwcli = None
    nvme_service = None
    nvme_deployed = False

    try:
        # --- Setup workdirs ---
        for node in (source_client, client_a, client_b):
            exec_cmd(node=node, cmd=f"mkdir -p {workdir} && chmod 700 {workdir}")

        # --- Step 1: Validate cluster versions and health ---
        if validate_min_ceph_version(
            config,
            9,
            2,
            ("source", source_client),
            ("destination-client-a", client_a),
        ):
            return 1
        if validate_cluster_health_and_daemons(source_client, "source"):
            return 1
        if validate_cluster_health_and_daemons(client_a, "destination"):
            return 1

        # --- Step 2: Create source and destination pools ---
        for label, client, rbd, pool in (
            ("source", source_client, source_rbd, src_pool),
            ("destination", client_a, client_a_rbd, dst_pool),
        ):
            rc = create_single_pool_and_images(
                config=config,
                pool=pool,
                pool_config={
                    "pg_num": config.get("pg_num", 32),
                    "pgp_num": config.get("pgp_num", 32),
                },
                client=client,
                cluster="ceph",
                rbd=rbd,
                ceph_version=ceph_version,
                is_ec_pool=False,
                is_secondary=False,
                do_not_create_image=True,
            )
            if rc:
                log.error(f"{label} pool creation failed")
                return 1

        # --- Step 3: Create source image, write multi-bs patterns, snapshot ---
        out, err = source_rbd.create(
            **{"image-spec": f"{src_pool}/{src_image}", "size": image_size}
        )
        if out or err:
            log.error(f"Source image creation failed: {out} {err}")
            return 1

        io_profiles = get_block_io_profiles(config)
        source_profiles = filter_profiles_by_role(io_profiles, "source")
        target_profiles = filter_profiles_by_role(io_profiles, "target")
        log.info(
            "Using block IO profiles: source=%s target=%s",
            [f"{p['name']}({p['bs']})" for p in source_profiles],
            [f"{p['name']}({p['bs']})" for p in target_profiles],
        )
        for profile in source_profiles:
            if run_profile_fio(
                source_client, src_pool, src_image, profile, "write", "src-write-"
            ):
                log.error(f"Source write failed for profile {profile['name']}")
                return 1

        exec_cmd(
            node=source_client,
            cmd=f"rbd export {src_pool}/{src_image} - >/dev/null",
            long_running=True,
            timeout=7200,
        )

        out, err = source_rbd.snap.create(
            **{"snap-spec": f"{src_pool}/{src_image}@{snap_name}"}
        )
        if err and "error" in err.lower():
            log.error(f"Source snapshot creation failed: {out} {err}")
            return 1
        log.info(f"Source snapshot created: {src_pool}/{src_image}@{snap_name}")

        # --- Step 4: Baseline checksum ---
        baseline_checksum = get_checksum_rbd_image(
            source_client, f"{src_pool}/{src_image}@{snap_name}"
        )
        log.info(f"Source snapshot baseline sha256: {baseline_checksum}")

        # --- Steps 5-6: Source CephX client + mon_host ---
        source_entity, source_key = create_source_cephx_client(
            client=source_client,
            pool=src_pool,
            client_name=source_cephx_client,
        )
        mon_host = get_source_mon_host(source_client)
        source_fsid = exec_cmd(node=source_client, cmd="ceph fsid", output=True).strip()
        destination_fsid = exec_cmd(node=client_a, cmd="ceph fsid", output=True).strip()
        if source_fsid == destination_fsid:
            log.error(
                f"Source and destination resolve to the same cluster "
                f"fsid {source_fsid}"
            )
            return 1
        log.info(
            f"Destination fsid {destination_fsid}, source fsid {source_fsid} "
            f"(confirmed different clusters)"
        )

        # --- Steps 7-8: Deploy NVMe-oF gateway; Client-B is gateway node ---
        if not destination_cluster:
            log.error("destination_cluster is required for NVMe-oF gateway deploy")
            return 1

        log.info(
            f"Deploying basic NVMe-oF gateway "
            f"(nodes={nvme_config.get('gw_nodes')}, "
            f"rbd_pool={nvme_config.get('rbd_pool')}, "
            f"group={nvme_config.get('gw_group')}); "
            f"namespace will use migration target {dst_pool}/{target_image}"
        )
        nvme_service = deploy_basic_nvme_service(
            destination_cluster,
            nvme_config,
            client=client_a,
            test_data=kw.get("test_data", {}),
        )
        nvmegwcli = nvme_service.gateways[0]
        client_b = nvmegwcli.node
        nvme_deployed = True
        client_b_rbd = Rbd(client_b)
        exec_cmd(node=client_b, cmd=f"mkdir -p {workdir} && chmod 700 {workdir}")

        log.info(
            f"Client-A for prepare/execute/commit: {client_a.hostname}; "
            f"Client-B NVMe-oF gateway node: {client_b.hostname}"
        )
        # Ensure gateway node can run rbd/fio with destination-only conf
        if prepare_gateway_like_client(client_a, client_b):
            return 1
        if assert_no_source_cluster_config(
            client_b, source_fsid=source_fsid, source_mon_host=mon_host
        ):
            return 1

        # --- Steps 9-11: Source-spec + import-only prepare on Client-A ---
        spec = prepare_native_source_spec_with_key(
            client=client_a,
            spec_path=source_spec_path,
            mon_host=mon_host,
            client_name=source_entity,
            key=source_key,
            pool_name=src_pool,
            image_name=src_image,
            snap_name=snap_name,
        )
        if "cluster_name" in spec:
            log.error("mon_host/key source spec must not include cluster_name")
            return 1

        # Source-spec must never be copied to Client-B / gateway
        exec_cmd(
            node=client_b,
            cmd=f"rm -f {source_spec_path}",
            check_ec=False,
        )

        client_a_rbd.migration.prepare_import(
            source_spec_path=source_spec_path,
            dest_spec=target_spec,
        )

        out, err = client_a_rbd.ls(**{"pool-spec": dst_pool})
        if target_image not in out.split():
            log.error(f"Target image {target_spec} not listed in destination pool")
            return 1
        client_a_rbd.info(**{"image-or-snap-spec": target_spec})

        if verify_migration_state(
            action="prepare",
            image_spec=target_spec,
            client=client_a,
        ):
            log.error("Migration prepare state verification failed")
            return 1

        # Expose prepared target via NVMe-oF (subsystem/host + existing image)
        configure_nvme_for_existing_image(
            nvme_service,
            destination_cluster,
            dst_pool,
            target_image,
        )

        # --- Steps 12-15: Client-B/gateway source-backed reads without source config ---
        if assert_no_source_cluster_config(
            client_b, source_fsid=source_fsid, source_mon_host=mon_host
        ):
            return 1

        client_b_rbd.info(**{"image-or-snap-spec": target_spec})
        prepared_checksum = get_checksum_rbd_image(client_b, target_spec)
        if prepared_checksum != baseline_checksum:
            log.error(
                f"Client-B prepared target checksum mismatch: "
                f"source={baseline_checksum} client_b={prepared_checksum}"
            )
            return 1
        log.info(
            "Client-B/NVMe-oF gateway prepared target checksum matches source "
            "baseline (source-backed reads via persisted migration metadata)"
        )

        for profile in source_profiles:
            if run_profile_fio(
                client_b,
                dst_pool,
                target_image,
                profile,
                "read",
                "client-b-read-",
            ):
                log.error(
                    f"Client-B source-backed read failed for profile "
                    f"{profile['name']} (bs={profile['bs']})"
                )
                return 1

        # --- Steps 16-18: Restart NVMe-oF gateway daemon and re-read ---
        log.info(f"Redeploying NVMe-oF service {nvme_service.service_name}")
        nvme_service.redeploy(wait_sec=30)
        if wait_nvme_daemons_running(client_a):
            log.error("NVMe-oF gateway daemon restart failed")
            return 1
        # Also cycle any local librbd mapping on the gateway node
        simulate_librbd_consumer_restart(client_b, target_spec)

        client_b_rbd.info(**{"image-or-snap-spec": target_spec})
        post_restart_checksum = get_checksum_rbd_image(client_b, target_spec)
        if post_restart_checksum != baseline_checksum:
            log.error(
                f"Client-B post-gateway-restart checksum mismatch: "
                f"source={baseline_checksum} client_b={post_restart_checksum}"
            )
            return 1
        log.info(
            "Client-B post-NVMe-oF-restart reads match source baseline "
            "(unmigrated extents fetched via persisted metadata)"
        )
        for profile in source_profiles:
            if run_profile_fio(
                client_b,
                dst_pool,
                target_image,
                profile,
                "read",
                "client-b-post-restart-",
            ):
                log.error(
                    f"Client-B post-restart read failed for profile "
                    f"{profile['name']} (bs={profile['bs']})"
                )
                return 1

        # Drop NVMe subsystem so gateway releases exclusive lock before
        # Client-B librbd writes / migration execute (else large-bs FIO can hang).
        sub_cfg = _default_nvme_subsystem(nvme_config)
        nqn = sub_cfg.get("nqn") or sub_cfg.get("subnqn")
        try:
            nvme_service.gateways[0].subsystem.delete(
                **{"args": {"subsystem": nqn, "force": True}}
            )
            log.info(f"Released NVMe subsystem {nqn} (exclusive lock)")
        except Exception as error:
            log.info(f"NVMe subsystem release (best-effort): {error}")
        for _ in range(12):
            status_txt = (
                exec_cmd(
                    node=client_b,
                    cmd=f"rbd status {target_spec} --format json",
                    output=True,
                    check_ec=False,
                )
                or ""
            )
            try:
                watchers = json.loads(status_txt).get("watchers") or []
            except Exception:
                watchers = [] if "none" in status_txt.lower() else ["unknown"]
            if not watchers:
                break
            sleep(5)

        # --- Step 19: Multi-bs writes from Client-B ---
        for profile in target_profiles:
            if run_profile_fio(
                client_b,
                dst_pool,
                target_image,
                profile,
                "write",
                "client-b-tgt-write-",
            ):
                log.error(
                    f"Client-B target write failed for profile "
                    f"{profile['name']} (bs={profile['bs']})"
                )
                return 1
            if run_profile_fio(
                client_b,
                dst_pool,
                target_image,
                profile,
                "read",
                "client-b-tgt-verify-",
            ):
                log.error(
                    f"Client-B target write verify failed for profile "
                    f"{profile['name']} (bs={profile['bs']})"
                )
                return 1
        source_checksum_after = get_checksum_rbd_image(
            source_client, f"{src_pool}/{src_image}@{snap_name}"
        )
        if source_checksum_after != baseline_checksum:
            log.error("Source snapshot changed after Client-B target-side writes")
            return 1

        # --- Step 20: Execute + commit from Client-A (destination context) ---
        client_a_rbd.migration.action(action="execute", dest_spec=target_spec)
        if verify_migration_state(
            action="execute",
            image_spec=target_spec,
            client=client_a,
        ):
            log.error("Migration execute state verification failed")
            return 1

        client_a_rbd.migration.action(action="commit", dest_spec=target_spec)
        if verify_migration_state(
            action="commit",
            image_spec=target_spec,
            client=client_a,
        ):
            log.error("Migration commit state verification failed")
            return 1

        # --- Steps 21-22: Reopen on Client-B as standalone; final verify ---
        client_b_rbd.info(**{"image-or-snap-spec": target_spec})
        exec_cmd(
            node=client_b,
            cmd=f"rbd export {target_spec} - >/dev/null",
            long_running=True,
            timeout=7200,
        )
        for profile in source_profiles + target_profiles:
            if run_profile_fio(
                client_b,
                dst_pool,
                target_image,
                profile,
                "read",
                "final-",
            ):
                log.error(
                    f"Final verify failed for profile {profile['name']} "
                    f"(bs={profile['bs']})"
                )
                return 1

        final_source_checksum = get_checksum_rbd_image(
            source_client, f"{src_pool}/{src_image}@{snap_name}"
        )
        if final_source_checksum != baseline_checksum:
            log.error("Source snapshot changed by migration lifecycle")
            return 1

        # --- Step 23: Cluster health ---
        if validate_cluster_health_and_daemons(source_client, "source"):
            return 1
        if validate_cluster_health_and_daemons(client_a, "destination"):
            return 1

        # --- Step 24: Client-B log / key exposure checks ---
        test_start = kw.get("test_start")
        if test_start and source_key:
            if verify_gateway_like_logs(client_b, source_key, workdir, test_start):
                return 1
            if verify_key_not_logged(client_a, source_key, workdir, test_start):
                return 1

        log.info(
            "RBD native import gateway-like Client-B source-backed read test passed"
        )
        return 0

    except Exception as error:
        log.error(f"RBD native import gateway-like Client-B test failed: {error}")
        return 1

    finally:
        log.info(
            "Cleaning up RBD native import NVMe-oF gateway Client-B test resources"
        )

        if nvme_deployed and nvme_service:
            try:
                # Reuse gateway_entities.teardown (subsystem + orch remove).
                nvme_service.config.setdefault("cleanup", ["subsystems", "gateway"])
                if not nvme_service.config.get("subsystems"):
                    nvme_service.config["subsystems"] = [
                        _default_nvme_subsystem(nvme_config)
                    ]
                teardown(nvme_service, client_a_rbd)
            except Exception as error:
                log.info(f"NVMe teardown (best-effort): {error}")

        exec_cmd(
            node=client_b,
            cmd=f"rbd device unmap -t nbd {target_spec}",
            check_ec=False,
        )
        exec_cmd(
            node=client_a,
            cmd=f"rm -f {source_spec_path}",
            check_ec=False,
        )

        if source_key:
            verify_key_not_logged(
                client_a,
                source_key,
                workdir,
                kw.get("test_start", datetime.utcnow().isoformat()),
            )
            verify_key_not_logged(
                client_b,
                source_key,
                workdir,
                kw.get("test_start", datetime.utcnow().isoformat()),
            )

        exec_cmd(
            node=client_a,
            cmd=f"rbd migration abort {target_spec}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        exec_cmd(
            node=client_a,
            cmd=f"rbd rm {target_spec}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        exec_cmd(
            node=source_client,
            cmd=f"rbd snap rm {src_pool}/{src_image}@{snap_name}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        exec_cmd(
            node=source_client,
            cmd=f"rbd rm {src_pool}/{src_image}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )

        if source_entity:
            exec_cmd(
                node=source_client,
                cmd=f"ceph auth rm {source_entity}",
                check_ec=False,
            )

        pool_cleanup(
            client=client_a,
            pools=[dst_pool],
            ceph_version=ceph_version,
        )
        pool_cleanup(
            client=source_client,
            pools=[src_pool],
            ceph_version=ceph_version,
        )

        for node in (source_client, client_a, client_b):
            exec_cmd(node=node, cmd=f"rm -rf {workdir}", check_ec=False)


def test_native_import_sparse_image(
    source_client, destination_client, is_ec_pool=False, **kw
):
    """Execute CEPH-83632848 sparse native import-only migration validation.

    Creates a large sparse source image with data only at beginning/middle/end,
    verifies sparseness via ``rbd du --exact``, import-only migrates with
    mon_host + inline key, and confirms destination used size stays close to
    source without inflating to full provisioned size. Hole regions must read
    as zeroes throughout.

    Args:
        source_client: Source cluster CephNode client.
        destination_client: Destination cluster CephNode client.
        is_ec_pool: Unused; retained for dispatcher compatibility.
        kw: Test configuration keyword arguments.

    Returns:
        0 on success, 1 on failure.
    """
    config = kw.get("config", {})
    src_pool = config.get("src_pool", "src_pool")
    dst_pool = config.get("dst_pool", "dst_pool")
    src_image = config.get("src_image", "sparse_img")
    target_image = config.get("target_image", f"sparse_target_{random_string(len=5)}")
    snap_name = config.get("src_snap", "snap1")
    image_size = config.get("image_size", "10G")
    source_cephx_client = config.get("source_client_name", "client.rbd-migration")
    workdir = config.get("workdir", "/tmp/rbd-native-import-sparse-test")
    source_spec_path = config.get("source_spec_path", f"{workdir}/native-sparse.json")
    max_used_ratio = float(config.get("max_used_ratio", 0.5))
    max_inflation_ratio = float(config.get("max_inflation_ratio", 1.5))
    target_spec = f"{dst_pool}/{target_image}"
    ceph_version = get_ceph_major_version(config)

    source_rbd = Rbd(source_client)
    destination_rbd = Rbd(destination_client)
    source_entity = None
    source_key = None

    try:
        for client in (source_client, destination_client):
            exec_cmd(node=client, cmd=f"mkdir -p {workdir} && chmod 700 {workdir}")

        # --- Step 1: Version and health ---
        if validate_min_ceph_version(
            config,
            9,
            2,
            ("source", source_client),
            ("destination", destination_client),
        ):
            return 1
        if validate_cluster_health_and_daemons(source_client, "source"):
            return 1
        if validate_cluster_health_and_daemons(destination_client, "destination"):
            return 1

        # --- Step 2: Pools ---
        for label, client, rbd, pool in (
            ("source", source_client, source_rbd, src_pool),
            ("destination", destination_client, destination_rbd, dst_pool),
        ):
            rc = create_single_pool_and_images(
                config=config,
                pool=pool,
                pool_config={
                    "pg_num": config.get("pg_num", 32),
                    "pgp_num": config.get("pgp_num", 32),
                },
                client=client,
                cluster="ceph",
                rbd=rbd,
                ceph_version=ceph_version,
                is_ec_pool=False,
                is_secondary=False,
                do_not_create_image=True,
            )
            if rc:
                log.error(f"{label} pool creation failed")
                return 1

        # --- Steps 3-4: Large sparse image with partial writes ---
        out, err = source_rbd.create(
            **{"image-spec": f"{src_pool}/{src_image}", "size": image_size}
        )
        if out or err:
            log.error(f"Source sparse image creation failed: {out} {err}")
            return 1

        sparse_profiles = get_sparse_io_profiles(config)
        written_profiles = filter_profiles_by_role(sparse_profiles, "written")
        hole_profiles = filter_profiles_by_role(sparse_profiles, "hole")
        log.info(
            "Sparse IO profiles: written=%s holes=%s",
            [f"{p['name']}@{p['offset']}" for p in written_profiles],
            [f"{p['name']}@{p['offset']}" for p in hole_profiles],
        )

        for profile in written_profiles:
            if run_profile_fio(
                source_client,
                src_pool,
                src_image,
                profile,
                "write",
                "sparse-write-",
            ):
                log.error(f"Sparse write failed for {profile['name']}")
                return 1

        # --- Step 5: Source sparseness via rbd du --exact ---
        source_usage = get_rbd_du_exact(source_client, f"{src_pool}/{src_image}")
        if assert_image_is_sparse(source_usage, max_used_ratio=max_used_ratio):
            return 1

        # --- Step 6: Snapshot ---
        out, err = source_rbd.snap.create(
            **{"snap-spec": f"{src_pool}/{src_image}@{snap_name}"}
        )
        if err and "error" in err.lower():
            log.error(f"Source snapshot creation failed: {out} {err}")
            return 1
        log.info(f"Source snapshot created: {src_pool}/{src_image}@{snap_name}")

        snap_usage = get_rbd_du_exact(
            source_client, f"{src_pool}/{src_image}@{snap_name}"
        )
        if assert_image_is_sparse(snap_usage, max_used_ratio=max_used_ratio):
            return 1

        # --- Step 7: Baseline written + zero-filled holes ---
        for profile in written_profiles:
            if run_profile_fio(
                source_client,
                src_pool,
                src_image,
                profile,
                "read",
                "sparse-src-verify-",
            ):
                log.error(f"Source written-region verify failed for {profile['name']}")
                return 1
        for profile in hole_profiles:
            if run_profile_fio(
                source_client,
                src_pool,
                src_image,
                profile,
                "read",
                "sparse-src-hole-",
            ):
                log.error(f"Source hole (zero) verify failed for {profile['name']}")
                return 1

        # --- Steps 8-9: CephX + mon_host native source-spec ---
        source_entity, source_key = create_source_cephx_client(
            client=source_client,
            pool=src_pool,
            client_name=source_cephx_client,
        )
        mon_host = get_source_mon_host(source_client)
        spec = prepare_native_source_spec_with_key(
            client=destination_client,
            spec_path=source_spec_path,
            mon_host=mon_host,
            client_name=source_entity,
            key=source_key,
            pool_name=src_pool,
            image_name=src_image,
            snap_name=snap_name,
        )
        if "cluster_name" in spec:
            log.error("mon_host/key source spec must not include cluster_name")
            return 1

        source_fsid = exec_cmd(node=source_client, cmd="ceph fsid", output=True).strip()
        destination_fsid = exec_cmd(
            node=destination_client, cmd="ceph fsid", output=True
        ).strip()
        if source_fsid == destination_fsid:
            log.error(
                f"Source and destination resolve to the same cluster "
                f"fsid {source_fsid}"
            )
            return 1

        # --- Steps 10-11: Prepare import-only; du must stay sparse ---
        destination_rbd.migration.prepare_import(
            source_spec_path=source_spec_path,
            dest_spec=target_spec,
        )
        out, err = destination_rbd.ls(**{"pool-spec": dst_pool})
        if target_image not in out.split():
            log.error(f"Target image {target_spec} not listed in destination pool")
            return 1
        if verify_migration_state(
            action="prepare",
            image_spec=target_spec,
            client=destination_client,
        ):
            log.error("Migration prepare state verification failed")
            return 1

        prepared_usage = get_rbd_du_exact(destination_client, target_spec)
        if assert_image_is_sparse(prepared_usage, max_used_ratio=max_used_ratio):
            log.error("Destination allocated full size immediately after prepare")
            return 1

        # --- Step 12: Read written + hole regions from prepared target ---
        for profile in written_profiles:
            if run_profile_fio(
                destination_client,
                dst_pool,
                target_image,
                profile,
                "read",
                "sparse-prep-written-",
            ):
                log.error(
                    f"Prepared target written-region verify failed for "
                    f"{profile['name']}"
                )
                return 1
        for profile in hole_profiles:
            if run_profile_fio(
                destination_client,
                dst_pool,
                target_image,
                profile,
                "read",
                "sparse-prep-hole-",
            ):
                log.error(
                    f"Prepared target hole (zero) verify failed for {profile['name']}"
                )
                return 1

        # --- Steps 13-14: Execute; must not inflate to full provisioned size ---
        # On some builds ``rbd du --exact`` still reports used=0 while the
        # image is in executed (pre-commit) migration state. Prefer rbd diff
        # fallback, and only enforce anti-inflation here; require close-to-
        # source used size after commit (standalone image).
        destination_rbd.migration.action(action="execute", dest_spec=target_spec)
        if verify_migration_state(
            action="execute",
            image_spec=target_spec,
            client=destination_client,
        ):
            log.error("Migration execute state verification failed")
            return 1

        executed_usage = get_effective_used_size(destination_client, target_spec)
        if assert_used_size_close(
            snap_usage,
            executed_usage,
            max_inflation_ratio=max_inflation_ratio,
            min_ratio=0,
        ):
            return 1
        if assert_image_is_sparse(executed_usage, max_used_ratio=max_used_ratio):
            return 1

        # --- Steps 15-16: Commit; sparseness preserved and used ~ source ---
        destination_rbd.migration.action(action="commit", dest_spec=target_spec)
        if verify_migration_state(
            action="commit",
            image_spec=target_spec,
            client=destination_client,
        ):
            log.error("Migration commit state verification failed")
            return 1

        committed_usage = get_effective_used_size(destination_client, target_spec)
        if assert_used_size_close(
            snap_usage, committed_usage, max_inflation_ratio=max_inflation_ratio
        ):
            return 1
        if assert_image_is_sparse(committed_usage, max_used_ratio=max_used_ratio):
            return 1

        # --- Steps 17-18: Final data integrity ---
        for profile in written_profiles:
            if run_profile_fio(
                destination_client,
                dst_pool,
                target_image,
                profile,
                "read",
                "sparse-final-written-",
            ):
                log.error(f"Final written-region verify failed for {profile['name']}")
                return 1
        for profile in hole_profiles:
            if run_profile_fio(
                destination_client,
                dst_pool,
                target_image,
                profile,
                "read",
                "sparse-final-hole-",
            ):
                log.error(f"Final hole (zero) verify failed for {profile['name']}")
                return 1

        # --- Step 19: Health ---
        if validate_cluster_health_and_daemons(source_client, "source"):
            return 1
        if validate_cluster_health_and_daemons(destination_client, "destination"):
            return 1

        test_start = kw.get("test_start")
        if (
            test_start
            and source_key
            and verify_key_not_logged(
                destination_client, source_key, workdir, test_start
            )
        ):
            return 1

        log.info("RBD sparse native import-only migration test passed")
        return 0

    except Exception as error:
        log.error(f"RBD sparse native import test failed: {error}")
        return 1

    finally:
        log.info("Cleaning up RBD sparse native import test resources")

        exec_cmd(
            node=destination_client,
            cmd=f"rm -f {source_spec_path}",
            check_ec=False,
        )
        if source_key:
            verify_key_not_logged(
                destination_client,
                source_key,
                workdir,
                kw.get("test_start", datetime.utcnow().isoformat()),
            )

        exec_cmd(
            node=destination_client,
            cmd=f"rbd migration abort {target_spec}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        exec_cmd(
            node=destination_client,
            cmd=f"rbd rm {target_spec}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        exec_cmd(
            node=source_client,
            cmd=f"rbd snap rm {src_pool}/{src_image}@{snap_name}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        exec_cmd(
            node=source_client,
            cmd=f"rbd rm {src_pool}/{src_image}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        if source_entity:
            exec_cmd(
                node=source_client,
                cmd=f"ceph auth rm {source_entity}",
                check_ec=False,
            )

        pool_cleanup(
            client=destination_client,
            pools=[dst_pool],
            ceph_version=ceph_version,
        )
        pool_cleanup(
            client=source_client,
            pools=[src_pool],
            ceph_version=ceph_version,
        )
        for client in (source_client, destination_client):
            exec_cmd(node=client, cmd=f"rm -rf {workdir}", check_ec=False)


def test_native_import_encrypted_image(
    source_client, destination_client, is_ec_pool=False, **kw
):
    """Execute CEPH-83632849 encrypted native import-only migration validation.

    Formats a source RBD image with LUKS1/LUKS2, writes plaintext through the
    encryption layer, verifies wrong credentials hide plaintext, then
    import-only migrates the encrypted snapshot to a second cluster using
    mon_host + inline CephX key. Destination target is opened with the same
    passphrase before execute, written again after prepare, then execute /
    commit are verified for data integrity and encryption protection.

    Args:
        source_client: Source cluster CephNode client.
        destination_client: Destination cluster CephNode client.
        is_ec_pool: Unused; retained for dispatcher compatibility.
        kw: Test configuration keyword arguments.

    Returns:
        0 on success, 1 on failure.
    """
    config = kw.get("config", {})
    src_pool = config.get("src_pool", "src_pool")
    dst_pool = config.get("dst_pool", "dst_pool")
    src_image = config.get("src_image", "encrypted_img")
    target_image = config.get(
        "target_image", f"encrypted_target_{random_string(len=5)}"
    )
    snap_name = config.get("src_snap", "snap1")
    image_size = config.get("image_size", "2G")
    source_cephx_client = config.get("source_client_name", "client.rbd-migration")
    workdir = config.get("workdir", "/tmp/rbd-native-import-encrypted-test")
    source_spec_path = config.get(
        "source_spec_path", f"{workdir}/native-encrypted.json"
    )
    encryption_types = config.get("encryption_type", ["luks1", "luks2"])
    if isinstance(encryption_types, str):
        encryption_types = [encryption_types]
    encryption_type = random.choice(encryption_types)
    fio_size = config.get("fio", {}).get("size", config.get("fio_size", "100M"))
    target_spec = f"{dst_pool}/{target_image}"
    src_spec = f"{src_pool}/{src_image}"
    src_snap_spec = f"{src_spec}@{snap_name}"
    ceph_version = get_ceph_major_version(config)

    source_rbd = Rbd(source_client)
    destination_rbd = Rbd(destination_client)
    source_entity = None
    source_key = None
    passphrase = None
    dest_passphrase = None
    data_file = f"/mnt/mnt_{random_string(len=3)}/encrypted_data"
    post_prepare_file = f"/mnt/mnt_{random_string(len=3)}/post_prepare_data"

    try:
        for client in (source_client, destination_client):
            exec_cmd(node=client, cmd=f"mkdir -p {workdir} && chmod 700 {workdir}")
            _ensure_rbd_nbd(client)

        # --- Step 1: Version and health ---
        if validate_min_ceph_version(
            config,
            9,
            2,
            ("source", source_client),
            ("destination", destination_client),
        ):
            return 1
        if validate_cluster_health_and_daemons(source_client, "source"):
            return 1
        if validate_cluster_health_and_daemons(destination_client, "destination"):
            return 1

        # --- Step 2: Create source and destination pools ---
        for label, client, rbd, pool in (
            ("source", source_client, source_rbd, src_pool),
            ("destination", destination_client, destination_rbd, dst_pool),
        ):
            rc = create_single_pool_and_images(
                config=config,
                pool=pool,
                pool_config={
                    "pg_num": config.get("pg_num", 32),
                    "pgp_num": config.get("pgp_num", 32),
                },
                client=client,
                cluster="ceph",
                rbd=rbd,
                ceph_version=ceph_version,
                is_ec_pool=False,
                is_secondary=False,
                do_not_create_image=True,
            )
            if rc:
                log.error(f"{label} pool creation failed")
                return 1

        # --- Step 3: Create source image ---
        out, err = source_rbd.create(**{"image-spec": src_spec, "size": image_size})
        if out or err:
            log.error(f"Source image creation failed: {out} {err}")
            return 1
        log.info(f"Created source image {src_spec}")

        # --- Step 4: Format with LUKS/LUKS2 and resize for header ---
        log.info(f"Formatting {src_spec} with encryption type {encryption_type}")
        passphrase = (
            f"{workdir}/{encryption_type}_passphrase_{random_string(len=3)}.bin"
        )
        create_passphrase_file(source_client, passphrase)
        out, err = source_rbd.encryption_format(
            **{
                "image-spec": src_spec,
                "format": encryption_type,
                "passphrase-file": passphrase,
            }
        )
        if err:
            log.error(
                f"Encryption format {encryption_type} failed on {src_spec}: {err}"
            )
            return 1
        log.info(f"Successfully formatted {src_spec} with {encryption_type}")

        # Compensate LUKS header overhead so usable capacity matches image_size
        resize_out = source_rbd.resize(
            **{
                "image-spec": src_spec,
                "size": image_size,
                "encryption_config": [{"encryption-passphrase-file": passphrase}],
            }
        )
        log.info(f"Post-encryption resize output: {resize_out}")

        encryption_config = [
            {"encryption-format": encryption_type},
            {"encryption-passphrase-file": passphrase},
        ]

        # --- Steps 5-6: Open with passphrase and write known data ---
        io_config = {
            "rbd_obj": source_rbd,
            "client": source_client,
            "size": image_size,
            "do_not_create_image": True,
            "config": {
                "file_size": fio_size,
                "file_path": [data_file],
                "get_time_taken": True,
                "image_spec": [src_spec],
                "operations": {
                    "fs": "ext4",
                    "io": True,
                    "mount": True,
                    "device_map": True,
                },
                "cmd_timeout": 2400,
                "io_type": "write",
                "encryption_config": encryption_config,
            },
        }
        out, err = krbd_io_handler(**io_config)
        if out:
            log.error(f"Encrypted write I/O failed on {src_spec}: {err}")
            return 1
        log.info(f"Encrypted write I/O succeeded on {src_spec}")

        # --- Step 7: Read back with correct passphrase and baseline checksum ---
        baseline_checksum = get_encrypted_file_checksum(
            source_rbd,
            source_client,
            src_spec,
            encryption_config,
            data_file,
        )
        if not baseline_checksum:
            log.error("Failed to generate source plaintext baseline checksum")
            return 1
        log.info(f"Source encrypted image baseline md5: {baseline_checksum}")

        # --- Step 8: Wrong / missing passphrase must not expose plaintext ---
        if verify_wrong_passphrase_rejected(
            source_rbd, source_client, src_spec, encryption_type, workdir
        ):
            return 1
        if verify_no_passphrase_hides_plaintext(
            source_rbd, source_client, src_spec, data_file
        ):
            return 1

        # --- Step 9: Create source snapshot ---
        out, err = source_rbd.snap.create(**{"snap-spec": src_snap_spec})
        if err and "error" in err.lower():
            log.error(f"Source snapshot creation failed: {out} {err}")
            return 1
        log.info(f"Source snapshot created: {src_snap_spec}")

        snap_checksum = get_encrypted_file_checksum(
            source_rbd,
            source_client,
            src_snap_spec,
            encryption_config,
            data_file,
            read_only=True,
        )
        if snap_checksum != baseline_checksum:
            log.error(
                f"Snapshot plaintext checksum mismatch: "
                f"image={baseline_checksum} snap={snap_checksum}"
            )
            return 1
        log.info("Source snapshot plaintext checksum matches image baseline")

        # --- Steps 10-12: CephX client, mon_host, native source-spec ---
        source_entity, source_key = create_source_cephx_client(
            client=source_client,
            pool=src_pool,
            client_name=source_cephx_client,
        )
        mon_host = get_source_mon_host(source_client)
        spec = prepare_native_source_spec_with_key(
            client=destination_client,
            spec_path=source_spec_path,
            mon_host=mon_host,
            client_name=source_entity,
            key=source_key,
            pool_name=src_pool,
            image_name=src_image,
            snap_name=snap_name,
        )

        source_fsid = exec_cmd(node=source_client, cmd="ceph fsid", output=True).strip()
        destination_fsid = exec_cmd(
            node=destination_client, cmd="ceph fsid", output=True
        ).strip()
        if source_fsid == destination_fsid:
            log.error(
                f"Source and destination resolve to the same cluster fsid {source_fsid}"
            )
            return 1
        if "cluster_name" in spec:
            log.error("mon_host/key source spec must not include cluster_name")
            return 1
        log.info(
            f"Destination fsid {destination_fsid}, source fsid {source_fsid} "
            f"(confirmed different clusters)"
        )

        # Copy passphrase to destination for encrypted target I/O
        dest_passphrase = f"{workdir}/{encryption_type}_passphrase_dest.bin"
        copy_file(passphrase, source_client, destination_client, dest_passphrase)
        dest_encryption_config = [
            {"encryption-format": encryption_type},
            {"encryption-passphrase-file": dest_passphrase},
        ]

        # --- Step 13: Prepare import-only migration ---
        destination_rbd.migration.prepare_import(
            source_spec_path=source_spec_path,
            dest_spec=target_spec,
        )
        out, err = destination_rbd.ls(**{"pool-spec": dst_pool})
        if target_image not in out.split():
            log.error(f"Target image {target_spec} not listed in destination pool")
            return 1
        destination_rbd.info(**{"image-or-snap-spec": target_spec})

        if verify_migration_state(
            action="prepare",
            image_spec=target_spec,
            client=destination_client,
        ):
            log.error("Migration prepare state verification failed")
            return 1
        log.info(f"Migration prepare succeeded for encrypted target {target_spec}")

        # --- Steps 14-16: Read prepared target with passphrase; deny wrong key ---
        prepared_checksum = get_encrypted_file_checksum(
            destination_rbd,
            destination_client,
            target_spec,
            dest_encryption_config,
            data_file,
            read_only=True,
        )
        if prepared_checksum != baseline_checksum:
            log.error(
                f"Prepared target plaintext checksum mismatch: "
                f"source={baseline_checksum} destination={prepared_checksum}"
            )
            return 1
        log.info("Prepared encrypted target plaintext checksum matches source baseline")

        if verify_wrong_passphrase_rejected(
            destination_rbd,
            destination_client,
            target_spec,
            encryption_type,
            workdir,
        ):
            return 1
        if verify_no_passphrase_hides_plaintext(
            destination_rbd, destination_client, target_spec, data_file
        ):
            return 1

        # --- Step 17: New encrypted writes on destination after prepare ---
        post_io_config = {
            "rbd_obj": destination_rbd,
            "client": destination_client,
            "size": image_size,
            "do_not_create_image": True,
            "config": {
                "file_size": config.get("target_write_size", "50M"),
                "file_path": [post_prepare_file],
                "get_time_taken": True,
                "image_spec": [target_spec],
                "operations": {
                    "fs": "ext4",
                    "io": True,
                    "mount": True,
                    "device_map": True,
                },
                "cmd_timeout": 2400,
                "io_type": "write",
                "skip_mkfs": True,
                "encryption_config": dest_encryption_config,
            },
        }
        out, err = krbd_io_handler(**post_io_config)
        if out:
            log.error(f"Post-prepare encrypted write failed on {target_spec}: {err}")
            return 1
        log.info(f"Post-prepare encrypted write succeeded on {target_spec}")

        post_prepare_checksum = get_encrypted_file_checksum(
            destination_rbd,
            destination_client,
            target_spec,
            dest_encryption_config,
            post_prepare_file,
        )
        if not post_prepare_checksum:
            log.error("Failed to checksum post-prepare encrypted write")
            return 1

        # Source snapshot must remain immutable
        source_snap_after = get_encrypted_file_checksum(
            source_rbd,
            source_client,
            src_snap_spec,
            encryption_config,
            data_file,
            read_only=True,
        )
        if source_snap_after != baseline_checksum:
            log.error("Source encrypted snapshot changed after target-side writes")
            return 1

        # --- Step 18: Execute migration ---
        destination_rbd.migration.action(
            action="execute",
            dest_spec=target_spec,
        )
        if verify_migration_state(
            action="execute",
            image_spec=target_spec,
            client=destination_client,
        ):
            log.error("Migration execute state verification failed")
            return 1
        log.info("Migration execute completed for encrypted target")

        # --- Step 19: Commit migration ---
        destination_rbd.migration.action(
            action="commit",
            dest_spec=target_spec,
        )
        if verify_migration_state(
            action="commit",
            image_spec=target_spec,
            client=destination_client,
        ):
            log.error("Migration commit state verification failed")
            return 1
        log.info("Migration commit completed; target is standalone")

        # --- Step 20: Final encrypted reopen / checksum / health ---
        final_baseline = get_encrypted_file_checksum(
            destination_rbd,
            destination_client,
            target_spec,
            dest_encryption_config,
            data_file,
        )
        if final_baseline != baseline_checksum:
            log.error(
                f"Final source-backed plaintext mismatch: "
                f"expected={baseline_checksum} actual={final_baseline}"
            )
            return 1

        final_post = get_encrypted_file_checksum(
            destination_rbd,
            destination_client,
            target_spec,
            dest_encryption_config,
            post_prepare_file,
        )
        if final_post != post_prepare_checksum:
            log.error(
                f"Final post-prepare plaintext mismatch: "
                f"expected={post_prepare_checksum} actual={final_post}"
            )
            return 1

        if verify_wrong_passphrase_rejected(
            destination_rbd,
            destination_client,
            target_spec,
            encryption_type,
            workdir,
        ):
            return 1

        if validate_cluster_health_and_daemons(source_client, "source"):
            return 1
        if validate_cluster_health_and_daemons(destination_client, "destination"):
            return 1

        test_start = kw.get("test_start")
        if (
            test_start
            and source_key
            and verify_key_not_logged(
                destination_client, source_key, workdir, test_start
            )
        ):
            return 1

        log.info(
            f"CEPH-83632849 native import of {encryption_type}-encrypted image passed"
        )
        return 0

    except Exception as error:
        log.error(f"CEPH-83632849 encrypted native import failed: {error}")
        return 1

    finally:
        log.info("Cleaning up encrypted native import test resources")

        exec_cmd(
            node=destination_client,
            cmd=f"rm -f {source_spec_path}",
            check_ec=False,
        )
        if source_key:
            verify_key_not_logged(
                destination_client,
                source_key,
                workdir,
                kw.get("test_start", datetime.utcnow().isoformat()),
            )

        exec_cmd(
            node=destination_client,
            cmd=f"rbd migration abort {target_spec}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        exec_cmd(
            node=destination_client,
            cmd=f"rbd rm {target_spec}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        exec_cmd(
            node=source_client,
            cmd=f"rbd snap rm {src_snap_spec}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        exec_cmd(
            node=source_client,
            cmd=f"rbd rm {src_spec}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        if source_entity:
            exec_cmd(
                node=source_client,
                cmd=f"ceph auth rm {source_entity}",
                check_ec=False,
            )
        pool_cleanup(
            client=destination_client,
            pools=[dst_pool],
            ceph_version=ceph_version,
        )
        pool_cleanup(
            client=source_client,
            pools=[src_pool],
            ceph_version=ceph_version,
        )
        for client in (source_client, destination_client):
            exec_cmd(node=client, cmd=f"rm -rf {workdir}", check_ec=False)
            for path in (data_file, post_prepare_file):
                mount_point = path.rsplit("/", 1)[0]
                exec_cmd(node=client, cmd=f"umount {mount_point}", check_ec=False)
                exec_cmd(node=client, cmd=f"rm -rf {mount_point}", check_ec=False)


def _match_any_error(output, expected_substrings):
    """Return True if *output* contains any of the expected error substrings."""
    text = str(output).lower()
    return any(substr.lower() in text for substr in expected_substrings)


def _run_negative_prepare_case(
    destination_client,
    spec_path,
    dest_pool,
    target_image,
    case_name,
    spec,
    expected_errors,
    timeout=420,
):
    """Write *spec*, run prepare, assert failure + expected error + no stale target.

    Args:
        destination_client: Destination CephNode client.
        spec_path: Path to write the source-spec JSON on the destination node.
        dest_pool: Destination pool name.
        target_image: Target image name for the migration.
        case_name: Human-readable name for this negative case (used in logs).
        spec: Source-spec dict to write.
        expected_errors: List of error substrings; at least one must appear.
        timeout: Command timeout in seconds passed to
            ``attempt_migration_prepare_import``.  For cases that rely on
            Ceph's own connection timeout (e.g. unreachable mon_host ~300 s),
            this must be set **longer** than Ceph's internal timeout so the
            external wrapper does not preempt it.

    Returns:
        0 on success (prepare failed as expected), 1 on unexpected outcome.
    """
    target_spec = f"{dest_pool}/{target_image}"
    log.info(f"Negative case: {case_name} (timeout={timeout}s)")
    write_native_source_spec(destination_client, spec_path, spec)
    failed, output = attempt_migration_prepare_import(
        destination_client, spec_path, target_spec, timeout=timeout
    )
    if not failed:
        log.error(f"{case_name}: prepare unexpectedly succeeded")
        # Clean up unexpected target so later cases remain isolated
        exec_cmd(
            node=destination_client,
            cmd=f"rbd migration abort {target_spec}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        exec_cmd(
            node=destination_client,
            cmd=f"rbd rm {target_spec}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        return 1

    if not _match_any_error(output, expected_errors):
        log.error(
            f"{case_name}: prepare failed but error did not match expected "
            f"substrings {expected_errors}; got: {output}"
        )
        return 1

    if verify_no_stale_migration_target(destination_client, dest_pool, target_image):
        log.error(f"{case_name}: stale target/migration state remained after failure")
        return 1

    log.info(f"{case_name}: failed as expected with matching error")
    return 0


def test_native_import_mon_host_key_negative(
    source_client, destination_client, is_ec_pool=False, **kw
):
    """CEPH-83632850: negative validation for mon_host/key native import.

    Validates that invalid or mutually exclusive native source-spec fields
    cause ``rbd migration prepare --import-only`` to fail cleanly with no
    stale destination migration state, while the source image/snap remain
    unchanged.

    Args:
        source_client: Source cluster CephNode client.
        destination_client: Destination cluster CephNode client.
        is_ec_pool: Unused; kept for dispatcher compatibility.
        kw: Test configuration keyword arguments.

    Returns:
        0 on success, 1 on failure.
    """
    config = kw.get("config", {})
    src_pool = config.get("src_pool", "src_pool")
    dst_pool = config.get("dst_pool", "dst_pool")
    src_image = config.get("src_image", "src_image")
    target_image = config.get("target_image", f"neg_target_{random_string(len=5)}")
    snap_name = config.get("src_snap", "snap1")
    image_size = config.get("image_size", "1G")
    source_cephx_client = config.get("source_client_name", "client.rbd-migration")
    workdir = config.get("workdir", "/tmp/rbd-native-import-neg")
    source_spec_path = config.get("source_spec_path", f"{workdir}/native-negative.json")
    valid_target = config.get(
        "valid_target_image", f"valid_target_{random_string(len=5)}"
    )
    wrong_config_key_path = config.get(
        "wrong_config_key_path", "rbd/native/wrong_source_key"
    )
    missing_config_key_path = config.get(
        "missing_config_key_path", "rbd/native/missing_key"
    )
    ceph_version = get_ceph_major_version(config)

    source_rbd = Rbd(source_client)
    destination_rbd = Rbd(destination_client)
    source_entity = None
    source_key = None
    config_keys_to_remove = []

    try:
        for client in (source_client, destination_client):
            exec_cmd(node=client, cmd=f"mkdir -p {workdir} && chmod 700 {workdir}")

        # Steps 1: version + health
        if validate_min_ceph_version(
            config,
            9,
            2,
            ("source", source_client),
            ("destination", destination_client),
        ):
            return 1
        if validate_cluster_health_and_daemons(source_client, "source"):
            return 1
        if validate_cluster_health_and_daemons(destination_client, "destination"):
            return 1

        # Step 2: pools
        for label, client, rbd, pool in (
            ("source", source_client, source_rbd, src_pool),
            ("destination", destination_client, destination_rbd, dst_pool),
        ):
            rc = create_single_pool_and_images(
                config=config,
                pool=pool,
                pool_config={
                    "pg_num": config.get("pg_num", 32),
                    "pgp_num": config.get("pgp_num", 32),
                },
                client=client,
                cluster="ceph",
                rbd=rbd,
                ceph_version=ceph_version,
                is_ec_pool=False,
                is_secondary=False,
                do_not_create_image=True,
            )
            if rc:
                log.error(f"{label} pool creation failed")
                return 1

        # Step 3: source image, data, snap + baseline checksum
        out, err = source_rbd.create(
            **{"image-spec": f"{src_pool}/{src_image}", "size": image_size}
        )
        if out or err:
            log.error(f"Source image creation failed: {out} {err}")
            return 1

        run_rbd_fio(
            client=source_client,
            pool=src_pool,
            image=src_image,
            rw="write",
            offset=config.get("source_pattern_a_offset", "0"),
            size=config.get("source_pattern_size", "64M"),
            pattern=config.get("source_pattern_a", "0xAA"),
            name="neg-source-pattern-a",
        )
        out, err = source_rbd.snap.create(
            **{"snap-spec": f"{src_pool}/{src_image}@{snap_name}"}
        )
        if err and "error" in str(err).lower():
            log.error(f"Source snapshot creation failed: {out} {err}")
            return 1

        baseline_checksum = get_checksum_rbd_image(
            source_client, f"{src_pool}/{src_image}@{snap_name}"
        )
        log.info(f"Source snapshot baseline sha256: {baseline_checksum}")

        # Steps 4-5: valid CephX client + mon_host/key
        source_entity, source_key = create_source_cephx_client(
            client=source_client,
            pool=src_pool,
            client_name=source_cephx_client,
        )
        mon_host = get_source_mon_host(source_client)
        log.info(f"Valid source mon_host collected (length={len(mon_host)})")

        base_spec = {
            "type": "native",
            "mon_host": mon_host,
            "client_name": source_entity,
            "key": source_key,
            "pool_name": src_pool,
            "image_name": src_image,
            "snap_name": snap_name,
        }

        # Step 6: one valid prepare to confirm environment
        valid_spec_path = f"{workdir}/native-valid.json"
        valid_target_spec = f"{dst_pool}/{valid_target}"
        prepare_native_source_spec_with_key(
            client=destination_client,
            spec_path=valid_spec_path,
            mon_host=mon_host,
            client_name=source_entity,
            key=source_key,
            pool_name=src_pool,
            image_name=src_image,
            snap_name=snap_name,
        )
        destination_rbd.migration.prepare_import(
            source_spec_path=valid_spec_path,
            dest_spec=valid_target_spec,
        )
        if verify_migration_state(
            action="prepare",
            image_spec=valid_target_spec,
            client=destination_client,
        ):
            log.error("Valid migration prepare state verification failed")
            return 1
        log.info("Valid source-spec prepare succeeded (environment sanity check)")

        # Step 7: abort/cleanup valid target before negative cases
        exec_cmd(
            node=destination_client,
            cmd=f"rbd migration abort {valid_target_spec}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        exec_cmd(
            node=destination_client,
            cmd=f"rbd rm {valid_target_spec}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        if verify_no_stale_migration_target(destination_client, dst_pool, valid_target):
            log.error("Valid target remained after abort/cleanup")
            return 1

        # Build invalid key by flipping characters (keep length / charset-ish)
        invalid_key = source_key[::-1]
        if invalid_key == source_key:
            invalid_key = ("A" + source_key[1:]) if source_key else "invalidkey"

        # Wrong key in config-key store (step 15)
        store_source_key_in_config_key(
            destination_client, wrong_config_key_path, invalid_key, workdir
        )
        config_keys_to_remove.append(wrong_config_key_path)

        # Unique target image per case to make stale-state checks unambiguous
        def _target(suffix):
            return f"{target_image}_{suffix}"

        negative_cases = [
            # Step 8: mon_host without key
            {
                "name": "mon_host without key",
                "target": _target("no_key"),
                "spec": {k: v for k, v in base_spec.items() if k != "key"},
                "errors": [
                    "cannot specify mon host without key",
                    "without key",
                    "EINVAL",
                    "invalid",
                ],
            },
            # Step 9: key without mon_host
            {
                "name": "key without mon_host",
                "target": _target("no_mon"),
                "spec": {k: v for k, v in base_spec.items() if k != "mon_host"},
                "errors": [
                    "cannot specify key without mon host",
                    "without mon host",
                    "EINVAL",
                    "invalid",
                ],
            },
            # Step 10: cluster_name + mon_host
            {
                "name": "cluster_name and mon_host mutually exclusive",
                "target": _target("cluster_mon"),
                "spec": {**base_spec, "cluster_name": "source"},
                "errors": [
                    "cannot specify both cluster name and mon host",
                    "cluster name and mon host",
                    "EINVAL",
                    "invalid",
                ],
            },
            # Step 11: cluster_name + key (no mon_host) — key requires mon_host
            {
                "name": "cluster_name and key mixed",
                "target": _target("cluster_key"),
                "spec": {
                    "type": "native",
                    "cluster_name": "source",
                    "client_name": source_entity,
                    "key": source_key,
                    "pool_name": src_pool,
                    "image_name": src_image,
                    "snap_name": snap_name,
                },
                "errors": [
                    "cannot specify key without mon host",
                    "without mon host",
                    "cluster name",
                    "EINVAL",
                    "invalid",
                ],
            },
            # Step 12: unreachable mon_host with ceph timeout
            {
                "name": "invalid/unreachable mon_host",
                "target": _target("bad_mon"),
                "spec": {
                    **base_spec,
                    "mon_host": "[v2:127.0.0.1:1/0,v1:127.0.0.1:2/0]",
                },
                "errors": [
                    "connection",
                    "timed out",
                    "timeout",
                    "refused",
                    "unavailable",
                    "failed",
                    "error",
                    "errno",
                ],
                "timeout": 420,
            },
            # Step 13: invalid inline key
            {
                "name": "invalid inline key",
                "target": _target("bad_key"),
                "spec": {**base_spec, "key": invalid_key},
                "errors": [
                    "auth",
                    "authentication",
                    "permission",
                    "access",
                    "denied",
                    "failed",
                    "error",
                    "EINVAL",
                ],
            },
            # Step 14: missing config-key path
            {
                "name": "missing config:// key path",
                "target": _target("missing_cfg"),
                "spec": {
                    **base_spec,
                    "key": f"config://{missing_config_key_path}",
                },
                "errors": [
                    "config-key",
                    "config key",
                    "does not exist",
                    "no such",
                    "not found",
                    "ENOENT",
                    "failed",
                    "error",
                ],
            },
            # Step 15: wrong key via config://
            {
                "name": "wrong key via config://",
                "target": _target("wrong_cfg"),
                "spec": {
                    **base_spec,
                    "key": f"config://{wrong_config_key_path}",
                },
                "errors": [
                    "auth",
                    "authentication",
                    "permission",
                    "access",
                    "denied",
                    "failed",
                    "error",
                ],
            },
            # Step 17: invalid pool name
            {
                "name": "invalid source pool name",
                "target": _target("bad_pool"),
                "spec": {**base_spec, "pool_name": "nonexistent_src_pool_xyz"},
                "errors": [
                    "pool",
                    "No such",
                    "not found",
                    "does not exist",
                    "ENOENT",
                    "failed",
                    "error",
                ],
            },
            # Step 18: invalid image name
            {
                "name": "invalid source image name",
                "target": _target("bad_image"),
                "spec": {**base_spec, "image_name": "nonexistent_src_image_xyz"},
                "errors": [
                    "image",
                    "No such",
                    "not found",
                    "does not exist",
                    "ENOENT",
                    "failed",
                    "error",
                ],
            },
            # Step 19a: invalid snap name
            {
                "name": "invalid source snap name",
                "target": _target("bad_snap"),
                "spec": {**base_spec, "snap_name": "nonexistent_snap_xyz"},
                "errors": [
                    "snap",
                    "snapshot",
                    "No such",
                    "not found",
                    "does not exist",
                    "ENOENT",
                    "failed",
                    "error",
                ],
            },
            # Step 19b: missing snap for import-only
            {
                "name": "missing snap_name/snap_id for import-only",
                "target": _target("no_snap"),
                "spec": {k: v for k, v in base_spec.items() if k != "snap_name"},
                "errors": [
                    "snap name or snap id required for import",
                    "snap name or snap id required",
                    "required for import",
                    "EINVAL",
                    "invalid",
                ],
            },
            # Step 20a: pool_name + pool_id
            {
                "name": "pool_name and pool_id conflict",
                "target": _target("pool_both"),
                "spec": {**base_spec, "pool_id": 0},
                "errors": [
                    "cannot specify both pool name and pool id",
                    "pool name and pool id",
                    "EINVAL",
                    "invalid",
                ],
            },
            # Step 20b: snap_name + snap_id
            {
                "name": "snap_name and snap_id conflict",
                "target": _target("snap_both"),
                "spec": {**base_spec, "snap_id": 1},
                "errors": [
                    "cannot specify both snap name and snap id",
                    "snap name and snap id",
                    "EINVAL",
                    "invalid",
                ],
            },
        ]

        failures = 0
        for case in negative_cases:
            # Unique spec path per case avoids races / stale files
            case_spec_path = f"{workdir}/neg_{case['target']}.json"
            rc = _run_negative_prepare_case(
                destination_client=destination_client,
                spec_path=case_spec_path,
                dest_pool=dst_pool,
                target_image=case["target"],
                case_name=case["name"],
                spec=case["spec"],
                expected_errors=case["errors"],
                timeout=case.get("timeout", 420),
            )
            if rc:
                failures += 1

        if failures:
            log.error(f"{failures} negative prepare case(s) failed")
            return 1

        # Step 22: source image/snap unchanged
        final_checksum = get_checksum_rbd_image(
            source_client, f"{src_pool}/{src_image}@{snap_name}"
        )
        if final_checksum != baseline_checksum:
            log.error(
                "Source snapshot checksum changed after negative tests: "
                f"baseline={baseline_checksum} final={final_checksum}"
            )
            return 1
        log.info("Source image and snapshot remain unchanged after negative tests")

        # Step 23: cluster health
        if validate_cluster_health_and_daemons(source_client, "source"):
            return 1
        if validate_cluster_health_and_daemons(destination_client, "destination"):
            return 1

        # Step 24: no key leak in destination logs / command output already redacted
        test_start = kw.get("test_start")
        if (
            test_start
            and source_key
            and verify_key_not_logged(
                destination_client, source_key, workdir, test_start
            )
        ):
            return 1

        log.info("CEPH-83632850 native import mon_host/key negative validation passed")
        return 0

    except Exception as error:
        log.error(f"CEPH-83632850 negative validation failed: {error}")
        return 1

    finally:
        # Step 25: cleanup
        log.info("Cleaning up CEPH-83632850 negative test resources")
        exec_cmd(
            node=destination_client,
            cmd=f"rm -f {source_spec_path} {workdir}/native-valid.json",
            check_ec=False,
        )
        for path in config_keys_to_remove:
            remove_config_key(destination_client, path)

        # Abort/remove any leftover targets matching this run's prefix
        listed = exec_cmd(
            node=destination_client,
            cmd=f"rbd ls {dst_pool}",
            output=True,
            check_ec=False,
        )
        for img in (listed or "").split():
            if img == valid_target or img.startswith(f"{target_image}"):
                spec = f"{dst_pool}/{img}"
                exec_cmd(
                    node=destination_client,
                    cmd=f"rbd migration abort {spec}",
                    check_ec=False,
                    long_running=True,
                    timeout=7200,
                )
                exec_cmd(
                    node=destination_client,
                    cmd=f"rbd rm {spec}",
                    check_ec=False,
                    long_running=True,
                    timeout=7200,
                )

        if source_key:
            verify_key_not_logged(
                destination_client,
                source_key,
                workdir,
                kw.get("test_start", datetime.utcnow().isoformat()),
            )

        exec_cmd(
            node=source_client,
            cmd=f"rbd snap rm {src_pool}/{src_image}@{snap_name}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        exec_cmd(
            node=source_client,
            cmd=f"rbd rm {src_pool}/{src_image}",
            check_ec=False,
            long_running=True,
            timeout=7200,
        )
        if source_entity:
            exec_cmd(
                node=source_client,
                cmd=f"ceph auth rm {source_entity}",
                check_ec=False,
            )

        pool_cleanup(
            client=destination_client,
            pools=[dst_pool],
            ceph_version=ceph_version,
        )
        pool_cleanup(
            client=source_client,
            pools=[src_pool],
            ceph_version=ceph_version,
        )
        for client in (source_client, destination_client):
            exec_cmd(node=client, cmd=f"rm -rf {workdir}", check_ec=False)


def resolve_source_destination_clients(**kw):
    """Resolve source and destination client nodes from ceph_cluster_dict."""
    cluster_dict = kw.get("ceph_cluster_dict", {})
    if "ceph-rbd1" in cluster_dict and "ceph-rbd2" in cluster_dict:
        source_cluster = cluster_dict["ceph-rbd1"]
        destination_cluster = cluster_dict["ceph-rbd2"]
    else:
        clusters = list(cluster_dict.values())
        if len(clusters) < 2:
            raise ValueError("This test requires two Ceph clusters")
        source_cluster, destination_cluster = clusters[0], clusters[1]

    source_client = source_cluster.get_nodes(role="client")[0]
    destination_client = destination_cluster.get_nodes(role="client")[0]
    return source_cluster, destination_cluster, source_client, destination_client


def prepare_pool_type_config(**kw):
    """Select rep or ec pool config and merge into kw[config].

    When only one of ``rep_pool_config`` / ``ec_pool_config`` is present,
    that pool type is used. When both are present, one is chosen at random.
    """
    config = kw.get("config", {})
    available = [
        pool_type
        for pool_type in ("rep_pool_config", "ec_pool_config")
        if config.get(pool_type)
    ]
    if not available:
        raise ValueError("Config must include rep_pool_config and/or ec_pool_config")

    pool_type = available[0] if len(available) == 1 else random.choice(available)
    log.info("Running test on pool type: %s", pool_type)

    pool_cfg = config.get(pool_type, {})

    if pool_type == "rep_pool_config":
        config["rep-pool-only"] = True
        is_ec_pool = False
    else:
        config["ec-pool-only"] = True
        is_ec_pool = True

    config.update(pool_cfg)
    kw["config"] = config
    return pool_type, is_ec_pool


def run(**kw):
    """CephCI entry point for native import Polarion cases.

    Dispatches based on ``config.operation`` (same pattern as
    ``test_namespace_mirror_operations.py``):

    - CEPH-83632846: mon_host + inline key
    - CEPH-83632847: mon_host + config:// key
    - CEPH-83632851: NVMe-oF gateway Client-B source-backed reads
    - CEPH-83632848: sparse native import preserves used size / zero holes
    - CEPH-83632849: LUKS/LUKS2 encrypted native import with mon_host + key
    - CEPH-83632850: negative validation of mon_host/key source-spec

    When ``operation`` is omitted, defaults to CEPH-83632846.
    """
    kw["test_start"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    config = kw.get("config", {})
    operation = config.get("operation", "CEPH-83632846")

    operation_mapping = {
        "CEPH-83632846": "inline_key",
        "CEPH-83632847": "config_key",
        "CEPH-83632851": "nvmeof_gateway",
        "CEPH-83632848": "sparse_import",
        "CEPH-83632849": "encrypted_import",
        "CEPH-83632850": "negative_validation",
    }
    if operation not in operation_mapping:
        log.error(
            f"Unsupported operation {operation}. Supported: "
            f"{list(operation_mapping)}"
        )
        return 1

    log.info(f"Executing native import operation {operation}")

    try:
        (
            source_cluster,
            destination_cluster,
            source_client,
            destination_client,
        ) = resolve_source_destination_clients(**kw)
    except Exception as error:
        log.error(f"Unable to locate source/destination clients: {error}")
        return 1

    pool_type, is_ec_pool = prepare_pool_type_config(**kw)

    try:
        if operation == "CEPH-83632846":
            ret_val = test_native_import_mon_host_inline_key(
                source_client=source_client,
                destination_client=destination_client,
                is_ec_pool=is_ec_pool,
                **kw,
            )
        elif operation == "CEPH-83632847":
            ret_val = test_native_import_mon_host_config_key(
                source_client=source_client,
                destination_client=destination_client,
                is_ec_pool=is_ec_pool,
                **kw,
            )
        elif operation == "CEPH-83632848":
            ret_val = test_native_import_sparse_image(
                source_client=source_client,
                destination_client=destination_client,
                is_ec_pool=is_ec_pool,
                **kw,
            )
        elif operation == "CEPH-83632849":
            ret_val = test_native_import_encrypted_image(
                source_client=source_client,
                destination_client=destination_client,
                is_ec_pool=is_ec_pool,
                **kw,
            )
        elif operation == "CEPH-83632850":
            ret_val = test_native_import_mon_host_key_negative(
                source_client=source_client,
                destination_client=destination_client,
                is_ec_pool=is_ec_pool,
                **kw,
            )
        elif operation == "CEPH-83632851":
            nvme_config = dict(kw.get("config", {}).get("nvme_config", {}))
            nvme_config.setdefault("gw_nodes", ["node6"])
            nvme_config.setdefault("gw_group", "gw_group1")
            nvme_config.setdefault("rbd_pool", "rbd")
            nvme_config.setdefault("nvme_metadata_pool", "rbd")
            nvme_config.setdefault("install", True)
            nvme_config.setdefault("cleanup", ["subsystems", "gateway"])
            if not nvme_config.get("subsystems"):
                nvme_config["subsystems"] = [_default_nvme_subsystem(nvme_config)]
            if "gw_node" not in nvme_config and nvme_config.get("gw_nodes"):
                nvme_config.setdefault("gw_node", nvme_config["gw_nodes"][0])
            client_a = destination_client
            # Client-B = dedicated nvmeof-gw (node6).
            gw_node_id = nvme_config.get("gw_node") or nvme_config["gw_nodes"][0]
            try:
                client_b = get_node_by_id(destination_cluster, gw_node_id)
            except Exception:
                client_b = resolve_gateway_like_client(
                    destination_cluster, client_a, kw.get("config", {})
                )
            kw["destination_cluster"] = destination_cluster
            kw["nvme_config"] = nvme_config
            ret_val = test_native_import_gateway_like_client(
                source_client=source_client,
                client_a=client_a,
                client_b=client_b,
                is_ec_pool=is_ec_pool,
                **kw,
            )
        else:
            log.error(f"Unsupported operation: {operation}")
            return 1

    except Exception as error:
        log.error(f"Test {operation} failed with error: {error}")
        return 1

    if ret_val:
        log.error(f"Test {operation} FAILED on pool type: {pool_type}")
    else:
        log.info(f"Test {operation} PASSED on pool type: {pool_type}")
    return ret_val
