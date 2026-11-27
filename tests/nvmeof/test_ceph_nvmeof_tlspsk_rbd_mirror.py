"""
TLS-PSK on an RBD image mirrored between two Ceph clusters.

ceph-rbd1 is the primary. ceph-rbd2 is the secondary. Journal-mode image
mirroring is configured between them, then the primary image is exported
through a TLS-PSK NVMe-oF namespace. A write over ``nvme connect --tls``
must replay onto the secondary (entries_behind_primary == 0) and the image
checksums must match.

Planned failover deletes the primary namespace so the gateway drops the
image, demotes the primary, and promotes the secondary. The same image is
then exported from the secondary gateway and IO over ``--tls`` must succeed.

Cluster config: conf/tentacle/nvmeof/ceph_nvmeof_rbd_mirror.yaml
Suite: suites/tentacle/nvmeof/tier-3_nvmeof_tlspsk_rbd_mirror.yaml

``up+stopped`` on a cluster with no peer is not a successful sync.
"""

import datetime
import json
import re
import time
from copy import deepcopy

from ceph.ceph import Ceph
from ceph.rbd.initial_config import initial_mirror_config
from ceph.rbd.utils import get_md5sum_rbd_image
from ceph.rbd.workflows.cleanup import cleanup as rbd_mirror_pool_cleanup
from ceph.rbd.workflows.rbd_mirror import wait_for_status
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.gateway_entities import configure_subsystems, teardown
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
from tests.nvmeof.workflows.tlspsk import (
    add_host_with_psk,
    add_secure_listener,
    assert_plain_connect_refused,
    connect_with_tls,
    disconnect_nqn,
    setup_initiator_tls_psk,
)
from utility.log import Log

LOG = Log(__name__)

POOL = "rbd"
IMAGE = "nvmeof-tlspsk-mirror"
NQN = "nqn.2016-06.io.spdk:tlspsk-mirror"
_ENTRIES_BEHIND = re.compile(r"entries_behind_primary[\"']?\s*[:=]\s*(\d+)")


def entries_behind_primary(status):
    """Return entries still to replay, or None when the status has no count."""
    description = str((status or {}).get("description") or "")
    match = _ENTRIES_BEHIND.search(description)
    if not match:
        return None
    return int(match.group(1))


def secondary_caught_up(status):
    """True only when the secondary is replaying and has no entries left.

    ``up+stopped`` is the primary's idle state, and also the state of an
    image that was enabled for mirroring with no peer. It is not evidence
    that data reached the other cluster.
    """
    if (status or {}).get("state") != "up+replaying":
        return False
    return entries_behind_primary(status) == 0


def demoted_mirror_state(rhbuild):
    """Image state on both clusters after a demote.

    RHCS 3 and later report ``up+unknown`` until the peer is promoted.
    Older releases report ``up+stopped``.
    """
    major = int(str(rhbuild)[0])
    return "up+stopped" if major < 3 else "up+unknown"


def namespace_add_args(nqn, pool, image):
    """CLI args that export an existing mirrored image. Do not create another."""
    return {
        "subsystem": nqn,
        "rbd-pool": pool,
        "rbd-image": image,
    }


def _mirror_status(rbd, pool, image):
    spec = f"{pool}/{image}"
    out, err = rbd.mirror.image.status(**{"image-spec": spec, "format": "json"})
    if not out:
        raise RuntimeError(f"rbd mirror image status {spec} failed: {err}")
    return json.loads(out)


def wait_until_secondary_caught_up(rbd, pool, image, timeout=300):
    """Poll until the secondary has replayed every journal entry.

    Raises:
        AssertionError: The image hits an error state, or the timeout expires
            before entries_behind_primary is 0.
    """
    deadline = time.time() + timeout
    last = {}
    spec = f"{pool}/{image}"
    while time.time() < deadline:
        last = _mirror_status(rbd, pool, image)
        state = last.get("state", "")
        LOG.info(
            "Mirror status %s: state=%s entries_behind=%s",
            spec,
            state,
            entries_behind_primary(last),
        )
        if "error" in state:
            raise AssertionError(f"{spec} mirror entered {state}: {last}")
        if secondary_caught_up(last):
            return last
        time.sleep(10)
    raise AssertionError(
        f"{spec} did not finish replaying within {timeout}s. Last status: {last}"
    )


def _raise_on_rbd_err(err, operation):
    text = (err or "").strip()
    if text and "100% complete" not in text:
        raise RuntimeError(f"{operation} failed: {text}")


def _md5(rbd, client, pool, image):
    path = f"/tmp/tlspsk-mirror-{image}.img"
    digest = get_md5sum_rbd_image(
        image_spec=f"{pool}/{image}",
        file_path=path,
        rbd=rbd,
        client=client,
    )
    if not digest:
        raise AssertionError(f"Failed to export {pool}/{image} for checksum")
    return digest


def _nvme_config(config):
    """NVMeService config for one cluster. The namespace image is added later."""
    return {
        "spec_deployment": True,
        "tls_psk": True,
        "rbd_pool": config["pool"],
        "gw_group": config["gw_group"],
        "gw_nodes": config["gw_nodes"],
        "subsystems": [
            {
                "nqn": config["nqn"],
                "no-group-append": True,
                "secure_listener": True,
                "listener_port": config["listener_port"],
            }
        ],
        "cleanup": config.get("cleanup", ["subsystems", "gateway"]),
    }


def _deploy_tlspsk(cluster, nvme_config, custom_cfg):
    check_and_set_nvme_cli_image(cluster, config=custom_cfg)
    service = NVMeService(nvme_config, cluster)
    service.deploy()
    service.init_gateways()
    service.wait_for_gateways_ready()
    return service


def _prepare_subsystem(service, cluster, initiator_node, nqn, port):
    """Secure subsystem, listeners, and the initiator host PSK."""
    configure_subsystems(service, ceph_cluster=cluster)
    psk = setup_initiator_tls_psk(initiator_node, nqn)
    for gateway in service.gateways:
        add_secure_listener(
            gateway,
            nqn,
            gateway.node.ip_address,
            port,
            gateway.node.hostname,
        )
    add_host_with_psk(service.gateways[0], nqn, psk["hostnqn"], psk["key_data"])
    return psk


def _add_namespace(gateway, nqn, pool, image):
    args = namespace_add_args(nqn, pool, image)
    LOG.info("Exporting mirrored image %s/%s on %s", pool, image, nqn)
    try:
        gateway.namespace.add(**{"args": args})
    except Exception as exc:
        message = str(exc).lower()
        if "already" not in message and "exist" not in message:
            raise
        LOG.info("Namespace for %s/%s already present", pool, image)


def _delete_namespaces(gateway, nqn):
    """Drop every namespace so the gateway releases the RBD image."""
    out, _ = gateway.namespace.list(
        **{"base_cmd_args": {"format": "json"}, "args": {"subsystem": nqn}}
    )
    payload = json.loads(out) if out else {}
    for ns in payload.get("namespaces") or []:
        nsid = ns.get("nsid", ns.get("ns_id"))
        LOG.info("Deleting namespace %s on %s before demote", nsid, nqn)
        gateway.namespace.delete(
            **{"args": {"subsystem": nqn, "nsid": nsid, "force": True}}
        )


def _fio(initiator, io_type, size, name, discard=True):
    initiator.start_fio(
        io_size=size,
        serial=True,
        io_type=io_type,
        execute_blkdiscard=discard,
        test_name=name,
    )


def _planned_failover(rbd_primary, rbd_secondary, primary_name, secondary_name, spec, rhbuild):
    """Demote the primary image, then promote the secondary."""
    demoted = demoted_mirror_state(rhbuild)
    out, err = rbd_primary.mirror.image.demote(**{"image-spec": spec})
    _raise_on_rbd_err(err, f"demote {spec}")
    LOG.info("Demoted %s: %s", spec, out)
    wait_for_status(
        rbd=rbd_primary,
        cluster_name=primary_name,
        imagespec=spec,
        state_pattern=demoted,
        tout=datetime.timedelta(seconds=300),
        retry_interval=10,
    )
    wait_for_status(
        rbd=rbd_secondary,
        cluster_name=secondary_name,
        imagespec=spec,
        state_pattern=demoted,
        tout=datetime.timedelta(seconds=300),
        retry_interval=10,
    )
    out, err = rbd_secondary.mirror.image.promote(**{"image-spec": spec})
    _raise_on_rbd_err(err, f"promote {spec}")
    LOG.info("Promoted %s on %s: %s", spec, secondary_name, out)
    wait_for_status(
        rbd=rbd_secondary,
        cluster_name=secondary_name,
        imagespec=spec,
        description="local image is primary",
        tout=datetime.timedelta(seconds=300),
        retry_interval=10,
    )


def test_ceph_tlspsk_rbd_mirror(primary, secondary, config, custom_cfg, services):
    """Write through TLS-PSK, confirm the secondary copy, then fail over.

    Args:
        primary: Cluster dict from initial_mirror_config (not secondary).
        secondary: Cluster dict from initial_mirror_config (is secondary).
        config: Suite config. Uses pool, image, nqn, gateways, and initiator.
        custom_cfg: cephci custom-config, forwarded to the CLI image helper.
        services: Filled with the NVMeService objects as soon as they deploy,
            so cleanup still runs when a later step fails.
    """
    pool = config["pool"]
    image = config["image"]
    nqn = config["nqn"]
    port = config["listener_port"]
    fio_size = config.get("fio_size", "256m")
    replay_timeout = int(config.get("replay_timeout", 300))
    spec = f"{pool}/{image}"

    rbd_primary = primary["rbd"]
    rbd_secondary = secondary["rbd"]
    client_primary = primary["client"]
    client_secondary = secondary["client"]
    primary_cluster = primary["cluster"]
    secondary_cluster = secondary["cluster"]

    nvme_cfg = _nvme_config(config)
    services["primary"] = _deploy_tlspsk(primary_cluster, deepcopy(nvme_cfg), custom_cfg)
    services["secondary"] = _deploy_tlspsk(
        secondary_cluster, deepcopy(nvme_cfg), custom_cfg
    )
    primary_service = services["primary"]
    secondary_service = services["secondary"]

    primary_initiator_node = get_node_by_id(primary_cluster, config["initiator_node"])
    secondary_initiator_node = get_node_by_id(
        secondary_cluster, config["initiator_node"]
    )
    _prepare_subsystem(primary_service, primary_cluster, primary_initiator_node, nqn, port)
    _prepare_subsystem(
        secondary_service, secondary_cluster, secondary_initiator_node, nqn, port
    )

    primary_gw = primary_service.gateways[0]
    _add_namespace(primary_gw, nqn, pool, image)

    initiator = NVMeInitiator(primary_initiator_node)
    LOG.info("Writing %s to %s over TLS-PSK", fio_size, spec)
    connect_with_tls(
        primary_initiator_node, primary_gw.node.ip_address, port, nqn, expect_failure=False
    )
    _fio(initiator, "write", fio_size, "tlspsk-mirror-primary-write")
    disconnect_nqn(primary_initiator_node, nqn)
    assert_plain_connect_refused(
        primary_initiator_node,
        primary_gw.node.ip_address,
        port,
        nqn,
        context="primary",
    )

    LOG.info("Waiting for %s to replay onto %s", spec, secondary_cluster.name)
    wait_until_secondary_caught_up(
        rbd_secondary, pool, image, timeout=replay_timeout
    )
    primary_md5 = _md5(rbd_primary, client_primary, pool, image)
    secondary_md5 = _md5(rbd_secondary, client_secondary, pool, image)
    LOG.info("Checksum %s primary=%s secondary=%s", spec, primary_md5, secondary_md5)
    if primary_md5 != secondary_md5:
        raise AssertionError(
            f"{spec} checksum mismatch after replay: "
            f"primary {primary_md5} secondary {secondary_md5}"
        )

    LOG.info("Releasing %s on the primary gateway before demote", spec)
    _delete_namespaces(primary_gw, nqn)
    _planned_failover(
        rbd_primary,
        rbd_secondary,
        primary_cluster.name,
        secondary_cluster.name,
        spec,
        config["rhbuild"],
    )

    secondary_gw = secondary_service.gateways[0]
    _add_namespace(secondary_gw, nqn, pool, image)
    secondary_initiator = NVMeInitiator(secondary_initiator_node)
    LOG.info("Reading the promoted image %s over TLS-PSK", spec)
    connect_with_tls(
        secondary_initiator_node,
        secondary_gw.node.ip_address,
        port,
        nqn,
        expect_failure=False,
    )
    _fio(
        secondary_initiator,
        "read",
        fio_size,
        "tlspsk-mirror-secondary-read",
        discard=False,
    )
    _fio(
        secondary_initiator,
        "write",
        fio_size,
        "tlspsk-mirror-secondary-write",
        discard=False,
    )
    disconnect_nqn(secondary_initiator_node, nqn)
    assert_plain_connect_refused(
        secondary_initiator_node,
        secondary_gw.node.ip_address,
        port,
        nqn,
        context="secondary",
    )
    LOG.info(
        "TLS-PSK RBD mirror passed: %s replayed, checksums matched, "
        "and IO worked on %s after promote",
        spec,
        secondary_cluster.name,
    )


def _defaults(config):
    config.setdefault("pool", POOL)
    config.setdefault("image", IMAGE)
    config.setdefault("image_size", "2G")
    config.setdefault("nqn", NQN)
    config.setdefault("gw_group", "tlspsk-mirror")
    config.setdefault("gw_nodes", ["node6", "node7"])
    config.setdefault("listener_port", 4420)
    config.setdefault("initiator_node", "node2")
    config.setdefault("fio_size", "256m")
    config.setdefault("replay_timeout", 300)
    return config


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Peer ceph-rbd1 and ceph-rbd2, then run the TLS-PSK mirror failover."""
    config = _defaults(kwargs["config"])
    custom_cfg = kwargs.get("test_data", {}).get("custom-config")
    clusters = kwargs.get("ceph_cluster_dict") or {}
    primary_cluster = clusters.get("ceph-rbd1")
    secondary_cluster = clusters.get("ceph-rbd2")
    mirror_obj = None
    mirror_kwargs = None
    services = {"primary": None, "secondary": None}
    rbd_primary = None
    rbd_secondary = None

    if not primary_cluster or not secondary_cluster:
        LOG.error(
            "This test needs both ceph-rbd1 and ceph-rbd2 from "
            "conf/tentacle/nvmeof/ceph_nvmeof_rbd_mirror.yaml"
        )
        return 1

    try:
        mirror_kwargs = deepcopy(kwargs)
        mirror_kwargs["ceph_cluster"] = primary_cluster
        mirror_kwargs["ceph_cluster_dict"] = clusters
        mirror_kwargs["config"] = {
            "do_not_run_io": True,
            "rep-pool-only": True,
            "rhbuild": config["rhbuild"],
            "rep_pool_config": {
                config["pool"]: {
                    "mode": "image",
                    "mirrormode": "journal",
                    "way": "two-way",
                    "peer_mode": "bootstrap",
                    "rbd_client": "client.admin",
                    config["image"]: {
                        "size": config["image_size"],
                        "image-feature": "exclusive-lock,journaling",
                    },
                }
            },
        }
        LOG.info(
            "Configuring journal mirroring for %s/%s between %s and %s",
            config["pool"],
            config["image"],
            primary_cluster.name,
            secondary_cluster.name,
        )
        mirror_obj = initial_mirror_config(**mirror_kwargs)
        mirror_obj.pop("output", None)

        primary = secondary = None
        for site in mirror_obj.values():
            if site.get("is_secondary"):
                secondary = site
            else:
                primary = site
        if not primary or not secondary:
            raise RuntimeError("initial_mirror_config did not return both clusters")
        rbd_primary = primary["rbd"]
        rbd_secondary = secondary["rbd"]

        test_ceph_tlspsk_rbd_mirror(primary, secondary, config, custom_cfg, services)
        return 0
    except Exception as err:
        LOG.exception("TLS-PSK RBD mirror test FAILED: %s", err)
        return 1
    finally:
        for service, rbd in (
            (services["primary"], rbd_primary),
            (services["secondary"], rbd_secondary),
        ):
            if service is None or rbd is None:
                continue
            try:
                teardown(service, rbd)
            except Exception as exc:
                LOG.warning("NVMe teardown failed: %s", exc)
        if mirror_obj is not None and mirror_kwargs is not None:
            try:
                rbd_mirror_pool_cleanup(
                    ["rep_pool_config"],
                    mirror_obj,
                    config=mirror_kwargs["config"],
                )
            except Exception as exc:
                LOG.warning("RBD mirror pool cleanup failed: %s", exc)
