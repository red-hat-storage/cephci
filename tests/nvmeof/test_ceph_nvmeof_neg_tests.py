"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway
 with supported entities like subsystems , etc.,

"""

import json
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from time import sleep

from ceph.ceph import Ceph, SocketTimeoutException
from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.helper import check_service_exists
from ceph.ceph_admin.orch import Orch
from ceph.parallel import parallel
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.monitor_workflows import MonitorWorkflows
from ceph.rbd.workflows.cluster_operations import operation, osd_remove_and_add_back
from ceph.utils import get_node_by_id, get_nodes_by_ids
from cli.cephadm.cephadm import CephAdm
from cli.utilities.utils import get_running_containers, reboot_node
from tests.cephadm import test_nvmeof, test_orch
from tests.nvmeof.test_ceph_nvmeof_gateway import initiators
from tests.nvmeof.workflows.gateway_entities import (
    configure_gw_entities,
    configure_hosts,
    configure_listeners,
    configure_subsystems,
    teardown,
)
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_gateway import create_gateway
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    nvme_gw_cli_version_adapter,
    setup_firewalld,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.retry import retry
from utility.utils import generate_unique_id

LOG = Log(__name__)
cli_image = str()


def test_ceph_83575812(ceph_cluster, rbd, nvme_service, pool, config):
    """CEPH-83575812 Remove the image during NVMe images are in use."""
    try:
        # Deploy NVMe Service
        if config.get("install"):
            nvme_service.deploy()

        # Initialize gateways
        nvme_service.init_gateways()
        nvmegwcli = nvme_service.gateways[0]

        # Configure Subsystem, listeners, host, namespaces
        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd)

        # Get Image name
        list_args = {}
        list_args.setdefault("args", {}).update(
            {"subsystem": config["subsystems"][0]["nqn"]}
        )
        list_args.setdefault("base_cmd_args", {}).update({"format": "json"})
        namespaces, _ = nvmegwcli.namespace.list(**list_args)
        namespaces = json.loads(namespaces)
        rbd_image_name = namespaces.get("namespaces")[0].get("rbd_image_name")

        if config.get("initiators"):
            with parallel() as p:
                for initiator in config["initiators"]:
                    p.spawn(initiators, ceph_cluster, nvmegwcli, initiator)
                    sleep(20)
                    out, err = rbd.remove_image(
                        pool, rbd_image_name, **{"all": True, "check_ec": False}
                    )
                    if "rbd: error: image still has watchers" not in out + err:
                        raise Exception("RBD image removed when its in use.")
                    LOG.info("RBD image removal failed as expected when its in use....")

        LOG.info("Validation of CEPH-83575812 is successful.")

    except Exception as err:
        raise Exception(err)


def test_ceph_83575467(ceph_cluster, rbd, nvme_service, pool, config):
    """CEPH-83575467: Perform restart and validate the gateway entities"""
    try:
        # Deploy NVMe Service
        if config.get("install"):
            nvme_service.deploy()

        # Initialize gateways
        nvme_service.init_gateways()
        nvmegwcli = nvme_service.gateways[0]

        # Configure Subsystem, listeners, host, namespaces
        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd)

        @retry(IOError, tries=5, delay=5)
        def list_subsystems(**sub_args):
            try:
                out, _ = nvmegwcli.subsystem.list(**sub_args)
                return out
            except Exception as e:
                raise IOError(e)

        sub_args = {"base_cmd_args": {"format": "json"}}
        for initiator in config["initiators"]:
            initiators(ceph_cluster, nvmegwcli, initiator)
        gw_info_bkp = list_subsystems(**sub_args)
        gw_info_bkp = json.loads(gw_info_bkp.strip())["subsystems"]

        # restart nvmeof service
        group = config.get("gw_group", "")
        service_name = f"nvmeof.{pool}"
        if group:
            service_name = f"{service_name}.{group}"
        restart_cfg = {
            "no_cluster_state": False,
            "config": {
                "service": service_name,
                "command": "restart",
                "args": {"verify": True, "service_name": service_name},
                "pos_args": [service_name],
            },
        }
        test_orch.run(ceph_cluster, **restart_cfg)

        gw_info = list_subsystems(**sub_args)
        gw_info = json.loads(gw_info.strip())["subsystems"]

        if gw_info != gw_info_bkp:
            raise Exception(
                f"GW Entities aren't same. Actual: {gw_info_bkp} Current: {gw_info}"
            )
        LOG.info("Validation of Ceph-83575467 is successful.")

    except Exception as err:
        raise Exception(err)


def test_ceph_83575813(ceph_cluster, rbd, nvme_service, pool, config):
    """CEPH-83575813: Perform NVMeoF RBD operations expand on images."""
    client = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = NVMeInitiator(client)
    try:
        # Deploy NVMe Service
        if config.get("install"):
            nvme_service.deploy()

        # Initialize gateways
        nvme_service.init_gateways()
        nvmegwcli = nvme_service.gateways[0]

        # Configure Subsystem, listeners, host, namespaces
        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd)

        # Run IOS on nvme namespaces
        initiator.disconnect_all()
        for i in config["initiators"]:
            initiators(ceph_cluster, nvmegwcli, i)

        def check(_node, _size):
            out, _ = _node.exec_command(sudo=True, cmd="lsblk -bJ")
            for _img in json.loads(out)["blockdevices"]:
                if _img["name"].startswith("nvme") and _img["size"] == _size:
                    LOG.info(f"Found Image with {_size}: {_img}")
                    return True
            raise Exception(f"Did not find NVMe disk with Size {_size}")

        # Get Image name
        list_args = {}
        list_args.setdefault("args", {}).update(
            {"subsystem": config["subsystems"][0]["nqn"]}
        )
        list_args.setdefault("base_cmd_args", {}).update({"format": "json"})
        namespaces, _ = nvmegwcli.namespace.list(**list_args)
        namespaces = json.loads(namespaces)
        rbd_image_name1 = namespaces.get("namespaces")[0].get("rbd_image_name")
        rbd_image_name2 = namespaces.get("namespaces")[1].get("rbd_image_name")

        rbd_objs = rbd.get_disk_usage_for_pool(pool)["images"]
        imgs = {
            rbd_image_name1: config["subsystems"][0]["bdevs"][0]["size"],
            rbd_image_name2: config["subsystems"][0]["bdevs"][1]["size"],
        }
        for img in imgs.keys():
            rbd_img_size = [
                i["provisioned_size"] for i in rbd_objs if img == i["name"]
            ][0]
            check(client, rbd_img_size)

        # Expand the images and validate the sizes at the client side
        nvmegwcli.namespace.resize(
            **{
                "args": {
                    "subsystem": config["subsystems"][0]["nqn"],
                    "nsid": 1,
                    "size": "3G",
                }
            }
        )
        nvmegwcli.namespace.resize(
            **{
                "args": {
                    "subsystem": config["subsystems"][0]["nqn"],
                    "nsid": 2,
                    "size": "4G",
                }
            }
        )

        for img in rbd.get_disk_usage_for_pool(pool)["images"]:
            check(client, img["provisioned_size"])
        initiator.disconnect_all()
        for i in config["initiators"]:
            initiators(ceph_cluster, nvmegwcli, i)
        LOG.info("Validation of CEPH-83575813 is successful.")

    except Exception as err:
        raise Exception(err)


def test_ceph_83575455(ceph_cluster, rbd, nvme_service, pool, config):
    """CEPH-83575455: Validate Host access failures"""
    try:
        # Deploy NVMe Service
        if config.get("install"):
            nvme_service.deploy()

        # Initialize gateways
        nvme_service.init_gateways()
        nvmegwcli = nvme_service.gateways[0]

        # Configure Subsystem, listeners, namespaces
        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd)

        # Configure hosts
        client = get_node_by_id(ceph_cluster, config["initiator_node"])
        initiator = NVMeInitiator(client)
        initiator_nqn = initiator.initiator_nqn()
        host_args = {
            "args": {
                "subsystem": config["initiators"][0]["nqn"],
                "host": initiator_nqn,
            }
        }
        nvmegwcli.host.add(**host_args)

        # Connect the initiator to the subsystem
        initiators = config.get("initiators")
        initiator.connect_targets(nvmegwcli, initiators[0])

        targets = initiator.list_devices()

        _dir = f"/tmp/dir_{generate_unique_id(4)}"
        _file = f"{_dir}/test.log"
        client.exec_command(sudo=True, cmd=f"mkdir {_dir}")
        client.exec_command(sudo=True, cmd=f"mkfs.ext4 {targets[0]}")
        client.exec_command(sudo=True, cmd=f"mount {targets[0]} {_dir}")
        client.exec_command(sudo=True, cmd=f"cp /var/log/messages {_file}")
        client.exec_command(sudo=True, cmd=f"ls -ltrh {_file}")

        # Remove client host access to the namespaces
        # Check for the non-existence of nvme namespaces
        # Create a file to check IO failure on mount point
        LOG.info("Remove client host access to the namespaces")
        try:
            nvmegwcli.host.delete(**host_args)
        except Exception as host_del_err:
            if (
                "Reconnecting the host would fail unless it is re-added to the subsystem"
                not in str(host_del_err)
            ):
                raise Exception(
                    "Host deletion is failed with error:" + str(host_del_err)
                )
            else:
                LOG.info("Deletion of host is successful")

        sleep(20)
        LOG.info("Check targets are not found on client")
        targets = initiator.list_spdk_drives()
        if targets:
            raise Exception(f"NVMe Targets found on {client.hostname}!!!")
        LOG.info(f"NVMe targets not found on {client.hostname} as expected..")
        try:
            client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/zero of={_file}_test bs=1M count=1000",
                timeout=30,
            )
        except SocketTimeoutException as timeout:
            LOG.info(
                f"Command execution failure as expected with timeout"
                f" as IO fails on inaccessible mount point : {timeout}"
            )

        # Add client host access
        # Check the existence of the NVMe namespaces
        nvmegwcli.host.add(**host_args)
        sleep(10)
        targets = initiator.list_devices()
        if not targets:
            raise Exception(f"NVMe Targets not found on {client.hostname}")
        client.exec_command(
            sudo=True, cmd=f"dd if=/dev/zero of={_file}_test bs=1M count=1000"
        )
        client.exec_command(sudo=True, cmd=f"ls -ltrh {_file}_test")
        LOG.info("Validation of CEPH-83575455 is successful.")
    except Exception as err:
        raise Exception(err)


def test_ceph_83576084(ceph_cluster, rbd, nvme_service, pool, config):
    """CEPH-83576084: Delete-recreate bdev in loop and rediscover namespace."""
    client = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = NVMeInitiator(client)
    try:
        # Deploy NVMe Service
        if config.get("install"):
            nvme_service.deploy()

        # Initialize gateways
        nvme_service.init_gateways()
        nvmegwcli = nvme_service.gateways[0]

        # Configure Subsystem, listeners, host, namespaces
        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd)

        cmd_args = {
            "transport": "tcp",
            "traddr": nvmegwcli.node.ip_address,
        }

        json_format = {"output-format": "json"}
        _dir = f"/tmp/dir_{generate_unique_id(4)}"
        _file = f"{_dir}/test.log"

        def check_client(verify=False):
            disc_port = {"trsvcid": 8009}
            _disc_cmd = {**cmd_args, **disc_port, **json_format}
            initiator.disconnect_all()
            sub_nqns, _ = initiator.discover(**_disc_cmd)
            LOG.debug(sub_nqns)
            _cmd_args = deepcopy(cmd_args)
            for nqn in json.loads(sub_nqns)["records"]:
                if nqn["trsvcid"] == str(config["initiators"][0]["listener_port"]):
                    _cmd_args["nqn"] = nqn["subnqn"]
                    break
            else:
                raise Exception(f"Subsystem not found -- {cmd_args}")

            # Connect to the subsystem
            conn_port = {"trsvcid": config["initiators"][0]["listener_port"]}
            _conn_cmd = {**_cmd_args, **conn_port}
            LOG.debug(initiator.connect(**_conn_cmd))
            targets = initiator.list_devices()

            if not verify:
                return targets[0]
            client.exec_command(sudo=True, cmd=f"mount {targets[0]} {_dir}")
            client.exec_command(sudo=True, cmd=f"ls -ltrh {_file}")

        target = check_client()
        client.exec_command(sudo=True, cmd=f"mkdir {_dir}")
        client.exec_command(sudo=True, cmd=f"mkfs.ext4 {target}")
        client.exec_command(sudo=True, cmd=f"mount {target} {_dir}")
        client.exec_command(sudo=True, cmd=f"cp /var/log/messages {_file}")

        _ns_args = {"args": {"subsystem": config["initiators"][0]["subnqn"], "nsid": 1}}
        # Get Image name
        list_args = {}
        list_args.setdefault("args", {}).update(
            {"subsystem": config["subsystems"][0]["nqn"]}
        )
        list_args.setdefault("base_cmd_args", {}).update({"format": "json"})
        namespaces, _ = nvmegwcli.namespace.list(**list_args)
        namespaces = json.loads(namespaces)
        rbd_image_name = namespaces.get("namespaces")[0].get("rbd_image_name")
        ns_args = {
            "args": {
                "subsystem": config["subsystems"][0]["nqn"],
                "rbd-pool": pool,
                "rbd-image": rbd_image_name,
            }
        }
        for _ in "check":
            client.exec_command(sudo=True, cmd=f"umount {_dir}")
            nvmegwcli.namespace.delete(**_ns_args)
            nvmegwcli.namespace.add(**ns_args)
            check_client(verify=True)

        LOG.info("Validation of CEPH-83576084 is successful.")

    except Exception as err:
        raise Exception(err)


def test_ceph_83576085(ceph_cluster, rbd, nvme_service, pool, config):
    """CEPH-83576085: Perform map and unmap NVMe namespaces in loop."""
    client = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = NVMeInitiator(client)
    try:
        # Deploy NVMe Service
        if config.get("install"):
            nvme_service.deploy()

        # Initialize gateways
        nvme_service.init_gateways()
        nvmegwcli = nvme_service.gateways[0]
        cmd_args = {
            "transport": "tcp",
            "traddr": nvmegwcli.node.ip_address,
            "trsvcid": config["initiators"][0]["listener_port"],
        }

        # Configure Subsystem, listeners, host, namespaces
        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd)

        json_format = {"output-format": "json"}
        _dir = f"/tmp/dir_{generate_unique_id(4)}"
        _file = f"{_dir}/test.log"

        disc_port = {"trsvcid": 8009}
        _disc_cmd = {**cmd_args, **disc_port, **json_format}
        initiator.disconnect_all()
        sub_nqns, _ = initiator.discover(**_disc_cmd)
        LOG.debug(sub_nqns)
        _cmd_args = deepcopy(cmd_args)
        for nqn in json.loads(sub_nqns)["records"]:
            if nqn["trsvcid"] == str(config["initiators"][0]["listener_port"]):
                _cmd_args["nqn"] = nqn["subnqn"]
                break
        else:
            raise Exception(f"Subsystem not found -- {cmd_args}")

        # Connect to the subsystem
        conn_port = {"trsvcid": config["initiators"][0]["listener_port"]}
        _conn_cmd = {**_cmd_args, **conn_port}
        LOG.debug(initiator.connect(**_conn_cmd))
        targets = initiator.list_devices()

        client.exec_command(sudo=True, cmd=f"mkdir {_dir}")
        client.exec_command(sudo=True, cmd=f"mkfs.ext4 {targets[0]}")
        client.exec_command(sudo=True, cmd=f"mount {targets[0]} {_dir}")
        client.exec_command(sudo=True, cmd=f"cp /var/log/messages {_file}")

        for _ in "check":
            client.exec_command(sudo=True, cmd=f"ls -ltrh {_file}")
            client.exec_command(sudo=True, cmd=f"umount {_dir}")
            client.exec_command(sudo=True, cmd=f"mount {targets[0]} {_dir}")
        LOG.info("Validation of CEPH-83576085 is successful.")
    except Exception as err:
        raise Exception(err)


def test_ceph_83576087(ceph_cluster, rbd, nvme_service, pool, config):
    """CEPH-83576087: Reboot client node and validate NVMe namespaces"""
    client = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = NVMeInitiator(client)
    try:
        # Deploy NVMe Service
        if config.get("install"):
            nvme_service.deploy()

        # Initialize gateways
        nvme_service.init_gateways()
        nvmegwcli = nvme_service.gateways[0]
        cmd_args = {
            "transport": "tcp",
            "traddr": nvmegwcli.node.ip_address,
            "trsvcid": config["initiators"][0]["listener_port"],
        }

        # Configure Subsystem, listeners, host, namespaces
        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd)

        json_format = {"output-format": "json"}
        _dir = f"/tmp/dir_{generate_unique_id(4)}"
        _file = f"{_dir}/test.log"

        initiator.disconnect_all()
        disc_port = {"trsvcid": 8009}
        _disc_cmd = {**cmd_args, **disc_port, **json_format}
        sub_nqns, _ = initiator.discover(**_disc_cmd)
        LOG.debug(sub_nqns)
        _cmd_args = deepcopy(cmd_args)
        for nqn in json.loads(sub_nqns)["records"]:
            if nqn["trsvcid"] == str(config["initiators"][0]["listener_port"]):
                _cmd_args["nqn"] = nqn["subnqn"]
                break
        else:
            raise Exception(f"Subsystem not found -- {cmd_args}")

        # Connect to the subsystem
        conn_port = {"trsvcid": config["initiators"][0]["listener_port"]}
        _conn_cmd = {**_cmd_args, **conn_port}
        LOG.debug(initiator.connect(**_conn_cmd))
        targets = initiator.list_devices()

        client.exec_command(sudo=True, cmd=f"mkdir {_dir}")
        client.exec_command(sudo=True, cmd=f"mkfs.ext4 {targets[0]}")
        client.exec_command(sudo=True, cmd=f"mount {targets[0]} {_dir}")
        client.exec_command(sudo=True, cmd=f"cp /var/log/messages {_file}")
        client.exec_command(sudo=True, cmd=f"ls -ltrh {_file}")

        # Reboot client node and re-mount the namespaces.
        # Discover and reconnect to subsystem and validate the files on the mount-points.
        if not reboot_node(client):
            raise Exception("Host did not started post reboot!!!!")
        initiator.configure()
        LOG.debug(initiator.connect(**_cmd_args))
        targets = initiator.list_devices()
        client.exec_command(sudo=True, cmd=f"mount {targets[0]} {_dir}")
        client.exec_command(sudo=True, cmd=f"ls -ltrh {_file}")
        LOG.info("Validation of CEPH-83576087 is successful.")
    except Exception as err:
        raise Exception(err)


def test_ceph_83576093(ceph_cluster, rbd, nvme_service, pool, config):
    """CEPH-83576093: Perform reboot on GW node and validate the namespaces."""
    client = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = NVMeInitiator(client)

    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    try:
        # Deploy NVMe Service
        if config.get("install"):
            nvme_service.deploy()

        # Initialize gateways
        nvme_service.init_gateways()
        nvmegwcli = nvme_service.gateways[0]
        cmd_args = {
            "transport": "tcp",
            "traddr": nvmegwcli.node.ip_address,
            "trsvcid": config["initiators"][0]["listener_port"],
        }

        # Configure Subsystem, listeners, host, namespaces
        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd)

        json_format = {"output-format": "json"}
        _dir = f"/tmp/dir_{generate_unique_id(4)}"
        _file = f"{_dir}/test.log"

        disc_port = {"trsvcid": 8009}
        _disc_cmd = {**cmd_args, **disc_port, **json_format}
        initiator.disconnect_all()
        sub_nqns, _ = initiator.discover(**_disc_cmd)
        LOG.debug(sub_nqns)
        _cmd_args = deepcopy(cmd_args)
        for nqn in json.loads(sub_nqns)["records"]:
            if nqn["trsvcid"] == str(config["initiators"][0]["listener_port"]):
                _cmd_args["nqn"] = nqn["subnqn"]
                break
        else:
            raise Exception(f"Subsystem not found -- {cmd_args}")

        # Connect to the subsystem
        conn_port = {"trsvcid": config["initiators"][0]["listener_port"]}
        _conn_cmd = {**_cmd_args, **conn_port}
        LOG.debug(initiator.connect(**_conn_cmd))
        targets = initiator.list_devices()

        client.exec_command(sudo=True, cmd=f"mkdir {_dir}")
        client.exec_command(sudo=True, cmd=f"mkfs.ext4 {targets[0]}")
        client.exec_command(sudo=True, cmd=f"mount {targets[0]} {_dir}")
        client.exec_command(sudo=True, cmd=f"cp /var/log/messages {_file}")
        client.exec_command(sudo=True, cmd=f"ls -ltrh {_file}")

        # Reboot NVMeoF GW node and wait for the node recovery.
        # Wait for the GW service to be up and running.
        if not reboot_node(gw_node):
            raise Exception("Host did not started post reboot!!!!!")

        service_name = f"nvmeof.{pool}"
        gw_group = config.get("gw_group")
        service_name = f"{service_name}.{gw_group}" if gw_group else service_name
        check_service_exists(
            ceph_cluster.get_nodes(role="installer")[0],
            service_name=service_name,
            service_type="nvmeof",
        )
        client.exec_command(sudo=True, cmd=f"ls -ltrh {_file}")
        client.exec_command(sudo=True, cmd=f"cp /var/log/messages {_file}_test")
        client.exec_command(sudo=True, cmd=f"ls -ltrh {_file}_test")
        client.exec_command(sudo=True, cmd=f"umount {_dir}")
        LOG.info("Validation of CEPH-83576093 is successful.")
    except Exception as err:
        raise Exception(err)


def test_ceph_83575814(ceph_cluster, rbd, nvme_service, pool, config):
    """CEPH-83575814: Perform cluster operations when  IO operations between
    NVMeOF target NVMe-OF initiator are in progress.
    Args:
        ceph_cluster (CephCluster): The Ceph cluster instance.
        rbd (RadosBlockDevice): The RBD instance.
        pool (str): The Ceph pool name.
        config (dict): Configuration parameters.

    Returns:
        int: 0 on success, 1 on failure.
    """

    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    mon_obj = MonitorWorkflows(node=cephadm)
    rados_obj = RadosOrchestrator(node=cephadm)
    client = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = NVMeInitiator(client)
    try:
        # Deploy NVMe Service
        if config.get("install"):
            nvme_service.deploy()

        # Initialize gateways
        nvme_service.init_gateways()
        nvmegwcli = nvme_service.gateways[0]

        # Configure Subsystem, listeners, host, namespaces
        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd)

        mon_host = ceph_cluster.get_nodes(role="mon")[0]
        with parallel() as p:
            for initiator in config["initiators"]:
                p.spawn(initiators, ceph_cluster, nvmegwcli, initiator)

            LOG.info("Removing mon service from the cluster")
            p.spawn(operation, mon_obj, "remove_mon_service", host=mon_host.hostname)

            LOG.info("Adding mon service back to the cluster")
            p.spawn(operation, mon_obj, "add_mon_service", host=mon_host)
            sleep(10)
            p.spawn(
                operation, mon_obj, "check_mon_exists_on_host", host=mon_host.hostname
            )

            LOG.info("Removing osd service and adding back to the cluster")
            p.spawn(
                osd_remove_and_add_back,
                ceph_cluster=ceph_cluster,
                rados_obj=rados_obj,
                pool=pool,
            )
        LOG.info("Validation of CEPH-83575814 is successful")
    except Exception as err:
        LOG.error(err)
        return 1


def test_ceph_83581753(ceph_cluster, rbd, nvme_service, pool, config):
    """CEPH-83581753: Set QoS for namespace in subsystem with invalid values
    for mandatory args and invalid args.
    Args:
        ceph_cluster (CephCluster): The Ceph cluster instance.
        rbd (RadosBlockDevice): The RBD instance.
        pool (str): The Ceph pool name.
        config (dict): Configuration parameters.
    Returns:
        int: 0 on success, 1 on failure.
    """
    try:
        # Deploy NVMe Service
        if config.get("install"):
            nvme_service.deploy()

        # Initialize gateways
        nvme_service.init_gateways()
        nvmegwcli = nvme_service.gateways[0]

        # Configure Subsystem, listeners, host, namespaces
        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd)

        list_args = {}
        list_args.setdefault("args", {}).update(
            {"subsystem": config["initiators"][0]["subnqn"]}
        )
        list_args.setdefault("base_cmd_args", {}).update({"format": "json"})

        namespaces, _ = nvmegwcli.namespace.list(**list_args)
        namespaces = json.loads(namespaces)
        nsid = namespaces.get("namespaces")[0].get("nsid")
        try:
            qos_args_with_invalid_values = {}
            qos_args_with_invalid_values.setdefault("args", {}).update(
                {
                    "nsid": 10,
                    "subsystem": config["initiators"][0]["subnqn"],
                    "rw-ios-per-second": 10,
                }
            )
            _ = nvmegwcli.namespace.set_qos(**qos_args_with_invalid_values)
        except Exception as err:
            if "Can't find namespace" not in str(err):
                raise Exception(
                    "Set QoS was failed as expected due to invalid namespace."
                )
            LOG.info("Set QoS was failed as expected due to invalid namespace....")
        try:
            qos_args_with_invalid_args = {}
            qos_args_with_invalid_args.setdefault("args", {}).update(
                {
                    "nsid": nsid,
                    "load-balancing-group": "4",
                    "subsystem": config["initiators"][0]["subnqn"],
                    "rw-ios-per-second": 10,
                }
            )
            _ = nvmegwcli.namespace.set_qos(**qos_args_with_invalid_args)
        except Exception as err:
            if "unrecognized arguments:" not in str(
                err
            ) and "invalid command" not in str(err):
                raise Exception("Set QoS was failed as expected due to invalid args.")
            LOG.info("Set QoS was failed as expected due to invalid args....")
    except Exception as err:
        LOG.error(err)
        return 1


def test_ceph_83581945(ceph_cluster, rbd, nvme_service, pool, config):
    """CEPH-83581945: Set QoS for namespace in subsystem without mandatory values and args.
    Args:
        ceph_cluster (CephCluster): The Ceph cluster instance.
        rbd (RadosBlockDevice): The RBD instance.
    pool (str): The Ceph pool name.
    config (dict): Configuration parameters.
    Returns:
        int: 0 on success, 1 on failure.
    """
    try:
        # Deploy NVMe Service
        if config.get("install"):
            nvme_service.deploy()

        # Initialize gateways
        nvme_service.init_gateways()
        nvmegwcli = nvme_service.gateways[0]

        # Configure Subsystem, listeners, host, namespaces
        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd)

        list_args = {}
        list_args.setdefault("args", {}).update(
            {"subsystem": config["initiators"][0]["subnqn"]}
        )
        list_args.setdefault("base_cmd_args", {}).update({"format": "json"})

        namespaces, _ = nvmegwcli.namespace.list(**list_args)
        namespaces = json.loads(namespaces)
        nsid = namespaces.get("namespaces")[0].get("nsid")
        try:
            qos_args_without_mandatory_args = {}
            qos_args_without_mandatory_args.setdefault("args", {}).update(
                {"nsid": nsid}
            )
            _ = nvmegwcli.namespace.set_qos(**qos_args_without_mandatory_args)
        except Exception as err:
            if "the following arguments are required" not in str(err):
                raise Exception(
                    "Set QoS was failed as expected due to absence of mandatory args."
                )
            LOG.info(
                "Set QoS was failed as expected due to absence of mandatory args...."
            )
        try:
            qos_args_without_mandatory_args = {}
            qos_args_without_mandatory_args.setdefault("args", {}).update(
                {
                    "rw-ios-per-second": 10,
                    "subsystem": config["initiators"][0]["subnqn"],
                }
            )
            _ = nvmegwcli.namespace.set_qos(**qos_args_without_mandatory_args)
        except Exception as err:
            if (
                "At least one of --nsid or --uuid arguments is mandatory for set_qos command"
                not in str(err)
            ):
                raise Exception(
                    "Set QoS was failed as expected due to absence of mandatory args."
                )
            LOG.info(
                "Set QoS was failed as expected due to absence of mandatory args...."
            )
        try:
            namespace_delete_args = {}
            namespace_delete_args.setdefault("args", {}).update(
                {"subsystem": config["initiators"][0]["subnqn"], "nsid": 1}
            )
            nvmegwcli.namespace.delete(**namespace_delete_args)
            qos_args_without_mandatory_args.setdefault("args", {}).update(
                {
                    "rw-ios-per-second": 10,
                    "subsystem": config["initiators"][0]["subnqn"],
                    "nsid": 1,
                }
            )
            _ = nvmegwcli.namespace.set_qos(**qos_args_without_mandatory_args)
        except Exception as err:
            if "Can't find namespace" not in str(err):
                raise Exception(
                    "Set QoS was failed as expected due to absence of namespace."
                )
            LOG.info("Set QoS was failed as expected due to absence of namespace....")
    except Exception as err:
        LOG.error(err)
        return 1


def test_ceph_83581755(ceph_cluster, rbd, nvme_service, pool, config):
    """CEPH-83581755: Set QoS for multiple namespaces.
    Args:
        ceph_cluster (CephCluster): The Ceph cluster instance.
        rbd (RadosBlockDevice): The RBD instance.
    pool (str): The Ceph pool name.
    config (dict): Configuration parameters.
    Returns:
        int: 0 on success, 1 on failure.
    """
    try:
        # Deploy NVMe Service
        if config.get("install"):
            nvme_service.deploy()

        # Initialize gateways
        nvme_service.init_gateways()
        nvmegwcli = nvme_service.gateways[0]

        # Configure Subsystem, listeners, host, namespaces
        if config.get("subsystems"):
            configure_gw_entities(nvme_service, rbd_obj=rbd)

        list_args = {}
        list_args.setdefault("args", {}).update(
            {"subsystem": config["initiators"][0]["subnqn"]}
        )
        list_args.setdefault("base_cmd_args", {}).update({"format": "json"})
        nsid = []
        namespaces, _ = nvmegwcli.namespace.list(**list_args)
        list_namespaces = json.loads(namespaces)
        for i in range(2):
            nsid.append(list_namespaces.get("namespaces")[i].get("nsid"))

        qos_args_without_mandatory_args = {}
        qos_args_without_mandatory_args.setdefault("args", {}).update(
            {
                "nsid": f"{nsid[0]},{nsid[1]}",
                "subsystem": config["initiators"][0]["subnqn"],
                "rw-ios-per-second": 10,
            }
        )
        _ = nvmegwcli.namespace.set_qos(**qos_args_without_mandatory_args)
    except Exception as err:
        if "invalid literal for int() with base 10: '1,2" in str(
            err
        ) or "argument --nsid: invalid int value:" in str(err):
            LOG.info("Set QoS was failed as expected due to invalid namespace value.")
        else:
            raise Exception(
                "Set QoS was failed with an exception which is not expected: "
                + str(err)
            )
            return 1


def test_ceph_83608266(
    ceph_cluster, rbd, pool, config, placement_cfg, nvme_service, executor, io_tasks
):
    """CEPH-83608266: Deploy same GW node in different GWgroup.
    Returns:
        int: 0 on success, 1 on failure.
    """
    admin_node = ceph_cluster.get_nodes(role="installer")[0]
    conf = {}
    clients = []

    def string_to_dict(string):
        """Parse ANA states from the string."""
        states = string.replace(" ", "").split(",")
        dict = {}
        for state in states:
            if not state:
                continue
            _id, _state = state.split(":")
            dict[int(_id)] = _state
        return dict

    def deploy_nvme_service(conf, gw_node, nvmegwcli, nvme_service, redeploy=False):
        states = {}
        container_ids = []
        subsystem_list = []
        subsys_nqn = conf["nqn"]
        gw_nodes = get_nodes_by_ids(ceph_cluster, config.get("gw_nodes"))
        setup_firewalld(gw_nodes)
        if conf["placement"] == "apply_spec":
            cfg = {
                "no_cluster_state": False,
                "config": {
                    "command": "apply_spec",
                    "service": "nvmeof",
                    "validate-spec-services": False,
                    "specs": [
                        {
                            "service_type": "nvmeof",
                            "service_id": "rbd",
                            "placement": {"nodes": [i.hostname for i in gw_nodes]},
                            "spec": {
                                "pool": "rbd",
                                "enable_auth": False,
                            },
                        }
                    ],
                },
            }
            gw_group = conf["pos_args"][-1]
            cfg["config"]["specs"][0]["service_id"] = f"rbd.{gw_group}"
            cfg["config"]["specs"][0]["spec"]["group"] = gw_group
            test_nvmeof.run(ceph_cluster, **cfg)
        else:
            del conf["nqn"]
            CephAdm(admin_node).ceph.orch.apply(service_name="nvmeof", **conf)
        out = CephAdm(admin_node).ceph.orch.ls(service_type="nvmeof", format="json")
        service_details = json.loads(out)[0]

        if "service was created" not in service_details.get("events", [""])[-1]:
            raise RuntimeError("NVMe service deployment failed.")

        time.sleep(60)

        if not redeploy:
            running_containers, _ = get_running_containers(
                gw_node, format="json", expr="name=nvmeof", sudo=True
            )
            container_ids = [
                item.get("Names")[0] for item in json.loads(running_containers)
            ]

            subsystem = {
                "nqn": subsys_nqn,
                "serial": 83581755,
                "listener_port": 5003,
                "allow_host": "*",
            }

            nvme_service.group = "gw_group1"
            nvmegwcli.gateway_group = "gw_group1"
            nvme_service.init_gateways()

            # Configure Subsystem, listeners, host, namespaces
            if config.get("subsystems"):
                configure_subsystems(nvme_service)
                configure_listeners(nvme_service.gateways, nvme_service.config)
                configure_hosts(
                    nvme_service.gateways[0],
                    nvme_service.config,
                )

            for _ in range(2):
                name = generate_unique_id(length=4)
                img = f"{name}-image"
                rbd.create_image(pool, img, "5G")
                ns_args = {
                    "args": {
                        "subsystem": subsystem["nqn"],
                        "rbd-pool": pool,
                        "rbd-image": img,
                    }
                }
                nvmegwcli.namespace.add(**ns_args)

            subsystem_list_bkp, _ = nvmegwcli.subsystem.list(
                base_cmd_args={"format": "json"}
            )
            subsystem_list = json.loads(subsystem_list_bkp.strip())["subsystems"]
            states = collect_nvme_gateway_states(pool, {})

            return subsystem_list, states, container_ids

    def validate_redeployment_effects(
        initial_subsystems, initial_states, initial_containers, gw_node, nvmegwcli, pool
    ):
        updated_subsystems_raw, _ = nvmegwcli.subsystem.list(
            base_cmd_args={"format": "json"}
        )
        updated_subsystems = json.loads(updated_subsystems_raw.strip())["subsystems"]

        updated_states = collect_nvme_gateway_states(pool, {})
        running_containers, _ = get_running_containers(
            gw_node, format="json", expr="name=nvmeof", sudo=True
        )
        updated_container_ids = [
            item.get("Names")[0] for item in json.loads(running_containers)
        ]

        if not validate_subsystems(
            {"subsystems": initial_subsystems}, {"subsystems": updated_subsystems}
        ):
            raise Exception("Subsystems changed after redeployment.")

        if initial_states != updated_states:
            raise Exception("ANA states changed after redeployment.")

        if initial_containers != updated_container_ids:
            raise Exception("Container IDs changed after redeployment.")

        LOG.info("Redeployment validation successful.")

    def validate_subsystems(initial_output: dict, updated_output: dict):
        def get_subsystem_map(subsystems):
            return {
                s["nqn"]: set(ns["nsid"] for ns in s.get("namespaces", []))
                for s in subsystems.get("subsystems", [])
            }

        initial_map = get_subsystem_map(initial_output)
        updated_map = get_subsystem_map(updated_output)

        for nqn, nsids in initial_map.items():
            if nqn not in updated_map:
                return False
            if updated_map[nqn] != nsids:
                return False

        LOG.info("All subsystems and namespaces verified intact after redeployment.")
        return True

    def collect_nvme_gateway_states(pool, states):
        out = CephAdmin(cluster=ceph_cluster, **config).shell(
            args=["ceph", "nvme-gw", "show", pool, "gw_group1"]
        )
        out = json.loads(out[0])
        if out.get("Created Gateways:"):
            for gateway in out["Created Gateways:"]:
                gw = gateway["gw-id"]
                states[gw] = gateway
                states[gw].update((string_to_dict(gateway["ana states"])))
        return states

    def resolve_placement(ceph_cluster, placement_cfg):
        nodes = placement_cfg.get("nodes", ["node5", "node6"])
        node_ids = get_nodes_by_ids(ceph_cluster, nodes)
        placement = repr(" ".join([node.hostname for node in node_ids]))
        node = get_node_by_id(ceph_cluster, nodes[0])
        if "label" in placement_cfg:
            label = placement_cfg["label"]
            placement = f"label:{label}"
        if "apply_spec" in placement_cfg:
            placement = "apply_spec"
        return placement, node

    try:
        LOG.info(f"Deploying NVMe service via placement {placement_cfg}")

        conf["placement"], gw_node = resolve_placement(ceph_cluster, placement_cfg)

        if conf.get("pos_args"):
            conf["pos_args"].clear()

        conf["pos_args"] = list(config.get("pos_args", []))
        conf["pos_args"].append("gw_group1")
        conf["nqn"] = config["subsystems"][0]["nqn"]
        LOG.info("Deploying NVMe service in gw_group1")
        _cls = nvme_gw_cli_version_adapter(ceph_cluster)
        ceph = Orch(ceph_cluster, **{})

        args = {
            "shell": getattr(ceph, "shell"),
            "port": config.get("port", 5500),
        }
        nvmegwcli = _cls(gw_node, **args)
        initial_subsystems, initial_states, initial_containers = deploy_nvme_service(
            conf, gw_node, nvmegwcli, nvme_service, False
        )

        # Start IO Execution

        for io_client in config["initiators"]:
            node = get_node_by_id(ceph_cluster, io_client["node"])
            gateway = create_gateway(
                _cls,
                gw_node,
                shell=getattr(ceph, "shell"),
                port=config.get("port", 5500),
                gw_group=config.get("gw_group"),
            )
            client = NVMeInitiator(node)
            client.connect_targets(gateway, io_client)
            clients.append(client)

        for initiator in clients:
            io_tasks.append(executor.submit(initiator.start_fio))
        time.sleep(20)

        LOG.info(f"Deploying NVMe service using {placement_cfg} in gw_group2")
        conf["pos_args"][-1] = "gw_group2"
        conf["nqn"] = config["subsystems"][0]["nqn"]
        deploy_nvme_service(conf, gw_node, nvmegwcli, nvme_service, True)
        validate_redeployment_effects(
            initial_subsystems,
            initial_states,
            initial_containers,
            gw_node,
            nvmegwcli,
            pool,
        )
        LOG.info("Test completed successfully, which is a failure condition.")
    except Exception as e:
        LOG.info(
            f"Deployment failed as you are trying to have same GW node in different GWgroups: {e}"
        )
    finally:
        nvme_service.config["gw_node"] = gw_node.id
        teardown(nvme_service, rbd)
        nvme_service.group = "gw_group2"
        rc = nvme_service.delete_nvme_service()
        if rc != 0:
            LOG.warning("Failed to delete NVMe gateways")
        return 0


def run(ceph_cluster: Ceph, **kwargs) -> int:
    """Return the status of the Ceph NVMEof test execution.

    - Configure SPDK and install with control interface.
    - Configures Initiators and Run FIO on NVMe targets.
    - Runs Image operations and validate the results

    Args:
        ceph_cluster: Ceph cluster object
        kwargs: Key/value pairs of configuration information to be used in the test.

    Returns:
        int - 0 when the execution is successful else 1 (for failure).

    Example:

        # Execute the nvmeof GW test
            - test:
                name: Ceph NVMeoF imaage operation test
                desc: validate RBD image operations on NVMe devices
                config:
                    gw_node: node6
                    operation: remove

    """
    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    custom_config = kwargs.get("test_data", {}).get("custom-config")
    executor = ThreadPoolExecutor()
    io_tasks = []
    check_and_set_nvme_cli_image(ceph_cluster, config=custom_config)
    try:
        nvme_service = NVMeService(config, ceph_cluster)

        operation_mapping = {
            "CEPH-83575812": test_ceph_83575812,
            "CEPH-83576084": test_ceph_83576084,
            "CEPH-83575467": test_ceph_83575467,
            "CEPH-83576085": test_ceph_83576085,
            "CEPH-83576087": test_ceph_83576087,
            "CEPH-83575813": test_ceph_83575813,
            "CEPH-83576093": test_ceph_83576093,
            "CEPH-83575455": test_ceph_83575455,
            "CEPH-83575814": test_ceph_83575814,
            "CEPH-83581753": test_ceph_83581753,
            "CEPH-83581945": test_ceph_83581945,
            "CEPH-83581755": test_ceph_83581755,
        }
        operation = config["operation"]
        if operation in operation_mapping:
            rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
            operation_mapping[operation](
                ceph_cluster, rbd_obj, nvme_service, rbd_pool, config
            )
        else:
            if operation == "CEPH-83608266":
                placements = config["args"]["placement_options"]
                for placement_cfg in placements:
                    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
                    test_ceph_83608266(
                        ceph_cluster,
                        rbd_obj,
                        rbd_pool,
                        config,
                        placement_cfg,
                        nvme_service,
                        executor,
                        io_tasks,
                    )

        return 0

    except Exception as err:
        LOG.error(err)

    finally:
        if io_tasks:
            LOG.info("Waiting for completion of IOs.")
            executor.shutdown(wait=False, cancel_futures=True)
        if config.get("cleanup") and operation != "CEPH-83608266":
            teardown(nvme_service, rbd_obj)
    return 1
