"""
Test suite that verifies the deployment of Ceph NVMeoF Gateway
 with supported entities like subsystems , etc.,

"""

import json
from copy import deepcopy
from time import sleep

from ceph.ceph import Ceph, SocketTimeoutException
from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.helper import check_service_exists
from ceph.parallel import parallel
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.monitor_workflows import MonitorWorkflows
from ceph.rbd.workflows.cluster_operations import operation, osd_remove_and_add_back
from ceph.utils import get_node_by_id
from cli.utilities.utils import reboot_node
from tests.cephadm import test_orch
from tests.nvmeof.test_ceph_nvmeof_gateway import initiators
from tests.nvmeof.workflows.gateway_entities import configure_gw_entities, teardown
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image
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
    client = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = NVMeInitiator(client)
    initiator_nqn = initiator.initiator_nqn()
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

        for i in config["initiators"]:
            initiators(ceph_cluster, nvmegwcli, i)

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
            host_args = {
                "args": {
                    "subsystem": config["initiators"][0]["subnqn"],
                    "host": initiator_nqn,
                }
            }
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
        if "argument --nsid: invalid int value:" not in str(err):
            raise Exception(
                "Set QoS was failed as expected due to invalid namespace value."
            )
            return 1
        LOG.info("Set QoS was failed as expected due to invalid namespace value....")


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

        return 0

    except Exception as err:
        LOG.error(err)

    finally:
        if config.get("cleanup"):
            teardown(nvme_service, rbd_obj)
    return 1
