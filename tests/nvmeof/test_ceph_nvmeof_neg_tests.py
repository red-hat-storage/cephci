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
from ceph.nvmeof.initiator import Initiator
from ceph.nvmeof.nvmeof_gwcli import NVMeCLI, find_client_daemon_id
from ceph.parallel import parallel
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.monitor_workflows import MonitorWorkflows
from ceph.rbd.workflows.cluster_operations import operation, osd_remove_and_add_back
from ceph.utils import get_node_by_id
from cli.utilities.utils import reboot_node
from tests.cephadm import test_nvmeof, test_orch
from tests.nvmeof.test_ceph_nvmeof_gateway import (
    configure_subsystems,
    initiators,
    teardown,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import find_free_port, generate_unique_id

LOG = Log(__name__)


def test_ceph_83575812(ceph_cluster, rbd, pool, config):
    """CEPH-83575812 Remove the image during NVMe images are in use."""
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    gateway = NVMeCLI(gw_node)
    listener_port = find_free_port(gw_node)
    subsystem = dict()
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:cnode_negative",
            "serial": 114,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )

    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:cnode_negative",
        "listener_port": listener_port,
        "node": config["initiator_node"],
    }
    try:
        subsystem["gateway-name"] = find_client_daemon_id(
            ceph_cluster, pool, node_name=gw_node.hostname
        )
        configure_subsystems(rbd, pool, gateway, subsystem)
        name = generate_unique_id(length=4)

        # Create image
        img = f"{name}-image"
        rbd.create_image(pool, img, "1G")
        gateway.create_block_device(img, img, pool)
        gateway.add_namespace(subsystem["nqn"], img)

        config.update(initiator_cfg)
        with parallel() as p:
            p.spawn(initiators, ceph_cluster, gateway, initiator_cfg)
            sleep(20)
            out, err = rbd.remove_image(pool, img, **{"all": True, "check_ec": False})
            if "rbd: error: image still has watchers" not in out + err:
                raise Exception("RBD image removed when its in use.")
            LOG.info("RBD image removal failed as expected when its in use....")
    except Exception as err:
        raise Exception(err)
    finally:
        cleanup_cfg = {
            "gw_node": config["gw_node"],
            "initiators": [initiator_cfg],
            "cleanup": ["initiators", "gateway", "pool"],
            "rbd_pool": pool,
        }
        teardown(ceph_cluster, rbd, cleanup_cfg)


def test_ceph_83576084(ceph_cluster, rbd, pool, config):
    """CEPH-83576084: Delete-recreate bdev in loop and rediscover namespace."""
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    gateway = NVMeCLI(gw_node)

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph_83576084",
            "serial": 112,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )
    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph_83576084",
        "listener_port": listener_port,
        "node": config["initiator_node"],
    }
    try:
        subsystem["gateway-name"] = find_client_daemon_id(
            ceph_cluster, pool, node_name=gw_node.hostname
        )
        configure_subsystems(rbd, pool, gateway, subsystem)
        name = generate_unique_id(length=4)

        # Create image
        img = f"{name}-image"
        rbd.create_image(pool, img, "1G")
        gateway.create_block_device(img, img, pool)
        gateway.add_namespace(subsystem["nqn"], img)

        config.update(initiator_cfg)
        client = get_node_by_id(ceph_cluster, config["node"])
        initiator = Initiator(client)
        cmd_args = {
            "transport": "tcp",
            "traddr": gateway.node.ip_address,
        }

        json_format = {"output-format": "json"}
        _dir = f"/tmp/dir_{generate_unique_id(4)}"
        _file = f"{_dir}/test.log"

        def check_client(verify=False):
            disc_port = {"trsvcid": listener_port}
            _disc_cmd = {**cmd_args, **disc_port, **json_format}
            initiator.disconnect_all()
            sub_nqns, _ = initiator.discover(**_disc_cmd)
            LOG.debug(sub_nqns)
            _cmd_args = deepcopy(cmd_args)
            for nqn in json.loads(sub_nqns)["records"]:
                if nqn["trsvcid"] == listener_port:
                    _cmd_args["nqn"] = nqn["subnqn"]
                    break
            else:
                raise Exception(f"Subsystem not found -- {cmd_args}")

            # Connect to the subsystem
            conn_port = {"trsvcid": listener_port}
            _conn_cmd = {**_cmd_args, **conn_port}
            LOG.debug(initiator.connect(**_conn_cmd))
            targets = initiator.list_spdk_drives()
            if not targets:
                raise Exception(f"NVMe Targets not found on {client.hostname}")
            _target = targets[0]["DevicePath"]
            if not verify:
                return _target
            client.exec_command(sudo=True, cmd=f"mount {_target} {_dir}")
            client.exec_command(sudo=True, cmd=f"ls -ltrh {_file}")

        target = check_client()
        client.exec_command(sudo=True, cmd=f"mkdir {_dir}")
        client.exec_command(sudo=True, cmd=f"mkfs.ext4 {target}")
        client.exec_command(sudo=True, cmd=f"mount {target} {_dir}")
        client.exec_command(sudo=True, cmd=f"cp /var/log/messages {_file}")

        for _ in "check":
            client.exec_command(sudo=True, cmd=f"umount {_dir}")
            gateway.remove_namespace(subsystem["nqn"], img)
            gateway.add_namespace(subsystem["nqn"], img)
            check_client(verify=True)

        LOG.info("Validation of CEPH-83576084 is successful.")
    except Exception as err:
        raise Exception(err)
    finally:
        cleanup_cfg = {
            "gw_node": config["gw_node"],
            "initiators": [initiator_cfg],
            "cleanup": ["initiators", "gateway", "pool"],
            "rbd_pool": pool,
        }
        teardown(ceph_cluster, rbd, cleanup_cfg)


def test_ceph_83575467(ceph_cluster, rbd, pool, config):
    """CEPH-83575467: Perform restart and validate the gateway entities"""
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    gateway = NVMeCLI(gw_node)

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph_83575467",
            "serial": 111,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )
    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph_83575467",
        "listener_port": listener_port,
        "node": config["initiator_node"],
    }
    try:
        subsystem["gateway-name"] = find_client_daemon_id(
            ceph_cluster, pool, node_name=gw_node.hostname
        )
        configure_subsystems(rbd, pool, gateway, subsystem)
        name = generate_unique_id(length=4)

        # Create images
        for i in range(5):
            img = f"{name}-image{i}"
            rbd.create_image(pool, img, "500M")
            gateway.create_block_device(img, img, pool)
            gateway.add_namespace(subsystem["nqn"], img)

        config.update(initiator_cfg)
        initiators(ceph_cluster, gateway, initiator_cfg)
        _, gw_info_bkp = gateway.get_subsystems()
        gw_info_bkp = json.loads(gw_info_bkp.split("\n", 1)[1])

        # restart nvmeof service
        restart_cfg = {
            "config": {
                "service": f"nvmeof.{pool}",
                "command": "restart",
                "args": {"verify": True},
                "pos_args": [f"nvmeof.{pool}"],
            }
        }
        test_orch.run(ceph_cluster, **restart_cfg)
        _, gw_info = gateway.get_subsystems()
        gw_info = json.loads(gw_info.split("\n", 1)[1])

        if gw_info != gw_info_bkp:
            raise Exception(
                f"GW Entities aren't same. Actual: {gw_info_bkp} Current: {gw_info}"
            )
        LOG.info("Validation of Ceph-83575467 is successful.")
    except Exception as err:
        raise Exception(err)
    finally:
        cleanup_cfg = {
            "gw_node": config["gw_node"],
            "initiators": [initiator_cfg],
            "cleanup": ["initiators", "gateway", "pool"],
            "rbd_pool": pool,
        }
        teardown(ceph_cluster, rbd, cleanup_cfg)


def test_ceph_83576085(ceph_cluster, rbd, pool, config):
    """CEPH-83576085: Perform map and unmap NVMe namespaces in loop."""
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    gateway = NVMeCLI(gw_node)

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph_83576085",
            "serial": 113,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )

    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph_83576085",
        "listener_port": listener_port,
        "node": config["initiator_node"],
    }

    cmd_args = {
        "transport": "tcp",
        "traddr": gateway.node.ip_address,
        "trsvcid": listener_port,
    }

    json_format = {"output-format": "json"}
    _dir = f"/tmp/dir_{generate_unique_id(4)}"
    _file = f"{_dir}/test.log"

    try:
        subsystem["gateway-name"] = find_client_daemon_id(
            ceph_cluster, pool, node_name=gw_node.hostname
        )
        configure_subsystems(rbd, pool, gateway, subsystem)
        name = generate_unique_id(length=4)

        # Create image
        img = f"{name}-image"
        rbd.create_image(pool, img, "1G")
        gateway.create_block_device(img, img, pool)
        gateway.add_namespace(subsystem["nqn"], img)

        config.update(initiator_cfg)
        client = get_node_by_id(ceph_cluster, config["node"])
        initiator = Initiator(client)

        disc_port = {"trsvcid": listener_port}
        _disc_cmd = {**cmd_args, **disc_port, **json_format}
        initiator.disconnect_all()
        sub_nqns, _ = initiator.discover(**_disc_cmd)
        LOG.debug(sub_nqns)
        _cmd_args = deepcopy(cmd_args)
        for nqn in json.loads(sub_nqns)["records"]:
            if nqn["trsvcid"] == listener_port:
                _cmd_args["nqn"] = nqn["subnqn"]
                break
        else:
            raise Exception(f"Subsystem not found -- {cmd_args}")

        # Connect to the subsystem
        conn_port = {"trsvcid": listener_port}
        _conn_cmd = {**_cmd_args, **conn_port}
        LOG.debug(initiator.connect(**_conn_cmd))
        targets = initiator.list_spdk_drives()
        if not targets:
            raise Exception(f"NVMe Targets not found on {client.hostname}")
        _target = targets[0]["DevicePath"]

        client.exec_command(sudo=True, cmd=f"mkdir {_dir}")
        client.exec_command(sudo=True, cmd=f"mkfs.ext4 {_target}")
        client.exec_command(sudo=True, cmd=f"mount {_target} {_dir}")
        client.exec_command(sudo=True, cmd=f"cp /var/log/messages {_file}")

        for _ in "check":
            client.exec_command(sudo=True, cmd=f"ls -ltrh {_file}")
            client.exec_command(sudo=True, cmd=f"umount {_dir}")
            client.exec_command(sudo=True, cmd=f"mount {_target} {_dir}")
        LOG.info("Validation of CEPH-83576085 is successful.")
    except Exception as err:
        raise Exception(err)
    finally:
        cleanup_cfg = {
            "gw_node": config["gw_node"],
            "initiators": [initiator_cfg],
            "cleanup": ["initiators", "gateway", "pool"],
            "rbd_pool": pool,
        }
        teardown(ceph_cluster, rbd, cleanup_cfg)


def test_ceph_83576087(ceph_cluster, rbd, pool, config):
    """CEPH-83576087: Reboot client node and validate NVMe namespaces"""
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    gateway = NVMeCLI(gw_node)

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph_83576087",
            "serial": 113,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )

    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph_83576087",
        "listener_port": listener_port,
        "node": config["initiator_node"],
    }

    cmd_args = {
        "transport": "tcp",
        "traddr": gateway.node.ip_address,
        "trsvcid": listener_port,
    }

    json_format = {"output-format": "json"}
    _dir = f"/tmp/dir_{generate_unique_id(4)}"
    _file = f"{_dir}/test.log"

    try:
        subsystem["gateway-name"] = find_client_daemon_id(
            ceph_cluster, pool, node_name=gw_node.hostname
        )
        configure_subsystems(rbd, pool, gateway, subsystem)
        name = generate_unique_id(length=4)

        # Create image
        img = f"{name}-image"
        rbd.create_image(pool, img, "1G")
        gateway.create_block_device(img, img, pool)
        gateway.add_namespace(subsystem["nqn"], img)

        config.update(initiator_cfg)
        client = get_node_by_id(ceph_cluster, config["node"])
        initiator = Initiator(client)

        initiator.disconnect_all()
        disc_port = {"trsvcid": listener_port}
        _disc_cmd = {**cmd_args, **disc_port, **json_format}
        sub_nqns, _ = initiator.discover(**_disc_cmd)
        LOG.debug(sub_nqns)
        _cmd_args = deepcopy(cmd_args)
        for nqn in json.loads(sub_nqns)["records"]:
            if nqn["trsvcid"] == str(config["listener_port"]):
                _cmd_args["nqn"] = nqn["subnqn"]
                break
        else:
            raise Exception(f"Subsystem not found -- {cmd_args}")

        # Connect to the subsystem
        conn_port = {"trsvcid": listener_port}
        _conn_cmd = {**_cmd_args, **conn_port}
        LOG.debug(initiator.connect(**_conn_cmd))
        targets = initiator.list_spdk_drives()
        if not targets:
            raise Exception(f"NVMe Targets not found on {client.hostname}")
        _target = targets[0]["DevicePath"]

        client.exec_command(sudo=True, cmd=f"mkdir {_dir}")
        client.exec_command(sudo=True, cmd=f"mkfs.ext4 {_target}")
        client.exec_command(sudo=True, cmd=f"mount {_target} {_dir}")
        client.exec_command(sudo=True, cmd=f"cp /var/log/messages {_file}")
        client.exec_command(sudo=True, cmd=f"ls -ltrh {_file}")

        # Reboot client node and re-mount the namespaces.
        # Discover and reconnect to subsystem and validate the files on the mount-points.
        if not reboot_node(client):
            raise Exception("Host did not started post reboot!!!!")
        initiator.configure()
        LOG.debug(initiator.connect(**_cmd_args))
        targets = initiator.list_spdk_drives()
        if not targets:
            raise Exception(f"NVMe Targets not found on {client.hostname}")
        _target = targets[0]["DevicePath"]
        client.exec_command(sudo=True, cmd=f"mount {_target} {_dir}")
        client.exec_command(sudo=True, cmd=f"ls -ltrh {_file}")
        LOG.info("Validation of CEPH-83576087 is successful.")
    except Exception as err:
        raise Exception(err)
    finally:
        cleanup_cfg = {
            "gw_node": config["gw_node"],
            "initiators": [initiator_cfg],
            "cleanup": ["initiators", "gateway", "pool"],
            "rbd_pool": pool,
        }
        teardown(ceph_cluster, rbd, cleanup_cfg)


def test_ceph_83576093(ceph_cluster, rbd, pool, config):
    """CEPH-83576093: Perform reboot on GW node and validate the namespaces."""
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    gateway = NVMeCLI(gw_node)

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph_83576093",
            "serial": 113,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )
    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph_83576093",
        "listener_port": listener_port,
        "node": config["initiator_node"],
    }

    try:
        subsystem["gateway-name"] = find_client_daemon_id(
            ceph_cluster, pool, node_name=gw_node.hostname
        )
        configure_subsystems(rbd, pool, gateway, subsystem)
        name = generate_unique_id(length=4)

        # Create image
        img = f"{name}-image"
        rbd.create_image(pool, img, "1G")
        gateway.create_block_device(img, img, pool)
        gateway.add_namespace(subsystem["nqn"], img)

        config.update(initiator_cfg)
        client = get_node_by_id(ceph_cluster, config["node"])
        initiator = Initiator(client)
        cmd_args = {
            "transport": "tcp",
            "traddr": gateway.node.ip_address,
            "trsvcid": listener_port,
        }

        json_format = {"output-format": "json"}
        _dir = f"/tmp/dir_{generate_unique_id(4)}"
        _file = f"{_dir}/test.log"

        disc_port = {"trsvcid": listener_port}
        _disc_cmd = {**cmd_args, **disc_port, **json_format}
        initiator.disconnect_all()
        sub_nqns, _ = initiator.discover(**_disc_cmd)
        LOG.debug(sub_nqns)
        _cmd_args = deepcopy(cmd_args)
        for nqn in json.loads(sub_nqns)["records"]:
            if nqn["trsvcid"] == listener_port:
                _cmd_args["nqn"] = nqn["subnqn"]
                break
        else:
            raise Exception(f"Subsystem not found -- {cmd_args}")

        # Connect to the subsystem
        conn_port = {"trsvcid": listener_port}
        _conn_cmd = {**_cmd_args, **conn_port}
        LOG.debug(initiator.connect(**_conn_cmd))
        targets = initiator.list_spdk_drives()
        if not targets:
            raise Exception(f"NVMe Targets not found on {client.hostname}")
        _target = targets[0]["DevicePath"]

        client.exec_command(sudo=True, cmd=f"mkdir {_dir}")
        client.exec_command(sudo=True, cmd=f"mkfs.ext4 {_target}")
        client.exec_command(sudo=True, cmd=f"mount {_target} {_dir}")
        client.exec_command(sudo=True, cmd=f"cp /var/log/messages {_file}")
        client.exec_command(sudo=True, cmd=f"ls -ltrh {_file}")

        # Reboot NVMeoF GW node and wait for the node recovery.
        # Wait for the GW service to be up and running.
        if not reboot_node(gw_node):
            raise Exception("Host did not started post reboot!!!!!")

        check_service_exists(
            ceph_cluster.get_nodes(role="installer")[0],
            service_name=f"nvmeof.{pool}",
            service_type="nvmeof",
        )
        client.exec_command(sudo=True, cmd=f"ls -ltrh {_file}")
        client.exec_command(sudo=True, cmd=f"cp /var/log/messages {_file}_test")
        client.exec_command(sudo=True, cmd=f"ls -ltrh {_file}_test")
        client.exec_command(sudo=True, cmd=f"umount {_dir}")
        LOG.info("Validation of CEPH-83576093 is successful.")
    except Exception as err:
        raise Exception(err)
    finally:
        cleanup_cfg = {
            "gw_node": config["gw_node"],
            "initiators": [initiator_cfg],
            "cleanup": ["initiators", "gateway", "pool"],
            "rbd_pool": pool,
        }
        teardown(ceph_cluster, rbd, cleanup_cfg)


def test_ceph_83575455(ceph_cluster, rbd, pool, config):
    """CEPH-83575455: Validate Host access failures"""
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    gateway = NVMeCLI(gw_node)
    client = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = Initiator(client)
    initiator_nqn = initiator.nqn()

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph_83575455",
            "serial": 113,
            "listener_port": listener_port,
            "allow_host": initiator_nqn,
        }
    )
    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph_83575455",
        "listener_port": listener_port,
        "node": config["initiator_node"],
    }

    _dir = f"/tmp/dir_{generate_unique_id(4)}"
    _file = f"{_dir}/test.log"

    try:
        subsystem["gateway-name"] = find_client_daemon_id(
            ceph_cluster, pool, node_name=gw_node.hostname
        )
        configure_subsystems(rbd, pool, gateway, subsystem)
        name = generate_unique_id(length=4)

        # Create image
        img = f"{name}-image"
        rbd.create_image(pool, img, "5G")
        gateway.create_block_device(img, img, pool)
        gateway.add_namespace(subsystem["nqn"], img)

        config.update(initiator_cfg)

        cmd_args = {
            "transport": "tcp",
            "traddr": gateway.node.ip_address,
            "trsvcid": listener_port,
        }

        json_format = {"output-format": "json"}
        disc_port = {"trsvcid": listener_port}
        _disc_cmd = {**cmd_args, **disc_port, **json_format}
        initiator.disconnect_all()
        sub_nqns, _ = initiator.discover(**_disc_cmd)
        LOG.debug(sub_nqns)
        _cmd_args = deepcopy(cmd_args)
        for nqn in json.loads(sub_nqns)["records"]:
            if nqn["trsvcid"] == listener_port:
                _cmd_args["nqn"] = nqn["subnqn"]
                break
        else:
            raise Exception(f"Subsystem not found -- {cmd_args}")

        # Connect to the subsystem
        conn_port = {"trsvcid": listener_port}
        _conn_cmd = {**_cmd_args, **conn_port}
        LOG.debug(initiator.connect(**_conn_cmd))
        targets = initiator.list_spdk_drives()
        if not targets:
            raise Exception(f"NVMe Targets not found on {client.hostname}")
        _target = targets[0]["DevicePath"]

        client.exec_command(sudo=True, cmd=f"mkdir {_dir}")
        client.exec_command(sudo=True, cmd=f"mkfs.ext4 {_target}")
        client.exec_command(sudo=True, cmd=f"mount {_target} {_dir}")
        client.exec_command(sudo=True, cmd=f"cp /var/log/messages {_file}")
        client.exec_command(sudo=True, cmd=f"ls -ltrh {_file}")

        # Remove client host access to the namespaces
        # Check for the non-existence of nvme namespaces
        # Create a file to check IO failure on mount point
        gateway.remove_host(subnqn=subsystem["nqn"], hostnqn=initiator_nqn)
        sleep(20)
        targets = initiator.list_spdk_drives()
        if targets:
            raise Exception(f"NVMe Targets found on {client.hostname}!!!")
        LOG.info(f"NVMe targets not found on {client.hostname} as expected..")
        try:
            client.exec_command(
                sudo=True,
                cmd=f"dd if=/dev/zero of={_file}_test bs=8096 count=10000000",
                timeout=10,
            )
        except SocketTimeoutException as timeout:
            LOG.info(
                f"Command execution failure as expected with timeout"
                f" as IO fails on inaccessible mount point : {timeout}"
            )

        # Add client host access
        # Check the existence of the NVMe namespaces
        gateway.add_host(subnqn=subsystem["nqn"], hostnqn=initiator_nqn)
        sleep(10)
        targets = initiator.list_spdk_drives()
        if not targets:
            raise Exception(f"NVMe Targets not found on {client.hostname}")
        client.exec_command(
            sudo=True, cmd=f"dd if=/dev/zero of={_file}_test bs=4096 count=10000"
        )
        client.exec_command(sudo=True, cmd=f"ls -ltrh {_file}_test")
        LOG.info("Validation of CEPH-83575455 is successful.")
    except Exception as err:
        raise Exception(err)
    finally:
        client.exec_command(sudo=True, cmd=f"umount {_dir}")
        cleanup_cfg = {
            "gw_node": config["gw_node"],
            "initiators": [initiator_cfg],
            "cleanup": ["initiators", "gateway", "pool"],
            "rbd_pool": pool,
        }
        teardown(ceph_cluster, rbd, cleanup_cfg)


def test_ceph_83575813(ceph_cluster, rbd, pool, config):
    """CEPH-83575813: Perform RBD operations shrink and expand on images."""
    # Todo: This Test case has to be re-visited,
    #       since this issue at RBD operations are considered
    #       for GA release. This test case will fail at Tech Preview.
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    gateway = NVMeCLI(gw_node)

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph_83575813",
            "serial": 83575813,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )

    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph_83575813",
        "listener_port": listener_port,
        "node": config["initiator_node"],
    }
    client = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = Initiator(client)
    try:
        subsystem["gateway-name"] = find_client_daemon_id(
            ceph_cluster, pool, node_name=gw_node.hostname
        )
        configure_subsystems(rbd, pool, gateway, subsystem)
        name = generate_unique_id(length=4)

        # Create image
        img1 = f"{name}-image1"
        img2 = f"{name}-image2"
        rbd.create_image(pool, img1, "10G")
        rbd.create_image(pool, img2, "5G")

        for img in [img1, img2]:
            gateway.create_block_device(img, img, pool)
            gateway.add_namespace(subsystem["nqn"], img)

        config.update(initiator_cfg)
        # Run IOS on nvme namespaces
        initiators(ceph_cluster, gateway, initiator_cfg)

        def check(_node, size):
            out, _ = _node.exec_command(sudo=True, cmd="lsblk -J")
            for _img in json.loads(out)["blockdevices"]:
                if _img["name"].startswith("nvme") and _img["size"] == size:
                    return True
            raise Exception(f"Did not find NVMe disk with Size {size}")

        for _size in ["10G", "5G"]:
            check(client, _size)

        # shrink and expand the images
        # validate the sizes at the client side
        rbd.image_resize(pool, img1, "11G")
        rbd.image_resize(pool, img2, "4G")
        initiator.disconnect_all()
        cmd_args = {
            "transport": "tcp",
            "traddr": gateway.node.ip_address,
            "nqn": "nqn.2016-06.io.spdk:ceph_83575813",
        }
        conn_port = {"trsvcid": config["listener_port"]}
        _conn_cmd = {**cmd_args, **conn_port}
        LOG.debug(initiator.connect(**_conn_cmd))
        for _size in ["11G", "4G"]:
            check(client, _size)
        LOG.info("Validation of CEPH-83575813 is successful.")
    except Exception as err:
        raise Exception(err)
    finally:
        cleanup_cfg = {
            "gw_node": config["gw_node"],
            "initiators": [initiator_cfg],
            "cleanup": ["initiators", "gateway", "pool"],
            "rbd_pool": pool,
        }
        teardown(ceph_cluster, rbd, cleanup_cfg)


def test_ceph_83575814(ceph_cluster, rbd, pool, config):
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
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    gateway = NVMeCLI(gw_node)
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    mon_obj = MonitorWorkflows(node=cephadm)
    rados_obj = RadosOrchestrator(node=cephadm)

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph_83575814",
            "serial": 83575814,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )
    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph_83575814",
        "listener_port": listener_port,
        "node": config.get("initiator_node"),
    }
    try:
        subsystem["gateway-name"] = find_client_daemon_id(
            ceph_cluster, pool, node_name=gw_node.hostname
        )
        configure_subsystems(rbd, pool, gateway, subsystem)
        name = generate_unique_id(length=4)

        # Create image
        img = f"{name}-image"
        rbd.create_image(pool, img, "10G")
        gateway.create_block_device(img, img, pool)
        gateway.add_namespace(subsystem["nqn"], img)

        config.update(initiator_cfg)
        mon_host = ceph_cluster.get_nodes(role="mon")[0]
        with parallel() as p:
            p.spawn(initiators, ceph_cluster, gateway, initiator_cfg)

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
    except Exception as err:
        LOG.error(err)
        return 1
    finally:
        cleanup_cfg = {
            "gw_node": config.get("gw_node"),
            "initiators": [initiator_cfg],
            "cleanup": ["initiators", "gateway", "pool"],
            "rbd_pool": pool,
        }
        teardown(ceph_cluster, rbd, cleanup_cfg)


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

    LOG.info("Running Ceph Ceph NVMEoF Negative tests.")
    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    overrides = kwargs.get("test_data", {}).get("custom-config")
    for key, value in dict(item.split("=") for item in overrides).items():
        if key == "nvmeof_cli_image":
            NVMeCLI.CEPH_NVMECLI_IMAGE = value
            break

    try:
        gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
        cfg = {
            "config": {
                "command": "apply",
                "service": "nvmeof",
                "args": {"placement": {"nodes": [gw_node.hostname]}},
                "pos_args": [rbd_pool],
            }
        }
        test_nvmeof.run(ceph_cluster, **cfg)
        if config["operation"] == "CEPH-83575812":
            test_ceph_83575812(ceph_cluster, rbd_obj, rbd_pool, config)
        if config["operation"] == "CEPH-83576084":
            test_ceph_83576084(ceph_cluster, rbd_obj, rbd_pool, config)
        if config["operation"] == "CEPH-83575467":
            test_ceph_83575467(ceph_cluster, rbd_obj, rbd_pool, config)
        if config["operation"] == "CEPH-83576085":
            test_ceph_83576085(ceph_cluster, rbd_obj, rbd_pool, config)
        if config["operation"] == "CEPH-83576087":
            test_ceph_83576087(ceph_cluster, rbd_obj, rbd_pool, config)
        if config["operation"] == "CEPH-83575813":
            test_ceph_83575813(ceph_cluster, rbd_obj, rbd_pool, config)
        if config["operation"] == "CEPH-83576093":
            test_ceph_83576093(ceph_cluster, rbd_obj, rbd_pool, config)
        if config["operation"] == "CEPH-83575455":
            test_ceph_83575455(ceph_cluster, rbd_obj, rbd_pool, config)
        if config["operation"] == "CEPH-83575814":
            test_ceph_83575814(ceph_cluster, rbd_obj, rbd_pool, config)
        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(ceph_cluster, rbd_obj, config)

    return 1
