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
from tests.nvmeof.test_ceph_nvmeof_gateway import (
    configure_subsystems,
    initiators,
    teardown,
)
from tests.nvmeof.workflows.initiator import NVMeInitiator
from tests.nvmeof.workflows.nvme_gateway import create_gateway
from tests.nvmeof.workflows.nvme_utils import (
    check_and_set_nvme_cli_image,
    deploy_nvme_service,
    nvme_gw_cli_version_adapter,
    setup_firewalld,
)
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.retry import retry
from utility.utils import find_free_port, generate_unique_id

LOG = Log(__name__)
cli_image = str()


def test_ceph_83575812(ceph_cluster, rbd, pool, config):
    """CEPH-83575812 Remove the image during NVMe images are in use."""
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    ceph = Orch(ceph_cluster, **{})
    nvmegwcli = create_gateway(
        nvme_gw_cli_version_adapter(ceph_cluster),
        gw_node,
        mtls=config.get("mtls"),
        shell=getattr(ceph, "shell"),
        port=config.get("gw_port", 5500),
        gw_group=config.get("gw_group"),
    )
    listener_port = find_free_port(gw_node)
    subsystem = dict()
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:negative",
            "serial": 114,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )

    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:negative",
        "listener_port": listener_port,
        "node": config["initiator_node"],
    }
    try:
        configure_subsystems(ceph_cluster, rbd, pool, nvmegwcli, subsystem)
        name = generate_unique_id(length=4)

        # Create image
        img = f"{name}-image"
        rbd.create_image(pool, img, "1G")
        ns_args = {
            "args": {"subsystem": subsystem["nqn"], "rbd-pool": pool, "rbd-image": img}
        }
        nvmegwcli.namespace.add(**ns_args)

        config.update(initiator_cfg)
        with parallel() as p:
            p.spawn(initiators, ceph_cluster, nvmegwcli, initiator_cfg)
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
            "disconnect_all": [config["initiator_node"]],
            "cleanup": ["disconnect_all", "gateway", "pool"],
            "rbd_pool": pool,
        }
        config.update(cleanup_cfg)
        teardown(ceph_cluster, rbd, nvmegwcli, config)


def test_ceph_83576084(ceph_cluster, rbd, pool, config):
    """CEPH-83576084: Delete-recreate bdev in loop and rediscover namespace."""
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    ceph = Orch(ceph_cluster, **{})
    nvmegwcli = create_gateway(
        nvme_gw_cli_version_adapter(ceph_cluster),
        gw_node,
        mtls=config.get("mtls"),
        shell=getattr(ceph, "shell"),
        port=config.get("gw_port", 5500),
        gw_group=config.get("gw_group"),
    )

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph-83576084",
            "serial": 112,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )
    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph-83576084",
        "listener_port": listener_port,
        "node": config["initiator_node"],
    }
    try:
        configure_subsystems(ceph_cluster, rbd, pool, nvmegwcli, subsystem)
        name = generate_unique_id(length=4)

        # Create image
        img = f"{name}-image"
        rbd.create_image(pool, img, "1G")
        # TODO: Removing nsid because in 9.0 we have known issue
        ns_args = {
            "args": {
                "subsystem": subsystem["nqn"],
                "rbd-pool": pool,
                "rbd-image": img,
            }
        }
        nvmegwcli.namespace.add(**ns_args)

        config.update(initiator_cfg)
        client = get_node_by_id(ceph_cluster, config["node"])
        initiator = NVMeInitiator(client)
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
                if nqn["trsvcid"] == listener_port:
                    _cmd_args["nqn"] = nqn["subnqn"]
                    break
            else:
                raise Exception(f"Subsystem not found -- {cmd_args}")

            # Connect to the subsystem
            conn_port = {"trsvcid": listener_port}
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

        _ns_args = {"args": {"subsystem": subsystem["nqn"], "nsid": 1}}
        for _ in "check":
            client.exec_command(sudo=True, cmd=f"umount {_dir}")
            nvmegwcli.namespace.delete(**_ns_args)
            nvmegwcli.namespace.add(**ns_args)
            check_client(verify=True)

        LOG.info("Validation of CEPH-83576084 is successful.")
    except Exception as err:
        raise Exception(err)
    finally:
        cleanup_cfg = {
            "gw_node": config["gw_node"],
            "disconnect_all": [config["initiator_node"]],
            "cleanup": ["disconnect_all", "gateway", "pool"],
            "rbd_pool": pool,
        }
        config.update(cleanup_cfg)
        teardown(ceph_cluster, rbd, nvmegwcli, config)


def test_ceph_83575467(ceph_cluster, rbd, pool, config):
    """CEPH-83575467: Perform restart and validate the gateway entities"""
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    ceph = Orch(ceph_cluster, **{})
    nvmegwcli = create_gateway(
        nvme_gw_cli_version_adapter(ceph_cluster),
        gw_node,
        mtls=config.get("mtls"),
        shell=getattr(ceph, "shell"),
        port=config.get("gw_port", 5500),
        gw_group=config.get("gw_group"),
    )

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph-83575467",
            "serial": 111,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )
    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph-83575467",
        "listener_port": listener_port,
        "node": config["initiator_node"],
    }
    try:
        configure_subsystems(ceph_cluster, rbd, pool, nvmegwcli, subsystem)
        name = generate_unique_id(length=4)

        # Create images
        for i in range(5):
            img = f"{name}-image{i}"
            rbd.create_image(pool, img, "500M")
            ns_args = {
                "args": {
                    "subsystem": subsystem["nqn"],
                    "rbd-pool": pool,
                    "rbd-image": img,
                }
            }
            nvmegwcli.namespace.add(**ns_args)

        @retry(IOError, tries=5, delay=5)
        def list_subsystems(**sub_args):
            try:
                out, _ = nvmegwcli.subsystem.list(**sub_args)
                return out
            except Exception as e:
                raise IOError(e)

        config.update(initiator_cfg)
        sub_args = {"base_cmd_args": {"format": "json"}}
        initiators(ceph_cluster, nvmegwcli, initiator_cfg)
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
    finally:
        cleanup_cfg = {
            "gw_node": config["gw_node"],
            "disconnect_all": [config["initiator_node"]],
            "cleanup": ["disconnect_all", "gateway", "pool"],
            "rbd_pool": pool,
        }
        config.update(cleanup_cfg)
        teardown(ceph_cluster, rbd, nvmegwcli, config)


def test_ceph_83576085(ceph_cluster, rbd, pool, config):
    """CEPH-83576085: Perform map and unmap NVMe namespaces in loop."""
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    ceph = Orch(ceph_cluster, **{})
    nvmegwcli = create_gateway(
        nvme_gw_cli_version_adapter(ceph_cluster),
        gw_node,
        mtls=config.get("mtls"),
        shell=getattr(ceph, "shell"),
        port=config.get("gw_port", 5500),
        gw_group=config.get("gw_group"),
    )

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph-83576085",
            "serial": 113,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )

    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph-83576085",
        "listener_port": listener_port,
        "node": config["initiator_node"],
    }

    cmd_args = {
        "transport": "tcp",
        "traddr": nvmegwcli.node.ip_address,
        "trsvcid": listener_port,
    }

    json_format = {"output-format": "json"}
    _dir = f"/tmp/dir_{generate_unique_id(4)}"
    _file = f"{_dir}/test.log"

    try:
        configure_subsystems(ceph_cluster, rbd, pool, nvmegwcli, subsystem)
        name = generate_unique_id(length=4)

        # Create image
        img = f"{name}-image"
        rbd.create_image(pool, img, "1G")
        ns_args = {
            "args": {"subsystem": subsystem["nqn"], "rbd-pool": pool, "rbd-image": img}
        }
        nvmegwcli.namespace.add(**ns_args)

        config.update(initiator_cfg)
        client = get_node_by_id(ceph_cluster, config["node"])
        initiator = NVMeInitiator(client)

        disc_port = {"trsvcid": 8009}
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
    finally:
        cleanup_cfg = {
            "gw_node": config["gw_node"],
            "disconnect_all": [config["initiator_node"]],
            "cleanup": ["disconnect_all", "gateway", "pool"],
            "rbd_pool": pool,
        }
        config.update(cleanup_cfg)
        teardown(ceph_cluster, rbd, nvmegwcli, config)


def test_ceph_83576087(ceph_cluster, rbd, pool, config):
    """CEPH-83576087: Reboot client node and validate NVMe namespaces"""
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    ceph = Orch(ceph_cluster, **{})
    nvmegwcli = create_gateway(
        nvme_gw_cli_version_adapter(ceph_cluster),
        gw_node,
        mtls=config.get("mtls"),
        shell=getattr(ceph, "shell"),
        port=config.get("gw_port", 5500),
        gw_group=config.get("gw_group"),
    )

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph-83576087",
            "serial": 113,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )

    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph-83576087",
        "listener_port": listener_port,
        "node": config["initiator_node"],
    }

    cmd_args = {
        "transport": "tcp",
        "traddr": nvmegwcli.node.ip_address,
        "trsvcid": listener_port,
    }

    json_format = {"output-format": "json"}
    _dir = f"/tmp/dir_{generate_unique_id(4)}"
    _file = f"{_dir}/test.log"

    try:
        configure_subsystems(ceph_cluster, rbd, pool, nvmegwcli, subsystem)
        name = generate_unique_id(length=4)

        # Create image
        img = f"{name}-image"
        rbd.create_image(pool, img, "1G")
        ns_args = {
            "args": {"subsystem": subsystem["nqn"], "rbd-pool": pool, "rbd-image": img}
        }
        nvmegwcli.namespace.add(**ns_args)

        config.update(initiator_cfg)
        client = get_node_by_id(ceph_cluster, config["node"])
        initiator = NVMeInitiator(client)

        initiator.disconnect_all()
        disc_port = {"trsvcid": 8009}
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
    finally:
        cleanup_cfg = {
            "gw_node": config["gw_node"],
            "disconnect_all": [config["initiator_node"]],
            "cleanup": ["disconnect_all", "gateway", "pool"],
            "rbd_pool": pool,
        }
        config.update(cleanup_cfg)
        teardown(ceph_cluster, rbd, nvmegwcli, config)


def test_ceph_83576093(ceph_cluster, rbd, pool, config):
    """CEPH-83576093: Perform reboot on GW node and validate the namespaces."""
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    ceph = Orch(ceph_cluster, **{})
    nvmegwcli = create_gateway(
        nvme_gw_cli_version_adapter(ceph_cluster),
        gw_node,
        mtls=config.get("mtls"),
        shell=getattr(ceph, "shell"),
        port=config.get("gw_port", 5500),
        gw_group=config.get("gw_group"),
    )

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph-83576093",
            "serial": 113,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )
    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph-83576093",
        "listener_port": listener_port,
        "node": config["initiator_node"],
    }

    try:
        configure_subsystems(ceph_cluster, rbd, pool, nvmegwcli, subsystem)
        name = generate_unique_id(length=4)

        # Create image
        img = f"{name}-image"
        rbd.create_image(pool, img, "1G")
        ns_args = {
            "args": {"subsystem": subsystem["nqn"], "rbd-pool": pool, "rbd-image": img}
        }
        nvmegwcli.namespace.add(**ns_args)

        config.update(initiator_cfg)
        client = get_node_by_id(ceph_cluster, config["node"])
        initiator = NVMeInitiator(client)
        cmd_args = {
            "transport": "tcp",
            "traddr": nvmegwcli.node.ip_address,
            "trsvcid": listener_port,
        }

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
            if nqn["trsvcid"] == listener_port:
                _cmd_args["nqn"] = nqn["subnqn"]
                break
        else:
            raise Exception(f"Subsystem not found -- {cmd_args}")

        # Connect to the subsystem
        conn_port = {"trsvcid": listener_port}
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
    finally:
        cleanup_cfg = {
            "gw_node": config["gw_node"],
            "disconnect_all": [config["initiator_node"]],
            "cleanup": ["disconnect_all", "gateway", "pool"],
            "rbd_pool": pool,
        }
        config.update(cleanup_cfg)
        teardown(ceph_cluster, rbd, nvmegwcli, config)


def test_ceph_83575455(ceph_cluster, rbd, pool, config):
    """CEPH-83575455: Validate Host access failures"""
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    ceph = Orch(ceph_cluster, **{})
    nvmegwcli = create_gateway(
        nvme_gw_cli_version_adapter(ceph_cluster),
        gw_node,
        mtls=config.get("mtls"),
        shell=getattr(ceph, "shell"),
        port=config.get("gw_port", 5500),
        gw_group=config.get("gw_group"),
    )
    client = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = NVMeInitiator(client)
    initiator_nqn = initiator.initiator_nqn()

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph-83575455",
            "serial": 113,
            "listener_port": listener_port,
            "allow_host": initiator_nqn,
        }
    )
    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph-83575455",
        "listener_port": listener_port,
        "node": config["initiator_node"],
    }

    _dir = f"/tmp/dir_{generate_unique_id(4)}"
    _file = f"{_dir}/test.log"

    try:
        configure_subsystems(ceph_cluster, rbd, pool, nvmegwcli, subsystem)
        name = generate_unique_id(length=4)

        # Create image
        img = f"{name}-image"
        rbd.create_image(pool, img, "5G")
        ns_args = {
            "args": {"subsystem": subsystem["nqn"], "rbd-pool": pool, "rbd-image": img}
        }
        nvmegwcli.namespace.add(**ns_args)

        config.update(initiator_cfg)

        cmd_args = {
            "transport": "tcp",
            "traddr": nvmegwcli.node.ip_address,
            "trsvcid": listener_port,
        }

        json_format = {"output-format": "json"}
        disc_port = {"trsvcid": 8009}
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
        targets = initiator.list_devices()
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
            host_args = {"args": {"subsystem": subsystem["nqn"], "host": initiator_nqn}}
            nvmegwcli.host.delete(**host_args)
        except Exception as host_del_err:
            if (
                "Reconnecting the host would fail unless it is re-added to the subsystem"
                not in str(host_del_err)
            ):
                raise Exception("Host deletion is failed")
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
    finally:
        client.exec_command(sudo=True, cmd=f"umount {_dir}")
        cleanup_cfg = {
            "gw_node": config["gw_node"],
            "disconnect_all": [config["initiator_node"]],
            "cleanup": ["disconnect_all", "gateway", "pool"],
            "rbd_pool": pool,
        }
        config.update(cleanup_cfg)
        teardown(ceph_cluster, rbd, nvmegwcli, config)


def test_ceph_83575813(ceph_cluster, rbd, pool, config):
    """CEPH-83575813: Perform NVMeoF RBD operations expand on images."""
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    ceph = Orch(ceph_cluster, **{})
    nvmegwcli = create_gateway(
        nvme_gw_cli_version_adapter(ceph_cluster),
        gw_node,
        mtls=config.get("mtls"),
        shell=getattr(ceph, "shell"),
        port=config.get("gw_port", 5500),
        gw_group=config.get("gw_group"),
    )

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph-83575813",
            "serial": 83575813,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )

    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph-83575813",
        "listener_port": listener_port,
        "node": config["initiator_node"],
    }
    client = get_node_by_id(ceph_cluster, config["initiator_node"])
    initiator = NVMeInitiator(client)
    try:
        configure_subsystems(ceph_cluster, rbd, pool, nvmegwcli, subsystem)
        name = generate_unique_id(length=4)

        # Create image
        img1 = f"{name}-image1"
        img2 = f"{name}-image2"
        imgs = {img1: "1G", img2: "2G"}

        for img, size in imgs.items():
            rbd.create_image(pool, img, size)
            ns_args = {
                "args": {
                    "subsystem": subsystem["nqn"],
                    "rbd-pool": pool,
                    "rbd-image": img,
                }
            }
            nvmegwcli.namespace.add(**ns_args)

        config.update(initiator_cfg)
        # Run IOS on nvme namespaces
        initiator.disconnect_all()
        initiators(ceph_cluster, nvmegwcli, initiator_cfg)

        def check(_node, _size):
            out, _ = _node.exec_command(sudo=True, cmd="lsblk -bJ")
            for _img in json.loads(out)["blockdevices"]:
                if _img["name"].startswith("nvme") and _img["size"] == _size:
                    LOG.info(f"Found Image with {_size}: {_img}")
                    return True
            raise Exception(f"Did not find NVMe disk with Size {_size}")

        rbd_objs = rbd.get_disk_usage_for_pool(pool)["images"]
        for img in imgs.keys():
            rbd_img_size = [
                i["provisioned_size"] for i in rbd_objs if img == i["name"]
            ][0]
            check(client, rbd_img_size)

        # Expand the images and validate the sizes at the client side
        nvmegwcli.namespace.resize(
            **{"args": {"subsystem": subsystem["nqn"], "nsid": 1, "size": "3G"}}
        )
        nvmegwcli.namespace.resize(
            **{"args": {"subsystem": subsystem["nqn"], "nsid": 2, "size": "4G"}}
        )

        for img in rbd.get_disk_usage_for_pool(pool)["images"]:
            check(client, img["provisioned_size"])
        initiator.disconnect_all()
        initiators(ceph_cluster, nvmegwcli, initiator_cfg)
        LOG.info("Validation of CEPH-83575813 is successful.")
    except Exception as err:
        raise Exception(err)
    finally:
        cleanup_cfg = {
            "gw_node": config["gw_node"],
            "disconnect_all": [config["initiator_node"]],
            "cleanup": ["disconnect_all", "gateway", "pool"],
            "rbd_pool": pool,
        }
        config.update(cleanup_cfg)
        teardown(ceph_cluster, rbd, nvmegwcli, config)


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
    ceph = Orch(ceph_cluster, **{})
    nvmegwcli = create_gateway(
        nvme_gw_cli_version_adapter(ceph_cluster),
        gw_node,
        mtls=config.get("mtls"),
        shell=getattr(ceph, "shell"),
        port=config.get("gw_port", 5500),
        gw_group=config.get("gw_group"),
    )
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    mon_obj = MonitorWorkflows(node=cephadm)
    rados_obj = RadosOrchestrator(node=cephadm)

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph-83575814",
            "serial": 83575814,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )
    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph-83575814",
        "listener_port": listener_port,
        "node": config.get("initiator_node"),
    }
    try:
        configure_subsystems(ceph_cluster, rbd, pool, nvmegwcli, subsystem)
        name = generate_unique_id(length=4)

        # Create image
        img = f"{name}-image"
        rbd.create_image(pool, img, "10G")
        ns_args = {
            "args": {"subsystem": subsystem["nqn"], "rbd-pool": pool, "rbd-image": img}
        }
        nvmegwcli.namespace.add(**ns_args)

        config.update(initiator_cfg)
        mon_host = ceph_cluster.get_nodes(role="mon")[0]
        with parallel() as p:
            p.spawn(initiators, ceph_cluster, nvmegwcli, initiator_cfg)

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
            "disconnect_all": [config["initiator_node"]],
            "cleanup": ["disconnect_all", "gateway", "pool"],
            "rbd_pool": pool,
        }
        config.update(cleanup_cfg)
        teardown(ceph_cluster, rbd, nvmegwcli, config)


def test_ceph_83581753(ceph_cluster, rbd, pool, config):
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

    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    ceph = Orch(ceph_cluster, **{})
    nvmegwcli = create_gateway(
        nvme_gw_cli_version_adapter(ceph_cluster),
        gw_node,
        mtls=config.get("mtls"),
        shell=getattr(ceph, "shell"),
        port=config.get("gw_port", 5500),
        gw_group=config.get("gw_group"),
    )

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph-83581753",
            "serial": 83581753,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )
    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph-83575814",
        "listener_port": listener_port,
        "node": config.get("initiator_node"),
    }
    try:
        configure_subsystems(ceph_cluster, rbd, pool, nvmegwcli, subsystem)
        list_args = {}
        list_args.setdefault("args", {}).update(
            {"subsystem": "nqn.2016-06.io.spdk:ceph-83581753"}
        )
        list_args.setdefault("base_cmd_args", {}).update({"format": "json"})
        name = generate_unique_id(length=4)

        # Create image
        img = f"{name}-image"
        rbd.create_image(pool, img, "10G")

        ns_args = {
            "args": {"subsystem": subsystem["nqn"], "rbd-pool": pool, "rbd-image": img}
        }
        nvmegwcli.namespace.add(**ns_args)
        config.update(initiator_cfg)
        namespaces, _ = nvmegwcli.namespace.list(**list_args)
        namespaces = json.loads(namespaces)
        nsid = namespaces.get("namespaces")[0].get("nsid")
        try:
            qos_args_with_invalid_values = {}
            qos_args_with_invalid_values.setdefault("args", {}).update(
                {"nsid": 10, "subsystem": subsystem["nqn"], "rw-ios-per-second": 10}
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
                    "subsystem": subsystem["nqn"],
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
        return 0
    finally:
        cleanup_cfg = {
            "gw_node": config.get("gw_node"),
            "disconnect_all": [config["initiator_node"]],
            "cleanup": ["disconnect_all", "gateway", "pool"],
            "rbd_pool": pool,
            "subsystems": [subsystem],
        }
        config.update(cleanup_cfg)
        teardown(ceph_cluster, rbd, nvmegwcli, config)
    return 1


def test_ceph_83581945(ceph_cluster, rbd, pool, config):
    """CEPH-83581945: Set QoS for namespace in subsystem without mandatory values and args.
    Args:
        ceph_cluster (CephCluster): The Ceph cluster instance.
        rbd (RadosBlockDevice): The RBD instance.
    pool (str): The Ceph pool name.
    config (dict): Configuration parameters.
    Returns:
        int: 0 on success, 1 on failure.
    """
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    ceph = Orch(ceph_cluster, **{})
    nvmegwcli = create_gateway(
        nvme_gw_cli_version_adapter(ceph_cluster),
        gw_node,
        mtls=config.get("mtls"),
        shell=getattr(ceph, "shell"),
        port=config.get("gw_port", 5500),
        gw_group=config.get("gw_group"),
    )

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph-83581945",
            "serial": 83581945,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )
    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph-83575814",
        "listener_port": listener_port,
        "node": config.get("initiator_node"),
    }
    try:
        configure_subsystems(ceph_cluster, rbd, pool, nvmegwcli, subsystem)
        list_args = {}
        list_args.setdefault("args", {}).update({"subsystem": subsystem["nqn"]})
        list_args.setdefault("base_cmd_args", {}).update({"format": "json"})
        name = generate_unique_id(length=4)

        # Create image
        img = f"{name}-image"
        rbd.create_image(pool, img, "10G")

        ns_args = {
            "args": {"subsystem": subsystem["nqn"], "rbd-pool": pool, "rbd-image": img}
        }
        config.update(initiator_cfg)
        nvmegwcli.namespace.add(**ns_args)
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
                {"rw-ios-per-second": 10, "subsystem": subsystem["nqn"]}
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
                {"subsystem": subsystem["nqn"], "nsid": 1}
            )
            nvmegwcli.namespace.delete(**namespace_delete_args)
            qos_args_without_mandatory_args.setdefault("args", {}).update(
                {"rw-ios-per-second": 10, "subsystem": subsystem["nqn"], "nsid": 1}
            )
            _ = nvmegwcli.namespace.set_qos(**qos_args_without_mandatory_args)
        except Exception as err:
            if "Can't find namespace" not in str(err):
                raise Exception(
                    "Set QoS was failed as expected due to absence of namespace."
                )
            LOG.info("Set QoS was failed as expected due to absence of namespace....")
    finally:
        cleanup_cfg = {
            "gw_node": config.get("gw_node"),
            "disconnect_all": [config["initiator_node"]],
            "cleanup": ["disconnect_all", "gateway", "pool"],
            "rbd_pool": pool,
            "subsystems": [subsystem],
        }
        config.update(cleanup_cfg)
        teardown(ceph_cluster, rbd, nvmegwcli, config)
        return 0


def test_ceph_83581755(ceph_cluster, rbd, pool, config):
    """CEPH-83581755: Set QoS for multiple namespaces.
    Args:
        ceph_cluster (CephCluster): The Ceph cluster instance.
        rbd (RadosBlockDevice): The RBD instance.
    pool (str): The Ceph pool name.
    config (dict): Configuration parameters.
    Returns:
        int: 0 on success, 1 on failure.
    """
    gw_node = get_node_by_id(ceph_cluster, config["gw_node"])
    ceph = Orch(ceph_cluster, **{})
    nvmegwcli = create_gateway(
        nvme_gw_cli_version_adapter(ceph_cluster),
        gw_node,
        mtls=config.get("mtls"),
        shell=getattr(ceph, "shell"),
        port=config.get("gw_port", 5500),
        gw_group=config.get("gw_group"),
    )

    subsystem = dict()
    listener_port = find_free_port(gw_node)
    subsystem.update(
        {
            "nqn": "nqn.2016-06.io.spdk:ceph-83581755",
            "serial": 83581755,
            "listener_port": listener_port,
            "allow_host": "*",
        }
    )
    initiator_cfg = {
        "subnqn": "nqn.2016-06.io.spdk:ceph-83575814",
        "listener_port": listener_port,
        "node": config.get("initiator_node"),
    }
    try:
        configure_subsystems(ceph_cluster, rbd, pool, nvmegwcli, subsystem)
        list_args = {}
        list_args.setdefault("args", {}).update({"subsystem": subsystem["nqn"]})
        list_args.setdefault("base_cmd_args", {}).update({"format": "json"})
        nsid = []
        for i in range(2):
            name = generate_unique_id(length=4)
            # Create image
            img = f"{name}-image"
            rbd.create_image(pool, img, "10G")
            ns_args = {
                "args": {
                    "subsystem": subsystem["nqn"],
                    "rbd-pool": pool,
                    "rbd-image": img,
                }
            }
            nvmegwcli.namespace.add(**ns_args)
            namespaces, _ = nvmegwcli.namespace.list(**list_args)
            list_namespaces = json.loads(namespaces)
            nsid.append(list_namespaces.get("namespaces")[i].get("nsid"))

        config.update(initiator_cfg)
        qos_args_without_mandatory_args = {}
        qos_args_without_mandatory_args.setdefault("args", {}).update(
            {
                "nsid": f"{nsid[0]},{nsid[1]}",
                "subsystem": subsystem["nqn"],
                "rw-ios-per-second": 10,
            }
        )
        _ = nvmegwcli.namespace.set_qos(**qos_args_without_mandatory_args)
    except Exception as err:
        if "argument --nsid: invalid int value:" not in str(err):
            raise Exception(
                "Set QoS was failed as expected due to invalid namespace value."
            )
        LOG.info("Set QoS was failed as expected due to invalid namespace value....")
    finally:
        cleanup_cfg = {
            "gw_node": config.get("gw_node"),
            "disconnect_all": [config["initiator_node"]],
            "cleanup": ["disconnect_all", "gateway", "pool"],
            "rbd_pool": pool,
            "subsystems": [subsystem],
        }
        config.update(cleanup_cfg)
        teardown(ceph_cluster, rbd, nvmegwcli, config)
        return 0


def test_ceph_83608266(
    ceph_cluster, rbd, pool, config, placement_cfg, executor, io_tasks
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

    def deploy_nvme_service(conf, gw_node, nvmegwcli, redeploy=False):
        states = {}
        container_ids = []
        subsystem_list = []
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
            CephAdm(admin_node).ceph.orch.apply(service_name="nvmeof", **conf)
        out = CephAdm(admin_node).ceph.orch.ls(service_type="nvmeof", format="json")
        service_details = json.loads(out)[0]

        if "service was created" not in service_details.get("events", [""])[-1]:
            raise RuntimeError("NVMe service deployment failed.")

        time.sleep(20)
        if not redeploy:
            running_containers, _ = get_running_containers(
                gw_node, format="json", expr="name=nvmeof", sudo=True
            )
            container_ids = [
                item.get("Names")[0] for item in json.loads(running_containers)
            ]

            subsystem = {
                "nqn": "nqn.2016-06.io.spdk:ceph-83581755",
                "serial": 83581755,
                "listener_port": 5003,
                "allow_host": "*",
            }

            configure_subsystems(ceph_cluster, rbd, pool, nvmegwcli, subsystem)

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

        LOG.info("Deploying NVMe service in gw_group1")
        _cls = nvme_gw_cli_version_adapter(ceph_cluster)
        ceph = Orch(ceph_cluster, **{})

        args = {
            "shell": getattr(ceph, "shell"),
            "port": config.get("port", 5500),
        }
        nvmegwcli = _cls(gw_node, **args)
        initial_subsystems, initial_states, initial_containers = deploy_nvme_service(
            conf, gw_node, nvmegwcli, False
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

        deploy_nvme_service(conf, gw_node, nvmegwcli, True)
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
        cleanup_cfg = {
            "gw_node": config.get("gw_node"),
            "cleanup": ["gateway", "pool"],
            "rbd_pool": pool,
        }
        config.update(cleanup_cfg)
        for group in ["gw_group1", "gw_group2"]:
            config["gw_group"] = group
            teardown(ceph_cluster, rbd, nvmegwcli, config)
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
    global cli_image
    LOG.info("Running Ceph Ceph NVMEoF Negative tests.")
    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    executor = ThreadPoolExecutor()
    io_tasks = []

    overrides = kwargs.get("test_data", {}).get("custom-config")
    check_and_set_nvme_cli_image(ceph_cluster, config=overrides)
    try:
        if config["operation"] != "CEPH-83608266":
            rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
            deploy_nvme_service(ceph_cluster, config)

        if config["operation"] == "CEPH-83608266":
            placements = config["args"]["placement_options"]
            for placement_cfg in placements:
                rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]
                test_ceph_83608266(
                    ceph_cluster,
                    rbd_obj,
                    rbd_pool,
                    config,
                    placement_cfg,
                    executor,
                    io_tasks,
                )
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
            operation_mapping[operation](ceph_cluster, rbd_obj, rbd_pool, config)

        return 0

    except Exception as err:
        LOG.error(err)

    finally:
        if io_tasks:
            LOG.info("Waiting for completion of IOs.")
            executor.shutdown(wait=False, cancel_futures=True)
    return 1
