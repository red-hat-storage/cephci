# Classes(based on Operations) for RBD
# snapshots   -- create, list, delete, rollback, purge, protect, clone, unprotect, flatten, list children
# basic --- remove, list, create, resize, info, restore
# mirroring  -- enable, disble, mirror-snapshots, promotion&demotion, status
# migration -- execute, migrate, abort

# test_config ---> workflowlayer(we combine various SDK layer operations)
# --> rbd_utils made into modular based on operation --> call specific SDK

import os
from time import sleep

import yaml

from ceph.ceph import CommandFailed
from ceph.ceph_admin import CephAdmin
from ceph.parallel import parallel
from ceph.util_map import UTIL_MAP
from cephV2.ceph.ceph import Ceph
from cephV2.rbd.rbd import Rbd
from utility.log import Log

log = Log(__name__)


ROOT_COMMAND_MAP = dict(
    {
        "rbd": Rbd,
        "ceph": Ceph,
    }
)


def operator(test_config, step_config, **kw):
    """
    Using the provided test config file, this method triggers SDK calls of RBD
    of that specific scenario

    Arguments:
        test_config: containing the key/value pairs passed from the test-suite
        step_config: arguments required for a specific operation
        args: test data

    Returns:
        0 on success or 1 for failures
    """

    if step_config.get("command") == "shell":
        cephadm = CephAdmin(cluster=kw["ceph_cluster_dict"], **test_config)
        cephadm.shell(args=step_config["args"])

    elif step_config.get("command") is None:
        util = list(step_config)[0]
        if UTIL_MAP.get(util, None) is None:
            log.error(f"Utility {util} has not been implemented yet")
            return 1

        UTIL_MAP[util].run(kw, step_config["args"])

    else:
        # Extract where to run
        cluster_name = step_config.get("cluster_name")
        ceph_nodes = (
            kw["ceph_cluster_dict"][cluster_name] if cluster_name else kw["ceph_nodes"]
        )
        node_role = step_config.get("node_role", "client")
        step_node = ceph_nodes.get_ceph_object(node_role)

        # Extract what to run
        step_command = step_config["command"]
        step_attribute = ROOT_COMMAND_MAP[step_command.split(" ", 1)[0]](step_node)
        for sub_command in step_command.split(" ")[1:]:
            step_attribute = getattr(step_attribute, sub_command)

        # Run
        step_attribute(args=step_config["args"])


def run(**kw):
    """
    Rbd Workflow module to manage ceph-rbd services

    Sample test script

        - test:
            abort-on-fail: true
            name: snap and clone operations on imported image
            desc: Snap and clone operations on imported image
            module: test_rbdV2.py
            clusters:
                ceph-rbd1:
                config:
                    node: node6
                    test_config: test_snap_clone_imported_image.yaml

    Arguments:
        args: Dict - containing the key/value pairs passed from the test-suite

    Returns:
        0 on success or 1 for failures
    """
    try:
        config = kw["config"]
        test_configs_path = "tests/rbd/test_configs/" + config["test_config"]
        with open(os.path.abspath(test_configs_path)) as test_config_file:
            test_config = yaml.safe_load(test_config_file)
            # Step2: Get class and method names for an entrypoint to trigger
            if test_config.get("parallel", False):
                log.info("execution started")
                with parallel() as p:
                    for step in test_config["steps"]:
                        p.spawn(operator, test_config, step, **kw)
                        sleep(1)
            else:
                for step in test_config["steps"]:
                    operator(test_config, step, **kw)

    except CommandFailed as error:
        log.error(error)
        return 1

    finally:
        log.info("")
    return 0
