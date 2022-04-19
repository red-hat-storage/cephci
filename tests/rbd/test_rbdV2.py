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
from cephV2.ceph.pool import Pool
from ceph.parallel import parallel
from cephV2.rbd.feature import Feature
from cephV2.rbd.mirror.mirror import Mirror
from cephV2.rbd.rbd import Rbd
from cephV2.rbd.snap import Snapshot
from ceph.util_map import UTIL_MAP
from utility.log import Log

log = Log(__name__)


CLASS_MAP = dict(
    {
        "RbdMirror": Mirror,
        "Snapshot": Snapshot,
        "Rbd": Rbd,
        "Pool": Pool,
        "Feature": Feature,
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
    if step_config.get("method") == "shell":
        cephadm = CephAdmin(kw["ceph_cluster_dict"], test_config)
        cephadm.shell(args=step_config["args"])

    elif step_config.get("method", None) is None:
        util = list(step_config)[0]
        if UTIL_MAP.get(util, None) is None:
            log.error(f"Utility {util} has not been implemented yet")
            return 1

        UTIL_MAP[util].run(kw, step_config["args"])

    else:
        # maintain dictionary to map to classes based on service
        # instantiate class
        cluster_name = step_config.get("cluster_name", None)
        ceph_nodes = (
            kw["ceph_cluster_dict"][cluster_name] if cluster_name else kw["ceph_nodes"]
        )
        instance = CLASS_MAP[step_config["class"]](nodes=ceph_nodes)
        method = getattr(instance, step_config["method"])
        log.info(method)
        method(step_config["args"])
    return 0


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
