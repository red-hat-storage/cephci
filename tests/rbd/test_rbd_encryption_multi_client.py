import importlib

from ceph.parallel import parallel
from ceph.rbd.initial_config import initial_rbd_config
from ceph.rbd.utils import copy_file, getdict
from ceph.rbd.workflows.cleanup import cleanup
from ceph.rbd.workflows.encryption import IO_CONFIG, encrypt_image_and_run_io
from ceph.rbd.workflows.krbd_io_handler import krbd_io_handler
from ceph.utils import get_node_by_id
from cli.utilities.dictionary import deep_update
from utility.log import Log

log = Log(__name__)


def add_client(**kw):
    """
    Add client to the node specified.
    """
    config = {
        "command": "add",
        "id": "client.2",
        "node": kw.get("client_add"),
        "install_packages": ["ceph-common", "fio", "rbd-nbd"],
        "copy_admin_keyring": "true",
    }

    test_mod = importlib.import_module("test_client")
    return test_mod.run(
        ceph_cluster=kw["ceph_cluster"],
        ceph_nodes=kw["ceph_nodes"],
        config=config,
        test_data=kw["test_data"],
        ceph_cluster_dict=kw["ceph_cluster_dict"],
        clients=kw["clients"],
    )


def test_rbd_encryption_multi_client(rbd_obj, **kw):
    """
    Encrypt an image and run IOs on it from 2 clients
    """
    for pool_type in rbd_obj.get("pool_types"):
        rbd_config = kw.get("config", {}).get(pool_type, {})
        multi_pool_config = getdict(rbd_config)
        test_config = multi_pool_config.pop("test_config")
        for pool, pool_config in multi_pool_config.items():
            multi_image_config = getdict(pool_config)
            for image, image_config, enc_type in zip(
                multi_image_config.keys(),
                multi_image_config.values(),
                test_config.get("encryption_type"),
            ):
                log.info(f"Testing multi client IOs for image {pool}/{image}")
                rbd = rbd_obj.get("rbd")
                passphrase = f"{enc_type}_passphrase.bin"

                update_dict = {
                    "rbd": rbd,
                    "client": kw.get("client1"),
                    "size": image_config.get("size"),
                    "config": {
                        "file_path": "create_rand_path",
                        "image_spec": [f"{pool}/{image}"],
                        "operations": {"nounmap": False},
                        "encryption_config": [
                            {
                                "encryption-format": enc_type,
                                "encryption-passphrase-file": passphrase,
                            }
                        ],
                    },
                }
                io_config = deep_update(IO_CONFIG, update_dict)
                update_dict["client"] = kw.get("client2")

                log.info(f"Encrypting image {pool}/{image}")

                ret_val = encrypt_image_and_run_io(
                    **io_config,
                    do_not_map_and_mount=True,
                )
                if ret_val:
                    log.error(f"Encryption failed for image {pool}/{image}")
                    return 1

                copy_file(passphrase, kw.get("client1"), kw.get("client2"))

                io_config["config"]["operations"]["io"] = False

                krbd_io_handler(**io_config)

                io_config["config"]["operations"]["io"] = True
                io_config["config"]["skip_mkfs"] = True

                with parallel() as p:
                    p.spawn(krbd_io_handler, **io_config)
                    io_config["client"] = kw.get("client2")
                    p.spawn(krbd_io_handler, **io_config)
    return 0


def run(**kw):
    """Have multiple clients and start IOs to the encrypted images and verify

    Pre-requisites :
    We need atleast one client node with ceph-common and fio packages,
    conf and keyring files

    Test cases covered -
    1) CEPH-83575259 - Have multiple clients and start IOs to the encrypted images and verify

    Test Case Flow
    1. Create a pool and an image
    2. Apply encryption format to the images Luks1/Luks2
    3. Load the encryption
    4. Have multiple clients and run IOs to the encrypted images
    """
    log.info("Running IOs on multiple clients on an encrypted image CEPH-83575259")

    try:
        rbd_obj = initial_rbd_config(**kw)
        client1 = rbd_obj.get("client")
        client2 = get_node_by_id(kw["ceph_cluster"], "node3")
        rc = add_client(client_add="node3", **kw)
        if rc:
            log.error("Adding client failed")
            return 1
        pool_types = rbd_obj.get("pool_types")
        ret_val = test_rbd_encryption_multi_client(
            rbd_obj=rbd_obj, client1=client1, client2=client2, **kw
        )
    except Exception as e:
        log.error(
            f"Running IOs on multiple clients on an encrypted image\
                   failed with error {str(e)}"
        )
        ret_val = 1
    finally:
        cluster_name = kw.get("ceph_cluster", {}).name
        obj = {cluster_name: rbd_obj}
        cleanup(pool_types=pool_types, multi_cluster_obj=obj, **kw)
    return ret_val
