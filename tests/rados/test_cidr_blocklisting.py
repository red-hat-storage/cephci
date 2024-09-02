import random
import time

import yaml

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from tests.rados.test_data_migration_bw_pools import create_given_pool
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Test to verify CIDR blocklisting of ceph clients
    Returns:
        1 -> Fail, 0 -> Pass
    """
    log.info(run.__doc__)
    config = kw["config"]
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    client_nodes = rados_obj.ceph_cluster.get_nodes(role="client")
    pool_details = config["pool_configs"]
    image_count = config.get("image_count", 2)
    pool_configs_path = config["pool_configs_path"]
    log.debug("Verifying CIDR Blocklisting of ceph clients")

    try:
        with open(pool_configs_path, "r") as fd:
            pool_conf = yaml.safe_load(fd)

        pool_names = []
        for i in pool_details.values():
            pool = pool_conf[i["type"]][i["conf"]]
            pool.update({"app_name": "rbd"})
            create_given_pool(rados_obj, pool)
            pool_names.append(pool["pool_name"])

        log.debug(
            f"Pools created on the cluster for testing CIDR blocklisting : {pool_names}. "
            f"Initializing the pools with RBD and creating images on the pools for testing"
        )

        rbd_images = {}
        img_count = 0
        for pool_name in pool_names:
            for _ in range(image_count):
                img_count += 1
                img_name = f"test_img_{pool_name}-{img_count}"
                # Creating 25Gib Images on the pool
                rados_obj.create_rbd_image(pool_name=pool_name, img_name=img_name)
                if pool_name not in rbd_images:
                    rbd_images[pool_name] = []
                rbd_images[pool_name].extend([img_name])
        log.debug(
            "Completed creating images on the pools. Proceeding to mount them on clients"
        )

        # Proceeding to mount images
        count = 0
        for client in client_nodes:
            log.debug(f"Client selected to mount is : {client.hostname}")
            for pool_name in pool_names:
                log.debug(
                    f"Pool selected : {pool_name}. Images on the pool : {rbd_images[pool_name]}"
                )
                for img in rbd_images[pool_name]:
                    count += 1
                    mount_path = f"/tmp/rbd_mounts/path{count}/"
                    out = rados_obj.mount_image_on_client(
                        pool_name=pool_name,
                        img_name=img,
                        client_obj=client,
                        mount_path=mount_path,
                    )
                    if not out:
                        log.error(
                            f"Failed to mount image : {img} on client : {client.hostname}"
                        )
                        raise Exception("Image not mounted error")
                    log.info(f"Image {img} mounted on Client: {client.hostname}")
        log.info("Completed mapping images onto all the clients")

        # Proceeding to write data on to all Images from all clients
        for client in client_nodes:
            log.debug(f"Client selected to write IOs is : {client.hostname}")
            for pool in pool_names:
                for img in rbd_images[pool]:
                    log.info(
                        f"Proceeding to write data into pool : {pool} and image : {img}"
                    )
                    out, status = rados_obj.rbd_bench_write(
                        client_obj=client, pool_name=pool, image_name=img
                    )
                    if not status:
                        log.error(
                            f"IOs could not be written on pool : {pool} and image : {img} "
                        )
                        raise Exception("IOs not written error")
                    log.debug(f"Items written into pool : {out}")
                    log.info("IO's written onto RBD image. ")
        log.info(
            "completed writing data into all the images on the pools"
            "Sleeping for 60 seconds for the watchers to be updated..."
        )

        # Sleeping for 40 seconds and checking if clients are updated as watchers...
        time.sleep(60)

        for client in client_nodes:
            log.debug(
                f"Client selected to check if it's added as watcher : {client.hostname}"
            )
            for pool in pool_names:
                for img in rbd_images[pool]:
                    # Checking if the client has been added as a watcher
                    watcher_clients = rados_obj.get_rbd_client_ips(
                        pool_name=pool, image_name=img
                    )
                    if client.ip_address not in watcher_clients:
                        log.error(
                            f"Client : {client.hostname} not listed as watcher "
                            f"for the image : {img} on pool : {pool}"
                        )
                        raise Exception("Client IP not listed as watcher error")
        log.debug(
            "Confirmed that all clients are listed as watchers on the pool/images created.."
            "Proceeding to test blocklisting commands"
        )

        # Starting workflows to test various kinds of blocklisting...

        log.info("------ Scenario 1: testing blocklisting of client IP --------")
        test_client = random.choice(client_nodes)
        test_pool = random.choice(pool_names)
        test_image = random.choice(rbd_images[test_pool])
        log.info(
            f"Client selected to be blocklisted : {test_client.hostname}"
            f"Testing single client block. IP : {test_client.ip_address}"
        )
        if not rados_obj.add_client_blocklisting(ip=test_client.ip_address):
            log.error(f"Could not add IP {test_client.ip_address} in blocklist")
            raise Exception("IP could not be blocklisted error")
        time.sleep(10)

        log.debug(
            f"Client IP : {test_client.ip_address} blocklisted. testing IO access"
        )
        out, status = rados_obj.rbd_bench_write(
            client_obj=test_client, pool_name=test_pool, image_name=test_image
        )
        if status:
            log.error("Able to write into image from client after blocklisting it")
            raise Exception("IOs working with Blocklist error")
        log.debug(
            f"Error msg displayed when IO's are attempted during blocklisting : \n{out}"
        )
        log.debug(
            f"Tested working of blocklisting of clinet {test_client.hostname}. working as expected\n"
            f"Testing if other non-blocklisted clients are still able to serve IO's "
        )

        for client in client_nodes:
            if client.hostname != test_client.hostname:
                log.debug(
                    f"Testing IO access on : {client.hostname}, which is not blocklisted"
                )
                out, status = rados_obj.rbd_bench_write(
                    client_obj=client, pool_name=test_pool, image_name=test_image
                )
                if not status:
                    log.error(
                        "UnAble to write into image from client after blocklisting peer client IP"
                    )
                    log.error(
                        f"\nBlocklisted client details :\n Name : {test_client.hostname}\n "
                        f"IP : {test_client.ip_address}\n Subnet : {test_client.subnet}"
                        f"\n Test Client Details (non-blocklisted):\n Name : {client.hostname}\n "
                        f"IP : {client.ip_address}\n Subnet : {client.subnet}"
                    )
                    raise Exception("IOs working with Blocklist error")
                log.info(
                    f"IO's working as expected on non-blocklisted client : {client.hostname}"
                )
        log.info(
            "Tested IO access on all non-blocklisted clients. Working as expected.\n"
        )
        log.debug(
            f"Proceeding to Removing the blocklisting from client ip : {test_client.ip_address}"
        )

        if not rados_obj.rm_client_blocklisting(ip=test_client.ip_address):
            log.error(f"Could not remove IP {test_client.ip_address} from blocklist")
            raise Exception("IP could not be removed from blocklisted error")
        log.info("Completed testing of IP blocklisting")

        log.info("---- Scenario 2: testing blocklisting of client CIDR -----")
        test_client = random.choice(client_nodes)
        test_pool = random.choice(pool_names)
        test_image = random.choice(rbd_images[test_pool])
        clients_blocked = get_all_clients_in_cidr(
            cidr=test_client.subnet, clients=client_nodes
        )

        log.debug(
            f"All the clients that would be blocklisted are : {[cli.hostname for cli in clients_blocked]}"
        )

        log.info(
            f"Client CIDR selected to be blocklisted : {test_client.hostname}"
            f"Testing CIDR level client blocklisting. IP : {test_client.subnet}"
        )
        if not rados_obj.add_client_blocklisting(cidr=test_client.subnet):
            log.error(f"Could not add CIDR {test_client.subnet} in blocklist")
            raise Exception("CIDR could not be blocklisted error")
        time.sleep(10)

        log.debug(
            f"Client IP : {test_client.subnet} blocklisted. testing IO access on all clients belonging to that subnet"
        )
        for client in clients_blocked:
            out, status = rados_obj.rbd_bench_write(
                client_obj=client, pool_name=test_pool, image_name=test_image
            )
            if status:
                log.error("Able to write into image from client after blocklisting it")
                raise Exception("IOs working with Blocklist error")
            log.debug(
                f"Error msg displayed when IO's are attempted during blocklisting : \n{out}"
            )

        log.debug(
            f"Tested working of blocklisting of clinet subnet {test_client.hostname}. working as expected"
            f"Testing if other non-blocklisted clients from other subnets are still able to serve IO's "
        )

        for client in client_nodes:
            if client.hostname not in [host.hostname for host in clients_blocked]:
                log.debug(
                    f"Testing IO access on : {client.hostname}, which is not blocklisted"
                )
                out, status = rados_obj.rbd_bench_write(
                    client_obj=client, pool_name=test_pool, image_name=test_image
                )
                if not status:
                    log.error(
                        "UnAble to write into image from client after blocklisting peer client CIDR"
                    )
                    log.error(
                        f"\nBlocklisted client details :\n Name : {test_client.hostname}\n "
                        f"IP : {test_client.ip_address}\n Subnet : {test_client.subnet}"
                        f"\n Test Client Details (non-blocklisted):\n Name : {client.hostname}\n "
                        f"IP : {client.ip_address}\n Subnet : {client.subnet}"
                    )
                    raise Exception("IOs working with Blocklist error")
                log.info(
                    f"IO's working as expected on non-blocklisted client : {client.hostname}"
                )
        log.info(
            "Tested IO access on all non-blocklisted clients. Working as expected."
        )

        log.debug(
            f"Proceeding to Removing the blocklisting from client cidr : {test_client.ip_address}"
        )

        if not rados_obj.rm_client_blocklisting(cidr=test_client.subnet):
            log.error(f"Could not remove CIDR {test_client.subnet} from blocklist")
            raise Exception("CIDR could not be removed from blocklisted error")
        log.info("Completed testing of CIDR blocklisting")

    except Exception as err:
        log.error(f"Hit Exception : {err} during execution. Failed...")
        return 1
    finally:
        log.info("----------------- IN FINALLY BLOCK ------------------")
        if not rados_obj.rm_client_blocklisting(cidr=test_client.subnet):
            log.error(f"Could not remove CIDR {test_client.subnet} from blocklist")
            return 1
        for pool in pool_names:
            rados_obj.delete_pool(pool=pool)
        # log cluster health
        rados_obj.log_cluster_health()
        # check for crashes after test execution
        if rados_obj.check_crash_status():
            log.error("Test failed due to crash at the end of test")
            return 1
    log.info("Completed testing CIDR blocklisting of ceph clients. Pass!")
    return 0


def get_all_clients_in_cidr(cidr, clients):
    """
    Method to get all the client objects that are present in the same CIDR sent
    Args:
        cidr: CIDR to be searched
        clients: All client objects that are present on the cluster
    Returns:
        list of client objects that are present in the CIDR sent
    """
    log.debug(
        f"CIDR sent : {cidr}. Iterating through all clients to select nodes belonging to cidr sent"
    )
    cidr_list = []
    for client in clients:
        if client.subnet == cidr:
            cidr_list.append(client)
    return cidr_list
