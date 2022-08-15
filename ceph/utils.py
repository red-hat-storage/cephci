import datetime
import os
import re
import time
import traceback
from json import loads
from time import mktime

import requests
import yaml
from gevent import sleep
from htmllistparse import fetch_listing
from libcloud.common.exceptions import BaseHTTPError
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider

from compute.baremetal import CephBaremetalNode
from compute.ibm_vpc import CephVMNodeIBM, get_ibm_service
from compute.openstack import CephVMNodeV2, NetworkOpFailure, NodeError, VolumeOpFailure
from utility.log import Log
from utility.retry import retry
from utility.utils import generate_node_name

from .ceph import Ceph, CommandFailed, RolesContainer
from .parallel import parallel

log = Log(__name__)
RETRY_EXCEPTIONS = (NodeError, VolumeOpFailure, NetworkOpFailure)
DEFAULT_OSBS_SERVER = "http://file.rdu.redhat.com/~kdreyer/osbs/"


def cleanup_ibmc_ceph_nodes(ibm_cred, pattern):
    """
    Clean up the DNS records, instance and volumes that matches the given pattern.

    Args:
         ibm_cred     global configuration file(ibm)
         pattern      pattern to match instance name
    """
    glbs = ibm_cred.get("globals")
    ibmc = glbs.get("ibm-credentials")

    ibmc_client = get_ibm_service(
        access_key=ibmc["access-key"], service_url=ibmc["service-url"]
    )
    resp = ibmc_client.list_instances(vpc_name=ibmc["vpc_name"])
    if resp.get_status_code() != 200:
        log.warning("Failed to retrieve instances")
        return 1

    instances = [i for i in resp.get_result()["instances"] if pattern in i["name"]]
    log.info(f"Cleaning up instances : {instances}")

    while "next" in resp.get_result().keys():
        start = resp.get_result()["next"]["href"].split("start=")[-1]
        resp = ibmc_client.list_instances(start=start, vpc_name=ibmc["vpc_name"])
        if resp.get_status_code() != 200:
            log.warning("Failed to fetch instance details, breaking out.")
            break
        instance_list = [
            i for i in resp.get_result()["instances"] if pattern in i["name"]
        ]
        instances += instance_list

    # Throttling removal otherwise Cloudflare will blacklist us
    counter = 0
    with parallel() as p:
        for instance in instances:
            sleep(counter * 3)
            vsi = CephVMNodeIBM(
                access_key=ibmc["access-key"],
                service_url=ibmc["service-url"],
                node=instance,
            )
            p.spawn(vsi.delete, ibmc["zone_name"])
            counter += 1

    log.info(f"Done cleaning up nodes with pattern {pattern}")


def create_baremetal_ceph_nodes(cluster_conf):
    """
    Creates the nodes with cluster details provided

    Args:
        cluster_conf (dict):    Information regarding the participating cluster

    Returns:
        List of CephNode
    """
    log.info("Creating the baremetal ceph nodes")
    nodes = cluster_conf["ceph-cluster"]["nodes"]
    ceph_nodes = dict()

    with parallel() as p:
        for n, node in enumerate(nodes):
            node_id = node.get("id") or f"node{n}"
            params = dict(
                {
                    "ip": node.get("ip"),
                    "hostname": node.get("hostname"),
                    "root_password": node.get("root_password"),
                    "root_private_key": node.get("root_private_key"),
                    "role": RolesContainer(node.get("role")),
                    "no-of-volumes": len(node.get("volumes", [])),
                    "volumes": node.get("volumes"),
                    "subnet": cluster_conf["ceph-cluster"]["networks"]["public"][0],
                    "location": node.get("location"),
                    "id": node_id,
                }
            )

            p.spawn(setup_vm_node_baremetal, node_id, ceph_nodes, **params)

    log.info("Done creating nodes")
    return ceph_nodes


def setup_vm_node_baremetal(node, ceph_nodes, **params):
    """
    Create the VM node using details provided.
    """
    vm = CephBaremetalNode(**params)
    vm.role = params["role"]
    ceph_nodes[node] = vm


def create_ibmc_ceph_nodes(
    cluster_conf, inventory, ibm_creds, run_id, instances_name=None
):
    """
    creates instances in IBM cloud

    Args:
         cluster_conf     configuration of cluster
         inventory        Instance configuration file
         ibm_creds        global configuration file(ibm)
         run_id           unique id for the run
         instances_name   Name of the instance
    """
    log.info("testing ibm stage")
    ibm_glbs = ibm_creds.get("globals")
    ibm_cred = ibm_glbs.get("ibm-credentials")
    params = dict()
    ceph_cluster = cluster_conf.get("ceph-cluster")
    ceph_nodes = dict()
    if ceph_cluster.get("inventory"):
        inventory_path = os.path.abspath(ceph_cluster.get("inventory"))
        with open(inventory_path, "r") as inventory_stream:
            inventory = yaml.safe_load(inventory_stream)
    node_count = 0

    params["cloud-data"] = inventory.get("instance").get("setup")
    params["accesskey"] = ibm_cred["access-key"]
    params["service_url"] = ibm_cred["service-url"]
    params["zone_name"] = ibm_cred["zone_name"]
    params["vpc_name"] = ibm_cred["vpc_name"]
    params["zone_id_model_name"] = ibm_cred["zone_id_model_name"]

    if inventory.get("instance").get("create"):
        if ceph_cluster.get("image-name"):
            params["image-name"] = ceph_cluster.get("image-name")
        else:
            params["image-name"] = (
                inventory.get("instance").get("create").get("image-name")
            )

        params["cluster-name"] = ceph_cluster.get("name")
        params["network_name"] = (
            inventory.get("instance").get("create").get("network_name")
        )
        params["private_key"] = (
            inventory.get("instance").get("create").get("private_key")
        )
        params["group_access"] = (
            inventory.get("instance").get("create").get("group_access")
        )
        params["profile"] = inventory.get("instance").get("create").get("profile")

        if params.get("root-login") is False:
            params["root-login"] = False
        else:
            params["root-login"] = True

        with parallel() as p:
            for node in range(1, 100):
                node = "node" + str(node)
                if not ceph_cluster.get(node):
                    break

                node_dict = ceph_cluster.get(node)
                node_params = params.copy()
                node_params["role"] = RolesContainer(node_dict.get("role"))
                node_params["id"] = node_dict.get("id") or node
                node_params["location"] = node_dict.get("location")
                node_params["node-name"] = generate_node_name(
                    node_params.get("cluster-name", "ceph"),
                    instances_name,
                    run_id,
                    node,
                    node_params["role"],
                )

                if node_dict.get("no-of-volumes"):
                    node_params["no-of-volumes"] = node_dict.get("no-of-volumes")
                    node_params["size-of-disks"] = node_dict.get("disk-size")
                    # osd-scenario option is not mandatory and,
                    # can be used only for specific OSD_SCENARIO
                    node_params["osd-scenario"] = node_dict.get("osd-scenario")

                if node_dict.get("image-name"):
                    node_params["image-name"] = node_dict.get("image-name")

                if node_dict.get("cloud-data"):
                    node_params["cloud-data"] = node_dict.get("cloud-data")

                # Throttling the spawning of VSI's to avoid hammering of provisioner
                sleep(node_count * 5)
                node_count += 1

                p.spawn(setup_vm_node_ibm, node, ceph_nodes, **node_params)

    if len(ceph_nodes) != node_count:
        log.error(
            f"Mismatch error in number of VMs creation. "
            f"Initiated: {node_count}  \tSpawned: {len(ceph_nodes)}"
        )
        raise NodeError("Required number of nodes not created")

    log.info("Done creating nodes")
    return ceph_nodes


@retry(RETRY_EXCEPTIONS, tries=3, delay=10)
def setup_vm_node_ibm(node, ceph_nodes, **params):
    """
    Create the VM node using IBM API calls.

    The retry decorator will trigger a rerun when a soft error is encountered. The VM
    node is removed in exception scope before throwing raising the exception again.
    """
    vm = None
    try:
        vm = CephVMNodeIBM(
            access_key=params["accesskey"], service_url=params["service_url"]
        )

        vm.create(
            node_name=params["node-name"],
            image_name=params["image-name"],
            network_name=params["network_name"],
            private_key=params["private_key"],
            vpc_name=params["vpc_name"],
            profile=params["profile"],
            group_access=params["group_access"],
            zone_name=params["zone_name"],
            zone_id_model_name=params["zone_id_model_name"],
            size_of_disks=params.get("size-of-disks", 0),
            no_of_volumes=params.get("no-of-volumes", 0),
            userdata=params.get("cloud-data", ""),
        )

        vm.role = params["role"]
        vm.root_login = params["root-login"]
        vm.osd_scenario = params.get("osd-scenario")
        vm.location = params.get("location")
        vm.id = params.get("id")
        ceph_nodes[node] = vm
    except RETRY_EXCEPTIONS as retry_except:
        log.warning(retry_except, exc_info=True)
        if vm is not None:
            vm.delete(params["zone_name"])

        raise
    except BaseException as be:  # noqa
        log.error(be, exc_info=True)
        raise


def create_ceph_nodes(
    cluster_conf, inventory, osp_cred, run_id, instances_name=None, enable_eus=False
):
    osp_glbs = osp_cred.get("globals")
    os_cred = osp_glbs.get("openstack-credentials")
    params = dict()
    ceph_cluster = cluster_conf.get("ceph-cluster")

    if ceph_cluster.get("inventory"):
        inventory_path = os.path.abspath(ceph_cluster.get("inventory"))
        with open(inventory_path, "r") as inventory_stream:
            inventory = yaml.safe_load(inventory_stream)
    node_count = 0
    params["cloud-data"] = inventory.get("instance").get("setup")
    params["username"] = os_cred["username"]
    params["password"] = os_cred["password"]
    params["auth-url"] = os_cred["auth-url"]
    params["auth-version"] = os_cred["auth-version"]
    params["tenant-name"] = os_cred["tenant-name"]
    params["service-region"] = os_cred["service-region"]
    params["domain"] = os_cred["domain"]
    params["tenant-domain-id"] = os_cred["tenant-domain-id"]
    params["keypair"] = os_cred.get("keypair", None)
    ceph_nodes = dict()
    if enable_eus and not inventory.get("instance").get("eus-supported", False):
        raise Exception("EUS release is not supported for this distro")
    if inventory.get("instance").get("create"):
        if ceph_cluster.get("image-name"):
            params["image-name"] = ceph_cluster.get("image-name")
        else:
            params["image-name"] = (
                inventory.get("instance").get("create").get("image-name")
            )

        params["cluster-name"] = ceph_cluster.get("name")
        params["vm-size"] = inventory.get("instance").get("create").get("vm-size")
        params["vm-network"] = inventory.get("instance").get("create").get("vm-network")

        if params.get("root-login") is False:
            params["root-login"] = False
        else:
            params["root-login"] = True

        with parallel() as p:
            for node in range(1, 100):
                sleep(10 * node)
                node = "node" + str(node)
                if not ceph_cluster.get(node):
                    break

                node_dict = ceph_cluster.get(node)
                node_params = params.copy()
                node_params["role"] = RolesContainer(node_dict.get("role"))
                user = os.getlogin()
                node_params["id"] = node_dict.get("id") or node
                node_params["location"] = node_dict.get("location")
                node_params["node-name"] = generate_node_name(
                    node_params.get("cluster-name", "ceph"),
                    instances_name or user,
                    run_id,
                    node,
                    node_params["role"],
                )

                node_params["networks"] = node_dict.get("networks", [])
                if node_dict.get("no-of-volumes"):
                    node_params["no-of-volumes"] = node_dict.get("no-of-volumes")
                    node_params["size-of-disks"] = node_dict.get("disk-size")
                    # osd-scenario option is not mandatory and,
                    # can be used only for specific OSD_SCENARIO
                    node_params["osd-scenario"] = node_dict.get("osd-scenario")

                if node_dict.get("image-name"):
                    node_params["image-name"] = node_dict.get("image-name")

                if node_dict.get("cloud-data"):
                    node_params["cloud-data"] = node_dict.get("cloud-data")
                node_count += 1
                p.spawn(setup_vm_node, node, ceph_nodes, **node_params)

    if len(ceph_nodes) != node_count:
        log.error(
            f"Mismatch error in number of VMs creation. "
            f"Initiated: {node_count}  \tSpawned: {len(ceph_nodes)}"
        )
        raise NodeError("Required number of nodes not created")

    log.info("Done creating nodes")
    return ceph_nodes


@retry(RETRY_EXCEPTIONS, tries=3, delay=10)
def setup_vm_node(node, ceph_nodes, **params):
    """
    Create the VM node using OpenStack API calls.

    The retry decorator will trigger a rerun when a soft error is encountered. The VM
    node is removed in exception scope before throwing raising the exception again.
    """
    vm = None
    try:
        vm = CephVMNodeV2(
            username=params["username"],
            password=params["password"],
            auth_url=params["auth-url"],
            auth_version=params["auth-version"],
            tenant_name=params["tenant-name"],
            tenant_domain_id=params["tenant-domain-id"],
            service_region=params["service-region"],
            domain_name=params["domain"],
        )

        vm.create(
            node_name=params["node-name"],
            image_name=params["image-name"],
            vm_size=params["vm-size"],
            cloud_data=params["cloud-data"],
            vm_network=params.get("networks", []),
            size_of_disks=params.get("size-of-disks", 0),
            no_of_volumes=params.get("no-of-volumes", 0),
        )

        vm.role = params["role"]
        vm.root_login = params["root-login"]
        vm.keypair = params["keypair"]
        vm.osd_scenario = params.get("osd-scenario", False)
        vm.location = params.get("location")
        vm.id = params.get("id")
        ceph_nodes[node] = vm
    except RETRY_EXCEPTIONS as retry_except:
        log.warning(retry_except, exc_info=True)
        if vm is not None:
            vm.delete()

        raise
    except BaseException as be:  # noqa
        log.error(be, exc_info=True)
        raise


def get_openstack_driver(yaml):
    OpenStack = get_driver(Provider.OPENSTACK)
    glbs = yaml.get("globals")
    os_cred = glbs.get("openstack-credentials")
    username = os_cred["username"]
    password = os_cred["password"]
    auth_url = os_cred["auth-url"]
    auth_version = os_cred["auth-version"]
    tenant_name = os_cred["tenant-name"]
    service_region = os_cred["service-region"]
    domain_name = os_cred["domain"]
    tenant_domain_id = os_cred["tenant-domain-id"]
    driver = OpenStack(
        username,
        password,
        ex_force_auth_url=auth_url,
        ex_force_auth_version=auth_version,
        ex_tenant_name=tenant_name,
        ex_force_service_region=service_region,
        ex_domain_name=domain_name,
        ex_tenant_domain_id=tenant_domain_id,
    )
    return driver


def cleanup_ceph_nodes(osp_cred, pattern=None, timeout=300):
    user = os.getlogin()
    name = pattern if pattern else "-{user}-".format(user=user)
    driver = get_openstack_driver(osp_cred)
    timeout = datetime.timedelta(seconds=timeout)
    with parallel() as p:
        for volume in driver.list_volumes():
            if volume.name is None:
                log.info("Volume has no name, skipping")
            elif name in volume.name:
                p.spawn(volume_cleanup, volume, osp_cred)
                sleep(1)
        sleep(30)
        log.info("Done cleaning up volumes")
        for node in driver.list_nodes():
            if name in node.name:
                starttime = datetime.datetime.now()
                log.info(
                    "Destroying node {node_name} with {timeout} timeout".format(
                        node_name=node.name, timeout=timeout
                    )
                )
                while True:
                    try:
                        node.destroy()
                        break
                    except AttributeError:
                        if datetime.datetime.now() - starttime > timeout:
                            raise RuntimeError(
                                "Failed to destroy node {node_name} with {timeout} timeout:\n{stack_trace}".format(
                                    node_name=node.name,
                                    timeout=timeout,
                                    stack_trace=traceback.format_exc(),
                                )
                            )
                        else:
                            sleep(1)
                sleep(5)
    log.info("Done cleaning up nodes")


def volume_cleanup(volume, osp_cred):
    log.info("Removing volume %s", volume.name)
    errors = {}
    driver = get_openstack_driver(osp_cred)
    sleep(10)
    timeout = datetime.timedelta(seconds=200)
    starttime = datetime.datetime.now()
    while True:
        try:
            volobj = driver.ex_get_volume(volume.id)
            driver.detach_volume(volobj)
            sleep(30)
            driver.destroy_volume(volobj)
            break
        except BaseHTTPError as e:
            log.error(e, exc_info=True)
            errors.update({volume.name: e.message})
            if errors:
                if datetime.datetime.now() - starttime > timeout:
                    for vol, err in errors.items():
                        log.error(
                            "Error destroying {vol}: {err}".format(vol=vol, err=err)
                        )
                    return 1


def keep_alive(ceph_nodes):
    for node in ceph_nodes:
        node.exec_command(cmd="uptime", check_ec=False)


def setup_repos(ceph, base_url, installer_url=None):
    repos = ["MON", "OSD", "Tools", "Calamari", "Installer"]
    base_repo = generate_repo_file(base_url, repos)
    base_file = ceph.remote_file(
        sudo=True, file_name="/etc/yum.repos.d/rh_ceph.repo", file_mode="w"
    )
    base_file.write(base_repo)
    base_file.flush()
    if installer_url is not None:
        installer_repos = ["Agent", "Main", "Installer"]
        inst_repo = generate_repo_file(installer_url, installer_repos)
        log.info("Setting up repo on %s", ceph.hostname)
        inst_file = ceph.remote_file(
            sudo=True, file_name="/etc/yum.repos.d/rh_ceph_inst.repo", file_mode="w"
        )
        inst_file.write(inst_repo)
        inst_file.flush()


def check_ceph_healthly(
    ceph_mon, num_osds, num_mons, build, mon_container=None, timeout=300
):
    """
    Function to check ceph is in healthy state

    Args:
       ceph_mon: monitor node
       num_osds: number of osds in cluster
       num_mons: number of mons in cluster
       build: rhcs build version
       mon_container: monitor container name if monitor is placed in the container
       timeout: 300 seconds(default) max time to check

    :note:  if cluster is not healthy within timeout period return 1

    Returns:
       return 0 when ceph is in healthy state, else 1
    """

    timeout = datetime.timedelta(seconds=timeout)
    starttime = datetime.datetime.now()
    pending_states = ["peering", "activating", "creating"]
    valid_states = ["active+clean"]

    out = None
    while datetime.datetime.now() - starttime <= timeout:
        if mon_container:
            distro_info = ceph_mon.distro_info
            distro_ver = distro_info["VERSION_ID"]
            if distro_ver.startswith("8"):
                out, _ = ceph_mon.exec_command(
                    cmd="sudo podman exec {container} ceph -s".format(
                        container=mon_container
                    )
                )
            else:
                out, _ = ceph_mon.exec_command(
                    cmd="sudo docker exec {container} ceph -s".format(
                        container=mon_container
                    )
                )
        else:
            out, _ = ceph_mon.exec_command(cmd="sudo ceph -s")

        if not any(state in out for state in pending_states):
            if all(state in out for state in valid_states):
                break
        sleep(5)
    log.info(out)
    if not all(state in out for state in valid_states):
        log.error("Valid States are not found in the health check")
        return 1
    if build.startswith("4"):
        match = re.search(r"(\d+)\s+osds:\s+(\d+)\s+up\s\(\w+\s\w+\),\s(\d+)\sin", out)
    else:
        match = re.search(r"(\d+)\s+osds:\s+(\d+)\s+up,\s+(\d+)\s+in", out)
    all_osds = int(match.group(1))
    up_osds = int(match.group(2))
    in_osds = int(match.group(3))
    if num_osds != all_osds:
        log.error("Not all osd's are up. %s / %s" % (num_osds, all_osds))
        return 1
    if up_osds != in_osds:
        log.error("Not all osd's are in. %s / %s" % (up_osds, all_osds))
        return 1

    # attempt luminous pattern first, if it returns none attempt jewel pattern
    match = re.search(r"(\d+) daemons, quorum", out)
    if not match:
        match = re.search(r"(\d+) mons at", out)
    all_mons = int(match.group(1))
    if all_mons != num_mons:
        log.error("Not all monitors are in cluster")
        return 1
    if "HEALTH_ERR" in out:
        log.error("HEALTH in ERROR STATE")
        return 1
    return 0


def generate_repo_file(base_url, repos):
    return Ceph.generate_repository_file(base_url, repos)


def get_iso_file_url(base_url):
    return Ceph.get_iso_file_url(base_url)


def create_ceph_conf(
    fsid,
    mon_hosts,
    pg_num="128",
    pgp_num="128",
    size="2",
    auth="cephx",
    pnetwork="172.16.0.0/12",
    jsize="1024",
):
    fsid = "fsid = " + fsid + "\n"
    mon_init_memb = "mon initial members = "
    mon_host = "mon host = "
    public_network = "public network = " + pnetwork + "\n"
    auth = "auth cluster required = cephx\nauth service \
            required = cephx\nauth client required = cephx\n"
    jsize = "osd journal size = " + jsize + "\n"
    size = "osd pool default size = " + size + "\n"
    pgnum = "osd pool default pg num = " + pg_num + "\n"
    pgpnum = "osd pool default pgp num = " + pgp_num + "\n"
    for mhost in mon_hosts:
        mon_init_memb = mon_init_memb + mhost.shortname + ","
        mon_host = mon_host + mhost.internal_ip + ","
    mon_init_memb = mon_init_memb[:-1] + "\n"
    mon_host = mon_host[:-1] + "\n"
    conf = "[global]\n"
    conf = (
        conf
        + fsid
        + mon_init_memb
        + mon_host
        + public_network
        + auth
        + size
        + jsize
        + pgnum
        + pgpnum
    )
    return conf


def setup_deb_repos(node, ubuntu_repo):
    node.exec_command(cmd="sudo rm -f /etc/apt/sources.list.d/*")
    repos = ["MON", "OSD", "Tools"]
    for repo in repos:
        cmd = (
            "sudo echo deb "
            + ubuntu_repo
            + "/{0}".format(repo)
            + " $(lsb_release -sc) main"
        )
        node.exec_command(cmd=cmd + " > " + "/tmp/{0}.list".format(repo))
        node.exec_command(
            cmd="sudo cp /tmp/{0}.list /etc/apt/sources.list.d/".format(repo)
        )
    ds_keys = [
        "https://www.redhat.com/security/897da07a.txt",
        "https://www.redhat.com/security/f21541eb.txt",
        # 'https://prodsec.redhat.com/keys/00da75f2.txt',
        # TODO: replace file file.rdu.redhat.com/~kdreyer with prodsec.redhat.com when it's back
        "http://file.rdu.redhat.com/~kdreyer/keys/00da75f2.txt",
        "https://www.redhat.com/security/data/fd431d51.txt",
    ]

    for key in ds_keys:
        wget_cmd = "sudo wget -O - " + key + " | sudo apt-key add -"
        node.exec_command(cmd=wget_cmd)
    node.exec_command(cmd="sudo apt-get update")


def setup_deb_cdn_repo(node, build=None):
    user = "redhat"
    passwd = "OgYZNpkj6jZAIF20XFZW0gnnwYBjYcmt7PeY76bLHec9"
    num = build.split(".")[0]
    cmd = (
        "umask 0077; echo deb https://{user}:{passwd}@rhcs.download.redhat.com/{num}-updates/Tools "
        "$(lsb_release -sc) main | tee /etc/apt/sources.list.d/Tools.list".format(
            user=user, passwd=passwd, num=num
        )
    )
    node.exec_command(sudo=True, cmd=cmd)
    node.exec_command(
        sudo=True,
        cmd="wget -O - https://www.redhat.com/security/fd431d51.txt | apt-key add -",
    )
    node.exec_command(sudo=True, cmd="apt-get update")


def update_ca_cert(node, cert_url, out_file, timeout=120, check_ec=True):
    """
    Update CA cert in the nodes.
      by default options picked for RHEL platforms

    Args:
        node: node object
        cert_url: path to download certificate
        out_file: output file name
        timeout: timeout in seconds
        check_ec: bool by default true, checks the error code
    """
    output_dir = "/etc/pki/ca-trust/source/anchors/"
    update_cmd = "update-ca-trust extract"

    if node.pkg_type == "deb":
        output_dir = "/usr/local/share/ca-certificates/"
        update_cmd = "update-ca-certificates"

    download_cmd = f"curl -m 120 --fail {cert_url} -o {output_dir}{out_file}"
    for cmd in [download_cmd, update_cmd]:
        node.exec_command(
            sudo=True,
            cmd=cmd,
            timeout=timeout,
            check_ec=check_ec,
        )


def search_ethernet_interface(ceph_node, ceph_node_list):
    """
    Search interface on the given node node which allows every node in the cluster accesible by it's shortname.

    Args:
        ceph_node (ceph.ceph.CephNode): node where check is performed
        ceph_node_list(list): node list to check
    """
    return ceph_node.search_ethernet_interface(ceph_node_list)


def config_ntp(ceph_node, cloud_type="openstack"):
    """
    Configure NTP/Chronyc service based on the OS platform

    Args:
        ceph_node:      Ceph Node
        cloud_type:     IaaS provider
    """
    distro_info = ceph_node.distro_info
    distro_ver = distro_info["VERSION_ID"]
    key = "7" if distro_ver.split(".")[0] == "7" else "all"

    ntp_config = {
        "7": [
            "sed -i '/server*/d' /etc/ntp.conf",
            "echo 'server clock.corp.redhat.com iburst' | tee -a /etc/ntp.conf",
        ],
        "all": [
            "sed -i '/pool*/d;/server*/d' /etc/chrony.conf",
            "sed -i '1i server clock.corp.redhat.com iburst' /etc/chrony.conf",
        ],
    }
    if cloud_type not in ["ibmc", "baremetal"]:
        # IBM-Cloud hosted environments are configured via the DHCP client.
        commands = ntp_config[key]
        for cmd in commands:
            ceph_node.exec_command(sudo=True, cmd=cmd, long_running=True)

    ntp_run = {
        "7": [
            "ntpd -gq",
            "systemctl enable ntpd",
            "systemctl start ntpd",
            "ntpq -p",
            "ntpstat",
        ],
        "all": [
            "systemctl stop chronyd.service",
            "systemctl start chronyd.service",
            "chronyc makestep",
            "chronyc sources",
        ],
    }
    commands = ntp_run[key]

    for cmd in commands:
        ceph_node.exec_command(sudo=True, cmd=cmd, long_running=True)

    return True


def get_ceph_versions(ceph_nodes, containerized=False):
    """
    Log and return the ceph or ceph-ansible versions for each node in the cluster.

    Args:
        ceph_nodes: nodes in the cluster
        containerized: is the cluster containerized or not

    Returns:
        A dict of the name / version pair for each node or container in the cluster
    """
    versions_dict = {}

    for node in ceph_nodes:
        try:
            if node.role == "installer":
                if node.pkg_type == "rpm":
                    out, rc = node.exec_command(cmd="rpm -qa | grep ceph-ansible")
                else:
                    out, rc = node.exec_command(cmd="dpkg -s ceph-ansible")
                log.info(out)
                versions_dict.update({node.shortname: out.rstrip()})

            else:
                if containerized:
                    containers = []
                    # client and grafana role not supported for ceph commands
                    if node.role == "client" or node.role == "grafana":
                        pass
                    else:
                        distro_info = node.distro_info
                        distro_ver = distro_info["VERSION_ID"]
                        if distro_ver.startswith("8"):
                            out, rc = node.exec_command(
                                sudo=True, cmd='podman ps --format "{{.Names}}"'
                            )
                        else:
                            out, rc = node.exec_command(
                                sudo=True, cmd='docker ps --format "{{.Names}}"'
                            )
                        containers = [
                            container
                            for container in out.split("\n")
                            if container != ""
                        ]
                        log.info("Containers: {}".format(containers))

                    for container_name in containers:
                        # node-exporter container not supported for ceph commands
                        if container_name != "node-exporter":
                            if distro_ver.startswith("8"):
                                out, rc = node.exec_command(
                                    sudo=True,
                                    cmd="sudo podman exec {container} ceph --version".format(
                                        container=container_name
                                    ),
                                )
                            else:
                                out, rc = node.exec_command(
                                    sudo=True,
                                    cmd="sudo docker exec {container} ceph --version".format(
                                        container=container_name
                                    ),
                                )
                        log.info(out)
                        versions_dict.update({container_name: out.rstrip()})

                else:
                    #  client and grafana role not supported for ceph commands
                    if node.role == "client" or node.role == "grafana":
                        pass
                    out, rc = node.exec_command(cmd="ceph --version")
                    output = out.rstrip()
                    log.info(output)
                    versions_dict.update({node.shortname: output})

        except CommandFailed:
            log.info("No ceph versions on {}".format(node.shortname))

    return versions_dict


def hard_reboot(gyaml, name=None):
    user = os.getlogin()
    if name is None:
        name = "ceph-" + user
    driver = get_openstack_driver(gyaml)
    for node in driver.list_nodes():
        if node.name.startswith(name):
            log.info("Hard-rebooting %s" % node.name)
            driver.ex_hard_reboot_node(node)

    return 0


def node_power_failure(gyaml, sleep_time=300, name=None):
    user = os.getlogin()
    if name is None:
        name = "ceph-" + user
    driver = get_openstack_driver(gyaml)
    for node in driver.list_nodes():
        if node.name.startswith(name):
            log.info("Doing power-off on %s" % node.name)
            driver.ex_stop_node(node)
            time.sleep(20)
            op = driver.ex_get_node_details(node)
            if op.state == "stopped":
                log.info("Node stopped successfully")
            time.sleep(sleep_time)
            log.info("Doing power-on on %s" % node.name)
            driver.ex_start_node(node)
            time.sleep(20)
            op = driver.ex_get_node_details(node)
            if op.state == "running":
                log.info("Node restarted successfully")
            time.sleep(20)
    return 0


def get_root_permissions(node, path):
    """
    Transfer ownership of root to current user for the path given. Recursive.
    Args:
        node(ceph.ceph.CephNode):
        path: file path
    """
    node.obtain_root_permissions(path)


def get_public_network(nodes):
    """Get the configured public network subnet from nodes in the cluster.

    Args:
        nodes: cluster nodes

    Returns:
        (str) public network subnet(s)
    """
    subnets = []
    for node in nodes:
        if node.subnet not in subnets:
            subnets.append(node.subnet)
    return ",".join(subnets)


def get_disk_info(node):
    """
    Get node disk(s) info

    Args:
        node: remote node object
    Returns:
        disks: remote disk details
    """
    _BOOT_DISK = "findmnt -v -n -T / -o SOURCE "
    _GET_DISKS = "lsblk -np -r -o {} "

    # get boot disk
    out, _ = node.exec_command(cmd="%s" % _BOOT_DISK)

    boot_disk = re.sub(r"\d", "", out)
    log.info("Boot disk found : %s", boot_disk)

    # get disk and skip boot disk
    headers = ["name", "type"]
    out, _ = node.exec_command(
        cmd="{} | grep disk".format(_GET_DISKS.format(",".join(headers)))
    )
    disks_info = out.strip().split("\n")

    disks = []
    for disk in list([x for x in disks_info if x]):
        disk_info = dict(list(zip(headers, re.split(r"\s+", disk))))
        disk_name = disk_info["name"]

        if boot_disk in disk_name:
            continue
        disks.append(disk_name)

    return disks


def get_node_by_id(cluster, node_name):
    """
    Fetch node using provided node substring::

        As per the naming convention used at VM creation, where each node.shortname
        is framed with hyphen("-") separated string as below, please refer
        ceph.utils.create_ceph_nodes definition
            "ceph-<name>-node1-<roles>"

            name: RHOS-D username or provided --instances-name
            roles: roles attached to node in inventory file

        In this method we use hyphen("-") appended to node_name string to try fetch exact node.
        for example,
            "node1" ----> "node1-"

        Note: But however if node_name doesn't follow naming convention as mentioned in
        inventory, the first searched node will be returned.
        for example,
            "node" ----> it might return any node which matched first, like node11.

        return None, If this cluster has no nodes with this substring.

    Args:
        cluster: ceph object
        node_name: node1        # try to use node<Id>
    Returns:
        node instance (CephVMNode)
    """
    for node in cluster.get_nodes():
        searches = re.findall(rf"{node_name}?\d*", node.hostname, re.IGNORECASE)
        for ele in searches:
            if ele == node_name:
                return node


def get_nodes_by_ids(cluster, node_names):
    """
    Fetch nodes using provided substring of nodes

    Returns node list which matched node ID(eg., 'node1')
    else all nodes object list if node_names is empty.

    Args:
        cluster: ceph object
        node_names: node name list (eg., ['node1'])

    Returns:
        node_list: list of nodes
    """
    if not node_names:
        return cluster.get_nodes()

    nodes = []
    for name in node_names:
        node = get_node_by_id(cluster, name)
        if node:
            nodes.append(node)
    return nodes


def fetch_image_builds(version):
    """
    Fetch ceph container image builds

        1) Search Share path for ceph image under DEFAULT_OSBS_SERVER
        2) look for particular RHCS version json files.
        3) Sort builds based on timestamp and return builds.

    todo: Fix when upgrade scenario needs image from source path
    """
    try:
        cwd, c_list = fetch_listing(DEFAULT_OSBS_SERVER, timeout=60)
        assert c_list, "Container file(s) not found"
        c_list = [i for i in c_list if i.endswith("json")]

        builds = dict()
        for comp in c_list:
            if version in comp.name:
                dt = datetime.datetime.fromtimestamp(mktime(comp.modified)).timestamp()
                builds.update({dt: comp})

        builds = [builds[k] for k in sorted(builds)]

        return builds
    except AssertionError as err:
        log.warning(err)
        raise AssertionError(f"Ceph Image builds not found : {DEFAULT_OSBS_SERVER}")


def fetch_build(version, custom_build):
    """
    Fetch build details based on the custom build
    Args:
        version: rhceph version
        custom_build:

    Returns:
        ceph image and compose

    custom_build:
        latest_build: latest ceph image to be considered
        last_previous_build: last but one ceph image(n-1)

    todo: Fix when upgrade scenario needs image from source path
    """
    builds = fetch_image_builds(version)

    def get_build_details(build):
        build = requests.get(f"{DEFAULT_OSBS_SERVER}{build.name}", verify=False).json()
        return build.get("compose_url"), build.get("repository")

    # To fetch (N-1) ceph image and build compose
    if custom_build == "last_previous_build":
        if len(builds) < 2:
            raise AssertionError("Only one Ceph Image build found, needed N-1")
        return get_build_details(builds[-2])
    elif custom_build == "latest_build":
        return get_build_details(builds[-1])
    else:
        raise NotImplementedError


def translate_to_ip(clusters, cluster_name: str, string: str) -> str:
    """
    Return the string after replacing node_ip: <node> pattern with IP address of <node>.

    In this method, the pattern {node_ip:<cluster>#<node>} would be replaced with the
    value of node.ipaddress.

    Args:
        clusters:       Ceph cluster instance
        cluster_name:   Name of the cluster under test.
        string:         String that needs to be searched

    Return:
        String with node IDs replaced with IP addresses
    """
    replaced_string = string
    node_list = re.findall("{node_ip:(.+?)}", string)

    for node in node_list:
        node_ = node
        if "#" in node:
            cluster_name, node = node.split("#")

        node_ip = get_node_by_id(clusters[cluster_name], node).ip_address
        replacement_pattern = "{node_ip:" + node_ + "}"
        replaced_string = re.sub(replacement_pattern, node_ip, replaced_string)

    return replaced_string


def set_container_info(ceph_cluster, config, use_cdn, containerized):
    """
    Set container information in ansible configuration
    Args:
        ceph_cluster: ceph cluster object
        use_cdn: boolean to check CDN
        config: test config
        containerized: boolean indicates containerized build.
    Returns:
        ansi_config
    """
    ansi_config = dict()

    if use_cdn:
        ceph_cluster.use_cdn = True
        config["use_cdn"] = True
        ansi_config["ceph_origin"] = "repository"
        ansi_config["ceph_repository_type"] = "cdn"
    else:
        if containerized:
            ansi_config["ceph_docker_registry"] = config.get("ceph_docker_registry")
            ansi_config["ceph_docker_image"] = config.get("ceph_docker_image")
            ansi_config["ceph_docker_image_tag"] = config.get("ceph_docker_image_tag")
    return ansi_config


def is_legacy_container_present(ceph_cluster):
    """
    Verifies if any legacy containers are present in any of the nodes in the cluster
    Args:
        ceph_cluster: The ceph_cluster object

    Returns:
        True if legacy containers are present in any of the nodes, else False
    """
    for node in ceph_cluster.get_nodes(ignore="client"):
        out, err = node.exec_command(cmd="cephadm ls", sudo=True)
        legacy_services = [x for x in loads(out) if x["style"] == "legacy"]
        if legacy_services:
            log.info(
                f"Legacy services present in cephadm node {node.hostname} after adopting the cluster"
            )
            return True

    return False
