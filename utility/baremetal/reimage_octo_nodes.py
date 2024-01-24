"""
Usage:
  reimage_octo_nodes.py [--username=<username>] [--password=<password>] --os_version=<version> --nodes=<nodes>
  reimage_octo_nodes.py (-h | --help)

Options:
  -h --help          Show this help message and exit
  --username=<username>   Username for authentication
  --password=<password>   Password for authentication

Examples:
  # Install OS version 8.6 on nodes cali0xx1 and cali0xx2 with custom username and password
  reimage_octo_nodes.py --version=8.6 --nodes=cali0xx1 cali0xx2 --username=myusername --password=mypassword

Note: This works only on Dell Servers. SuperMicro support yet to implement
Server details and their Manufacturer details are present in following document:
    https://docs.google.com/spreadsheets/d/1-s4eJ6z3rBoqmtrd-UInpqep-exNstUviwylFh33bgw/edit#gid=0
"""

import logging
import multiprocessing
import os
import subprocess
import threading
import time

import requests
from docopt import docopt

# Disable the InsecureRequestWarning
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from ceph.ceph import CommandFailed
from utility.baremetal.utils import (
    append_domain,
    authenticate,
    clean_up,
    create_install_dir,
    download_requirements,
    get_manufacturer,
    ping_node,
    validate_installation,
)
from utility.retry import retry
from utility.utils import generate_unique_id

LOG = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Define the base URL and credentials
base_url = "https://{}"
username = "xxxx"
password = "xxxx"

# Define a dictionary to store OS versions and their respective ISO URLs
image_list = {
    "9.3": "http://10.8.128.6/rhel93ks_1.iso",
    "8.6": "http://10.8.128.6/rhel86ks_updated.iso",
}
access_tokens = {}


# Support functions for Supermicro Servers
def mount_iso_supermicro(idrac_ip, os_version, username, password, stop_event):
    image_name = os.path.basename(image_list.get(os_version))
    if not image_name:
        raise CommandFailed(f"OS version {os_version} not present in ftp server")
    command = f"""
        {base_cmd} {idrac_ip} {username} {password} shell
        vmwa dev2iso {config_dir}/rhel93ks_1.iso
        """
    process = subprocess.Popen(
        "/bin/bash", stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    process.stdin.write(command)
    process.stdin.flush()  # Ensure the command is sent

    while not stop_event.is_set():
        # Keep the process open by sending a command periodically
        process.stdin.write("echo 'Still running...'\n")
        process.stdin.flush()
        time.sleep(10)

    # Gracefully exit the shell
    process.stdin.write("bye\n")
    process.stdin.flush()

    # Wait for the process to finish
    process.communicate()


def unmount_iso_supermicro(baremetal_obj, config_dir):
    LOG.info(f"config_dir : {config_dir}")
    smic_tool_path = f"{config_dir}/SMCIPMITool_2.26.0_build.220209_bundleJRE_Linux_x64"
    commands = f"""
    pwd
    cd {smic_tool_path}
    ./SMCIPMITool {baremetal_obj.node_ip} {baremetal_obj.username} {baremetal_obj.password} shell
    vmwa dev2stop
    """
    process = subprocess.Popen(
        "/bin/bash", stdin=subprocess.PIPE, stdout=subprocess.PIPE
    )
    out, err = process.communicate(commands.encode("utf-8"))
    LOG.info(out.decode("utf-8"))
    if "Device 2: None" not in out.decode("utf-8"):
        LOG.error("Unable to unmount ISO ")


def eject_iso_supermicro(idrac_ip, username, password):
    cmd = f"{base_cmd} {idrac_ip} {username} {password} shell"
    commands = f"""
        {cmd} shell
        vmwa dev2stop
        """
    process = subprocess.Popen(
        "/bin/bash", stdin=subprocess.PIPE, stdout=subprocess.PIPE
    )
    out, err = process.communicate(commands.encode("utf-8"))
    LOG.info(out.decode("utf-8"))
    if "Device 2: None" in out.decode("utf-8") or "No Mounted Device" not in out.decode(
        "utf-8"
    ):
        LOG.error("Unable to unmount ISO ")
        return False
    return True


@retry(CommandFailed, tries=5, delay=30)
def validate_attach(idrac_ip, username, password):
    cmd = f"{base_cmd} {idrac_ip} {username} {password} shell"
    commands = f"""
        {cmd} shell
        vmwa status
        """
    process = subprocess.Popen(
        "/bin/bash", stdin=subprocess.PIPE, stdout=subprocess.PIPE
    )
    out, err = process.communicate(commands.encode("utf-8"))
    LOG.info(out.decode("utf-8"))
    if "Device 2: ISO File [" in out.decode(
        "utf-8"
    ) or "Device 2 :Exist an effective Connect from others" in out.decode("utf-8"):
        pass  # Do nothing if either string is present
    else:
        LOG.error("Unable to unmount ISO ")
        raise CommandFailed("Dev not mounted Yet")
    return True


def update_boot_source_supermicro(idrac_ip, username, password):
    cmd = f"{base_cmd} {idrac_ip} {username} {password} shell"
    commands = f"""
                    {cmd} shell
                    ipmi power bootoption 8
                    """
    process = subprocess.Popen(
        "/bin/bash", stdin=subprocess.PIPE, stdout=subprocess.PIPE
    )
    out, err = process.communicate(commands.encode("utf-8"))
    LOG.info(out.decode("utf-8"))
    if "Set boot device done" not in out.decode("utf-8"):
        LOG.error("One time Boot device is set to CD/ROM")
        return False
    return True


def initiate_graceful_restart_supermicro(idrac_ip, username, password):
    cmd = f"{base_cmd} {idrac_ip} {username} {password} shell"
    commands = f"""
                {cmd} shell
                ipmi power reset
                """
    process = subprocess.Popen(
        "/bin/bash", stdin=subprocess.PIPE, stdout=subprocess.PIPE
    )
    out, err = process.communicate(commands.encode("utf-8"))
    LOG.info(out.decode("utf-8"))
    if "Done" not in out.decode("utf-8"):
        LOG.error("One time Boot device is set to CD/ROM")
        return False
    return True


# Support functions for Dell Servers
def eject_iso_dell(idrac_ip, username, password, access_tokens):
    eject_media_url = (
        base_url.format(idrac_ip)
        + "/redfish/v1/Managers/iDRAC.Embedded.1/VirtualMedia/CD/Actions/VirtualMedia.EjectMedia"
    )
    access_token = authenticate(idrac_ip, username, password, access_tokens)
    response = requests.post(
        eject_media_url,
        json={},
        headers={"X-Auth-Token": access_token, "Content-Type": "application/json"},
        verify=False,
    )
    return response.status_code


def mount_iso_dell(idrac_ip, os_version, username, password, access_tokens):
    virtual_media_url = (
        base_url.format(idrac_ip)
        + "/redfish/v1/Managers/iDRAC.Embedded.1/VirtualMedia/CD/Actions/VirtualMedia.InsertMedia"
    )
    access_token = authenticate(idrac_ip, username, password, access_tokens)
    image_name = os.path.basename(image_list.get(os_version))
    if not image_name:
        raise CommandFailed(f"OS version {os_version} not present in ftp server")
    iso_payload = {"Image": image_list.get(os_version), "Inserted": True}
    response = requests.post(
        virtual_media_url,
        json=iso_payload,
        headers={"X-Auth-Token": access_token, "Content-Type": "application/json"},
        verify=False,
    )
    return response.status_code


def update_boot_source_dell(idrac_ip, username, password, access_tokens):
    patch_url = base_url.format(idrac_ip) + "/redfish/v1/Systems/System.Embedded.1"
    access_token = authenticate(idrac_ip, username, password, access_tokens)
    patch_payload = {
        "Boot": {"BootSourceOverrideTarget": "Cd", "BootSourceOverrideEnabled": "Once"}
    }
    headers = {"X-Auth-Token": access_token, "Content-Type": "application/json"}
    response = requests.patch(
        patch_url, json=patch_payload, headers=headers, verify=False
    )
    return response.status_code


def initiate_graceful_restart_dell(idrac_ip, username, password, access_tokens):
    reset_url = (
        base_url.format(idrac_ip)
        + "/redfish/v1/Systems/System.Embedded.1/Actions/ComputerSystem.Reset"
    )
    access_token = authenticate(idrac_ip, username, password, access_tokens)
    reset_payload = {"ResetType": "GracefulRestart"}
    headers = {"X-Auth-Token": access_token, "Content-Type": "application/json"}
    response = requests.post(
        reset_url, json=reset_payload, headers=headers, verify=False
    )
    return response.status_code


@retry(CommandFailed, tries=15, delay=60)
def reimage_node(node_ip, os_version, username, password):
    node_ipmi = append_domain(node_ip)

    # Ping test
    if not ping_node(node_ipmi):
        print(f"Node {node_ipmi} is not pingable. Skipping...")
    manufacturer = get_manufacturer(node_ipmi, username, password, access_tokens)
    print(manufacturer)
    if "Supermicro" in manufacturer:
        # Eject the current ISO
        eject_status_code = eject_iso_supermicro(node_ipmi, username, password)
        if eject_status_code:
            print(f"ISO Ejected successfully for {node_ipmi}")
        else:
            print(f"Failed to eject ISO for {node_ipmi}")
        stop_event = threading.Event()
        # Mount the specified ISO
        mountIso = threading.Thread(
            target=mount_iso_supermicro,
            args=(node_ipmi, os_version, username, password, stop_event),
        )
        mountIso.start()
        validate_attach(node_ipmi, username, password)
        # Update boot source
        boot_source_status_code = update_boot_source_supermicro(
            node_ipmi, username, password
        )
        if boot_source_status_code:
            print(f"Boot source updated successfully for {node_ipmi}")
        else:
            print(f"Failed to update boot source for {node_ipmi}")

        # Initiate graceful restart
        restart_status_code = initiate_graceful_restart_supermicro(
            node_ipmi, username, password
        )
        if restart_status_code:
            print(f"Graceful restart initiated successfully for {node_ipmi}")
        else:
            print(f"Failed to initiate graceful restart for {node_ip}")
        rc = validate_installation(node_ip, os_version)
        stop_event.set()
        mountIso.join()
        return rc
    if "Dell Inc." in manufacturer:
        # Eject the current ISO
        eject_status_code = eject_iso_dell(node_ipmi, username, password, access_tokens)
        if eject_status_code == 204:
            print(f"ISO Ejected successfully for {node_ipmi}")
        else:
            print(f"Failed to eject ISO for {node_ipmi}")

        # Mount the specified ISO
        mount_status_code = mount_iso_dell(
            node_ipmi, os_version, username, password, access_tokens
        )
        if mount_status_code == 204:
            print(f"ISO mounted successfully for {node_ipmi}")
        else:
            print(f"Failed to mount ISO for {node_ipmi}")

        # Update boot source
        boot_source_status_code = update_boot_source_dell(
            node_ipmi, username, password, access_tokens
        )
        if boot_source_status_code == 200:
            print(f"Boot source updated successfully for {node_ipmi}")
        else:
            print(f"Failed to update boot source for {node_ipmi}")

        # Initiate graceful restart
        restart_status_code = initiate_graceful_restart_dell(
            node_ipmi, username, password, access_tokens
        )
        if restart_status_code == 204:
            print(f"Graceful restart initiated successfully for {node_ipmi}")
        else:
            print(f"Failed to initiate graceful restart for {node_ip}")
        rc = validate_installation(node_ip, os_version)
        return rc
    else:
        return "Unsupported hardware"


if __name__ == "__main__":
    arguments = docopt(__doc__)
    os_version = arguments["--os_version"]
    nodes = arguments["--nodes"]
    username = arguments["--username"]
    password = arguments["--password"]
    nodes_list = [item for item in nodes.split(",")] if "," in nodes else [nodes]
    run_id = generate_unique_id(length=6)
    config_dir = create_install_dir(run_id)
    base_cmd = download_requirements(config_dir, image_list.get(os_version))
    with multiprocessing.Pool() as pool:
        results = pool.starmap(
            reimage_node,
            [(node_ip, os_version, username, password) for node_ip in nodes_list],
        )
    for input_args, result in zip(nodes_list, results):
        print(f"Input: {input_args}, Result: {result}")

    clean_up(config_dir)
