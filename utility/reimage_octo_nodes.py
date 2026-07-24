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

import multiprocessing
import subprocess
import time

import paramiko
import requests
from docopt import docopt

# Disable the InsecureRequestWarning
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from ceph.ceph import CommandFailed
from utility.retry import retry

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Define the base URL and credentials
base_url = "https://{}"
username = "xxxx"
password = "xxxx"

# Define a dictionary to store OS versions and their respective ISO URLs
image_list = {
    "9.2": "http://10.8.128.6/rhel92ks_updated.iso",
    "8.6": "http://10.8.128.6/rhel86ks_updated.iso",
}


# Ensure all node IPs have 'ipmi.ceph.redhat.com' appended
def append_domain(ip, domain=".ipmi.ceph.redhat.com"):
    if not ip.endswith(".ipmi.ceph.redhat.com"):
        return ip + domain
    return ip


def ping_node(ip):
    # Check if the node is pingable for 3 times with a 5-second interval
    for _ in range(3):
        result = subprocess.run(["ping", "-c", "1", ip])
        if result.returncode == 0:
            return True
        time.sleep(5)
    return False


def get_manufacturer(idrac_ip, username, password):
    # Authenticate to iDRAC
    access_token = authenticate(idrac_ip, username, password)

    # Retrieve system information from the redfish/v1/Systems/ endpoint
    systems_url = base_url.format(idrac_ip) + "/redfish/v1/Systems/"
    response = requests.get(
        systems_url, headers={"X-Auth-Token": access_token}, verify=False
    )

    if response.status_code == 200:
        systems_info = response.json()

        # Check if there are members in the list
        members = systems_info.get("Members", [])
        if members:
            # Get the first system's URL
            system_url = members[0].get("@odata.id")

            # Retrieve the system information
            system_response = requests.get(
                base_url.format(idrac_ip) + system_url,
                headers={"X-Auth-Token": access_token},
                verify=False,
            )

            if system_response.status_code == 200:
                system_info = system_response.json()
                manufacturer = system_info.get("Manufacturer", "")
                return manufacturer

    return None


access_tokens = {}


def authenticate(idrac_ip, username, password):
    # Check if an access token is already available for this server
    if idrac_ip in access_tokens:
        return access_tokens[idrac_ip]

    # If not, authenticate and store the token
    auth_url = base_url.format(idrac_ip) + "/redfish/v1/SessionService/Sessions"
    auth_data = {"UserName": username, "Password": password}
    response = requests.post(auth_url, json=auth_data, verify=False)

    if response.status_code == 201:
        access_token = response.headers.get("X-Auth-Token")
        access_tokens[idrac_ip] = access_token  # Store the token in the dictionary
        return access_token
    else:
        raise Exception(
            f"Failed to authenticate with {idrac_ip}: {response.status_code} {response.text}"
        )


def eject_iso(idrac_ip, username, password):
    eject_media_url = (
        base_url.format(idrac_ip)
        + "/redfish/v1/Managers/iDRAC.Embedded.1/VirtualMedia/CD/Actions/VirtualMedia.EjectMedia"
    )
    access_token = authenticate(idrac_ip, username, password)
    response = requests.post(
        eject_media_url,
        json={},
        headers={"X-Auth-Token": access_token, "Content-Type": "application/json"},
        verify=False,
    )
    return response.status_code


def mount_iso(idrac_ip, os_version, username, password):
    virtual_media_url = (
        base_url.format(idrac_ip)
        + "/redfish/v1/Managers/iDRAC.Embedded.1/VirtualMedia/CD/Actions/VirtualMedia.InsertMedia"
    )
    access_token = authenticate(idrac_ip, username, password)
    iso_payload = {"Image": image_list.get(os_version), "Inserted": True}
    response = requests.post(
        virtual_media_url,
        json=iso_payload,
        headers={"X-Auth-Token": access_token, "Content-Type": "application/json"},
        verify=False,
    )
    return response.status_code


def update_boot_source(idrac_ip, username, password):
    patch_url = base_url.format(idrac_ip) + "/redfish/v1/Systems/System.Embedded.1"
    access_token = authenticate(idrac_ip, username, password)
    patch_payload = {
        "Boot": {"BootSourceOverrideTarget": "Cd", "BootSourceOverrideEnabled": "Once"}
    }
    headers = {"X-Auth-Token": access_token, "Content-Type": "application/json"}
    response = requests.patch(
        patch_url, json=patch_payload, headers=headers, verify=False
    )
    return response.status_code


def initiate_graceful_restart(idrac_ip, username, password):
    reset_url = (
        base_url.format(idrac_ip)
        + "/redfish/v1/Systems/System.Embedded.1/Actions/ComputerSystem.Reset"
    )
    access_token = authenticate(idrac_ip, username, password)
    reset_payload = {"ResetType": "GracefulRestart"}
    headers = {"X-Auth-Token": access_token, "Content-Type": "application/json"}
    response = requests.post(
        reset_url, json=reset_payload, headers=headers, verify=False
    )
    return response.status_code


def login_to_node(node_ip, username="root", password="passwd"):
    try:
        # Create an SSH client instance
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect to the node
        ssh_client.connect(node_ip, username=username, password=password)

        # Run a command to retrieve the OS version
        stdin, stdout, stderr = ssh_client.exec_command("cat /etc/os-release")
        os_release_info = stdout.read().decode()
        print(os_release_info)

        # Parse the OS version from the output
        os_version = None
        for line in os_release_info.splitlines():
            if line.startswith("VERSION_ID="):
                os_version = line.split("=")[1].strip().strip('"')
                break

        # Close the SSH connection
        ssh_client.close()

        if os_version:
            # Return True and the OS version to indicate a successful login
            return True, os_version
        else:
            # Return False if OS version could not be determined
            return False, None

    except Exception as e:
        # Handle any exceptions (e.g., authentication failure)
        print(f"Failed to login to {node_ip}: {str(e)}")
        return False, None

    except Exception as e:
        # Handle any exceptions (e.g., authentication failure)
        print(f"Failed to login to {node_ip}: {str(e)}")
        return False


@retry(CommandFailed, tries=15, delay=60)
def reimage_node(node_ip, os_version, username, password):
    node_ipmi = append_domain(node_ip)

    # Ping test
    if not ping_node(node_ipmi):
        print(f"Node {node_ipmi} is not pingable. Skipping...")
    manufacturer = get_manufacturer(node_ipmi, username, password)

    if "Dell Inc." not in manufacturer:
        return "Unsupported hardware"

    # Eject the current ISO
    eject_status_code = eject_iso(node_ipmi, username, password)
    if eject_status_code == 204:
        print(f"ISO Ejected successfully for {node_ipmi}")
    else:
        print(f"Failed to eject ISO for {node_ipmi}")

    # Mount the specified ISO
    mount_status_code = mount_iso(node_ipmi, os_version, username, password)
    if mount_status_code == 204:
        print(f"ISO mounted successfully for {node_ipmi}")
    else:
        print(f"Failed to mount ISO for {node_ipmi}")

    # Update boot source
    boot_source_status_code = update_boot_source(node_ipmi, username, password)
    if boot_source_status_code == 200:
        print(f"Boot source updated successfully for {node_ipmi}")
    else:
        print(f"Failed to update boot source for {node_ipmi}")

    # Initiate graceful restart
    restart_status_code = initiate_graceful_restart(node_ipmi, username, password)
    if restart_status_code == 204:
        print(f"Graceful restart initiated successfully for {node_ipmi}")
    else:
        print(f"Failed to initiate graceful restart for {node_ip}")
    rc = validate_installation(node_ip, os_version)
    return rc


@retry(CommandFailed, tries=15, delay=60)
def validate_installation(node_ip, os_version):
    time.sleep(60)
    if ping_node(f"{node_ip}.ceph.redhat.com"):
        print(f"Node {node_ip} is pingable after restart.")
        login_successful, actual_os_version = login_to_node(
            f"{node_ip}.ceph.redhat.com", "bootuser", "passwd"
        )

        if login_successful:
            print(f"Login successful to {node_ip}")
            if actual_os_version == os_version:
                print(f"OS version matches the expected version ({os_version}).")
                return 0
            else:
                print(
                    f"OS version ({actual_os_version}) does not match the expected version ({os_version})."
                )
        else:
            raise CommandFailed(f"Login failed to {node_ip}")

    else:
        raise CommandFailed(
            f"Node {node_ip} is not pingable after restart. Reimage failed."
        )


if __name__ == "__main__":
    arguments = docopt(__doc__)
    os_version = arguments["--os_version"]
    nodes = arguments["--nodes"]
    username = arguments["--username"]
    password = arguments["--password"]
    nodes_list = [item for item in nodes.split(",")] if "," in nodes else [nodes]

    with multiprocessing.Pool() as pool:
        results = pool.starmap(
            reimage_node,
            [(node_ip, os_version, username, password) for node_ip in nodes_list],
        )
    for input_args, result in zip(nodes_list, results):
        print(f"Input: {input_args}, Result: {result}")
