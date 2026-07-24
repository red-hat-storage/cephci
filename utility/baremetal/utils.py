import getpass
import logging
import os
import subprocess
import time

import paramiko
import requests

# Disable the InsecureRequestWarning
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from ceph.ceph import CommandFailed
from utility.retry import retry

LOG = logging.getLogger(__name__)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Define the base URL and credentials
base_url = "https://{}"
username = "xxxx"
password = "xxxx"
# base_cmd = "/root/supermicro/SMCIPMITool_2.26.0_build.220209_bundleJRE_Linux_x64/SMCIPMITool"
# Define a dictionary to store OS versions and their respective ISO URLs


def create_install_dir(run_id, config_dir=""):
    """
    Create the directory where configuration files will be placed.

    Args:
        run_id: id of the test run. used to name the directory
        config_dir: configuration directory name.
    Returns:
        Full path of the created directory
    """
    msg = """\nNote :
    1. Custom log directory will be disabled if '/ceph/cephci-jenkins' exists.
    2. If custom log directory not specified, then '/tmp' directory is considered .
    """
    LOG.info(msg)
    dir_name = "config-dir-{run_id}".format(run_id=run_id)
    base_dir = "/ceph/cephci-jenkins"

    if config_dir:
        base_dir = config_dir
        if not os.path.isabs(config_dir):
            base_dir = os.path.join(os.getcwd(), config_dir)
    elif not os.path.isdir(base_dir):
        base_dir = f"/tmp/{dir_name}"
    else:
        base_dir = os.path.join(base_dir, dir_name)

    LOG.info(f"config directory - {base_dir}")
    try:
        os.makedirs(base_dir)
    except OSError:
        if "jenkins" in getpass.getuser():
            raise

    return base_dir


def execute_command(chan, command):
    chan.send(command + "\n")
    time.sleep(2)  # Adjust sleep duration as needed
    output = chan.recv(4096).decode("utf-8")
    return output


def run_commands_in_shell(chan, commands):
    for command in commands:
        output = execute_command(chan, command)
        print(output)


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


def get_manufacturer(idrac_ip, username, password, access_tokens):
    # Authenticate to iDRAC
    access_token = authenticate(idrac_ip, username, password, access_tokens)

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


def authenticate(idrac_ip, username, password, access_tokens):
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


def download_requirements(config_dir, iso_url):
    """
    Downloads all required files for reimaging the Supermicro systems
    """
    LOG.info(
        "Downloading all required files \n============================================="
    )
    commands = f"""
    cd {config_dir}
    wget -nc https://www.supermicro.com/Bios/sw_download/404/SMCIPMITool_2.26.0_build.220209_bundleJRE_Linux_x64.tar.gz
    tar -xvf SMCIPMITool_2.26.0_build.220209_bundleJRE_Linux_x64.tar.gz -C {config_dir}
    dnf install ipmitool --nogpgcheck -y
    wget {iso_url} .
    chmod -R 777 ./
    """
    process = subprocess.Popen(
        "/bin/bash", stdin=subprocess.PIPE, stdout=subprocess.PIPE
    )
    out, err = process.communicate(commands.encode("utf-8"))
    LOG.info(out.decode("utf-8"))
    global base_cmd
    base_cmd = (
        f"{config_dir}/SMCIPMITool_2.26.0_build.220209_bundleJRE_Linux_x64/SMCIPMITool"
    )
    return base_cmd


def clean_up(config_dir):
    LOG.info(
        "Cleaning up all downloaded files\n============================================="
    )
    commands = f"""
        cd {config_dir}
        rm -rf SMCIP*
        rm -rf *.iso
        """
    process = subprocess.Popen(
        "/bin/bash", stdin=subprocess.PIPE, stdout=subprocess.PIPE
    )
    out, err = process.communicate(commands.encode("utf-8"))
    LOG.info(out.decode("utf-8"))


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
