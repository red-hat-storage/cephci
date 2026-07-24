"""
Utility to cleanup orphan volumes from IBM environment
"""

import sys
from time import sleep
from typing import Dict

import yaml
from docopt import docopt

from compute.ibm_vpc import get_ibm_service

doc = """
Utility to cleanup orphan volumes from IBM cloud.
    Usage:
        ibm_volume_cleanup.py --creds <cred-file>
        ibm_volume_cleanup.py (-h | --help)
    Options:
        -h --help          Shows the command usage
        --creds <file>     API Credential file to access ibm cloud.
                           sample/example <file>:
                           globals:
                                ibm-credentials:
                                    access-key: '<user_access_key>'
                                    service-url: 'https://xxxx.iaas.cloud.ibm.com/v1'
                                    zone_name: '<dns_zone>'
                                    vpc_name: '<vpc_name>'
"""


def run(args: Dict):
    """
    Using the provided credential file, this method removes the orphan volume entries from ibm cloud.
    Arguments:
        args: Dict - containing the key/value pairs passed by the user

    Returns:
        0 on success or 1 for failures
    """
    cred_file = args["--creds"]

    with open(cred_file, "r") as cred_stream:
        yh = yaml.safe_load(cred_stream)
        ibm_cred = yh["globals"]["ibm-credentials"]
        ibmc_client = get_ibm_service(
            access_key=ibm_cred["access-key"], service_url=ibm_cred["service-url"]
        )
        resp = ibmc_client.list_volumes()
        if resp.get_status_code() != 200:
            print("\nFailed to retrieve volumes\n")
            return 1
        response = resp.get_result()
        orphan_volumes = [
            volume
            for volume in response["volumes"]
            if volume["status"] == "available" and not volume["volume_attachments"]
        ]
        if not orphan_volumes:
            print("\nThere are no orphan volumes in the IBM environment\n")
            return 0
        sleep_time = 1
        for volume in orphan_volumes:
            print(f"\nDeleting orphan volume {volume['name']}\n")
            resp = ibmc_client.delete_volume(id=volume["id"])
            if resp.get_status_code() != 200:
                print("\nFailed to delete volume\n")
                return 1
            sleep(sleep_time)
            sleep_time += 1
        print("\nSuccessfully removed the orphan volumes from IBM environment\n")
    return 0


if __name__ == "__main__":
    try:
        arguments = docopt(doc)
        rc = run(arguments)
    except Exception:
        print("\nFailed to remove the orphan DNS record from IBM\n")
        rc = 1
    sys.exit(rc)
