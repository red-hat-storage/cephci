""" Utility to cleanup instance/node along with its volumes"""

import smtplib
import sys
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import yaml
from docopt import docopt
from libcloud.common.types import LibcloudError
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider

from ceph.parallel import parallel
from utility.retry import retry

doc = """
Utility to cleanup instance/node.

 Usage:
  cleanup_env.py --osp-cred FILE

Options:
  --osp-cred <file>                 openstack credentials as separate file [default: osp/osp-cred-ci-2.yaml]
"""


def get_instances_from_openstack_driver(yaml, tenant):
    """
    Create an openstack handle from config to manage RHOS-D resources
    :param:
       - yaml: openstack details as yaml file
       - tenant: project name
    :return: driver, openstack handle to retrieve driver info
    """
    openstack = get_driver(Provider.OPENSTACK)
    glbs = yaml.get("globals")
    os_cred = glbs.get("openstack-credentials")
    username = os_cred["username"]
    password = os_cred["password"]
    auth_url = os_cred["auth-url"]
    auth_version = os_cred["auth-version"]
    service_region = os_cred["service-region"]
    domain_name = os_cred["domain"]
    tenant_domain_id = os_cred["tenant-domain-id"]
    driver = openstack(
        username,
        password,
        ex_force_auth_url=auth_url,
        ex_force_auth_version=auth_version,
        ex_tenant_name=tenant,
        ex_force_service_region=service_region,
        ex_domain_name=domain_name,
        ex_tenant_domain_id=tenant_domain_id,
    )
    return driver


def instance_cleanup(instance, results, volumes, driver, instances_dict):
    """
    Remove specified instance and its volumes if any from RHOS-D
    :param:
       - instance: instance object need to be removed
       - results: list: for storing instance which are failed to remove/delete
       - volumes: volume information
       - driver: openstack handler
       - instances_dict: dict: for storing instance name as key and
            date of its creation as value in case of success in its removal
    """
    try:
        for v in volumes:
            if instance.name in v.name:
                print("Removing volume:", v.name)
                volobj = driver.ex_get_volume(v.id)
                driver.detach_volume(volobj)
                driver.destroy_volume(volobj)
        print("Removing instance:", instance.name)
        instance.destroy()
        instances_dict[instance.name] = instance.extra["created"].split("T")[0]
    except Exception:
        results.append(instance.name)


def cleanup_instances_with_error(osp_cred):
    """
    Removes instance that are in error state and its volumes if any
    and Send email about the removed instances
    :param:
       - osp_cred: openstack details
    :return: error_results:contains list of instance name in case of failure else empty list
    """
    error_results = []
    dict_instance = dict()
    projects = ["ceph-ci", "ceph-core", "ceph-jenkins"]
    with parallel() as p:
        for tenant in projects:
            print("Removing instance that are in Error state from Project:", tenant)
            driver = get_instances_from_openstack_driver(osp_cred, tenant)
            instances = driver.list_nodes()
            volumes = driver.list_volumes()
            instances_dict = dict()
            for instance in instances:
                if instance.state == "error":
                    p.spawn(
                        instance_cleanup,
                        instance,
                        error_results,
                        volumes,
                        driver,
                        instances_dict,
                    )
            dict_instance[tenant] = instances_dict
    send_email_of_remove_instance(dict_instance, error=True)
    return error_results


def cleanup_instances_older_than_ttl(osp_cred):
    """
    Removes instance that are older than its time spam and its
    volumes if any and Send email about the removed instances
    :param:
       - osp_cred: openstack details
    :return: ttl_results:contains list of instance name in case of failure else empty list
    """
    ttl_results = []
    dict_instance = dict()
    projects = ["ceph-ci", "ceph-core", "ceph-jenkins"]
    with parallel() as p:
        for tenant in projects:
            ttl = 3 if tenant == "ceph-jenkins" else 14
            print(
                f"Removing instance that are older than {ttl} days from Project:{tenant}"
            )
            driver = get_instances_from_openstack_driver(osp_cred, tenant)
            instances = driver.list_nodes()
            volumes = driver.list_volumes()
            instances_dict = dict()
            for instance in instances:
                if "Do-Not-Delete".lower() not in instance.name.lower():
                    age = str(datetime.now(timezone.utc) - instance.created_at)
                    if "days" in age and int(age.split("days")[0]) > ttl:
                        p.spawn(
                            instance_cleanup,
                            instance,
                            ttl_results,
                            volumes,
                            driver,
                            instances_dict,
                        )
            dict_instance[tenant] = instances_dict
    send_email_of_remove_instance(dict_instance, error=False)
    return ttl_results


def send_email_of_remove_instance(instance_dict, error):
    """
    This Method is for sending email that contains table of removed instances,
    its project and instance creation date
    :param:
       - instance_dict: openstack details
       - error: bool- true for instance with error state, False for instance with expired subscription
    """
    sender = "cephci@redhat.com"
    recipient = "cephci@redhat.com"
    msg = MIMEMultipart("alternative")
    reason = "it's Error state" if error else "it's expired VM subscription"

    msg["Subject"] = f"Removed instances due to {reason}"
    msg["From"] = sender
    msg["To"] = recipient
    html = """\
        <html><head>
          <style>table, th, td {border: 1px solid black; }</style></head>
          <body><p>Hi Team,</p>
            <p>Please find the bellow table which contains the summary of removal of instance</p>
            </table><h4>Summary for removal of instance From Project</h4><table>
              <tr><th>Project</th><th>Instance name</th><th>Created On</th></tr>"""
    for pro, instance in instance_dict.items():
        no_of_instance = len(instance)
        if no_of_instance == 0:
            html += f"<tr><td>{pro}</td><td></td><td></td></tr>"
        else:
            html += f"<tr><td rowspan = '{no_of_instance}'>{pro}</td>"
        for inst, con in instance.items():
            if inst != list(instance.keys())[0]:
                html += "<tr>"
            html += f"<td>{inst}</td>"
            html += f"<td>{con}</td></tr>"
    html += "</table></body></html>"
    part1 = MIMEText(html, "html")
    msg.attach(part1)
    s = smtplib.SMTP("localhost")
    s.sendmail(sender, recipient, msg.as_string())
    s.quit()


@retry(LibcloudError, tries=5, delay=15)
def run(args):
    """
    Method to remove instances that are in error state and instance that
    are expired its subscription along with volumes if there is any
    :param:
       - args: required arguments for removal of instance
    :return: 0 on success, 1 for failures
    """
    osp_cred_file = args["--osp-cred"]
    if osp_cred_file:
        with open(osp_cred_file, "r") as osp_cred_stream:
            osp_cred = yaml.safe_load(osp_cred_stream)
    error_result = cleanup_instances_with_error(osp_cred)
    if len(error_result) > 0:
        print(f"\nFailed to delete instances with error state: {error_result}")
    ttl_result = cleanup_instances_older_than_ttl(osp_cred)
    if len(ttl_result) > 0:
        print(
            f"\nFailed to delete instances with time to live greater the specified ttl: {ttl_result}"
        )
    error_result.extend(ttl_result)
    if error_result:
        return 1
    return 0


if __name__ == "__main__":
    args = docopt(doc)
    rc = run(args)
    sys.exit(rc)
