"""
    Utility to cleanup instances in RHOS-D environment that have crossed the maximum
    allowed duration and instances that are in error status. There are multiple
    projects/tenants under CephQE purview and each of them have a different configuration.

    For example, ceph-jenkins has the least allowable time as the intent is to enable
    the pipeline is executed under a stable environment. The durations for each project
    are
        ceph-jenkins        3 days
        ceph-ci             2 weeks
        ceph-core           2 weeks
"""
import smtplib
import sys
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict

import yaml
from docopt import docopt
from libcloud.common.types import LibcloudError
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider

from ceph.parallel import parallel
from mita.v2 import CephVMNodeV2, Node
from utility.retry import retry

doc = """
Utility to cleanup instances in a RHOS cloud.

    Usage:
        cleanup_env.py --osp-cred <cred-file>
        cleanup_env.py (-h | --help)

    Options:
        -h --help           Shows the command usage
        --osp-cred <file>   API Credential file to access RHOS cloud.
"""


def add_key_to_ref(
    ref: Dict, category: str, email: str, vm_name: str, tenant: str
) -> None:
    """
    Adds the provide information to ref

    Arguments:
        ref         Data structure to which the values need to be added
        email       Primary identifier of the data structure
        category    Whether the VM has been marked or deleted or has an error
        vm_name     The name of instance
        tenant      The project to which the instance belongs

    Returns:
        None but the ref is update with the provided values
    """
    if ref.get(email, {}):
        if ref[email].get(category, {}):
            if ref[email][category].get(tenant, []):
                ref[email][category][tenant].append(vm_name)
            else:
                ref[email][category].update({tenant: [vm_name]})
        else:
            ref[email].update({category: {tenant: [vm_name]}})
    else:
        ref.update({email: {category: {tenant: [vm_name]}}})


def cleanup(
    osp_identity, osp_cred: Dict, node: Node, results: Dict, tenant: str
) -> None:
    """
    Removes VMs whose subscription has expired.

    The instances are not removed if they have do-not-delete or is locked. They are
    deleted if they have crossed the specified duration.

    Arguments:
         osp_identity   Class Object of libcloud
         osp_cred       Credential information for accessing the cloud
         node           The NodeDriver instance of the VM
         results        Captures the results of the operation
         tenant         The project to which the node belongs

    Returns:
        None
    """
    if "do-not-delete" in node.name.lower():
        return

    user_ = osp_identity.get_user(node.extra["userId"])
    if node.state.lower() != "error":
        node_age = datetime.now(timezone.utc) - node.created_at
        max_age = 3 if tenant == "ceph-jenkins" else 14

        if (max_age - 1) > node_age.days > (max_age // 2):
            if user_.name == "psi-ceph-jenkins":
                return

            add_key_to_ref(results, "warn", user_.email, node.name, tenant)
            return

        if node_age.days < max_age:
            return
    else:
        add_key_to_ref(results, "error", user_.email, node.name, tenant)

    try:
        ceph_node = CephVMNodeV2(
            username=osp_cred["username"],
            password=osp_cred["password"],
            auth_url=osp_cred["auth-url"],
            auth_version=osp_cred["auth-version"],
            tenant_name=tenant,
            tenant_domain_id=osp_cred["tenant-domain-id"],
            service_region=osp_cred["service-region"],
            domain_name=osp_cred["domain"],
            node_name=node.name,
        )

        ceph_node.delete()
        if user_.name == "psi-ceph-jenkins":
            # PSI Ceph Jenkins is a service account. Hence, ignoring email
            return

        add_key_to_ref(results, "deleted", user_.email, node.name, tenant)
    except BaseException:  # noqa
        add_key_to_ref(results, "marked", user_.email, node.name, tenant)


def send_email(payload: Dict) -> list:
    """
    Sends an email to all recipients given in the payload.

    Arguments:
        payload     A dictionary having the primary key as email id

    Returns:
        list

    payload example
        {
            "abc@xyz.com": {
                                "deleted": { "ceph-jenkins": ["vm3"] },
                                "marked": { "ceph-core": ["vm10"] }
                            },
            "abd@xyz.com": {
                                "warn": { "ceph-jenkins": [...]},
                            }}
    """
    failed_vm = []
    sender = "cephci@redhat.com"
    for email, ct in payload.items():
        if email is not None:
            recipient = email

            msg = MIMEMultipart("alternative")
            msg[
                "Subject"
            ] = "Notification: Your subscriptions are marked/removed in RHOS-D"
            msg["From"] = sender
            msg["To"] = recipient
            html = """\
                    <html><head><style>table, th, td {border: 1px solid black; }</style>
                        </head><body>
                            <p>Hi,<br><br>
                            Below cloud subscriptions are expired/expiring.<br>
                            The durations for subscriptions expire in project are for ceph-jenkins its 3 days,
                            for ceph-ci and ceph-core its 2 weeks i.e, 14days.</p>"""

            for category, pro in ct.items():
                statement = "Summary: "
                if category == "warn":
                    statement += "Subscription expiring Nodes from RHOS-D"
                elif category == "deleted":
                    statement += (
                        "Subscription expired Nodes which are Removed from RHOS-D"
                    )
                elif category == "marked":
                    statement += "Nodes which are failed to Remove from RHOS-D"
                elif category == "error":
                    continue

                html += f"""\
                    </table><h4 style='color:navy;'>{statement}</h4>
                    <table><tr><th>Project</th><th>Instance name</th></tr>"""
                for project, instance in pro.items():
                    if category == "marked":
                        failed_vm.extend(instance)
                    html += f"<tr><td rowspan = '{len(instance)}'>{project}</td>"
                    for i in instance:
                        if i != instance[0]:
                            html += "<tr>"
                        html += f"<td>{i}</td></tr>"
                html += "</table><br>"

            html += """\
                </table><p style="color:red;">NOTE: Please add 'do-not-delete' along with the node name
                in case it should not be removed</p></body><br><br></html>"""
            part1 = MIMEText(html, "html")
            msg.attach(part1)
            s = smtplib.SMTP("localhost")
            s.sendmail(sender, recipient, msg.as_string())
            s.quit()
    return failed_vm


@retry(LibcloudError, tries=5, delay=15)
def run(args: Dict) -> int:
    """
    Using the provided credential file, this method removes the instances that are
    running passed the allowable duration.

    Arguments:
        args: Dict - containing the key/value pairs passed by the user

    Returns:
        0 on success or 1 for failures
    """
    osp_cred_file = args["--osp-cred"]

    with open(osp_cred_file, "r") as osp_cred_stream:
        yh = yaml.safe_load(osp_cred_stream)
        osp_cred = yh["globals"]["openstack-credentials"]

        results = dict()

        tenants = ["ceph-ci", "ceph-core", "ceph-jenkins"]
        for tenant in tenants:
            driver_ = get_driver(Provider.OPENSTACK)
            osp_driver = driver_(
                osp_cred["username"],
                osp_cred["password"],
                api_version="2.2",
                ex_force_auth_url=osp_cred["auth-url"],
                ex_force_auth_version=osp_cred["auth-version"],
                ex_tenant_name=tenant,
                ex_force_service_region=osp_cred["service-region"],
                ex_domain_name=osp_cred["domain"],
                ex_tenant_domain_id=osp_cred["tenant-domain-id"],
            )

            osp_identity = osp_driver.connection.get_auth_class()
            osp_identity.connect()

            with parallel() as p:
                for node in osp_driver.list_nodes():
                    p.spawn(cleanup, osp_identity, osp_cred, node, results, tenant)

    response = send_email(results)

    if response:
        print(f"Failed to delete Instances/nodes: {response}")
        return 1
    else:
        return 0


if __name__ == "__main__":
    arguments = docopt(doc)
    rc = run(arguments)
    sys.exit(rc)
