import json
import os
import shlex
import smtplib
import subprocess
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict

from docopt import docopt
from jinja2 import Environment, FileSystemLoader, select_autoescape
from jinja_markdown import MarkdownExtension

from ceph.ceph import CommandFailed
from run import load_file

doc = """
Utility to notify resource usage in RHOS-D.

    Usage:
        rhos_quota.py --osp-cred <cred-file>
        rhos_quota.py (-h | --help)

    Options:
        -h --help           Shows the command usage
        --osp-cred <file>   API Credential file to access RHOS cloud.
"""


def execute(cmd, fail_ok=False, merge_stderr=False):
    """Executes specified command for the given action."""
    cmdlist = shlex.split(cmd)
    stdout = subprocess.PIPE
    stderr = subprocess.STDOUT if merge_stderr else subprocess.PIPE
    proc = subprocess.Popen(cmdlist, stdout=stdout, stderr=stderr)
    result, result_err = proc.communicate()
    result = result.decode("utf-8")

    if not fail_ok and proc.returncode != 0:
        raise CommandFailed(proc.returncode, cmd, result, result_err)

    return json.loads(result)


def openstack_basecmd(*args, **kwargs) -> str:
    """Generates the openstack base command"""
    cmd = ".venv/bin/openstack"
    cmd += f" --os-auth-url {kwargs['auth-url']}"
    cmd += f" --os-project-domain-name {kwargs['domain']}"
    cmd += f" --os-user-domain-name {kwargs['domain']}"
    cmd += f" --os-project-name {kwargs['project']}"
    cmd += f" --os-username {kwargs['username']}"
    cmd += f" --os-password {kwargs['password']}"
    return cmd


def map_userto_instances(os_nodes, os_cred):
    """
    Generate Dictionary with list of users and corresponding vm details and quota usage.
    Returns :
     instance_detail
    """
    user_detail = {}
    instance_detail = {}
    instance_count = 0
    for instance in os_nodes:
        instance_count += 1
        instance_id = instance["ID"]
        print(f"fetching the details of {instance_id}")
        print(
            f"Fetching {instance_count} instance out of {len(os_nodes)} in project : {os_cred['project']}"
        )
        try:
            os_node_detail_json = execute(
                cmd=openstack_basecmd(**os_cred) + f" server show {instance_id} -f json"
            )
            if not os_node_detail_json:
                continue
            state = os_node_detail_json["status"]
            flavor = os_node_detail_json["flavor"]
            if not user_detail.get(os_node_detail_json["user_id"]):
                user_json = execute(
                    cmd=openstack_basecmd(**os_cred)
                    + f" user show {os_node_detail_json['user_id']} -f json"
                )
                if not user_json:
                    continue
                user_detail[os_node_detail_json["user_id"]] = user_json["name"]
            if state == "ACTIVE":
                os_instance_usage_detail_json = execute(
                    cmd=openstack_basecmd(**os_cred)
                    + f" flavor show -c ram -c vcpus -c disk {flavor.split()[0]} -f json"
                )
                username = user_detail[os_node_detail_json["user_id"]]
                if instance_detail.get(username):
                    instance_detail[username]["Instances"].append(instance["Name"])
                    instance_detail[username]["Instance States"].append(state)
                    instance_detail[username]["RAM Used Per Instance in MB"].append(
                        os_instance_usage_detail_json["ram"]
                    )
                    instance_detail[username]["VCPUs Used Per Instance"].append(
                        os_instance_usage_detail_json["vcpus"]
                    )
                    instance_detail[username]["Volume Used Per Instance in GB"].append(
                        os_instance_usage_detail_json["disk"]
                    )

                else:
                    instance_dict = {
                        "Instances": [instance["Name"]],
                        "Instance States": [state],
                        "RAM Used Per Instance in MB": [
                            os_instance_usage_detail_json["ram"]
                        ],
                        "VCPUs Used Per Instance": [
                            os_instance_usage_detail_json["vcpus"]
                        ],
                        "Volume Used Per Instance in GB": [
                            os_instance_usage_detail_json["disk"]
                        ],
                    }
                    instance_detail[username] = instance_dict
        except CommandFailed as cf:
            print(f"Openstack command failed with {cf.args[-1]}")
    return instance_detail


def get_complete_quota(instance_detail, os_cred, project_name):
    """Generates Projects stats and user stats and collate them in to dictionary.

    Returns : quota_stats (dict)
        [{
            'project_stats': {
                'Project Name': 'ceph-jenkins',
                'RAM usage in %': 34.22,
                'VCPU usage in %': 29.2,
                'Storage usage in %': 29.73},
                'user_stats': [{
                    'User': 'psi-ceph-jenkins',
                    'Project': 'ceph-jenkins',
                    'Instance Count': 21,
                    'RAM Used in GB': 84.0,
                    'VCPU Used': 42,
                    'Volume Used in GB': 820
                }, {
                    'User': 'user1',
                    'Project': 'ceph-jenkins',
                    'Instance Count': 14,
                    'RAM Used in GB': 56.0,
                    'VCPU Used': 28,
                    'Volume Used in GB': 560
                }]
        }]
    """
    total_instances_used = 0
    total_vcpus_used = 0
    total_ram_used = 0
    total_volume_used = 0
    user_stats = []
    quota_stats = dict()

    for k in instance_detail.keys():
        user = k
        stat_map = {
            "User": user,
            "Project": project_name,
            "Instance Count": len(instance_detail[user]["Instances"]),
            "RAM Used in GB": sum(instance_detail[user]["RAM Used Per Instance in MB"])
            / 1024,
            "VCPU Used": sum(instance_detail[user]["VCPUs Used Per Instance"]),
            "Volume Used in GB": sum(
                instance_detail[user]["Volume Used Per Instance in GB"]
            ),
        }
        user_stats.append(stat_map)
        total_instances_used += stat_map["Instance Count"]
        total_ram_used += stat_map["RAM Used in GB"]
        total_vcpus_used += stat_map["VCPU Used"]
        total_volume_used += stat_map["Volume Used in GB"]
    try:
        os_quota_json = execute(
            cmd=openstack_basecmd(**os_cred) + " quota show -f json"
        )
        ram_percent = round((total_ram_used * 100) / (os_quota_json["ram"] / 1024), 2)
        vcpu_percent = round((total_vcpus_used * 100) / os_quota_json["cores"], 2)
        storage_percent = round(
            (total_volume_used * 100) / (os_quota_json["gigabytes"]), 2
        )
        quota_usage_dict = {
            "Project Name": project_name,
            "RAM usage in %": ram_percent,
            "VCPU usage in %": vcpu_percent,
            "Storage usage in %": storage_percent,
        }
        quota_stats = {"project_stats": quota_usage_dict, "user_stats": user_stats}
    except CommandFailed as cf:
        print(f"Openstack Command failed while fetching the quota: {cf.args[-1]}")

    return quota_stats


def send_email(html):
    """Sends Email with all the quota details."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = "Quota Usage Statistics for rhos-d projects."
    part1 = MIMEText(html, "html")
    msg.attach(part1)

    # result properties file and summary html log for injecting vars in jenkins jobs,
    # gitlab JJB to parse

    sender = "cephci@redhat.com"
    recipients = ["ceph-qe@redhat.com", "cephci@redhat.com"]

    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    try:
        s = smtplib.SMTP("localhost")
        s.sendmail(sender, recipients, msg.as_string())
        s.quit()
        print("Results have been emailed to {recipients}".format(recipients=recipients))

    except Exception as e:
        print("\n")
        print(e)


def run(args: Dict) -> None:
    """Generates Report with the usage details of RHOS-D environment."""
    osp_cred_file = args["--osp-cred"]
    osp_cred = load_file(osp_cred_file)
    glbs = osp_cred.get("globals")
    os_cred = glbs.get("openstack-credentials")
    projects = ["ceph-jenkins", "ceph-ci", "ceph-core"]
    quota_details = []
    for project in projects:
        os_cred["project"] = project
        try:
            os_nodes = execute(
                cmd=openstack_basecmd(**os_cred) + " server list -f json"
            )
            if not os_nodes:
                continue
            instance_detail = map_userto_instances(os_nodes, os_cred)
            quota_stats = get_complete_quota(instance_detail, os_cred, project)
            quota_details.append(quota_stats)
        except CommandFailed as cf:
            print(
                f"Openstack Command failed to retrieve the server details {cf.args[-1]}"
            )

    # This path we are looking for is <repo-dir>/templates
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    template_dir = os.path.join(project_dir, "templates")
    jinja_env = Environment(
        extensions=[MarkdownExtension],
        loader=FileSystemLoader(template_dir),
        autoescape=select_autoescape(["html", "xml"]),
    )
    user_stats = []
    for users in quota_details:
        user_stats.extend(users["user_stats"])

    final_details = {
        "project_stats": [i["project_stats"] for i in quota_details],
        "user_stats": user_stats,
    }

    final_details["project_stats"] = sorted(
        final_details["project_stats"], key=lambda d: d["RAM usage in %"], reverse=True
    )
    final_details["user_stats"] = sorted(
        final_details["user_stats"], key=lambda d: d["RAM Used in GB"], reverse=True
    )

    template = jinja_env.get_template("quota-template-stats.html")
    html = template.render(quota=final_details, pro_range=[70, 50], user_range=[20, 10])
    send_email(html)


if __name__ == "__main__":
    arguments = docopt(doc)
    run(arguments)
