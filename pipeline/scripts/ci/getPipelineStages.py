import json
import logging
import os
import random
import string
import sys

import yaml
from docopt import docopt

log = logging.getLogger(__name__)
doc = """
This script fetches all the tests to be run for a pipeline based on the RHCS version and overrides

    Usage:
        getPipelineStages.py --rhcephVersion <VER> --tags <tags>
                  [--overrides <str>]

        getPipelineStages.py (-h | --help)

    Options:
        -h --help          Shows the command usage
        -v --rhcephVersion VER     The rhcephVersion for which test stages need to be fetched
        -t --tags <str>    tags to be used for filtering test scripts
        -o --overrides <str>       Overrides to be considered for execution
"""


def generate_random_string(length):
    """Generate a random alphanumeric string of given length"""
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


def fetch_stages(args):
    """
    Fetch test stages to be executed based on the input provided
    Args:
        args:
            Dictionary containing the tier_level, cloud_type and all other necessary override parameters
            to be used to fetch test stages for execution

            arguments = {
                "rhcephVersion": <the rhbuild version for which the stages need to be fetched>,
                "tags": comma separated string containing the tags based on which the stages should be filtered,
                "overrides": <overrides in json format, all keys accepted by run.py are supported to be overridden
                            if any key similar to --store which does not have a value it can be passed as "store": ""
            }

            Example:
                args = {
                    "rhcephVersion": 5.1,
                    "tags": "tier-1,stage-1,ibmc",
                    "overrides": {"cloud_type": "openstack", "platform": "rhel-8"}

    Returns:
        Yaml string containing CLI to be executed for each test suite at every stage
        Examples:
            "test-cephfs-core-features":
                "execute_cli": ".venv/bin/python run.py ......",
                 "cleanup_cli": "...."
            .....
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    metadata_dir = os.path.abspath(f"{current_dir}/../../metadata")
    metadata_file = f"{metadata_dir}/{args['rhcephVersion']}.yaml"
    data = yaml.safe_load(open(metadata_file, "r"))
    tags = args["tags"].split(",")
    overrides = json.loads(args.get("overrides", {}))
    cloud_type = "openstack"

    filtered_data = filter(lambda d: all(tag in d["metadata"] for tag in tags), data)

    test_stages = dict()
    for script in filtered_data:
        script_name = script.pop("name")
        del script["metadata"]
        instances_name = f"ci-{generate_random_string(5)}"
        cleanup_cli = ".venv/bin/python run.py --osp-cred $HOME/osp-cred-ci-2.yaml"
        cleanup_cli += f" --cleanup {instances_name}"
        cleanup_cli += " --log-level DEBUG"

        execute_cli = ".venv/bin/python run.py --osp-cred $HOME/osp-cred-ci-2.yaml"
        execute_cli += f" --instances-name {instances_name}"
        execute_cli += " --log-level DEBUG"
        if "ibmc" in tags:
            cloud_type = "ibmc"
            logDir = f"logs/${generate_random_string(5)}"  # -${currentBuild.number}"
            os.makedirs(logDir)
            execute_cli += f" --log-dir ${logDir} --cloud {cloud_type}"
            cleanup_cli += f" --cloud {cloud_type}"
        else:
            execute_cli += " --post-results --report-portal"

        script.update({"inventory": script["inventory"][cloud_type]})
        script.update(overrides)

        for (k, v) in script.items():
            execute_cli += f" --{k} {v}"

        test_stages.update(
            {script_name: {"execute_cli": execute_cli, "cleanup_cli": cleanup_cli}}
        )
    return test_stages


if __name__ == "__main__":
    cli_args = docopt(doc)
    log.info(cli_args)
    arguments = {
        "rhcephVersion": cli_args.get("--rhcephVersion"),
        "tags": cli_args.get("--tags"),
        "overrides": cli_args.get("--overrides"),
        "metadata": cli_args.get("--metadata"),
    }
    try:
        testStages = fetch_stages(arguments)
    except Exception as e:
        raise e
    log.info(testStages)
    sys.stdout.write(yaml.dump(testStages))
