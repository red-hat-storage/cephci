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
        getTestsForPipeline.py --rhcephVersion <VER> --tags <tags>
                  [--overrides <str>]

        getTestsForPipeline.py (-h | --help)

    Options:
        -h --help          Shows the command usage
        -v --rhcephVersion VER     The rhcephVersion for which test stages need to be fetched
        -t --tags <str>    tags to be used for filtering test scripts
        -o --overrides <str>       Overrides to be considered for execution
"""


def generate_random_string(length):
    """Generate a random alphanumeric string of given length"""
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def update_overrides(metadata_overrides, cloud_type, overrides, data):
    """
    Updates the given dictionary "data" with the overrides provided
    Args:
        metadata_overrides: RHCeph version level overrides to be updated,
        cloud_type: ibmc|openstack
        overrides: User specified overrides to be updated
        data: dictionary to which overrides need to be updated
    """
    cloud_overrides = {
        "ibmc": metadata_overrides.pop("ibmc", {}),
        "openstack": metadata_overrides.pop("openstack", {}),
    }
    data.update(cloud_overrides[cloud_type])
    data.update(metadata_overrides)
    data.update(overrides)


def fetch_from_metadata(rhcephVersion):
    """
    Fetches the test data from metadata file for the specified RHCS version
    Args:
        rhcephVersion: RHCS version for which metadata needs to be fetched
    Returns:
        The fetched test data
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    metadata_dir = os.path.abspath(f"{current_dir}/../../metadata")
    metadata_file = f"{metadata_dir}/{rhcephVersion}.yaml"
    metadata_content = yaml.safe_load(open(metadata_file, "r"))
    return metadata_content


def is_final_stage_or_tier(stage_or_tier, data):
    """
    Returns whether the current stage specified in tags is the final stage
    for the given pipeline
    Args:
        stage_or_tier: current test stage
        data: data containing all the stages/tiers in the required pipeline
    Returns:
        Whether the stage mentioned in the tags is the final stage
        for the given pipeline
    """
    curr_t_or_s_num = int(stage_or_tier.split("-")[1])
    next_t_or_s_in_tier = [
        s for s in data.keys() if int(s.split("-")[1]) > curr_t_or_s_num
    ]
    return not bool(next_t_or_s_in_tier)


def construct_cli(
    data, cloud_type, filters, metadata_overrides, user_overrides, test_data
):
    """

    Args:
        data:
        cloud_type:
        filters:
        metadata_overrides:
        user_overrides:
        test_data:
    Returns:

    """
    workspace = user_overrides.get("workspace", "")
    build_number = user_overrides.get("build_number", "")
    overrides = {
        k: v
        for (k, v) in user_overrides.items()
        if k not in ["workspace", "build_number"]
    }
    meta_overrides = {
        k: v for (k, v) in metadata_overrides.items() if k not in ["ibmc", "openstack"]
    }
    cloud_overrides = metadata_overrides.get(cloud_type, {})
    filtered_data = (
        filter(lambda d: all(tag in d["metadata"] for tag in filters), data)
        if filters
        else data
    )

    for test_suite in filtered_data:
        script_name = test_suite.pop("name")
        test_suite.pop("execution_time")
        del test_suite["metadata"]
        instances_name = f"ci-{generate_random_string(5)}"
        cleanup_cli = ".venv/bin/python run.py --osp-cred $HOME/osp-cred-ci-2.yaml"
        cleanup_cli += f" --cleanup {instances_name}"
        cleanup_cli += " --log-level DEBUG"

        execute_cli = ".venv/bin/python run.py --osp-cred $HOME/osp-cred-ci-2.yaml"
        execute_cli += f" --instances-name {instances_name}"
        execute_cli += " --log-level DEBUG"
        logdir = f"{workspace}/logs/{generate_random_string(5)}-{build_number}"

        execute_cli += " --xunit-results"
        # os.makedirs(logdir)
        execute_cli += f" --log-dir {logdir}"
        if cloud_type == "ibmc":
            execute_cli += f" --cloud {cloud_type}"
            cleanup_cli += f" --cloud {cloud_type}"

        test_suite.update({"inventory": test_suite["inventory"][cloud_type]})
        test_suite.update(cloud_overrides)

        test_suite.update(meta_overrides)
        test_suite.update(overrides)

        for k, v in test_suite.items():
            if isinstance(v, list):
                for item in v:
                    execute_cli += f" --{k} {item}"
            else:
                execute_cli += f" --{k} {v}"

        test_data.update(
            {
                script_name: {
                    "execute_cli": execute_cli,
                    "cleanup_cli": cleanup_cli,
                    "log_dir": logdir,
                }
            }
        )


def fetch_test_cli(
    data, cloud_type, filters, metadata_overrides, user_overrides, test_data
):
    """

    Args:
        data:
        cloud_type:
        filters:
        metadata_overrides:
        user_overrides:
        test_data
    Returns:

    """
    if isinstance(data, list):
        construct_cli(
            data, cloud_type, filters, metadata_overrides, user_overrides, test_data
        )
        return
    for data_key, data_val in data.items():
        if isinstance(data_val, list):
            construct_cli(
                data_val,
                cloud_type,
                filters,
                metadata_overrides,
                user_overrides,
                test_data,
            )
        else:
            fetch_test_cli(
                data_val,
                cloud_type,
                filters,
                metadata_overrides,
                user_overrides,
                test_data,
            )


def fetch_tests(args):
    """
    Fetch tests to be executed based on the input provided
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
                    "tags": "sanity,tier-1,stage-1,ibmc",
                    "overrides": {"cloud_type": "openstack", "platform": "rhel-8"}

    Returns:
        Yaml string containing CLI to be executed for each test suite at every stage
        Examples:
            "test-cephfs-core-features":
                "execute_cli": ".venv/bin/python run.py ......",
                 "cleanup_cli": "...."
            .....
    """
    is_final_tier = False
    is_final_stage = False
    test_data = dict()

    # Fetch the necessary test data
    metadata_content = fetch_from_metadata(args.get("rhcephVersion"))
    # RHCS version level overrides
    metadata_overrides = metadata_content.get("overrides", {})
    # User specified overrides
    user_overrides = json.loads(args.get("overrides", {}))
    # Test suites for a specific RHCS version
    test_pipelines = metadata_content.get("pipelines")

    # Fetch the pipeline, tier and stage values from tags
    tags = args["tags"].split(",")
    cloud_type = "ibmc" if "ibmc" in tags else "openstack"
    (pipeline, pipeline_data) = next(
        ((i, test_pipelines[i]) for i in tags if i in test_pipelines.keys()), None
    )
    tier = next((i for i in tags if i.startswith("tier-")), None)
    stage = next((i for i in tags if i.startswith("stage-")), None)
    filters = [i for i in tags if i not in [pipeline, tier, stage, cloud_type]]
    if not pipeline_data:
        log.error(
            f"The pipeline requested in {tags} is not available for version {args.get('rhcephVersion')}"
        )
        is_final_stage = True
        is_final_tier = True
        return {
            "scripts": test_data,
            "final_tier": is_final_tier,
            "final_stage": is_final_stage,
        }

    # If tier is not specified in tags, then fetch suites for all the tiers in the given pipeline
    if not tier:
        fetch_test_cli(
            pipeline_data,
            cloud_type,
            filters,
            metadata_overrides,
            user_overrides,
            test_data,
        )
        return {
            "scripts": test_data,
            "final_tier": is_final_tier,
            "final_stage": is_final_stage,
        }
    elif not pipeline_data.get(tier):
        log.error(
            f"The tier for the pipeline requested in {tags} is not available for version {args.get('rhcephVersion')}"
        )
        is_final_stage = True
        return {
            "scripts": test_data,
            "final_tier": is_final_tier,
            "final_stage": is_final_stage,
        }
    is_final_tier = is_final_stage_or_tier(tier, pipeline_data)

    # If stage is not specified in tags, then fetch suites for all stages for given tier in given pipeline
    if not stage:
        fetch_test_cli(
            pipeline_data.get(tier),
            cloud_type,
            filters,
            metadata_overrides,
            user_overrides,
            test_data,
        )
        return {
            "scripts": test_data,
            "final_tier": is_final_tier,
            "final_stage": is_final_stage,
        }
    elif not pipeline_data.get(tier).get(stage):
        log.error(
            f"The stage for the pipeline requested in {tags} is not available for version {args.get('rhcephVersion')}"
        )
        is_final_stage = True
        return {
            "scripts": test_data,
            "final_tier": is_final_tier,
            "final_stage": is_final_stage,
        }

    # If tier and stage are specified, then fetch suites for the given tier and stage in given pipeline
    is_final_stage = is_final_stage_or_tier(stage, pipeline_data.get(tier))
    fetch_test_cli(
        pipeline_data.get(tier).get(stage),
        cloud_type,
        filters,
        metadata_overrides,
        user_overrides,
        test_data,
    )
    return {
        "scripts": test_data,
        "final_tier": is_final_tier,
        "final_stage": is_final_stage,
    }


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
        testStages = fetch_tests(arguments)
    except Exception as e:
        raise e
    log.info(testStages)
    sys.stdout.write(yaml.dump(testStages))
