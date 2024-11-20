#! /usr/bin/env python3

# -*- code: utf-8 -*-

from datetime import datetime, timezone
from logging import getLogger

from docopt import docopt
from utils.configs import get_cloud_credentials, get_configs, get_database_credentials
from utils.utility import set_logging_env

from cli.cloudproviders import CloudProvider
from cli.exceptions import ConfigError
from cli.reports.database import Database

# Set logging env
LOG = getLogger(__name__)

# Table names corresponding to different usecases
HARDWARE_MONITORING_TABLE = "hardware_monitoring"

doc = """
Utility to get Cluster Stats

    Usage:
        cephci/stats.py --report <STR>
            [--owner <STR>]
            [--log-level <LOG>]
            [--log-dir <PATH>]

        cephci/provision.py --help

    Options:
        -h --help               Help
        --report <STR>          Report type [bm-hardware-status|cpu-memory-usage|nfs-scale-report]
        --owner <STR>           Baremetal node owner
        --config <YAML>         Config file with cloud credentials
        --log-level <LOG>       Log level for log utility Default: DEBUG
        --log-dir <PATH>        Log directory for logs
"""


def get_baremetal_hardware_status(node_stats):
    # Get the creds specific to the database
    credentials = get_database_credentials(config, "database")
    credentials["table"] = HARDWARE_MONITORING_TABLE
    reporting = Database(**credentials)

    # Indentify the cols
    cols = []
    for key, _ in node_stats[0]:
        cols.append(key)
    cols.append("locked_days")
    cols = " ,".join(cols)

    # For each node identify the required data and push to db
    for node in node_stats:
        data = []
        opendays = "NA"

        # Find open since days
        dt = node.get("locked_since")
        if dt:
            dt = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S.%f").replace(
                tzinfo=timezone.utc
            )
            opendays = (datetime.now(timezone.utc) - dt).days

        # For some fields, teuthology returns Attribute Error with long error message or empty value
        # In such cases, add custom value (Info Not Available or AttributeError) to the db
        for _, val in node.items():
            val = str(val) if val else "Info Not Available"
            if (
                "AttributeError" in val
            ):  # Teuthology returns Attribute error for some cols
                val = "AttributeError"
            data.append(f"'{val}'")

        data.append(f"'{str(opendays)}'")
        data = " ,".join(data)

        # Append the data to the database
        reporting.insert(table=HARDWARE_MONITORING_TABLE, cols=cols, data=data)

    # Close the db connection
    reporting.db_conn.close()


if __name__ == "__main__":
    # Set configs
    cloud_configs, global_configs = {}, []

    # Set user parameters
    args = docopt(doc)

    # Get user parameters
    report = args.get("--report")
    owner = args.get("--owner")
    config = args.get("--config")
    log_level = args.get("--log-level")
    log_dir = args.get("--log-dir")

    # Set log level
    LOG = set_logging_env(level=log_level, path=log_dir)

    # Read configuration for cloud
    get_configs(config)

    # Check for mandatory baremetal paramters
    if report == "bm-hardware-status":
        if not owner:
            raise ConfigError("Mandatory owner are not provided")
        # Set cloud owner
        cloud_configs["owner"] = owner

        # Read cloud configurations
        cloud_configs.update(get_cloud_credentials(report))

        # Get timeout and interval
        timeout, retry = cloud_configs.pop("timeout"), cloud_configs.pop("retry")

        # Connect to cloud
        cloud = CloudProvider(report, **cloud_configs)

        # Get the Hardware Stats
        node_stats = cloud.get_node_details()

        # Get the baremetal hardware status
        get_baremetal_hardware_status(node_stats)
