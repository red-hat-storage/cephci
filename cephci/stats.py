from datetime import datetime

from docopt import docopt
from utils.configs import get_cloud_credentials, get_configs, get_reporting_credentials
from utils.utility import set_logging_env

from cli.cloudproviders import CloudProvider
from cli.exceptions import ConfigError
from cli.reports.grafana import Grafana
from utility.log import Log

# Set logging env
LOG = Log(__name__)

doc = """
Utility to get Cluster Stats

    Usage:
        cephci/stats.py --cloud-type <CLOUD>
            [--owner <STR>]
            [--config <YAML>]
            [--log-level <LOG>]
            [--log-dir <PATH>]

        cephci/provision.py --help

    Options:
        -h --help               Help
        --cloud-type <CLOUD>    Cloud type [openstack|ibmc|baremetal]
        --owner <STR>           Baremetal node owner
        --config <YAML>         Config file with cloud credentials
        --log-level <LOG>       Log level for log utility Default: DEBUG
        --log-dir <PATH>        Log directory for logs
"""


def get_baremetal_hardware_status():
    for node in node_stats[:2]:
        cols = []
        data = []
        opendays = "NA"

        # Find open since days
        dt = node["locked_since"]
        if dt:
            dt = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S.%f")
            opendays = (datetime.now() - dt).days
        for key, val in node.items():
            cols.append(key)
            val = str(val) if val else "Info Not Available"
            if (
                "AttributeError" in val
            ):  # Teuthology returns Attribute error for some cols
                val = "AttributeError"
            data.append(f"'{val}'")

        data.append(f"'{str(opendays)}'")
        cols.append("locked_days")
        cols = " ,".join(cols)
        data = " ,".join(data)
        reporting.insert(table=db_table, cols=cols, data=data)
    reporting.db_conn.close()


if __name__ == "__main__":
    # Set configs
    cloud_configs, global_configs = {}, []

    # Set user parameters
    args = docopt(doc)

    # Get user parameters
    cloud = args.get("--cloud-type")
    owner = args.get("--owner")
    config = args.get("--config")
    log_level = args.get("--log-level")
    log_dir = args.get("--log-dir")

    # Set log level
    LOG = set_logging_env(level=log_level, path=log_dir)

    # Read configuration for cloud
    get_configs(config)

    # Check for mandatory OSP paramters
    if cloud == "openstack":
        pass

    # Check for mandatory baremetal paramters
    elif cloud == "baremetal":
        if not owner:
            raise ConfigError("Mandatory owner are not provided")

        # Set cloud owner
        cloud_configs["owner"] = owner

    # Read cloud configurations
    cloud_configs.update(get_cloud_credentials(cloud))

    # Get timeout and interval
    timeout, retry = cloud_configs.pop("timeout"), cloud_configs.pop("retry")
    interval = int(timeout / retry) if timeout and retry else None

    # Connect to cloud
    cloud = CloudProvider(cloud, **cloud_configs)

    node_stats = cloud.get_node_details()

    reporting_creds = get_reporting_credentials(config)["grafana"]
    db_name = reporting_creds.get("db_name")
    db_user = reporting_creds.get("db_user")
    db_pwd = reporting_creds.get("db_pwd")
    db_host = reporting_creds.get("db_host")
    db_port = reporting_creds.get("db_port")
    db_table = reporting_creds.get("db_table")

    reporting = Grafana(
        db_name=db_name,
        db_user=db_user,
        db_pwd=db_pwd,
        db_host=db_host,
        db_port=db_port,
    )

    get_baremetal_hardware_status()
