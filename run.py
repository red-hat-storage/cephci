"""Entry point for Red Hat Ceph QE CI."""
from gevent import monkey

monkey.patch_all()
from docopt import docopt

usage = """
An orchestrator that calls the relevant methods based on the provided workflow.

Usage:
  run.py -h | --help
  run.py (--conf <conf-file>)...
    [--rhcs <rhcs-version>]
    [--build <build-type>]
    [--clusters-spec <cluster-spec>]
    [--test-suite <test-suite>]...
    [--verbose]

Options:
  -h --help         Displays the usage and supported options.
  --conf            Configuration files in YAML format containing details for execution.
  --rhcs            Red Hat Ceph Storage version
  --build           Type of build to be used for deployment. Default the latest CDN bits
  --clusters-spec   System Under Test layout file
  --test-suite      Absolute test suite file. Supports multiple values
  --verbose         Increases the log level. Default, log level is info
"""

if __name__ == "__main__":
    args = docopt(usage)
