#!/usr/bin/env python3
"""Entry point of CephCI.

The implementation lives in :mod:`cephci.orchestration`; this module stays at the root
of the repository because the pipelines invoke it by path.
"""

import sys

from docopt import docopt

from cephci.orchestration.doc import doc
from cephci.orchestration.runner import run
from utility.log import Log

log = Log()


if __name__ == "__main__":
    args = docopt(doc)
    rc = run(args)
    log.info("final rc of test run %d" % rc)
    sys.exit(rc)
