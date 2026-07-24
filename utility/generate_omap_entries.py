"""
Module used to:
1. Create objects on pool,
2. generate specified amount of dummy key value pairs, add it as attributes to the object,
 thereby increasing the omap entries on the pool
"""

# !/usr/bin/env python
from __future__ import print_function

import os
import sys

from docopt import docopt
from rados import Rados, WriteOpCtx

doc = """
Usage:
  generate_omap_entries.py --pool <pool_name> --start <init_count> --end <end_count> --key-count <num_keys>

Options:
  --pool <name>                     Name of the pool where the omap entries need to be generated
  --start <num>                     Start point/count to create objects
  --end <num>                       end point/count to create objects
  --key-count <num>                 Number of kw pairs to be created for each object

"""


def run(args):
    pool = args["--pool"]
    start = int(args["--start"])
    end = int(args["--end"])
    keys_per_object = int(args["--key-count"])
    keys = tuple(["key_" + str(x) for x in range(keys_per_object)])
    values = tuple(["value_" + str(x) for x in range(keys_per_object)])

    with Rados(conffile="") as cluster:
        with cluster.open_ioctx(pool) as ioctx:
            prefix = "omap_obj_" + str(os.getpid()) + "_"
            for i in range(start, end):
                with WriteOpCtx(ioctx) as write_op:
                    ioctx.set_omap(write_op, keys, values)
                    ioctx.operate_write_op(write_op, prefix + str(i))
                    print(
                        "wrote",
                        (i + 1) * keys_per_object,
                        "of",
                        (end - start) * keys_per_object,
                        "omap entries",
                        end="\r",
                    )
                    sys.stdout.flush()
    print("\nDone!")


if __name__ == "__main__":
    args = docopt(doc)
    try:
        run(args)
    except Exception as err:
        print(
            f"Exception hit while generating OMAP entries on the given pool.\n error : {err} "
        )
        exit(1)
