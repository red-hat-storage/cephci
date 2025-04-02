# module can be used to generate fragmented objects on replicated pool only
# support for EC pool may be added later
# !/usr/bin/env python
from __future__ import print_function

import sys

from docopt import docopt
from rados import Rados

doc = """
Usage:
  generate_frag_objs.py --pool <pool_name> --create <object_count> --remove <object_count> --size <each_object_size>

Options:
  --pool <name>      Name of the pool where the omap entries need to be generated
  --create <num>       Start point/count to create objects
  --remove <num>       Start point/count to create objects
  --size <num>       Number of kw pairs to be created for each object

"""


def run(args):
    pool = args["--pool"]
    create = int(args["--create"])
    remove = int(args["--remove"])
    size = int(args["--size"])
    data = bytes(size)
    with Rados(conffile="") as cluster:
        with cluster.open_ioctx(pool) as ioctx:
            prefix = "filler_" + str(size) + "_"
            for i in range(0, create):
                ioctx.write(prefix + str(i), data, 0)
                print(
                    "create",
                    i,
                    "of",
                    create,
                    "objects",
                    end="\r",
                )
                sys.stdout.flush()
            for i in range(0, create, 2):
                ioctx.remove_object(prefix + str(i))
                print(
                    "removed - fragment",
                    int(i / 2),
                    "of",
                    int(create / 2),
                    "objects",
                    end="\r",
                )
                sys.stdout.flush()
            for i in range(0, remove, 2):
                ioctx.remove_object(prefix + str(i + 1))
                print(
                    "removed - defragment",
                    int(i / 2),
                    "of",
                    int(remove / 2),
                    "objects",
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
            f"Exception hit while creating/removing objects on the given pool.\n error : {err} "
        )
        exit(1)
