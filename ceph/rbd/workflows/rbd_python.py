import sys

import rados
import rbd
from docopt import docopt

doc = """
A simple test suite wrapper that executes tests based on yaml test configuration

 Usage:
  rbd_python.py --pool <name>
        (--image <name>)
        (--image-size <size>)
        [--conf-file FILE]
        [--data-pool <name>]

Options:
  -h --help                         show this screen
  --pool <pool>                     pool name
  --image <image>                   image name
  --image-size <size>               image size
  --conf-file <file>                conf file path
  --data-pool <name>                data pool for ec pool
"""


def rbd_python(args):
    """
    Create image and write random data to it, read data written and verify
    """
    conf_file = args.get("--conf-file", "/etc/ceph/ceph.conf")
    pool = args.get("--pool")
    image_name = args.get("--image")
    image_size = int(args.get("--image-size"))
    data_pool = args.get("--data-pool")

    # Create a rados cluster instance
    with rados.Rados(conffile=conf_file) as cluster:
        # Open the pool object
        with cluster.open_ioctx(pool) as ioctx:
            # Create rbd instance
            rbd_inst = rbd.RBD()
            # Create rbd image
            if data_pool:
                rbd_inst.create(ioctx, image_name, image_size, data_pool=data_pool)
            else:
                rbd_inst.create(ioctx, image_name, image_size)
            # Open rbd image for write and read
            with rbd.Image(ioctx, image_name) as image:
                data = str.encode("bar" * 20000)
                image.write(data, 0)
                mydata = image.read(0, 60000)
                if data != mydata:
                    print(
                        f"Data written and read back don't match for image {image_name}"
                    )
                    return 1
    print(f"Data written matches with the data read back for image {image_name}")
    return 0


if __name__ == "__main__":
    args = docopt(doc, help=True)
    rc = rbd_python(args)
    sys.exit(rc)
