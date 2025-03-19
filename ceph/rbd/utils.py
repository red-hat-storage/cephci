import math
import random
import string

from ceph.ceph import CommandFailed
from utility.log import Log

log = Log(__name__)


getdict = lambda x: {k: v for (k, v) in x.items() if isinstance(v, dict)}

isdict = lambda x: [isinstance(i, dict) for i in x]


def find(key, dictionary):
    """Find list of values for a particular key from a nested dictionary."""
    for k, v in dictionary.items():
        if k == key:
            yield v
        elif isinstance(v, dict):
            for result in find(key, v):
                yield result
        elif isinstance(v, list):
            for d in v:
                if isinstance(d, dict):
                    for result in find(key, d):
                        yield result
                else:
                    yield d


def value(key, dictionary):
    """Retrieve first value of a particular key from a nested dictionary."""
    return str(list(find(key, dictionary))[0])


def copy_file(file_name, src, dest, dest_file_name=None):
    """Copies the given file from src node to dest node

    Args:
        file_name: Full path of the file to be copied
        src: Source CephNode object
        dest: Destination CephNode object
        dest_file_name: Destination file name
    """
    contents, err = src.exec_command(sudo=True, cmd="cat {}".format(file_name))
    key_file = dest.remote_file(
        sudo=True, file_name=dest_file_name or file_name, file_mode="w"
    )
    key_file.write(contents)
    key_file.flush()


def get_md5sum_rbd_image(**kw):
    """
    Get md5sum of an RBD image.
    kw: {
        "image_spec": <pool/image> for which md5sum needs to be fetched,
        "file_path": <path/filename> to which image is exported/mounted to fetch md5sum,
        "rbd": <rbd_object>,
        "client": <client_node>
    }
    """
    if kw.get("image_spec"):
        export_spec = {
            "source-image-or-snap-spec": kw.get("image_spec"),
            "path-name": kw.get("file_path"),
        }
        out, err = kw.get("rbd").export(**export_spec)
        if "100% complete...done" not in out + err:
            log.error(f"Export failed for image {kw.get('image_spec')}")
            return None
    return exec_cmd(
        output=True,
        cmd=f"md5sum {kw['file_path']} && rm -f {kw['file_path']}",
        node=kw.get("client"),
    ).split()[0]


def check_data_integrity(**kw):
    """
    kw: {
        "first":{
            "image_spec": <>,
            "file_path": <>,
            "rbd":<rbd_object>,
            "client":<client_node>
        },
        "second":{
            "image_spec": <>,
            "file_path":<>,
            "rbd":<rbd_object>,
            "client":<client_node>
        }
    }
    """
    md5_sum_first = get_md5sum_rbd_image(**kw.get("first"))
    if not md5_sum_first:
        log.error("Error while fetching md5sum")
        return 1
    md5_sum_second = get_md5sum_rbd_image(**kw.get("second"))
    if not md5_sum_second:
        log.error("Error while fetching md5sum")
        return 1

    log.info(
        f"md5sum values of given images are {md5_sum_first} and {md5_sum_second} respectively"
    )
    if md5_sum_first != md5_sum_second:
        log.error("md5sum values don't match")
        return 1
    return 0


def exec_cmd(node, cmd, **kw):
    """
    exec_command wrapper with additional functionality

    Args:
        node: Ceph Node in which command needs to be executed
        cmd: the command to be executed
        kw: {
            output: True if command output needs to be returned
            all: Both out and err outputs returned if this is set to True
                    In case command execution gives an exception, then
                    out,err will be 1,<exception message>
            long_running: True if you want long_running functionality of exec_command
            check_ec: False will run the command and not wait for exit code
        }

    Returns:  0 -> pass, 1 -> fail, by default and output if output arg is set to true
                out,err if all arg is set to true
    """
    try:
        if kw.get("long_running"):
            out = node.exec_command(
                sudo=True,
                cmd=cmd,
                long_running=True,
                check_ec=kw.get("check_ec", True),
            )
            return out

        out, err = node.exec_command(
            sudo=True,
            cmd=cmd,
            long_running=False,
            check_ec=kw.get("check_ec", True),
        )

        if kw.get("output", False):
            return out

        if kw.get("all", False):
            return out, err

        log.info("Command execution complete")
        return 0

    except CommandFailed as e:
        log.error(f"Command {cmd} execution failed")
        if kw.get("all", False):
            return 1, e
        return 1


def random_string(**kw):
    """
    Generates a random string and returns it

    Args:
        kw: {
            "len": 20
        }

    Returns:
        The generated string
    """
    length = kw.get("len", 10)
    temp_str = "".join([random.choice(string.ascii_letters) for _ in range(length)])
    return temp_str


def create_map_options(encryption_config):
    """ """
    options = ""
    for encryption in encryption_config:
        for k, v in encryption.items():
            options += f"{k}={v},"
    options = options[:-1]  # remove trailing comma

    return options


def convert_size(size_bytes):
    """
    Convert size in bytes to other pretty formats
    """
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "K", "M", "G", "T", "P", "E", "Z", "Y")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p)
    return "%s%s" % (s, size_name[i])


def configure_common_client_node(client1, client2):
    """
    Configure the common client node as client1 to access both clusters.
    Args:
        client1: Cluster1 client node object
        client2: Cluster2 client node object
    """

    # Ensure /etc/ceph directory exists and is writable on client1
    client1.exec_command(cmd="sudo mkdir -p /etc/ceph && sudo chmod 777 /etc/ceph")

    # Copy cluster2 configuration and keyring files to client1
    cluster2_files = [
        ("/etc/ceph/ceph.conf", "/etc/ceph/cluster2.conf"),
        (
            "/etc/ceph/ceph.client.admin.keyring",
            "/etc/ceph/cluster2.client.admin.keyring",
        ),
    ]
    for file, dest_path in cluster2_files:
        copy_file(file_name=file, src=client2, dest=client1, dest_file_name=dest_path)

    client1.exec_command(sudo=True, cmd="chmod 644 /etc/ceph/*")

    # verify cluster accessibility for both clusters
    for cluster_name in ["ceph", "cluster2"]:
        out, err = client1.exec_command(
            cmd=f"ceph -s --cluster {cluster_name}", output=True
        )
        log.info(f"Cluster {cluster_name} status: {out}")
        if err:
            raise Exception(
                f"Unable to access cluster {cluster_name} from common client node"
            )
            return 1
    log.info("Common client node configured successfully.")
    return 0
