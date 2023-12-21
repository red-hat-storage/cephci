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


def copy_file(file_name, src, dest):
    """Copies the given file from src node to dest node

    Args:
        file_name: Full path of the file to be copied
        src: Source CephNode object
        dest: Destination CephNode object
    """
    contents, err = src.exec_command(sudo=True, cmd="cat {}".format(file_name))
    key_file = dest.remote_file(sudo=True, file_name=file_name, file_mode="w")
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
        output=True, cmd="md5sum {}".format(kw.get("file_path")), node=kw.get("client")
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


# def check_data_integrity_for_multiple_images(**kw):
#     """
#     Check data integrity for multiple images in multiple pools
#     kw: {
#         "pool_type":<>,
#         "rbd":<>,
#         "sec_obj":<>,
#         "client":<>,
#         "sec_client":<>,
#         <multipool and multiimage config>
#     }
#     """
#     pdb.set_trace()
#     pool_type = kw.get("pool_type")
#     rbd = kw.get("rbd")
#     sec_rbd = kw.get("sec_obj")
#     client = kw.get("client")
#     sec_client = kw.get("sec_client")
#     config = deepcopy(kw.get("config").get(pool_type))
#     for pool, pool_config in getdict(config).items():
#         multi_image_config = getdict(pool_config)
#         multi_image_config.pop("test_config", {})
#         for image in multi_image_config.keys():
#             image_spec = f"{pool}/{image}"
#             data_integrity_spec = {
#                 "first": {"image_spec": image_spec, "rbd": rbd, "client": client},
#                 "second": {
#                     "image_spec": image_spec,
#                     "rbd": sec_rbd,
#                     "client": sec_client,
#                 },
#             }
#             rc = check_data_integrity(**data_integrity_spec)
#             if rc:
#                 log.error(f"Data integrity check failed for {image_spec}")
#                 return 1
#     return 0


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
