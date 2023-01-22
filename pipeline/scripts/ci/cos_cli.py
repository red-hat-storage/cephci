"""This script is to update report artifacts on IBM

- Upload test result files to "qe-ci-reports" bucket
- Executes bucket file options like upload, download, delete and list
"""

import logging
import sys

from docopt import docopt

from storage.ibm_cos import CloudObjectStorage
from utility.retry import retry

LOG = logging.getLogger(__name__)

usage = """
This script helps to update reports in IBM.

Usage:
  cos_cli.py upload <SOURCE_FILE> <OBJ_KEYNAME> <bucket_name>
  cos_cli.py upload_directory <SOURCE_FILE> <OBJ_KEYNAME> <bucket_name>
  cos_cli.py download <OBJ_KEYNAME> <bucket_name> <DESTINATION_FILE>
  cos_cli.py delete <OBJ_KEYNAME> <bucket_name>
  cos_cli.py list <bucket_name>
  cos_cli.py -h | --help

    action              upload | download | delete | list
    bucket_name         bucket name

Example:
    python cos_cli.py upload test.xml test-run-1 qe-ci-reports-bucket
    python cos_cli.py upload_directory results test-run-1 qe-ci-reports
    python cos_cli.py download test-run-1 qe-ci-reports test.xml
    python cos_cli.py delete test-run-1 qe-ci-reports
    python cos_cli.py list qe-ci-reports

Options:
    -h --help                       Show this screen
"""
cos = CloudObjectStorage()


def upload_objfile(bucket: str, local_file: str, obj_key: str):
    """Upload file as file object.

    Args:
        bucket: bucket name
        local_file: local file path
        obj_key: object name to store file

    """
    with open(local_file, "rb") as in_file:
        cos.resource.Bucket(bucket).upload_fileobj(in_file, obj_key)
    LOG.info(f"Uploaded successfully object({obj_key}) using file({local_file})")


def download_objfile(bucket: str, local_file: str, obj_key: str):
    """Download file object as file.

    Args:
        bucket: bucket name
        local_file: local file path
        obj_key: object name to store file

    """
    with open(local_file, "wb") as out_file:
        cos.resource.Bucket(bucket).download_fileobj(obj_key, out_file)
    LOG.info(
        f"Downloaded successfully using KeyName({obj_key}) into file({local_file})"
    )


def delete_objfile(bucket: str, obj_key: str):
    """Delete file object.

    Args:
        bucket: bucket name
        obj_key: object name to store file
    """
    obj = cos.resource.Bucket(bucket).Object(obj_key)
    obj.delete()
    obj.wait_until_not_exists()
    LOG.info(f"Deleted successfully the file object({obj_key})")


@retry(BaseException, tries=5, delay=30)
def upload_directory(bucket: str, local_dir: str, prefix: str) -> None:
    """
    Uploads the given file to the provided bucket with the mentioned name.

    Args:
        bucket (str):       The name of container to which file has to be uploaded
        local_dir (str):    Complete path to the file that needs to be uploaded
        prefix (str):       The name to be used for the uploaded object
    """
    cos.upload_directory(local_dir, prefix, bucket)
    LOG.info("Successfully upload the object to IBM COS !!!")


def list_objects(bucket: str):
    """List file objects from bucket.

    Args:
        bucket: bucket name
    Returns:
        list
    """
    _objs = cos.client.list_objects(Bucket=bucket)
    print([i["Key"] for i in _objs.get("Contents")])


OPS = {
    "upload": upload_objfile,
    "download": download_objfile,
    "delete": delete_objfile,
    "list": list_objects,
    "upload_directory": upload_directory,
}

if __name__ == "__main__":

    _args = docopt(usage, help=True, version=None, options_first=False)
    logging.basicConfig(
        handlers=[logging.StreamHandler(sys.stdout)],
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    )

    try:
        _def = None
        for key, value in OPS.items():
            if _args[key]:
                _def = value
                break
        else:
            raise Exception("please provide right action")

        bucket_name = _args["<bucket_name>"]
        args = [bucket_name]

        file_path = _args.get("<SOURCE_FILE>", None) or _args.get(
            "<DESTINATION_FILE>", None
        )

        if file_path:
            args.append(file_path)

        obj_keyname = _args.get("<OBJ_KEYNAME>", None)
        if obj_keyname:
            args.append(obj_keyname)

        _def(*args)
    except BaseException as be:
        LOG.error(be)
        print(usage)
