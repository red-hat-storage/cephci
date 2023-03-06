"""
This module provides the upload of the directories and files to IBM COS(Cloud Object Storage)
It awscli for uploading the local dir to COS
Functionality:
    1. It uploads the directory to IBM COS
Pre-requisites:
    create ~/.aws/credentials
References : https://github.com/aws/aws-cli
"""
import logging

import awscli.clidriver

from utility.utils import get_cephci_config

LOG = logging.getLogger(__name__)


class CloudObjectStorageAWS:
    """
    Interface for uploading and downloading the files from IBM COS
    """

    def __init__(self):
        self._conf = get_cephci_config()["cos"]
        self.endpoint = self._conf["endpoint"]
        self.driver = awscli.clidriver.create_clidriver()

    def upload_directory(self, bucket_name, local_dir, object_prefix):
        """
        upload the directory to IBM COS to given bucket name
        Args:
            bucket_name: Name of the bucket where we need to push
            local_dir: local directory to were we need to push
            object_prefix: Destination folder in IBM COS to Push

        Returns:

        """
        try:
            s3_path = f"s3://{bucket_name}/{object_prefix}"
            cmd = [
                "s3",
                "cp",
                local_dir,
                s3_path,
                "--recursive",
                "--endpoint-url",
                self.endpoint,
            ]
            rc = self.driver.main(cmd)
            LOG.info(rc)
            if rc == 1:
                raise Exception("Upload to COS has been failed")
        except Exception as e:
            LOG.error(e)
            raise e

    def download_directory(self):
        pass
