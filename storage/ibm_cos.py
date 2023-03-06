"""
Provides the necessary interfaces to interact with IBM Cloud Object Storage.

The purpose of this module is to enable RH Ceph QE team access to various development
builds (mainly RPMs).

This module depends on the availability of `cos` section in CephCI's global config file.
The required keys in the `cos` section are
    - endpoint
    - api-key
    - resource-id
    - location-constraint

Currently, the below methods are available for use
    # upload
    # upload_directory
    # download
    # download_directory
    # create_bucket
    # bucket_exists
    # set_lifecycle_rules

This module requires the below configuration information in CephCI config file.
cos:
  api-key: <service-or-user-api-key>
  endpoint: <resource-public-endpoint>
  resource-id: <deployed-instance-resource-id>
  location-constraint: <storage-class>

Reference:
  https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-python
"""
import logging

import ibm_boto3
from ibm_botocore.client import ClientError, Config

from storage.ibm_cos_awscli import CloudObjectStorageAWS
from utility.utils import get_cephci_config

LOG = logging.getLogger(__name__)


# Custom Exceptions
class CreateBucketException(Exception):
    pass


class ApplyBucketLifeCycleException(Exception):
    pass


class UploadException(Exception):
    pass


class DownloadException(Exception):
    pass


class CloudObjectStorage:
    """Interface to IBM's Cloud Object Storage."""

    def __init__(self):
        """Initialize the instance using global configuration."""
        self._conf = get_cephci_config()["cos"]
        self._api_key = self._conf["api-key"]
        self._resource_id = self._conf["resource-id"]

        self.endpoint = self._conf["endpoint"]
        self.location_constraint = dict(
            {"LocationConstraint": self._conf["location-constraint"]}
        )

        self.client = ibm_boto3.client(
            "s3",
            ibm_api_key_id=self._api_key,
            ibm_service_instance_id=self._resource_id,
            config=Config(signature_version="oauth"),
            endpoint_url=self.endpoint,
        )

        self.resource = ibm_boto3.resource(
            "s3",
            ibm_api_key_id=self._api_key,
            ibm_service_instance_id=self._resource_id,
            config=Config(signature_version="oauth"),
            endpoint_url=self.endpoint,
        )

    def create_bucket(self, name: str) -> None:
        """
        Create a bucket with the provided name.

        Args:
            name (str): The name of the bucket to be created.
        Returns:
            (None):
        Raises:
            CreateBucketException on failure to create a bucket
        """
        try:
            self.resource.Bucket(name).create(
                CreateBucketConfiguration=self.location_constraint
            )
            LOG.debug(f"Successfully created bucket with {name}")
            return
        except ClientError as ce:
            LOG.debug(ce)
        except BaseException as be:  # no-qa
            LOG.debug(be)

        raise CreateBucketException(f"Failed to create bucket with name {name}")

    def set_lifecycle_rules(self, name: str) -> None:
        """
        Apply the lifecycle rule on the given bucket.

        Args:
            name (str): The name of the bucket
        Returns:
            (None):
        Raises:
            ApplyBucketLifecycleException
        """
        try:
            bucket_lc = self.resource.BucketLifecycleConfiguration(name)
            rules = [
                {
                    "Expiration": {
                        "Days": 3,
                    },
                    "ID": "compose-expiration-rule-01",
                    "Filter": {
                        "Prefix": "",
                    },
                    "Status": "Enabled",
                    "Transitions": [],
                },
            ]
            bucket_lc.put(LifecycleConfiguration={"Rules": rules})
            LOG.debug("Successfully applied the default lifecycle policy.")
            return
        except ClientError as ce:
            LOG.debug(ce)
        except BaseException as be:  # no-qa
            logging.debug(be)

        raise ApplyBucketLifeCycleException(
            f"Failed to apply default lifecycle policy on bucket - {name}."
        )

    def bucket_exists(self, name: str) -> bool:
        """
        Check if the given bucket exists or not.

        Args:
             name (str):    The name of the bucket to be checked.
        Returns:
            (bool):         True if the bucket exists else False
        """
        _date = self.resource.Bucket(name).creation_date
        return _date is not None

    def upload(self, bucket_name: str, object_name: str, abs_file_path: str) -> None:
        """
        Uploads the provided file with the name to the mentioned container.

        Args:
            bucket_name (str):  The name of the bucket to which the file is uploaded.
            object_name (str):  The name of the object to be given once uploaded.
            abs_file_path (str):  Absolute path to the file that needs to be uploaded.
        Returns:
            None
        Raises:
            UploadException
        """
        _size = 1024 * 1024 * 5
        _threshold = 1024 * 1024 * 15

        _config = ibm_boto3.s3.transfer.TransferConfig(
            multipart_threshold=_threshold, multipart_chunksize=_size
        )
        _mgr = ibm_boto3.s3.transfer.TransferManager(self.client, config=_config)
        try:
            LOG.debug(f"Uploading {abs_file_path} to bucket - {bucket_name}.")
            future = _mgr.upload(abs_file_path, bucket_name, object_name)
            future.result()
        except BaseException as be:  # no-qa
            LOG.debug(be)
            raise UploadException("Upload: Encountered and error during upload.")
        finally:
            _mgr.shutdown()

    def download(self, bucket_name: str, object_name: str, abs_file_path: str) -> None:
        """
        Downloads the provided object to the specified file path.

        Args:
            bucket_name (str):      The name of the bucket to download from
            object_name (str):      The name of the object to be downloaded.
            abs_file_path (str):    Absolute path to the file to be downloaded.
        Returns:
            None
        Raises:
            DownloadException
        """
        _mgr = ibm_boto3.s3.transfer.TransferManager(self.client)
        LOG.debug(f"Downloading {abs_file_path} to bucket - {bucket_name}.")
        future = _mgr.download(bucket_name, object_name, abs_file_path)
        try:
            future.result()
        except BaseException as be:  # no-qa
            LOG.debug(be)
            raise DownloadException("Download: Encountered and error during download.")
        finally:
            _mgr.shutdown()

    def upload_directory(self, local_dir: str, object_prefix: str, bucket_name) -> None:
        """
        Uploads the contents of local_dir folder with the prefix to the provided bucket.

        Args:
            local_dir (str):        Absolute path to the source folder.
            object_prefix (str):    Prefix to be added to the contents.
            bucket_name (str):      Bucket name to which the contents are uploaded.
        """
        try:
            from ibm_s3transfer.aspera.manager import AsperaTransferManager
        except BaseException as be:
            # COS-Aspera package is unavailable in Python 3.7
            LOG.debug(be)
            cos_v1 = CloudObjectStorageAWS()
            cos_v1.upload_directory(bucket_name, local_dir, object_prefix)
            return

        with AsperaTransferManager(client=self.client) as _mgr:
            future = _mgr.upload_directory(local_dir, bucket_name, object_prefix)
            future.result()

        LOG.info(f"Successfully uploaded contents of {local_dir}")

    def download_directory(
        self, bucket_name: str, object_prefix: str, local_dir: str
    ) -> None:
        """
        Downloads the objects having the given prefix from the provided bucket.

        Args:
            bucket_name (str):      Bucket name to which the contents are uploaded.
            object_prefix (str):    Prefix to be added to the contents.
            local_dir (str):        Absolute path to store the objects.
        """
        try:
            from ibm_s3transfer.aspera.manager import AsperaTransferManager
        except BaseException as be:
            # COS-Aspera package is unavailable in Python 3.7
            LOG.debug(be)
            raise NotImplementedError("Method is unsupported in this platform.")

        with AsperaTransferManager(client=self.client) as _mgr:
            future = _mgr.download(bucket_name, object_prefix, local_dir)
            future.result()

        LOG.info(f"Successfully downloaded the contents to {local_dir}.")
