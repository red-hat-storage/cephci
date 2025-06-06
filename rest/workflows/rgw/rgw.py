from rest.endpoints.rgw.rgw import RGW
from utility.log import Log

log = Log(__name__)


def create_bucket_verify(bucket, uid, rest, rest_v1):
    """
    Create a bucket for a specified user and verify its creation.

    Args:
        bucket (str): The name of the bucket to be created.
        uid (str): The user ID for which the bucket should be created.
        rest (object): REST client instance for RGW operations (typically v2 API).
        rest_v1 (object): REST client instance for RGW v1 API.

    Returns:
        int:
            0 if the bucket is successfully created and verified,
            1 if any step (user creation, bucket creation, verification) fails.
    """
    if not bucket or not uid:
        log.error("Both 'bucket' and 'uid' are required to create a bucket.")
        return 1

    rgw_rest = RGW(rest=rest)
    rgw_rest_v1 = RGW(rest=rest_v1)

    # Check if user exists
    user_list = rgw_rest.list_user()
    if uid not in user_list:
        log.info(f"User '{uid}' not found. Creating user...")
        try:
            resp = rgw_rest.create_user(uid=uid, display_name=uid)
        except Exception as e:
            log.error(f"Failed to create user '{uid}': {resp} with exception {str(e)}")
            return 1
        log.info(f"User '{uid}' created successfully.")

    # Create bucket
    log.info(f"Creating bucket '{bucket}' for user '{uid}'...")
    try:
        bucket_resp = rgw_rest.create_bucket(bucket=bucket, uid=uid)
        log.info(f"the bucket_repsonse {bucket_resp}")
    except Exception as e:
        log.error(f"Bucket creation failed for '{bucket}': with exception {str(e)}")
        return 1
    log.info(f"PASS: Bucket {bucket} creation succesfull")

    # Verify bucket exists in list
    bucket_list = rgw_rest_v1.list_bucket()
    if bucket not in bucket_list:
        log.error(f"FAILED :Bucket '{bucket}' not found in bucket list after creation.")
        return 1

    log.info(f"PASSED : Bucket '{bucket}' successfully created and verified.")
    return 0
