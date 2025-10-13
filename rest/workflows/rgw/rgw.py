from rest.endpoints.rgw.rgw import RGW
from utility.log import Log

log = Log(__name__)


def create_bucket_verify(bucket, uid, rest, rest_v1, lifecycle, ratelimit):
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

    # List Available RGW daemons
    daemons = rgw_rest_v1.list_daemons()

    # Get the first listed RGW daemon details
    if not daemons:
        log.error("Daemon details not listed")
    service_id = daemons[0]["id"]
    rgw_rest_v1.get_daemon(svc_id=service_id)

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

    # List bucket lifecycle policies on bucket
    rgw_rest_v1.list_bucket_lifecycle(bucket=bucket)

    # List Global Ratelimits on bucket
    rgw_rest_v1.list_bucket_ratelimit_global()

    # Put bucket ratelimit
    log.info(f"Put bucket ratelimit for {bucket}")
    try:
        bucket_resp = rgw_rest.set_bucket_ratelimit(bucket=bucket, ratelimit=ratelimit)
        log.info(f"the bucket_repsonse {bucket_resp}")
    except Exception as e:
        log.error(
            f"Put bucket ratelimit failed for '{bucket}': with exception {str(e)}"
        )
        return 1
    log.info(f"PASS: Put bucket ratelimit successful for {bucket}")

    # List bucket level ratelimit
    rgw_rest_v1.list_bucket_ratelimit(bucket=bucket)

    # List bucket encryption for the bucket
    rgw_rest_v1.list_bucket_encryptionConfig()

    # List bucket encryption for the bucket
    # rgw_rest_v1.list_bucket_encryption(bucket=bucket)

    # Put bucket lifecycle
    # log.info(f"Put licycle configuration for {bucket}")
    # try:
    #     bucket_resp = rgw_rest.update_bucket_lifecycle(bucket=bucket, lifecycle=lifecycle)
    #     log.info(f"the bucket_repsonse {bucket_resp}")
    # except Exception as e:
    #     log.error(f"Put bucket lifecycle failed for '{bucket}': with exception {str(e)}")
    #     return 1
    # log.info(f"PASS: Put LC config successful for {bucket}")

    # Get details of the bucket
    bucket_resp = rgw_rest.get_bucket(bucket=bucket)
    log.info(f"the bucket_repsonse {bucket_resp}")

    # Delete bucket
    log.info(f"Deleting bucket '{bucket}'")
    try:
        bucket_resp = rgw_rest.delete_bucket(bucket=bucket)
        log.info(f"the bucket_repsonse {bucket_resp}")
    except Exception as e:
        log.error(f"Bucket deletion failed for '{bucket}': with exception {str(e)}")
        return 1
    log.info(f"PASS: Bucket {bucket} deletion succesfull")

    log.info(f"PASSED : Bucket '{bucket}' successfully created and verified.")
    return 0
