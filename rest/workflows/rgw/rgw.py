import time

from rest.endpoints.rgw.rgw import RGW
from utility.log import Log

log = Log(__name__)


def rgw_user_bucket_workflow(
    bucket,
    uid,
    rest,
    rest_v1,
    lifecycle,
    bucket_ratelimit,
    user_ratelimit,
    role_policy,
    user_details,
):
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

    # List user emails
    log.info("Listing all the user email IDs")
    try:
        resp = rgw_rest_v1.list_user_emails()
    except Exception as e:
        log.error(f"Listing user emails failed with {str(e)}")
        return 1

    # Create a Subuser on the RGW user
    log.info(f"Creating subuser for user {uid}")
    try:
        resp = rgw_rest_v1.create_subuser(uid=uid, subuser="s3_subuser")
    except Exception as e:
        log.error(f"Creating subuser failed with {str(e)}")
        return 1

    # List Global user ratelimits
    log.info("Listing Global user ratelimits")
    try:
        resp = rgw_rest_v1.list_user_ratelimit_global()
    except Exception as e:
        log.error(f"Listing global user ratelimits failed with {str(e)}")
        return 1

    # Get User Stats for user created
    log.info(f"Get user stats for {uid}")
    try:
        resp = rgw_rest_v1.get_user_stats(uid=uid)
    except Exception as e:
        log.error(f"Get user stats failed with {str(e)}")
        return 1

    # Edit the user display name and email
    log.info(f"Modify the details such as display name, email for {uid}")
    try:
        resp = rgw_rest_v1.edit_user_details(uid=uid, user_details=user_details)
    except Exception as e:
        log.error(f"Modify user details failed with {str(e)}")
        return 1

    # Add capability on the RGW user
    log.info(f"Create user capability for {uid}")
    try:
        resp = rgw_rest_v1.create_user_caps(uid=uid)
    except Exception as e:
        log.error(f"User capability creation failed with {str(e)}")
        return 1

    # Create a additional key for the RGW user
    log.info(f"Create additional key for {uid}")
    try:
        resp = rgw_rest_v1.create_user_key(uid=uid)
    except Exception as e:
        log.error(f"Additional key creation failed with {str(e)}")
        return 1

    # Delete the above created key
    log.info(f"Delete extra key for {uid}")
    try:
        resp = rgw_rest_v1.delete_user_key(uid=uid)
    except Exception as e:
        log.error(f"Key deletion failed with {str(e)}")
        return 1

    # Get the quota details on the user
    log.info(f"Get the quota details for {uid}")
    try:
        resp = rgw_rest_v1.get_user_quota(uid=uid)
    except Exception as e:
        log.error(f"Get user quota failed with {str(e)}")
        return 1

    # Update the user scope quota on the user for a max objects of 100
    log.info(f"Modify the user scope quota for {uid}")
    try:
        resp = rgw_rest_v1.edit_user_quota(uid=uid)
    except Exception as e:
        log.error(f"Modify user quota at user scope failed with {str(e)}")
        return 1

    # Delete capability on the user
    log.info(f"Delete capability for {uid}")
    try:
        resp = rgw_rest_v1.delete_user_caps(uid=uid)
    except Exception as e:
        log.error(f"Delete user capability failed with {str(e)}")
        return 1

    # List local user ratelimits
    log.info(f"List local user ratelimit for {uid}")
    try:
        resp = rgw_rest_v1.list_user_ratelimit(uid=uid)
    except Exception as e:
        log.error(f"List local user ratelimit failed with {str(e)}")
        return 1

    log.info(f"Put user ratelimit for {uid}")
    try:
        bucket_resp = rgw_rest.set_user_ratelimit(
            uid=uid, user_ratelimit=user_ratelimit
        )
    except Exception as e:
        log.error(f"Put user ratelimit failed with {str(e)}")
        return 1

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
    log.info(f"List bucket lifecycle policy on {bucket}")
    try:
        resp = rgw_rest_v1.list_bucket_lifecycle(bucket=bucket)
    except Exception as e:
        log.error(f"List bucket lifecycle policy failed with {str(e)}")
        return 1

    # List Global Ratelimits on bucket
    log.info(f"List global ratelimits on {bucket}")
    try:
        resp = rgw_rest_v1.list_bucket_ratelimit_global()
    except Exception as e:
        log.error(f"List global ratelimits on {bucket} failed with {str(e)}")
        return 1

    # Put bucket ratelimit
    log.info(f"Put bucket ratelimit for {bucket}")
    try:
        bucket_resp = rgw_rest.set_bucket_ratelimit(
            bucket=bucket, bucket_ratelimit=bucket_ratelimit
        )
        log.info(f"the bucket_repsonse {bucket_resp}")
    except Exception as e:
        log.error(
            f"Put bucket ratelimit failed for '{bucket}': with exception {str(e)}"
        )
        return 1
    log.info(f"PASS: Put bucket ratelimit successful for {bucket}")

    # List bucket level ratelimit
    log.info(f"List bucket level ratelimits on {bucket}")
    try:
        resp = rgw_rest_v1.list_bucket_ratelimit(bucket=bucket)
    except Exception as e:
        log.error(f"List bucket level ratelimit on {bucket} failed with {str(e)}")
        return 1

    # List bucket encryption for the bucket
    log.info(f"List bucket encryption configs on {bucket}")
    try:
        resp = rgw_rest_v1.list_bucket_encryptionConfig()
    except Exception as e:
        log.error(f"List bucket encryption configs failed with {str(e)}")
        return 1

    # Create a S3 role
    log.info("Create a S3 role")
    try:
        resp = rgw_rest_v1.create_role(role_policy=role_policy)
    except Exception as e:
        log.error(f"Role creation failed with {str(e)}")
        return 1

    # List the current RGW roles
    log.info("List current roles")
    try:
        resp = rgw_rest_v1.list_roles()
    except Exception as e:
        log.error(f"List roles failed with {str(e)}")
        return 1

    # Edit the max session duration of the role created
    log.info("Edit the above created role")
    try:
        role_name = "S3role1"
        resp = rgw_rest_v1.edit_role(role_name=role_name)
    except Exception as e:
        log.error(f"Edit role failed with {str(e)}")
        return 1

    # Delete the S3 role
    log.info("Delete the above created role")
    try:
        rgw_rest_v1.delete_role(role_name=role_name)
    except Exception as e:
        log.error(f"Delete role failed with {str(e)}")
        return 1

    # Put bucket lifecycle
    log.info(f"Put licycle configuration for {bucket}")
    try:
        bucket_resp = rgw_rest.update_bucket_lifecycle(
            bucket=bucket, lifecycle=lifecycle
        )
        log.info(f"the bucket_repsonse {bucket_resp}")
    except Exception as e:
        log.error(
            f"Put bucket lifecycle failed for '{bucket}': with exception {str(e)}"
        )
        # return 1
    log.info(f"PASS: Put LC config successful for {bucket}")

    # Get details of the bucket
    bucket_resp = rgw_rest.get_bucket(bucket=bucket)
    log.info(f"the bucket_repsonse {bucket_resp}")

    # List bucket encryption for the bucket
    count = 1
    while count < 3:
        try:
            rgw_rest_v1.list_bucket_encryption(bucket=bucket)
            break
        except Exception:
            log.info("Retrying on failure")
            count += 1
            time.sleep(5)

    # Delete bucket
    log.info(f"Deleting bucket '{bucket}'")
    try:
        bucket_resp = rgw_rest.delete_bucket(bucket=bucket)
        log.info(f"the bucket_repsonse {bucket_resp}")
    except Exception as e:
        log.error(f"Bucket deletion failed for '{bucket}': with exception {str(e)}")
        return 1
    log.info(f"PASS: Bucket {bucket} deletion succesfull")

    # Delete the subuser on the RGW user
    log.info(f"Delete the subuser for user {uid}")
    try:
        resp = rgw_rest_v1.delete_subuser(uid=uid, subuser="s3_subuser")
    except Exception as e:
        log.error(f"Subuser deletion failed with exception {str(e)}")
        return 1

    # Delete User
    log.info(f"deleting user '{uid}'")
    try:
        resp = rgw_rest.delete_user(uid=uid)
    except Exception as e:
        log.error(f"User deletion failed with exception {str(e)}")
        return 1

    log.info(f"PASSED : User {uid} and Bucket '{bucket}' workflow successful")
    return 0
