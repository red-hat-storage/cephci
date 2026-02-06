"""AWS EC2 provider implementation for CephVMNode."""

from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep
from typing import List, Optional

import yaml

from utility.log import Log

from .exceptions import NodeDeleteFailure, NodeError

LOG = Log(__name__)


def process_aws_custom_config(custom_config):
    """Process the custom config for AWS target VPC.

    Users can provide a specific VPC for deployment via aws_vpc. Platform
    details are loaded from conf/aws/{vpc_name}.yaml (e.g. conf/aws/default.yaml).

    Arguments:
        custom_config(list): List of <key>=<value> (e.g. aws_vpc=default, aws_instance_type=t3.medium)

    Returns:
        A dictionary containing the platform information (subnet_id, security_group_ids, etc.)
    """
    repo_dir = Path(__file__).resolve().parent.parent
    vpc_name = "default"

    if custom_config:
        overrides = dict(
            item.split("=") for item in custom_config if item.startswith("aws_")
        )
        if "aws_vpc" in overrides:
            vpc_name = overrides["aws_vpc"]

    platform_conf = repo_dir.joinpath(f"conf/aws/{vpc_name}.yaml")
    if not platform_conf.exists():
        return {"vpc_name": vpc_name}

    with platform_conf.open() as fh:
        platform_dict = yaml.safe_load(fh) or {}

    if custom_config:
        overrides = dict(
            item.split("=") for item in custom_config if item.startswith("aws_")
        )
        if "aws_instance_type" in overrides:
            platform_dict["instance_type"] = overrides["aws_instance_type"]
        if "aws_subnet_id" in overrides:
            platform_dict["subnet_id"] = overrides["aws_subnet_id"]

    return platform_dict


def cleanup_aws_ceph_nodes(aws_cred, pattern, custom_config=None):
    """
    Clean up EC2 instances that match the given pattern (in instance Name tag).

    Args:
        aws_cred: Global configuration file (with globals["aws-credentials"]).
        pattern: Pattern to match instance name (e.g. run id or prefix).
        custom_config: Optional list of key=value for platform overrides.
    """
    from ceph.parallel import parallel

    LOG.info("Destroying existing AWS instances matching pattern %s", pattern)
    glbs = aws_cred.get("globals")
    if not glbs:
        raise NodeError("Missing 'globals' section in AWS credentials file")
    aws_cfg = glbs.get("aws-credentials")
    if not aws_cfg:
        raise NodeError("Missing 'aws-credentials' section in globals")

    platform = process_aws_custom_config(custom_config or [])

    region = aws_cfg.get("region")
    if not region:
        raise NodeError("Missing 'region' in aws-credentials")

    access_key = aws_cfg.get("access_key")
    secret_key = aws_cfg.get("secret_key")

    if access_key:
        access_key = access_key.strip()
    if secret_key:
        secret_key = secret_key.strip()

    if not access_key or not secret_key:
        LOG.warning(
            "access_key or secret_key not provided, using default credential chain"
        )

    vpc_id_from_creds = aws_cfg.get("vpc_id")
    vpc_id_from_platform = platform.get("vpc_id")

    if vpc_id_from_platform and (
        "xxxxxxxx" in vpc_id_from_platform
        or "placeholder" in vpc_id_from_platform.lower()
    ):
        LOG.debug(
            "Ignoring placeholder VPC ID from platform config: %s", vpc_id_from_platform
        )
        vpc_id_from_platform = None

    vpc_id = vpc_id_from_creds or vpc_id_from_platform

    if vpc_id:
        source = "credentials file" if vpc_id_from_creds else "platform config"
        LOG.debug("Using VPC ID %s from %s", vpc_id, source)

    client = get_ec2_client(
        region=region,
        access_key=access_key,
        secret_key=secret_key,
    )

    try:
        LOG.debug("Testing AWS credentials...")
        client.describe_regions()
        LOG.debug("AWS credentials validated successfully")
    except Exception as e:
        LOG.warning("Credential validation failed (will proceed anyway): %s", e)
        LOG.warning(
            "Region: %s, Access key present: %s (starts with: %s), Secret key present: %s",
            region,
            bool(access_key),
            access_key[:10] + "..." if access_key and len(access_key) > 10 else "N/A",
            bool(secret_key),
        )
        LOG.warning(
            "If EC2 operations fail, verify credentials in AWS Console: "
            "IAM → Users → ceph-sys-test → Security credentials"
        )

    filters = [
        {
            "Name": "instance-state-name",
            "Values": ["running", "pending", "stopping", "stopped", "terminating"],
        }
    ]

    if vpc_id:
        filters.append({"Name": "vpc-id", "Values": [vpc_id]})
        LOG.info("Filtering instances by VPC: %s (from conf/osp-cred-aws.yaml)", vpc_id)
    else:
        LOG.warning(
            "No VPC ID specified in config - searching all VPCs in region %s", region
        )

    instances = []
    paginator = client.get_paginator("describe_instances")

    for page in paginator.paginate(Filters=filters):
        for res in page.get("Reservations", []):
            for inst in res.get("Instances", []):
                name = ""
                inst_vpc_id = inst.get("VpcId", "no-vpc")
                for tag in inst.get("Tags", []):
                    if tag.get("Key") == "Name":
                        name = tag.get("Value", "")
                        break

                if pattern.lower() in name.lower():
                    inst["Name"] = name or inst.get("InstanceId", "")
                    instances.append(inst)
                    LOG.debug(
                        "Matched pattern '%s' in instance: %s (ID: %s, VPC: %s)",
                        pattern,
                        name,
                        inst.get("InstanceId"),
                        inst_vpc_id,
                    )

    LOG.info(
        "Found %d instances matching pattern '%s' in VPC %s: %s",
        len(instances),
        pattern,
        vpc_id or "all",
        [i.get("Name") or i.get("InstanceId") for i in instances],
    )

    counter = 0
    with parallel() as p:
        for instance in instances:
            sleep(counter * 3)
            vm = CephVMNodeAWS(
                aws_cred={
                    "region": region,
                    "access_key": aws_cfg.get("access_key"),
                    "secret_key": aws_cfg.get("secret_key"),
                },
                node=instance,
            )
            p.spawn(vm.delete)
            counter += 1

    LOG.info("Done cleaning up AWS nodes with pattern %s", pattern)


def get_ec2_client(
    region: str, access_key: Optional[str] = None, secret_key: Optional[str] = None
):
    """
    Return an EC2 client for the given region and credentials.

    Args:
        region: AWS region (e.g. us-east-1).
        access_key: Optional access key; if omitted, uses default credential chain.
        secret_key: Optional secret key; if omitted, uses default credential chain.
    """
    import boto3

    kwargs = {"region_name": region}
    if access_key and secret_key:
        kwargs["aws_access_key_id"] = access_key
        kwargs["aws_secret_access_key"] = secret_key
    return boto3.client("ec2", **kwargs)


class CephVMNodeAWS:
    """Represent the VM node required for cephci on AWS EC2."""

    def __init__(
        self,
        aws_cred: dict,
        node: Optional[dict] = None,
    ) -> None:
        """
        Initialize the instance using the provided information.

        Args:
            aws_cred: Dictionary containing 'region' and optionally 'access_key', 'secret_key'.
            node: Optional instance dict from describe_instances (for cleanup or get-by-id).
        """
        self._aws_cred = aws_cred
        self._subnet: str = ""
        self._roles: list = list()
        self.node = None
        self.root_login: bool = True

        # Lazy import: boto3 is only needed when actually using AWS
        self.client = get_ec2_client(
            region=aws_cred["region"],
            access_key=aws_cred.get("access_key"),
            secret_key=aws_cred.get("secret_key"),
        )

        if node:
            self.node = node

    def __getstate__(self) -> dict:
        """Return state for pickle; exclude non-picklable EC2 client."""
        state = self.__dict__.copy()
        state.pop("client", None)
        return state

    def __setstate__(self, state: dict) -> None:
        """Restore state from pickle; recreate EC2 client from credentials."""
        self.__dict__.update(state)
        self.client = get_ec2_client(
            region=self._aws_cred["region"],
            access_key=self._aws_cred.get("access_key"),
            secret_key=self._aws_cred.get("secret_key"),
        )

    @property
    def ip_address(self) -> str:
        """Return the private IP address of the node."""
        if not self.node:
            return ""
        return self.node.get("PrivateIpAddress", "")

    @property
    def hostname(self) -> str:
        """Return the hostname of the VM (private DNS name or instance name)."""
        if not self.node:
            return ""
        return self.node.get("PrivateDnsName") or self.node.get("Name", "")

    @property
    def volumes(self) -> List:
        """Return the list of EBS block device attachments (excluding root)."""
        if not self.node:
            return []
        attachments = []
        for bdm in self.node.get("BlockDeviceMappings", []):
            if bdm.get("DeviceName") == self.node.get("RootDeviceName"):
                continue
            if "Ebs" in bdm:
                attachments.append(bdm)
        return attachments

    @property
    def subnet(self) -> str:
        """Return the subnet CIDR or id for the node."""
        if self._subnet:
            return self._subnet
        if not self.node:
            return ""
        subnet_id = None
        for eni in self.node.get("NetworkInterfaces", []):
            if eni.get("Attachment", {}).get("DeviceIndex") == 0:
                subnet_id = eni.get("SubnetId")
                break
        if not subnet_id:
            return ""
        from botocore.exceptions import ClientError

        try:
            resp = self.client.describe_subnets(SubnetIds=[subnet_id])
            subnets = resp.get("Subnets", [])
            if subnets:
                self._subnet = subnets[0].get("CidrBlock", "")
                return self._subnet
        except ClientError as e:
            LOG.warning("Failed to describe subnet %s: %s", subnet_id, e)
        return ""

    @property
    def shortname(self) -> str:
        """Return the short form of the hostname."""
        return self.hostname.split(".")[0] if self.hostname else ""

    @property
    def no_of_volumes(self) -> int:
        """Return the number of volumes attached to the VM."""
        return len(self.volumes)

    @property
    def role(self) -> List:
        """Return the Ceph roles of the instance."""
        return self._roles

    @role.setter
    def role(self, roles: list) -> None:
        """Set the roles for the VM."""
        self._roles = deepcopy(roles)

    @property
    def node_type(self) -> str:
        """Return the provider type."""
        return "aws"

    def create(
        self,
        node_name: str,
        image_id: str,
        subnet_id: str,
        security_group_ids: List[str],
        key_name: str,
        instance_type: str,
        userdata: str = "",
        size_of_disks: int = 0,
        no_of_volumes: int = 0,
    ) -> None:
        """
        Create the EC2 instance with the provided data.

        Uses existing VPC/subnet; does not create VPC or DNS records.

        Args:
            node_name: Name of the VM (used for Name tag).
            image_id: AMI id.
            subnet_id: Existing subnet id.
            security_group_ids: List of security group ids.
            key_name: Name of the key pair.
            instance_type: Instance type (e.g. t3.medium).
            userdata: User data (cloud-init) string.
            size_of_disks: Size in GiB for each additional EBS volume.
            no_of_volumes: Number of additional EBS volumes to attach.
        """
        from botocore.exceptions import ClientError

        LOG.info("Starting to create EC2 instance with name %s", node_name)
        try:
            block_devices = []
            if no_of_volumes and size_of_disks:
                for i in range(no_of_volumes):
                    # Use /dev/sdf, /dev/sdg, ... for additional EBS volumes
                    device_name = f"/dev/sd{chr(ord('f') + i)}"
                    block_devices.append(
                        {
                            "DeviceName": device_name,
                            "Ebs": {
                                "VolumeSize": size_of_disks,
                                "VolumeType": "gp3",
                                "DeleteOnTermination": True,
                            },
                        }
                    )

            # Ensure security_group_ids is a list of strings (VPC requires SecurityGroupIds, not groupName)
            # This is critical - if passed as string, boto3 misinterprets it and causes groupName error
            if security_group_ids is None:
                raise NodeError(
                    "security_group_ids is required for VPC instances (cannot be None)"
                )

            # Convert to list if needed
            if isinstance(security_group_ids, str):
                security_group_ids = [security_group_ids]
            elif not isinstance(security_group_ids, (list, tuple)):
                raise NodeError(
                    f"security_group_ids must be a list or tuple, got {type(security_group_ids)}: {security_group_ids}"
                )
            else:
                # Convert tuple to list if needed
                security_group_ids = list(security_group_ids)

            # Filter and validate each element is a non-empty string
            security_group_ids = [str(sg).strip() for sg in security_group_ids if sg]
            security_group_ids = [
                sg for sg in security_group_ids if sg and sg.startswith("sg-")
            ]

            if not security_group_ids:
                raise NodeError(
                    f"security_group_ids list cannot be empty. Got: {security_group_ids}"
                )

            LOG.debug(
                "Validated security_group_ids: %s (type: %s)",
                security_group_ids,
                type(security_group_ids),
            )

            # Build run_instances parameters - VPC mode (SubnetId + SecurityGroupIds)
            # Do NOT use groupName (EC2-Classic) when SubnetId is specified
            run_kwargs = {
                "ImageId": image_id,
                "InstanceType": instance_type,
                "MinCount": 1,
                "MaxCount": 1,
                "SubnetId": subnet_id,  # VPC subnet - must use SecurityGroupIds, not groupName
                "SecurityGroupIds": security_group_ids,  # VPC security groups
            }

            # Optional parameters
            if key_name:
                run_kwargs["KeyName"] = key_name
            if userdata:
                run_kwargs["UserData"] = userdata
            if block_devices:
                run_kwargs["BlockDeviceMappings"] = block_devices

            # Tags
            run_kwargs["TagSpecifications"] = [
                {
                    "ResourceType": "instance",
                    "Tags": [{"Key": "Name", "Value": node_name}],
                }
            ]

            # Log what we're sending (excluding sensitive data)
            LOG.info(
                "run_instances parameters: ImageId=%s, SubnetId=%s, SecurityGroupIds=%s (type: %s), InstanceType=%s",
                image_id,
                subnet_id,
                security_group_ids,
                type(security_group_ids).__name__,
                instance_type,
            )
            LOG.debug(
                "Full run_kwargs: %s",
                {k: v for k, v in run_kwargs.items() if k != "UserData"},
            )

            # Explicitly check for any groupName-related keys (case-insensitive)
            forbidden_keys = [
                k
                for k in run_kwargs.keys()
                if "group" in k.lower() and "name" in k.lower()
            ]
            if forbidden_keys:
                raise NodeError(
                    "Forbidden parameter(s) detected: "
                    f"{forbidden_keys}. Use SecurityGroupIds (list) for VPC, "
                    "not groupName."
                )

            # Ensure SecurityGroupIds is definitely a list (not tuple, not string)
            if "SecurityGroupIds" in run_kwargs:
                sg_ids = run_kwargs["SecurityGroupIds"]
                if not isinstance(sg_ids, list):
                    raise NodeError(
                        f"SecurityGroupIds must be a list, got {type(sg_ids)}: {sg_ids}"
                    )
                # Convert any non-list iterables to list
                run_kwargs["SecurityGroupIds"] = list(sg_ids)
                LOG.debug(
                    "SecurityGroupIds after validation: %s (type: %s)",
                    run_kwargs["SecurityGroupIds"],
                    type(run_kwargs["SecurityGroupIds"]).__name__,
                )

            resp = self.client.run_instances(**run_kwargs)
            instances = resp.get("Instances", [])
            if not instances:
                raise NodeError("run_instances returned no instances")

            instance_id = instances[0]["InstanceId"]
            self._wait_until_vm_state(instance_id, target_state="running")

            self.node = self._get_instance(instance_id)
            if not self.node:
                raise NodeError(f"Failed to get instance {instance_id} after run")

            # Store subnet cidr for subnet property
            try:
                sub_resp = self.client.describe_subnets(SubnetIds=[subnet_id])
                subnets = sub_resp.get("Subnets", [])
                if subnets:
                    self._subnet = subnets[0].get("CidrBlock", "")
            except ClientError:
                pass

            LOG.info("Created EC2 instance %s (%s)", node_name, instance_id)

        except NodeError:
            raise
        except ClientError as e:
            LOG.error(e, exc_info=True)
            raise NodeError(f"Failed to create VM {node_name}: {e}")
        except BaseException as be:
            LOG.error(be, exc_info=True)
            raise NodeError(f"Unknown error. Failed to create VM with name {node_name}")

    def delete(self) -> None:
        """Remove the EC2 instance (and its EBS volumes if DeleteOnTermination)."""
        if not self.node:
            return

        instance_id = self.node["InstanceId"]
        node_name = self.node.get("Name", instance_id)

        from botocore.exceptions import ClientError

        LOG.info("Terminating instance %s (%s)", node_name, instance_id)
        try:
            self.client.terminate_instances(InstanceIds=[instance_id])
        except ClientError as e:
            LOG.warning("terminate_instances failed: %s", e)
            raise NodeDeleteFailure(f"Failed to terminate {node_name}: {e}")

        self._wait_until_vm_state(instance_id, target_state="terminated")
        self.node = None
        LOG.info("Successfully removed %s", node_name)

    def _get_instance(self, instance_id: str) -> Optional[dict]:
        """Return instance dict from describe_instances, with Name tag set."""
        from botocore.exceptions import ClientError

        try:
            resp = self.client.describe_instances(InstanceIds=[instance_id])
            for res in resp.get("Reservations", []):
                for inst in res.get("Instances", []):
                    name = ""
                    for tag in inst.get("Tags", []):
                        if tag.get("Key") == "Name":
                            name = tag.get("Value", "")
                            break
                    inst["Name"] = name or instance_id
                    return inst
        except ClientError as e:
            LOG.warning("describe_instances failed: %s", e)
        return None

    def _wait_until_vm_state(
        self, instance_id: str, target_state: str, timeout: int = 1200
    ) -> None:
        """Wait until the instance reaches the given state."""
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=timeout)
        valid_states = ("pending", "running", "stopping", "stopped", "terminated")

        while end_time > datetime.now():
            sleep(10)
            try:
                inst = self._get_instance(instance_id)
                if not inst:
                    if target_state == "terminated":
                        return
                    continue
                state = inst.get("State", {}).get("Name", "")
                if state not in valid_states:
                    continue
                if state == target_state:
                    duration = (datetime.now() - start_time).total_seconds()
                    LOG.info(
                        "Instance %s reached %s in %s seconds.",
                        instance_id,
                        target_state,
                        duration,
                    )
                    return
                if state == "terminated" and target_state != "terminated":
                    raise NodeError(f"Instance {instance_id} terminated unexpectedly")
            except NodeError:
                raise
            except BaseException as be:
                LOG.warning(be)

        raise NodeError(
            f"Instance {instance_id} did not reach {target_state} within {timeout}s"
        )
