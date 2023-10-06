"""
Module to Generate, Modify and Apply Crush maps on ceph cluster. Methods include:
1. Generate bin file.
2. Compile bin file
3. decomiple bin file
4. setting the new bin file on cluster.
5. Addition of new bucket into bin file, and it's verification.
6. Re-weighting the OSDs in bin file, and it's verification.
7. Moving buckets from one to other in bin file, and it's Verification.
8. bin file tests. ( stats, bad mappings etc.)
9. dump and verify bin file contents
"""
import json
import re

from ceph.ceph_admin import CephAdmin
from ceph.rados.core_workflows import RadosOrchestrator
from utility.log import Log

log = Log(__name__)


def count(func):
    """
    Decorator method to check how many times a particular method has been invoked
    :param func: name of the function
    :return: wrapped method
    """

    def wrapped(*args, **kwargs):
        wrapped.calls += 1
        return func(*args, **kwargs)

    wrapped.calls = 0
    return wrapped


class CrushToolWorkflows:
    """
    Contains various functions that help to Generate, Modify and Apply Crush maps on ceph cluster
    """

    def __init__(self, node: CephAdmin):
        """
        initializes the env to run CRUSH commands

        Initializes the crush object with the cluster objects,along with installing necessary packages
        required for executing crushtool commands
        Args:
            node: CephAdmin object
        """
        self.rados_obj = RadosOrchestrator(node=node)
        self.cluster = node.cluster
        self.config = node.config
        self.client = node.cluster.get_nodes(role="client")[0]

        # Checking and installing ceph-base package on Client
        try:
            out, rc = self.client.exec_command(
                sudo=True, cmd="rpm -qa | grep ceph-base"
            )
        except Exception:
            self.client.exec_command(sudo=True, cmd="yum install -y ceph-base")

    def generate_crush_map_bin(self, loc="/tmp") -> (bool, str):
        """Module to generate the CRUSH bin file

         This method makes a getcrushmap call and saves the output the provided location.
         Args::
            loc: Location where the crush file needs to be generated
        Examples::
            status, file_loc = obj.generate_crush_map_bin(loc="/tmp/crush.map.bin")
        Returns::
            Returns a tuple consisting of the execution status and the bin file location
            (True, /tmp/crush.map.bin)
        """
        # Extracting the ceush map from the cluster
        cmd = f"ceph osd getcrushmap > {loc}/crush.map.bin"
        self.client.exec_command(cmd=cmd, sudo=True)
        return (
            self.rados_obj.check_file_exists_on_client(loc=f"{loc}/crush.map.bin"),
            f"{loc}/crush.map.bin",
        )

    def decompile_crush_map_txt(self, **kwargs) -> (bool, str):
        """Module to generate the CRUSH text file by decompiling the bin file

         This method makes use of crushtool to decompile the bin file and save the output the provided location.
         Args::
            kwargs:
                source_loc: Location from where the bin file should be sourced
                target_loc: Location where the crush file needs to be generated
        Examples::
            status, file_loc = obj.decompile_crush_map_txt(
                source_loc="/tmp/crush.map.bin",
                target_loc="/tmp/crush.map.txt",
            )
        Returns::
            Returns a tuple consisting of the execution status and the bin file location
            (True, /tmp/crush.map.bin)
        """
        source_loc = kwargs.get("source_loc", "/tmp/crush.map.bin")
        target_loc = kwargs.get("target_loc", "/tmp")
        if not self.rados_obj.check_file_exists_on_client(loc=source_loc):
            log.error(f"file : {source_loc} not present on the Client")
            return False, ""

        # Generating text file for the bin file generated
        cmd = f"crushtool -d {source_loc} -o {target_loc}/crush.map.txt"
        self.client.exec_command(cmd=cmd, sudo=True)
        return (
            self.rados_obj.check_file_exists_on_client(
                loc=f"{target_loc}/crush.map.txt"
            ),
            f"{target_loc}/crush.map.txt",
        )

    def compile_crush_map_txt(self, **kwargs) -> (bool, str):
        """Module to generate the CRUSH bin file by compiling the text file

         This method makes use of crushtool to compile the text file and save the output the provided location.
         Args::
            kwargs:
                source_loc: Location from where the bin file should be sourced
                target_loc: Location where the crush file needs to be generated
        Examples::
            status, file_loc = obj.compile_crush_map_txt(
                source_loc="/tmp/crush.map.txt",
                target_loc="/tmp/crush.map.bin",
            )
        Returns::
            Returns a tuple consisting of the execution status and the bin file location
            (True, /tmp/crush.map.bin)
        """
        source_loc = kwargs.get("source_loc", "/tmp/crush.map.txt")
        target_loc = kwargs.get("target_loc", "/tmp")
        if not self.rados_obj.check_file_exists_on_client(loc=source_loc):
            log.error(f"file : {source_loc} not present on the Client")
            return False

        # Generating bin file for the modified crush text file
        cmd = f"crushtool -c {source_loc} -o {target_loc}/crush_modified.map.bin"
        try:
            self.client.exec_command(cmd=cmd, sudo=True)
        except Exception as error:
            log.error(f"Failed the compile the text file into bin file. error: {error}")
            return False
        return (
            self.rados_obj.check_file_exists_on_client(
                loc=f"{target_loc}/crush_modified.map.bin"
            ),
            f"{target_loc}/crush_modified.map.bin",
        )

    def set_crush_map_bin(self, loc="/tmp/crush_modified.map.bin") -> bool:
        """Module to set the CRUSH bin file as the acting crush map on cluster

         This method makes a getcrushmap call and sets the provided crush bin file as the crush map for the cluster.
         Args::
            loc: Location from where the crush bin file needs to be fetched
        Examples::
            status = obj.set_crush_map_bin(loc="/tmp/crush.map.bin")
        Returns::
            Returns a tuple consisting of the execution status and the bin file location
            (True, /tmp/crush.map.bin)
        """
        if not self.rados_obj.check_file_exists_on_client(loc=loc):
            log.error(f"Bin file : {loc} not present on the Client")
            return False

        # Setting the modified crush bin as the new crush map for ceph
        cmd = f"ceph osd setcrushmap -i {loc}"
        try:
            self.client.exec_command(cmd=cmd, sudo=True)
        except Exception as error:
            log.error(
                f"Failed to set the modified bin file as the new crush map for the cluster. error: {error}"
            )
            return False
        log.debug(f"Successfully set file: {loc} as the New Crush map in the cluster")
        return True

    @count
    def add_new_bucket_into_bin(self, **kwargs) -> (bool, str):
        """Method to add a new crush bucket into the crush map

        This method uses crushtool utility to modify the crush map to add new buckets.
        A new file would be automatically be created with a unique count, which indicates the number
         of times the method has been invoked

        Args::
            kwargs: Various KW arguments that need to be passed are :
                source_loc: Name of the source bin file that should be modified
                bucket_name: Name of the bucket to be added
                bucket_type: Type of bucket to be added
                target_loc: Target location along with the filename for the new bin file

        Examples::
            status, file_loc = obj.add_new_bucket_into_bin(
                source_loc="/tmp/crush.map.bin",
                target_loc="/tmp/crush_modified.map.bin",
                bucket_name="arbiter",
                bucket_type="datacenter",
            )

        Returns::
        Tuple containing the execution status and the file path for modified bin
        (True, "/tmp/crush_modified.map.bin")
        """
        source_loc = kwargs.get("source_loc", "/tmp/crush.map.bin")
        target_loc = kwargs.get(
            "target_loc",
            f"/tmp/crush_modified_add_{self.add_new_bucket_into_bin.calls}.map.bin",
        )
        bucket_name = kwargs.get("bucket_name")
        bucket_type = kwargs.get("bucket_type")

        if not self.rados_obj.check_file_exists_on_client(loc=source_loc):
            log.error(f"file : {source_loc} not present on the Client")
            return False, target_loc

        log.debug(
            f"Modification sent for addition of bucket: {bucket_name} with Crush type : {bucket_type}."
        )
        # Generating bin file for the addition of new buckets
        cmd = f"crushtool -i {source_loc} --add-bucket {bucket_name} {bucket_type} -o {target_loc}"
        try:
            self.client.exec_command(cmd=cmd, sudo=True)
        except Exception as error:
            log.error(
                f"Failed to modify and add bucket: {bucket_name} of type : {bucket_type} into bin file. error: {error}"
            )
            return False, target_loc

        log.debug(f"Successfully modified the bin file : {target_loc}")
        return self.rados_obj.check_file_exists_on_client(loc=target_loc), target_loc

    def verify_add_new_bucket_into_bin(self, **kwargs) -> bool:
        """Method to Verify addition of new crush buckets into the crush map

        This method uses crushtool utility to verify modification the crush map to add new buckets.
        Args::
            kwargs: Various KW arguments that need to be passed are :
                loc: Name of the source bin file that should be tested
                bucket_name: Name of the bucket to be added
                bucket_type: Type of bucket to be added

        Examples::
            status, file_loc = obj.verify_add_new_bucket_into_bin(
                loc="/tmp/crush_modified.map.bin",
                bucket_name="arbiter",
                bucket_type="datacenter",
            )

        Returns::
            True -> If the bucket is present in the bin supplied
            False -> If the bucket is not present in the bin supplied
        """
        loc = kwargs.get("loc", "/tmp/crush_modified.map.bin")
        bucket_name = kwargs.get("bucket_name")
        bucket_type = kwargs.get("bucket_type")

        if not self.rados_obj.check_file_exists_on_client(loc=loc):
            log.error(f"file : {loc} not present on the Client")
            return False

        log.debug(
            f"Checking for addition of bucket: {bucket_name} with Crush type : {bucket_type}."
        )

        # Getting the crush map dump
        res, crush_dump = self.dump_bin_contents(loc=loc)
        for entry in crush_dump["buckets"]:
            if entry["type_name"] == bucket_type:
                if entry["name"] == bucket_name:
                    log.info(
                        f"Bucket {bucket_name} of type {bucket_type} successfully added to the bin"
                    )
                    return True

        log.error(
            f"Failed verification of add bucket for: {bucket_name} of type : {bucket_type} into bin file."
        )
        return False

    @count
    def reweight_buckets_in_bin(self, **kwargs) -> (bool, str):
        """Method to modify the weight of crush items passed

        This method uses crushtool utility to modify the crush map to change thw wright of objects.
        These mostly include OSD daemons which are the leaves of the CRUSH tree.
        A new file would be automatically be created with a unique count, which indicates the number
         of times the method has been invoked

        Args::
            kwargs: Various KW arguments that need to be passed are :
                source_loc: Name of the source bin file that should be modified
                bucket_name: Name of the bucket to be added
                reweight_val: weight of the item to be set
                target_loc: Target location along with the filename for the new bin file

        Examples::
            status, file_loc = obj.reweight_buckets_in_bin(
                source_loc="/tmp/crush.map.bin",
                target_loc="/tmp/crush_modified.map.bin",
                bucket_name="osd.11",
                reweight_val="0.020",
            )
        Returns::
        Tuple containing the execution status and the file path for modified bin
        (True, "/tmp/crush_modified.map.bin")
        """
        source_loc = kwargs.get("source_loc", "/tmp/crush.map.bin")
        target_loc = kwargs.get(
            "target_loc",
            f"/tmp/crush_modified_reweight_{self.reweight_buckets_in_bin.calls}.map.bin",
        )
        bucket_name = kwargs.get("bucket_name")
        reweight_val = kwargs.get("reweight_val")

        if not self.rados_obj.check_file_exists_on_client(loc=source_loc):
            log.error(f"file : {source_loc} not present on the Client")
            return False, target_loc

        log.debug(
            f"Modification sent for reweight of bucket: {bucket_name} to Value : {reweight_val}."
        )
        # Generating bin file for reweight of buckets
        cmd = f"crushtool -i {source_loc} --reweight-item {bucket_name} {reweight_val} -o {target_loc}"
        try:
            self.client.exec_command(cmd=cmd, sudo=True)
        except Exception as error:
            log.error(
                f"Failed to modify the weight of bucket: {bucket_name} to Value : {reweight_val}. error: {error}"
            )
            return False, target_loc

        log.debug(f"Successfully modified the bin file : {target_loc}")
        return self.rados_obj.check_file_exists_on_client(loc=target_loc), target_loc

    def verify_reweight_buckets_in_bin(self, **kwargs) -> bool:
        """Method to Verify modification of weights of buckets into the crush map

        This method uses crushtool utility to verify modification the crush map to modify the weights of buckets.
        Args::
            kwargs: Various KW arguments that need to be passed are :
                loc: Name of the source bin file that should be tested
                bucket_name: Name of the bucket to be added
                reweight_val: Value that should be verified

        Examples::
            status = obj.verify_add_new_bucket_into_bin(
                loc="/tmp/crush_modified.map.bin",
                bucket_name="arbiter",
                reweight_val="0.020"
            )
        Returns::
        True -> If the bucket is weighted as sent in the bin supplied
        False -> If the bucket is not weighted as sent in the bin supplied
        """
        loc = kwargs.get("loc", "/tmp/crush_modified.map.bin")
        bucket_name = kwargs.get("bucket_name")
        reweight_val = float(kwargs.get("reweight_val"))

        # todo: Need to get a way to reliably get the value set vs values dumped from bin file
        value_to_weight_ratio = 0.024 / 1599

        if not self.rados_obj.check_file_exists_on_client(loc=loc):
            log.error(f"file : {loc} not present on the Client")
            return False

        log.debug(
            f"Checking for weight of bucket: {bucket_name} after the reweight operation"
        )

        # Getting the bucket ID of the bucket
        res, bucket_id = self.get_bucket_id_from_bin(loc=loc, bucket_name=bucket_name)
        # Getting the crush map dump
        res, crush_dump = self.dump_bin_contents(loc=loc)
        for entry in crush_dump["buckets"]:
            log.debug(entry)
            for item in entry["items"]:
                log.debug(item)
                if int(item["id"]) == bucket_id:
                    log.debug(
                        f"bucket : {bucket_name} present inside bucket {entry['name']}"
                    )
                    expected_val = reweight_val / value_to_weight_ratio
                    obtained_weight = float(item["weight"])
                    log.debug(
                        f"Weight of the OSD is : {obtained_weight} Vs Calculated val : {expected_val}"
                    )
                    deviation = abs(
                        (expected_val - obtained_weight) / obtained_weight * 100
                    )
                    if deviation > 25:
                        log.error(f"Failed to set weights on bucket : {bucket_name}")
                        return False
                    log.info(
                        f"Verified weights on bucket : {bucket_name}, Set successfully"
                    )
                    return True
        log.error(f"bucket : {bucket_name} with ID : {bucket_id} not found")
        return False

    @count
    def move_existing_bucket_in_bin(self, **kwargs) -> (bool, str):
        """Method to add move an existing crush bucket into another bucket in the crush map

        This method uses crushtool utility to modify the crush map to move buckets around in the crush map
        A new file would be automatically be created with a unique count, which indicates the number
         of times the method has been invoked

        Args::
            kwargs: Various KW arguments that need to be passed are :
                source_loc: Name of the source bin file that should be modified
                source_bucket: Name of the bucket to be moved
                target_bucket_name: Name of the target bucket name where the source should be moved
                target_bucket_type: type of the target crush bucket
                target_loc: Target location along with the filename for the new bin file

        Examples::
            status, file_loc = obj.move_existing_bucket_in_bin(
                source_loc="/tmp/crush.map.bin",
                target_loc="/tmp/crush_modified.map.bin",
                source_bucket="test-host-1",
                target_bucket_name="DC1",
                target_bucket_type="datacenter",
            )
        Returns::
        Tuple containing the execution status and the file path for modified bin
        (True, "/tmp/crush_modified.map.bin")
        """
        source_loc = kwargs.get("source_loc", "/tmp/crush.map.bin")
        target_loc = kwargs.get(
            "target_loc",
            f"/tmp/crush_modified_move_{self.move_existing_bucket_in_bin.calls}.map.bin",
        )
        source_bucket = kwargs.get("source_bucket")
        target_bucket_name = kwargs.get("target_bucket_name")
        target_bucket_type = kwargs.get("target_bucket_type")

        if not self.rados_obj.check_file_exists_on_client(loc=source_loc):
            log.error(f"file : {source_loc} not present on the Client")
            return False, target_loc

        log.debug(
            f"Modification sent to move bucket: {source_bucket} under CRUSH bucket"
            f" : {target_bucket_name} - {target_bucket_type}."
        )
        # Generating bin file for movement of buckets in the crush map
        cmd = (
            f"crushtool -i {source_loc} --move {source_bucket} --loc"
            f" {target_bucket_type} {target_bucket_name} -o {target_loc}"
        )
        try:
            self.client.exec_command(cmd=cmd, sudo=True)
        except Exception as error:
            log.error(
                f"Failed to move bucket: {source_bucket} into {target_bucket_name} - {target_bucket_type}."
                f" error: {error}"
            )
            return False, target_loc

        log.debug(f"Successfully modified the bin file : {target_loc}")
        return self.rados_obj.check_file_exists_on_client(loc=target_loc), target_loc

    def verify_move_bucket_in_bin(self, **kwargs) -> bool:
        """Method to add move an existing crush bucket into another bucket in the crush map

        This method uses crushtool utility to modify the crush map to move buckets around in the crush map
        Args::
            kwargs: Various KW arguments that need to be passed are :
                loc: Name of the source bin file that should be verified for modification
                source_bucket: Name of the bucket to be moved
                target_bucket_name: Name of the target bucket name where the source should be moved

        Examples::
            status = obj.verify_move_bucket_in_bin(
                loc="/tmp/crush_modified.map.bin",
                source_bucket="test-host-1",
                target_bucket="DC1",
            )
        Returns::
        True -> Move successful, Fail -> Move not successful
        """
        loc = kwargs.get("loc", "/tmp/crush_modified.map.bin")
        source_bucket = kwargs.get("source_bucket")
        target_bucket = kwargs.get("target_bucket")

        if not self.rados_obj.check_file_exists_on_client(loc=loc):
            log.error(f"file : {loc} not present on the Client")
            return False

        log.debug(
            f"Modification Verification of bucket: {source_bucket} under CRUSH bucket {target_bucket}"
        )

        out, source_bucket_id = self.get_bucket_id_from_bin(
            loc=loc,
            bucket_name=source_bucket,
        )
        if not out:
            log.error(f"failed to get bucket ID for bucket : {source_bucket}")
            return False

        # Checking if the source bucket ID is present in the destination bucket
        res, crush_dump = self.dump_bin_contents(loc=loc)
        log.debug(f"Crush buckets present in the crush map: {crush_dump['buckets']}")
        for entry in crush_dump["buckets"]:
            if entry["name"] == target_bucket:
                log.debug(f"found entry for the CRUSH bucket : {entry}")
                for item in entry["items"]:
                    if int(item["id"]) == source_bucket_id:
                        log.info(
                            f"bucket : {source_bucket} present inside bucket {target_bucket}"
                        )
                        return True
                log.error(
                    f"bucket : {source_bucket} with ID : {source_bucket_id} not found inside bucket {target_bucket}"
                )
                return False

    def get_bucket_id_from_bin(self, **kwargs) -> (bool, int):
        """Method to fetch the bucket ID from the crush map

        This method uses crushtool utility to fetch the bucket ID from the crush map
        Args::
            kwargs: Various KW arguments that need to be passed are :
                loc: Name of the source bin file that should be tested
                bucket_name: Name of the bucket to be checked

        Examples::
            status, id = obj.get_bucket_id_from_bin(
                loc="/tmp/crush_modified.map.bin",
                bucket_name="DC1",
            )
        Returns::
        Tuple containing the execution status and ID of passed daemon
        (True, "-22")
        """
        loc = kwargs.get("loc", "/tmp/crush_modified.map.bin")
        bucket_name = kwargs.get("bucket_name")

        if not self.rados_obj.check_file_exists_on_client(loc=loc):
            log.error(f"file : {loc} not present on the Client")
            return False, 0

        log.debug(f"Checking for the ID of bucket: {bucket_name}.")

        # Fetching the bucket ID for the source bucket if OSD
        match = re.search(r"osd.(\d)", bucket_name)
        if match:
            source_bucket_id = match.groups()[0]
            return True, int(source_bucket_id)

        # Getting the crush map dump
        res, crush_dump = self.dump_bin_contents(loc=loc)
        log.debug(f"Crush buckets present in the crush map: {crush_dump['buckets']}")
        for entry in crush_dump["buckets"]:
            if entry["name"] == bucket_name:
                log.info(f"ID of Bucket {bucket_name} is {entry['id']}")
                return True, int(entry["id"])
        log.error(f"Failed to get ID bucket for: {bucket_name} in bin file.")
        return False, 0

    def dump_bin_contents(self, loc="/tmp/crush_modified.map.bin") -> (bool, dict):
        """Module to dump the contents the CRUSH bin file in json format

         This method uses crushtool utility to dump the contents of the bn file provided.
         Args::
            loc: Location from where the crush bin file needs to be fetched
        Examples::
            status, cluster_map = obj.dump_bin_contents(loc="/tmp/crush.map.bin")
        Returns::
            Returns a tuple consisting of the execution status and the bin file location
            (True, {})
        """
        if not self.rados_obj.check_file_exists_on_client(loc=loc):
            log.error(f"Bin file : {loc} not present on the Client")
            return False, ""

        # Setting the contents of the bin file in json
        cmd = f"crushtool -i {loc} --dump -f json"
        try:
            out, err = self.client.exec_command(cmd=cmd, sudo=True)
            crush_contents = json.loads(out)
        except Exception as error:
            log.error(f"Failed to get the contents of the bin file. error: {error}")
            return False, ""
        log.debug(
            f"Successfully fetched the contents of the bin file : {crush_contents}"
        )
        return True, crush_contents

    def test_crush_map_bin(
        self, loc="/tmp/crush_modified.map.bin", test="show-statistics"
    ) -> (bool, dict):
        """Method to perform tests on the crush map provided
        Args::
            loc: Name of the source bin file that should be modified
            test: Test to be performed on the crush map.
              Allowed tests:
                1. [ Default ] show-statistics - Displays a summary of the distribution.
                2. show-mappings - Displays the mapping of each value in the range [--min-x,--max-x]
                3. show-bad-mappings - Displays which value failed to be mapped to the required number of devices.
                4. show-utilization - Displays the expected and actual utilization for each device,
                                      for each number of replicas.
                5. show-utilization-all - Displays the same as –show-utilization but does not suppress
                                          output when the weight of a device is zero.
                6. show-choose-tries - Displays how many attempts were needed to find a device mapping.

        Examples::
            test_crush_map_bin(
                loc="/tmp/crush_modified.map.bin"
                test="show-statistics"
            )

        Returns::
        Tuple containing the execution status and the output of the test executed
        (True, "bad mapping rule 1 x 781 num_rep 7 result [8,10,2,11,6,9]")
        """
        if not self.rados_obj.check_file_exists_on_client(loc=loc):
            log.error(f"Bin file : {loc} not present on the Client")
            return False, " "

        # Setting the contents of the bin file in json
        cmd = f"crushtool -i {loc} --test --{test}"
        regex = r"\s*(\d.\d)-rhel-\d"
        build = (
            re.search(regex, self.config.get("build", self.config.get("rhbuild")))
        ).groups()[0]
        if float(build) >= 6.0:
            cmd = cmd + " --num-rep 100"

        try:
            out, err = self.client.exec_command(cmd=cmd, sudo=True)
        except Exception as error:
            log.error(f"Failed to test bin file with test : {test}. error: {error}")
            return False, " "
        log.debug(f"Successfully tested the bin file for {test} and output : {out}")
        return True, out

    def add_crush_rule(self, rule_name: str, rules: str) -> bool:
        """
        Method to add new crush rules into the cluster
        Args:
            rules: Crush rules.
            rule_name: name of the rule
        Usage:
            add_crush_rule(rule_name=test_rule, rules=<string of rules>
        Returns:
            True -> Rules added successfully
            False -> Rules not added
        """
        _, loc = self.generate_crush_map_bin()
        _, loc = self.decompile_crush_map_txt(source_loc=loc)

        # Adding the crush rules into the file
        rule_str = f"rule {rule_name} {{ \n{rules}\n}}"
        cmd = f"echo '{rule_str}' >> {loc}"
        self.client.exec_command(cmd=cmd, sudo=True)

        _, loc = self.compile_crush_map_txt(source_loc=loc)
        if not self.set_crush_map_bin(loc=loc):
            log.error(f"Failed to set rules {rules} on the cluster")
            return False
        log.info(f"Successfully set rules with name {rule_name} on the cluster")
        return True


class OsdToolWorkflows:
    """
    Contains various functions that help to test osdmaptool
    """

    def __init__(self, node: CephAdmin):
        """
        initializes the env to run osdmaptool commands

        Initializes the crush object with the cluster objects,along with installing necessary packages
        required for executing osdmaptool commands
        Args:
            node: CephAdmin object
        """
        self.rados_obj = RadosOrchestrator(node=node)
        self.cluster = node.cluster
        self.config = node.config
        self.client = node.cluster.get_nodes(role="client")[0]

        # Checking and installing ceph-base package on Client
        try:
            out, rc = self.client.exec_command(
                sudo=True, cmd="rpm -qa | grep ceph-base"
            )
        except Exception:
            self.client.exec_command(sudo=True, cmd="yum install -y ceph-base")

    def generate_osdmap(self, loc="/tmp") -> (bool, str):
        """Module to generate the osdmap bin file

         This method makes a getcrushmap call and saves the output the provided location.
         Args::
            loc: Location where the osdmap file needs to be generated
        Examples::
            status, file_loc = obj.generate_osdmap(loc="/tmp")
        Returns::
            Returns a tuple consisting of the execution status and the bin file location
            (True, /tmp/osd_map)
        """
        # Extracting the ceush map from the cluster
        cmd = f"ceph osd  getmap -o {loc}/osd_map"
        self.client.exec_command(cmd=cmd, sudo=True)
        return (
            self.rados_obj.check_file_exists_on_client(loc=f"{loc}/osd_map"),
            f"{loc}/osd_map",
        )

    def apply_osdmap_upmap(self, **kwargs) -> (bool, str):
        """Module to generate the upmap recommendations from the osdmaptool

         This method makes use of osdmaptool to get the PG - OSD placement recommendations
         Args::
            kwargs:
                source_loc: Location from where the bin file should be sourced
                target_loc: Location where the upmap file needs to be generated
        Examples::
            status, file_loc = obj.apply_osdmap_upmap(
                source_loc="/tmp/osd_map",
                target_loc="/tmp/upmap_res.txt",
            )
        Returns::
            Returns a tuple consisting of the execution status and the bin file location
            (True, /tmp/upmap_res.txt)
        """
        source_loc = kwargs.get("source_loc", "/tmp/osd_map")
        target_loc = kwargs.get("target_loc", "/tmp")
        if not self.rados_obj.check_file_exists_on_client(loc=source_loc):
            log.error(f"file : {source_loc} not present on the Client")
            return False, ""

        # osdmaptool /tmp/osd_map --upmap /tmp/out.txt
        # Generating text file for the bin file generated
        cmd = f"osdmaptool {source_loc} --upmap {target_loc}/upmap_res.txt"
        out, err = self.client.exec_command(cmd=cmd, sudo=True)
        log.debug(f"output of command : {cmd} on cluster : \n\n {out}\n\n")
        return (
            self.rados_obj.check_file_exists_on_client(
                loc=f"{target_loc}/upmap_res.txt"
            ),
            f"{target_loc}/upmap_res.txt",
        )

    def apply_osdmap_read(self, pool_name, **kwargs) -> (bool, str):
        """Module to generate the read recommendations from the osdmaptool

         This method makes use of osdmaptool to get the PG - OSD placement recommendations
         Args::
            kwargs:
                pool_name: name of the pool on which the read balancing needs to be applied
                source_loc: Location from where the bin file should be sourced
                target_loc: Location where the upmap file needs to be generated
        Examples::
            status, file_loc = obj.apply_osdmap_read(
                pool_name= "test-pool"
                source_loc="/tmp/osd_map",
                target_loc="/tmp/test-pool_res.txt",
            )
        Returns::
            Returns a tuple consisting of the execution status and the bin file location
            (True, /tmp/upmap_res.txt)
        """
        source_loc = kwargs.get("source_loc", "/tmp/osd_map")
        target_loc = kwargs.get("target_loc", "/tmp")
        if not self.rados_obj.check_file_exists_on_client(loc=source_loc):
            log.error(f"file : {source_loc} not present on the Client")
            return False, ""

        # osdmaptool /tmp/osd_map --read /tmp/test1_read_op --read-pool test1
        cmd = f"osdmaptool {source_loc} --read {target_loc}/{pool_name}_res.txt --read-pool {pool_name}"
        out, err = self.client.exec_command(cmd=cmd, sudo=True)
        log.debug(f"output of command : {cmd} on cluster : \n\n {out}\n\n")
        return (
            self.rados_obj.check_file_exists_on_client(
                loc=f"{target_loc}/{pool_name}_res.txt"
            ),
            f"{target_loc}/{pool_name}_res.txt",
        )
