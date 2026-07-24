from cli import Cli


class Crush(Cli):
    """This module provides CLI interface to manage the Crush service."""

    def __init__(self, nodes, base_cmd):
        super(Crush, self).__init__(nodes)

        self.base_cmd = f"{base_cmd} crush"

    def rule(self, *Kargs):
        """
        To create rules
        Kargs:
            Supported args
            rule_type (str): create-simple | create-replicated |create-erasure
            rule_name (str): name of the rule
            root (str): root of the CRUSH hierarchy
            failure_domain_type (str): failure domain (host/rack)
            device_class (str): storage device class (hdd/sdd)
            replicated (bool): if the rule is replicated or not
        """
        cmd = f"{self.base_cmd} rule"
        for arg in Kargs:
            cmd += f" {arg}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def set_device_class(self, device_class, osd_id):
        """
        To set device class to osd
        Args:
            device_class (str): device class (hdd/ssd)
            osd_id (list): list of osd's
        """
        cmd = f"{self.base_cmd} set-device-class {device_class}"
        for _osd in osd_id:
            cmd += f" {_osd}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def rm_device_class(self, device_class, osd_id):
        """
        To remove device class to osd
        Args:
            device_class (str): device class (hdd/ssd)
            osd_id (list): list of osd's
        """
        cmd = f"{self.base_cmd} rm-device-class {device_class}"
        for _osd in osd_id:
            cmd += f" {_osd}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def rename_device_class(self, old_name, new_name):
        """
        To rename device class
        Args:
            old_name (str): old class name
            new_name (str): new class name
        """
        cmd = f"{self.base_cmd} class rename {old_name} {new_name}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def ls_osd(self, device_class):
        """
        To list all OSDs that belong to a particular class
        Args:
            device_class (str): device class (hdd/ssd)
        """
        cmd = f"{self.base_cmd} class ls-osd {device_class}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def add_bucket(self, name, type):
        """
        To add a bucket instance to CRUSH hierarchy
        Args:
            name (str): bucket name
            type (str): type of bucket
        """
        cmd = f"{self.base_cmd} add-bucket {name} {type}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def move(self, name, type):
        """
        To move a bucket instance to a particular location in CRUSH hierarchy
        Args:
            name (str): bucket name
            type (str): type of bucket
        """
        cmd = f"{self.base_cmd} move {name} {type}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def add(self, osd, weight, bucket_details):
        """
        To add an OSD to a CRUSH hierarchy
        Args:
            osd (str): osd id or name
            weight (str): weight to be assigned
            bucket_details (list): details of format {bucket-type}={bucket-name}
        """
        cmd = f"{self.base_cmd} add {osd} {weight} "
        cmd += " ".join(bucket_details)
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def remove(self, item):
        """
        To remove an OSD from the CRUSH map of a running cluster
        Args:
            item (str): osd id or bucket name to be removed
        """
        cmd = f"{self.base_cmd} remove {item}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def set(self, key, value):
        """
        Set value to give key
        Args:
            key (str): Key to be updated
            value (str): Value to be set to the key
        """
        cmd = f"{self.base_cmd} set {key} {value}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
