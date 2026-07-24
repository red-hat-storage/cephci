from cli import Cli
from utility.log import Log

log = Log(__name__)


class TunedProfile(Cli):
    def __init__(self, nodes, base_cmd):
        super(TunedProfile, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} tuned-profile"

    def apply(self, spec_file, check_ec=False):
        """
        Apply tuned profile.
        Args:
            spec_file: tune spec file details
        """
        cmd = f"{self.base_cmd} apply -i {spec_file}"
        out = self.execute(sudo=True, cmd=cmd, check_ec=check_ec)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def list(self):
        """
        List tuned profile.
        """
        cmd = f"{self.base_cmd} ls"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def remove(self, profile_name):
        """
        Remove tuned profile.
        Args:
            profile_name : tune profile name
        """
        cmd = f"{self.base_cmd} rm {profile_name}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def modify(self, profile_name, setting, value):
        """
        Modify tuned profile.
        Args:
            profile_name : tune profile name
            setting : tune setting
            value : tune setting value
        """
        cmd = f"{self.base_cmd} add-setting {profile_name} {setting} {value}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
