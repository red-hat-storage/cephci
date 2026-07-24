from cli import Cli
from cli.utilities.utils import build_cmd_from_args
from utility.log import Log

log = Log(__name__)


class Osd(Cli):
    def __init__(self, nodes, base_cmd):
        super(Osd, self).__init__(nodes)
        self.base_cmd = f"{base_cmd} osd"

    def rm(self, osd_id=None, status=False, **kw):
        """
        List hosts
        Args:
            osd_id (str): ID of the osd to remove
            status (bool): Whether to check for the rm status or not
            format (bool): Format in which status has to be fetched
            kw (dict): Key/value pairs that needs to be provided to the installer

            Supported Keys:
                force (bool): Whether to append force with rm command
                --zap (bool) : Whether to append zap with rm command
        """
        cmd = [self.base_cmd, "rm"]
        if status:
            cmd.append("status")
        elif osd_id:
            cmd.append(osd_id)
        cmd.append(build_cmd_from_args(**kw))
        out = self.execute(sudo=True, cmd=" ".join(cmd))
        if isinstance(out, tuple):
            return out[0].strip()
        return out
