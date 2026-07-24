from functools import partialmethod


class Cli:
    def __init__(self, ctx):
        self.ctx = ctx

    def execute(self, cmd, sudo=False, long_running=False, check_ec=False, **kwargs):
        """Inerface to execute commands on node(s).

        Args:
            cmd (str): Command to be execute
            sudo (bool): Use root access
            long_running (bool): Long running command
            check_exit_status (bool): Check command exit status
        """
        if isinstance(self.ctx, list):
            out = {}
            for ctx in self.ctx:
                out[ctx.shortname] = ctx.exec_command(
                    cmd=cmd,
                    sudo=sudo,
                    long_running=long_running,
                    check_ec=check_ec,
                    timeout=kwargs.get("timeout", 3600),
                )
            return out
        else:
            return self.ctx.exec_command(
                cmd=cmd,
                sudo=sudo,
                long_running=long_running,
                check_ec=check_ec,
                timeout=kwargs.get("timeout", 3600),
            )

    execute_as_sudo = partialmethod(execute, sudo=True)
