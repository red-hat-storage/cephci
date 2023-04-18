from cli import Cli


class Balancer(Cli):
    """This module provides CLI interface to manage the balancer module."""

    def __init__(self, nodes, base_cmd):
        super(Balancer, self).__init__(nodes)

        self.base_cmd = f"{base_cmd} balancer"

    def status(self):
        """Check status of balancer module."""
        cmd = f"{self.base_cmd} status"
        out = self.execute(sudo=True, check_ec=False, long_running=False, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def set_state(self, state):
        """
        Sets balancer state to ON/OFF
        Args:
            state (str): state of balancer on/off
        """
        cmd = f"{self.base_cmd} {state}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def mode(self, mode):
        """
        Sets the balancer mode
        Args:
            mode (str): mode to be set (crush-compat / upmap)
        """
        cmd = f"{self.base_cmd} mode {mode}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def eval(self, plan=None, pool_name=None, verbose=False):
        """
        Performs evaluation and score the current distribution
        Args:
            plan (str): Plan to be evaluated
            pool_name (str): Evaluate the distribution for a single pool
            verbose (bool): Set True to see greater detail for the evaluation
        """
        cmd = f"{self.base_cmd} eval"
        if verbose:
            cmd += "-verbose"
        if pool_name:
            cmd += f" {pool_name}"
        if plan:
            cmd += f" {plan}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def execute_plan(self, plan):
        """
        Execute a given plan
        Args:
            plan (str): Name of the Plan
        """
        cmd = f"{self.base_cmd} execute {plan}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def rm(self, plan):
        """
        Removes a given plan
        Args:
            plan (str): Name of the Plan
        """
        cmd = f"{self.base_cmd} rm {plan}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out

    def optimize(self, plan):
        """
        Generate a plan using the currently configured mode
        Args:
            plan (str): Name of the Plan
        """
        cmd = f"{self.base_cmd} optimize {plan}"
        out = self.execute(sudo=True, cmd=cmd)
        if isinstance(out, tuple):
            return out[0].strip()
        return out
