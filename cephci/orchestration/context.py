"""Identity, logging and timing of a single execution."""

import datetime
import os
import sys
from getpass import getuser
from typing import Dict, Optional

from cephci.orchestration.config import RunConfig
from utility.log import Log
from utility.utils import create_run_dir, generate_unique_id, magna_url

log = Log(__name__)

# Runs whose logs land here are published, so the log links are rewritten.
PUBLISHED_LOG_ROOT = "/ceph/cephci-jenkins"


class RunContext:
    """Everything that identifies an execution rather than what it tests."""

    def __init__(self, run_id: str, run_dir: str) -> None:
        """Initialize the context.

        Args:
            run_id (str): Unique identifier of the execution.
            run_dir (str): Directory holding the logs of the execution.
        """
        self.run_id = run_id
        self.run_dir = run_dir
        self.start_time = datetime.datetime.now()
        self.cli_arguments = f"{sys.executable} {' '.join(sys.argv)}"
        self.jenkins_url = os.environ.get("BUILD_URL")
        self.trigger_user = getuser()

        # Written to run_summary.json at the end of the run. Nothing populates it
        # today; the file is kept because the pipelines collect it.
        self.summary: Dict = {}

    @classmethod
    def create(cls, config: RunConfig) -> "RunContext":
        """Create the run directory and start logging into it.

        Args:
            config (RunConfig): The run settings.

        Returns:
            RunContext: The context of this execution.
        """
        run_id = generate_unique_id(length=6)
        run_dir = create_run_dir(run_id, config.log_directory)

        log.configure_logger("startup", run_dir, config.disable_console_log)
        if config.log_level:
            log.logger.setLevel(config.log_level.upper())

        return cls(run_id, run_dir)

    @property
    def url_base(self) -> str:
        """Location the logs of this run can be read from."""
        if PUBLISHED_LOG_ROOT in self.run_dir:
            return magna_url + self.run_dir.split("/")[-1]

        return self.run_dir

    def elapsed(self) -> Dict:
        """Describe how long the run has taken so far.

        Returns:
            dict: Start, end and total duration, formatted for the report.
        """
        end_time = datetime.datetime.now()
        duration = divmod((end_time - self.start_time).total_seconds(), 60)

        return {
            "start": self.start_time.strftime("%d %B %Y , %I:%M:%S %p"),
            "end": end_time.strftime("%d %B %Y , %I:%M:%S %p"),
            "total": f"{int(duration[0])} mins, {int(duration[1])} secs",
        }

    def cluster_state_file(self, instances_name: Optional[str]) -> str:
        """Return the path the environment is pickled to, creating its directory.

        Args:
            instances_name (str): Name prefix of the environment.

        Returns:
            str: Path of the pickle.
        """
        path = f"rerun/{instances_name}-{self.run_id}"
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        return path
