from random import choice
from string import ascii_lowercase, digits

from cli import Cli
from cli.exceptions import IOError

SMALL_FILE_COMMAND = "python3 smallfile/smallfile_cli.py "


class SmallFile(Cli):
    def __init__(self, client):
        super(SmallFile, self).__init__(client)
        self.client = client

    def pull_smallfile_repo(self):
        """
        Git pulls Smallfile repo
        """
        try:
            # Install Fio packages
            cmd = (
                "git clone https://github.com/distributed-system-analysis/smallfile.git"
            )
            self.execute(sudo=True, long_running=True, cmd=cmd)
        except Exception:
            raise IOError("Failed to pull SmallFile")

    def run(
        self,
        mount_dir,
        operations,
        threads=10,
        file_size=4,
        files=1000,
        files_per_dir=10,
        dirs_per_dir=2,
        record_size=128,
    ):
        """
        Runs small file IO
        Args:
            mount_dir (str): Mounting directory
            operations (list): Operation to be performed
            threads (str): No of threads
            file_size (str): File size
            files (str): No of files
            files_per_dir (str):  No files per directory created
            dirs_per_dir (str): No of dirs per directory created
            record_size (str): Record size
        """
        if not isinstance(operations, list):
            operations = [operations]

        # Install smallfile
        self.pull_smallfile_repo()

        dir = "".join(choice(ascii_lowercase + digits) for _ in range(10))

        # Create tmp folder
        path = "/var/tmp/smf"
        self.client.create_dirs(dir_path=path, sudo=True)

        # Create dir inside mount dir for IO
        path = f"{mount_dir}/{dir}"
        self.client.create_dirs(dir_path=path, sudo=True)

        # Run smallfile
        for operation in operations:
            cmd = (
                f"{SMALL_FILE_COMMAND} --operation {operation} --threads {threads} "
                f"--file-size {file_size} --files {files} --files-per-dir {files_per_dir} "
                f"--dirs-per-dir {dirs_per_dir} --record-size {record_size} --top {mount_dir}/{dir} "
                f"--output-json={operation}_{dir}.json"
            )
            self.execute(sudo=True, cmd=cmd)
        return True
