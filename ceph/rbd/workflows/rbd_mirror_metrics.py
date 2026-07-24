import json
import re

from utility.log import Log

log = Log(__name__)


def create_symlink_and_get_metrics(mirror_node):
    """Create symbolic link to the rbd-mirror.asok file and return metrics.
    Args:
        mirror_node: rbd-mirror daemon node.

    returns:
        metrics: rbd mirror related metrics.
    """
    # Set the ceph directory path
    ceph_dir_path = "/var/run/ceph"

    try:
        # install ceph-common required packages
        mirror_node.exec_command(
            sudo=True,
            cmd="yum install -y ceph-common --nogpgcheck",
            check_ec=False,
        )

        # List the contents of the ceph directory
        directory_contents = mirror_node.exec_command(
            cmd=f"ls {ceph_dir_path}", sudo=True
        )

        # Check if there are any directories in the current directory
        if directory_contents:
            target_directory = directory_contents[0].strip()

            # List the contents of the target directory
            directory_contents = mirror_node.exec_command(
                cmd=f"ls {ceph_dir_path}/{target_directory}/", sudo=True
            )

            # Find the file with the specific pattern
            file_pattern = "ceph-client.rbd-mirror.*"
            target_file = None
            for file in directory_contents[0].splitlines():
                if re.match(file_pattern, file):
                    target_file = file
                    break

            # Check if the target file exists then create symbolic link to it.
            if target_file:
                file_path = f"{ceph_dir_path}/{target_directory}"
                mirror_node.exec_command(
                    cmd=f"ln -s {file_path}/{target_file} {file_path}/rbd-mirror.asok",
                    sudo=True,
                )

                # List the contents of the current directory
                directory_contents = mirror_node.exec_command(
                    cmd=f"ls {ceph_dir_path}/{target_directory}/", sudo=True
                )

                # Check if the symlink file named 'rbd-mirror.asok' is in the directory_contents
                if "rbd-mirror.asok" in directory_contents[0].splitlines():
                    log.info("Symlink file named rbd-mirror.asok created successfully")
                else:
                    log.error(
                        "Symlink file named rbd-mirror.asok not created successfully"
                    )
                    return 1

        # Validation of mirror related metrics
        command = f"ceph --admin-daemon {ceph_dir_path}/{target_directory}/rbd-mirror.asok counter dump"

        out = mirror_node.exec_command(cmd=command, output=True, sudo=True)
        metrics = json.loads(out[0])
        return metrics

    except Exception as e:
        log.error(f"Error creating symbolic link and fetching metrics: {e}")
        return 1

    finally:
        # Remove the generated symbolic link file
        log.info("Removing generated symbolic link file")
        mirror_node.exec_command(
            cmd=f"rm -f {ceph_dir_path}/{target_directory}/rbd-mirror.asok", sudo=True
        )
