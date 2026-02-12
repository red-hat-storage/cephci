"""Module for handling registry authentication in Ceph clusters."""

import json

from utility.log import Log

logger = Log(__name__)

PODMAN_AUTH_PATH = "/etc/ceph/podman-auth.json"


def validate_podman_auth_file(auth_file_path: str) -> str:
    """
    Validate and read a podman auth.json file.

    This function validates that the file exists, contains valid JSON,
    and has the expected 'auths' key structure.

    Args:
        auth_file_path (str): Path to the local podman-auth.json file

    Returns:
        str: The content of the auth file

    Raises:
        FileNotFoundError: If the auth file doesn't exist
        json.JSONDecodeError: If the file contains invalid JSON
        Exception: For other errors reading the file

    Example:
        auth_content = validate_podman_auth_file("/path/to/podman-auth.json")
    """
    try:
        with open(auth_file_path, "r") as f:
            auth_content = f.read()
            # Validate it's valid JSON
            auth_dict = json.loads(auth_content)

            if "auths" not in auth_dict:
                logger.warning(
                    f"Warning: {auth_file_path} doesn't contain 'auths' key. "
                    "This may not be a valid podman auth file."
                )
        return auth_content
    except FileNotFoundError:
        logger.error(f"Podman auth file not found: {auth_file_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in podman auth file: {e}")
        raise
    except Exception as e:
        logger.error(f"Error reading podman auth file: {e}")
        raise


def distribute_podman_auth_file(node, auth_file_path: str):
    """
    Distribute a pre-validated podman auth.json file to a single node.

    This function takes a podman auth.json file (which can contain multiple registry
    credentials) and distributes it to the specified node at /etc/ceph/podman-auth.json.
    The file should be validated before calling this function using validate_podman_auth_file().

    Args:
        node: Node object to distribute the file to
        auth_file_path (str): Path to the local podman-auth.json file (already validated)

    Example:
        # Validate once before parallel execution
        auth_content = validate_podman_auth_file("/path/to/podman-auth.json")
        # Then distribute to each node
        distribute_podman_auth_file(node, "/path/to/podman-auth.json")

    The podman-auth.json file should be in standard podman format:
        {
            "auths": {
                "registry.redhat.io": {
                    "auth": "base64_encoded_credentials"
                },
                "registry.stage.redhat.io": {
                    "auth": "base64_encoded_credentials"
                }
            }
        }
    """
    try:
        # Read the file content (already validated, so no need to re-validate)
        with open(auth_file_path, "r") as f:
            auth_content = f.read()

        # Ensure /etc/ceph directory exists
        node.exec_command(sudo=True, cmd="mkdir -p /etc/ceph")

        # Write auth file
        auth_file = node.remote_file(
            sudo=True, file_name=PODMAN_AUTH_PATH, file_mode="w"
        )
        auth_file.write(auth_content)
        auth_file.flush()

        # Set proper permissions
        node.exec_command(sudo=True, cmd=f"chmod 600 {PODMAN_AUTH_PATH}")

        logger.info(
            f"Successfully distributed podman auth file to {node.shortname} at {PODMAN_AUTH_PATH}"
        )
    except Exception as e:
        logger.error(f"Failed to distribute podman auth to {node.shortname}: {str(e)}")
        raise
