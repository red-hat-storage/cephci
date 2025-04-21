import base64
import json
import os
import random
import re
import string
import unicodedata

from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.exceptions import CharMapValidationError, UnsupportedInput
from utility.log import Log

log = Log(__name__)


class CephFSAttributeUtilities(object):
    def __init__(self, ceph_cluster):
        self.ceph_cluster = ceph_cluster
        self.fs_util = FsUtils(ceph_cluster)

        self.mons = self.fs_util.mons
        self.mgrs = self.fs_util.mgrs
        self.osds = self.fs_util.osds
        self.mdss = self.fs_util.mdss
        self.clients = self.fs_util.clients
        self.installer = self.fs_util.installer

    def set_attributes(self, client, directory, **kwargs):
        """
        Set attributes for case sensitivity, normalization, and encoding.
        Usage: set_attributes("/mnt/dir", casesensitive=True, normalization="nfd", encoding="utf8")

        Args:
            directory (str): The path to the directory.
            **kwargs: Key-value pairs of attributes to set. Supported attributes:
                - casesensitive (bool): Enable or disable case sensitivity.
                - normalization (str): Set normalization (e.g., "nfd", "nfc").
                - encoding (str): Set encoding format (e.g., "utf8").

        Raises:
            ValueError: If an unsupported attribute is passed.

        """
        supported_attributes = ["casesensitive", "normalization", "encoding"]
        for key, value in kwargs.items():
            if key not in supported_attributes:
                raise UnsupportedInput(
                    "Unsupported attribute: {}. Supported attributes are {}".format(
                        key, supported_attributes
                    )
                )
            cmd = "setfattr -n ceph.dir.{} -v {} '{}'".format(key, value, directory)
            log.debug(cmd)
            client.exec_command(sudo=True, cmd=cmd)
            log.info("Set {} to {} on '{}'".format(key, value, directory))

    def get_charmap(self, client, directory):
        """Retrieve charmap attributes and return them as a dictionary.
        Args:
            directory (str): The path to the directory.

        Returns:
            dict: A dictionary containing the charmap attributes.
                Example:
                {
                    "casesensitive": False,
                    "normalization": "nfd",
                    "encoding": "utf8"
                }

        Example:
            >>> get_charmap("/mnt/mani-fs-2-client2/Dir1/")
            {
                "casesensitive": False,
                "normalization": "nfd",
                "encoding": "utf8"
            }
        """
        cmd = "getfattr -n ceph.dir.charmap '{}' --only-values".format(directory)
        out, _ = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
        try:
            # Extract JSON using regex to handle unexpected shell prompt artifacts
            json_match = re.search(r"(\{.*\})", out.strip())
            if json_match:
                charmap_str = json_match.group(1)
                log.info("Charmap output for cmd {}: {}".format(cmd, charmap_str))
                return json.loads(charmap_str)
            else:
                log.info("Failed to extract JSON from charmap output: {}".format(out))
                raise CharMapValidationError("Invalid charmap output")
        except Exception:
            log.error("Failed to parse charmap for '{}'".format(directory))
            return {}

    def validate_charmap(self, client, dir, expected_values):
        """
        Validates that specific attributes in a charmap match expected values.

        Parameters:
            client (Any): The client object used to retrieve the charmap.
            dir (str): The directory path to validate.
            expected_values (dict): A dictionary of expected key-value pairs to check
                                    in the charmap (e.g., {"encoding": "utf8"}).

        Raises:
            ValueError: If any expected value does not match the actual value in the charmap.

        Returns:
            bool: True if all expected values match.
        """
        charmap = self.get_charmap(client, dir)
        for key, expected in expected_values.items():
            actual = charmap.get(key)
            if actual != expected:
                raise CharMapValidationError(
                    "{}: {} must be {}, got {}".format(
                        dir, key, repr(expected), repr(actual)
                    )
                )
        return True

    def compare_charmaps(self, client, charmap1, charmap2):
        """Compare two charmap dictionaries and return if they match.
        Args:
            charmap1 (dict): The first charmap dictionary.
            charmap2 (dict): The second charmap dictionary.

        Returns:
            bool: True if both charmaps are identical, False otherwise.

        Example:
            >>> charmap1 = {"casesensitive": False, "normalization": "nfd", "encoding": "utf8"}
            >>> charmap2 = {"casesensitive": False, "normalization": "nfd", "encoding": "utf8"}
            >>> compare_charmaps(charmap1, charmap2)
            True

            >>> charmap3 = {"casesensitive": True, "normalization": "nfc", "encoding": "utf8"}
            >>> compare_charmaps(charmap1, charmap3)
            False
        """
        return charmap1 == charmap2

    def update_required_client_features(self, client, fs_name):
        """Update the required client feature attribute for a Ceph filesystem.

        Args:
            fs_name (str): The name of the Ceph filesystem to update.

        Returns:
            None
        """
        cmd = "ceph fs required_client_features {} add charmap".format(fs_name)
        client.exec_command(sudo=True, cmd=cmd)
        log.info("Updated client feature for {}".format(fs_name))

    def create_directory(self, client, path, force=False):
        """Create a directory, including parent directories if needed.

        Args:
            path (str): The full path of the directory to create.

        Example:
            create_directory("/mnt/cephfs/subdir1/subdir2")
            # This will execute: mkdir -p /mnt/cephfs/subdir1/subdir2
        """
        cmd = "mkdir {} '{}'".format("-p" if force else "", path)
        client.exec_command(sudo=True, cmd=cmd)
        log.info("Created directory: {}".format(path))

    def create_special_character_directories(self, client, base_path, count=1):
        """Create directories with a mix of special characters.
        Args:
            base_path (str): The parent directory where the new directories will be created.
            count (int, optional): The number of directories to create. Defaults to 1.

        Returns:
            Full path name
        """
        special_chars = "@#$%^&*()-_=+[]{|};:,<>?"
        for _ in range(count):
            dir_name = "".join(
                random.choices(
                    string.ascii_letters + string.digits + special_chars,
                    k=random.randint(1, 255),
                )
            )
            full_path = os.path.join(base_path, dir_name)
            self.create_directory(client, full_path)
            log.info("Created special character directory: '{}'".format(full_path))
            return full_path

    def delete_directory(self, client, dir_path, recursive=False):
        """
        Delete a directory at the specified path.

        Args:
            dir_path (str): The full path of the directory to delete.
            recursive (bool): If True, delete directory and contents recursively. Defaults to False.

        Returns:
            bool: True if deletion was successful, False otherwise.
        """
        cmd = "rm {} '{}'".format("-rf" if recursive else "-f", dir_path)
        client.exec_command(sudo=True, cmd=cmd)
        log.info("Deleted directory: '{}' (recursive={})".format(dir_path, recursive))
        return True

    def rename_directory(self, client, old_dir, new_dir):
        """
        Renames a directory on a remote client.

        Args:
            client (object): The remote client executing the command.
            old_dir (str): The current directory path.
            new_dir (str): The new directory path.

        Returns:
            bool: True if the directory was successfully renamed, False otherwise.
        """
        cmd = "mv {} {}".format(old_dir, new_dir)
        try:
            client.exec_command(sudo=True, cmd=cmd)
            return True
        except Exception as e:
            log.error("Failed to rename directory: {}".format(e))
            return False

    def create_file(self, client, file_path, content=None):
        """
        Create a file in the specified directory with optional content.

        Args:
            file_path (str): The path of the file to create.
            content (str, optional): Content to write to the file. Defaults to None.

        Returns:
            str: The full path of the created file.
        """
        cmd = "touch '{}'".format(file_path)
        client.exec_command(sudo=True, cmd=cmd)
        log.info("Created file: {}".format(file_path))

        if content:
            cmd = "echo '{}' > '{}'".format(content, file_path)
            client.exec_command(sudo=True, cmd=cmd)
            log.info("Wrote content to file: {}".format(file_path))

        return file_path

    def check_if_file_exists(self, client, file_path):
        """Check if a file exists at the specified path.
        Args:
            client (object): The remote client executing the command.
            file_path (str): The path of the file to check.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        cmd = "ls {}".format(file_path)
        try:
            client.exec_command(sudo=True, cmd=cmd)
            log.info("File exists: {}".format(file_path))
            return True
        except Exception as e:
            log.error("File does not exist: {}".format(e))
            return False

    def delete_file(self, client, file_path):
        """
        Delete a file at the specified path.

        Args:
            file_path (str): The full path of the file to delete.

        Returns:
            bool: True if deletion was successful, False otherwise.
        """
        cmd = "rm -f '{}'".format(file_path)
        client.exec_command(sudo=True, cmd=cmd)
        log.info("Deleted file: {}".format(file_path))
        return True

    def random_shuffle_case(self, string):
        """Returns a string with randomized casing.
        Args:
            string (str): The input string to shuffle.

        Returns:
            str: The input string with randomized casing.
        """
        return "".join(random.choice([c.upper(), c.lower()]) for c in string)

    def check_ls_case_sensitivity(self, client, dir_path):
        """
        Check if 'ls' can retrieve a directory with different case variations.

        Args:
            dir_path (str): The full path where the directory is located.

        Returns:
            bool: True if 'ls' successfully lists the directory (case-insensitive),
                False if it fails (case-sensitive).
        """
        base_path, dir_name = (
            "/".join(dir_path.split("/")[:-1]),
            dir_path.split("/")[-1],
        )
        log.debug("Base Path: {}".format(base_path))
        log.debug("Directory Name: {}".format(dir_name))

        test_variations = [
            dir_name.upper(),
            dir_name.lower(),
            self.random_shuffle_case(dir_name),
        ]

        for variant in test_variations:
            log.debug("Testing case insensitivity name: {}".format(variant))
            test_path = os.path.join(base_path, variant)
            cmd = "ls '{}'".format(test_path)
            try:
                out, rc = client.exec_command(sudo=True, cmd=cmd)
                log.debug(rc)
                log.debug("ls output of {}/{}: {}".format(base_path, dir_name, out))
            except Exception as e:
                log.error(
                    "Directory '{}' is inaccessible. Failed: {}".format(test_path, e)
                )
                return False

        log.info("Directory '{}' is accessible with different cases.".format(dir_name))
        return True

    def create_links(self, client, target, link_name, link_type="soft"):
        """Create soft or hard links.
        Args:
            target (str): The file or directory to link to.
            link_name (str): The name of the link to be created.
            link_type (str, optional): Type of link to create.
                                       Use "soft" for a symbolic link (default)
                                       or "hard" for a hard link.
        """
        cmd = f'ln {"-s" if link_type == "soft" else ""} {target} {link_name}'
        client.exec_command(sudo=True, cmd=cmd)
        log.info("Created {} link: {} -> {}".format(link_type, link_name, target))

    def delete_links(self, client, link_name):
        """Delete a link.
        Args:
            link_name (str): The name of the link to delete.

        Returns:
            bool: True if deletion was successful, False otherwise.
        """
        unlink_cmd = "unlink {}".format(link_name)
        rm_cmd = "rm -f {}".format(link_name)
        try:
            log.info("Unlinking the link: {}".format(link_name))
            client.exec_command(sudo=True, cmd=unlink_cmd)
            log.info("Removing the link: {}".format(link_name))
            client.exec_command(sudo=True, cmd=rm_cmd)
            return True
        except Exception as e:
            log.error("Failed to delete link: {}".format(e))
            return False

    def remove_attributes(self, client, directory, *attributes):
        """
        Remove specified attributes from a directory.

        Args:
            client: The client object to execute commands.
            directory (str): The path to the directory.
            *attributes: Attributes to remove. Supported attributes:
                - "casesensitive"
                - "normalization"
                - "encoding"
                - "charmap"

        Raises:
            ValueError: If an unsupported attribute is passed.
        """
        supported_attributes = ["casesensitive", "normalization", "encoding", "charmap"]
        for attribute in attributes:
            if attribute not in supported_attributes:
                raise UnsupportedInput(
                    "Unsupported attribute: {}. Supported attributes are {}".format(
                        attribute, supported_attributes
                    )
                )

            cmd = "setfattr -x ceph.dir.{} '{}'".format(attribute, directory)
            log.debug(cmd)
            client.exec_command(sudo=True, cmd=cmd)
            log.info("Removed {} from {}".format(attribute, directory))

    def generate_random_unicode_names(self, count=1):
        """
        Generate random directory names that change under Unicode normalization.

        Args:
            count (int): Number of names to generate. Default: 1

        Returns:
            list: A list of generated directory names.
        """
        characters = [
            "Ä",
            "é",
            "ü",
            "ô",
            "ñ",  # Accented characters
            "ﬁ",
            "ﬂ",
            "Æ",
            "œ",  # Ligatures
            "Ａ",
            "Ｂ",
            "Ｃ",  # Full-width letters
            "¹",
            "²",
            "³",  # Superscripts
            "ß",
            "Ω",
            "µ",  # Special cases
        ]

        names = []
        for _ in range(count):
            name_length = random.randint(3, 8)  # Random length between 3 and 8
            name = "".join(random.choices(characters, k=name_length))
            names.append(name)

        return names

    def validate_normalization(
        self,
        client,
        fs_name,
        base_path,
        dir_name_wo_norm,
        norm_type,
        casesensitive=True,
    ):
        """check if dir names are normalized by the filesystem.

        Args:
            base_path (str): Directory where test directories are created.
            dir_name_wo_norm (str): Directory name without normalization.
            norm_type (str): Expected normalization type (NFC, NFD, NFKC, NFKD)
            casesensitive (bool, optional): If False, performs case-insensitive validation. Defaults to True.

        Returns:
            bool: True if the filesystem-normalized name matches the expected normalization.
        """

        log.info(
            "Fetching the active MDS node for file system {fs_name}".format(
                fs_name=fs_name
            )
        )
        active_mds = self.fs_util.get_active_mdss(client, fs_name)
        log.info(active_mds)

        # Get the normalized dir name
        dir_name = self.normalize_unicode(dir_name_wo_norm, norm_type)
        log.info(
            "Directory Name after normalization: {} : {}".format(norm_type, dir_name)
        )

        if not casesensitive:
            dir_name = dir_name.casefold()

        log.info(
            "Directory Name after lowercase + normalization: {} : {}".format(
                norm_type, dir_name
            )
        )

        cmd = (
            "ceph tell mds.{active_mds} dump tree '{base_path}' 0 2>/dev/null "
            "| jq -r '.[0].dirfrags[0].dentries | "
            'map(select(.path == "{base_path}/{dir_name}") | .path) | .[]\' '
            "| awk -F'/' '{{print $NF}}' "
            "| lua -e 'for line in io.lines() do "
            "for p, c in utf8.codes(line) do "
            'io.write(("\\\\u%04x"):format(c)) end io.write("\\n") end\''
        ).format(active_mds=active_mds[0], base_path=base_path, dir_name=dir_name)

        out, _ = client.exec_command(sudo=True, cmd=cmd)

        if out == "":
            cmd = ("ceph tell mds.{active_mds} dump tree '{base_path}' 0").format(
                active_mds=active_mds[0], base_path=base_path
            )
            debug_out, _ = client.exec_command(sudo=True, cmd=cmd)
            log.debug("MDS dump: {}".format(debug_out))

        log.info("Unicode Points from MDS: {}".format(out))

        # Check if it's normalized
        is_normal = unicodedata.is_normalized(norm_type, dir_name)
        log.info("Normalization Check: {}".format(is_normal))

        # Print Unicode code points
        code_points = "".join("\\u{:04x}".format(ord(char)) for char in dir_name)
        log.info("Unicode Points: {}".format(code_points))

        if not out.strip() == code_points.strip():
            log.error(
                "Normalization unicode points mismatch. Expected: {}, Actual: {}".format(
                    code_points, out
                )
            )
            return False

        log.info("Normalization Validated and it's matching")
        return True

    def fetch_alternate_name(self, client, fs_name, dir_path):
        """
        Fetches a dictionary of file paths and their corresponding alternate names for a specified Ceph file system.

        This function identifies the active MDS (Metadata Server) node for the given file system, executes a command to
        dump the directory tree in JSON format, and extracts the 'path' and 'alternate_name' fields into a dictionary.

        Args:
            client (object): The client object used to execute remote commands on the Ceph cluster.
            fs_name (str): The name of the Ceph file system for which to fetch alternate names.

        Returns:
            dict: A dictionary where the keys are file paths and the values are the corresponding alternate names.
        """
        log.info(
            "Fetching the active MDS node for file system {fs_name}".format(
                fs_name=fs_name
            )
        )
        active_mds = self.fs_util.get_active_mdss(client, fs_name)
        log.info(active_mds)
        cmd = "ceph tell mds.{active_mds} dump tree '{dir_path}' 5 -f json".format(
            active_mds=active_mds[0], dir_path=dir_path
        )
        out, _ = client.exec_command(sudo=True, cmd=cmd)

        # Remove log lines using regex to isolate JSON
        cleaned_output = re.sub(r"^[^\[{]*", "", out).strip()

        # Load JSON data
        try:
            json_data = json.loads(cleaned_output)
        except json.JSONDecodeError:
            log.error("Failed to decode JSON from the cleaned output")
            log.error(cleaned_output)
            return False

        log.info("JSON dump: %s", cleaned_output)
        # Extract 'path' and 'alternate_name' as a dictionary
        alternate_name_dict = {
            dentry.get("path", ""): dentry.get("alternate_name", "")
            for inode in json_data
            for dirfrag in inode.get("dirfrags", [])
            for dentry in dirfrag.get("dentries", [])
        }
        log.info(
            "Dict of alternate names: {alternate_name_dict}".format(
                alternate_name_dict=alternate_name_dict
            )
        )
        return alternate_name_dict

    def validate_alternate_name(
        self,
        alternate_name_dict,
        relative_path,
        norm_type,
        empty_name=False,
        casesensitive=True,
    ):
        """
        Validates whether the base64-decoded alternate name for a given path matches the path itself.

        Args:
            alternate_name_dict (dict): A dictionary where keys are relative paths (str) and
                                        values are their base64-encoded alternate names.
            relative_path (str): The relative path to validate.
            norm_type (str): The normalization type to use for the path.
            empty_name (bool, optional): If True, expects the alternate name to be empty. Defaults to False.
            casesensitive (bool, optional): If False, performs case-insensitive validation. Defaults to True.

        Returns:
            bool:
                - True if the decoded alternate name matches the given path.
                - True if `empty_name` is True and no alternate name is found.
                - False if the alternate name does not match or the path is not found in the dictionary.
        """
        log.debug("Validating for the path: '%s'", relative_path)
        casesensitive_rel_path = relative_path

        # Case-sensitive handling is required because the MDS converts characters
        # to lowercase when case sensitivity is set to False. However, the
        # alternate_name retains the original source value. To manage this,
        # the case-sensitive parameter is passed as input.
        if not casesensitive:
            relative_path = "{}/{}".format(
                "/".join(relative_path.split("/")[:-1]),
                relative_path.split("/")[-1].casefold(),
            )
        relative_path_unicode = self.normalize_unicode(relative_path, norm_type)

        # Normalize dictionary keys
        alternate_name_dict = {
            self.normalize_unicode(k, norm_type): v
            for k, v in alternate_name_dict.items()
        }

        log.debug("Relative Path Unicode: '{}'".format(relative_path_unicode))
        log.debug("Dict after normalization: {}".format(alternate_name_dict))

        if alternate_name_dict.get(relative_path_unicode) and empty_name is False:
            alternate_name = alternate_name_dict[relative_path_unicode]
            # Decode the string
            decoded_bytes = base64.b64decode(alternate_name)

            # Convert bytes to string
            decoded_str = decoded_bytes.decode("utf-8")
            log.info("Decoded String: '%s'", decoded_str)

            if not casesensitive:
                relative_path = "{}/{}".format(
                    casesensitive_rel_path.split("/")[0],
                    casesensitive_rel_path.split("/")[-1],
                )
            log.info("Path from the MDS: '%s'", os.path.basename(relative_path).strip())
            if decoded_str.strip() == os.path.basename(relative_path).strip():
                log.info("Alternate name decoded successfully and matching")
                return True
            else:
                log.error("Alternate and decoded name not matching")
                return False
        elif empty_name is True:
            log.info("Expected the alternate name to be empty and it succeeded")
            return True
        log.error(
            "Path {} not found in the dict: {}".format(
                relative_path, alternate_name_dict
            )
        )
        return False

    def normalize_unicode(self, text, unicode_form="NFC"):
        """
        Normalizes a given Unicode string to NFC (Normalization Form C).

        Args:
            text (str): The input string to normalize.

        Returns:
            str: The normalized string in NFC form.
        """
        return unicodedata.normalize(unicode_form, text)

    def validate_snapshot_from_mount(self, client, mounting_dir, snap_names):
        """
        Validates the existence of specific snapshots in the .snap directory of a mounted CephFS volume.

        Args:
            client (object): Ceph client node used to execute commands.
            mounting_dir (str): The directory where CephFS is mounted.
            snap_names (list): A list of snapshot names to validate.

        Returns:
            bool: True if at least one matching snapshot is found, False otherwise.
        """
        for snap in snap_names:
            cmd = "ls {}/.snap | grep -E '^_{}_.*' || true".format(mounting_dir, snap)
            output, _ = client.exec_command(sudo=True, cmd=cmd)

            if output.strip():
                log.info("Matching files found: %s", output.strip().split("\n"))
                return True
            else:
                log.error("No matching files found.")
                return False

    def delete_snapshots_from_mount(self, client, mounting_dir, snap_names=None):
        """
        Deletes specific snapshots or all snapshots from the .snap directory of a mounted CephFS volume.

        Args:
            client (object): Ceph client node used to execute commands.
            mounting_dir (str): The directory where CephFS is mounted.
            snap_names (list, optional): A list of snapshot names to delete. If None, all snapshots will be deleted.

        Returns:
            None
        """
        if snap_names:
            for snap in snap_names:
                cmd = "rm -rf {}/.snap/{}".format(mounting_dir, snap)
                log.info("Deleting snapshot: {}".format(snap))
                client.exec_command(sudo=True, cmd=cmd)
        else:
            cmd = "rm -rf {}/.snap/*".format(mounting_dir)
            log.info("Deleting all snapshots.")
            client.exec_command(sudo=True, cmd=cmd)

        log.info("Snapshot deletion process completed.")

    def fail_fs(self, client, fs_name):
        """
        Marks a Ceph filesystem as failed.

        Args:
            client: The client object used to execute the Ceph command.
            fs_name (str): The name of the Ceph filesystem to be marked as failed.

        Returns:
            bool: True if the command executes successfully, False otherwise.
        """
        try:
            cmd = "ceph fs fail " + fs_name
            client.exec_command(sudo=True, cmd=cmd)
            return True
        except Exception as e:
            log.error("Failed to fail filesystem '{}': {}".format(fs_name, e))
            return False
