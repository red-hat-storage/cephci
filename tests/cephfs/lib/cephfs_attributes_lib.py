import json
import os
import random
import string
import unicodedata

from tests.cephfs.cephfs_utilsV1 import FsUtils
from utility.log import Log

log = Log(__name__)


class CephFSAttributeUtilities:
    def __init__(self, ceph_cluster):
        self.ceph_cluster = ceph_cluster
        self.fs_util = FsUtils(ceph_cluster)

        self.mons = self.fs_util.mons
        self.mgrs = self.fs_util.mgrs
        self.osds = self.fs_util.osds
        self.mdss = self.fs_util.mdss
        self.clients = self.fs_util.clients
        self.installer = self.fs_util.installer

    def set_attributes(self, directory, **kwargs):
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
                raise ValueError(
                    f"Unsupported attribute: {key}. Supported attributes are {supported_attributes}"
                )

            cmd = f"setfattr -n ceph.dir.{key} -v {value} {directory}"
            self.client.exec_command(sudo=True, cmd=cmd)
            log.info(f"Set {key} to {value} on {directory}")

    def get_charmap(self, directory):
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
        cmd = f"getfattr -n ceph.dir.charmap {directory}"
        out, _ = self.client.exec_command(sudo=True, cmd=cmd, check_ec=False)
        try:
            charmap_line = next(
                line for line in out.split("\n") if "ceph.dir.charmap" in line
            )
            charmap_str = charmap_line.split("=", 1)[1].strip().strip('"')
            return json.loads(charmap_str)
        except Exception:
            log.error(f"Failed to parse charmap for {directory}")
            return {}

    def compare_charmaps(self, charmap1, charmap2):
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

    def update_required_client_features(self, fs_name):
        """Update the required client feature attribute for a Ceph filesystem.

        Args:
            fs_name (str): The name of the Ceph filesystem to update.

        Returns:
            None
        """
        cmd = f"ceph fs required_client_features {fs_name} add charmap"
        self.client.exec_command(sudo=True, cmd=cmd)
        log.info(f"Updated client feature for {fs_name}")

    def create_directory(self, path):
        """Create a directory, including parent directories if needed.

        Args:
            path (str): The full path of the directory to create.

        Example:
            create_directory("/mnt/cephfs/subdir1/subdir2")
            # This will execute: mkdir -p /mnt/cephfs/subdir1/subdir2
        """
        cmd = f"mkdir -p {path}"
        self.client.exec_command(sudo=True, cmd=cmd)
        log.info(f"Created directory: {path}")

    def create_special_character_directories(self, base_path, count=1):
        """Create directories with a mix of special characters.
        Args:
            base_path (str): The parent directory where the new directories will be created.
            count (int, optional): The number of directories to create. Defaults to 1.

        Returns:
            None
        """
        special_chars = "@#$%^&*()-_=+[]{}|;:'\",<>?/"
        for _ in range(count):
            dir_name = "".join(
                random.choices(
                    string.ascii_letters + string.digits + special_chars,
                    k=random.randint(1, 255),
                )
            )
            full_path = os.path.join(base_path, dir_name)
            self.create_directory(full_path)
            log.info(f"Created special character directory: {full_path}")

    def check_ls_case_sensitivity(self, base_path, dir_name):
        """
        Check if 'ls' can retrieve a directory with different case variations.

        Args:
            base_path (str): The base path where the directory is located.
            dir_name (str): The original directory name.

        Returns:
            bool: True if 'ls' successfully lists the directory (case-insensitive),
                False if it fails (case-sensitive).
        """
        test_variations = [dir_name.lower(), dir_name.upper(), dir_name.swapcase()]

        for variant in test_variations:
            test_path = os.path.join(base_path, variant)
            cmd = f"ls {test_path}"
            out, rc = self.client.exec_command(sudo=True, cmd=cmd, check_ec=False)
            log.debug(f"ls output of {base_path}/{dir_name}: {out}")

            if rc != 0:  # 'ls' failed
                log.info(f"Directory '{test_path}' is inaccessible.")
                return False

        log.info(f"Directory '{dir_name}' is accessible with different cases.")
        return True

    def create_links(self, target, link_name, link_type="soft"):
        """Create soft or hard links.
        Args:
            target (str): The file or directory to link to.
            link_name (str): The name of the link to be created.
            link_type (str, optional): Type of link to create.
                                       Use "soft" for a symbolic link (default)
                                       or "hard" for a hard link.
        """
        cmd = f'ln {"-s" if link_type == "soft" else ""} {target} {link_name}'
        self.client.exec_command(sudo=True, cmd=cmd)
        log.info(f"Created {link_type} link: {link_name} -> {target}")

    def remove_charmap_settings(self, directory):
        """Remove charmap attributes from a directory.
        Args:
            directory (str): The path to the directory from which charmap attributes should be removed.
        """
        cmd = f"setfattr -x ceph.dir.charmap {directory}"
        self.client.exec_command(sudo=True, cmd=cmd)
        log.info(f"Removed charmap settings from {directory}")

    def generate_random_unicode_names(count=1):
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

    def validate_normalization(self, base_path, dir_name, norm_type):
        """check if dir names are normalized by the filesystem.

        Args:
            base_path (str): Directory where test directories are created.
            dir_name (str): Original directory name before normalization.
            norm_type (str): Expected normalization type (NFC, NFD, NFKC, NFKD).

        Returns:
            bool: True if the filesystem-normalized name matches the expected normalization.
        """
        cmd = f"ls {base_path}/{dir_name}"
        out, _ = self.client.exec_command(sudo=True, cmd=cmd)

        retrieved_names = out.split("\n")
        normalized_expected = unicodedata.normalize(norm_type, dir_name)

        for name in retrieved_names:
            normalized_name = unicodedata.normalize(norm_type, name)
            if normalized_name == normalized_expected:
                return True  # Match found

        # If no match, log expected vs actual names
        log.error(f"Normalization mismatch in {dir_name}:")
        log.error(f"Expected (Normalized): {normalized_expected}")
        log.error(f"Actual Retrieved Names: {retrieved_names}")

        return False
