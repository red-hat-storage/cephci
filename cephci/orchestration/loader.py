"""Resolution of the ``module`` key of a test suite to an importable module.

Two spellings are supported:

``module: test_host.py``
    The historical form. The file is found on the search path below, so every file name
    under ``tests`` has to be unique for the reference to be unambiguous.

``module: tests.cephadm.test_host``
    The preferred form for new suites. It names exactly one file and survives a module
    being moved into a package that already has a file of that name.

The search path is also what lets a test import a sibling by bare name, as the modules
under ``tests/nfs`` and ``tests/smb`` do, so it is installed even when every suite in
the run uses dotted references.
"""

import importlib
import os
import sys
from types import ModuleType
from typing import List, Optional

from utility.log import Log

log = Log(__name__)

# Directories placed on sys.path, in the order they are searched. The order decides
# which file wins when a name exists in more than one directory, so entries may be
# appended but not reordered.
TEST_SEARCH_PATHS = [
    "tests",
    "tests/rados",
    "tests/cephadm",
    "tests/rbd",
    "tests/rbd/rest",
    "tests/rbd_mirror",
    "tests/cephfs",
    "tests/iscsi",
    "tests/rgw",
    "tests/ceph_ansible",
    "tests/ceph_installer",
    "tests/mgr",
    "tests/dashboard",
    "tests/misc_env",
    "tests/parallel",
    "tests/upgrades",
    "tests/ceph_volume",
    "tests/nvmeof",
    "tests/nvmeof/rest",
    "tests/rgw/rest",
    "tests/nfs",
    "tests/smb",
]


def repository_root() -> str:
    """Return the absolute path of the CephCI checkout in use."""
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestModuleLoader:
    """Imports the Python module that implements a test."""

    # Keeps pytest from collecting the class as a suite of its own.
    __test__ = False

    def __init__(self, root: Optional[str] = None) -> None:
        """Initialize the loader.

        Args:
            root (str): The CephCI checkout to load tests from. Defaults to the
                checkout this module belongs to.
        """
        self.root = root or repository_root()
        self.search_paths = [
            os.path.join(self.root, path) for path in TEST_SEARCH_PATHS
        ]

    def install(self) -> None:
        """Place the test directories on the import path.

        Returns:
            None
        """
        for path in self.search_paths:
            if path not in sys.path:
                sys.path.append(path)

    def candidates(self, name: str) -> List[str]:
        """Return the files a bare module name could refer to.

        Args:
            name (str): Module name without the extension.

        Returns:
            list: Absolute paths of the matching files, in search order.
        """
        matches = []
        for path in self.search_paths:
            candidate = os.path.join(path, f"{name}.py")
            if os.path.isfile(candidate):
                matches.append(candidate)

        return matches

    def resolve(self, module: str) -> ModuleType:
        """Import the module named by a suite entry.

        Args:
            module (str): Value of the ``module`` key of a test.

        Returns:
            ModuleType: The imported module.
        """
        name = self.module_name(module)

        if "." not in name:
            matches = self.candidates(name)
            if len(matches) > 1:
                # Resolution follows the search order, which may not be the file the
                # suite author had in mind.
                log.warning(
                    "Test module %s is ambiguous, %s all define it. Using %s. "
                    "State the module as a dotted path to remove the ambiguity.",
                    name,
                    ", ".join(matches),
                    matches[0],
                )

        return importlib.import_module(name)

    @staticmethod
    def module_name(module: str) -> str:
        """Convert the value of a ``module`` key into an importable name.

        Args:
            module (str): A file name, a path or a dotted module reference.

        Returns:
            str: The importable module name.
        """
        root, extension = os.path.splitext(module)

        # A dotted reference has no extension to strip; splitext would otherwise cut
        # tests.cephadm.test_host down to tests.cephadm.
        name = root if extension == ".py" else module

        return name.replace("/", ".")
