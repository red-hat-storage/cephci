"""Module to contain UTIL_MAP.

UTIL_MAP is a dict of utils available and to its class.
"""

from utility.copy_file import CopyFile
from utility.create_dummy_file import CreateDummyFile

UTIL_MAP = dict(
    {
        "create_dummy_file": CreateDummyFile,
        "copy_file": CopyFile,
    }
)
