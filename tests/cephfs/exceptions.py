"""
Custom exceptions categorized by CephFS functionalities in the CephCI framework.

Categories:
-----------
Generic Exception Handler
-----------
1. Base Exception
2. General / Input / Test-related
3. Mount Operations
4. Subvolume Operations
5. FS Volume Operations
6. Quota Operations
7. Attribute & CharMap Errors
8. File Operations on Mounts
9. Snapshot Operations
"""

import logging
import traceback

log = logging.getLogger(__name__)


# --------------------------------------
# Generic Exception Handler
# --------------------------------------
def log_and_fail(message: str, exception_obj: Exception) -> int:
    """
    Logs the exception with traceback and returns a standard failure code.

    Args:
        message (str): Contextual message describing the error.
        exception_obj (Exception): The actual exception instance.

    Returns:
        int: Returns 1 to indicate failure.
    """
    log.error(f"{message}: {exception_obj}")
    log.error(traceback.format_exc())
    return 1


# --------------------------------------
# 1. Base Exception
# --------------------------------------
class FsBaseException(Exception):
    """Base exception class for CephFS-related errors."""

    pass


# --------------------------------------
# 2. General / Input / Test-related
# --------------------------------------
class InvalidInput(FsBaseException, ValueError):
    """Raised when an invalid input is passed to a CephFS CLI method."""

    pass


class UnsupportedFeature(FsBaseException):
    """Raised when a CephFS feature is unsupported in the current cluster/version."""

    pass


class UnsupportedInput(FsBaseException):
    """Raised when an unsupported input is passed to a CephFS CLI method."""

    pass


class FileSystemFailOperationError(FsBaseException):
    """Raised when a file system operation fails."""

    pass


# --------------------------------------
# 3. Mount Operations
# --------------------------------------
class MountFailAssertion(FsBaseException):
    """Raised when a CephFS mount operation fails."""

    pass


class UnmountFailure(FsBaseException):
    """Raised when a CephFS unmount fails."""

    pass


class MountPointBusy(FsBaseException):
    """Raised when attempting to unmount a busy mount point."""

    pass


# --------------------------------------
# 4. Subvolume Operations
# --------------------------------------
class SubvolumeError(FsBaseException):
    """Raised when subvolume operation fails."""

    pass


class SubvolumeDeleteError(SubvolumeError):
    """Raised when subvolume deletion fails."""

    pass


class SubvolumeResizeError(SubvolumeError):
    """Raised when resizing a subvolume fails."""

    pass


class SubvolumeNotFound(SubvolumeError):
    """Raised when the specified subvolume is not found."""

    pass


# --------------------------------------
# 5. FS Volume Operations
# --------------------------------------
class VolumeError(FsBaseException):
    """Raised when volume operation fails."""

    pass


class VolumeDeleteError(VolumeError):
    """Raised when volume deletion fails."""

    pass


class VolumeNotFound(VolumeError):
    """Raised when the specified volume is not found."""

    pass


class VolumeAlreadyExists(VolumeError):
    """Raised when trying to create a volume that already exists."""

    pass


# --------------------------------------
# 6. Quota Operations
# --------------------------------------
class QuotaError(FsBaseException):
    """Raised when quota operation fails on a subvolume or directory."""

    pass


class QuotaGetError(QuotaError):
    """Raised when fetching quota fails."""

    pass


# --------------------------------------
# 7. Attribute/CharMap Errors
# --------------------------------------


class CharMapValidationError(FsBaseException):
    """Raised when a charmap validation operation fails."""

    pass


class NormalizationValidationError(CharMapValidationError):
    """Raised when normalization validation fails on a directory."""

    pass


class CaseSensitivityValidationError(CharMapValidationError):
    """Raised when a case sensitivity check fails."""

    pass


# --------------------------------------
# 8. File Operations on Mounts
# --------------------------------------
class FileOperationError(FsBaseException):
    """Raised when read/write operations fail on mounted CephFS."""

    pass


class DirectoryNotFound(FileOperationError):
    """Raised when expected directory is not found."""

    pass


class FileNotFound(FileOperationError):
    """Raised when expected file is not found."""

    pass


class FileDoesNotExistError(FileOperationError):
    """Raised when a file does not exist at the specified path."""

    pass


class RenameDirectoryError(FileOperationError):
    """Raised when a directory or subvolume rename operation fails."""

    pass


class LinkDeletionError(FileOperationError):
    """Raised when a symbolic link deletion operation fails."""

    pass


# --------------------------------------
# 9. Snapshot Operations
# --------------------------------------
class SnapshotValidationError(FsBaseException):
    """Raised when a snapshot validation fails."""

    pass
