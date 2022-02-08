class RbdBaseException(Exception):
    """Base exception for all RBD modules"""

    pass


class CreateFileError(RbdBaseException):
    """Raised when create_file_to_import function call fails"""

    pass


class ImportFileError(RbdBaseException):
    """Raised when import_file function call fails"""

    pass


class SnapCreateError(RbdBaseException):
    """Raised when rbd snap create fails"""

    pass


class ProtectSnapError(RbdBaseException):
    """Raised when protecting a snap fails"""

    pass


class CreateCloneError(RbdBaseException):
    """Raised when Creating a clone fails"""

    pass
