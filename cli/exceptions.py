class ConfigError(Exception):
    """
    Custom exception thrown when there is an unrecoverable configuration error.
    """


class NotSupportedError(Exception):
    """
    Custom exception thrown when we do not support a particular feature in
    particular version.
    """


class CloudProviderError(Exception):
    """
    Custom exception thrown when operation failed in cloud provider libraries.
    """


class ResourceNotFoundError(Exception):
    """
    Custom exception thrown when expected resource not available.
    """


class UnexpectedStateError(Exception):
    """
    Custom exception thrown when resource is not in expected state.
    """


class OperationFailedError(Exception):
    """
    Custom exception thrown when any operation fails.
    """


class NodeConfigError(Exception):
    """
    Custom exception thrown when node configuration fails
    """
