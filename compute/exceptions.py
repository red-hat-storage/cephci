"""Custom exceptions for compute driver implementations."""


class ResourceNotFound(Exception):
    pass


class ExactMatchFailed(Exception):
    pass


class VolumeOpFailure(Exception):
    pass


class NetworkOpFailure(Exception):
    pass


class NodeError(Exception):
    pass


class NodeDeleteFailure(Exception):
    pass
