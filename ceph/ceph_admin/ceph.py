"""Interface to the base command ceph."""
from ceph.ceph_admin import CephAdmin


class CephCLI(CephAdmin):
    """Interface to the ceph CLI."""
