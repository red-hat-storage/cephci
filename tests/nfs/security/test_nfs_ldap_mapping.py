"""Legacy module name; delegates to security.test_ldap_feature."""

from tests.nfs.security.test_ldap_feature import run  # noqa: F401

__all__ = ["run"]
