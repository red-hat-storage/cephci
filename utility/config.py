"""
CephCI singleton configuration model.
Provides an easy method to access the test configurations. The only drawback is the test
developer must use define a global variable. They tend to get loaded earlier that lead
to empty values.
"""


class Singleton(type):
    """Ensures a single instance."""

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances.keys():
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)

        return cls._instances[cls]


class TestMetaData(dict, metaclass=Singleton):
    """Ceph CI configuration object."""

    def __init__(self, *arg, **kw):
        super(TestMetaData, self).__init__(*arg, **kw)
        self.__dict__ = kw
