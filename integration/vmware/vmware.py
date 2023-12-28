"""
Note: Do not use this module directly in the Test Cases.
"""
import re
import string

from pyVim import connect
from pyVmomi import vim
import six

IP_REGEX = r"(^[12]?\d{1,2}\.[12]?\d{1,3}\.[12]?\d{1,2}\.[12]?\d{1,2}$)"


class VmWare(object):

    def __init__(self):
        try:
            self.hostname = g.config['cprovider']['vcenter']['hostname']
            self.username = g.config['cprovider']['vcenter']['username']
            self.password = g.config['cprovider']['vcenter']['password']
            self.port = g.config['cprovider']['vcenter'].get('port', 443)
        except KeyError:
            msg = ("Config file doesn't have values related to vmware Cloud"
                   " Provider.")
            g.log.error(msg)
            raise exceptions.ConfigError(msg)

        # Connect vsphere client
        try:
            self.vsphere_client = connect.ConnectNoSSL(
                self.hostname, self.port, self.username, self.password)
        except Exception as e:
            g.log.error(e)
            raise exceptions.CloudProviderError(e)

    def __del__(self):
        # Disconnect vsphere client
        try:
            connect.Disconnect(self.vsphere_client)
        except Exception as e:
            g.log.error(e)
            raise exceptions.CloudProviderError(e)

