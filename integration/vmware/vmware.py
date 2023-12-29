"""
Note: Do not use this module directly in the Test Cases.
"""
from asyncio import exceptions
from logging import exception
import re
import string

import connect
from integration.vmware.configs import configs as g
from cli.exceptions import ConfigError
from utility.log import Log

IP_REGEX = r"(^[12]?\d{1,2}\.[12]?\d{1,3}\.[12]?\d{1,2}\.[12]?\d{1,2}$)"


class VMWare(object):

    def __init__(self):
        try:

            self.hostname = g.config['vcenters']['vcenter-bng']['hostname']
            self.username = g.config['esxi']['vcenter-bng']['username']
            self.password = g.config['vcenters']['vcenter-bng']['password']
            self.port = g.config['vcenters']['vcenter'].get('port', 443)
            
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
            raise exception.CloudProviderError(e)

