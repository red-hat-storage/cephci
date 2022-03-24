import json
import logging
import os
import uuid

import xmltodict

from utility.rp_utils.configs import Configs
from utility.rp_utils.xunit_xml import XunitXML

log = logging.getLogger(__name__)


class PreProcClient:
    """PreProc client class for preprocessing test results for ReportPortal"""

    @property
    def args(self):
        """Args from cli or service"""
        return self._args

    @property
    def configs(self):
        """Configs arg"""
        return self._configs

    def __init__(self, args):
        # read config files and update g.config attributes
        self._args = args
        self._config_file = self.args.get("config_file", None)
        log.debug("ARGS: %s", self._args)
        self._configs = None
        self.config_file_path = None
        self._configs = Configs(args)

    @staticmethod
    def get_uuid():
        """Unique ID helper"""
        return uuid.uuid1()

    @staticmethod
    def read_config_file(fqpath):
        """Read a json formatted file into a dictionary"""
        configfd = open(fqpath, "r")
        if configfd:
            config = json.load(configfd)

            return config

        return None

    @staticmethod
    def process_response(response):
        """Process an HTTP response into something usable"""
        # TODO: process status code and set return
        log.debug("RESPONSE...")
        log.debug(response)
        log.debug("status_code: %s", response.status_code)
        log.debug("text: %s", response.text)
        return_code = response.status_code
        if response.status_code == 200:
            return_code = 0

        return return_code

    @staticmethod
    def get_results_dir(args, config):
        """Get the directoy containing results from config, args, etc."""
        if args.results_dir is not None:
            results_dir = args.results_dir
        else:
            results_dir = config.get("results_dir", None)

        return results_dir

    def __process_file(self, rportal, fqpath):
        with open(fqpath) as xmlfd:
            log.debug("Processing fqpath %s", fqpath)
            filename = os.path.basename(fqpath)
            filename_base, _ = os.path.splitext(filename)
            log.debug("%s %s", filename, filename_base)

            log.debug("Parsing XML...")
            xml_data = xmltodict.parse(xmlfd.read())
            xunit_xml = XunitXML(
                rportal, name=filename_base, configs=self._configs, xml_data=xml_data
            )
            xunit_xml.process()
