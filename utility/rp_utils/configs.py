import json
import logging
import os

log = logging.getLogger(__name__)

# Define sentinel object NULL constant
NULL = object()


# pylint: disable=too-many-instance-attributes
#         reviewed and disabled (jdh)
class Configs:
    """Configuration class to handle config file, args, env tasks.
    Order of precedence (ordoprec) cli > envvars > config > defaults
    """

    def __init__(self, args, fqpath=None, config=None):
        self._args = args
        log.debug(self._args)
        self._fqpath = fqpath

        # define args using sentinel (not None)
        self._service_url = NULL
        self._payload_dir = NULL
        self._merge_launches = NULL
        self._simple_xml = NULL
        self._debug = NULL
        self._config = {}
        log.debug("Configs.init(): reading %s", self._fqpath)

        if config is not None:
            self._config = config
        else:
            self._config = self._read_fqpath(self.fqpath)
        log.info("Configs.init(): CONFIG: %s", self._config)
        log.debug("Configs.init(): CONFIG: %s", self._config)

    # PROPERTIES
    # using these to handle order of presidence of config, args, etc.
    @property
    def args(self):
        """args - the args object from CLI or REST API"""
        return self._args

    @property
    def fqpath(self):
        """fqpath - config file fully qualified path"""
        if self._fqpath is None:
            self._fqpath = self.args.get("config_file", None)

        return self._fqpath

    @property
    def config(self):
        """Return the config object"""
        return self._config

    @config.setter
    def config(self, config):
        self._config = config

    @property
    def service_config(self):
        """The RP PreProc section of the config file"""
        return self._config.get("rp_preproc", None)

    @property
    def rp_config(self):
        """The RP PreProc section of the config file"""
        if self._config is not None:
            return self._config.get("reportportal", None)

        return None

    @property
    def payload_dir(self):
        """Directory containing the payload files"""
        if self._payload_dir is NULL:
            self._payload_dir = self.get_config_item(
                "payload_dir", config=self.service_config
            )

        return self._payload_dir

    @payload_dir.setter
    def payload_dir(self, payload_dir):
        self._payload_dir = payload_dir

    @property
    def service_url(self):
        """use_service - send to service or use local client"""
        if self._service_url is NULL:
            self._service_url = self.get_config_item(
                "service_url", config=self.service_config
            )

        return self._service_url

    @property
    def simple_xml(self):
        """Use simple xml import into ReportPortal instead of processing"""
        if self._simple_xml is NULL:
            self._simple_xml = self.get_config_item("simple_xml", config=self.rp_config)

        return self._simple_xml

    @property
    def merge_launches(self):
        """Config merge launches after import"""
        if self._merge_launches is NULL:
            self._merge_launches = self.get_config_item(
                "merge_launches", config=self.rp_config
            )

        return self._merge_launches

    # PRIVATE METHODS
    def _read_fqpath(self, fqpath=None):
        """Read a json formatted file into a dictionary"""
        log.debug("Configs._read_fqpath(): CONFIG FQPATH: %s", fqpath)
        if fqpath is not None:
            self._fqpath = fqpath

        configfd = open(self._fqpath, "r")
        if configfd:
            self._config = json.load(configfd)

            return self._config

        return None

    def get_config_item(self, config_item, config=None, ordoprec=None, default=None):
        """Order of precedence helper for configs"""
        # cli > envvars > config > defaults
        if config is None:
            config = self.config
        if ordoprec is None:
            ordoprec = ["config", "env", "cli"]
        ordoprec_dict = dict()

        # cli
        ordoprec_dict["cli"] = self.args.get(config_item, None)
        # config
        log.debug("Configs.get_config_item()...from config: %s", config)
        ordoprec_dict["config"] = config.get(config_item, None)
        # env
        if config_item == "auto-dashboard":
            env_name = "RP_AUTO_DASHBOARD"
        else:
            env_name = "RP_{}".format(config_item.upper())
        ordoprec_dict["env"] = os.getenv(env_name, None)

        config_value = default
        for source in ordoprec:
            log.debug(
                "Configs.get_config_item()... " "Config item (%s): %s = %s",
                config_item,
                source,
                ordoprec_dict[source],
            )

            if ordoprec_dict[source] is not None:
                config_value = ordoprec_dict[source]

        log.debug("Configs.get_config_item()... config_value: %s", config_value)
        return config_value
