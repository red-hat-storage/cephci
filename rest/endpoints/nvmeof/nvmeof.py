import json
from copy import deepcopy

from rest.common.config.config import Config


class NVMEoF:
    def __init__(self, rest):
        """
        Constructor for NVMEoF related REST endpoints
        """
        self._config = Config()
        self._rest = rest
        config_file_reader = self._config.get_config()
        self._gateway_info = config_file_reader["endpoints"]["nvmeof"]["GATEWAY_INFO"]
        self._create_subsystem = config_file_reader["endpoints"]["nvmeof"][
            "CREATE_SUBSYSTEM"
        ]
        self._get_subsystem = config_file_reader["endpoints"]["nvmeof"]["GET_SUBSYSTEM"]
        self._list_subsystem = config_file_reader["endpoints"]["nvmeof"][
            "LIST_SUBSYSTEM"
        ]
        self._add_host = config_file_reader["endpoints"]["nvmeof"][
            "ALLOW_HOSTS_SUBSYSTEM"
        ]
        self._list_host = config_file_reader["endpoints"]["nvmeof"][
            "LIST_ALLOWED_HOSTS_SUBSYSTEM"
        ]

    def get_gateway_info(self):
        """
        REST GET endpoint /api/nvmeof/gateway
        """
        response = self._rest.get(relative_url=self._gateway_info)
        return response

    def create_subsystem(self, **kw):
        """
        REST POST endpoint /api/nvmeof/subsystem
        Request data details
        {
            "nqn": "string",
            "enable_ha": true,
            "max_namespaces": 256
        }
        Args:
            kw: create subsystem related kw args
        """
        data = deepcopy(kw)
        response = self._rest.post(
            relative_url=self._create_subsystem, data=json.dumps(data)
        )
        return response

    def list_subsystem(self):
        """
        REST GET endpoint /api/nvmeof/subsystem
        """
        response = self._rest.get(relative_url=self._list_subsystem)
        return response

    def get_subsystem(self, subsystem_nqn):
        """
        REST GET endpoint /api/nvmeof/subsystem/{nqn}
        """
        _get_subsystem = self._get_subsystem.format(nqn=subsystem_nqn)
        response = self._rest.get(relative_url=_get_subsystem)
        return response

    def add_host(self, **kw):
        """
        REST POST endpoint /api/nvmeof/subsystem/{nqn}/host
        Request data details
        {
            "nqn": "string",
            "host_nqn": "string"
        }
        """
        data = deepcopy(kw)
        _add_host = self._add_host.format(nqn=data.pop("nqn"))
        response = self._rest.post(relative_url=_add_host, data=json.dumps(data))
        return response

    def list_host(self, subsystem_nqn):
        """
        REST GET endpoint /api/nvmeof/subsystem/{nqn}/host
        """
        # _get_host = self._get_subsystem.format(nqn=subsystem_nqn)
        # _get_host = f"{self._get_subsystem.format(nqn=subsystem_nqn)}/host"
        _get_host = self._list_host.format(nqn=subsystem_nqn)
        response = self._rest.get(relative_url=_get_host)
        return response
