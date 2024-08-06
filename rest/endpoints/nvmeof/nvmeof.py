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
        self._add_listener = config_file_reader["endpoints"]["nvmeof"][
            "ADD_SUBSYSTEM_LISTENER"
        ]
        self._list_listener = config_file_reader["endpoints"]["nvmeof"][
            "LIST_ALL_SUBSYSTEM_LISTENER"
        ]
        self._add_namespace = config_file_reader["endpoints"]["nvmeof"][
            "CREATE_NAMESPACE_SUBSYSTEM"
        ]
        self._list_namespace = config_file_reader["endpoints"]["nvmeof"][
            "LIST_NAMESPACES_SUBSYSTEM"
        ]
        self._delete_namespace = config_file_reader["endpoints"]["nvmeof"][
            "DELETE_NAMESPACE"
        ]
        self._delete_subsystem = config_file_reader["endpoints"]["nvmeof"][
            "DELETE_SUBSYSTEM"
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
        _get_host = self._list_host.format(nqn=subsystem_nqn)
        response = self._rest.get(relative_url=_get_host)
        return response

    def add_listener(self, **kw):
        """
        REST POST endpoint /api/nvmeof/subsystem/{nqn}/listener
        Request data details
        {
            "host_name": "string",
            "traddr": "string",
            "trsvcid": 4420,
            "adrfam": 0
        }
        """
        data = deepcopy(kw)
        _add_listener = self._add_listener.format(nqn=data.pop("nqn"))
        response = self._rest.post(relative_url=_add_listener, data=json.dumps(data))
        return response

    def list_listener(self, subsystem_nqn):
        """
        REST GET endpoint /api/nvmeof/subsystem/{nqn}/listener
        """
        _get_listener = self._list_listener.format(nqn=subsystem_nqn)
        response = self._rest.get(relative_url=_get_listener)
        return response

    def add_namespace(self, **kw):
        """
        REST POST endpoint /api/nvmeof/subsystem/{nqn}/namespace
        Request data details
        {
            "rbd_image_name": "string",
            "rbd_pool": "rbd",
            "create_image": true,
            "size": 1024,
            "block_size": 512,
            "load_balancing_group": null
        }
        """
        data = deepcopy(kw)
        _add_namespace = self._add_namespace.format(nqn=data.pop("nqn"))
        response = self._rest.post(relative_url=_add_namespace, data=json.dumps(data))
        return response

    def list_namespace(self, subsystem_nqn):
        """
        REST GET endpoint /api/nvmeof/subsystem/{nqn}/namespace
        """
        _get_namespace = self._list_namespace.format(nqn=subsystem_nqn)
        response = self._rest.get(relative_url=_get_namespace)
        return response

    def delete_namespace(self, subsystem_nqn, nsid):
        """
        REST DELETE endpoint /api/nvmeof/subsystem/{nqn}/namespace/{nsid}
        """
        _delete_namespace = self._delete_namespace.format(nqn=subsystem_nqn, nsid=nsid)
        response = self._rest.delete(relative_url=_delete_namespace)
        return response

    def delete_subsystem(self, subsystem_nqn):
        """
        REST DELETE endpoint /api/nvmeof/subsystem/{nqn}
        """
        _delete_subsystem = self._delete_subsystem.format(nqn=subsystem_nqn)
        response = self._rest.delete(relative_url=_delete_subsystem)
        return response
