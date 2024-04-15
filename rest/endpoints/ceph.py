import json
from copy import deepcopy

from rest.common.config.config import Config
from utility.log import Log

log = Log(__name__)


class CEPH:
    def __init__(self, rest):
        """
        Constructor for CEPH related REST API calls
        """
        self._config = Config()
        self._rest = rest

    def create_pool(self, **kw):
        """
        To create a specific pool

        REST endpoint is POST /api/pool

        POST Request data details
        {
            "application_metadata": "STRING",
            "configuration": "STRING",
            "erasure_code_profile": "STRING",
            "flags": "STRING",
            "pg_num": 1,
            "pool": "STRING",
            "pool_type": "STRING",
            "rule_name": "STRING"
        }
        Args:
            kw(dict): kw details of pool data of REST API
        """
        pool_endpoint = self._config.get_config()["endpoints"]["rbd"]["CREATE_POOL"]
        data = deepcopy(kw)
        response = self._rest.post(relative_url=pool_endpoint, data=json.dumps(data))
        return response

    def get_a_pool(self, pool):
        """
        To get a specific pool

        GET request end point /api/pool/{pool_name}

        Args:
            pool(string): pool name
        """
        get_pool_ep = self._config.get_config()["endpoints"]["rbd"]["GET_POOL"].format(
            pool_name=pool
        )
        response = self._rest.get(relative_url=get_pool_ep)
        return response

    def list_pool(self):
        """
        To list all pools
        GET request end point /api/pool
        """
        get_pool_ep = self._config.get_config()["endpoints"]["rbd"]["LIST_POOL"]
        response = self._rest.get(relative_url=get_pool_ep)
        return response
