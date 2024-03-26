import json
from copy import deepcopy

from rest.common.config.config import Config


class RBD:
    def __init__(self, rest):
        """
        Constructor for RBD related REST endpoints
        """
        self._config = Config()
        self._rest = rest

    def create_image(self, **kw):
        """
        REST endpoint /api/block/image
        Request data details
        {
            "configuration": "string",
            "data_pool": "string",
            "features": "string",
            "metadata": "string",
            "mirror_mode": "string",
            "name": "string",
            "namespace": "string",
            "obj_size": 1,
            "pool_name": "string",
            "schedule_interval": "string",
            "size": 1,
            "stripe_count": 1,
            "stripe_unit": "string"
        }
        Args:
            kw(dict): image data related params for REST
        """
        image_endpoint = self._config.get_config()["endpoints"]["rbd"]["CREATE_IMAGE"]
        data = deepcopy(kw)
        response = self._rest.post(relative_url=image_endpoint, data=json.dumps(data))
        return response

    def get_image(self, image_spec):
        """
        To get a specific image

        GET request end point /api/block/image/{image_spec} where image_config is <pool>/<image>
        """
        get_image_ep = self._config.get_config()["endpoints"]["rbd"][
            "GET_IMAGE"
        ].format(image_spec=image_spec)
        response = self._rest.get(relative_url=get_image_ep)
        return response

    def list_image(self):
        """
        To list all images
        GET request end point /api/block/image
        """
        get_image_ep = self._config.get_config()["endpoints"]["rbd"]["LIST_IMAGE"]
        response = self._rest.get(relative_url=get_image_ep)
        return response
