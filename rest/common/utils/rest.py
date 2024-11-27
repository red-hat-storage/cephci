"""
Python module for initiating and executing commands via REST API.
"""

import json
import time

import requests

from rest.common.config.config import Config
from rest.common.utils.exceptions import CommandExecutionError, HTTPError
from utility.log import Log

# from dotenv import dotenv_values


log = Log(__name__)


def rest(**kw):
    """
    REST method to be used by any test method
    # config = {"IP": "10.12.34.23", "USERNAME": "admin", "PASSWORD":"passwd", "PORT":"3211"}
    """
    # config = dotenv_values(".env")
    # username = config["USERNAME"]
    # password = config["PASSWORD"]
    # port = config["PORT"]
    # ip = config["IP"]
    # rest = REST(ip=ip, username=username, password=password, port=port)
    ceph_cluster = kw.get("ceph_cluster")
    nodes = ceph_cluster.node_list
    ip = nodes[0].ip_address
    rest = REST(ip=ip)
    return rest


class REST(object):
    """REST class for invoking REST calls GET, POST, PUT, DELETE."""

    # Constants representing REST API keys.
    HEADERS = "headers"
    DATA = "data"

    # Constants representing type of REST request
    GET = "get"
    PATCH = "patch"
    POST = "post"
    PUT = "put"
    DELETE = "delete"

    def __init__(self, **kwargs):
        """This class defines methods to invoke REST calls.

        Args:
          ip(str): IP address.
          username(str, optional): Username to be used for authentication.
            Default: 'admin'.
          password(str, optional): Password to be used for authentication.
            Default: 'admin123'.
          port(int,optional): Port to connect for sending REST calls. Default: 9440.
          base_uri(str,optional): URI for sending REST calls to.
            Default: Prism gateway URI.

        Returns
          Returns REST object instance.
        """
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/vnd.ceph.api.v1.0+json",
        }
        self._config = Config()
        self._ip = kwargs.get("ip", None)
        if not self._ip:
            raise ValueError("Invalid IP address '%s'." % self._ip)

        self._username = kwargs.get("username", "admin")
        self._password = kwargs.get("password", "admin123")
        self._port = kwargs.get("port", 8443)
        base_url = f"https://{self._ip}:{self._port}"
        self._base_uri = kwargs.get("base_uri", base_url)

        # Disable HTTPS certificate warning.
        requests.packages.urllib3.disable_warnings()
        auth_token = kwargs.get("token", None)
        if auth_token is None:
            self.auth()
        else:
            self.check_auth(auth_token)

    def check_auth(self, token):
        """
        checks the authentication for the given token
        Args:
          token(str): auth token
        """
        check_auth_relative_endpoint = self._config.get_config()["endpoints"]["common"][
            "CHECK_AUTH"
        ]
        _data = {"token": token}
        try:
            _ = self.post(
                relative_url=check_auth_relative_endpoint,
                headers=self.headers,
                data=_data,
            )
            self.headers.update({"Authorization": f"Bearer {token}"})
        except Exception as e:
            # or can call self.auth()
            log.error(f"Auth check failed for the token f{e}")
            raise Exception("Auth token invalid")

    def auth(self):
        """
        Calls the auth method and returns auth bearer token
          request data :{
          "username": <string>,
          "password": <string>
          }
        """
        auth_relative_endpoint = self._config.get_config()["endpoints"]["common"][
            "AUTH"
        ]
        _data = {"username": self._username, "password": self._password}
        auth_res = self.post(
            relative_url=auth_relative_endpoint, headers=self.headers, data=_data
        )
        self.headers.update({"Authorization": f"Bearer {auth_res['token']}"})

    def delete(self, relative_url, **kwargs):
        """This routine is used to invoke DELETE call for REST API.

        Args:
          relative_url(str): Relative URL for the particular API call.
          kwargs:
            headers(str, optional): Custom headers for making the REST call.
              Default: {}.
            data(str, optional): Data to be send for making the REST call.
              Default: {}.

        Returns:
          str: response text.
        """
        kwargs["operation"] = REST.DELETE
        return self.__perform_operation(relative_url, **kwargs)

    def get(self, relative_url, **kwargs):
        """This routine is used to invoke GET call for REST API.

        Args:
          relative_url: Relative URL for the particular API call.
          kwargs:
            headers(dict, optional): Custom headers for making the REST call.
              Default: {}
            data(dict, optional): Data to be send for making the REST call.
              Default: {}

        Returns:
          str: response text.
        """
        kwargs["operation"] = REST.GET
        return self.__perform_operation(relative_url, **kwargs)

    def patch(self, relative_url, **kwargs):
        """This routine is used to invoke PATCH call for REST API.

        Args:
          relative_url(str): Relative URL for the particular API call.
          kwargs:
            headers(str, optional): Custom headers for making the REST call.
              Default: {}
            data(str, optional): Data to be send for making the REST call.
              Default: {}

        Returns:
          str: response text.
        """
        kwargs["operation"] = REST.PATCH
        return self.__perform_operation(relative_url, **kwargs)

    def post(self, relative_url, **kwargs):
        """This routine is used to invoke POST call for REST API.

        Args:
          relative_url(str): Relative URL for the particular API call.
          kwargs:
            headers(str, optional): Custom headers for making the REST call.
              Default: {}
            data(str, optional): Data to be send for making the REST call.
              Default: {}

        Returns:
          str: response text.
        """
        kwargs["operation"] = REST.POST
        return self.__perform_operation(relative_url, **kwargs)

    def put(self, relative_url, **kwargs):
        """This routine is used to invoke PUT call for REST API.

        Args:
          relative_url(str): Relative URL for the particular API call.
          kwargs:
            headers(str, optional): Custom headers for making the REST call.
              Default: {}
            data(str, optional): Data to be send for making the REST call.
              Default: {}

        Returns:
          str: response text.
        """
        kwargs["operation"] = REST.PUT
        return self.__perform_operation(relative_url, **kwargs)

    def __perform_operation(self, relative_url, **kwargs):
        """
        Private Method which can be used to perform operations like post, get,
        patch, delete and put.

        Returns:
          str: Response text.

        """
        custom_headers = kwargs.get(REST.HEADERS, self.headers)
        custom_data = kwargs.get(REST.DATA, {})
        raw_response = kwargs.pop("raw_response", False)
        max_retries = kwargs.get("max_retires", 3)
        main_uri = "".join([self._base_uri, relative_url])
        if "operation" not in kwargs:
            raise ValueError("Operation value not specified.")
        operation = kwargs.get("operation")

        # Encode(Serialize) the data using json.dumps
        data = json.dumps((custom_data), indent=2)
        auth = (self._username, self._password)
        response = self.__send_request(
            req_type=operation,
            main_uri=main_uri,
            headers=custom_headers,
            verify=False,
            data=data,
            auth=auth,
            max_retries=max_retries,
            raw_response=raw_response,
        )
        return response

    def __send_request(self, **kwargs):
        """
        Private Method which can be used to send any kind of
        HTTP request with custom retries
        """
        req_type = kwargs.pop("req_type", None)
        if not req_type:
            raise ValueError("REST request type not specified.")

        main_uri = kwargs.pop("main_uri", None)
        if not main_uri:
            raise ValueError("REST request URL not specified.")

        headers = kwargs.pop("headers", {})
        data = kwargs.pop("data", {})
        auth = kwargs.pop("auth", {})
        raw_response = kwargs.pop("raw_response", False)

        max_retries = kwargs.pop("max_retries", 3)
        verify = kwargs.pop("verify", False)
        timeout = kwargs.pop("timeout", 60)
        interval = kwargs.pop("interval", 5)

        retry_count = 1
        log.info(f"REST call Details {req_type.upper()}: {main_uri}, {headers}, {data}")
        while retry_count <= max_retries:
            method_to_call = getattr(requests, req_type)
            response = method_to_call(
                main_uri, headers=headers, verify=verify, data=data, timeout=timeout
            )
            if raw_response:
                return response

            if (
                response.status_code == requests.codes.OKAY
                or response.status_code == requests.codes.CREATED
                or response.status_code == requests.codes.ACCEPTED
                or response.status_code == requests.codes.NO_CONTENT
            ):
                # Decode(Deserialize) the json object using json.loads
                if response.status_code == requests.codes.NO_CONTENT:
                    return {}
                return_val = json.loads(response.text)
                log.debug(f"<< {json.dumps(return_val, indent=2)}")
                return return_val

            if response.status_code == requests.codes.UNAUTHORIZED:
                log.debug(
                    f"UNAUTHORIZED ERROR({response.status_code}:{response.text}).Please check given credentials:{auth}"
                )
                raise HTTPError(f"{response.status_code} Credential used are {auth}")

            if response.status_code == requests.codes.BAD_REQUEST:
                api_used = f"req_type= {req_type}, url={main_uri}, headers={headers}, data={data}, auth={auth}"
                log.debug(f"BAD REQUEST {response.status_code}: api = {api_used}")
                raise HTTPError(
                    f"{response.status_code} Check the api content: {api_used}"
                )

            if response.status_code == requests.codes.UNSUPPORTED_MEDIA_TYPE:
                message = f"{response.status_code}: Check if the request has been serialized before sending"
                log.debug(f"UNSUPPORTED MEDIA TYPE {message}")
                raise HTTPError(message)

            if response.status_code == requests.codes.INTERNAL_SERVER_ERROR:
                message = f"[{response.status_code}:{response.text}]."
                log.debug(f"INTERNAL SERVER ERROR {message}")
                raise CommandExecutionError(message)

            if response.status_code == requests.codes.CONFLICT:
                message = f"[{response.status_code}:{response.text}]."
                log.debug(f"CONFLICT {message}")
                raise HTTPError(message)

            if retry_count == max_retries:
                log.debug(f"RESTError: {req_type} response: {response.text}")
                raise CommandExecutionError(
                    f"Reached max retries: {max_retries} "
                    "waiting for proper {req_type} response. "
                    "Response:[{response.status_code}:{response.text}] "
                )
            retry_count = retry_count + 1
            time.sleep(interval)
