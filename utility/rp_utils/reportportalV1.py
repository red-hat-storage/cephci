import json
import logging
import os
import posixpath
import re
import time
import uuid
from mimetypes import guess_type
from zipfile import ZipFile

import requests
import urllib3
from reportportal_client import ReportPortalService

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
log = logging.getLogger(__name__)


class ReportPortalV1:
    """ReportPortal class to assist with RP API calls"""

    def __init__(
        self,
        config,
        endpoint=None,
        api_token=None,
        project=None,
        merge_launches=None,
        rpuid=None,
    ):
        """Create a ReportPortal client instance

        Args:
            config (str): the reportportal config section from file
        """
        self._rpuid = rpuid
        self._service = None
        self._config = config
        self._endpoint = endpoint
        self._api_token = api_token
        self._project = project
        self._merge_launches = merge_launches
        self._launches = Launches(self)

    @property
    def rpuid(self):
        """get the unique id of this instance"""
        if self._rpuid is None:
            self._rpuid = uuid.uuid1().hex

        return self._rpuid

    @property
    def config(self):
        """get the rp section json object"""
        # TODO: add validation for required elements

        return self._config

    @property
    def endpoint(self):
        """get endpoint"""
        if self._endpoint is None:
            self._endpoint = self.config.get(
                "host_url", os.environ.get("RP_HOST_URL", None)
            )
        # TODO: raise exception on endpoint is still None
        return self._endpoint

    @property
    def api_token(self):
        """Get the user api_token"""
        if self._api_token is None:
            self._api_token = self.config.get(
                "api_token", os.environ.get("RP_API_TOKEN", None)
            )
        # TODO: validate api_token is not None
        # TODO: validate api_token works ???
        return self._api_token

    @property
    def project(self):
        """project attr getter"""
        if self._project is None:
            self._project = self.config.get(
                "project", os.environ.get("RP_PROJECT", None)
            )

        return self._project

    @property
    def service(self):
        """get service"""
        # creating service on first call to get service
        if self._service is None:
            self._service = ReportPortalService(
                endpoint=self.endpoint,
                project=self.project,
                token=self.api_token,
                log_batch_size=1,
                verify_ssl=False,
            )
            self._service.session.verify = False

            # TODO: validate the service works

        return self._service

    @property
    def launches(self):
        """launch list attr getter"""
        return self._launches

    @property
    def merge_launches(self):
        """Should launches be merged?"""
        if self._merge_launches is None:
            self._merge_launches = self.config.get("merge_launches", False)

        log.debug("ReportPortal.merge_launches: %s", self._merge_launches)
        return self._merge_launches

    @merge_launches.setter
    def merge_launches(self, merge_launches_value):
        """Override merge_launches setting"""
        self._merge_launches = merge_launches_value

    @property
    def launch_config(self):
        """launch_config attr gettr"""
        launch_config = Launch.get_config(self.config)

        return launch_config

    def api_put(self, api_path, put_data=None, verify=False):
        """PUT to the ReportPortal API"""
        url = posixpath.join(self.endpoint, "api/v1/", self.project, api_path)
        log.debug("url: %s", url)

        session = requests.Session()
        session.headers["Authorization"] = "bearer {0}".format(self.api_token)
        session.headers["Content-type"] = "application/json"
        session.headers["Accept"] = "application/json"
        response = session.put(url, data=json.dumps(put_data), verify=verify)

        log.debug("r.status_code: %s", response.status_code)
        log.debug("r.text: %s", response.text)

        return response

    def api_get(self, api_path, get_data=None, verify=False):
        """GET from the ReportPortal API

        Args:
            get_data (list): list of key=value pairs

        Returns:
            session response object
        """
        url = posixpath.join(self.endpoint, "api/v1/", self.project, api_path)
        if get_data is not None:
            get_string = "?{}".format("&".join(get_data))
            url += get_string
        log.debug("url: %s", url)

        session = requests.Session()
        session.headers["Authorization"] = "bearer {0}".format(self.api_token)
        session.headers["Accept"] = "application/json"

        response = session.get(url, verify=verify)

        log.debug("r.status_code: %s", response.status_code)
        log.debug("r.text: %s", response.text)

        return response

    def api_post(self, api_path, post_data=None, filepath=None, verify=False):
        """POST to the ReportPortal API"""
        url = posixpath.join(self.endpoint, "api/v1/", self.project, api_path)
        log.debug("url: %s", url)

        session = requests.Session()
        session.headers["Authorization"] = "bearer {0}".format(self.api_token)

        if filepath is None:
            session.headers["Content-type"] = "application/json"
            session.headers["Accept"] = "application/json"
            log.debug("API_POST(): %s" % json.dumps(post_data))
            response = session.post(url, data=json.dumps(post_data), verify=verify)
        else:
            files = {"file": open(filepath, "rb")}
            response = session.post(url, data={}, files=files, verify=False)

        log.debug("r.status_code: %s", response.status_code)
        log.debug("r.text: %s", response.text)

        return response

    def api_post_zipfile(self, infile, outfile="/tmp/myresults.zip"):
        """POST a single zip file to the ReportPortal API"""
        with ZipFile(outfile, "w") as zipit:
            zipit.write(infile)
        api_path = "launch/import"

        response = self.api_post(api_path, filepath=outfile)

        try:
            response_json = response.json()
            log.debug("r.json: %s", response_json)
            idregex = re.match(".*id = (.*) is.*", response_json["message"])
            # this gives the uuid of the launch
            launch_uuid = idregex.group(1)

            # to get the launch id
            api_path = "launch"
            get_string = "filter.eq.uuid={}".format(launch_uuid)
            response = self.api_get(api_path, get_data=[get_string])
            response_json = response.json()
            log.debug("GET LAUNCH ID BY UUID: %s", response_json)
            if response_json["content"]:
                response_filter = response_json["content"][0]
                launch_id = response_filter["id"]
                log.debug("Launch id from xml import: %s", launch_id)
                self.launches.add(launch_id)
                return response_json
        except json.JSONDecodeError:
            return_response = response.text
            log.debug("r.text: %s", return_response)

        return None


class Launches:
    """Class to handle multiple launches"""

    def __init__(self, rportal):
        self._rportal = rportal
        self._list = []

    @property
    def list(self):
        """Get the list of launches"""
        return self._list

    def add(self, launch_id):
        """Add a launch to the list"""
        self.list.append(launch_id)

    def merge(
        self, name="Merged Launch", description="merged launches", merge_type="BASIC"
    ):
        """Merge all launches in the list into one launch"""
        log.debug("merging launches: %s", self.list)

        name = self._rportal.launch_config.get("name", name)
        description = self._rportal.launch_config.get("description", description)
        post_merge_json = {
            "description": description,
            "extendSuitesDescription": True,
            "launches": self.list,
            "mergeType": merge_type,
            "mode": "DEFAULT",
            "name": name,
        }
        api_path = "launch/merge"
        response = self._rportal.api_post(api_path, post_data=post_merge_json)

        try:
            return_response = response.json()
            log.debug("r.json: %s", return_response)
            merged_launch = return_response["id"]
            log.debug("merged launch: %s", merged_launch)

            return merged_launch
        except json.JSONDecodeError:
            # TODO: chase down the specific json exception
            return_response = response.text
            log.debug("r.text: %s", return_response)

        return None


class Launch:
    """ReportPortal launch class"""

    @staticmethod
    def get_config(config):
        """Get the launch config section from a ReportPortal config"""
        launch_config = config.get("launch", None)

        return launch_config

    def __init__(self, rportal, name=None, description=None, attributes=None):
        """Create an instance of ReportPortal launch class

        Args:
            rportal (obj): A ReportPortal class instance
            name (str): The name of the launch
            description (str): Information describing the launch
            attributes (list): A list of attributes to add to the launch
        """
        self._rportal = rportal
        self._service = rportal.service
        self._config = Launch.get_config(rportal.config)
        self._name = name
        self._attributes = attributes
        self._description = description
        self._start_time = None
        self._end_time = None
        self._launch_uuid = None
        self._launch_id = None

        log.debug("launch_name: %s", self.name)
        log.debug("launch_description: %s", self._description)
        log.debug("launch_attributes: %s", self._attributes)

    @property
    def name(self):
        """Get the name of the launch from config, env, etc."""
        if self._name is None:
            if self._config.get("merge_launches"):
                self._name = self._rportal.rpuid
            else:
                self._name = self._config.get("name", "RP_PreProc_Example_Launch")

        return self._name

    @property
    def description(self):
        """Get the description of the launch from config, env, etc."""
        default = "Example launch created by RP PreProc"
        if self._description is None:
            if self._config.get("merge"):
                self._description = self._config.get("name")
            else:
                self._description = self._config.get("description", default)

        return self._description

    @property
    def attributes(self):
        """Get launch attributes from the config"""
        if self._attributes is None:
            self._attributes = self._config.get("attributes", None)

        return self._attributes

    @property
    def start_time(self):
        """Get the launch start time"""
        if self._start_time is None:
            # TODO: get this from the config and default to NOW if None
            self._start_time = str(int(time.time() * 1000))

        return self._start_time

    @property
    def end_time(self):
        """Get the launch end time"""
        if self._end_time is None:
            # TODO: get this from the config and default to NOW if None
            self._end_time = str(int(time.time() * 1000))

        return self._end_time

    @property
    def launch_id(self):
        """Return the longint ID number"""
        if self._launch_id is None:
            self._launch_id = self._lookup_launch_id()

        return self._launch_id

    def _lookup_launch_id(self):
        api_path = "launch"
        get_string = "filter.eq.uuid={}".format(self._launch_uuid)
        response = self._rportal.api_get(api_path, get_data=[get_string])
        response_json = response.json()
        log.debug("GET LAUNCH ID BY UUID: %s", response_json)
        if response_json["content"]:
            response_filter = response_json["content"][0]
            launch_id = response_filter["id"]

            return launch_id

        return None

    def start(self, start_time=None):
        """Start a launch

        Args:
            start_time (str): Launch start time (default: None)

        Returns:
            launch_id (str) on success
            None on fail

        """

        if start_time is not None:
            self._start_time = start_time

        log.debug("Starting launch %s @ %s", self.name, self.start_time)
        self._launch_uuid = self._service.start_launch(
            name=self.name,
            start_time=self.start_time,
            attributes=self.attributes,
            description=self.description,
        )
        log.debug("Started launch with uuid %s ", self._launch_uuid)

        return self._launch_uuid

        # TODO: "requests.exceptions.HTTPError: 401 Client Error: "
        #       "Unauthorized for url: <https://reportportal.example.com/"
        #       "api/v1/myproject/launch "
        # TODO: UMB integration (here @ launch and start finish???)
        # TODO: set launch id class attr

    def finish(self, end_time=None):
        """Finish the launch"""
        if end_time is not None:
            self._end_time = end_time

        self._service.finish_launch(end_time=self.end_time)
        log.debug("time elapsed = %s - %s", self.end_time, self.start_time)
        time_elapsed = int(self.end_time) - int(self.start_time)
        self._rportal.launches.add(self.launch_id)
        log.debug("Finished launch")
        log.debug("Launch import completed in %s seconds", time_elapsed)

        # TODO: ERROR CHECKING and return the result
        return self.launch_id


class RpLog:
    """Log an event in ReportPortal.
    ReportPortal works with the concept of "logging" results"""

    def __init__(self, rportal):
        self.service = rportal.service

    def add_attachment(self, test_item_id, filepath, level="INFO"):
        """Add an attachment to a testcase in ReportPortal"""

        filename = os.path.basename(filepath)
        log.debug("Attaching file %s", filepath)
        with open(filepath, "rb") as file_handle:
            file_data = file_handle.read()
        mime_type = guess_type(filepath)[0]

        attachment = {"name": filename, "data": file_data, "mime": mime_type}
        log_item_id = self.service.log(
            str(int(time.time() * 1000)),
            message=filename,
            level=level,
            attachment=attachment,
            item_id=test_item_id,
        )

        log.debug("Log ID: %s", log_item_id)

        return log_item_id

    def add_attachments(self, fqpath, xml_name, tc_attach_dir, test_item_id):
        """Add attachments from testcase directory

        Args:
            fqpath (str): fullpath to attachments base dir
            xml_name (str): name from the xml file
            tc_attach_dir (str): testcase attachment subdirectory
            test_item_id (str): test item ID returned from RP
        """
        basepath = os.path.join(fqpath, tc_attach_dir)
        xml_dirpath = os.path.join(fqpath, xml_name, tc_attach_dir)
        testitem_path = os.path.join(fqpath, test_item_id)

        attached_files = []
        for dirpath in [xml_dirpath, basepath, testitem_path]:
            if os.path.exists(dirpath):
                # added followlinks=True for allowing symlinks
                for root, _, files in os.walk(dirpath, followlinks=True):
                    for file in files:
                        file_name = os.path.join(root, file)
                        log.debug(file_name)
                        self.add_attachment(test_item_id, file_name)
                        attached_files.append(file_name)
        return attached_files

    def add_message(
        self, message="N/A", level="INFO", msg_time=None, test_item_id=None
    ):
        """Log a message in ReportPortal"""
        if msg_time is None:
            msg_time = str(int(time.time() * 1000))
        self.service.log(
            time=msg_time, message=message, level=level, item_id=test_item_id
        )

    def truncate_message(self, msg_txt, tc_name, test_item_id, itempath, type):
        """Checks if the message to be logged is of a certain length. If it is greater than that
        then it truncates the message and logs it and then puts the remaining message as an attachment"
        """
        message_length = len(msg_txt)
        file_name = type + ".txt"
        if message_length > 500000:
            log.debug("\tMESSAGE LENGTH: %d", len(msg_txt))
            log.debug("TEST CASE: %s as %s", tc_name, test_item_id)

            # write out to attachments dir
            sysout_filepath = os.path.join(itempath, file_name)
            log.debug("WRITING AS ATTACHMENT: %s", sysout_filepath)

            if not os.path.exists(itempath):
                log.debug("Creating item dir %s", itempath)
                os.makedirs(itempath)
            write_fh = open(sysout_filepath, "wb")
            # TODO: make this limit configurable
            write_fh.write(msg_txt[-10000000:].encode("utf8"))
            write_fh.write(b"\n")
            write_fh.close()

            # TODO: make these limits configurable
            # cut message to first 1000 and last 3000 bytes
            message = msg_txt[:1000]
            message += (
                "\n\n...\n\n  TRUNCATED by RP PREPROC " "(see attachments)\n...\n\n"
            )
            message += msg_txt[-10000:]
            return message
        return msg_txt
