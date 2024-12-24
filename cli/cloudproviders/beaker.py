import binascii
from urllib.parse import urlparse
from xmlrpc.client import SafeTransport, ServerProxy

import gssapi

from cli.cloudproviders.cacert import SSL_CONTEXT
from utility.log import Log

log = Log(__name__)

TIMEOUT = 280
API_VERSION = "2.2"
STATE_PENDING = "pending"
STATE_RUNNING = "running"
STATE_AVAILABLE = "available"
STATE_DESTROY = "destroy"


class Beaker:
    """Insterface for Openstack operations"""

    def __init__(self, api_version=API_VERSION, timeout=TIMEOUT, **config):
        """Initialize instance using provided details

        Args:
            api_version (str): API version
            timeout (int): Socket timeout

        **Kwargs:
            username (str): Name of user to be set for the session
            password (str): Password of user
            auth-url (str): Endpoint that can authenticate the user
            auth-version (str): Version to be used for authentication
            tenant-name (str): Name of user's project
            tenant-domain_id (str): id of user's project
            service-region (str): Realm to be used
            domain (str): Authentication domain to be used
        """
        self._nodes, self._volumes = {}, {}

    def create_node(self, name, timeout=300, interval=10, **config):
        """Create node on openstack

        Args:
            name (str): Name of node
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec

        Kwargs:
            image (str): Image to be used for node
            size (int): Node root volume size
            networks (list|tuple): Networks to be attached to node
        """
        print("CREATE NODE")

    def create_cluster(self, name, timeout=300, interval=10, **config):
        """Create node on openstack

        Args:
            name (str): Name of node
            timeout (int): Operation waiting time in sec
            interval (int): Operation retry time in sec

        Kwargs:
            image (str): Image to be used for node
            size (int): Node root volume size
            networks (list|tuple): Networks to be attached to node
        """
        print("CREATE NODE")

    def submit_job_xml(self, hub_url, job_xml):
        """Submits the job as xml to beaker

        Args:
            hub_url (str): Url of the beaker hib
            job_xml (str): job to be submitted to the beaker
        """
        rpc = ServerProxy(
            "%s/RPC2" % hub_url,
            context=SSL_CONTEXT,
            transport=_SafeTransportWithCookieJar(context=SSL_CONTEXT),
        )

        # log in with kerberos auth
        fqdn = urlparse(hub_url)[1].split(":")[0]
        ticket = self._generate_krbv_ticket("HTTP", fqdn)
        rpc.auth.login_krbv(ticket)
        rpc.jobs.upload(job_xml)

    def _generate_krbv_ticket(self, service, fqdn):
        server_name = gssapi.Name(
            "%s@%s" % (service, fqdn), name_type=gssapi.NameType.hostbased_service
        )
        ctx = gssapi.SecurityContext(
            name=server_name,
            flags=(
                gssapi.RequirementFlag.mutual_authentication
                | gssapi.RequirementFlag.replay_detection
                | gssapi.RequirementFlag.out_of_sequence_detection
            ),
        )
        token = ctx.step()
        # strip empty message containing only an ASN.1 ObjectIdentifier
        # (don't want to pull pyasn1 in as a dependency)
        token = token[17:]
        return binascii.b2a_base64(token).decode("ascii")

    def get_nodes(self, refresh=False):
        """Get nodes available from cloud

        Args:
            refresh (bool): Option to reload
        """
        print("GET NODE")

    def get_nodes_by_prefix(self, prefix, refresh=False):
        """Get list of nodes by prefix

        Args:
            prefix (str): Node name prefix
            refresh (bool): Option to reload
        """
        pass

    def get_node_id(self, name):
        """Get node by id

        Args:
            name (str): Node name
        """
        pass

    def get_node_by_id(self, id):
        """Get node by id

        Args:
            id (str): Node id
        """
        pass

    def get_node_by_name(self, name):
        """Get node object by name

        Args:
            name (str): Name of node
        """
        pass

    def get_node_state_by_name(self, name):
        """Get node status by name

        Args:
            name (str): Name of node
        """
        pass


class _SafeTransportWithCookieJar(SafeTransport, object):
    def parse_response(self, response):
        cookie = response.msg.get("Set-Cookie")
        if cookie is not None:
            cookie = cookie.split(":", 1)[0]
            key, value = cookie.split("=", 1)
            if self._extra_headers is None:
                # empty list gets reset to None in py2.7 by .make_connection()
                # as .get_host_info() returns None when there's not "login:password"
                # in server URL
                self._extra_headers = []
            self._extra_headers.append(("Cookie", "%s=%s" % (key, value)))
        return super(_SafeTransportWithCookieJar, self).parse_response(response)
