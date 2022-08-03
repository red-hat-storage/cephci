import requests
from uplink import Consumer, delete, get, json, post, put
from uplink.arguments import Body, Query
from uplink.auth import BearerToken

from .constants import API_ENDPOINT, BASE_URL, DELETE_PAGE_ENDOINT, UPDATE_PAGE_ENDPOINT


class Confluence(Consumer):
    def __init__(self, token, **kwargs):
        auth = BearerToken(token)
        session = requests.Session()
        session.verify = False
        super().__init__(base_url=BASE_URL, auth=auth, client=session, **kwargs)

    @get(API_ENDPOINT)
    def _get_page(self, title: Query, spaceKey: Query, expand: Query):
        """
        Gets the confluence page for given title and spaceKey
        Args:
            title: title of the page to be fetched
            spaceKey: spaceKey to the space to which the page belongs
            expand: any details to be expanded. For ex: body.storage, history, version

        Returns:
            The page info based on the details specified
        """

    @json
    @post(API_ENDPOINT)
    def _create_page(self, body: Body):
        """
        Creates the page for the details specified in body parameter
        Args:
            body: body of the page to be created

        Examples::
            {
                'type': 'page',
                'title': 'new child page in jenkins v3',
                'ancestors': [
                    {
                        'id': 276649316,
                    },
                ],
                'space': {
                    'key': 'rhcsqe',
                },
                'body': {
                    'storage': {
                        'value': '<p>This is <br/> a new child page</p>',
                        'representation': 'storage',
                    },
                },
            }

        Returns:
            The page info for the page created
        """

    @json
    @put(UPDATE_PAGE_ENDPOINT)
    def _update_page(self, pageId, body: Body):
        """
        Updates the page for the given pageId with the given body
        Args:
            pageId: pageId for the page to be updated
            body: body for the page to be updated

        Examples::
            {
                'type': 'page',
                'title': 'new child page in jenkins v3',
                'ancestors': [
                    {
                        'id': 276649316,
                    },
                ],
                'space': {
                    'key': 'rhcsqe',
                },
                'body': {
                    'storage': {
                        'value': '<p>This is <br/> a new child page</p>',
                        'representation': 'storage',
                    },
                },
                'version': {
                    'number': 2
                }
            }

        Returns:
            Info on the updated page
        """

    @delete(DELETE_PAGE_ENDOINT)
    def _delete_page(self, pageId):
        """
        Deletes the page with give pageId
        Args:
            pageId: pageId of the page to be deleted

        Returns:
            Info on the deleted page
        """

    def get_page(self, title, spaceKey, expand):
        """
        Gets the confluence page for given title and spaceKey
        Args:
            title: title of the page to be fetched
            spaceKey: spaceKey to the space to which the page belongs
            expand: any details to be expanded. For ex: body.storage, history, version

        Returns:
            The page info based on the details specified
        """
        resp = self._get_page(title, spaceKey, expand)
        return resp

    def create_page(
        self,
        title,
        spaceKey,
        parentId,
        data,
        page_type="page",
        representation="storage",
    ):
        """
        Creates the page for the details specified.
        Args:
            title: title of the page to be created
            spaceKey: spaceKey for the space under which the page to be created
            parentId: parentId for the page which should be the parent of the page being created
            data: data to be added into the page in plaintext or html format
            page_type: type of the page. Default: "page"
            representation: type of representation. Default: "storage"

        Returns:
            The info on the page created
        """
        body = {
            "type": page_type,
            "title": title,
            "ancestors": [
                {
                    "id": parentId,
                },
            ],
            "space": {
                "key": spaceKey,
            },
            "body": {
                "storage": {
                    "value": data,
                    "representation": representation,
                },
            },
        }
        resp = self._create_page(body)
        return resp

    def update_page(
        self,
        pageId,
        title,
        spaceKey,
        data,
        version,
        page_type="page",
        representation="storage",
    ):
        """
        Updates the page for the given parameters
        Args:
            pageId: id of the page to be updated
            title: title of the page to be updated
            spaceKey: spaceKey for the given page
            data: data to be updated
            version: update version for the page
            page_type: type of page. Default: "page"
            representation: type of representation. Default: "storage"

        Returns:
            Info on updated page
        """
        body = {
            "id": str(pageId),
            "type": page_type,
            "title": title,
            "space": {
                "key": spaceKey,
            },
            "body": {
                "storage": {
                    "value": data,
                    "representation": representation,
                },
            },
            "version": {
                "number": version,
            },
        }
        resp = self._update_page(pageId, body)
        return resp

    def delete_page(self, pageId):
        """
        Deletes the page with given pageId
        Args:
            pageId: id of the page to be deleted

        Returns:
            Info on deleted page
        """
        resp = self._delete_page(pageId)
        return resp
