# Copyright 2024 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""The cluster client module to interact with microceph cluster.

The client module can interact over unix socket or http. This
module can be used to manage microceph cluster. All the operations
on microceph can be performed using this module.
"""

import logging
from abc import ABC
from typing import List
from urllib.parse import quote

import requests_unixsocket
import urllib3
from requests.exceptions import ConnectionError, HTTPError
from requests.sessions import Session

LOG = logging.getLogger(__name__)
MICROCEPH_SOCKET = "/var/snap/microceph/common/state/control.socket"


class RemoteException(Exception):
    """An Exception raised when interacting with the remote microclusterd service."""

    pass


class ClusterServiceUnavailableException(RemoteException):
    """Raised when cluster service is not yet bootstrapped."""

    pass


class ServiceNotFoundException(RemoteException):
    """Raised when ceph service is not found."""

    pass


class BaseService(ABC):
    """BaseService is the base service class for microclusterd services."""

    def __init__(self, session: Session, endpoint: str):
        """Creates a new BaseService for the  microceph daemon API.

        The service class is used to provide convenient APIs for clients to
        use when interacting with the microceph daemon api.


        :param session: session to use when interacting with the microceph daemon API
        :type: Session
        """
        self.__session = session
        self._endpoint = endpoint

    def _request(self, method, path, **kwargs):
        if path.startswith("/"):
            path = path[1:]
        netloc = self._endpoint
        url = f"{netloc}/{path}"

        try:
            LOG.debug("[%s] %s, args=%s", method, url, kwargs)
            response = self.__session.request(method=method, url=url, **kwargs)
            LOG.debug("Response(%s) = %s", response, response.text)
        except ConnectionError as e:
            msg = str(e)
            if "FileNotFoundError" in msg:
                raise ClusterServiceUnavailableException(
                    "Microceph Cluster socket not found, is clusterd running ?"
                    " Check with 'snap services microceph.daemon'",
                ) from e
            raise ClusterServiceUnavailableException(msg)

        try:
            response.raise_for_status()
        except HTTPError as e:
            # Do some nice translating to microcephd exceptions
            error = response.json().get("error")
            LOG.warning(error)
            if 'failed to remove service from db "rgw": Service not found' in error:
                raise ServiceNotFoundException("RGW Service not found")
            else:
                raise e

        return response.json()

    def _get(self, path, **kwargs):
        kwargs.setdefault("allow_redirects", True)
        return self._request("get", path, **kwargs)

    def _head(self, path, **kwargs):
        kwargs.setdefault("allow_redirects", False)
        return self._request("head", path, **kwargs)

    def _post(self, path, data=None, json=None, **kwargs):
        return self._request("post", path, data=data, json=json, **kwargs)

    def _patch(self, path, data=None, **kwargs):
        return self._request("patch", path, data=data, **kwargs)

    def _put(self, path, data=None, **kwargs):
        return self._request("put", path, data=data, **kwargs)

    def _delete(self, path, **kwargs):
        return self._request("delete", path, **kwargs)

    def _options(self, path, **kwargs):
        kwargs.setdefault("allow_redirects", True)
        return self._request("options", path, **kwargs)


class Client:
    """A client for interacting with the remote client API."""

    def __init__(self, endpoint: str):
        super(Client, self).__init__()
        self._endpoint = endpoint
        self._session = Session()
        if self._endpoint.startswith("http+unix://"):
            self._session.mount(
                requests_unixsocket.DEFAULT_SCHEME, requests_unixsocket.UnixAdapter()
            )
        else:
            # TODO(gboutry): remove this when proper TLS communication is
            # implemented
            urllib3.disable_warnings()
            self._session.verify = False

        self.cluster = ClusterService(self._session, self._endpoint)

    @classmethod
    def from_socket(cls) -> "Client":
        """Return a client initialized to the clusterd socket."""
        escaped_socket_path = quote(MICROCEPH_SOCKET, safe="")
        return cls("http+unix://" + escaped_socket_path)

    @classmethod
    def from_http(cls, endpoint: str) -> "Client":
        """Return a client initialized to the clusterd http endpoint."""
        return cls(endpoint)


class ClusterService(BaseService):
    """Lists and manages cluster.

    TODO(team): Add bootstrap and other commands in microceph and use the
    socket API calls instead of subprocess.
    """

    def list_services(self) -> List[str]:
        """List all services."""
        services = self._get("/1.0/services")
        return services.get("metadata")
