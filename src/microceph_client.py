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

import json
import logging
from abc import ABC
from typing import Any, List
from urllib.parse import quote

import requests_unixsocket
import urllib3
from requests.exceptions import ConnectionError, HTTPError
from requests.sessions import Session

LOG = logging.getLogger(__name__)
MICROCEPH_SOCKET = "/var/snap/microceph/common/state/control.socket"


# Add custom microceph daemon exceptions here
class RemoteException(Exception):
    """An Exception raised when interacting with the remote microclusterd service."""

    pass


class ClusterServiceUnavailableException(RemoteException):
    """Raised when cluster service is not yet bootstrapped."""

    pass


class CephServiceNotFoundException(RemoteException):
    """Raised when ceph service is not found."""

    pass


class UnrecognizedClusterConfigOption(RemoteException):
    """Raised when config option is not found."""

    pass


class MaintenanceOperationFailedException(RemoteException):
    """Raised when any maintenance operation failed."""

    def __init__(self, message: str, response: dict[Any]):
        super().__init__(message)
        self.response = response


class BaseService(ABC):
    """BaseService is the base service class for microclusterd services."""

    def __init__(self, session: Session, endpoint: str):
        """Creates a new BaseService for the  microceph daemon API.

        The service class is used to provide convenient APIs for clients to
        use when interacting with the microceph daemon api.


        :param session: session to use when interacting with the microceph daemon API
        :type: Session
        :param endpoint: http or unix socket microceph daemon API endpoint
        :type: str
        """
        self.__session = session
        self._endpoint = endpoint

    def _request(self, method, path, **kwargs):  # noqa: C901
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
            if "Daemon not yet initialized" in error or "Database is not yet initialized" in error:
                raise ClusterServiceUnavailableException("Microceph Cluster not initialized")
            elif 'failed to remove service from db "rgw": Service not found' in error:
                raise CephServiceNotFoundException("RGW Service not found")
            elif "Error EINVAL: unrecognized config option" in error:
                raise UnrecognizedClusterConfigOption("Option not found")
            elif "Error EINVAL: unrecognized config target" in error:
                raise UnrecognizedClusterConfigOption("Option not found")
            elif "maintenance operations failed" in error:
                raise MaintenanceOperationFailedException(error, response.json())
            else:
                raise e

        return response.json()

    def _get(self, path, data=None, **kwargs):
        kwargs.setdefault("allow_redirects", True)
        return self._request("get", path, data=data, **kwargs)

    def _head(self, path, **kwargs):
        kwargs.setdefault("allow_redirects", False)
        return self._request("head", path, **kwargs)

    def _post(self, path, data=None, **kwargs):
        return self._request("post", path, data=data, **kwargs)

    def _patch(self, path, data=None, **kwargs):
        return self._request("patch", path, data=data, **kwargs)

    def _put(self, path, data=None, **kwargs):
        return self._request("put", path, data=data, **kwargs)

    def _delete(self, path, data=None, **kwargs):
        return self._request("delete", path, data=data, **kwargs)

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
    """Lists and manages microceph cluster.

    Placeholder to add socket API calls to interact with microceph daemon instead
    of using microceph cli subprocess.
    """

    def list_services(self) -> List[dict]:
        """List all services."""
        services = self._get("/1.0/services")
        return services.get("metadata")

    def get_config(self, key: str | None = None) -> List[dict]:
        """Get value of the config parameter.

        If key is not specified, returns all the configs from microceph.
        """
        data = {"key": key}
        configs = self._get("/1.0/configs", data=json.dumps(data))
        return configs.get("metadata")

    def update_config(self, key: str, value: Any, skip_restart: bool = False):
        """Update configuration in database, create if missing."""
        data = {"key": key, "value": value, "wait": True, "skip_restart": skip_restart}
        self._put("/1.0/configs", data=json.dumps(data))

    def delete_config(self, key: str):
        """Delete configuration in database if exists."""
        data = {"key": key, "wait": True}
        self._delete("/1.0/configs", data=json.dumps(data))

    def get_mon_addresses(self) -> List[str]:
        """Get mon addresses."""
        mon_status = self._get("/1.0/services/mon").get("metadata")
        return mon_status.get("addresses", [])

    def exit_maintenance_mode(
        self, node: str, dry_run: bool, check_only: bool, ignore_check: bool
    ) -> List[str]:
        """Bring the node out of maintenance mode."""
        data = {
            "status": "non-maintenance",
            "dry_run": dry_run,
            "check_only": check_only,
            "ignore_check": ignore_check,
        }
        return self._put(f"/1.0/ops/maintenance/{node}", data=json.dumps(data))

    def enter_maintenance_mode(
        self,
        node: str,
        force: bool,
        dry_run: bool,
        set_noout: bool,
        stop_osds: bool,
        check_only: bool,
        ignore_check: bool,
    ) -> List[str]:
        """Bring the node into maintenance mode."""
        data = {
            "status": "maintenance",
            "force": force,
            "dry_run": dry_run,
            "set_noout": set_noout,
            "stop_osds": stop_osds,
            "check_only": check_only,
            "ignore_check": ignore_check,
        }
        return self._put(f"/1.0/ops/maintenance/{node}", data=json.dumps(data))
